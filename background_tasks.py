# background_tasks.py
# Location: cms-backend-staging/background_tasks.py
"""
Background Task Scheduler
=========================
Runs periodic tasks:
1. Company expiration checks (every minute) - NEW
2. Device expiration checks (every minute)
3. Content approval expiration (every 5 minutes)
4. Device status duration updates (on status change)

Add to startup in device_video_shop_group.py:
    from background_tasks import start_background_tasks, stop_background_tasks
    
    @app.on_event("startup")
    async def startup_event():
        ...
        await start_background_tasks()
    
    @app.on_event("shutdown")
    async def shutdown_event():
        await stop_background_tasks()
        ...
"""

import asyncio
from datetime import datetime
from typing import Optional

from database import pg_conn
from websocket_routes import notify_device_offline

# Control flags
_scheduler_running = False
_scheduler_task: Optional[asyncio.Task] = None

# Check intervals (in seconds)
COMPANY_EXPIRATION_CHECK_INTERVAL = 60  # Check company expirations every minute
DEVICE_EXPIRATION_CHECK_INTERVAL = 60   # Check device expirations every minute
APPROVAL_EXPIRE_INTERVAL = 300          # Check approval expirations every 5 minutes
NOTIFICATION_CHECK_INTERVAL = 3600      # Check for upcoming expirations every hour


async def start_background_tasks():
    """Start all background task loops."""
    global _scheduler_running, _scheduler_task
    _scheduler_running = True
    _scheduler_task = asyncio.create_task(_main_scheduler_loop())
    print("[SCHEDULER] Background tasks started")


async def stop_background_tasks():
    """Stop all background tasks."""
    global _scheduler_running, _scheduler_task
    _scheduler_running = False
    if _scheduler_task:
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except asyncio.CancelledError:
            pass
    print("[SCHEDULER] Background tasks stopped")


async def _main_scheduler_loop():
    """Main scheduler loop that runs periodic tasks."""
    last_company_expiration_check = 0
    last_device_expiration_check = 0
    last_approval_check = 0
    last_notification_check = 0

    while _scheduler_running:
        try:
            now = asyncio.get_event_loop().time()

            run_company  = now - last_company_expiration_check  >= COMPANY_EXPIRATION_CHECK_INTERVAL
            run_device   = now - last_device_expiration_check   >= DEVICE_EXPIRATION_CHECK_INTERVAL
            run_approval = now - last_approval_check            >= APPROVAL_EXPIRE_INTERVAL
            run_notify   = now - last_notification_check        >= NOTIFICATION_CHECK_INTERVAL

            # Open ONE connection for all tasks that are due this tick
            if run_company or run_device or run_approval or run_notify:
                try:
                    with pg_conn() as conn:
                        if run_company:
                            _process_company_expirations_conn(conn)
                            last_company_expiration_check = now

                        if run_device:
                            _process_device_expirations_conn(conn)
                            _process_scheduled_tasks_conn(conn)
                            last_device_expiration_check = now

                        if run_approval:
                            _expire_pending_requests_conn(conn)
                            last_approval_check = now

                        if run_notify:
                            _send_expiration_notifications_conn(conn)
                            last_notification_check = now
                except Exception as e:
                    print(f"[SCHEDULER] DB error in tick: {e}")

            # Sleep for 30 seconds before next check
            await asyncio.sleep(30)

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[SCHEDULER] Error in main loop: {e}")
            await asyncio.sleep(60)  # Wait a minute before retrying


def _process_company_expirations_conn(conn):
    """Process company expirations using an existing connection."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, slug, expires_at, expiration_status, grace_period_days, suspended_at
                FROM public.company
                WHERE expires_at IS NOT NULL
                  AND suspended_at IS NULL
                ORDER BY expires_at ASC
                LIMIT 100;
            """)
            companies = cur.fetchall()
            updated_count = 0

            for company_id, name, slug, expires_at, current_status, grace_days, suspended_at in companies:
                new_status = _calculate_company_status(expires_at, grace_days or 7, suspended_at)
                if new_status != current_status:
                    cur.execute("""
                        UPDATE public.company
                        SET expiration_status = %s, updated_at = NOW()
                        WHERE id = %s;
                    """, (new_status, company_id))
                    cur.execute("""
                        INSERT INTO public.company_expiration_log
                        (company_id, event_type, old_status, new_status, performed_by, notes)
                        VALUES (%s, %s, %s, %s, NULL, %s);
                    """, (company_id, f"status_changed_to_{new_status}", current_status, new_status,
                          "Automatic status update by scheduler"))
                    updated_count += 1
                    print(f"[COMPANY_EXPIRATION] {name} ({slug}): {current_status} -> {new_status}")

            if updated_count > 0:
                conn.commit()
                print(f"[COMPANY_EXPIRATION] Updated {updated_count} company status(es)")
    except Exception as e:
        print(f"[COMPANY_EXPIRATION] Error: {e}")


def _calculate_company_status(expires_at, grace_period_days: int, suspended_at) -> str:
    """Calculate what the company's expiration status should be."""
    if suspended_at:
        return "suspended"
    
    if not expires_at:
        return "active"
    
    # Make timezone aware if needed
    from datetime import timezone
    now = datetime.now(timezone.utc)
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    
    if now < expires_at:
        return "active"
    
    # Past expiration
    days_since = (now - expires_at).days
    
    if days_since <= grace_period_days:
        return "grace_period"
    
    return "expired"


def _process_device_expirations_conn(conn):
    """Process device expirations using an existing connection."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, mobile_id, device_name, tenant_id, expiration_action
                FROM public.device
                WHERE expires_at <= NOW()
                  AND is_active = TRUE
                  AND expires_at IS NOT NULL
                LIMIT 100;
            """)
            expired_devices = cur.fetchall()

            for did, mobile_id, device_name, tenant_id, action in expired_devices:
                try:
                    if action == "deactivate" or action is None:
                        cur.execute("""
                            UPDATE public.device
                            SET is_active = FALSE,
                                deactivated_at = NOW(),
                                deactivation_reason = 'Automatic expiration',
                                is_online = FALSE,
                                updated_at = NOW()
                            WHERE id = %s;
                        """, (did,))
                        if tenant_id:
                            asyncio.create_task(notify_device_offline(tenant_id, mobile_id, device_name))
                        print(f"[DEVICE_EXPIRATION] Deactivated {mobile_id}")
                    elif action == "notify_only":
                        cur.execute("""
                            UPDATE public.device
                            SET expiration_notified_at = NOW(), updated_at = NOW()
                            WHERE id = %s AND expiration_notified_at IS NULL;
                        """, (did,))
                        print(f"[DEVICE_EXPIRATION] Notified {mobile_id}")
                except Exception as e:
                    print(f"[DEVICE_EXPIRATION] Error processing {mobile_id}: {e}")

            if expired_devices:
                conn.commit()
                print(f"[DEVICE_EXPIRATION] Processed {len(expired_devices)} device(s)")
    except Exception as e:
        print(f"[DEVICE_EXPIRATION] Error: {e}")


def _process_scheduled_tasks_conn(conn):
    """Process scheduled tasks using an existing connection."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, tenant_id, device_id, group_id, schedule_type, action, action_params
                FROM public.device_schedule
                WHERE status = 'active'
                  AND next_execution_at <= NOW()
                LIMIT 50;
            """)
            schedules = cur.fetchall()

            for schedule in schedules:
                schedule_id, tenant_id, device_id, group_id, schedule_type, action, action_params = schedule
                try:
                    devices_affected = 0
                    if action == "deactivate":
                        if device_id:
                            cur.execute("""
                                UPDATE public.device
                                SET is_active = FALSE, deactivated_at = NOW(),
                                    deactivation_reason = 'Scheduled deactivation',
                                    is_online = FALSE, updated_at = NOW()
                                WHERE id = %s AND is_active = TRUE
                                RETURNING mobile_id, device_name, tenant_id;
                            """, (device_id,))
                            rows = cur.fetchall()
                            devices_affected = len(rows)
                            for mobile_id, device_name, tid in rows:
                                asyncio.create_task(notify_device_offline(tid, mobile_id, device_name))
                        elif group_id:
                            cur.execute("""
                                UPDATE public.device
                                SET is_active = FALSE, deactivated_at = NOW(),
                                    deactivation_reason = 'Scheduled group expiration',
                                    is_online = FALSE, updated_at = NOW()
                                WHERE id IN (SELECT did FROM public.device_assignment WHERE gid = %s)
                                  AND is_active = TRUE
                                RETURNING mobile_id, device_name, tenant_id;
                            """, (group_id,))
                            rows = cur.fetchall()
                            devices_affected = len(rows)
                            for mobile_id, device_name, tid in rows:
                                asyncio.create_task(notify_device_offline(tid, mobile_id, device_name))
                    elif action == "activate":
                        if device_id:
                            cur.execute("""
                                UPDATE public.device
                                SET is_active = TRUE, deactivated_at = NULL,
                                    deactivation_reason = NULL, updated_at = NOW()
                                WHERE id = %s;
                            """, (device_id,))
                            devices_affected = cur.rowcount

                    cur.execute("""
                        INSERT INTO public.schedule_execution_log (schedule_id, status, devices_affected)
                        VALUES (%s, 'success', %s);
                    """, (schedule_id, devices_affected))
                    cur.execute("""
                        UPDATE public.device_schedule
                        SET last_executed_at = NOW(),
                            execution_count = execution_count + 1,
                            status = CASE WHEN repeat_type IS NULL THEN 'completed' ELSE status END,
                            updated_at = NOW()
                        WHERE id = %s;
                    """, (schedule_id,))
                    print(f"[SCHEDULER] Executed schedule #{schedule_id}: {action}")
                except Exception as e:
                    cur.execute("""
                        INSERT INTO public.schedule_execution_log (schedule_id, status, error_message)
                        VALUES (%s, 'failed', %s);
                    """, (schedule_id, str(e)))
                    print(f"[SCHEDULER] Error schedule #{schedule_id}: {e}")

            if schedules:
                conn.commit()
    except Exception as e:
        print(f"[SCHEDULER] Error in scheduled tasks: {e}")


def _expire_pending_requests_conn(conn):
    """Expire content change requests using an existing connection."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE public.content_change_request
                SET status = 'expired', updated_at = NOW()
                WHERE status = 'pending' AND expires_at < NOW()
                RETURNING id;
            """)
            expired = cur.fetchall()
            if expired:
                conn.commit()
                print(f"[APPROVAL] Expired {len(expired)} pending request(s)")
    except Exception as e:
        print(f"[APPROVAL] Error: {e}")


def _send_expiration_notifications_conn(conn):
    """Send expiration notifications using an existing connection."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, slug, expires_at, expiration_notify_email
                FROM public.company
                WHERE expires_at IS NOT NULL
                  AND expires_at <= NOW() + INTERVAL '7 days'
                  AND expires_at > NOW()
                  AND expiration_status = 'active'
                  AND expiration_notified_at IS NULL
                LIMIT 50;
            """)
            companies_to_notify = cur.fetchall()

            for company_id, name, slug, expires_at, notify_email in companies_to_notify:
                cur.execute("""
                    UPDATE public.company SET expiration_notified_at = NOW() WHERE id = %s;
                """, (company_id,))
                days_left = (expires_at - datetime.utcnow()).days
                print(f"[NOTIFICATION] Company {name} ({slug}) expires in {days_left} days")
                # TODO: Send actual email notification

            if companies_to_notify:
                conn.commit()
                print(f"[NOTIFICATION] Notified {len(companies_to_notify)} company(ies)")
    except Exception as e:
        print(f"[NOTIFICATION] Error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# STATUS DURATION TRACKING - Call these when device status changes
# ══════════════════════════════════════════════════════════════════════════════

def update_device_status_duration(conn, device_id: int, new_is_online: bool, old_is_online: bool):
    """
    Update device status duration tracking when status changes.
    Call this from set_device_online_status endpoint when status actually changes.
    """
    if new_is_online == old_is_online:
        return  # No change
    
    with conn.cursor() as cur:
        if new_is_online:
            # Device just came online - update offline duration
            cur.execute("""
                UPDATE public.device 
                SET total_offline_seconds = total_offline_seconds + 
                        COALESCE(EXTRACT(EPOCH FROM (NOW() - current_status_since))::BIGINT, 0),
                    last_offline_duration_seconds = 
                        COALESCE(EXTRACT(EPOCH FROM (NOW() - current_status_since))::BIGINT, 0),
                    current_status_since = NOW()
                WHERE id = %s;
            """, (device_id,))
        else:
            # Device just went offline - update online duration
            cur.execute("""
                UPDATE public.device 
                SET total_online_seconds = total_online_seconds + 
                        COALESCE(EXTRACT(EPOCH FROM (NOW() - current_status_since))::BIGINT, 0),
                    last_online_duration_seconds = 
                        COALESCE(EXTRACT(EPOCH FROM (NOW() - current_status_since))::BIGINT, 0),
                    current_status_since = NOW()
                WHERE id = %s;
            """, (device_id,))
