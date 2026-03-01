# background_tasks.py
# Location: cms-backend-staging/background_tasks.py
"""
Background Task Scheduler
=========================
Runs periodic tasks:
1. Device expiration checks (every minute)
2. Content approval expiration (every 5 minutes)
3. Device status duration updates (on status change)

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
EXPIRATION_CHECK_INTERVAL = 60      # Check device expirations every minute
APPROVAL_EXPIRE_INTERVAL = 300      # Check approval expirations every 5 minutes
NOTIFICATION_CHECK_INTERVAL = 3600  # Check for upcoming expirations every hour


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
    last_expiration_check = 0
    last_approval_check = 0
    last_notification_check = 0
    
    while _scheduler_running:
        try:
            now = asyncio.get_event_loop().time()
            
            # Check device expirations every minute
            if now - last_expiration_check >= EXPIRATION_CHECK_INTERVAL:
                await _process_device_expirations()
                await _process_scheduled_tasks()
                last_expiration_check = now
            
            # Check approval expirations every 5 minutes
            if now - last_approval_check >= APPROVAL_EXPIRE_INTERVAL:
                await _expire_pending_requests()
                last_approval_check = now
            
            # Check for upcoming expiration notifications every hour
            if now - last_notification_check >= NOTIFICATION_CHECK_INTERVAL:
                await _send_expiration_notifications()
                last_notification_check = now
            
            # Sleep for 30 seconds before next check
            await asyncio.sleep(30)
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[SCHEDULER] Error in main loop: {e}")
            await asyncio.sleep(60)  # Wait a minute before retrying


async def _process_device_expirations():
    """Process devices that have reached their expiration date."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            # Find expired devices that are still active
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
                        
                        # Broadcast via WebSocket
                        if tenant_id:
                            asyncio.create_task(
                                notify_device_offline(tenant_id, mobile_id, device_name)
                            )
                        
                        print(f"[EXPIRATION] Deactivated device {mobile_id} (expired)")
                        
                    elif action == "delete":
                        # Soft delete - just mark as inactive and add deletion flag
                        cur.execute("""
                            UPDATE public.device 
                            SET is_active = FALSE, 
                                deactivated_at = NOW(),
                                deactivation_reason = 'Automatic deletion (expired)',
                                is_online = FALSE,
                                updated_at = NOW()
                            WHERE id = %s;
                        """, (did,))
                        print(f"[EXPIRATION] Marked device {mobile_id} for deletion (expired)")
                        
                    elif action == "notify_only":
                        # Just mark as notified
                        cur.execute("""
                            UPDATE public.device 
                            SET expiration_notified_at = NOW(), updated_at = NOW()
                            WHERE id = %s AND expiration_notified_at IS NULL;
                        """, (did,))
                        print(f"[EXPIRATION] Notification sent for device {mobile_id} (expired)")
                        
                except Exception as e:
                    print(f"[EXPIRATION] Error processing device {mobile_id}: {e}")
            
            if expired_devices:
                conn.commit()
                print(f"[EXPIRATION] Processed {len(expired_devices)} expired device(s)")


async def _process_scheduled_tasks():
    """Process scheduled tasks that are due."""
    with pg_conn() as conn:
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
                                    deactivation_reason = 'Scheduled expiration',
                                    is_online = FALSE, updated_at = NOW()
                                WHERE id = %s AND is_active = TRUE
                                RETURNING mobile_id, device_name, tenant_id;
                            """, (device_id,))
                            rows = cur.fetchall()
                            devices_affected = len(rows)
                            
                            for mobile_id, device_name, tid in rows:
                                asyncio.create_task(
                                    notify_device_offline(tid, mobile_id, device_name)
                                )
                                
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
                                asyncio.create_task(
                                    notify_device_offline(tid, mobile_id, device_name)
                                )
                                
                        else:
                            # Company-wide
                            cur.execute("""
                                UPDATE public.device 
                                SET is_active = FALSE, deactivated_at = NOW(),
                                    deactivation_reason = 'Scheduled company expiration',
                                    is_online = FALSE, updated_at = NOW()
                                WHERE tenant_id = %s AND is_active = TRUE
                                RETURNING mobile_id, device_name, tenant_id;
                            """, (tenant_id,))
                            rows = cur.fetchall()
                            devices_affected = len(rows)
                            
                            for mobile_id, device_name, tid in rows:
                                asyncio.create_task(
                                    notify_device_offline(tid, mobile_id, device_name)
                                )
                    
                    elif action == "activate":
                        if device_id:
                            cur.execute("""
                                UPDATE public.device 
                                SET is_active = TRUE, deactivated_at = NULL, 
                                    deactivation_reason = NULL, updated_at = NOW()
                                WHERE id = %s;
                            """, (device_id,))
                            devices_affected = cur.rowcount
                    
                    elif action == "refresh_content":
                        if device_id:
                            cur.execute("""
                                UPDATE public.device 
                                SET download_status = FALSE, needs_refresh = TRUE, updated_at = NOW()
                                WHERE id = %s;
                            """, (device_id,))
                            devices_affected = cur.rowcount
                        elif group_id:
                            cur.execute("""
                                UPDATE public.device 
                                SET download_status = FALSE, needs_refresh = TRUE, updated_at = NOW()
                                WHERE id IN (SELECT did FROM public.device_assignment WHERE gid = %s);
                            """, (group_id,))
                            devices_affected = cur.rowcount
                    
                    # Log execution
                    cur.execute("""
                        INSERT INTO public.schedule_execution_log (schedule_id, status, devices_affected)
                        VALUES (%s, 'success', %s);
                    """, (schedule_id, devices_affected))
                    
                    # Update schedule status
                    cur.execute("""
                        UPDATE public.device_schedule 
                        SET last_executed_at = NOW(), 
                            execution_count = execution_count + 1,
                            status = CASE 
                                WHEN repeat_type IS NULL THEN 'completed'
                                ELSE status 
                            END,
                            updated_at = NOW()
                        WHERE id = %s;
                    """, (schedule_id,))
                    
                    print(f"[SCHEDULER] Executed schedule #{schedule_id}: {action} on {devices_affected} device(s)")
                    
                except Exception as e:
                    cur.execute("""
                        INSERT INTO public.schedule_execution_log (schedule_id, status, error_message)
                        VALUES (%s, 'failed', %s);
                    """, (schedule_id, str(e)))
                    print(f"[SCHEDULER] Error executing schedule #{schedule_id}: {e}")
            
            if schedules:
                conn.commit()


async def _expire_pending_requests():
    """Expire content change requests that have passed their expiration time."""
    with pg_conn() as conn:
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
                print(f"[APPROVAL] Expired {len(expired)} pending content change request(s)")


async def _send_expiration_notifications():
    """Send notifications for devices expiring soon."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            # Find devices expiring in the next 7 days that haven't been notified
            cur.execute("""
                SELECT d.id, d.mobile_id, d.device_name, d.expires_at, d.tenant_id,
                       c.name as company_name, c.approval_notify_email
                FROM public.device d
                JOIN public.company c ON c.id = d.tenant_id
                WHERE d.expires_at IS NOT NULL
                  AND d.expires_at <= NOW() + INTERVAL '7 days'
                  AND d.expires_at > NOW()
                  AND d.is_active = TRUE
                  AND d.expiration_notified_at IS NULL
                LIMIT 100;
            """)
            
            devices_to_notify = cur.fetchall()
            
            for did, mobile_id, device_name, expires_at, tenant_id, company_name, notify_email in devices_to_notify:
                # Mark as notified
                cur.execute("""
                    UPDATE public.device SET expiration_notified_at = NOW() WHERE id = %s;
                """, (did,))
                
                # TODO: Send actual email notification if notify_email is set
                print(f"[NOTIFICATION] Device {mobile_id} ({device_name}) expires at {expires_at}")
            
            if devices_to_notify:
                conn.commit()
                print(f"[NOTIFICATION] Sent expiration warnings for {len(devices_to_notify)} device(s)")


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
