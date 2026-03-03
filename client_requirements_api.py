# client_requirements_api.py
# Location: cms-backend-staging/client_requirements_api.py
"""
API Endpoints for Client Requirements
=====================================
1. Storage Management (80% limit)
2. Device Status Duration
3. Device Expiration & Scheduling
4. Content Approval Workflow

Include this router in device_video_shop_group.py:
    from client_requirements_api import router as client_requirements_router
    app.include_router(client_requirements_router, tags=["Client Requirements"])
"""

import asyncio
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from enum import Enum

from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from pydantic import BaseModel, Field

from database import pg_conn
from tenant_context import get_current_user, require_permission, log_audit, active_sessions
from websocket_routes import notify_device_status, notify_pending_approvals
from video_service import presign_get_object, _parse_s3_link

router = APIRouter()


# ══════════════════════════════════════════════════════════════════════════════
# 1. STORAGE MANAGEMENT (80% Limit)
# ══════════════════════════════════════════════════════════════════════════════

class StorageReportIn(BaseModel):
    """Device reports its storage status"""
    total_bytes: int = Field(..., description="Total device storage in bytes")
    used_bytes: int = Field(..., description="Used storage in bytes")
    content_bytes: int = Field(0, description="Storage used by downloaded content")


class StorageReportOut(BaseModel):
    mobile_id: str
    total_bytes: int
    used_bytes: int
    available_for_content_bytes: int
    storage_limit_percent: int
    max_content_bytes: int
    current_content_bytes: int
    can_download_more: bool
    storage_warning: Optional[str] = None


class StorageAllocationOut(BaseModel):
    """Response when requesting content download"""
    mobile_id: str
    can_download: bool
    available_bytes: int
    requested_bytes: int
    reason: Optional[str] = None


@router.post("/device/{mobile_id}/storage/report", response_model=StorageReportOut)
def report_device_storage(mobile_id: str, body: StorageReportIn):
    """
    Device reports its storage capacity and usage.
    Called by Android app on startup and periodically.
    """
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, storage_limit_percent FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            
            did, storage_limit_percent = row
            storage_limit_percent = storage_limit_percent or 80
            
            # Calculate available storage for content (e.g., 80% of total)
            max_content_bytes = int(body.total_bytes * (storage_limit_percent / 100.0))
            available_for_content = max_content_bytes - body.content_bytes
            
            # Update device storage info
            cur.execute("""
                UPDATE public.device 
                SET total_storage_bytes = %s,
                    used_storage_bytes = %s,
                    available_storage_bytes = %s,
                    last_storage_report_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s;
            """, (body.total_bytes, body.used_bytes, available_for_content, did))
            
        conn.commit()
        
        # Determine if there's a warning
        warning = None
        usage_percent = (body.content_bytes / max_content_bytes * 100) if max_content_bytes > 0 else 0
        if usage_percent >= 95:
            warning = "Storage critical: Content storage is 95%+ full"
        elif usage_percent >= 80:
            warning = "Storage warning: Content storage is 80%+ full"
        
        return StorageReportOut(
            mobile_id=mobile_id,
            total_bytes=body.total_bytes,
            used_bytes=body.used_bytes,
            available_for_content_bytes=max(0, available_for_content),
            storage_limit_percent=storage_limit_percent,
            max_content_bytes=max_content_bytes,
            current_content_bytes=body.content_bytes,
            can_download_more=available_for_content > 0,
            storage_warning=warning
        )


@router.get("/device/{mobile_id}/storage/check")
def check_storage_allocation(
    mobile_id: str,
    required_bytes: int = Query(..., description="Bytes needed for new content")
):
    """
    Check if device has enough storage for new content.
    Called before downloading new content.
    """
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT available_storage_bytes, storage_limit_percent, total_storage_bytes
                FROM public.device WHERE mobile_id = %s LIMIT 1;
            """, (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            
            available, limit_percent, total = row
            available = available or 0
            
            can_download = available >= required_bytes
            
            return StorageAllocationOut(
                mobile_id=mobile_id,
                can_download=can_download,
                available_bytes=available,
                requested_bytes=required_bytes,
                reason=None if can_download else f"Insufficient storage. Need {required_bytes} bytes but only {available} available."
            )


@router.put("/device/{mobile_id}/storage/limit")
def set_storage_limit(
    mobile_id: str,
    limit_percent: int = Query(..., ge=50, le=95, description="Storage limit percentage (50-95%)"),
    user: Dict = Depends(get_current_user)
):
    """Set the storage limit percentage for a device."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE public.device 
                SET storage_limit_percent = %s, updated_at = NOW()
                WHERE mobile_id = %s
                RETURNING id;
            """, (limit_percent, mobile_id))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
        conn.commit()
        
    return {"mobile_id": mobile_id, "storage_limit_percent": limit_percent}


# ══════════════════════════════════════════════════════════════════════════════
# 2. DEVICE STATUS DURATION TRACKING
# ══════════════════════════════════════════════════════════════════════════════

class DeviceStatusDurationOut(BaseModel):
    mobile_id: str
    is_online: bool
    current_status: str  # "online" or "offline"
    current_status_since: Optional[datetime]
    current_status_duration_seconds: int
    current_status_duration_human: str  # "2 hours 15 minutes"
    total_online_seconds: int
    total_offline_seconds: int
    uptime_percentage: float
    last_online_session_seconds: int
    last_offline_period_seconds: int


def format_duration(seconds: int) -> str:
    """Convert seconds to human-readable duration."""
    if seconds < 60:
        return f"{seconds} seconds"
    elif seconds < 3600:
        mins = seconds // 60
        secs = seconds % 60
        return f"{mins} minute{'s' if mins != 1 else ''}" + (f" {secs} second{'s' if secs != 1 else ''}" if secs > 0 else "")
    elif seconds < 86400:
        hours = seconds // 3600
        mins = (seconds % 3600) // 60
        return f"{hours} hour{'s' if hours != 1 else ''}" + (f" {mins} minute{'s' if mins != 1 else ''}" if mins > 0 else "")
    else:
        days = seconds // 86400
        hours = (seconds % 86400) // 3600
        return f"{days} day{'s' if days != 1 else ''}" + (f" {hours} hour{'s' if hours != 1 else ''}" if hours > 0 else "")


@router.get("/device/{mobile_id}/status/duration", response_model=DeviceStatusDurationOut)
def get_device_status_duration(mobile_id: str):
    """
    Get device status with duration information.
    Shows how long device has been online/offline.
    """
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT is_online, current_status_since, 
                       total_online_seconds, total_offline_seconds,
                       last_online_duration_seconds, last_offline_duration_seconds,
                       EXTRACT(EPOCH FROM (NOW() - current_status_since))::INT as current_duration
                FROM public.device WHERE mobile_id = %s LIMIT 1;
            """, (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            
            is_online, status_since, total_online, total_offline, last_online, last_offline, current_duration = row
            
            total_online = total_online or 0
            total_offline = total_offline or 0
            current_duration = current_duration or 0
            
            # Add current duration to the appropriate total for accurate calculation
            if is_online:
                effective_online = total_online + current_duration
                effective_offline = total_offline
            else:
                effective_online = total_online
                effective_offline = total_offline + current_duration
            
            total_time = effective_online + effective_offline
            uptime_percentage = (effective_online / total_time * 100) if total_time > 0 else 0.0
            
            return DeviceStatusDurationOut(
                mobile_id=mobile_id,
                is_online=is_online,
                current_status="online" if is_online else "offline",
                current_status_since=status_since,
                current_status_duration_seconds=current_duration,
                current_status_duration_human=format_duration(current_duration),
                total_online_seconds=effective_online,
                total_offline_seconds=effective_offline,
                uptime_percentage=round(uptime_percentage, 2),
                last_online_session_seconds=last_online or 0,
                last_offline_period_seconds=last_offline or 0
            )


@router.get("/devices/status/duration")
def get_all_devices_status_duration(
    user: Dict = Depends(get_current_user),
    online_only: bool = Query(False, description="Filter to online devices only"),
    limit: int = Query(100, le=500)
):
    """Get status duration for all devices (for dashboard display)."""
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id") or 1
    
    with pg_conn() as conn:
        with conn.cursor() as cur:
            filter_clause = "AND is_online = TRUE" if online_only else ""
            cur.execute(f"""
                SELECT mobile_id, device_name, is_online, current_status_since,
                       EXTRACT(EPOCH FROM (NOW() - current_status_since))::INT as current_duration,
                       total_online_seconds, total_offline_seconds
                FROM public.device 
                WHERE tenant_id = %s {filter_clause}
                ORDER BY is_online DESC, current_status_since DESC
                LIMIT %s;
            """, (tenant_id, limit))
            
            devices = []
            for row in cur.fetchall():
                mobile_id, device_name, is_online, status_since, current_duration, total_online, total_offline = row
                current_duration = current_duration or 0
                devices.append({
                    "mobile_id": mobile_id,
                    "device_name": device_name,
                    "is_online": is_online,
                    "current_status": "online" if is_online else "offline",
                    "current_status_since": status_since.isoformat() if status_since else None,
                    "current_status_duration_seconds": current_duration,
                    "current_status_duration_human": format_duration(current_duration),
                })
            
            return {"devices": devices, "count": len(devices)}


# ══════════════════════════════════════════════════════════════════════════════
# 3. DEVICE EXPIRATION & SCHEDULING SYSTEM
# ══════════════════════════════════════════════════════════════════════════════

class ScheduleType(str, Enum):
    EXPIRATION = "expiration"
    MAINTENANCE = "maintenance"
    CONTENT_REFRESH = "content_refresh"
    CUSTOM = "custom"


class ScheduleAction(str, Enum):
    DEACTIVATE = "deactivate"
    ACTIVATE = "activate"
    REFRESH_CONTENT = "refresh_content"
    NOTIFY = "notify"
    DELETE = "delete"


class RepeatType(str, Enum):
    NONE = None
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"


class DeviceScheduleCreateIn(BaseModel):
    """Create a schedule for device(s)"""
    device_id: Optional[int] = Field(None, description="Specific device ID (null for group/company-wide)")
    group_id: Optional[int] = Field(None, description="Group ID (null for specific device or company-wide)")
    schedule_type: ScheduleType
    scheduled_at: datetime = Field(..., description="When to execute")
    action: ScheduleAction
    action_params: Optional[Dict] = None
    repeat_type: Optional[str] = None
    repeat_interval: int = 1
    repeat_until: Optional[datetime] = None
    notify_days_before: int = 7
    notify_email: Optional[str] = None


class DeviceScheduleOut(BaseModel):
    id: int
    tenant_id: int
    device_id: Optional[int]
    device_name: Optional[str]
    group_id: Optional[int]
    group_name: Optional[str]
    schedule_type: str
    scheduled_at: datetime
    action: str
    action_params: Optional[Dict]
    repeat_type: Optional[str]
    status: str
    next_execution_at: Optional[datetime]
    notify_days_before: int
    created_at: datetime


@router.post("/schedules", response_model=DeviceScheduleOut)
def create_device_schedule(body: DeviceScheduleCreateIn, user: Dict = Depends(get_current_user)):
    """
    Create a schedule for device expiration or other actions.
    Can target: specific device, all devices in a group, or all devices in company.
    """
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id") or 1
    user_id = user.get("user_id")
    
    with pg_conn() as conn:
        with conn.cursor() as cur:
            # Validate device/group exists
            device_name = None
            group_name = None
            
            if body.device_id:
                cur.execute("SELECT device_name FROM public.device WHERE id = %s AND tenant_id = %s;", (body.device_id, tenant_id))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Device not found")
                device_name = row[0]
            
            if body.group_id:
                cur.execute('SELECT gname FROM public."group" WHERE id = %s AND tenant_id = %s;', (body.group_id, tenant_id))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Group not found")
                group_name = row[0]
            
            # Calculate next execution
            next_execution = body.scheduled_at
            
            cur.execute("""
                INSERT INTO public.device_schedule (
                    tenant_id, device_id, group_id, schedule_type, scheduled_at,
                    action, action_params, repeat_type, repeat_interval, repeat_until,
                    notify_days_before, notify_email, status, next_execution_at, created_by
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'active', %s, %s)
                RETURNING id, created_at;
            """, (
                tenant_id, body.device_id, body.group_id, body.schedule_type.value,
                body.scheduled_at, body.action.value, 
                None if body.action_params is None else str(body.action_params),
                body.repeat_type, body.repeat_interval, body.repeat_until,
                body.notify_days_before, body.notify_email, next_execution, user_id
            ))
            
            schedule_id, created_at = cur.fetchone()
            
            # If this is an expiration schedule for a specific device, also set expires_at on device
            if body.schedule_type == ScheduleType.EXPIRATION and body.device_id and body.action == ScheduleAction.DEACTIVATE:
                cur.execute("""
                    UPDATE public.device 
                    SET expires_at = %s, expiration_action = %s, updated_at = NOW()
                    WHERE id = %s;
                """, (body.scheduled_at, body.action.value, body.device_id))
            
        conn.commit()
        
        return DeviceScheduleOut(
            id=schedule_id,
            tenant_id=tenant_id,
            device_id=body.device_id,
            device_name=device_name,
            group_id=body.group_id,
            group_name=group_name,
            schedule_type=body.schedule_type.value,
            scheduled_at=body.scheduled_at,
            action=body.action.value,
            action_params=body.action_params,
            repeat_type=body.repeat_type,
            status="active",
            next_execution_at=next_execution,
            notify_days_before=body.notify_days_before,
            created_at=created_at
        )


@router.get("/schedules")
def list_schedules(
    user: Dict = Depends(get_current_user),
    status: str = Query("active", description="Filter by status"),
    device_id: Optional[int] = None,
    limit: int = Query(50, le=200)
):
    """List all schedules for the company."""
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id") or 1
    
    with pg_conn() as conn:
        with conn.cursor() as cur:
            filters = ["s.tenant_id = %s"]
            params = [tenant_id]
            
            if status != "all":
                filters.append("s.status = %s")
                params.append(status)
            
            if device_id:
                filters.append("s.device_id = %s")
                params.append(device_id)
            
            params.append(limit)
            
            cur.execute(f"""
                SELECT s.id, s.device_id, d.device_name, d.mobile_id, s.group_id, g.gname,
                       s.schedule_type, s.scheduled_at, s.action, s.status,
                       s.next_execution_at, s.notify_days_before, s.created_at
                FROM public.device_schedule s
                LEFT JOIN public.device d ON d.id = s.device_id
                LEFT JOIN public."group" g ON g.id = s.group_id
                WHERE {' AND '.join(filters)}
                ORDER BY s.next_execution_at ASC NULLS LAST
                LIMIT %s;
            """, params)
            
            schedules = []
            for row in cur.fetchall():
                schedules.append({
                    "id": row[0],
                    "device_id": row[1],
                    "device_name": row[2],
                    "mobile_id": row[3],
                    "group_id": row[4],
                    "group_name": row[5],
                    "schedule_type": row[6],
                    "scheduled_at": row[7].isoformat() if row[7] else None,
                    "action": row[8],
                    "status": row[9],
                    "next_execution_at": row[10].isoformat() if row[10] else None,
                    "notify_days_before": row[11],
                    "created_at": row[12].isoformat() if row[12] else None,
                })
            
            return {"schedules": schedules, "count": len(schedules)}


@router.put("/device/{mobile_id}/expiration")
def set_device_expiration(
    mobile_id: str,
    expires_at: datetime = Query(..., description="Expiration date/time"),
    action: str = Query("deactivate", description="Action on expiration: deactivate, delete, notify_only"),
    notify_days_before: int = Query(7, ge=0, le=30),
    user: Dict = Depends(get_current_user)
):
    """Quick way to set expiration for a single device."""
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id") or 1
    
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE public.device 
                SET expires_at = %s, expiration_action = %s, updated_at = NOW()
                WHERE mobile_id = %s AND tenant_id = %s
                RETURNING id, device_name;
            """, (expires_at, action, mobile_id, tenant_id))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            
            did, device_name = row
            
            # Create a schedule entry too
            cur.execute("""
                INSERT INTO public.device_schedule (
                    tenant_id, device_id, schedule_type, scheduled_at,
                    action, notify_days_before, status, next_execution_at, created_by
                ) VALUES (%s, %s, 'expiration', %s, %s, %s, 'active', %s, %s)
                ON CONFLICT DO NOTHING;
            """, (tenant_id, did, expires_at, action, notify_days_before, expires_at, user.get("user_id")))
            
        conn.commit()
        
    return {
        "mobile_id": mobile_id,
        "device_name": device_name,
        "expires_at": expires_at.isoformat(),
        "action": action,
        "notify_days_before": notify_days_before
    }


@router.delete("/device/{mobile_id}/expiration")
def remove_device_expiration(mobile_id: str, user: Dict = Depends(get_current_user)):
    """Remove expiration from a device."""
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id") or 1
    
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE public.device 
                SET expires_at = NULL, expiration_action = NULL, updated_at = NOW()
                WHERE mobile_id = %s AND tenant_id = %s
                RETURNING id;
            """, (mobile_id, tenant_id))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            
            # Cancel related schedules
            cur.execute("""
                UPDATE public.device_schedule 
                SET status = 'cancelled', updated_at = NOW()
                WHERE device_id = %s AND schedule_type = 'expiration' AND status = 'active';
            """, (row[0],))
            
        conn.commit()
        
    return {"mobile_id": mobile_id, "expiration_removed": True}


@router.delete("/schedules/{schedule_id}")
def delete_schedule(schedule_id: int, user: Dict = Depends(get_current_user)):
    """Cancel/delete a schedule."""
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id") or 1
    
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE public.device_schedule 
                SET status = 'cancelled', updated_at = NOW()
                WHERE id = %s AND tenant_id = %s
                RETURNING id;
            """, (schedule_id, tenant_id))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Schedule not found")
        conn.commit()
        
    return {"schedule_id": schedule_id, "status": "cancelled"}


# ══════════════════════════════════════════════════════════════════════════════
# 4. CONTENT APPROVAL WORKFLOW (Two-Factor Content Update)
# ══════════════════════════════════════════════════════════════════════════════

class ContentChangeRequestIn(BaseModel):
    """Request content change that needs approval"""
    request_type: str = Field(..., description="video_assign, video_remove, bulk_assign, group_update")
    target_type: str = Field(..., description="device, group, shop")
    target_id: int
    change_data: Dict = Field(..., description="The change details")
    request_note: Optional[str] = None
    expires_in_hours: int = Field(72, description="Auto-reject after X hours")


class ContentChangeRequestOut(BaseModel):
    id: int
    tenant_id: int
    request_type: str
    target_type: str
    target_id: int
    target_name: Optional[str]
    change_data: Dict
    requested_by: int
    requested_by_name: Optional[str]
    requested_at: datetime
    request_note: Optional[str]
    status: str
    reviewed_by: Optional[int]
    reviewed_by_name: Optional[str]
    reviewed_at: Optional[datetime]
    review_note: Optional[str]
    expires_at: Optional[datetime]


class ContentChangeReviewIn(BaseModel):
    """Review (approve/reject) a content change request"""
    action: str = Field(..., description="approve or reject")
    review_note: Optional[str] = None


@router.post("/content-changes/media-preview")
def get_media_preview_urls(
    body: Dict,
    user: Dict = Depends(get_current_user)
):
    """
    Given lists of video_names and ad_names, return presigned S3 URLs
    and content_type for each so the frontend can render previews.
    body: { "video_names": [...], "ad_names": [...] }
    """
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id") or 1
    video_names = body.get("video_names", [])
    ad_names = body.get("ad_names", [])

    results = {"videos": [], "ads": []}

    with pg_conn() as conn:
        with conn.cursor() as cur:
            if video_names:
                cur.execute("""
                    SELECT video_name, s3_link, content_type
                    FROM public.video
                    WHERE video_name = ANY(%s) AND tenant_id = %s;
                """, (video_names, tenant_id))
                for row in cur.fetchall():
                    vname, s3_link, content_type = row
                    try:
                        bucket, key = _parse_s3_link(s3_link)
                        presigned = presign_get_object(bucket, key) if key else None
                    except Exception:
                        presigned = None
                    results["videos"].append({
                        "name": vname,
                        "s3_link": presigned,
                        "content_type": content_type or "video",
                    })

            if ad_names:
                cur.execute("""
                    SELECT ad_name, s3_link, content_type
                    FROM public.advertisement
                    WHERE ad_name = ANY(%s) AND tenant_id = %s;
                """, (ad_names, tenant_id))
                for row in cur.fetchall():
                    aname, s3_link, content_type = row
                    try:
                        bucket, key = _parse_s3_link(s3_link)
                        presigned = presign_get_object(bucket, key) if key else None
                    except Exception:
                        presigned = None
                    results["ads"].append({
                        "name": aname,
                        "s3_link": presigned,
                        "content_type": content_type or "image",
                    })

    return results


@router.get("/company/approval-settings")
def get_approval_settings(user: Dict = Depends(get_current_user)):
    """Get content approval settings for the company."""
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id") or 1
    
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT require_content_approval, auto_approve_roles, approval_notify_email
                FROM public.company WHERE id = %s;
            """, (tenant_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Company not found")
            
            return {
                "require_content_approval": row[0] or False,
                "auto_approve_roles": row[1] or ["admin", "manager"],
                "approval_notify_email": row[2]
            }


@router.put("/company/approval-settings")
def update_approval_settings(
    require_approval: bool = Query(...),
    auto_approve_roles: List[str] = Query(["admin", "manager"]),
    notify_email: Optional[str] = None,
    user: Dict = Depends(get_current_user)
):
    """Update content approval settings for the company."""
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id") or 1
    
    # Only admin/company_admin can change this
    if user.get("user_type") != "platform" and user.get("role") not in ["admin", "platform_admin", "company_admin"]:
        raise HTTPException(status_code=403, detail="Only admin can change approval settings")
    
    with pg_conn() as conn:
        with conn.cursor() as cur:
            import json
            cur.execute("""
                UPDATE public.company 
                SET require_content_approval = %s,
                    auto_approve_roles = %s,
                    approval_notify_email = %s,
                    updated_at = NOW()
                WHERE id = %s;
            """, (require_approval, json.dumps(auto_approve_roles), notify_email, tenant_id))
        conn.commit()
        
    return {
        "require_content_approval": require_approval,
        "auto_approve_roles": auto_approve_roles,
        "approval_notify_email": notify_email
    }


@router.post("/content-changes", response_model=ContentChangeRequestOut)
def create_content_change_request(body: ContentChangeRequestIn, background_tasks: BackgroundTasks, user: Dict = Depends(get_current_user)):
    """
    Request a content change. 
    If approval is required and user doesn't have auto-approve role, 
    request goes to pending state for manager approval.
    """
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id") or 1
    user_id = user.get("user_id")
    user_role = user.get("role", "")
    
    import json

    # Collect all data needed for the response outside the connection block
    request_id = None
    requested_at = None
    target_name = None
    status = None
    expires_at = None
    requested_by_name = None
    auto_approved = False

    with pg_conn() as conn:
        with conn.cursor() as cur:
            # Check if approval is required
            cur.execute("""
                SELECT require_content_approval, auto_approve_roles
                FROM public.company WHERE id = %s;
            """, (tenant_id,))
            company_row = cur.fetchone()
            require_approval = company_row[0] if company_row else False
            auto_approve_roles = company_row[1] if company_row else ["admin", "manager"]

            # Get target name — for link_content use gname from change_data directly
            if body.request_type == "link_content":
                target_name = body.change_data.get("gname")
            elif body.target_type == "device":
                cur.execute("SELECT device_name, mobile_id FROM public.device WHERE id = %s;", (body.target_id,))
                row = cur.fetchone()
                target_name = (row[0] or row[1]) if row else None
            elif body.target_type == "group":
                cur.execute('SELECT gname FROM public."group" WHERE id = %s;', (body.target_id,))
                row = cur.fetchone()
                target_name = row[0] if row else None
            elif body.target_type == "shop":
                cur.execute("SELECT shop_name FROM public.shop WHERE id = %s;", (body.target_id,))
                row = cur.fetchone()
                target_name = row[0] if row else None

            # Platform users (including impersonation) are always auto-approved
            is_platform = user.get("user_type") == "platform"
            auto_approved = is_platform or not require_approval or user_role in (auto_approve_roles or [])
            status = "approved" if auto_approved else "pending"
            expires_at = datetime.now() + timedelta(hours=body.expires_in_hours) if not auto_approved else None

            # Duplicate guard: reject if a pending request for the same
            # request_type + target already exists from this user
            if not auto_approved:
                dup_target_name = target_name or str(body.target_id)
                cur.execute("""
                    SELECT id FROM public.content_change_request
                    WHERE tenant_id = %s
                      AND request_type = %s
                      AND target_name = %s
                      AND requested_by = %s
                      AND status = 'pending'
                    LIMIT 1;
                """, (tenant_id, body.request_type, dup_target_name, user_id))
                existing = cur.fetchone()
                if existing:
                    raise HTTPException(
                        status_code=409,
                        detail=f"A pending approval request for '{dup_target_name}' already exists (#{existing[0]}). Wait for it to be reviewed before submitting again."
                    )

            cur.execute("""
                INSERT INTO public.content_change_request (
                    tenant_id, request_type, target_type, target_id, target_name,
                    change_data, requested_by, request_note, status, expires_at,
                    reviewed_by, reviewed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, requested_at;
            """, (
                tenant_id, body.request_type, body.target_type, body.target_id, target_name,
                json.dumps(body.change_data), user_id, body.request_note, status, expires_at,
                user_id if auto_approved else None,
                datetime.now() if auto_approved else None
            ))

            request_id, requested_at = cur.fetchone()

            # If auto-approved, execute the change immediately
            if auto_approved:
                _execute_content_change(cur, request_id, body.request_type, body.target_type,
                                        body.target_id, body.change_data, tenant_id)
                cur.execute("""
                    UPDATE public.content_change_request
                    SET executed_at = NOW() WHERE id = %s;
                """, (request_id,))

            # Fetch requester name while cursor is still open
            cur.execute("SELECT full_name, username FROM public.users WHERE id = %s;", (user_id,))
            user_row = cur.fetchone()
            requested_by_name = (user_row[0] or user_row[1]) if user_row else None

        conn.commit()
    # Connection is released back to pool here, before building the response

    # If the request is pending (needs approval), push the new count to admins via WS
    if status == "pending":
        with pg_conn() as conn2:
            with conn2.cursor() as cur2:
                cur2.execute("""
                    SELECT COUNT(*) FROM public.content_change_request
                    WHERE tenant_id = %s AND status = 'pending';
                """, (tenant_id,))
                pending_count = cur2.fetchone()[0]
        background_tasks.add_task(notify_pending_approvals, tenant_id, pending_count)

    return ContentChangeRequestOut(
        id=request_id,
        tenant_id=tenant_id,
        request_type=body.request_type,
        target_type=body.target_type,
        target_id=body.target_id,
        target_name=target_name,
        change_data=body.change_data,
        requested_by=user_id,
        requested_by_name=requested_by_name,
        requested_at=requested_at,
        request_note=body.request_note,
        status=status,
        reviewed_by=user_id if auto_approved else None,
        reviewed_by_name=requested_by_name if auto_approved else None,
        reviewed_at=datetime.now() if auto_approved else None,
        review_note="Auto-approved" if auto_approved else None,
        expires_at=expires_at
    )


@router.get("/content-changes")
def list_content_change_requests(
    user: Dict = Depends(get_current_user),
    status: str = Query("pending", description="Filter by status: pending, approved, rejected, all"),
    limit: int = Query(50, le=200)
):
    """List content change requests."""
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id") or 1
    
    with pg_conn() as conn:
        with conn.cursor() as cur:
            status_filter = "" if status == "all" else "AND ccr.status = %s"
            params = [tenant_id]
            if status != "all":
                params.append(status)
            params.append(limit)
            
            cur.execute(f"""
                SELECT ccr.id, ccr.request_type, ccr.target_type, ccr.target_id, ccr.target_name,
                       ccr.change_data, ccr.requested_by, u1.full_name as requester_name,
                       ccr.requested_at, ccr.request_note, ccr.status,
                       ccr.reviewed_by, u2.full_name as reviewer_name, ccr.reviewed_at,
                       ccr.review_note, ccr.expires_at,
                       u1.role as requester_role, u1.username as requester_username
                FROM public.content_change_request ccr
                LEFT JOIN public.users u1 ON u1.id = ccr.requested_by
                LEFT JOIN public.users u2 ON u2.id = ccr.reviewed_by
                WHERE ccr.tenant_id = %s {status_filter}
                ORDER BY 
                    CASE WHEN ccr.status = 'pending' THEN 0 ELSE 1 END,
                    ccr.requested_at DESC
                LIMIT %s;
            """, params)
            
            requests = []
            for row in cur.fetchall():
                import json
                requests.append({
                    "id": row[0],
                    "request_type": row[1],
                    "target_type": row[2],
                    "target_id": row[3],
                    "target_name": row[4],
                    "change_data": json.loads(row[5]) if isinstance(row[5], str) else row[5],
                    "requested_by": row[6],
                    "requested_by_name": row[7] or row[17],  # full_name or username
                    "requested_at": row[8].isoformat() if row[8] else None,
                    "request_note": row[9],
                    "status": row[10],
                    "reviewed_by": row[11],
                    "reviewed_by_name": row[12],
                    "reviewed_at": row[13].isoformat() if row[13] else None,
                    "review_note": row[14],
                    "expires_at": row[15].isoformat() if row[15] else None,
                    "requested_by_role": row[16],
                })
            
            # Count pending
            cur.execute("""
                SELECT COUNT(*) FROM public.content_change_request 
                WHERE tenant_id = %s AND status = 'pending';
            """, (tenant_id,))
            pending_count = cur.fetchone()[0]
            
            return {
                "requests": requests,
                "count": len(requests),
                "pending_count": pending_count
            }


@router.post("/content-changes/{request_id}/review")
def review_content_change_request(
    request_id: int,
    body: ContentChangeReviewIn,
    background_tasks: BackgroundTasks,
    user: Dict = Depends(get_current_user)
):
    """Approve or reject a content change request."""
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id") or 1
    user_id = user.get("user_id")
    user_role = user.get("role", "")
    user_type = user.get("user_type", "")

    # Roles that can approve: platform users (impersonating), company admins, managers
    CAN_APPROVE_ROLES = {"admin", "manager", "platform_admin", "company_admin", "content_manager"}

    if user_type == "platform":
        # Platform users (including impersonation) can always approve
        pass
    elif user_role not in CAN_APPROVE_ROLES:
        # Check if user has can_approve_content flag
        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT can_approve_content FROM public.users WHERE id = %s;", (user_id,))
                row = cur.fetchone()
                if not row or not row[0]:
                    raise HTTPException(status_code=403, detail="You don't have permission to approve content changes")
    
    if body.action not in ["approve", "reject"]:
        raise HTTPException(status_code=400, detail="Action must be 'approve' or 'reject'")
    
    with pg_conn() as conn:
        with conn.cursor() as cur:
            # Get the request
            cur.execute("""
                SELECT request_type, target_type, target_id, change_data, status
                FROM public.content_change_request 
                WHERE id = %s AND tenant_id = %s;
            """, (request_id, tenant_id))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Request not found")
            
            request_type, target_type, target_id, change_data, current_status = row
            
            if current_status != "pending":
                raise HTTPException(status_code=400, detail=f"Request is already {current_status}")
            
            new_status = "approved" if body.action == "approve" else "rejected"
            
            cur.execute("""
                UPDATE public.content_change_request 
                SET status = %s, reviewed_by = %s, reviewed_at = NOW(), review_note = %s, updated_at = NOW()
                WHERE id = %s;
            """, (new_status, user_id, body.review_note, request_id))
            
            # If approved, execute the change
            if body.action == "approve":
                import json
                change_data_dict = json.loads(change_data) if isinstance(change_data, str) else change_data
                _execute_content_change(cur, request_id, request_type, target_type, 
                                       target_id, change_data_dict, tenant_id)
                cur.execute("""
                    UPDATE public.content_change_request 
                    SET executed_at = NOW() WHERE id = %s;
                """, (request_id,))
            
        conn.commit()

        # Count remaining pending requests for this tenant to push via WS
        with pg_conn() as conn2:
            with conn2.cursor() as cur2:
                cur2.execute("""
                    SELECT COUNT(*) FROM public.content_change_request
                    WHERE tenant_id = %s AND status = 'pending';
                """, (tenant_id,))
                pending_count = cur2.fetchone()[0]

    background_tasks.add_task(notify_pending_approvals, tenant_id, pending_count)

    return {
        "request_id": request_id,
        "status": new_status,
        "reviewed_by": user_id,
        "review_note": body.review_note
    }


def _execute_content_change(cur, request_id: int, request_type: str, target_type: str, 
                           target_id: int, change_data: Dict, tenant_id: int):
    """Execute the actual content change after approval."""
    try:
        if request_type == "video_assign":
            # Assign videos to device/group
            video_ids = change_data.get("video_ids", [])
            for vid in video_ids:
                if target_type == "device":
                    # Get or create shop for this device
                    cur.execute("""
                        SELECT s.id FROM public.device_assignment da
                        JOIN public.shop s ON s.id = da.sid
                        WHERE da.did = %s LIMIT 1;
                    """, (target_id,))
                    shop_row = cur.fetchone()
                    if shop_row:
                        sid = shop_row[0]
                        cur.execute("""
                            INSERT INTO public.device_video_shop_group (did, vid, sid, tenant_id)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT DO NOTHING;
                        """, (target_id, vid, sid, tenant_id))
                        
                elif target_type == "group":
                    cur.execute("""
                        INSERT INTO public.group_video (gid, vid, tenant_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (target_id, vid, tenant_id))
            
            # Mark devices for refresh
            if target_type == "device":
                cur.execute("""
                    UPDATE public.device SET download_status = FALSE, needs_refresh = TRUE, updated_at = NOW()
                    WHERE id = %s;
                """, (target_id,))
            elif target_type == "group":
                cur.execute("""
                    UPDATE public.device SET download_status = FALSE, needs_refresh = TRUE, updated_at = NOW()
                    WHERE id IN (SELECT did FROM public.device_assignment WHERE gid = %s);
                """, (target_id,))
        
        elif request_type == "video_remove":
            video_ids = change_data.get("video_ids", [])
            for vid in video_ids:
                if target_type == "device":
                    cur.execute("""
                        DELETE FROM public.device_video_shop_group WHERE did = %s AND vid = %s;
                    """, (target_id, vid))
                elif target_type == "group":
                    cur.execute("""
                        DELETE FROM public.group_video WHERE gid = %s AND vid = %s;
                    """, (target_id, vid))
            
            # Mark devices for refresh
            if target_type == "device":
                cur.execute("""
                    UPDATE public.device SET download_status = FALSE, needs_refresh = TRUE, updated_at = NOW()
                    WHERE id = %s;
                """, (target_id,))
            elif target_type == "group":
                cur.execute("""
                    UPDATE public.device SET download_status = FALSE, needs_refresh = TRUE, updated_at = NOW()
                    WHERE id IN (SELECT did FROM public.device_assignment WHERE gid = %s);
                """, (target_id,))

        elif request_type == "link_content":
            # Name-based linking: link video/ad names to a group by name
            # change_data: { "gname": "...", "video_names": [...], "ad_names": [...] }
            gname = change_data.get("gname", "")
            video_names = change_data.get("video_names", [])
            ad_names = change_data.get("ad_names", [])

            if gname:
                # Resolve group id
                cur.execute('SELECT id FROM public."group" WHERE gname = %s AND tenant_id = %s LIMIT 1;',
                            (gname, tenant_id))
                g_row = cur.fetchone()
                if g_row:
                    gid = g_row[0]

                    # Sync videos: replace all videos for this group with the new set
                    if video_names is not None:
                        # Resolve video ids
                        if video_names:
                            cur.execute(
                                'SELECT id, video_name FROM public.video WHERE video_name = ANY(%s) AND tenant_id = %s;',
                                (video_names, tenant_id)
                            )
                            vid_rows = cur.fetchall()
                            vid_ids = [r[0] for r in vid_rows]
                        else:
                            vid_ids = []

                        # Remove videos not in the new list
                        if vid_ids:
                            cur.execute(
                                'DELETE FROM public.group_video WHERE gid = %s AND vid NOT IN %s;',
                                (gid, tuple(vid_ids))
                            )
                        else:
                            cur.execute('DELETE FROM public.group_video WHERE gid = %s;', (gid,))

                        # Insert new ones
                        for vid in vid_ids:
                            cur.execute("""
                                INSERT INTO public.group_video (gid, vid, tenant_id)
                                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;
                            """, (gid, vid, tenant_id))

                    # Sync advertisements
                    if ad_names is not None:
                        if ad_names:
                            cur.execute(
                                'SELECT id FROM public.advertisement WHERE ad_name = ANY(%s) AND tenant_id = %s;',
                                (ad_names, tenant_id)
                            )
                            aid_rows = cur.fetchall()
                            aid_ids = [r[0] for r in aid_rows]
                        else:
                            aid_ids = []

                        if aid_ids:
                            cur.execute(
                                'DELETE FROM public.group_advertisement WHERE gid = %s AND aid NOT IN %s;',
                                (gid, tuple(aid_ids))
                            )
                        else:
                            cur.execute('DELETE FROM public.group_advertisement WHERE gid = %s;', (gid,))

                        for aid in aid_ids:
                            cur.execute("""
                                INSERT INTO public.group_advertisement (gid, aid, tenant_id)
                                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;
                            """, (gid, aid, tenant_id))

                    # Mark all devices in this group for refresh
                    cur.execute("""
                        UPDATE public.device SET download_status = FALSE, needs_refresh = TRUE, updated_at = NOW()
                        WHERE id IN (SELECT did FROM public.device_assignment WHERE gid = %s);
                    """, (gid,))

        # Log success
        print(f"[APPROVAL] Executed content change request #{request_id}: {request_type} on {target_type}:{target_id}")
        
    except Exception as e:
        # Log error but don't fail the transaction
        cur.execute("""
            UPDATE public.content_change_request 
            SET execution_error = %s WHERE id = %s;
        """, (str(e), request_id))
        print(f"[APPROVAL] Error executing content change #{request_id}: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# BACKGROUND TASKS
# ══════════════════════════════════════════════════════════════════════════════

async def process_scheduled_tasks():
    """
    Background task to process scheduled device actions.
    Should be called periodically (e.g., every minute).
    """
    with pg_conn() as conn:
        with conn.cursor() as cur:
            # Find schedules that need to be executed
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
                                    deactivation_reason = 'Scheduled expiration', updated_at = NOW()
                                WHERE id = %s AND is_active = TRUE;
                            """, (device_id,))
                            devices_affected = cur.rowcount
                        elif group_id:
                            cur.execute("""
                                UPDATE public.device 
                                SET is_active = FALSE, deactivated_at = NOW(),
                                    deactivation_reason = 'Scheduled group expiration', updated_at = NOW()
                                WHERE id IN (SELECT did FROM public.device_assignment WHERE gid = %s)
                                  AND is_active = TRUE;
                            """, (group_id,))
                            devices_affected = cur.rowcount
                        else:
                            # Company-wide
                            cur.execute("""
                                UPDATE public.device 
                                SET is_active = FALSE, deactivated_at = NOW(),
                                    deactivation_reason = 'Scheduled company expiration', updated_at = NOW()
                                WHERE tenant_id = %s AND is_active = TRUE;
                            """, (tenant_id,))
                            devices_affected = cur.rowcount
                    
                    elif action == "activate":
                        if device_id:
                            cur.execute("""
                                UPDATE public.device 
                                SET is_active = TRUE, deactivated_at = NULL, 
                                    deactivation_reason = NULL, updated_at = NOW()
                                WHERE id = %s;
                            """, (device_id,))
                            devices_affected = cur.rowcount
                    
                    # Log execution
                    cur.execute("""
                        INSERT INTO public.schedule_execution_log (schedule_id, status, devices_affected)
                        VALUES (%s, 'success', %s);
                    """, (schedule_id, devices_affected))
                    
                    # Update schedule
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
            
        conn.commit()


async def expire_pending_requests():
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
                print(f"[APPROVAL] Expired {len(expired)} pending content change requests")
        conn.commit()
