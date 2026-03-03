# announcement_api.py
# Location: cms-backend-staging/announcement_api.py
"""
Platform Announcement API
=========================
Endpoints for super admin to create announcements visible to all company users.
Announcements are stored in the database and fetched by all logged-in users.
Now with WebSocket broadcast for real-time updates!

Include this router in device_video_shop_group.py:
    from announcement_api import router as announcement_router
    app.include_router(announcement_router, tags=["Platform Announcements"])
"""

import asyncio
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel, Field

from database import pg_conn
from tenant_context import get_current_user, require_platform_user, TenantContext
from websocket_routes import notify_announcement, notify_announcement_cleared

router = APIRouter()


# ══════════════════════════════════════════════════════════════════════════════
# MODELS
# ══════════════════════════════════════════════════════════════════════════════

class AnnouncementIn(BaseModel):
    message: str = Field(..., min_length=1, max_length=500, description="Announcement message")
    type: str = Field("info", description="Banner type: info, warning, critical")
    is_active: bool = Field(True, description="Whether the announcement is currently active")
    expires_at: Optional[datetime] = Field(None, description="When the announcement auto-expires (None = never)")
    target_type: str = Field("all", description="Who sees this: 'all' or 'company'")
    target_company_id: Optional[int] = Field(None, description="Company ID when target_type='company'")


class AnnouncementOut(BaseModel):
    id: int
    message: str
    type: str
    is_active: bool
    created_by: Optional[int]
    created_by_username: Optional[str]
    created_at: datetime
    updated_at: datetime
    expires_at: Optional[datetime]
    target_type: str
    target_company_id: Optional[int]


# ══════════════════════════════════════════════════════════════════════════════
# SCHEMA - Run once to create the table
# ══════════════════════════════════════════════════════════════════════════════

def ensure_announcement_schema(conn):
    """Create the platform_announcement table if it doesn't exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS public.platform_announcement (
        id SERIAL PRIMARY KEY,
        message TEXT NOT NULL,
        type VARCHAR(20) NOT NULL DEFAULT 'info',
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        created_by INTEGER,
        expires_at TIMESTAMPTZ,
        target_type VARCHAR(20) NOT NULL DEFAULT 'all',
        target_company_id INTEGER,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    
    -- Index for quick active announcement lookup
    CREATE INDEX IF NOT EXISTS idx_announcement_active ON public.platform_announcement (is_active) WHERE is_active = TRUE;
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
        # Idempotent migrations for existing tables
        cur.execute("""
            ALTER TABLE public.platform_announcement
            ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;
        """)
        cur.execute("""
            ALTER TABLE public.platform_announcement
            ADD COLUMN IF NOT EXISTS target_type VARCHAR(20) NOT NULL DEFAULT 'all';
        """)
        cur.execute("""
            ALTER TABLE public.platform_announcement
            ADD COLUMN IF NOT EXISTS target_company_id INTEGER;
        """)
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC ENDPOINT - Get active announcement (for all logged-in users)
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/announcement/active")
def get_active_announcement(user: dict = Depends(get_current_user)):
    """
    Get the currently active platform announcement for this user.
    - Platform users see all announcements.
    - Company users see global ('all') announcements AND announcements targeted to their company.
    Auto-deactivates expired announcements.
    """
    # Determine the caller's company id (company users have tenant_id)
    caller_company_id = user.get("tenant_id") or user.get("company_id")
    is_platform = user.get("user_type") == "platform" or user.get("is_platform_user")

    with pg_conn() as conn:
        ensure_announcement_schema(conn)
        
        with conn.cursor() as cur:
            # Auto-deactivate expired announcements
            cur.execute("""
                UPDATE public.platform_announcement
                SET is_active = FALSE, updated_at = NOW()
                WHERE is_active = TRUE AND expires_at IS NOT NULL AND expires_at <= NOW();
            """)
            
            if is_platform:
                # Platform admins see the most recent active announcement (any target)
                cur.execute("""
                    SELECT a.id, a.message, a.type, a.is_active, a.created_by,
                           u.username, a.created_at, a.updated_at, a.expires_at,
                           a.target_type, a.target_company_id
                    FROM public.platform_announcement a
                    LEFT JOIN public.users u ON u.id = a.created_by
                    WHERE a.is_active = TRUE
                    ORDER BY a.updated_at DESC
                    LIMIT 1;
                """)
            else:
                # Company users: show global OR announcements targeted to their company
                cur.execute("""
                    SELECT a.id, a.message, a.type, a.is_active, a.created_by,
                           u.username, a.created_at, a.updated_at, a.expires_at,
                           a.target_type, a.target_company_id
                    FROM public.platform_announcement a
                    LEFT JOIN public.users u ON u.id = a.created_by
                    WHERE a.is_active = TRUE
                      AND (
                        a.target_type = 'all'
                        OR (a.target_type = 'company' AND a.target_company_id = %s)
                      )
                    ORDER BY
                      -- Prioritise company-specific announcements over global ones
                      CASE WHEN a.target_type = 'company' THEN 0 ELSE 1 END,
                      a.updated_at DESC
                    LIMIT 1;
                """, (caller_company_id,))
            
            row = cur.fetchone()
        conn.commit()
        
        if not row:
            return {"announcement": None}
        
        return {
            "announcement": {
                "id": row[0],
                "message": row[1],
                "type": row[2],
                "is_active": row[3],
                "created_by": row[4],
                "created_by_username": row[5],
                "created_at": row[6].isoformat() if row[6] else None,
                "updated_at": row[7].isoformat() if row[7] else None,
                "expires_at": row[8].isoformat() if row[8] else None,
                "target_type": row[9],
                "target_company_id": row[10],
            }
        }


# ══════════════════════════════════════════════════════════════════════════════
# PLATFORM ADMIN ENDPOINTS - Create/Update/Delete announcements
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/platform/announcement", response_model=dict)
async def create_or_update_announcement(
    body: AnnouncementIn,
    background_tasks: BackgroundTasks,
    user: TenantContext = Depends(require_platform_user)
):
    """
    Create or update the platform announcement.
    Platform admin only.
    
    - target_type='all'     → visible to every company user (global)
    - target_type='company' → visible only to users of target_company_id
    
    Broadcasts via WebSocket for immediate display.
    """
    if body.type not in ("info", "warning", "critical"):
        raise HTTPException(status_code=400, detail="Type must be: info, warning, or critical")
    if body.target_type not in ("all", "company"):
        raise HTTPException(status_code=400, detail="target_type must be 'all' or 'company'")
    if body.target_type == "company" and not body.target_company_id:
        raise HTTPException(status_code=400, detail="target_company_id is required when target_type='company'")
    
    with pg_conn() as conn:
        ensure_announcement_schema(conn)
        
        with conn.cursor() as cur:
            # Deactivate existing active announcements with the same scope:
            # - A new global announcement clears all active announcements
            # - A new company-specific one clears only that company's active announcements
            if body.target_type == "all":
                cur.execute("""
                    UPDATE public.platform_announcement 
                    SET is_active = FALSE, updated_at = NOW()
                    WHERE is_active = TRUE;
                """)
            else:
                cur.execute("""
                    UPDATE public.platform_announcement
                    SET is_active = FALSE, updated_at = NOW()
                    WHERE is_active = TRUE
                      AND (target_type = 'all' OR (target_type = 'company' AND target_company_id = %s));
                """, (body.target_company_id,))
            
            cur.execute("""
                INSERT INTO public.platform_announcement
                    (message, type, is_active, created_by, expires_at, target_type, target_company_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id, created_at, updated_at, expires_at;
            """, (
                body.message, body.type, body.is_active, user.user_id,
                body.expires_at, body.target_type, body.target_company_id
            ))
            
            row = cur.fetchone()
            announcement_id, created_at, updated_at, expires_at = row
        
        conn.commit()
    
    announcement_data = {
        "id": announcement_id,
        "message": body.message,
        "type": body.type,
        "is_active": body.is_active,
        "created_by": user.user_id,
        "created_by_username": user.username,
        "created_at": created_at.isoformat(),
        "updated_at": updated_at.isoformat(),
        "expires_at": expires_at.isoformat() if expires_at else None,
        "target_type": body.target_type,
        "target_company_id": body.target_company_id,
    }
    
    if body.is_active:
        background_tasks.add_task(notify_announcement, announcement_data)
    
    return {
        **announcement_data,
        "status": "published"
    }


@router.delete("/platform/announcement")
async def clear_announcement(
    background_tasks: BackgroundTasks,
    user: TenantContext = Depends(require_platform_user)
):
    """
    Clear/deactivate the current announcement.
    Platform admin only.
    Broadcasts via WebSocket to all connected users for immediate removal.
    """
    with pg_conn() as conn:
        # Ensure table exists
        ensure_announcement_schema(conn)
        
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE public.platform_announcement 
                SET is_active = FALSE, updated_at = NOW()
                WHERE is_active = TRUE
                RETURNING id;
            """)
            deactivated = cur.fetchall()
        
        conn.commit()
    
    # Broadcast to all connected users via WebSocket
    if deactivated:
        background_tasks.add_task(notify_announcement_cleared)
    
    return {
        "cleared": len(deactivated),
        "message": "Announcement cleared" if deactivated else "No active announcement to clear"
    }


@router.get("/platform/announcements")
def list_announcements(
    limit: int = 20,
    user: TenantContext = Depends(require_platform_user)
):
    """
    List all announcements (history).
    Platform admin only.
    """
    with pg_conn() as conn:
        # Ensure table exists
        ensure_announcement_schema(conn)
        
        with conn.cursor() as cur:
            cur.execute("""
                SELECT a.id, a.message, a.type, a.is_active, a.created_by,
                       u.username as created_by_username, a.created_at, a.updated_at, a.expires_at,
                       a.target_type, a.target_company_id
                FROM public.platform_announcement a
                LEFT JOIN public.users u ON u.id = a.created_by
                ORDER BY a.created_at DESC
                LIMIT %s;
            """, (limit,))
            
            rows = cur.fetchall()
    
    return {
        "announcements": [
            {
                "id": r[0],
                "message": r[1],
                "type": r[2],
                "is_active": r[3],
                "created_by": r[4],
                "created_by_username": r[5],
                "created_at": r[6].isoformat() if r[6] else None,
                "updated_at": r[7].isoformat() if r[7] else None,
                "expires_at": r[8].isoformat() if r[8] else None,
                "target_type": r[9],
                "target_company_id": r[10],
            }
            for r in rows
        ],
        "count": len(rows)
    }
