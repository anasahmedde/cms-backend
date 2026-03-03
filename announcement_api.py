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


class AnnouncementOut(BaseModel):
    id: int
    message: str
    type: str
    is_active: bool
    created_by: Optional[int]
    created_by_username: Optional[str]
    created_at: datetime
    updated_at: datetime


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
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    
    -- Index for quick active announcement lookup
    CREATE INDEX IF NOT EXISTS idx_announcement_active ON public.platform_announcement (is_active) WHERE is_active = TRUE;
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC ENDPOINT - Get active announcement (for all logged-in users)
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/announcement/active")
def get_active_announcement(user: dict = Depends(get_current_user)):
    """
    Get the currently active platform announcement.
    Available to all logged-in users (company users and platform users).
    Returns null if no active announcement exists.
    """
    with pg_conn() as conn:
        # Ensure table exists
        ensure_announcement_schema(conn)
        
        with conn.cursor() as cur:
            cur.execute("""
                SELECT a.id, a.message, a.type, a.is_active, a.created_by, 
                       u.username as created_by_username, a.created_at, a.updated_at
                FROM public.platform_announcement a
                LEFT JOIN public.users u ON u.id = a.created_by
                WHERE a.is_active = TRUE
                ORDER BY a.updated_at DESC
                LIMIT 1;
            """)
            row = cur.fetchone()
            
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
    
    This deactivates any existing active announcements and creates a new one.
    Broadcasts via WebSocket to all connected users for immediate display.
    """
    if body.type not in ("info", "warning", "critical"):
        raise HTTPException(status_code=400, detail="Type must be: info, warning, or critical")
    
    with pg_conn() as conn:
        # Ensure table exists
        ensure_announcement_schema(conn)
        
        with conn.cursor() as cur:
            # Deactivate all existing announcements
            cur.execute("""
                UPDATE public.platform_announcement 
                SET is_active = FALSE, updated_at = NOW()
                WHERE is_active = TRUE;
            """)
            
            # Create new announcement
            cur.execute("""
                INSERT INTO public.platform_announcement (message, type, is_active, created_by)
                VALUES (%s, %s, %s, %s)
                RETURNING id, created_at, updated_at;
            """, (body.message, body.type, body.is_active, user.user_id))
            
            row = cur.fetchone()
            announcement_id, created_at, updated_at = row
        
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
    }
    
    # Broadcast to all connected users via WebSocket
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
                       u.username as created_by_username, a.created_at, a.updated_at
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
            }
            for r in rows
        ],
        "count": len(rows)
    }
