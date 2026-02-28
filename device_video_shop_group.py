# ===========================================
# device_video_shop_group.py - MODIFICATION GUIDE
# ===========================================
# Location: cms-backend-staging/device_video_shop_group.py
#
# This file shows ONLY the sections you need to change.
# Your existing file is 6340 lines - don't replace the whole thing!
# ===========================================


# ===========================================
# CHANGE 1: Update imports (at top of file, around lines 1-40)
# ===========================================
# REPLACE the imports section with this:

# device_video_shop_group.py
# Run: uvicorn device_video_shop_group:app --host 0.0.0.0 --port 8005 --reload
# Enhanced with: rotation, fit_mode, content_type, logs, counter reset, online threshold
# UPDATED: Connection pooling + WebSocket real-time updates

import os
import io
import csv
import json
import hashlib
import secrets
import asyncio  # <-- ADD THIS
from contextlib import contextmanager
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Tuple

from migrations.dvsg_schema import ensure_dvsg_schema
from migrations.multitenant_schema import ensure_multitenant_schema
from pydantic import BaseModel, Field

from fastapi import FastAPI, HTTPException, Query, Path, Depends, Header, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, StreamingResponse, JSONResponse
from starlette.requests import Request
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
import boto3
import traceback as _tb

# NEW: Import from new database module (replaces old pool code)
from database import (
    pg_conn, pg_init_pool, pg_close_pool,
    AsyncDatabasePool, startup_db_pools, shutdown_db_pools,
    check_database_health
)

# NEW: Import WebSocket components
from websocket_manager import ws_manager, DeviceStatus
from websocket_routes import (
    router as ws_router,
    notify_device_online, notify_device_offline,
    notify_device_status, notify_device_temperature,
    notify_download_progress
)

# Multi-tenant auth
from tenant_context import (
    TenantContext, get_tenant_context, get_current_user,
    require_permission, require_platform_user, require_tenant_context,
    hash_password, verify_password, generate_token, create_session,
    invalidate_user_sessions, invalidate_tenant_sessions,
    active_sessions, log_audit, ROLE_PERMISSIONS,
)
from platform_api import router as platform_router

load_dotenv()


# ===========================================
# CHANGE 2: DELETE old pool code (around lines 406-453)
# ===========================================
# DELETE these lines completely (they are now in database.py):
#
# pg_pool: Optional[ThreadedConnectionPool] = None
# 
# def pg_init_pool():
#     ...
# 
# def pg_close_pool():
#     ...
# 
# @contextmanager
# def pg_conn():
#     ...


# ===========================================
# CHANGE 3: Add lifecycle events (right after app = FastAPI(...), around line 64)
# ===========================================
# AFTER this line:
#     app = FastAPI(title="Device-Video-Shop-Group Service", version="3.0.0")
#
# ADD these:

@app.on_event("startup")
async def startup_event():
    """Initialize database pools and start WebSocket manager."""
    await startup_db_pools()
    await ws_manager.start()
    print("[APP] Startup complete - pools and WebSocket ready")


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up database pools and stop WebSocket manager."""
    await ws_manager.stop()
    await shutdown_db_pools()
    print("[APP] Shutdown complete")


# Include WebSocket router
app.include_router(ws_router, tags=["WebSocket"])


# ===========================================
# CHANGE 4: Add database health endpoint (add anywhere, suggested after debug/health)
# ===========================================

@app.get("/db/health")
async def database_health():
    """Check database connectivity and pool status."""
    return await check_database_health()


# ===========================================
# CHANGE 5: Modify online_update endpoint (around line 3008)
# ===========================================
# REPLACE the entire function with this:

@app.post("/device/{mobile_id}/online_update")
async def set_device_online_status(mobile_id: str, body: DeviceOnlineUpdateIn):
    """Update device online status and broadcast via WebSocket."""
    
    tenant_id = None
    device_name = None
    previous_status = None
    
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                # Get current status first
                cur.execute("""
                    SELECT id, is_online, is_active, 
                           COALESCE(needs_refresh, FALSE) as needs_refresh,
                           download_status, tenant_id, device_name
                    FROM public.device WHERE mobile_id = %s LIMIT 1;
                """, (mobile_id,))
                device_row = cur.fetchone()
                if not device_row:
                    raise HTTPException(status_code=404, detail="Device not found")
                
                did = device_row[0]
                previous_status = device_row[1]
                is_active = device_row[2] if device_row[2] is not None else True
                needs_refresh = device_row[3] if device_row[3] is not None else False
                download_status = device_row[4] if device_row[4] is not None else False
                tenant_id = device_row[5]
                device_name = device_row[6]
                
                # If device is deactivated
                if not is_active:
                    cur.execute("""
                        UPDATE public.device 
                        SET needs_refresh = FALSE, updated_at = NOW()
                        WHERE mobile_id = %s;
                    """, (mobile_id,))
                    conn.commit()
                    return {
                        "mobile_id": mobile_id,
                        "is_online": False,
                        "is_active": False,
                        "force_refresh": True,
                        "download_status": download_status
                    }
                
                # Update the device status
                cur.execute("""
                    UPDATE public.device 
                    SET is_online = %s, last_online_at = NOW(), updated_at = NOW(),
                        needs_refresh = FALSE
                    WHERE mobile_id = %s
                    RETURNING is_online;
                """, (body.is_online, mobile_id))
                row = cur.fetchone()
                
                # Log status change if it actually changed
                if previous_status != body.is_online:
                    event_type = "online" if body.is_online else "offline"
                    cur.execute("""
                        INSERT INTO public.device_online_history (did, event_type, event_at)
                        VALUES (%s, %s, NOW());
                    """, (did, event_type))
                
            conn.commit()
            
            # ============================================
            # NEW: Broadcast via WebSocket
            # ============================================
            if tenant_id and previous_status != body.is_online:
                try:
                    if body.is_online:
                        asyncio.create_task(
                            notify_device_online(tenant_id, mobile_id, device_name)
                        )
                    else:
                        asyncio.create_task(
                            notify_device_offline(tenant_id, mobile_id, device_name)
                        )
                except Exception as e:
                    print(f"[WS] Broadcast error: {e}")
            
            return {
                "mobile_id": mobile_id, 
                "is_online": bool(row[0]),
                "is_active": True,
                "force_refresh": needs_refresh,
                "download_status": download_status
            }
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


# ===========================================
# CHANGE 6: Modify temperature_update endpoint (around line 1634)
# ===========================================
# REPLACE the entire function with this:

@app.post("/device/{mobile_id}/temperature_update")
async def set_temperature(mobile_id: str, body: DeviceTemperatureUpdateIn):
    """Update device temperature and broadcast via WebSocket."""
    
    tenant_id = None
    
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id, tenant_id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Device not found")
                did = row[0]
                tenant_id = row[1]
                
                cur.execute("""
                    UPDATE public.device SET temperature = %s, updated_at = NOW()
                    WHERE id = %s RETURNING temperature;
                """, (body.temperature, did))
                temp = cur.fetchone()[0]
                
                # Log temperature
                _log_event(conn, did, 'temperature', body.temperature)
                
                # Store time-series point
                _insert_temperature_point(conn, did, float(body.temperature))
            conn.commit()
            
            # ============================================
            # NEW: Broadcast via WebSocket
            # ============================================
            if tenant_id:
                try:
                    asyncio.create_task(
                        notify_device_temperature(tenant_id, mobile_id, float(body.temperature))
                    )
                except Exception as e:
                    print(f"[WS] Broadcast error: {e}")
            
            return {"mobile_id": mobile_id, "temperature": float(temp)}
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


# ===========================================
# CHANGE 7: Modify mark_offline_devices endpoint (around line 3982)
# ===========================================
# REPLACE the entire function with this:

@app.post("/admin/mark_offline_devices")
async def mark_offline_devices():
    """Mark devices as offline if stale. Broadcasts via WebSocket."""
    
    marked_devices = []
    
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE public.device
                    SET is_online = FALSE, updated_at = NOW()
                    WHERE is_online = TRUE
                      AND (last_online_at IS NULL 
                           OR last_online_at < NOW() - INTERVAL '{ONLINE_THRESHOLD_SECONDS} seconds')
                    RETURNING id, mobile_id, device_name, tenant_id;
                """)
                marked_devices = cur.fetchall()
                
                # Log offline events
                for row in marked_devices:
                    cur.execute("""
                        INSERT INTO public.device_online_history (did, event_type, event_at)
                        VALUES (%s, 'offline', NOW());
                    """, (row[0],))
                
            conn.commit()
            
            # ============================================
            # NEW: Broadcast via WebSocket
            # ============================================
            for did, mobile_id, device_name, tenant_id in marked_devices:
                if tenant_id:
                    try:
                        asyncio.create_task(
                            notify_device_offline(tenant_id, mobile_id, device_name)
                        )
                    except Exception as e:
                        print(f"[WS] Broadcast error: {e}")
            
            return {"marked_offline": len(marked_devices), "devices": [d[1] for d in marked_devices]}
        except:
            conn.rollback()
            raise


# ===========================================
# END OF CHANGES
# ===========================================
