# device_video_shop_group.py
# Run: uvicorn device_video_shop_group:app --host 0.0.0.0 --port 8005 --reload
# Enhanced with: rotation, fit_mode, content_type, logs, counter reset, online threshold
# UPDATED: Connection pooling + WebSocket real-time updates + Background offline checker

import os
import io
import csv
import json
import hashlib
import secrets
import asyncio
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

# Import from database module (connection pooling)
from database import (
    pg_conn, pg_init_pool, pg_close_pool,
    AsyncDatabasePool, startup_db_pools, shutdown_db_pools,
    check_database_health
)

# Import WebSocket components
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

# ---------- Settings ----------
DB_HOST = os.getenv("PGHOST", "172.31.17.177")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_NAME = os.getenv("PGDATABASE", "dgx")
DB_USER = os.getenv("PGUSER", "app_user")
DB_PASS = os.getenv("PGPASSWORD", "strongpassword")
DB_MIN_CONN = int(os.getenv("PG_MIN_CONN", "1"))
DB_MAX_CONN = int(os.getenv("PG_MAX_CONN", "50"))

AWS_REGION = os.getenv("AWS_REGION")
PRESIGN_EXPIRES = int(os.getenv("PRESIGN_EXPIRES", "900"))  # 15 min

# CHANGED: Reduced from 120 to 45 seconds for faster offline detection
ONLINE_THRESHOLD_SECONDS = 45

# Background task check interval (30 seconds)
OFFLINE_CHECK_INTERVAL_SECONDS = 30

# NOTE: active_sessions is now imported from tenant_context

# Download progress tracking (in-memory)
download_progress_store: Dict[str, Dict] = {}

# Treat any of these as "no group"
NO_GROUP_SENTINELS = {"_none", "none", "null", "(none)", ""}

# Flag to control background task
_offline_checker_running = False

# ===========================================================
# CREATE APP
# ===========================================================
app = FastAPI(title="Device-Video-Shop-Group Service", version="3.0.0")

# --- CORS ---
_CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*")
if _CORS_ORIGINS.strip() == "*":
    _origins = ["*"]
else:
    _origins = [o.strip() for o in _CORS_ORIGINS.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ===========================================================
# BACKGROUND TASK: Check for offline devices every 30 seconds
# Memory: ~0 KB (just a coroutine in existing event loop)
# ===========================================================
async def offline_checker_task():
    """
    Background task to mark stale devices as offline.
    Runs every 30 seconds, uses minimal resources.
    """
    global _offline_checker_running
    _offline_checker_running = True
    
    while _offline_checker_running:
        try:
            await asyncio.sleep(OFFLINE_CHECK_INTERVAL_SECONDS)
            
            if not _offline_checker_running:
                break
            
            # Use sync connection (lightweight, from pool)
            marked_devices = []
            with pg_conn() as conn:
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
                    for did, mobile_id, device_name, tenant_id in marked_devices:
                        cur.execute("""
                            INSERT INTO public.device_online_history (did, event_type, event_at)
                            VALUES (%s, 'offline', NOW());
                        """, (did,))
                
                conn.commit()
            
            # Broadcast via WebSocket (non-blocking)
            for did, mobile_id, device_name, tenant_id in marked_devices:
                if tenant_id:
                    try:
                        asyncio.create_task(
                            notify_device_offline(tenant_id, mobile_id, device_name)
                        )
                    except Exception:
                        pass  # Don't let broadcast errors stop the checker
            
            if marked_devices:
                print(f"[OFFLINE] Marked {len(marked_devices)} devices offline: {[d[1] for d in marked_devices]}")
                
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[OFFLINE] Checker error: {e}")
            # Continue running despite errors


# ===========================================================
# LIFECYCLE EVENTS
# ===========================================================
@app.on_event("startup")
async def startup_event():
    """Initialize database pools, WebSocket manager, and background tasks."""
    await startup_db_pools()
    await ws_manager.start()
    
    # Start background offline checker (lightweight coroutine)
    asyncio.create_task(offline_checker_task())
    
    print("[APP] Startup complete - pools, WebSocket, and offline checker ready")
    print(f"[APP] Offline threshold: {ONLINE_THRESHOLD_SECONDS}s, check interval: {OFFLINE_CHECK_INTERVAL_SECONDS}s")


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up all resources."""
    global _offline_checker_running
    _offline_checker_running = False  # Stop background task
    
    await ws_manager.stop()
    await shutdown_db_pools()
    print("[APP] Shutdown complete")


# Include routers
app.include_router(ws_router, tags=["WebSocket"])
app.include_router(platform_router, prefix="/platform", tags=["Platform"])


# ===========================================================
# Exception handler
# ===========================================================
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    error_detail = str(exc)
    tb = _tb.format_exc()
    print(f"[UNHANDLED ERROR] {request.method} {request.url.path}: {error_detail}\n{tb}", flush=True)
    return JSONResponse(
        status_code=500,
        content={"detail": error_detail, "traceback": tb.split("\n")[-4:]},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        },
    )


# ===========================================================
# Health endpoints
# ===========================================================
@app.get("/db/health")
async def database_health():
    """Check database connectivity and pool status."""
    health = await check_database_health()
    health["offline_checker"] = {
        "running": _offline_checker_running,
        "threshold_seconds": ONLINE_THRESHOLD_SECONDS,
        "check_interval_seconds": OFFLINE_CHECK_INTERVAL_SECONDS,
    }
    return health


@app.get("/debug/health")
def debug_health():
    """Quick health check with migration status."""
    try:
        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT column_default FROM information_schema.columns 
                    WHERE table_schema='public' AND table_name='device' AND column_name='tenant_id';
                """)
                device_default = cur.fetchone()
                
                cur.execute("SELECT COUNT(*) FROM public.company;")
                company_count = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM public.role;")
                role_count = cur.fetchone()[0]
                
                cur.execute("SELECT id, slug, name, status FROM public.company ORDER BY id LIMIT 5;")
                companies = [{"id": r[0], "slug": r[1], "name": r[2], "status": r[3]} for r in cur.fetchall()]
                
                # Count online devices
                cur.execute("SELECT COUNT(*) FROM public.device WHERE is_online = TRUE;")
                online_count = cur.fetchone()[0]
                
                return {
                    "status": "ok",
                    "device_tenant_id_default": str(device_default) if device_default else "NO DEFAULT",
                    "companies": company_count,
                    "roles": role_count,
                    "company_list": companies,
                    "active_sessions": len(active_sessions),
                    "online_devices": online_count,
                    "offline_threshold_seconds": ONLINE_THRESHOLD_SECONDS,
                }
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# ---------- Pydantic models ----------
class DeviceCountsOut(BaseModel):
    mobile_id: str
    daily_count: Optional[int] = None
    monthly_count: Optional[int] = None


class DeviceTemperatureUpdateIn(BaseModel):
    temperature: float


class DeviceDailyCountUpdateIn(BaseModel):
    daily_count: int


# ===========================================================
# IMPORTANT: DELETE the old pg_pool code from your file!
# (around lines 406-453 in the original)
# The functions pg_conn, pg_init_pool, pg_close_pool are now
# imported from database.py
# ===========================================================

# ... REST OF YOUR FILE CONTINUES FROM HERE ...
# (More Pydantic models, all endpoints, etc.)
