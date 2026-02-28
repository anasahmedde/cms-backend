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
import asyncio  # NEW
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

# NEW: Import from database module (connection pooling)
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

# Multi-tenant auth (replaces old inline auth)
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

# Online threshold in seconds (2 minutes - allows for network delays)
ONLINE_THRESHOLD_SECONDS = 120

# NOTE: active_sessions is now imported from tenant_context

# Download progress tracking (in-memory)
download_progress_store: Dict[str, Dict] = {}

# Treat any of these as "no group"
NO_GROUP_SENTINELS = {"_none", "none", "null", "(none)", ""}

# ===========================================================
# CREATE APP FIRST
# ===========================================================
app = FastAPI(title="Device-Video-Shop-Group Service", version="3.0.0")

# --- CORS (frontend runs on different origin/port) ---
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
# NEW: LIFECYCLE EVENTS (must be AFTER app = FastAPI())
# ===========================================================
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


# Include routers
app.include_router(ws_router, tags=["WebSocket"])
app.include_router(platform_router, prefix="/platform", tags=["Platform"])


# ===========================================================
# Global exception handler
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
# NEW: Database health endpoint
# ===========================================================
@app.get("/db/health")
async def database_health():
    """Check database connectivity and pool status."""
    return await check_database_health()


# Debug endpoint to check server state
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
                
                return {
                    "status": "ok",
                    "device_tenant_id_default": str(device_default) if device_default else "NO DEFAULT",
                    "companies": company_count,
                    "roles": role_count,
                    "company_list": companies,
                    "active_sessions": len(active_sessions),
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
# IMPORTANT: DELETE the old pg_pool code (around lines 406-453)
# The functions pg_conn, pg_init_pool, pg_close_pool are now
# imported from database.py - DO NOT define them again!
# ===========================================================

# ... REST OF YOUR FILE CONTINUES FROM HERE ...
# (Pydantic models, endpoints, etc. - no changes needed)
