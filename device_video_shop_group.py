# device_video_shop_group.py
# Run: uvicorn device_video_shop_group:app --host 0.0.0.0 --port 8005 --reload
# Enhanced with: rotation, fit_mode, content_type, logs, counter reset, online threshold

import os
import io
import csv
import json
import hashlib
import secrets
from contextlib import contextmanager
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Tuple

from migrations.dvsg_schema import ensure_dvsg_schema
from migrations.multitenant_schema import ensure_multitenant_schema
from pydantic import BaseModel, Field

from fastapi import FastAPI, HTTPException, Query, Path, Depends, Header, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, StreamingResponse
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool, PoolError
import boto3

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

# Download progress tracking (in-memory) - stores real-time download progress per device
# Format: { "mobile_id": { "current_file": 1, "total_files": 3, "file_name": "video.mp4", "progress": 65, "downloaded_bytes": 5242880, "total_bytes": 8388608, "updated_at": datetime } }
download_progress_store: Dict[str, Dict] = {}

# Treat any of these as "no group"
NO_GROUP_SENTINELS = {"_none", "none", "null", "(none)", ""}

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

# Global exception handler to preserve CORS on 500s and log actual errors
from fastapi.responses import JSONResponse
from starlette.requests import Request
import traceback as _tb

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

# Debug endpoint to check server state
@app.get("/debug/health")
def debug_health():
    """Quick health check with migration status."""
    try:
        with pg_conn() as conn:
            with conn.cursor() as cur:
                # Check if tenant_id column exists and has DEFAULT
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


# ---------- User Management Models ----------
class UserLoginIn(BaseModel):
    username: str
    password: str

class UserLoginOut(BaseModel):
    token: str
    user_id: int
    username: str
    full_name: Optional[str]
    role: str
    permissions: List[str]

class UserCreateIn(BaseModel):
    username: str
    password: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    role: str = "viewer"
    permissions: List[str] = []
    company_slug: Optional[str] = None  # Platform admins can assign user to a specific company

class UserUpdateIn(BaseModel):
    email: Optional[str] = None
    full_name: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None
    permissions: Optional[List[str]] = None

class UserChangePasswordIn(BaseModel):
    current_password: str
    new_password: str

class UserOut(BaseModel):
    id: int
    username: str
    email: Optional[str]
    full_name: Optional[str]
    role: str
    is_active: bool
    created_at: datetime
    last_login: Optional[datetime]
    permissions: List[str] = []
    user_type: Optional[str] = None
    tenant_id: Optional[int] = None
    company_slug: Optional[str] = None
    company_name: Optional[str] = None

class UserListOut(BaseModel):
    items: List[UserOut]
    total: int

ROLE_PERMISSIONS_LEGACY = ROLE_PERMISSIONS  # backward compat alias

# NOTE: hash_password, verify_password, generate_token, invalidate_user_sessions,
# get_current_user, require_permission are now imported from tenant_context


class DeviceMonthlyCountUpdateIn(BaseModel):
    monthly_count: int


class LinkCreate(BaseModel):
    mobile_id: str
    video_name: str
    shop_name: str
    gname: Optional[str] = None
    display_order: int = 0


class LinkOut(BaseModel):
    id: int
    did: int
    vid: int
    sid: int
    gid: Optional[int] = None
    created_at: datetime
    updated_at: datetime
    mobile_id: str
    device_name: Optional[str] = None
    video_name: str
    s3_link: Optional[str] = None
    shop_name: str
    gname: Optional[str] = None
    is_online: Optional[bool] = None
    temperature: Optional[float] = None
    daily_count: Optional[int] = None
    monthly_count: Optional[int] = None
    rotation: Optional[int] = 0
    content_type: Optional[str] = "video"
    fit_mode: Optional[str] = "cover"
    display_duration: Optional[int] = 10
    display_order: Optional[int] = 0
    resolution: Optional[str] = None
    device_rotation: Optional[int] = None
    device_resolution: Optional[str] = None
    grid_position: Optional[int] = 0
    grid_size: Optional[str] = None


class PresignedURLItem(BaseModel):
    link_id: int
    video_name: str
    url: str
    expires_in: int
    filename: str
    rotation: int = 0
    content_type: str = "video"
    fit_mode: str = "cover"
    display_duration: int = 10
    display_order: int = 0
    resolution: Optional[str] = None
    device_rotation: Optional[int] = None
    device_resolution: Optional[str] = None
    grid_position: int = 0
    grid_size: Optional[str] = None


class PresignedURLListOut(BaseModel):
    mobile_id: str
    items: List[PresignedURLItem]
    count: int
    layout_mode: str = "single"
    layout_config: Optional[str] = None


class DeviceLayoutIn(BaseModel):
    layout_mode: str = "single"
    layout_config: Optional[str] = None


class DeviceLayoutOut(BaseModel):
    mobile_id: str
    layout_mode: str
    layout_config: Optional[str] = None


class LinkUpdateIn(BaseModel):
    device_rotation: Optional[int] = None
    device_resolution: Optional[str] = None
    grid_position: Optional[int] = None
    grid_size: Optional[str] = None
    display_order: Optional[int] = None


class DeviceDownloadStatusOut(BaseModel):
    mobile_id: str
    download_status: bool
    total_links: int
    downloaded_count: int


class DeviceDownloadProgressIn(BaseModel):
    current_file: int = 0
    total_files: int = 0
    file_name: Optional[str] = None
    progress: int = 0  # 0-100 percentage
    downloaded_bytes: int = 0
    total_bytes: int = 0
    is_downloading: bool = False


class DeviceDownloadProgressOut(BaseModel):
    mobile_id: str
    current_file: int = 0
    total_files: int = 0
    file_name: Optional[str] = None
    progress: int = 0
    downloaded_bytes: int = 0
    total_bytes: int = 0
    is_downloading: bool = False
    updated_at: Optional[str] = None


class DeviceDownloadStatusSetIn(BaseModel):
    status: bool


class GroupVideoUpdateIn(BaseModel):
    video_name: str


class GroupVideoUpdateOut(BaseModel):
    gid: Optional[int] = None
    gname: Optional[str] = None
    vid: int
    video_name: str
    inserted_count: int
    deleted_count: int
    remaining_count: int
    devices_marked: int


class GroupVideosUpdateIn(BaseModel):
    video_names: List[str]


class GroupVideosUpdateOut(BaseModel):
    gid: Optional[int] = None
    gname: Optional[str] = None
    vids: List[int]
    video_names: List[str]
    inserted_count: int
    deleted_count: int
    updated_count: int
    devices_marked: int


class DeviceOnlineStatusOut(BaseModel):
    mobile_id: str
    is_online: bool


class DeviceOnlineUpdateIn(BaseModel):
    is_online: bool = True


class DeviceActiveStatusIn(BaseModel):
    """Request body for setting device active status"""
    is_active: bool


class DeviceCreateIn(BaseModel):
    mobile_id: str
    device_name: Optional[str] = None  # Friendly name for the device
    group_name: Optional[str] = None
    shop_name: Optional[str] = None
    resolution: Optional[str] = None


class DeviceNameUpdateIn(BaseModel):
    device_name: str


class VideoRotationUpdateIn(BaseModel):
    rotation: int = Field(..., ge=0, le=270)


class VideoFitModeUpdateIn(BaseModel):
    fit_mode: str


class VideoOut(BaseModel):
    id: int
    video_name: str
    s3_link: Optional[str] = None
    rotation: int = 0
    content_type: Optional[str] = "video"
    fit_mode: Optional[str] = "cover"
    display_duration: Optional[int] = 10
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None



# ---------- PG pool ----------
pg_pool: Optional[ThreadedConnectionPool] = None


def pg_init_pool():
    global pg_pool
    if pg_pool is None:
        pg_pool = ThreadedConnectionPool(
            DB_MIN_CONN, DB_MAX_CONN,
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS, connect_timeout=5,
        )


def pg_close_pool():
    global pg_pool
    if pg_pool:
        pg_pool.closeall()
        pg_pool = None


@contextmanager
def pg_conn():
    global pg_pool
    if pg_pool is None:
        pg_init_pool()
    conn = None
    try:
        try:
            conn = pg_pool.getconn()
        except PoolError:
            raise HTTPException(status_code=503, detail="Database connection pool exhausted")
        yield conn
    finally:
        if conn is not None:
            if pg_pool is not None:
                try:
                    pg_pool.putconn(conn)
                except PoolError:
                    try:
                        conn.close()
                    except:
                        pass
            else:
                try:
                    conn.close()
                except:
                    pass


# ---------- Counter reset logic ----------
def _check_and_reset_counters(conn, did: int) -> Tuple[bool, bool]:
    """Check and reset daily/monthly counters if needed. Saves to history before reset. Returns (daily_reset, monthly_reset)."""
    today = date.today()
    month_start = today.replace(day=1)
    daily_reset = False
    monthly_reset = False
    
    with conn.cursor() as cur:
        # Ensure count_history table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.count_history (
                id SERIAL PRIMARY KEY,
                did INTEGER NOT NULL REFERENCES public.device(id) ON DELETE CASCADE,
                period_type VARCHAR(10) NOT NULL, -- 'daily' or 'monthly'
                period_date DATE NOT NULL,
                count_value INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(did, period_type, period_date)
            );
            CREATE INDEX IF NOT EXISTS idx_count_history_did ON public.count_history(did);
            CREATE INDEX IF NOT EXISTS idx_count_history_period ON public.count_history(period_type, period_date);
        """)
        
        cur.execute(
            """
            SELECT last_daily_reset, last_monthly_reset, daily_count, monthly_count 
            FROM public.device WHERE id = %s;
            """,
            (did,)
        )
        row = cur.fetchone()
        if not row:
            return False, False
        
        last_daily = row[0]
        last_monthly = row[1]
        current_daily_count = row[2] or 0
        current_monthly_count = row[3] or 0
        
        updates = []
        params = []
        
        # Reset daily if last reset was before today
        if last_daily is None or last_daily < today:
            # Save yesterday's count to history (if there was a previous day)
            if last_daily is not None and current_daily_count > 0:
                try:
                    cur.execute("""
                        INSERT INTO public.count_history (did, period_type, period_date, count_value)
                        VALUES (%s, 'daily', %s, %s)
                        ON CONFLICT (did, period_type, period_date) DO UPDATE SET count_value = EXCLUDED.count_value;
                    """, (did, last_daily, current_daily_count))
                except Exception as e:
                    pass  # Ignore errors on history save
            
            updates.append("daily_count = 0")
            updates.append("last_daily_reset = %s")
            params.append(today)
            daily_reset = True
        
        # Reset monthly if last reset was before this month
        if last_monthly is None or last_monthly < month_start:
            # Save last month's count to history
            if last_monthly is not None and current_monthly_count > 0:
                try:
                    cur.execute("""
                        INSERT INTO public.count_history (did, period_type, period_date, count_value)
                        VALUES (%s, 'monthly', %s, %s)
                        ON CONFLICT (did, period_type, period_date) DO UPDATE SET count_value = EXCLUDED.count_value;
                    """, (did, last_monthly, current_monthly_count))
                except Exception as e:
                    pass  # Ignore errors on history save
            
            updates.append("monthly_count = 0")
            updates.append("last_monthly_reset = %s")
            params.append(month_start)
            monthly_reset = True
        
        if updates:
            params.append(did)
            cur.execute(
                f"UPDATE public.device SET {', '.join(updates)}, updated_at = NOW() WHERE id = %s;",
                tuple(params)
            )
    
    return daily_reset, monthly_reset


def _log_event(conn, did: int, log_type: str, value: float = None):
    """Log an event (temperature or door_open) to device_logs table."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.device_logs (did, log_type, value)
            VALUES (%s, %s, %s);
            """,
            (did, log_type, value)
        )





def _insert_temperature_point(conn, did: int, temperature: float):
    """Store a temperature point for reports.

    Compatibility notes:
    - Some deployments created `public.device_temperature`
    - New requirement uses `public.temperature`
    We insert into whichever exists (prefer `temperature`).
    """
    with conn.cursor() as cur:
        # Prefer new table
        cur.execute("SELECT to_regclass('public.temperature');")
        has_temperature = cur.fetchone()[0] is not None

        cur.execute("SELECT to_regclass('public.device_temperature');")
        has_device_temperature = cur.fetchone()[0] is not None

        if has_temperature:
            cur.execute(
                """
                INSERT INTO public.temperature (did, temperature, recorded_at)
                VALUES (%s, %s, NOW());
                """,
                (did, temperature),
            )
            return

        if has_device_temperature:
            # Some schemas used recorded_at, some created_at
            cur.execute("""
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='device_temperature' AND column_name='recorded_at'
                LIMIT 1;
            """)
            ts_col = "recorded_at" if cur.fetchone() else "created_at"

            q = sql.SQL("""
                INSERT INTO {tbl} (did, temperature, {ts})
                VALUES (%s, %s, NOW());
            """).format(
                tbl=sql.Identifier("public", "device_temperature"),
                ts=sql.Identifier(ts_col),
            )
            cur.execute(q, (did, temperature))
            return

        # Last resort: store only in device_logs (already stored elsewhere typically)
        try:
            _log_event(conn, did, "temperature", temperature)
        except Exception:
            pass

# ---------- SQL helpers ----------

READ_JOIN_SQL = """
SELECT l.id, l.did, l.vid, l.sid, l.gid, l.created_at, l.updated_at,
       d.mobile_id, d.is_online,
       d.temperature, d.daily_count, d.monthly_count,
       v.video_name, v.s3_link, s.shop_name, g.gname,
       v.rotation, v.content_type, v.fit_mode, v.display_duration,
       l.display_order, v.resolution,
       l.device_rotation, l.device_resolution, l.grid_position, l.grid_size,
       d.device_name, d.is_active
FROM public.device_video_shop_group l
JOIN public.device d ON d.id = l.did
JOIN public.video  v ON v.id = l.vid
JOIN public.shop   s ON s.id = l.sid
LEFT JOIN public."group" g ON g.id = l.gid
"""


def _row_to_link_dict(row) -> Dict[str, Any]:
    temp_val = row[9]
    if temp_val is not None:
        temp_val = float(temp_val)
    return {
        "id": row[0],
        "did": row[1],
        "vid": row[2],
        "sid": row[3],
        "gid": row[4],
        "created_at": row[5],
        "updated_at": row[6],
        "mobile_id": row[7],
        "is_online": row[8],
        "temperature": temp_val,
        "daily_count": row[10],
        "monthly_count": row[11],
        "video_name": row[12],
        "s3_link": row[13],
        "shop_name": row[14],
        "gname": row[15],
        "rotation": row[16] if len(row) > 16 else 0,
        "content_type": row[17] if len(row) > 17 else "video",
        "fit_mode": row[18] if len(row) > 18 else "cover",
        "display_duration": row[19] if len(row) > 19 else 10,
        "display_order": row[20] if len(row) > 20 else 0,
        "resolution": row[21] if len(row) > 21 else None,
        "device_rotation": row[22] if len(row) > 22 else None,
        "device_resolution": row[23] if len(row) > 23 else None,
        "grid_position": row[24] if len(row) > 24 else 0,
        "grid_size": row[25] if len(row) > 25 else None,
        "device_name": row[26] if len(row) > 26 else None,
        "is_active": row[27] if len(row) > 27 else True,
    }


def get_device_id_by_mobile(conn, mobile_id: str) -> Optional[int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM public.device WHERE mobile_id = %s ORDER BY id DESC LIMIT 1;",
            (mobile_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else None


def fetch_link_by_id(conn, link_id: int) -> Optional[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(READ_JOIN_SQL + " WHERE l.id = %s;", (link_id,))
        row = cur.fetchone()
        return _row_to_link_dict(row) if row else None


def fetch_links_for_mobile(conn, mobile_id: str, limit: int, offset: int) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            READ_JOIN_SQL + """
            WHERE d.mobile_id = %s
            ORDER BY l.display_order ASC, l.updated_at DESC, l.id DESC
            LIMIT %s OFFSET %s;
            """,
            (mobile_id, limit, offset),
        )
        rows = cur.fetchall()
        return [_row_to_link_dict(r) for r in rows]


def list_links(conn, mobile_id, video_name, shop_name, gname, did, vid, sid, gid, limit, offset, tenant_id=None):
    """
    List all device-video-shop-group links.
    Also includes:
    - Devices from device_assignment that have group/shop assigned but no video links yet
    - Completely unassigned devices (not in any group) - shown with gname="Unassigned"
    """
    where, params = [], []
    # Tenant scoping
    if tenant_id:
        where.append("l.tenant_id = %s")
        params.append(tenant_id)
    if mobile_id:
        where.append("(d.mobile_id ILIKE %s OR d.device_name ILIKE %s)")
        params.append(f"%{mobile_id}%")
        params.append(f"%{mobile_id}%")
    if video_name:
        where.append("v.video_name ILIKE %s")
        params.append(f"%{video_name}%")
    if shop_name:
        where.append("s.shop_name ILIKE %s")
        params.append(f"%{shop_name}%")
    if gname:
        # Special handling for "Unassigned" filter
        if gname.lower() == "unassigned":
            where.append("g.gname IS NULL")
        else:
            where.append("g.gname ILIKE %s")
            params.append(f"%{gname}%")
    if did is not None:
        where.append("l.did = %s")
        params.append(did)
    if vid is not None:
        where.append("l.vid = %s")
        params.append(vid)
    if sid is not None:
        where.append("l.sid = %s")
        params.append(sid)
    if gid is not None:
        where.append("l.gid = %s")
        params.append(gid)

    # Main query for existing links
    sql = READ_JOIN_SQL + (" WHERE " + " AND ".join(where) if where else "")
    sql += " ORDER BY l.display_order ASC, l.id DESC LIMIT %s OFFSET %s;"
    params.extend([limit, offset])

    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        links = [_row_to_link_dict(r) for r in rows]
        
        # Get device IDs that already have links
        existing_dids = {link["did"] for link in links}
        
        # Query devices from device_assignment that have no video links
        # Only add these if we're not filtering by video_name or vid (since they have no videos)
        if vid is None and video_name is None:
            assignment_where = []
            assignment_params = []
            
            if mobile_id:
                assignment_where.append("(d.mobile_id ILIKE %s OR d.device_name ILIKE %s)")
                assignment_params.append(f"%{mobile_id}%")
                assignment_params.append(f"%{mobile_id}%")
            if shop_name:
                assignment_where.append("s.shop_name ILIKE %s")
                assignment_params.append(f"%{shop_name}%")
            if gname and gname.lower() != "unassigned":
                assignment_where.append("g.gname ILIKE %s")
                assignment_params.append(f"%{gname}%")
            if did is not None:
                assignment_where.append("da.did = %s")
                assignment_params.append(did)
            if sid is not None:
                assignment_where.append("da.sid = %s")
                assignment_params.append(sid)
            if gid is not None:
                assignment_where.append("da.gid = %s")
                assignment_params.append(gid)
            
            # Skip this query if filtering for "Unassigned" since device_assignment devices have a group
            if not (gname and gname.lower() == "unassigned"):
                assignment_sql = """
                    SELECT da.id, da.did, NULL as vid, da.sid, da.gid, da.created_at, da.updated_at,
                           d.mobile_id, d.is_online,
                           d.temperature, d.daily_count, d.monthly_count,
                           NULL as video_name, NULL as s3_link, s.shop_name, g.gname,
                           0 as rotation, 'video' as content_type, 'cover' as fit_mode, 10 as display_duration,
                           0 as display_order, NULL as resolution,
                           NULL as device_rotation, NULL as device_resolution, 0 as grid_position, NULL as grid_size,
                           d.device_name, d.is_active
                    FROM public.device_assignment da
                    JOIN public.device d ON d.id = da.did
                    LEFT JOIN public.shop s ON s.id = da.sid
                    LEFT JOIN public."group" g ON g.id = da.gid
                    WHERE NOT EXISTS (
                        SELECT 1 FROM public.device_video_shop_group l WHERE l.did = da.did
                    )
                """
                if assignment_where:
                    assignment_sql += " AND " + " AND ".join(assignment_where)
                assignment_sql += " ORDER BY da.id DESC LIMIT %s OFFSET %s;"
                assignment_params.extend([limit, offset])
                
                cur.execute(assignment_sql, tuple(assignment_params))
                assignment_rows = cur.fetchall()
                
                for row in assignment_rows:
                    if row[1] not in existing_dids:  # row[1] is did
                        links.append(_row_to_link_dict(row))
                        existing_dids.add(row[1])
            
            # Query completely unassigned devices (not in device_assignment and not in device_video_shop_group)
            # These are devices that exist but have never been assigned to any group
            unassigned_where = []
            unassigned_params = []
            
            if mobile_id:
                unassigned_where.append("(d.mobile_id ILIKE %s OR d.device_name ILIKE %s)")
                unassigned_params.append(f"%{mobile_id}%")
                unassigned_params.append(f"%{mobile_id}%")
            if did is not None:
                unassigned_where.append("d.id = %s")
                unassigned_params.append(did)
            
            # Only show unassigned devices if not filtering by specific shop, group, or gid
            # (since unassigned devices don't have those)
            if sid is None and gid is None and (not shop_name) and (not gname or gname.lower() == "unassigned"):
                unassigned_sql = """
                    SELECT 0 as id, d.id as did, NULL as vid, NULL as sid, NULL as gid, d.created_at, d.updated_at,
                           d.mobile_id, d.is_online,
                           d.temperature, d.daily_count, d.monthly_count,
                           NULL as video_name, NULL as s3_link, NULL as shop_name, 'Unassigned' as gname,
                           0 as rotation, 'video' as content_type, 'cover' as fit_mode, 10 as display_duration,
                           0 as display_order, NULL as resolution,
                           NULL as device_rotation, NULL as device_resolution, 0 as grid_position, NULL as grid_size,
                           d.device_name, d.is_active
                    FROM public.device d
                    WHERE NOT EXISTS (
                        SELECT 1 FROM public.device_video_shop_group l WHERE l.did = d.id
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM public.device_assignment da WHERE da.did = d.id
                    )
                """
                if unassigned_where:
                    unassigned_sql += " AND " + " AND ".join(unassigned_where)
                unassigned_sql += " ORDER BY d.is_online DESC, d.id DESC LIMIT %s OFFSET %s;"
                unassigned_params.extend([limit, offset])
                
                cur.execute(unassigned_sql, tuple(unassigned_params))
                unassigned_rows = cur.fetchall()
                
                for row in unassigned_rows:
                    if row[1] not in existing_dids:  # row[1] is did
                        links.append(_row_to_link_dict(row))
                        existing_dids.add(row[1])
        
        return links


def delete_link_row(conn, link_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM public.device_video_shop_group WHERE id = %s RETURNING id;",
            (link_id,),
        )
        rows = cur.fetchall()
    return len(rows)


def _aggregate_from_links(conn, did: int) -> Tuple[int, int, bool]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int,
                   COALESCE(SUM(CASE WHEN dl_status THEN 1 ELSE 0 END)::int, 0)
            FROM public.device_video_shop_group
            WHERE did = %s;
            """,
            (did,),
        )
        total, done = cur.fetchone()
        return total, done, (total > 0 and done == total)


def _write_device_flag(conn, did: int, value: bool) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.device
               SET download_status = %s, updated_at = NOW()
             WHERE id = %s;
            """,
            (value, did),
        )


def _enforce_single_group_shop_for_device(conn, did: int, target_gid: Optional[int], target_sid: int) -> int:
    """
    Enforce that a device can only be assigned to one group/shop combination.
    If the device already has links to a DIFFERENT group, raise an error.
    """
    with conn.cursor() as cur:
        # Check if device already has links to a different group
        cur.execute(
            """
            SELECT DISTINCT gid, 
                   (SELECT gname FROM public."group" WHERE id = gid) as gname
            FROM public.device_video_shop_group
            WHERE did = %s AND gid IS NOT NULL
            LIMIT 1;
            """,
            (did,),
        )
        existing = cur.fetchone()
        
        if existing and existing[0] is not None:
            existing_gid = existing[0]
            existing_gname = existing[1] or f"Group ID {existing_gid}"
            
            # If trying to assign to a different group, raise error
            if target_gid is None or existing_gid != target_gid:
                raise HTTPException(
                    status_code=409,
                    detail=f"Device is already assigned to group '{existing_gname}'. A device can only belong to one group. Please remove existing links first or use the same group."
                )
        
        # If assigning to same group but different shop, check that too
        if target_gid is not None:
            cur.execute(
                """
                SELECT DISTINCT sid,
                       (SELECT shop_name FROM public.shop WHERE id = sid) as shop_name
                FROM public.device_video_shop_group
                WHERE did = %s AND gid = %s AND sid != %s
                LIMIT 1;
                """,
                (did, target_gid, target_sid),
            )
            diff_shop = cur.fetchone()
            if diff_shop:
                existing_shop = diff_shop[1] or f"Shop ID {diff_shop[0]}"
                raise HTTPException(
                    status_code=409,
                    detail=f"Device is already assigned to shop '{existing_shop}' in this group. Please remove existing links first or use the same shop."
                )
        
        return 0


# ---------- S3 helpers ----------
def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    if not s3_uri or not s3_uri.startswith("s3://"):
        raise ValueError("Invalid S3 URI")
    without = s3_uri[5:]
    parts = without.split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError("Invalid S3 URI")
    return parts[0], parts[1]


def presign_get_object(s3_uri: str, expires_in: int = PRESIGN_EXPIRES) -> Tuple[str, str]:
    bucket, key = parse_s3_uri(s3_uri)
    s3 = boto3.client("s3", region_name=AWS_REGION) if AWS_REGION else boto3.client("s3")
    filename = key.split("/")[-1] or "download.mp4"
    
    # Detect content type from extension
    ext = filename.lower().split('.')[-1] if '.' in filename else 'mp4'
    content_types = {
        'mp4': 'video/mp4', 'webm': 'video/webm', 'mov': 'video/quicktime',
        'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
        'gif': 'image/gif', 'webp': 'image/webp',
        'html': 'text/html', 'htm': 'text/html',
        'pdf': 'application/pdf',
    }
    content_type = content_types.get(ext, 'video/mp4')
    
    params = {
        "Bucket": bucket,
        "Key": key,
        "ResponseContentDisposition": f'attachment; filename="{filename}"',
        "ResponseContentType": content_type,
    }
    url = s3.generate_presigned_url("get_object", Params=params, ExpiresIn=expires_in)
    return url, filename


# ---------- Group video operations ----------
def _resolve_group_id(conn, gname: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            'SELECT id FROM public."group" WHERE gname = %s ORDER BY id DESC LIMIT 1;',
            (gname,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Group not found: {gname}")
        return int(row[0])


def _resolve_video_id(conn, video_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM public.video WHERE video_name = %s ORDER BY id DESC LIMIT 1;",
            (video_name,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Video not found: {video_name}")
        return int(row[0])


def _resolve_video_ids(conn, names: List[str]) -> List[int]:
    unique = list(dict.fromkeys([n.strip() for n in names if n.strip()]))
    if not unique:
        raise HTTPException(status_code=400, detail="video_names must be non-empty")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT video_name, id FROM public.video WHERE video_name = ANY(%s);",
            (unique,),
        )
        found = dict(cur.fetchall() or [])
    missing = [n for n in unique if n not in found]
    if missing:
        raise HTTPException(status_code=404, detail=f"Videos not found: {', '.join(missing)}")
    return [found[n] for n in unique]


def _set_group_video(conn, gid: int, vid: int):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT did FROM public.device_video_shop_group WHERE gid = %s;", (gid,))
        dev_ids = [r[0] for r in cur.fetchall()]
        
        cur.execute("""
        WITH targets AS (
            SELECT did, sid, gid FROM public.device_video_shop_group WHERE gid = %s GROUP BY did, sid, gid
        ),
        ins AS (
            INSERT INTO public.device_video_shop_group (did, vid, sid, gid)
            SELECT t.did, %s, t.sid, t.gid FROM targets t
            ON CONFLICT DO NOTHING RETURNING id
        ),
        del AS (
            DELETE FROM public.device_video_shop_group l
            USING targets t WHERE l.did = t.did AND l.sid = t.sid AND l.gid = t.gid AND l.vid <> %s
            RETURNING l.id
        ),
        upd AS (
            UPDATE public.device_video_shop_group l SET updated_at = NOW()
            FROM targets t WHERE l.did = t.did AND l.sid = t.sid AND l.gid = t.gid AND l.vid = %s
            RETURNING l.id
        )
        SELECT (SELECT COUNT(*) FROM ins)::int, (SELECT COUNT(*) FROM del)::int, (SELECT COUNT(*) FROM upd)::int;
        """, (gid, vid, vid, vid))
        inserted, deleted, remaining = cur.fetchone()
        
        cur.execute('SELECT gname FROM public."group" WHERE id = %s;', (gid,))
        gname = cur.fetchone()[0]
        cur.execute("SELECT video_name FROM public.video WHERE id = %s;", (vid,))
        video_name = cur.fetchone()[0]
        
        devices_marked = 0
        if dev_ids:
            cur.execute("UPDATE public.device SET download_status = FALSE, updated_at = NOW() WHERE id = ANY(%s);", (dev_ids,))
            devices_marked = cur.rowcount or 0
    
    return inserted, deleted, remaining, gname, video_name, devices_marked


def _set_group_videos(conn, gid: int, vids: List[int]):
    """
    Links videos to a group via the group_video table.
    Also updates device_video_shop_group for any devices already in this group
    (checking both device_video_shop_group and device_assignment tables).
    """
    vids = list(dict.fromkeys(vids))  # Remove duplicates while preserving order
    
    with conn.cursor() as cur:
        inserted = 0
        deleted = 0
        updated = 0
        
        if vids:
            # First, update the group_video table (group-level association)
            cur.execute("""
            WITH ins AS (
                INSERT INTO public.group_video (gid, vid, display_order)
                SELECT %s, v, row_number() OVER () - 1
                FROM UNNEST(%s::bigint[]) AS v
                ON CONFLICT (gid, vid) DO UPDATE SET updated_at = NOW()
                RETURNING id, (xmax = 0) AS was_inserted
            ),
            del AS (
                DELETE FROM public.group_video 
                WHERE gid = %s AND NOT (vid = ANY(%s::bigint[]))
                RETURNING id
            )
            SELECT 
                (SELECT COUNT(*) FILTER (WHERE was_inserted) FROM ins)::int,
                (SELECT COUNT(*) FROM del)::int,
                (SELECT COUNT(*) FILTER (WHERE NOT was_inserted) FROM ins)::int;
            """, (gid, vids, gid, vids))
            inserted, deleted, updated = cur.fetchone()
        else:
            # If no videos, just delete all group_video entries for this group
            cur.execute("DELETE FROM public.group_video WHERE gid = %s RETURNING id;", (gid,))
            deleted = cur.rowcount or 0
        
        # Get devices assigned to this group from device_assignment table
        cur.execute("SELECT did, sid FROM public.device_assignment WHERE gid = %s;", (gid,))
        assigned_devices = cur.fetchall() or []
        
        # Also check device_video_shop_group for legacy device-group links
        cur.execute("SELECT DISTINCT did, sid FROM public.device_video_shop_group WHERE gid = %s;", (gid,))
        legacy_devices = cur.fetchall() or []
        
        # Combine both sources (dedup by did)
        all_devices = {}
        for did, sid in legacy_devices:
            all_devices[did] = sid
        for did, sid in assigned_devices:
            all_devices[did] = sid
        
        dev_links = [(did, sid) for did, sid in all_devices.items()]
        
        if dev_links:
            if vids:
                # Remove videos no longer in the group from device links
                cur.execute("""
                    DELETE FROM public.device_video_shop_group 
                    WHERE gid = %s AND NOT (vid = ANY(%s::bigint[]));
                """, (gid, vids))
                
                # Add new videos to all devices in this group
                for did, sid in dev_links:
                    for vid in vids:
                        cur.execute("""
                            INSERT INTO public.device_video_shop_group (did, vid, sid, gid)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT DO NOTHING;
                        """, (did, vid, sid, gid))
            else:
                # No videos - remove all device_video_shop_group entries for this group
                cur.execute("DELETE FROM public.device_video_shop_group WHERE gid = %s;", (gid,))
        
        cur.execute('SELECT gname FROM public."group" WHERE id = %s;', (gid,))
        gname = cur.fetchone()[0]
        
        video_names = []
        if vids:
            cur.execute("SELECT video_name FROM public.video WHERE id = ANY(%s::bigint[]) ORDER BY video_name;", (vids,))
            video_names = [r[0] for r in cur.fetchall()]
        
        # Mark existing devices for re-download
        devices_marked = 0
        if dev_links:
            dev_ids = [d[0] for d in dev_links]
            cur.execute("UPDATE public.device SET download_status = FALSE, updated_at = NOW() WHERE id = ANY(%s);", (dev_ids,))
            devices_marked = cur.rowcount or 0
    
    return inserted, deleted, updated, gname, video_names, devices_marked


def _set_nogroup_video(conn, vid: int):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT did FROM public.device_video_shop_group WHERE gid IS NULL;")
        dev_ids = [r[0] for r in cur.fetchall()]
        
        cur.execute("""
        WITH targets AS (SELECT did, sid FROM public.device_video_shop_group WHERE gid IS NULL GROUP BY did, sid),
        ins AS (INSERT INTO public.device_video_shop_group (did, vid, sid, gid) SELECT t.did, %s, t.sid, NULL FROM targets t ON CONFLICT DO NOTHING RETURNING id),
        del AS (DELETE FROM public.device_video_shop_group l USING targets t WHERE l.did = t.did AND l.sid = t.sid AND l.gid IS NULL AND l.vid <> %s RETURNING l.id),
        upd AS (UPDATE public.device_video_shop_group l SET updated_at = NOW() FROM targets t WHERE l.did = t.did AND l.sid = t.sid AND l.gid IS NULL AND l.vid = %s RETURNING l.id)
        SELECT (SELECT COUNT(*) FROM ins)::int, (SELECT COUNT(*) FROM del)::int, (SELECT COUNT(*) FROM upd)::int;
        """, (vid, vid, vid))
        inserted, deleted, remaining = cur.fetchone()
        
        cur.execute("SELECT video_name FROM public.video WHERE id = %s;", (vid,))
        video_name = cur.fetchone()[0]
        
        devices_marked = 0
        if dev_ids:
            cur.execute("UPDATE public.device SET download_status = FALSE, updated_at = NOW() WHERE id = ANY(%s);", (dev_ids,))
            devices_marked = cur.rowcount or 0
    
    return inserted, deleted, remaining, video_name, devices_marked


def _set_nogroup_videos(conn, vids: List[int]):
    vids = list(dict.fromkeys(vids))
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT did FROM public.device_video_shop_group WHERE gid IS NULL;")
        dev_ids = [r[0] for r in cur.fetchall()]
        
        cur.execute("""
        WITH targets AS (SELECT did, sid FROM public.device_video_shop_group WHERE gid IS NULL GROUP BY did, sid),
        ins AS (INSERT INTO public.device_video_shop_group (did, vid, sid, gid) SELECT t.did, x, t.sid, NULL FROM targets t, UNNEST(%s::bigint[]) AS x ON CONFLICT DO NOTHING RETURNING id),
        del AS (DELETE FROM public.device_video_shop_group l USING targets t WHERE l.did = t.did AND l.sid = t.sid AND l.gid IS NULL AND NOT (l.vid = ANY(%s::bigint[])) RETURNING l.id),
        upd AS (UPDATE public.device_video_shop_group l SET updated_at = NOW() FROM targets t WHERE l.did = t.did AND l.sid = t.sid AND l.gid IS NULL AND l.vid = ANY(%s::bigint[]) RETURNING l.id)
        SELECT (SELECT COUNT(*) FROM ins)::int, (SELECT COUNT(*) FROM del)::int, (SELECT COUNT(*) FROM upd)::int;
        """, (vids, vids, vids))
        inserted, deleted, updated = cur.fetchone()
        
        cur.execute("SELECT video_name FROM public.video WHERE id = ANY(%s::bigint[]) ORDER BY video_name;", (vids,))
        video_names = [r[0] for r in cur.fetchall()]
        
        devices_marked = 0
        if dev_ids:
            cur.execute("UPDATE public.device SET download_status = FALSE, updated_at = NOW() WHERE id = ANY(%s);", (dev_ids,))
            devices_marked = cur.rowcount or 0
    
    return inserted, deleted, updated, video_names, devices_marked


# ---------- Link creation ----------
def create_link_by_names(conn, payload: LinkCreate) -> Dict[str, Any]:
    did = get_device_id_by_mobile(conn, payload.mobile_id)
    if did is None:
        raise HTTPException(status_code=404, detail=f"Device not found: {payload.mobile_id}")
    
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM public.video WHERE video_name = %s ORDER BY id DESC LIMIT 1;", (payload.video_name,))
        vrow = cur.fetchone()
        if not vrow:
            raise HTTPException(status_code=404, detail=f"Video not found: {payload.video_name}")
        vid = int(vrow[0])
        
        cur.execute("SELECT id FROM public.shop WHERE shop_name = %s ORDER BY id DESC LIMIT 1;", (payload.shop_name,))
        srow = cur.fetchone()
        if not srow:
            raise HTTPException(status_code=404, detail=f"Shop not found: {payload.shop_name}")
        sid = int(srow[0])
        
        gid = None
        gname_in = (payload.gname or "").strip()
        if gname_in and gname_in.lower() not in NO_GROUP_SENTINELS:
            cur.execute('SELECT id FROM public."group" WHERE gname = %s ORDER BY id DESC LIMIT 1;', (gname_in,))
            grow = cur.fetchone()
            if not grow:
                raise HTTPException(status_code=404, detail=f"Group not found: {gname_in}")
            gid = int(grow[0])
        
        _enforce_single_group_shop_for_device(conn, did, gid, sid)
        
        if gid is None:
            # Try insert first, if duplicate exists update it
            cur.execute("""
                INSERT INTO public.device_video_shop_group (did, vid, sid, gid, display_order)
                VALUES (%s, %s, %s, NULL, %s)
                ON CONFLICT DO NOTHING;
            """, (did, vid, sid, payload.display_order))
            if cur.rowcount == 0:
                # Already exists, update and fetch
                cur.execute("""
                    UPDATE public.device_video_shop_group SET updated_at = NOW(), display_order = %s
                    WHERE did = %s AND vid = %s AND sid = %s AND gid IS NULL
                    RETURNING id;
                """, (payload.display_order, did, vid, sid))
            else:
                cur.execute("""
                    SELECT id FROM public.device_video_shop_group
                    WHERE did = %s AND vid = %s AND sid = %s AND gid IS NULL
                    ORDER BY id DESC LIMIT 1;
                """, (did, vid, sid))
        else:
            cur.execute("""
                INSERT INTO public.device_video_shop_group (did, vid, sid, gid, display_order)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (did, vid, sid, gid, payload.display_order))
            if cur.rowcount == 0:
                cur.execute("""
                    UPDATE public.device_video_shop_group SET updated_at = NOW(), display_order = %s
                    WHERE did = %s AND vid = %s AND sid = %s AND gid = %s
                    RETURNING id;
                """, (payload.display_order, did, vid, sid, gid))
            else:
                cur.execute("""
                    SELECT id FROM public.device_video_shop_group
                    WHERE did = %s AND vid = %s AND sid = %s AND gid = %s
                    ORDER BY id DESC LIMIT 1;
                """, (did, vid, sid, gid))
            
            # Also add to group_video table to ensure group-level association exists
            # This ensures future device assignments will get this video
            cur.execute("""
                INSERT INTO public.group_video (gid, vid, display_order)
                VALUES (%s, %s, COALESCE(%s, 0))
                ON CONFLICT (gid, vid) DO NOTHING;
            """, (gid, vid, payload.display_order))
        
        lrow = cur.fetchone()
        cur.execute(READ_JOIN_SQL + " WHERE l.id = %s;", (lrow[0],))
        full = cur.fetchone()
        _write_device_flag(conn, did, False)
    
    return _row_to_link_dict(full)


# ---------- startup/shutdown ----------
@app.on_event("startup")
def on_startup():
    pg_init_pool()
    with pg_conn() as conn:
        ensure_dvsg_schema(conn)
        ensure_multitenant_schema(conn)


@app.on_event("shutdown")
def on_shutdown():
    pg_close_pool()

# Mount platform router
app.include_router(platform_router, prefix="/platform", tags=["platform"])


# ---------- health endpoints ----------
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/debug/group/{gname}/videos")
def debug_group_videos(gname: str):
    """Debug endpoint to check what videos are linked to a group."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            # Get group ID
            cur.execute('SELECT id FROM public."group" WHERE gname = %s ORDER BY id DESC LIMIT 1;', (gname,))
            grow = cur.fetchone()
            if not grow:
                return {"error": f"Group not found: {gname}"}
            gid = int(grow[0])
            
            # Check group_video table
            cur.execute("""
                SELECT gv.id, gv.vid, v.video_name, gv.display_order
                FROM public.group_video gv
                JOIN public.video v ON v.id = gv.vid
                WHERE gv.gid = %s
                ORDER BY gv.display_order;
            """, (gid,))
            group_videos = [{"id": r[0], "vid": r[1], "video_name": r[2], "display_order": r[3]} for r in cur.fetchall()]
            
            # Check device_video_shop_group for this group
            cur.execute("""
                SELECT DISTINCT dvsg.vid, v.video_name
                FROM public.device_video_shop_group dvsg
                JOIN public.video v ON v.id = dvsg.vid
                WHERE dvsg.gid = %s;
            """, (gid,))
            dvsg_videos = [{"vid": r[0], "video_name": r[1]} for r in cur.fetchall()]
            
            # Check device_assignment for this group
            cur.execute("""
                SELECT da.did, d.mobile_id, d.device_name, da.sid, s.shop_name
                FROM public.device_assignment da
                JOIN public.device d ON d.id = da.did
                LEFT JOIN public.shop s ON s.id = da.sid
                WHERE da.gid = %s;
            """, (gid,))
            assigned_devices = [{"did": r[0], "mobile_id": r[1], "device_name": r[2], "sid": r[3], "shop_name": r[4]} for r in cur.fetchall()]
            
            # Check device_video_shop_group links for devices in this group
            cur.execute("""
                SELECT dvsg.did, d.mobile_id, dvsg.vid, v.video_name
                FROM public.device_video_shop_group dvsg
                JOIN public.device d ON d.id = dvsg.did
                JOIN public.video v ON v.id = dvsg.vid
                WHERE dvsg.gid = %s
                ORDER BY dvsg.did, dvsg.display_order;
            """, (gid,))
            device_links = [{"did": r[0], "mobile_id": r[1], "vid": r[2], "video_name": r[3]} for r in cur.fetchall()]
            
            return {
                "group_name": gname,
                "gid": gid,
                "group_video_table": {
                    "count": len(group_videos),
                    "videos": group_videos
                },
                "device_video_shop_group_videos": {
                    "count": len(dvsg_videos),
                    "videos": dvsg_videos
                },
                "assigned_devices": {
                    "count": len(assigned_devices),
                    "devices": assigned_devices
                },
                "device_links": {
                    "count": len(device_links),
                    "links": device_links
                }
            }


@app.get("/db_health")
def db_health():
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_user;")
            user = cur.fetchone()[0]
            cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='device_video_shop_group');")
            has_link = cur.fetchone()[0]
        return {"ok": True, "db_user": user, "dvsg_table_exists": has_link}


@app.get("/pool_stats")
def pool_stats():
    global pg_pool
    if pg_pool is None:
        return {"status": "no_pool"}
    return {"minconn": pg_pool.minconn, "maxconn": pg_pool.maxconn, "used": len(pg_pool._used), "free": len(pg_pool._pool)}


# ---------- Device counts ----------
@app.get("/device/{mobile_id}/counts", response_model=DeviceCountsOut)
def get_device_counts(mobile_id: str):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, daily_count, monthly_count FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            did = row[0]
        # Check and reset counters
        _check_and_reset_counters(conn, did)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT daily_count, monthly_count FROM public.device WHERE id = %s;", (did,))
            row = cur.fetchone()
            return DeviceCountsOut(mobile_id=mobile_id, daily_count=row[0], monthly_count=row[1])


# ---------- Group videos ----------
@app.get("/group/{gname}/videos")
def list_group_videos_by_name(gname: str):
    """
    List videos linked to a group.
    Reads from both group_video table and device_video_shop_group table.
    """
    with pg_conn() as conn:
        with conn.cursor() as cur:
            if gname.strip().lower() in NO_GROUP_SENTINELS:
                # For "no group", still use device_video_shop_group (legacy behavior)
                cur.execute("""
                    SELECT DISTINCT v.id, v.video_name
                    FROM public.device_video_shop_group l
                    JOIN public.video v ON v.id = l.vid
                    WHERE l.gid IS NULL ORDER BY v.video_name;
                """)
                rows = cur.fetchall() or []
                return {"gid": None, "gname": None, "vids": [r[0] for r in rows], "video_names": [r[1] for r in rows], "count": len(rows)}
            
            cur.execute('SELECT id FROM public."group" WHERE gname = %s ORDER BY id DESC LIMIT 1;', (gname,))
            grow = cur.fetchone()
            if not grow:
                raise HTTPException(status_code=404, detail=f"Group not found: {gname}")
            gid = int(grow[0])
            
            # Read from group_video table (primary source for group-level associations)
            cur.execute("""
                SELECT v.id, v.video_name
                FROM public.group_video gv
                JOIN public.video v ON v.id = gv.vid
                WHERE gv.gid = %s 
                ORDER BY gv.display_order, v.video_name;
            """, (gid,))
            rows = cur.fetchall() or []
            video_ids = {r[0] for r in rows}
            videos = [{"id": r[0], "name": r[1]} for r in rows]
            
            # Also check device_video_shop_group for additional video links
            cur.execute("""
                SELECT DISTINCT v.id, v.video_name
                FROM public.device_video_shop_group dvsg
                JOIN public.video v ON v.id = dvsg.vid
                WHERE dvsg.gid = %s
                ORDER BY v.video_name;
            """, (gid,))
            for r in cur.fetchall():
                if r[0] not in video_ids:
                    videos.append({"id": r[0], "name": r[1]})
                    video_ids.add(r[0])
            
            return {
                "gid": gid, 
                "gname": gname, 
                "vids": [v["id"] for v in videos], 
                "video_names": [v["name"] for v in videos], 
                "count": len(videos)
            }


@app.get("/group/{gname}/attachments")
def get_group_attachments(gname: str):
    """Get all attachments (videos, images, devices) for a group."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            # Get group ID
            cur.execute('SELECT id FROM public."group" WHERE gname = %s ORDER BY id DESC LIMIT 1;', (gname,))
            grow = cur.fetchone()
            if not grow:
                raise HTTPException(status_code=404, detail=f"Group not found: {gname}")
            gid = int(grow[0])
            
            # Get videos from group_video table
            cur.execute("""
                SELECT v.id, v.video_name
                FROM public.group_video gv
                JOIN public.video v ON v.id = gv.vid
                WHERE gv.gid = %s 
                ORDER BY gv.display_order, v.video_name;
            """, (gid,))
            videos = [{"id": r[0], "video_name": r[1]} for r in cur.fetchall()]
            video_ids = {v["id"] for v in videos}
            
            # Also check device_video_shop_group for additional video links
            cur.execute("""
                SELECT DISTINCT v.id, v.video_name
                FROM public.device_video_shop_group dvsg
                JOIN public.video v ON v.id = dvsg.vid
                WHERE dvsg.gid = %s
                ORDER BY v.video_name;
            """, (gid,))
            for r in cur.fetchall():
                if r[0] not in video_ids:
                    videos.append({"id": r[0], "video_name": r[1]})
                    video_ids.add(r[0])
            
            # Get advertisements from group_advertisement table
            cur.execute("""
                SELECT a.id, a.ad_name, a.s3_link, a.rotation, a.fit_mode
                FROM public.group_advertisement ga
                JOIN public.advertisement a ON ga.aid = a.id
                WHERE ga.gid = %s
                ORDER BY ga.display_order;
            """, (gid,))
            advertisements = []
            ad_names_set = set()
            for r in cur.fetchall():
                advertisements.append({
                    "id": r[0],
                    "ad_name": r[1],
                    "s3_link": r[2],
                    "rotation": r[3],
                    "fit_mode": r[4],
                })
                ad_names_set.add(r[1])
            
            # Also check device_layout for devices in this group that have ads in their layout_config
            cur.execute("""
                SELECT DISTINCT dl.layout_config
                FROM public.device_layout dl
                JOIN public.device d ON d.id = dl.did
                LEFT JOIN public.device_assignment da ON da.did = d.id
                LEFT JOIN public.device_video_shop_group dvsg ON dvsg.did = d.id
                WHERE (da.gid = %s OR dvsg.gid = %s) AND dl.layout_config IS NOT NULL;
            """, (gid, gid))
            
            # Parse layout_configs to find ad_names
            found_ad_names = set()
            for row in cur.fetchall():
                if row[0]:
                    try:
                        config = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                        if isinstance(config, list):
                            for slot in config:
                                if slot.get("ad_name") and slot.get("content_type") == "image":
                                    found_ad_names.add(slot["ad_name"])
                    except:
                        pass
            
            # Get details for any new ad_names found
            new_ad_names = found_ad_names - ad_names_set
            if new_ad_names:
                cur.execute("""
                    SELECT id, ad_name, s3_link, rotation, fit_mode
                    FROM public.advertisement
                    WHERE ad_name = ANY(%s);
                """, (list(new_ad_names),))
                for r in cur.fetchall():
                    advertisements.append({
                        "id": r[0],
                        "ad_name": r[1],
                        "s3_link": r[2],
                        "rotation": r[3],
                        "fit_mode": r[4],
                    })
            
            # Get devices from device_assignment
            cur.execute("""
                SELECT DISTINCT d.id, d.mobile_id, d.device_name
                FROM public.device_assignment da
                JOIN public.device d ON d.id = da.did
                WHERE da.gid = %s
                ORDER BY d.device_name, d.mobile_id;
            """, (gid,))
            devices = [{"id": r[0], "mobile_id": r[1], "device_name": r[2]} for r in cur.fetchall()]
            device_ids = {d["id"] for d in devices}
            
            # Also check device_video_shop_group for devices
            cur.execute("""
                SELECT DISTINCT d.id, d.mobile_id, d.device_name
                FROM public.device_video_shop_group dvsg
                JOIN public.device d ON d.id = dvsg.did
                WHERE dvsg.gid = %s
                ORDER BY d.device_name, d.mobile_id;
            """, (gid,))
            for r in cur.fetchall():
                if r[0] not in device_ids:
                    devices.append({"id": r[0], "mobile_id": r[1], "device_name": r[2]})
                    device_ids.add(r[0])
            
            return {
                "gid": gid,
                "gname": gname,
                "videos": videos,
                "video_count": len(videos),
                "advertisements": advertisements,
                "advertisement_count": len(advertisements),
                "devices": devices,
                "device_count": len(devices),
            }


# ---------- Temperature, daily, monthly updates ----------
@app.post("/device/{mobile_id}/temperature_update")
def set_temperature(mobile_id: str, body: DeviceTemperatureUpdateIn):
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Device not found")
                did = row[0]
                
                cur.execute("""
                    UPDATE public.device SET temperature = %s, updated_at = NOW()
                    WHERE id = %s RETURNING temperature;
                """, (body.temperature, did))
                temp = cur.fetchone()[0]
                
                # Log temperature
                _log_event(conn, did, 'temperature', body.temperature)
            

                # Store time-series point for line graph
                _insert_temperature_point(conn, did, float(body.temperature))
            conn.commit()
            return {"mobile_id": mobile_id, "temperature": float(temp)}
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise





@app.get("/device/{mobile_id}/temperature_series")
def get_temperature_series(
    mobile_id: str,
    days: int = Query(30, ge=1, le=365),
    bucket: str = Query("day"),
):
    """
    Time-series temperature data for line graph.

    - days: how many days back (default 30)
    - bucket: 'day' or 'hour' (averages values inside each bucket)

    This endpoint supports multiple schema variants:
    - public.temperature (new)
    - public.device_temperature (legacy)
    - public.device_logs with log_type='temperature' (fallback)
    """
    bucket = (bucket or "day").lower().strip()
    if bucket not in ("day", "hour"):
        raise HTTPException(status_code=422, detail="bucket must be 'day' or 'hour'")

    with pg_conn() as conn:
        with conn.cursor() as cur:
            # Resolve device id
            cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            did = int(row[0])

            # Pick best available source for temperature history
            cur.execute("SELECT to_regclass('public.temperature');")
            has_temperature = cur.fetchone()[0] is not None

            cur.execute("SELECT to_regclass('public.device_temperature');")
            has_device_temperature = cur.fetchone()[0] is not None

            cur.execute("SELECT to_regclass('public.device_logs');")
            has_device_logs = cur.fetchone()[0] is not None

            if has_temperature:
                table = "temperature"
                ts_col = "recorded_at"
                val_col = "temperature"
                extra = sql.SQL("")
            elif has_device_temperature:
                table = "device_temperature"
                # Some schemas used recorded_at, some created_at
                cur.execute("""
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='public' AND table_name='device_temperature' AND column_name='recorded_at'
                    LIMIT 1;
                """)
                ts_col = "recorded_at" if cur.fetchone() else "created_at"
                val_col = "temperature"
                extra = sql.SQL("")
            elif has_device_logs:
                table = "device_logs"
                ts_col = "logged_at"
                val_col = "value"
                extra = sql.SQL("AND log_type = 'temperature'")
            else:
                return {"mobile_id": mobile_id, "bucket": bucket, "days": days, "count": 0, "items": []}

            q = sql.SQL("""
                SELECT
                  date_trunc(%s, {ts}) AS t,
                  AVG({val})::double precision AS temperature
                FROM {tbl}
                WHERE did = %s
                  {extra}
                  AND {ts} >= NOW() - (%s || ' days')::interval
                GROUP BY 1
                ORDER BY 1;
            """).format(
                ts=sql.Identifier(ts_col),
                val=sql.Identifier(val_col),
                tbl=sql.Identifier("public", table),
                extra=extra,
            )

            cur.execute(q, (bucket, did, days))
            rows = cur.fetchall() or []

    items = [{"t": r[0].isoformat(), "temperature": float(r[1])} for r in rows]
    return {"mobile_id": mobile_id, "bucket": bucket, "days": days, "count": len(items), "items": items}


@app.post("/device/{mobile_id}/daily_update")
def set_device_daily_count(mobile_id: str, body: DeviceDailyCountUpdateIn):
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Device not found")
                did = row[0]
            
            _check_and_reset_counters(conn, did)
            
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.device SET daily_count = %s, updated_at = NOW()
                    WHERE id = %s RETURNING daily_count;
                """, (body.daily_count, did))
                count = cur.fetchone()[0]
            
            conn.commit()
            return {"mobile_id": mobile_id, "daily_count": int(count)}
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


@app.post("/device/{mobile_id}/monthly_update")
def set_device_monthly_count(mobile_id: str, body: DeviceMonthlyCountUpdateIn):
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Device not found")
                did = row[0]
            
            _check_and_reset_counters(conn, did)
            
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.device SET monthly_count = %s, updated_at = NOW()
                    WHERE id = %s RETURNING monthly_count;
                """, (body.monthly_count, did))
                count = cur.fetchone()[0]
            
            conn.commit()
            return {"mobile_id": mobile_id, "monthly_count": int(count)}
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


@app.post("/device/{mobile_id}/door_open")
def record_door_open(mobile_id: str):
    """Record a door open event - increments both daily and monthly counts, logs the event."""
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Device not found")
                did = row[0]
            
            # Check and reset counters first
            _check_and_reset_counters(conn, did)
            
            with conn.cursor() as cur:
                # Increment both counters
                cur.execute("""
                    UPDATE public.device 
                    SET daily_count = daily_count + 1,
                        monthly_count = monthly_count + 1,
                        updated_at = NOW()
                    WHERE id = %s
                    RETURNING daily_count, monthly_count;
                """, (did,))
                daily, monthly = cur.fetchone()
                
                # Log the door open event
                _log_event(conn, did, 'door_open', 1)
            
            conn.commit()
            return {"mobile_id": mobile_id, "daily_count": daily, "monthly_count": monthly}
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


# ---------- Link CRUD ----------
@app.post("/link", response_model=LinkOut)
def create_link(req: LinkCreate):
    with pg_conn() as conn:
        try:
            row = create_link_by_names(conn, req)
            conn.commit()
            return row
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


@app.get("/link/{link_id}", response_model=LinkOut)
def get_link(link_id: int = Path(..., ge=1)):
    with pg_conn() as conn:
        row = fetch_link_by_id(conn, link_id)
        if not row:
            raise HTTPException(status_code=404, detail="Link not found")
        return row


@app.get("/links")
def list_links_route(
    mobile_id: Optional[str] = Query(None),
    video_name: Optional[str] = Query(None),
    shop_name: Optional[str] = Query(None),
    gname: Optional[str] = Query(None),
    did: Optional[int] = Query(None, ge=1),
    vid: Optional[int] = Query(None, ge=1),
    sid: Optional[int] = Query(None, ge=1),
    gid: Optional[int] = Query(None, ge=1),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    user: Dict = Depends(get_current_user),
):
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id")
    with pg_conn() as conn:
        rows = list_links(conn, mobile_id, video_name, shop_name, gname, did, vid, sid, gid, limit, offset, tenant_id=tenant_id)
        return {"count": len(rows), "items": rows, "limit": limit, "offset": offset}


@app.delete("/link/{link_id}")
def delete_link_route(link_id: int = Path(..., ge=1)):
    with pg_conn() as conn:
        try:
            row = fetch_link_by_id(conn, link_id)
            if not row:
                raise HTTPException(status_code=404, detail="Link not found")
            did = int(row["did"])
            deleted = delete_link_row(conn, link_id)
            if deleted == 0:
                raise HTTPException(status_code=404, detail="Link not found")
            total, done, all_done = _aggregate_from_links(conn, did)
            _write_device_flag(conn, did, all_done)
            conn.commit()
            return {"deleted_count": deleted, "device_download_status": all_done, "total_links": total, "downloaded_count": done}
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


@app.post("/link/{link_id}/delete")
def delete_link_route_fallback(link_id: int = Path(..., ge=1)):
    return delete_link_route(link_id)


@app.delete("/device/{mobile_id}/links")
def delete_all_device_links(mobile_id: str):
    """Delete all links for a device. Used before deleting the device itself."""
    with pg_conn() as conn:
        try:
            did = get_device_id_by_mobile(conn, mobile_id)
            if did is None:
                raise HTTPException(status_code=404, detail=f"Device not found: {mobile_id}")
            
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM public.device_video_shop_group WHERE did = %s RETURNING id;",
                    (did,)
                )
                deleted_count = cur.rowcount or 0
                
                # Reset device download status
                cur.execute(
                    "UPDATE public.device SET download_status = false WHERE id = %s;",
                    (did,)
                )
            
            conn.commit()
            return {
                "mobile_id": mobile_id,
                "deleted_count": deleted_count,
                "message": f"Deleted {deleted_count} link(s) for device {mobile_id}"
            }
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))


@app.post("/device/{mobile_id}/links/delete")
def delete_all_device_links_fallback(mobile_id: str):
    """Fallback POST endpoint for delete all links."""
    return delete_all_device_links(mobile_id)


# Link a device to a group - inherits all videos from the group
class DeviceToGroupIn(BaseModel):
    mobile_id: str
    gname: str
    shop_name: str

@app.post("/link/device-to-group")
def link_device_to_group(body: DeviceToGroupIn):
    """
    Link a device to a group. This will:
    1. Create/update a device_assignment record (always, regardless of videos)
    2. Create links for all videos currently in the group (if any)
    A device can only belong to ONE group - changing groups will automatically remove old assignment.
    """
    try:
        with pg_conn() as conn:
            # Get device ID
            did = get_device_id_by_mobile(conn, body.mobile_id)
            if did is None:
                raise HTTPException(status_code=404, detail=f"Device not found: {body.mobile_id}")
            
            with conn.cursor() as cur:
                # Get shop ID
                cur.execute("SELECT id FROM public.shop WHERE shop_name = %s ORDER BY id DESC LIMIT 1;", (body.shop_name,))
                srow = cur.fetchone()
                if not srow:
                    raise HTTPException(status_code=404, detail=f"Shop not found: {body.shop_name}")
                sid = int(srow[0])
                
                # Get group ID
                cur.execute('SELECT id FROM public."group" WHERE gname = %s ORDER BY id DESC LIMIT 1;', (body.gname,))
                grow = cur.fetchone()
                if not grow:
                    raise HTTPException(status_code=404, detail=f"Group not found: {body.gname}")
                gid = int(grow[0])
                
                # Check if device is already in another group (for info message)
                cur.execute("""
                    SELECT da.gid, g.gname 
                    FROM public.device_assignment da
                    LEFT JOIN public."group" g ON g.id = da.gid
                    WHERE da.did = %s AND da.gid IS NOT NULL AND da.gid != %s;
                """, (did, gid))
                existing = cur.fetchone()
                old_group_name = existing[1] if existing else None
                
                # Create/update device_assignment record (this tracks device-group-shop independently)
                # ON CONFLICT will update the existing record, effectively changing the group
                cur.execute("""
                    INSERT INTO public.device_assignment (did, gid, sid)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (did) DO UPDATE SET gid = %s, sid = %s, updated_at = NOW()
                    RETURNING id;
                """, (did, gid, sid, gid, sid))
                
                # Get all videos linked to this group from group_video table
                cur.execute("""
                    SELECT v.id, v.video_name
                    FROM public.group_video gv
                    JOIN public.video v ON v.id = gv.vid
                    WHERE gv.gid = %s
                    ORDER BY gv.display_order;
                """, (gid,))
                group_videos = cur.fetchall() or []
                
                # If no videos in group_video table, check device_video_shop_group (legacy/other devices)
                if not group_videos:
                    cur.execute("""
                        SELECT DISTINCT v.id, v.video_name
                        FROM public.device_video_shop_group l
                        JOIN public.video v ON v.id = l.vid
                        WHERE l.gid = %s;
                    """, (gid,))
                    group_videos = cur.fetchall() or []
                    
                    # If we found videos from device_video_shop_group, also add them to group_video 
                    # so future device assignments will find them
                    if group_videos:
                        for idx, (vid, vname) in enumerate(group_videos):
                            cur.execute("""
                                INSERT INTO public.group_video (gid, vid, display_order)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (gid, vid) DO NOTHING;
                            """, (gid, vid, idx))
                
                # Remove any old links for this device
                cur.execute("DELETE FROM public.device_video_shop_group WHERE did = %s;", (did,))
                deleted_count = cur.rowcount or 0
                
                # Create links for each video in the group (if any)
                created_links = []
                if group_videos:
                    for vid, video_name in group_videos:
                        cur.execute("""
                            INSERT INTO public.device_video_shop_group (did, vid, sid, gid)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT DO NOTHING;
                        """, (did, vid, sid, gid))
                        if cur.rowcount > 0:
                            cur.execute("SELECT id FROM public.device_video_shop_group WHERE did=%s AND vid=%s AND sid=%s AND gid=%s ORDER BY id DESC LIMIT 1;", (did, vid, sid, gid))
                        else:
                            cur.execute("""
                                UPDATE public.device_video_shop_group SET updated_at = NOW()
                                WHERE did=%s AND vid=%s AND sid=%s AND gid=%s RETURNING id;
                            """, (did, vid, sid, gid))
                        link_row = cur.fetchone()
                        if link_row:
                            created_links.append({"link_id": link_row[0], "video_name": video_name})
                
                # Mark device for re-download
                cur.execute("UPDATE public.device SET download_status = FALSE, updated_at = NOW() WHERE id = %s;", (did,))
                
                conn.commit()
                
                message = f"Device linked to group '{body.gname}'"
                if old_group_name:
                    message = f"Device moved from group '{old_group_name}' to '{body.gname}'"
                if not group_videos:
                    message += " (no videos in group yet)"
                
                return {
                    "message": message,
                    "device_id": body.mobile_id,
                    "group": body.gname,
                    "shop": body.shop_name,
                    "old_group": old_group_name,
                    "links_created": len(created_links),
                    "links_deleted": deleted_count,
                    "videos_in_group": len(group_videos),
                    "videos": [l["video_name"] for l in created_links]
                }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


class GroupSyncIn(BaseModel):
    source_mobile_id: str  # The device to copy settings/videos FROM
    layout_mode: Optional[str] = None
    layout_config: Optional[str] = None


@app.post("/group/{gname}/sync-to-devices")
def sync_group_devices(gname: str, body: GroupSyncIn):
    """
    Sync videos and layout settings from a source device to ALL devices in the group.
    This ensures all devices in the group have the same videos and layout.
    """
    try:
        with pg_conn() as conn:
            with conn.cursor() as cur:
                # Get group ID
                cur.execute('SELECT id FROM public."group" WHERE gname = %s ORDER BY id DESC LIMIT 1;', (gname,))
                grow = cur.fetchone()
                if not grow:
                    raise HTTPException(status_code=404, detail=f"Group not found: {gname}")
                gid = int(grow[0])
                
                # Get source device ID and its shop
                cur.execute("""
                    SELECT d.id, l.sid
                    FROM public.device d
                    JOIN public.device_video_shop_group l ON l.did = d.id
                    WHERE d.mobile_id = %s AND l.gid = %s
                    LIMIT 1;
                """, (body.source_mobile_id, gid))
                source_row = cur.fetchone()
                if not source_row:
                    raise HTTPException(status_code=404, detail=f"Source device not found in group: {body.source_mobile_id}")
                source_did = int(source_row[0])
                source_sid = int(source_row[1])
                
                # Get all videos linked to source device in this group (with their settings)
                cur.execute("""
                    SELECT l.vid, l.display_order, l.device_rotation, l.device_resolution, l.grid_position, l.grid_size
                    FROM public.device_video_shop_group l
                    WHERE l.did = %s AND l.gid = %s
                    ORDER BY l.display_order ASC, l.id ASC;
                """, (source_did, gid))
                source_videos = cur.fetchall()
                
                if not source_videos:
                    raise HTTPException(status_code=400, detail=f"Source device has no videos linked in this group")
                
                # Get all other devices in the same group
                cur.execute("""
                    SELECT DISTINCT d.id, d.mobile_id, l.sid
                    FROM public.device d
                    JOIN public.device_video_shop_group l ON l.did = d.id
                    WHERE l.gid = %s AND d.id != %s;
                """, (gid, source_did))
                other_devices = cur.fetchall()
                
                results = {
                    "source_device": body.source_mobile_id,
                    "group": gname,
                    "videos_count": len(source_videos),
                    "devices_updated": 0,
                    "links_created": 0,
                    "links_updated": 0,
                    "layouts_applied": 0,
                    "device_results": []
                }
                
                for target_did, target_mobile_id, target_sid in other_devices:
                    device_result = {
                        "mobile_id": target_mobile_id,
                        "links_created": 0,
                        "links_updated": 0,
                        "layout_applied": False
                    }
                    
                    # For each video from source, create/update link on target device
                    for vid, display_order, device_rotation, device_resolution, grid_position, grid_size in source_videos:
                        cur.execute("""
                            INSERT INTO public.device_video_shop_group 
                                (did, vid, sid, gid, display_order, device_rotation, device_resolution, grid_position, grid_size)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING;
                        """, (target_did, vid, target_sid, gid, display_order, device_rotation, device_resolution, grid_position, grid_size))
                        inserted = cur.rowcount > 0
                        if not inserted:
                            cur.execute("""
                                UPDATE public.device_video_shop_group SET 
                                    display_order = %s, device_rotation = %s, device_resolution = %s,
                                    grid_position = %s, grid_size = %s, updated_at = NOW()
                                WHERE did = %s AND vid = %s AND sid = %s AND gid = %s
                                RETURNING id;
                            """, (display_order, device_rotation, device_resolution, grid_position, grid_size,
                                  target_did, vid, target_sid, gid))
                        link_row = True
                        if link_row:
                            if inserted:
                                device_result["links_created"] += 1
                                results["links_created"] += 1
                            else:
                                device_result["links_updated"] += 1
                                results["links_updated"] += 1
                    
                    # Apply layout settings if provided
                    if body.layout_mode:
                        cur.execute("""
                            INSERT INTO public.device_layout (did, layout_mode, layout_config)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (did) DO UPDATE SET 
                                layout_mode = EXCLUDED.layout_mode,
                                layout_config = EXCLUDED.layout_config,
                                updated_at = NOW();
                        """, (target_did, body.layout_mode, body.layout_config))
                        device_result["layout_applied"] = True
                        results["layouts_applied"] += 1
                    
                    # Mark device for re-download
                    cur.execute("UPDATE public.device SET download_status = FALSE, updated_at = NOW() WHERE id = %s;", (target_did,))
                    
                    results["devices_updated"] += 1
                    results["device_results"].append(device_result)
                
                conn.commit()
                
                return results
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


# ---------- Device video endpoints ----------
@app.get("/device/{mobile_id}/video", response_model=LinkOut)
def get_latest_video_for_device(mobile_id: str):
    with pg_conn() as conn:
        rows = fetch_links_for_mobile(conn, mobile_id, limit=1, offset=0)
        if not rows:
            raise HTTPException(status_code=404, detail="No video linked to this device")
        return rows[0]


@app.get("/device/{mobile_id}/videos")
def list_videos_for_device(mobile_id: str, limit: int = Query(200, ge=1, le=1000), offset: int = Query(0, ge=0)):
    with pg_conn() as conn:
        rows = fetch_links_for_mobile(conn, mobile_id, limit, offset)
        return {"count": len(rows), "items": rows, "limit": limit, "offset": offset, "mobile_id": mobile_id}


@app.get("/device/{mobile_id}/videos/downloads", response_model=PresignedURLListOut)
def list_download_urls_for_device(mobile_id: str, limit: int = Query(200, ge=1, le=1000), offset: int = Query(0, ge=0)):
    with pg_conn() as conn:
        rows = fetch_links_for_mobile(conn, mobile_id, limit, offset)
        
        # Get device layout
        layout_mode = "single"
        layout_config = None
        with conn.cursor() as cur:
            cur.execute("""
                SELECT dl.layout_mode, dl.layout_config 
                FROM public.device_layout dl
                JOIN public.device d ON d.id = dl.did
                WHERE d.mobile_id = %s LIMIT 1;
            """, (mobile_id,))
            layout_row = cur.fetchone()
            if layout_row:
                layout_mode = layout_row[0] or "single"
                layout_config = layout_row[1]
        
        # Parse layout_config to get content slots (both videos and images)
        # json imported at top
        config_slots = []
        video_slots = {}  # video_name -> slot_info
        ad_slots = {}     # ad_name -> slot_info
        if layout_config:
            try:
                config = json.loads(layout_config) if isinstance(layout_config, str) else layout_config
                if isinstance(config, list):
                    config_slots = config
                    for slot in config:
                        if slot.get("ad_name") and slot.get("content_type") == "image":
                            ad_slots[slot["ad_name"]] = slot
                        elif slot.get("video_name"):
                            video_slots[slot["video_name"]] = slot
            except:
                pass
        
        # Fetch advertisements if there are any in the layout
        ad_items = []
        if ad_slots:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, ad_name, s3_link, rotation, fit_mode, display_duration
                    FROM public.advertisement
                    WHERE ad_name = ANY(%s);
                """, (list(ad_slots.keys()),))
                for ad_row in cur.fetchall():
                    ad_name = ad_row[1]
                    slot_info = ad_slots.get(ad_name, {})
                    s3_link = ad_row[2]
                    if s3_link:
                        url, filename = ad_presign_get_object(s3_link, AD_PRESIGN_EXPIRES)
                        ad_items.append(PresignedURLItem(
                            link_id=ad_row[0],
                            video_name=ad_name,  # Use ad_name as video_name for compatibility
                            url=url,
                            expires_in=AD_PRESIGN_EXPIRES,
                            filename=filename,
                            rotation=slot_info.get("rotation") or ad_row[3] or 0,
                            content_type="image",
                            fit_mode=slot_info.get("fit_mode") or ad_row[4] or "cover",
                            display_duration=ad_row[5] or 10,
                            display_order=0,
                            resolution=None,
                            device_rotation=slot_info.get("rotation"),
                            device_resolution=None,
                            grid_position=slot_info.get("position", 0),
                            grid_size=None,
                        ))
    
    items = []
    for r in rows:
        if not r.get("s3_link"):
            continue
        
        video_name = r["video_name"]
        slot_info = video_slots.get(video_name, {})
        
        url, filename = presign_get_object(r["s3_link"], PRESIGN_EXPIRES)
        
        # Use slot rotation from layout_config if available, then device-specific, then video defaults
        effective_rotation = slot_info.get("rotation") if slot_info.get("rotation") is not None else (
            r.get("device_rotation") if r.get("device_rotation") is not None else r.get("rotation", 0)
        )
        effective_resolution = r.get("device_resolution") if r.get("device_resolution") else r.get("resolution")
        
        # Use grid_position from layout_config if available
        grid_position = slot_info.get("position", r.get("grid_position", 0))
        
        items.append(PresignedURLItem(
            link_id=r["id"],
            video_name=r["video_name"],
            url=url,
            expires_in=PRESIGN_EXPIRES,
            filename=filename,
            rotation=effective_rotation,
            content_type=r.get("content_type", "video"),
            fit_mode=r.get("fit_mode", "cover"),
            display_duration=r.get("display_duration", 10),
            display_order=r.get("display_order", 0),
            resolution=effective_resolution,
            device_rotation=r.get("device_rotation"),
            device_resolution=r.get("device_resolution"),
            grid_position=grid_position,
            grid_size=r.get("grid_size"),
        ))
    
    # Combine video and advertisement items
    all_items = items + ad_items
    
    # If we have a layout_config, filter and order items according to it
    if config_slots and layout_mode != "single":
        # Build ordered list based on layout_config positions
        ordered_items = []
        for slot in sorted(config_slots, key=lambda s: s.get("position", 0)):
            slot_video_name = slot.get("video_name")
            slot_ad_name = slot.get("ad_name")
            slot_content_type = slot.get("content_type", "video")
            
            if slot_content_type == "image" and slot_ad_name:
                # Find matching advertisement
                matching = [item for item in all_items if item.content_type == "image" and item.video_name == slot_ad_name]
                if matching:
                    ordered_items.append(matching[0])
            elif slot_video_name:
                # Find matching video
                matching = [item for item in all_items if item.video_name == slot_video_name]
                if matching:
                    ordered_items.append(matching[0])
        
        # Use ordered items if we found any, otherwise fall back to all_items sorted
        if ordered_items:
            all_items = ordered_items
        else:
            # Sort by grid_position to maintain layout order
            all_items.sort(key=lambda x: (x.grid_position or 0, x.link_id or 0))
    else:
        # Sort by grid_position to maintain layout order
        all_items.sort(key=lambda x: (x.grid_position or 0, x.link_id or 0))
    
    # For single mode: if layout_config specifies content, return only that content
    if layout_mode == "single" and config_slots:
        first_slot = config_slots[0] if config_slots else {}
        if first_slot.get("content_type") == "image" and first_slot.get("ad_name"):
            # Single mode with image - return only the image
            all_items = [item for item in all_items if item.content_type == "image" and item.video_name == first_slot.get("ad_name")]
        elif first_slot.get("video_name"):
            # Single mode with specific video - return only that video
            all_items = [item for item in all_items if item.video_name == first_slot.get("video_name")]
    
    if not all_items:
        raise HTTPException(status_code=404, detail="No content linked to this device")
    return PresignedURLListOut(mobile_id=mobile_id, items=all_items, count=len(all_items), layout_mode=layout_mode, layout_config=layout_config)


@app.get("/device/{mobile_id}/video/{link_id}/download")
def download_video_for_link(mobile_id: str, link_id: int):
    with pg_conn() as conn:
        row = fetch_link_by_id(conn, link_id)
        if not row or row["mobile_id"] != mobile_id:
            raise HTTPException(status_code=404, detail="Video not found for this device")
        if not row.get("s3_link"):
            raise HTTPException(status_code=404, detail="No s3_link for this video")
    url, _ = presign_get_object(row["s3_link"], PRESIGN_EXPIRES)
    return RedirectResponse(url=url, status_code=302)


# ---------- Download status ----------
@app.get("/device/{mobile_id}/download_status", response_model=DeviceDownloadStatusOut)
def get_device_download_status(mobile_id: str):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, download_status FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
            drow = cur.fetchone()
            if not drow:
                raise HTTPException(status_code=404, detail="Device not found")
            did, dev_flag = int(drow[0]), bool(drow[1])
        total, done, _ = _aggregate_from_links(conn, did)
        return DeviceDownloadStatusOut(mobile_id=mobile_id, download_status=dev_flag, total_links=total, downloaded_count=done)


@app.post("/device/{mobile_id}/download_update", response_model=DeviceDownloadStatusOut)
def set_device_download_flag(mobile_id: str, body: DeviceDownloadStatusSetIn):
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
                drow = cur.fetchone()
                if not drow:
                    raise HTTPException(status_code=404, detail="Device not found")
                did = int(drow[0])
            _write_device_flag(conn, did, bool(body.status))
            total, done, _ = _aggregate_from_links(conn, did)
            conn.commit()
            return DeviceDownloadStatusOut(mobile_id=mobile_id, download_status=bool(body.status), total_links=total, downloaded_count=done)
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


# ---------- Download Progress (Real-time) ----------
@app.post("/device/{mobile_id}/download_progress", response_model=DeviceDownloadProgressOut)
def update_device_download_progress(mobile_id: str, body: DeviceDownloadProgressIn):
    """Update real-time download progress for a device. Called by Android app during downloads."""
    # Store in memory for real-time access
    download_progress_store[mobile_id] = {
        "current_file": body.current_file,
        "total_files": body.total_files,
        "file_name": body.file_name,
        "progress": body.progress,
        "downloaded_bytes": body.downloaded_bytes,
        "total_bytes": body.total_bytes,
        "is_downloading": body.is_downloading,
        "updated_at": datetime.now()
    }
    
    return DeviceDownloadProgressOut(
        mobile_id=mobile_id,
        current_file=body.current_file,
        total_files=body.total_files,
        file_name=body.file_name,
        progress=body.progress,
        downloaded_bytes=body.downloaded_bytes,
        total_bytes=body.total_bytes,
        is_downloading=body.is_downloading,
        updated_at=datetime.now().isoformat()
    )


@app.get("/device/{mobile_id}/download_progress", response_model=DeviceDownloadProgressOut)
def get_device_download_progress(mobile_id: str):
    """Get real-time download progress for a device. Called by web frontend to display progress."""
    progress = download_progress_store.get(mobile_id)
    
    if not progress:
        # No progress data - device might not be downloading
        return DeviceDownloadProgressOut(
            mobile_id=mobile_id,
            current_file=0,
            total_files=0,
            file_name=None,
            progress=0,
            downloaded_bytes=0,
            total_bytes=0,
            is_downloading=False,
            updated_at=None
        )
    
    # Check if the progress is stale (older than 30 seconds means download likely finished or failed)
    updated_at = progress.get("updated_at")
    is_stale = False
    if updated_at:
        age_seconds = (datetime.now() - updated_at).total_seconds()
        is_stale = age_seconds > 30
    
    return DeviceDownloadProgressOut(
        mobile_id=mobile_id,
        current_file=progress.get("current_file", 0),
        total_files=progress.get("total_files", 0),
        file_name=progress.get("file_name"),
        progress=progress.get("progress", 0),
        downloaded_bytes=progress.get("downloaded_bytes", 0),
        total_bytes=progress.get("total_bytes", 0),
        is_downloading=progress.get("is_downloading", False) and not is_stale,
        updated_at=updated_at.isoformat() if updated_at else None
    )


@app.delete("/device/{mobile_id}/download_progress")
def clear_device_download_progress(mobile_id: str):
    """Clear download progress for a device. Called when download completes or is cancelled."""
    if mobile_id in download_progress_store:
        del download_progress_store[mobile_id]
    return {"status": "cleared", "mobile_id": mobile_id}


@app.post("/device/{mobile_id}/download_update", response_model=DeviceDownloadStatusOut)
def set_device_download_flag(mobile_id: str, body: DeviceDownloadStatusSetIn):
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
                drow = cur.fetchone()
                if not drow:
                    raise HTTPException(status_code=404, detail="Device not found")
                did = int(drow[0])
            _write_device_flag(conn, did, bool(body.status))
            total, done, _ = _aggregate_from_links(conn, did)
            conn.commit()
            return DeviceDownloadStatusOut(mobile_id=mobile_id, download_status=bool(body.status), total_links=total, downloaded_count=done)
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


# ---------- Device Layout Management ----------
@app.get("/device/{mobile_id}/layout", response_model=DeviceLayoutOut)
def get_device_layout(mobile_id: str):
    """Get device layout configuration."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT dl.layout_mode, dl.layout_config
                FROM public.device_layout dl
                JOIN public.device d ON d.id = dl.did
                WHERE d.mobile_id = %s LIMIT 1;
            """, (mobile_id,))
            row = cur.fetchone()
            if not row:
                return DeviceLayoutOut(mobile_id=mobile_id, layout_mode="single", layout_config=None)
            return DeviceLayoutOut(mobile_id=mobile_id, layout_mode=row[0], layout_config=row[1])


@app.post("/device/{mobile_id}/layout", response_model=DeviceLayoutOut)
def set_device_layout(mobile_id: str, body: DeviceLayoutIn):
    """Set device layout configuration."""
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
                drow = cur.fetchone()
                if not drow:
                    raise HTTPException(status_code=404, detail="Device not found")
                did = int(drow[0])
                
                cur.execute("""
                    INSERT INTO public.device_layout (did, layout_mode, layout_config)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (did) DO UPDATE SET
                        layout_mode = EXCLUDED.layout_mode,
                        layout_config = EXCLUDED.layout_config,
                        updated_at = NOW()
                    RETURNING layout_mode, layout_config;
                """, (did, body.layout_mode, body.layout_config))
                row = cur.fetchone()
                
                # Reset download_status to trigger re-sync on Android
                cur.execute("UPDATE public.device SET download_status = FALSE WHERE id = %s;", (did,))
                
                conn.commit()
                return DeviceLayoutOut(mobile_id=mobile_id, layout_mode=row[0], layout_config=row[1])
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))


# ---------- Link-level settings (per-device rotation, resolution, grid) ----------
@app.put("/link/{link_id}/settings")
def update_link_settings(link_id: int, body: LinkUpdateIn):
    """Update per-device settings for a specific link (rotation, resolution, grid position)."""
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                # Get device ID for this link
                cur.execute("SELECT id, did FROM public.device_video_shop_group WHERE id = %s LIMIT 1;", (link_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Link not found")
                did = row[1]
                
                updates = []
                params = []
                
                if body.device_rotation is not None:
                    updates.append("device_rotation = %s")
                    params.append(body.device_rotation)
                
                if body.device_resolution is not None:
                    updates.append("device_resolution = %s")
                    params.append(body.device_resolution)
                
                if body.grid_position is not None:
                    updates.append("grid_position = %s")
                    params.append(body.grid_position)
                
                if body.grid_size is not None:
                    updates.append("grid_size = %s")
                    params.append(body.grid_size)
                
                if body.display_order is not None:
                    updates.append("display_order = %s")
                    params.append(body.display_order)
                
                if updates:
                    updates.append("updated_at = NOW()")
                    params.append(link_id)
                    cur.execute(f"""
                        UPDATE public.device_video_shop_group 
                        SET {', '.join(updates)}
                        WHERE id = %s
                        RETURNING id;
                    """, tuple(params))
                    
                    # Mark device as needing refresh so Android picks up the change
                    cur.execute("""
                        UPDATE public.device SET download_status = FALSE, updated_at = NOW() WHERE id = %s;
                    """, (did,))
                
                conn.commit()
                return {"link_id": link_id, "updated": True}
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))

# ADD THIS ENDPOINT TO device_video_shop_group.py
# Insert BEFORE the "# ---------- Device Resolution ----------" section (around line 1772)
# This endpoint is called by Android app to poll for rotation/layout changes every 10 seconds

# ---------- Device Rotation Polling (for Android app) ----------
@app.get("/device/{mobile_id}/rotation")
def get_device_rotation_settings(mobile_id: str):
    """
    Get rotation and grid settings for all videos/images on a device.
    Called by Android app to poll for rotation/layout changes.
    Returns format: { rotations: [...], layout_mode: "..." }
    """
    with pg_conn() as conn:
        rows = fetch_links_for_mobile(conn, mobile_id, limit=100, offset=0)
        
        # Get layout mode from device_layout table
        layout_mode = "single"
        layout_config = None
        with conn.cursor() as cur:
            cur.execute("""
                SELECT dl.layout_mode, dl.layout_config 
                FROM public.device_layout dl
                JOIN public.device d ON d.id = dl.did
                WHERE d.mobile_id = %s LIMIT 1;
            """, (mobile_id,))
            layout_row = cur.fetchone()
            if layout_row:
                layout_mode = layout_row[0] or "single"
                layout_config = layout_row[1]
        
        # Parse layout_config to get slot information
        # json imported at top
        config_slots = []
        video_slots = {}  # video_name -> slot_info
        ad_slots = {}     # ad_name -> slot_info
        if layout_config:
            try:
                config = json.loads(layout_config) if isinstance(layout_config, str) else layout_config
                if isinstance(config, list):
                    config_slots = config
                    for slot in config:
                        if slot.get("ad_name") and slot.get("content_type") == "image":
                            ad_slots[slot["ad_name"]] = slot
                        elif slot.get("video_name"):
                            video_slots[slot["video_name"]] = slot
            except:
                pass
        
        rotations = []
        
        # Add video rotations
        for r in rows:
            video_name = r.get("video_name")
            slot_info = video_slots.get(video_name, {})
            
            # Use slot rotation from layout_config if available, then device_rotation, then video rotation
            effective_rotation = slot_info.get("rotation") if slot_info.get("rotation") is not None else (
                r.get("device_rotation") if r.get("device_rotation") is not None else r.get("rotation", 0)
            )
            
            # Parse grid_size if it's a JSON string
            grid_size = r.get("grid_size")
            grid_width = 100
            grid_height = 100
            if grid_size:
                try:
                    if isinstance(grid_size, str):
                        gs = json.loads(grid_size)
                        grid_width = gs.get("w", 100)
                        grid_height = gs.get("h", 100)
                    elif isinstance(grid_size, dict):
                        grid_width = grid_size.get("w", 100)
                        grid_height = grid_size.get("h", 100)
                except:
                    pass
            
            # Get filename from s3_link
            s3_link = r.get("s3_link", "")
            filename = s3_link.split("/")[-1] if s3_link else r.get("video_name", "")
            
            # Use grid_position from layout_config if available
            grid_position = slot_info.get("position", r.get("grid_position", 1))
            
            rotations.append({
                "filename": filename,
                "video_name": video_name,
                "rotation": effective_rotation,
                "fit_mode": r.get("fit_mode", "cover"),
                "grid_position": grid_position,
                "grid_width": grid_width,
                "grid_height": grid_height,
                "content_type": r.get("content_type", "video"),
            })
        
        # Add image/advertisement rotations from layout_config
        if ad_slots:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, ad_name, s3_link, rotation, fit_mode
                    FROM public.advertisement
                    WHERE ad_name = ANY(%s);
                """, (list(ad_slots.keys()),))
                for ad_row in cur.fetchall():
                    ad_name = ad_row[1]
                    slot_info = ad_slots.get(ad_name, {})
                    s3_link = ad_row[2] or ""
                    filename = s3_link.split("/")[-1] if s3_link else ad_name
                    
                    rotations.append({
                        "filename": filename,
                        "video_name": ad_name,
                        "rotation": slot_info.get("rotation") or ad_row[3] or 0,
                        "fit_mode": slot_info.get("fit_mode") or ad_row[4] or "cover",
                        "grid_position": slot_info.get("position", 0),
                        "grid_width": 100,
                        "grid_height": 100,
                        "content_type": "image",
                    })
        
        # Sort by grid_position
        rotations.sort(key=lambda x: (x.get("grid_position", 0), x.get("video_name", "")))
        
        # For single mode: if layout_config specifies content, return only that content
        if layout_mode == "single" and config_slots:
            first_slot = config_slots[0] if config_slots else {}
            if first_slot.get("content_type") == "image" and first_slot.get("ad_name"):
                # Single mode with image - return only the image
                rotations = [r for r in rotations if r.get("content_type") == "image" and r.get("video_name") == first_slot.get("ad_name")]
            elif first_slot.get("video_name"):
                # Single mode with specific video - return only that video
                rotations = [r for r in rotations if r.get("video_name") == first_slot.get("video_name")]
        
        return {
            "mobile_id": mobile_id,
            "rotations": rotations,
            "layout_mode": layout_mode,
            "layout_config": layout_config,
        }

# ---------- Device Resolution ----------
@app.get("/device/{mobile_id}/resolution")
def get_device_resolution(mobile_id: str):
    """Get device screen resolution."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT resolution FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            return {"mobile_id": mobile_id, "resolution": row[0]}


@app.post("/device/{mobile_id}/resolution")
def set_device_resolution(mobile_id: str, resolution: str = Query(None)):
    """Set device screen resolution."""
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE public.device SET resolution = %s WHERE mobile_id = %s RETURNING id;", (resolution, mobile_id))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Device not found")
                conn.commit()
                return {"mobile_id": mobile_id, "resolution": resolution}
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))


@app.post("/device/{mobile_id}/name")
def set_device_name(mobile_id: str, body: DeviceNameUpdateIn):
    """Set/update device friendly name."""
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE public.device SET device_name = %s WHERE mobile_id = %s RETURNING id, device_name;", (body.device_name, mobile_id))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Device not found")
                conn.commit()
                return {"mobile_id": mobile_id, "device_name": row[1]}
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/device/{mobile_id}/name")
def get_device_name(mobile_id: str):
    """Get device friendly name."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT device_name FROM public.device WHERE mobile_id = %s ORDER BY id DESC LIMIT 1;", (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            return {"mobile_id": mobile_id, "device_name": row[0]}


@app.post("/device/{mobile_id}/grid_layout")
def set_device_grid_layout(mobile_id: str, body: Dict[str, Any]):
    """
    Set grid layout for all videos on a device.
    body: {
        "layout_mode": "split_v",  # or "split_h", "grid_3", "grid_4", "grid_1x4", "single"
        "items": [{"link_id": 1, "grid_position": 1, "grid_size": {"w": 50, "h": 50}}, ...]
    }
    Also supports legacy format: [{"link_id": 1, ...}, ...] (array directly)
    """
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
                drow = cur.fetchone()
                if not drow:
                    raise HTTPException(status_code=404, detail="Device not found")
                did = int(drow[0])
                
                # Handle both new format (object with layout_mode) and legacy format (array)
                if isinstance(body, list):
                    # Legacy format - array of items
                    layout = body
                    layout_mode = None
                else:
                    # New format - object with layout_mode and items
                    layout = body.get("items", [])
                    layout_mode = body.get("layout_mode")
                
                # If layout_mode not provided, guess based on number of items (legacy behavior)
                if not layout_mode:
                    num_items = len(layout)
                    if num_items <= 1:
                        layout_mode = "single"
                    elif num_items == 2:
                        layout_mode = "split_h"
                    elif num_items == 3:
                        layout_mode = "grid_3"
                    else:
                        layout_mode = "grid_4"
                
                # Update device layout
                cur.execute("""
                    INSERT INTO public.device_layout (did, layout_mode, layout_config)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (did) DO UPDATE SET
                        layout_mode = EXCLUDED.layout_mode,
                        layout_config = EXCLUDED.layout_config,
                        updated_at = NOW();
                """, (did, layout_mode, None))
                
                # Mark device as needing refresh (download_status = false triggers Android to re-fetch)
                cur.execute("""
                    UPDATE public.device SET download_status = FALSE, updated_at = NOW() WHERE id = %s;
                """, (did,))
                
                # Update each link's grid settings
                for item in layout:
                    link_id = item.get("link_id")
                    grid_position = item.get("grid_position", 0)
                    grid_size = item.get("grid_size")
                    display_order = item.get("display_order", 0)
                    device_rotation = item.get("device_rotation")
                    device_resolution = item.get("device_resolution")
                    
                    if link_id:
                        # json imported at top
                        grid_size_str = json.dumps(grid_size) if grid_size else None
                        cur.execute("""
                            UPDATE public.device_video_shop_group
                            SET grid_position = %s, grid_size = %s, display_order = %s,
                                device_rotation = COALESCE(%s, device_rotation),
                                device_resolution = COALESCE(%s, device_resolution),
                                updated_at = NOW()
                            WHERE id = %s AND did = %s;
                        """, (grid_position, grid_size_str, display_order, device_rotation, device_resolution, link_id, did))
                
                conn.commit()
                return {"mobile_id": mobile_id, "layout_mode": layout_mode, "items_updated": len(layout)}
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))


# ---------- Online status with 1-minute threshold ----------
@app.get("/device/{mobile_id}/online", response_model=DeviceOnlineStatusOut)
def get_device_online_status(mobile_id: str):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, is_online, last_online_at,
                       EXTRACT(EPOCH FROM (NOW() - last_online_at)) as seconds_ago,
                       is_active
                FROM public.device WHERE mobile_id = %s LIMIT 1;
            """, (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            
            did = row[0]
            is_online = row[1]
            seconds_ago = row[3]
            is_active = row[4] if row[4] is not None else True
            
            # If device is inactive, return 404 to show "not enrolled" screen on device
            if not is_active:
                raise HTTPException(status_code=404, detail="Device is deactivated")
            
            # If last heartbeat was more than threshold seconds ago, mark offline
            if seconds_ago is not None and seconds_ago > ONLINE_THRESHOLD_SECONDS and is_online:
                is_online = False
                cur.execute("UPDATE public.device SET is_online = FALSE WHERE mobile_id = %s;", (mobile_id,))
                # Log the offline event
                cur.execute("""
                    INSERT INTO public.device_online_history (did, event_type, event_at)
                    VALUES (%s, 'offline', NOW());
                """, (did,))
                conn.commit()
            
            return DeviceOnlineStatusOut(mobile_id=mobile_id, is_online=bool(is_online))


@app.post("/device/{mobile_id}/online_update")
def set_device_online_status(mobile_id: str, body: DeviceOnlineUpdateIn):
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                # Get current status first, including is_active and needs_refresh
                cur.execute("""
                    SELECT id, is_online, is_active, 
                           COALESCE(needs_refresh, FALSE) as needs_refresh,
                           download_status
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
                
                # If device is deactivated, return response with is_active=false and force_refresh=true
                # This tells the app to show enrollment screen
                if not is_active:
                    # Clear the needs_refresh flag since we're handling it
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
                
                # Update the device status and clear needs_refresh flag
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


# ---------- Shop Devices Lookup ----------
@app.get("/shop/{shop_name}/devices")
def get_shop_devices(shop_name: str):
    """Get all devices associated with this shop."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            # First get shop ID
            cur.execute("SELECT id FROM public.shop WHERE shop_name = %s LIMIT 1;", (shop_name,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Shop not found")
            sid = row[0]
            
            # Get devices from device_assignment table
            cur.execute("""
                SELECT DISTINCT d.id, d.mobile_id, d.device_name, g.gname
                FROM public.device_assignment da
                JOIN public.device d ON d.id = da.did
                LEFT JOIN public."group" g ON g.id = da.gid
                WHERE da.sid = %s
                ORDER BY d.device_name, d.mobile_id;
            """, (sid,))
            devices = [{"id": r[0], "mobile_id": r[1], "device_name": r[2], "group_name": r[3]} for r in cur.fetchall()]
            
            # Also check device_video_shop_group for legacy links
            cur.execute("""
                SELECT DISTINCT d.id, d.mobile_id, d.device_name, g.gname
                FROM public.device_video_shop_group l
                JOIN public.device d ON d.id = l.did
                LEFT JOIN public."group" g ON g.id = l.gid
                WHERE l.sid = %s
                ORDER BY d.device_name, d.mobile_id;
            """, (sid,))
            legacy_devices = [{"id": r[0], "mobile_id": r[1], "device_name": r[2], "group_name": r[3]} for r in cur.fetchall()]
            
            # Merge devices (dedup by id)
            device_ids = {d["id"] for d in devices}
            for ld in legacy_devices:
                if ld["id"] not in device_ids:
                    devices.append(ld)
            
            return {
                "shop_name": shop_name,
                "devices": devices,
                "device_count": len(devices)
            }


# ---------- Device creation with group/shop linking ----------
@app.post("/device/create")
def create_device_with_linking(body: DeviceCreateIn, user: Dict = Depends(get_current_user)):
    """Create a new device and optionally link to group and shop."""
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id") or 1
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                # Check if device exists within tenant
                cur.execute("SELECT id FROM public.device WHERE mobile_id = %s AND tenant_id = %s ORDER BY id DESC LIMIT 1;", (body.mobile_id, tenant_id))
                existing = cur.fetchone()
                
                if existing:
                    did = existing[0]
                    # Update resolution and device_name if provided
                    updates = []
                    params = []
                    if body.resolution:
                        updates.append("resolution = %s")
                        params.append(body.resolution)
                    if body.device_name:
                        updates.append("device_name = %s")
                        params.append(body.device_name)
                    if updates:
                        params.append(did)
                        cur.execute(f"UPDATE public.device SET {', '.join(updates)} WHERE id = %s;", params)
                else:
                    # Create new device with resolution and device_name
                    cur.execute("""
                        INSERT INTO public.device (mobile_id, download_status, is_online, last_online_at, resolution, device_name, tenant_id)
                        VALUES (%s, FALSE, FALSE, NOW(), %s, %s, %s)
                        RETURNING id;
                    """, (body.mobile_id, body.resolution, body.device_name, tenant_id))
                    did = cur.fetchone()[0]
                
                result = {
                    "device_id": did,
                    "mobile_id": body.mobile_id,
                    "device_name": body.device_name,
                    "created": not bool(existing),
                    "linked_to_group": False,
                    "linked_to_shop": False,
                    "gname": None,
                    "shop_name": None,
                    "resolution": body.resolution,
                    "videos_linked": 0,
                }
                
                # If group and shop provided, create links for ALL videos in the group
                if body.group_name and body.shop_name:
                    cur.execute('SELECT id FROM public."group" WHERE gname = %s ORDER BY id DESC LIMIT 1;', (body.group_name,))
                    grow = cur.fetchone()
                    if not grow:
                        conn.rollback()
                        raise HTTPException(status_code=404, detail=f"Group not found: {body.group_name}")
                    gid = grow[0]
                    
                    cur.execute("SELECT id FROM public.shop WHERE shop_name = %s ORDER BY id DESC LIMIT 1;", (body.shop_name,))
                    srow = cur.fetchone()
                    if not srow:
                        conn.rollback()
                        raise HTTPException(status_code=404, detail=f"Shop not found: {body.shop_name}")
                    sid = srow[0]
                    
                    # Create device_assignment record
                    cur.execute("""
                        INSERT INTO public.device_assignment (did, gid, sid)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (did) DO UPDATE SET gid = %s, sid = %s, updated_at = NOW();
                    """, (did, gid, sid, gid, sid))
                    
                    # Get ALL videos from group_video table (primary source)
                    cur.execute("""
                        SELECT v.id, v.video_name
                        FROM public.group_video gv
                        JOIN public.video v ON v.id = gv.vid
                        WHERE gv.gid = %s
                        ORDER BY gv.display_order;
                    """, (gid,))
                    group_videos = cur.fetchall() or []
                    
                    # If no videos in group_video, check device_video_shop_group (legacy)
                    if not group_videos:
                        cur.execute("""
                            SELECT DISTINCT v.id, v.video_name
                            FROM public.device_video_shop_group dvsg
                            JOIN public.video v ON v.id = dvsg.vid
                            WHERE dvsg.gid = %s;
                        """, (gid,))
                        group_videos = cur.fetchall() or []
                        
                        # Backfill to group_video table
                        if group_videos:
                            for idx, (vid, vname) in enumerate(group_videos):
                                cur.execute("""
                                    INSERT INTO public.group_video (gid, vid, display_order)
                                    VALUES (%s, %s, %s)
                                    ON CONFLICT (gid, vid) DO NOTHING;
                                """, (gid, vid, idx))
                    
                    # Remove any existing links for this device (in case of update)
                    cur.execute("DELETE FROM public.device_video_shop_group WHERE did = %s;", (did,))
                    
                    # Create links for ALL videos in the group
                    videos_linked = 0
                    for vid, video_name in group_videos:
                        cur.execute("""
                            INSERT INTO public.device_video_shop_group (did, vid, sid, gid)
                            VALUES (%s, %s, %s, %s)
                            RETURNING id;
                        """, (did, vid, sid, gid))
                        if cur.fetchone():
                            videos_linked += 1
                    
                    # Mark device for download
                    cur.execute("UPDATE public.device SET download_status = FALSE, updated_at = NOW() WHERE id = %s;", (did,))
                    
                    result["linked_to_group"] = True
                    result["linked_to_shop"] = True
                    result["gname"] = body.group_name
                    result["shop_name"] = body.shop_name
                    result["videos_linked"] = videos_linked
            
            conn.commit()
            return result
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            import traceback
            tb = traceback.format_exc()
            print(f"[DEVICE CREATE ERROR] {str(e)}\n{tb}", flush=True)
            return JSONResponse(
                status_code=500,
                content={"detail": f"Device create failed: {str(e)}", "traceback": tb.split("\n")[-6:]},
                headers={"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "*", "Access-Control-Allow-Headers": "*"},
            )


# ---------- Group video endpoints ----------
@app.post("/group/{gname}/video", response_model=GroupVideoUpdateOut)
def set_group_video_by_name(gname: str, body: GroupVideoUpdateIn):
    with pg_conn() as conn:
        try:
            if gname.strip().lower() in NO_GROUP_SENTINELS:
                vid = _resolve_video_id(conn, body.video_name)
                ins, dele, rem, vname, marked = _set_nogroup_video(conn, vid)
                conn.commit()
                return GroupVideoUpdateOut(gid=None, gname=None, vid=vid, video_name=vname, inserted_count=ins, deleted_count=dele, remaining_count=rem, devices_marked=marked)
            gid = _resolve_group_id(conn, gname)
            vid = _resolve_video_id(conn, body.video_name)
            ins, dele, rem, gname_res, vname_res, marked = _set_group_video(conn, gid, vid)
            conn.commit()
            return GroupVideoUpdateOut(gid=gid, gname=gname_res, vid=vid, video_name=vname_res, inserted_count=ins, deleted_count=dele, remaining_count=rem, devices_marked=marked)
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


@app.post("/group/{gname}/videos", response_model=GroupVideosUpdateOut)
def set_group_videos_by_names(gname: str, body: GroupVideosUpdateIn):
    with pg_conn() as conn:
        try:
            vids = _resolve_video_ids(conn, body.video_names)
            if gname.strip().lower() in NO_GROUP_SENTINELS:
                ins, dele, upd, vnames, marked = _set_nogroup_videos(conn, vids)
                conn.commit()
                return GroupVideosUpdateOut(gid=None, gname=None, vids=vids, video_names=vnames, inserted_count=ins, deleted_count=dele, updated_count=upd, devices_marked=marked)
            gid = _resolve_group_id(conn, gname)
            ins, dele, upd, gname_res, vnames, marked = _set_group_videos(conn, gid, vids)
            conn.commit()
            return GroupVideosUpdateOut(gid=gid, gname=gname_res, vids=vids, video_names=vnames, inserted_count=ins, deleted_count=dele, updated_count=upd, devices_marked=marked)
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


# ---------- Video read + presign (for dashboard preview) ----------
@app.get("/video/{video_name}", response_model=VideoOut)
def get_video_by_name(video_name: str = Path(..., description="Exact video_name to fetch")):
    """Return video metadata stored in public.video."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, video_name, s3_link, rotation, content_type, fit_mode, display_duration, created_at, updated_at
                FROM public.video
                WHERE video_name = %s
                ORDER BY id DESC
                LIMIT 1;
                """,
                (video_name,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Video not found")

    return VideoOut(
        id=row[0],
        video_name=row[1],
        s3_link=row[2],
        rotation=row[3] or 0,
        content_type=row[4] or "video",
        fit_mode=row[5] or "cover",
        display_duration=row[6] or 10,
        created_at=row[7],
        updated_at=row[8],
    )


@app.get("/video/{video_name}/presign")
def presign_video(
    video_name: str = Path(..., description="Exact video_name to presign"),
    expires_in: int = Query(PRESIGN_EXPIRES, ge=60, le=604800, description="Presigned URL expiry in seconds"),
):
    """Generate a presigned URL for the video using its s3_link (s3://bucket/key)."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s3_link
                FROM public.video
                WHERE video_name = %s
                ORDER BY id DESC
                LIMIT 1;
                """,
                (video_name,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Video not found")
            s3_link = row[0]

    if not s3_link:
        raise HTTPException(status_code=404, detail="Video s3_link is not set")

    # If DB already stores a public HTTPS URL, return it directly.
    if isinstance(s3_link, str) and (s3_link.startswith("http://") or s3_link.startswith("https://")):
        filename = s3_link.split("/")[-1] or "download"
        return {
            "video_name": video_name,
            "s3_link": s3_link,
            "expires_in": 0,
            "filename": filename,
            "url": s3_link,
            "presigned_url": s3_link,
        }

    try:
        url, filename = presign_get_object(s3_link, expires_in=expires_in)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid s3_link format. Expected s3://bucket/key")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to presign: {type(e).__name__}: {e}")

    # Frontend expects `url` OR `presigned_url`
    return {
        "video_name": video_name,
        "s3_link": s3_link,
        "expires_in": expires_in,
        "filename": filename,
        "url": url,
        "presigned_url": url,
    }


# ---------- Video rotation and fit mode ----------
@app.post("/video/{video_name}/rotation")
def set_video_rotation(video_name: str, body: VideoRotationUpdateIn):
    if body.rotation not in [0, 90, 180, 270]:
        raise HTTPException(status_code=400, detail="Rotation must be 0, 90, 180, or 270")
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.video SET rotation = %s, updated_at = NOW()
                    WHERE video_name = %s RETURNING id, rotation;
                """, (body.rotation, video_name))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Video not found")
            conn.commit()
            return {"video_name": video_name, "rotation": row[1]}
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


@app.post("/video/{video_name}/fit_mode")
def set_video_fit_mode(video_name: str, body: VideoFitModeUpdateIn):
    valid_modes = ["contain", "cover", "fill", "none"]
    if body.fit_mode not in valid_modes:
        raise HTTPException(status_code=400, detail=f"fit_mode must be one of: {', '.join(valid_modes)}")
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.video SET fit_mode = %s, updated_at = NOW()
                    WHERE video_name = %s RETURNING id, fit_mode;
                """, (body.fit_mode, video_name))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Video not found")
            conn.commit()
            return {"video_name": video_name, "fit_mode": row[1]}
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


class VideoResolutionUpdateIn(BaseModel):
    resolution: Optional[str] = None  # e.g. "1920x1080", "1280x720", null for auto


@app.post("/video/{video_name}/resolution")
def set_video_resolution(video_name: str, body: VideoResolutionUpdateIn):
    """Set default resolution for a video (e.g. 1920x1080, 1280x720)."""
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.video SET resolution = %s, updated_at = NOW()
                    WHERE video_name = %s RETURNING id, resolution;
                """, (body.resolution, video_name))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Video not found")
            conn.commit()
            return {"video_name": video_name, "resolution": row[1]}
        except HTTPException:
            conn.rollback()
            raise
        except:
            conn.rollback()
            raise


@app.get("/video/{video_name}/groups")
def get_video_groups(video_name: str):
    """Get all groups where this video is linked."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            # First get video ID
            cur.execute("SELECT id FROM public.video WHERE video_name = %s LIMIT 1;", (video_name,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Video not found")
            vid = row[0]
            
            # Get groups from group_video table
            cur.execute("""
                SELECT DISTINCT g.id, g.gname
                FROM public.group_video gv
                JOIN public."group" g ON g.id = gv.gid
                WHERE gv.vid = %s
                ORDER BY g.gname;
            """, (vid,))
            groups = [{"id": r[0], "gname": r[1]} for r in cur.fetchall()]
            group_ids = {g["id"] for g in groups}
            
            # Also check device_video_shop_group for additional group links
            cur.execute("""
                SELECT DISTINCT g.id, g.gname
                FROM public.device_video_shop_group dvsg
                JOIN public."group" g ON g.id = dvsg.gid
                WHERE dvsg.vid = %s AND dvsg.gid IS NOT NULL
                ORDER BY g.gname;
            """, (vid,))
            for r in cur.fetchall():
                if r[0] not in group_ids:
                    groups.append({"id": r[0], "gname": r[1]})
                    group_ids.add(r[0])
            
            # Sort by gname
            groups.sort(key=lambda x: x["gname"])
            
            return {
                "video_name": video_name,
                "groups": groups,
                "group_count": len(groups)
            }


# ---------- Logs and Reports ----------
@app.get("/device/{mobile_id}/logs")
def get_device_logs(
    mobile_id: str,
    log_type: Optional[str] = Query(None, description="Filter by log_type: temperature, door_open"),
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """Get logs for a device with optional filters."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            did = row[0]
            
            where = ["did = %s"]
            params = [did]
            
            if log_type:
                where.append("log_type = %s")
                params.append(log_type)
            if start_date:
                where.append("logged_at >= %s::date")
                params.append(start_date)
            if end_date:
                where.append("logged_at < (%s::date + interval '1 day')")
                params.append(end_date)
            
            params.extend([limit, offset])
            
            cur.execute(f"""
                SELECT id, log_type, value, logged_at
                FROM public.device_logs
                WHERE {' AND '.join(where)}
                ORDER BY logged_at DESC
                LIMIT %s OFFSET %s;
            """, tuple(params))
            rows = cur.fetchall()
            
            items = [{"id": r[0], "log_type": r[1], "value": r[2], "logged_at": r[3]} for r in rows]
            return {"mobile_id": mobile_id, "count": len(items), "items": items}


@app.get("/device/{mobile_id}/count_history")
def get_device_count_history(
    mobile_id: str,
    period_type: Optional[str] = Query(None, description="Filter by period type: daily, monthly"),
    year: Optional[int] = Query(None, description="Filter by year"),
    limit: int = Query(365, ge=1, le=1000),
):
    """Get count history for a device - daily and monthly door open counts over time."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            did = row[0]
            
            # Check if count_history table exists
            cur.execute("SELECT to_regclass('public.count_history');")
            if cur.fetchone()[0] is None:
                return {"mobile_id": mobile_id, "count": 0, "items": [], "message": "No history data available yet"}
            
            where = ["did = %s"]
            params = [did]
            
            if period_type:
                where.append("period_type = %s")
                params.append(period_type)
            if year:
                where.append("EXTRACT(YEAR FROM period_date) = %s")
                params.append(year)
            
            params.append(limit)
            
            cur.execute(f"""
                SELECT id, period_type, period_date, count_value, created_at
                FROM public.count_history
                WHERE {' AND '.join(where)}
                ORDER BY period_date DESC
                LIMIT %s;
            """, tuple(params))
            rows = cur.fetchall()
            
            items = [{
                "id": r[0],
                "period_type": r[1],
                "period_date": r[2].isoformat() if r[2] else None,
                "count_value": r[3],
                "created_at": r[4].isoformat() if r[4] else None
            } for r in rows]
            
            # Also get current counts
            cur.execute("""
                SELECT daily_count, monthly_count, last_daily_reset, last_monthly_reset
                FROM public.device WHERE id = %s;
            """, (did,))
            current = cur.fetchone()
            
            return {
                "mobile_id": mobile_id,
                "count": len(items),
                "items": items,
                "current": {
                    "daily_count": current[0] or 0,
                    "monthly_count": current[1] or 0,
                    "last_daily_reset": current[2].isoformat() if current[2] else None,
                    "last_monthly_reset": current[3].isoformat() if current[3] else None
                } if current else None
            }


@app.get("/device/{mobile_id}/count_history/summary")
def get_device_count_summary(
    mobile_id: str,
    year: Optional[int] = Query(None, description="Filter by year (default: current year)"),
):
    """Get yearly summary of door open counts - totals by month and overall."""
    if not year:
        year = date.today().year
        
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            did = row[0]
            
            # Check if count_history table exists
            cur.execute("SELECT to_regclass('public.count_history');")
            if cur.fetchone()[0] is None:
                return {"mobile_id": mobile_id, "year": year, "monthly_totals": [], "yearly_total": 0}
            
            # Get monthly totals from daily history
            cur.execute("""
                SELECT EXTRACT(MONTH FROM period_date)::int as month, SUM(count_value) as total
                FROM public.count_history
                WHERE did = %s AND period_type = 'daily' AND EXTRACT(YEAR FROM period_date) = %s
                GROUP BY EXTRACT(MONTH FROM period_date)
                ORDER BY month;
            """, (did, year))
            monthly_rows = cur.fetchall()
            
            monthly_totals = [{"month": r[0], "total": r[1] or 0} for r in monthly_rows]
            yearly_total = sum(m["total"] for m in monthly_totals)
            
            # Also get monthly recorded values for comparison
            cur.execute("""
                SELECT EXTRACT(MONTH FROM period_date)::int as month, count_value
                FROM public.count_history
                WHERE did = %s AND period_type = 'monthly' AND EXTRACT(YEAR FROM period_date) = %s
                ORDER BY month;
            """, (did, year))
            monthly_recorded = [{"month": r[0], "count": r[1] or 0} for r in cur.fetchall()]
            
            return {
                "mobile_id": mobile_id,
                "year": year,
                "monthly_totals": monthly_totals,
                "monthly_recorded": monthly_recorded,
                "yearly_total": yearly_total
            }


@app.get("/device/{mobile_id}/count_reconcile")
def reconcile_device_counts(mobile_id: str):
    """
    Reconcile and verify daily/monthly counts.
    Returns current counts, expected counts (from logs), and any discrepancies.
    """
    today = date.today()
    month_start = today.replace(day=1)
    
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, daily_count, monthly_count, last_daily_reset, last_monthly_reset 
                FROM public.device WHERE mobile_id = %s LIMIT 1;
            """, (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            
            did, current_daily, current_monthly, last_daily_reset, last_monthly_reset = row
            current_daily = current_daily or 0
            current_monthly = current_monthly or 0
            
            # Count door_open events from logs for today
            cur.execute("""
                SELECT COUNT(*) FROM public.device_logs
                WHERE did = %s AND log_type = 'door_open' AND logged_at::date = %s;
            """, (did, today))
            expected_daily = cur.fetchone()[0] or 0
            
            # Count door_open events from logs for this month
            cur.execute("""
                SELECT COUNT(*) FROM public.device_logs
                WHERE did = %s AND log_type = 'door_open' 
                AND logged_at >= %s AND logged_at < %s + interval '1 month';
            """, (did, month_start, month_start))
            expected_monthly = cur.fetchone()[0] or 0
            
            # Check count_history for any past daily counts this month
            cur.execute("SELECT to_regclass('public.count_history');")
            history_exists = cur.fetchone()[0] is not None
            
            history_monthly_sum = 0
            if history_exists:
                cur.execute("""
                    SELECT COALESCE(SUM(count_value), 0)
                    FROM public.count_history
                    WHERE did = %s AND period_type = 'daily' 
                    AND period_date >= %s AND period_date < %s;
                """, (did, month_start, today))
                history_monthly_sum = cur.fetchone()[0] or 0
            
            # Total expected monthly = history + today's count
            total_expected_monthly = history_monthly_sum + expected_daily
            
            daily_match = current_daily == expected_daily
            monthly_match = current_monthly == expected_monthly or current_monthly == total_expected_monthly
            
            return {
                "mobile_id": mobile_id,
                "date": str(today),
                "month_start": str(month_start),
                "current_counts": {
                    "daily": current_daily,
                    "monthly": current_monthly,
                },
                "expected_from_logs": {
                    "daily": expected_daily,
                    "monthly_from_logs": expected_monthly,
                    "monthly_from_history_plus_today": total_expected_monthly,
                },
                "last_reset": {
                    "daily": str(last_daily_reset) if last_daily_reset else None,
                    "monthly": str(last_monthly_reset) if last_monthly_reset else None,
                },
                "reconciled": {
                    "daily": daily_match,
                    "monthly": monthly_match,
                },
                "discrepancy": {
                    "daily": current_daily - expected_daily,
                    "monthly": current_monthly - total_expected_monthly,
                }
            }


@app.post("/device/{mobile_id}/count_fix")
def fix_device_counts(mobile_id: str):
    """
    Fix device counts based on actual logs.
    Recalculates daily and monthly counts from door_open events in logs.
    """
    today = date.today()
    month_start = today.replace(day=1)
    
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Device not found")
                did = row[0]
                
                # Count door_open events from logs for today
                cur.execute("""
                    SELECT COUNT(*) FROM public.device_logs
                    WHERE did = %s AND log_type = 'door_open' AND logged_at::date = %s;
                """, (did, today))
                correct_daily = cur.fetchone()[0] or 0
                
                # Count door_open events from logs for this month
                cur.execute("""
                    SELECT COUNT(*) FROM public.device_logs
                    WHERE did = %s AND log_type = 'door_open' 
                    AND logged_at >= %s AND logged_at < %s + interval '1 month';
                """, (did, month_start, month_start))
                correct_monthly = cur.fetchone()[0] or 0
                
                # Update device counts
                cur.execute("""
                    UPDATE public.device 
                    SET daily_count = %s, 
                        monthly_count = %s,
                        last_daily_reset = %s,
                        last_monthly_reset = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    RETURNING daily_count, monthly_count;
                """, (correct_daily, correct_monthly, today, month_start, did))
                new_daily, new_monthly = cur.fetchone()
                
            conn.commit()
            return {
                "mobile_id": mobile_id,
                "fixed": True,
                "new_counts": {
                    "daily": new_daily,
                    "monthly": new_monthly,
                },
                "message": "Counts have been recalculated from door_open logs"
            }
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/device/{mobile_id}/logs/download")
def download_device_logs(
    mobile_id: str,
    log_type: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Download device logs as CSV."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            did = row[0]
            
            where = ["did = %s"]
            params = [did]
            
            if log_type:
                where.append("log_type = %s")
                params.append(log_type)
            if start_date:
                where.append("logged_at >= %s::date")
                params.append(start_date)
            if end_date:
                where.append("logged_at < (%s::date + interval '1 day')")
                params.append(end_date)
            
            cur.execute(f"""
                SELECT log_type, value, logged_at
                FROM public.device_logs
                WHERE {' AND '.join(where)}
                ORDER BY logged_at DESC;
            """, tuple(params))
            rows = cur.fetchall()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["mobile_id", "log_type", "value", "logged_at"])
    for r in rows:
        writer.writerow([mobile_id, r[0], r[1], r[2].isoformat() if r[2] else ""])
    
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=logs_{mobile_id}.csv"}
    )


@app.get("/devices/logs/summary")
def get_all_devices_logs_summary(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Get summary of logs for all devices."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            date_filter = ""
            params = []
            if start_date:
                date_filter += " AND dl.logged_at >= %s::date"
                params.append(start_date)
            if end_date:
                date_filter += " AND dl.logged_at < (%s::date + interval '1 day')"
                params.append(end_date)
            
            cur.execute(f"""
                SELECT d.mobile_id,
                       COUNT(CASE WHEN dl.log_type = 'door_open' THEN 1 END) as door_open_count,
                       AVG(CASE WHEN dl.log_type = 'temperature' THEN dl.value END) as avg_temperature,
                       MIN(CASE WHEN dl.log_type = 'temperature' THEN dl.value END) as min_temperature,
                       MAX(CASE WHEN dl.log_type = 'temperature' THEN dl.value END) as max_temperature
                FROM public.device d
                LEFT JOIN public.device_logs dl ON dl.did = d.id {date_filter}
                GROUP BY d.id, d.mobile_id
                ORDER BY d.mobile_id;
            """, tuple(params))
            rows = cur.fetchall()
            
            items = [{
                "mobile_id": r[0],
                "door_open_count": r[1] or 0,
                "avg_temperature": round(float(r[2]), 2) if r[2] else None,
                "min_temperature": float(r[3]) if r[3] else None,
                "max_temperature": float(r[4]) if r[4] else None,
            } for r in rows]
            return {"count": len(items), "items": items}


@app.get("/devices/logs/summary/download")
def download_all_devices_logs_summary(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Download summary of all devices as CSV."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            date_filter = ""
            params = []
            if start_date:
                date_filter += " AND dl.logged_at >= %s::date"
                params.append(start_date)
            if end_date:
                date_filter += " AND dl.logged_at < (%s::date + interval '1 day')"
                params.append(end_date)
            
            cur.execute(f"""
                SELECT d.mobile_id, d.daily_count, d.monthly_count,
                       COUNT(CASE WHEN dl.log_type = 'door_open' THEN 1 END) as door_open_count,
                       AVG(CASE WHEN dl.log_type = 'temperature' THEN dl.value END) as avg_temp,
                       MIN(CASE WHEN dl.log_type = 'temperature' THEN dl.value END) as min_temp,
                       MAX(CASE WHEN dl.log_type = 'temperature' THEN dl.value END) as max_temp
                FROM public.device d
                LEFT JOIN public.device_logs dl ON dl.did = d.id {date_filter}
                GROUP BY d.id, d.mobile_id, d.daily_count, d.monthly_count
                ORDER BY d.mobile_id;
            """, tuple(params))
            rows = cur.fetchall()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["mobile_id", "daily_count", "monthly_count", "door_open_count", "avg_temperature", "min_temperature", "max_temperature"])
    for r in rows:
        writer.writerow([r[0], r[1], r[2], r[3] or 0, round(float(r[4]), 2) if r[4] else "", r[5] or "", r[6] or ""])
    
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=devices_summary.csv"}
    )


# ---------- Admin: Mark offline devices ----------
@app.post("/admin/mark_offline_devices")
def mark_offline_devices():
    """Mark devices as offline if their last_online_at is older than threshold. For cron job."""
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE public.device
                    SET is_online = FALSE, updated_at = NOW()
                    WHERE is_online = TRUE
                      AND (last_online_at IS NULL 
                           OR last_online_at < NOW() - INTERVAL '{ONLINE_THRESHOLD_SECONDS} seconds')
                    RETURNING id, mobile_id;
                """)
                rows = cur.fetchall()
                marked = [r[1] for r in rows]
                
                # Log offline events for all marked devices
                for row in rows:
                    cur.execute("""
                        INSERT INTO public.device_online_history (did, event_type, event_at)
                        VALUES (%s, 'offline', NOW());
                    """, (row[0],))
                
            conn.commit()
            return {"marked_offline": len(marked), "devices": marked}
        except:
            conn.rollback()
            raise


# ---------- Online History Report ----------
class OnlineHistoryItem(BaseModel):
    id: int
    event_type: str
    event_at: datetime


class OnlineHistoryOut(BaseModel):
    mobile_id: str
    items: List[OnlineHistoryItem]
    count: int


class UptimeStatsOut(BaseModel):
    mobile_id: str
    total_online_seconds: float
    total_offline_seconds: float
    online_percentage: float
    total_online_events: int
    total_offline_events: int
    first_event: Optional[datetime]
    last_event: Optional[datetime]
    sessions: List[Dict[str, Any]]


@app.get("/device/{mobile_id}/online_history")
def get_device_online_history(
    mobile_id: str,
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """Get online/offline history for a device."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            did = row[0]
            
            where = ["did = %s"]
            params = [did]
            
            if start_date:
                where.append("event_at >= %s::date")
                params.append(start_date)
            if end_date:
                where.append("event_at < (%s::date + interval '1 day')")
                params.append(end_date)
            
            params.extend([limit, offset])
            
            cur.execute(f"""
                SELECT id, event_type, event_at
                FROM public.device_online_history
                WHERE {' AND '.join(where)}
                ORDER BY event_at DESC
                LIMIT %s OFFSET %s;
            """, tuple(params))
            rows = cur.fetchall()
            
            items = [{"id": r[0], "event_type": r[1], "event_at": r[2]} for r in rows]
            return {"mobile_id": mobile_id, "count": len(items), "items": items}


@app.get("/device/{mobile_id}/uptime_report")
def get_device_uptime_report(
    mobile_id: str,
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
):
    """Get uptime statistics for a device including session details."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, is_online FROM public.device WHERE mobile_id = %s LIMIT 1;", (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            did = row[0]
            current_status = row[1]
            
            where = ["did = %s"]
            params = [did]
            
            if start_date:
                where.append("event_at >= %s::date")
                params.append(start_date)
            if end_date:
                where.append("event_at < (%s::date + interval '1 day')")
                params.append(end_date)
            
            # Get all events in chronological order
            cur.execute(f"""
                SELECT id, event_type, event_at
                FROM public.device_online_history
                WHERE {' AND '.join(where)}
                ORDER BY event_at ASC;
            """, tuple(params))
            rows = cur.fetchall()
            
            if not rows:
                return {
                    "mobile_id": mobile_id,
                    "total_online_seconds": 0,
                    "total_offline_seconds": 0,
                    "online_percentage": 0,
                    "total_online_events": 0,
                    "total_offline_events": 0,
                    "first_event": None,
                    "last_event": None,
                    "sessions": [],
                }
            
            # Calculate sessions and uptime
            sessions = []
            total_online_seconds = 0
            total_offline_seconds = 0
            online_events = 0
            offline_events = 0
            
            first_event = rows[0][2]
            last_event = rows[-1][2]
            
            # Process events to create sessions
            current_session_start = None
            current_session_type = None
            
            for r in rows:
                event_type = r[1]
                event_at = r[2]
                
                if event_type == "online":
                    online_events += 1
                    if current_session_type == "offline" and current_session_start:
                        # End offline session
                        duration = (event_at - current_session_start).total_seconds()
                        total_offline_seconds += duration
                        sessions.append({
                            "type": "offline",
                            "start": current_session_start.isoformat(),
                            "end": event_at.isoformat(),
                            "duration_seconds": duration,
                        })
                    current_session_start = event_at
                    current_session_type = "online"
                else:  # offline
                    offline_events += 1
                    if current_session_type == "online" and current_session_start:
                        # End online session
                        duration = (event_at - current_session_start).total_seconds()
                        total_online_seconds += duration
                        sessions.append({
                            "type": "online",
                            "start": current_session_start.isoformat(),
                            "end": event_at.isoformat(),
                            "duration_seconds": duration,
                        })
                    current_session_start = event_at
                    current_session_type = "offline"
            
            # Handle ongoing session based on ACTUAL current device status
            from datetime import datetime as dt, timezone
            now = dt.now(timezone.utc)
            if current_session_start and current_session_type:
                duration = (now - current_session_start).total_seconds()
                
                # If last recorded session type doesn't match current status, 
                # close that session and start a new one with current status
                if current_session_type == "online" and not current_status:
                    # Device went offline but we missed the event - close online session
                    total_online_seconds += duration
                    sessions.append({
                        "type": "online",
                        "start": current_session_start.isoformat(),
                        "end": now.isoformat(),
                        "duration_seconds": duration,
                        "ongoing": False,
                    })
                elif current_session_type == "offline" and current_status:
                    # Device came online but we missed the event - close offline session
                    total_offline_seconds += duration
                    sessions.append({
                        "type": "offline",
                        "start": current_session_start.isoformat(),
                        "end": now.isoformat(),
                        "duration_seconds": duration,
                        "ongoing": False,
                    })
                else:
                    # Session type matches current status - it's ongoing
                    if current_session_type == "online":
                        total_online_seconds += duration
                    else:
                        total_offline_seconds += duration
                    sessions.append({
                        "type": current_session_type,
                        "start": current_session_start.isoformat(),
                        "end": None,  # Ongoing
                        "duration_seconds": duration,
                        "ongoing": True,
                    })
            
            total_seconds = total_online_seconds + total_offline_seconds
            online_percentage = (total_online_seconds / total_seconds * 100) if total_seconds > 0 else 0
            
            # Return most recent sessions first (limit to last 100)
            sessions.reverse()
            sessions = sessions[:100]
            
            return {
                "mobile_id": mobile_id,
                "total_online_seconds": round(total_online_seconds, 2),
                "total_offline_seconds": round(total_offline_seconds, 2),
                "online_percentage": round(online_percentage, 2),
                "total_online_events": online_events,
                "total_offline_events": offline_events,
                "first_event": first_event,
                "last_event": last_event,
                "sessions": sessions,
            }


# ========== USER MANAGEMENT ENDPOINTS ==========

@app.post("/auth/login")
def login(body: UserLoginIn):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            # Fetch user with company and role info
            cur.execute("""
                SELECT u.id, u.username, u.password_hash, u.full_name, u.role, u.is_active,
                       u.user_type, u.tenant_id, u.role_id, u.must_change_password,
                       c.slug as company_slug, c.name as company_name, c.status as company_status,
                       r.name as role_name, r.permissions as role_permissions
                FROM public.users u
                LEFT JOIN public.company c ON u.tenant_id = c.id
                LEFT JOIN public.role r ON u.role_id = r.id
                WHERE u.username = %s LIMIT 1;
            """, (body.username.lower(),))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=401, detail="Invalid username or password")

            (user_id, username, password_hash, full_name, legacy_role, is_active,
             user_type, tenant_id, role_id, must_change_password,
             company_slug, company_name, company_status,
             role_name, role_permissions) = row

            if not is_active:
                raise HTTPException(status_code=401, detail="Account is disabled")
            if not verify_password(body.password, password_hash):
                raise HTTPException(status_code=401, detail="Invalid username or password")

            # Company users: check company status
            if user_type == "company" and tenant_id:
                if company_status == "suspended":
                    raise HTTPException(status_code=403, detail="Company account suspended. Contact platform administrator.")
                if company_status == "cancelled":
                    raise HTTPException(status_code=403, detail="Company account cancelled.")

            # Resolve permissions: prefer role table, fallback to legacy
            if role_permissions:
                if isinstance(role_permissions, str):
                    permissions = json.loads(role_permissions)
                elif isinstance(role_permissions, list):
                    permissions = role_permissions
                else:
                    permissions = list(role_permissions)
            else:
                # Fallback to legacy ROLE_PERMISSIONS
                permissions = list(ROLE_PERMISSIONS.get(legacy_role, []))

            # Also merge any custom permissions from user_permissions table
            cur.execute("SELECT permission FROM public.user_permissions WHERE user_id = %s;", (user_id,))
            custom_perms = [r[0] for r in cur.fetchall()]
            permissions = list(set(permissions + custom_perms))

            # Effective role name
            effective_role = role_name or legacy_role or "viewer"

            cur.execute("UPDATE public.users SET last_login = NOW() WHERE id = %s;", (user_id,))
            conn.commit()

            token = create_session(
                user_id=user_id,
                username=username,
                full_name=full_name,
                user_type=user_type or "company",
                tenant_id=tenant_id,
                role_name=effective_role,
                role_id=role_id,
                permissions=permissions,
                company_slug=company_slug,
                company_name=company_name,
            )

            # Audit log
            log_audit(conn, tenant_id, user_id, "user.login", "user", user_id)
            conn.commit()

            result = {
                "token": token,
                "user_id": user_id,
                "username": username,
                "full_name": full_name,
                "role": effective_role,
                "permissions": permissions,
                "user_type": user_type or "company",
                "must_change_password": must_change_password or False,
            }

            # Include company info for company users
            if tenant_id:
                result["company"] = {
                    "id": tenant_id,
                    "slug": company_slug,
                    "name": company_name,
                }

            return result

@app.post("/auth/logout")
def logout(authorization: Optional[str] = Header(None)):
    if authorization:
        token = authorization.replace("Bearer ", "") if authorization.startswith("Bearer ") else authorization
        if token in active_sessions:
            del active_sessions[token]
    return {"message": "Logged out successfully"}

@app.get("/auth/me")
def get_current_user_info(user: Dict = Depends(get_current_user)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.id, u.username, u.email, u.full_name, u.role, u.is_active, u.created_at, u.last_login,
                       u.user_type, u.tenant_id, u.must_change_password,
                       c.slug as company_slug, c.name as company_name
                FROM public.users u
                LEFT JOIN public.company c ON u.tenant_id = c.id
                WHERE u.id = %s LIMIT 1;
            """, (user["user_id"],))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="User not found")
            result = {
                "id": row[0], "username": row[1], "email": row[2], "full_name": row[3],
                "role": row[4], "is_active": row[5], "created_at": row[6], "last_login": row[7],
                "permissions": user.get("permissions", []),
                "user_type": row[8] or "company",
                "tenant_id": row[9],
                "must_change_password": row[10] or False,
            }
            if row[9]:
                result["company"] = {"id": row[9], "slug": row[11], "name": row[12]}
            return result

@app.get("/users", response_model=UserListOut)
def list_users(limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0), user: Dict = Depends(require_permission("manage_users"))):
    # Resolve tenant scope
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id")
    with pg_conn() as conn:
        with conn.cursor() as cur:
            if user.get("user_type") == "platform" and tenant_id is None:
                # Platform admin without impersonation: show all users with company info
                cur.execute("SELECT COUNT(*) FROM public.users;")
                total = cur.fetchone()[0]
                cur.execute("""SELECT u.id, u.username, u.email, u.full_name, u.role, u.is_active, u.created_at, u.last_login,
                    ARRAY_AGG(up.permission) FILTER (WHERE up.permission IS NOT NULL) as permissions,
                    u.user_type, u.tenant_id, c.slug as company_slug, c.name as company_name
                    FROM public.users u LEFT JOIN public.user_permissions up ON u.id = up.user_id
                    LEFT JOIN public.company c ON u.tenant_id = c.id
                    GROUP BY u.id, c.slug, c.name ORDER BY u.created_at DESC LIMIT %s OFFSET %s;""", (limit, offset))
            else:
                # Scoped to tenant
                cur.execute("SELECT COUNT(*) FROM public.users WHERE tenant_id = %s;", (tenant_id,))
                total = cur.fetchone()[0]
                cur.execute("""SELECT u.id, u.username, u.email, u.full_name, u.role, u.is_active, u.created_at, u.last_login,
                    ARRAY_AGG(up.permission) FILTER (WHERE up.permission IS NOT NULL) as permissions,
                    u.user_type, u.tenant_id, c.slug as company_slug, c.name as company_name
                    FROM public.users u LEFT JOIN public.user_permissions up ON u.id = up.user_id
                    LEFT JOIN public.company c ON u.tenant_id = c.id
                    WHERE u.tenant_id = %s
                    GROUP BY u.id, c.slug, c.name ORDER BY u.created_at DESC LIMIT %s OFFSET %s;""", (tenant_id, limit, offset))
            items = []
            for row in cur.fetchall():
                permissions = row[8] if row[8] else []
                all_perms = list(set(ROLE_PERMISSIONS.get(row[4], []) + permissions))
                items.append(UserOut(id=row[0], username=row[1], email=row[2], full_name=row[3], role=row[4], is_active=row[5], created_at=row[6], last_login=row[7], permissions=all_perms,
                    user_type=row[9], tenant_id=row[10], company_slug=row[11], company_name=row[12]))
            return UserListOut(items=items, total=total)

@app.post("/users", response_model=UserOut)
def create_user(body: UserCreateIn, user: Dict = Depends(require_permission("manage_users"))):
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id")

    # Platform admin can assign user to a specific company via company_slug
    target_company_slug = None
    target_company_name = None
    if body.company_slug and user.get("user_type") == "platform":
        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, slug, name FROM public.company WHERE slug = %s", (body.company_slug,))
                c = cur.fetchone()
                if not c:
                    raise HTTPException(status_code=404, detail=f"Company '{body.company_slug}' not found")
                tenant_id = c[0]
                target_company_slug = c[1]
                target_company_name = c[2]

    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                # Check uniqueness globally - same username cannot exist in any company
                cur.execute("""SELECT u.id, c.name FROM public.users u 
                    LEFT JOIN public.company c ON u.tenant_id = c.id 
                    WHERE u.username = %s;""", (body.username.lower(),))
                existing = cur.fetchone()
                if existing:
                    company_name = existing[1] or "Platform"
                    raise HTTPException(status_code=400, detail=f"Username '{body.username.lower()}' is already assigned to '{company_name}'. Please use another username.")
                if body.email:
                    cur.execute("""SELECT u.id, c.name FROM public.users u 
                        LEFT JOIN public.company c ON u.tenant_id = c.id 
                        WHERE u.email = %s;""", (body.email.lower(),))
                    existing_email = cur.fetchone()
                    if existing_email:
                        company_name = existing_email[1] or "Platform"
                        raise HTTPException(status_code=400, detail=f"Email '{body.email.lower()}' is already assigned to '{company_name}'. Please use another email.")
                if body.role not in ROLE_PERMISSIONS:
                    raise HTTPException(status_code=400, detail=f"Invalid role")

                # Check quota for company users
                if tenant_id:
                    cur.execute("SELECT max_users FROM public.company WHERE id = %s;", (tenant_id,))
                    company_row = cur.fetchone()
                    if company_row:
                        cur.execute("SELECT COUNT(*) FROM public.users WHERE tenant_id = %s;", (tenant_id,))
                        current_count = cur.fetchone()[0]
                        if current_count >= company_row[0]:
                            raise HTTPException(status_code=400, detail=f"User limit reached ({company_row[0]})")

                password_hash = hash_password(body.password)
                user_type = "platform" if tenant_id is None and user.get("user_type") == "platform" else "company"
                cur.execute("""INSERT INTO public.users (username, email, password_hash, full_name, role, created_by, tenant_id, user_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id, created_at;""",
                    (body.username.lower(), body.email.lower() if body.email else None, password_hash, body.full_name, body.role, user["user_id"], tenant_id, user_type))
                new_user_id, created_at = cur.fetchone()
                for perm in body.permissions:
                    cur.execute("INSERT INTO public.user_permissions (user_id, permission) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (new_user_id, perm))

                # Fetch company info if we don't have it yet
                if tenant_id and not target_company_slug:
                    cur.execute("SELECT slug, name FROM public.company WHERE id = %s", (tenant_id,))
                    cr = cur.fetchone()
                    if cr:
                        target_company_slug = cr[0]
                        target_company_name = cr[1]

                conn.commit()
                all_perms = list(set(ROLE_PERMISSIONS.get(body.role, []) + body.permissions))
                return UserOut(id=new_user_id, username=body.username.lower(), email=body.email, full_name=body.full_name, role=body.role, is_active=True, created_at=created_at, last_login=None, permissions=all_perms,
                    user_type=user_type, tenant_id=tenant_id, company_slug=target_company_slug, company_name=target_company_name)
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))

@app.put("/users/{user_id}", response_model=UserOut)
def update_user(user_id: int, body: UserUpdateIn, current_user: Dict = Depends(require_permission("manage_users"))):
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id, role FROM public.users WHERE id = %s;", (user_id,))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="User not found")
                updates, params = [], []
                if body.email is not None:
                    updates.append("email = %s")
                    params.append(body.email.lower() if body.email else None)
                if body.full_name is not None:
                    updates.append("full_name = %s")
                    params.append(body.full_name)
                if body.role is not None:
                    if body.role not in ROLE_PERMISSIONS:
                        raise HTTPException(status_code=400, detail="Invalid role")
                    updates.append("role = %s")
                    params.append(body.role)
                if body.is_active is not None:
                    updates.append("is_active = %s")
                    params.append(body.is_active)
                    # If deactivating user, invalidate all their sessions immediately
                    if not body.is_active:
                        invalidate_user_sessions(user_id)
                if updates:
                    updates.append("updated_at = NOW()")
                    params.append(user_id)
                    cur.execute(f"UPDATE public.users SET {', '.join(updates)} WHERE id = %s;", params)
                if body.permissions is not None:
                    cur.execute("DELETE FROM public.user_permissions WHERE user_id = %s;", (user_id,))
                    for perm in body.permissions:
                        cur.execute("INSERT INTO public.user_permissions (user_id, permission) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (user_id, perm))
                conn.commit()
                cur.execute("SELECT u.id, u.username, u.email, u.full_name, u.role, u.is_active, u.created_at, u.last_login, ARRAY_AGG(up.permission) FILTER (WHERE up.permission IS NOT NULL) as permissions FROM public.users u LEFT JOIN public.user_permissions up ON u.id = up.user_id WHERE u.id = %s GROUP BY u.id;", (user_id,))
                row = cur.fetchone()
                permissions = row[8] if row[8] else []
                all_perms = list(set(ROLE_PERMISSIONS.get(row[4], []) + permissions))
                return UserOut(id=row[0], username=row[1], email=row[2], full_name=row[3], role=row[4], is_active=row[5], created_at=row[6], last_login=row[7], permissions=all_perms)
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))

@app.delete("/users/{user_id}")
def delete_user(user_id: int, current_user: Dict = Depends(require_permission("manage_users"))):
    if user_id == current_user["user_id"]:
        raise HTTPException(status_code=400, detail="Cannot delete your own account")
    # Invalidate all sessions for the user being deleted
    invalidate_user_sessions(user_id)
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT username FROM public.users WHERE id = %s;", (user_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="User not found")
                if row[0] == "admin":
                    raise HTTPException(status_code=400, detail="Cannot delete the default admin account")
                cur.execute("DELETE FROM public.user_permissions WHERE user_id = %s;", (user_id,))
                cur.execute("DELETE FROM public.users WHERE id = %s;", (user_id,))
                conn.commit()
                return {"message": "User deleted successfully"}
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/users/{user_id}/reset-password")
def reset_user_password(user_id: int, new_password: str = Query(..., min_length=6), current_user: Dict = Depends(require_permission("manage_users"))):
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM public.users WHERE id = %s;", (user_id,))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="User not found")
                password_hash = hash_password(new_password)
                cur.execute("UPDATE public.users SET password_hash = %s, updated_at = NOW() WHERE id = %s;", (password_hash, user_id))
                conn.commit()
                return {"message": "Password reset successfully"}
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/auth/change-password")
def change_own_password(body: UserChangePasswordIn, current_user: Dict = Depends(get_current_user)):
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT password_hash FROM public.users WHERE id = %s;", (current_user["user_id"],))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="User not found")
                if not verify_password(body.current_password, row[0]):
                    raise HTTPException(status_code=400, detail="Current password is incorrect")
                password_hash = hash_password(body.new_password)
                cur.execute("UPDATE public.users SET password_hash = %s, updated_at = NOW() WHERE id = %s;", (password_hash, current_user["user_id"]))
                conn.commit()
                return {"message": "Password changed successfully"}
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/auth/permissions")
def get_available_permissions():
    all_permissions = set()
    for perms in ROLE_PERMISSIONS.values():
        all_permissions.update(perms)
    return {"permissions": sorted(list(all_permissions)), "roles": ROLE_PERMISSIONS}


# ===========================================================================================
# ADVERTISEMENT (IMAGE) API ENDPOINTS
# ===========================================================================================

# Pydantic models for advertisements
class AdvertisementOut(BaseModel):
    id: int
    ad_name: str
    s3_link: Optional[str] = None
    rotation: int = 0
    fit_mode: Optional[str] = "cover"
    display_duration: Optional[int] = 10
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class AdvertisementUpdate(BaseModel):
    ad_name: Optional[str] = None
    s3_link: Optional[str] = None
    rotation: Optional[int] = None
    fit_mode: Optional[str] = None
    display_duration: Optional[int] = None

class AdvertisementRotationUpdate(BaseModel):
    rotation: int

class AdvertisementFitModeUpdate(BaseModel):
    fit_mode: str

# S3 config for advertisements
S3_BUCKET = os.getenv("S3_BUCKET", "digix-videos")
S3_AD_PREFIX = os.getenv("S3_AD_PREFIX", "advertisements").strip("/")
AD_PRESIGN_EXPIRES = int(os.getenv("AD_PRESIGN_EXPIRES", "3600"))

import re
def _ad_slug(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^\w\-.]+", "-", s, flags=re.UNICODE)
    return re.sub(r"-{2,}", "-", s).strip("-").lower()

def _make_ad_s3_key(ad_name: str, ext: str = ".jpg", tenant_slug: str = None) -> str:
    base = _ad_slug(ad_name)
    filename = f"{base}{ext}"
    if tenant_slug:
        return f"tenants/{tenant_slug}/advertisements/{filename}"
    return f"{S3_AD_PREFIX}/{filename}".strip("/") if S3_AD_PREFIX else filename

def _to_ad_s3_uri(key: str) -> str:
    return f"s3://{S3_BUCKET}/{key}"

def _detect_image_extension(filename: str) -> str:
    ext = filename.lower().split('.')[-1] if '.' in filename else 'jpg'
    valid_exts = ['jpg', 'jpeg', 'png', 'gif', 'webp']
    return f".{ext}" if ext in valid_exts else ".jpg"

def _get_content_type_for_image(ext: str) -> str:
    content_types = {
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.png': 'image/png',
        '.gif': 'image/gif',
        '.webp': 'image/webp',
    }
    return content_types.get(ext.lower(), 'image/jpeg')

def ad_presign_get_object(s3_uri: str, expires_in: int = AD_PRESIGN_EXPIRES) -> Tuple[str, str]:
    """Generate presigned URL for advertisement image."""
    bucket, key = parse_s3_uri(s3_uri)
    s3 = boto3.client("s3", region_name=AWS_REGION) if AWS_REGION else boto3.client("s3")
    filename = key.split("/")[-1] or "download.jpg"
    
    ext = filename.lower().split('.')[-1] if '.' in filename else 'jpg'
    content_type = _get_content_type_for_image(f".{ext}")
    
    params = {
        "Bucket": bucket,
        "Key": key,
        "ResponseContentType": content_type,
    }
    url = s3.generate_presigned_url("get_object", Params=params, ExpiresIn=expires_in)
    return url, filename

# --- Advertisement CRUD ---
@app.get("/advertisements")
def list_advertisements(q: Optional[str] = Query(None), limit: int = Query(50, ge=1, le=1000), offset: int = Query(0, ge=0), user: Dict = Depends(get_current_user)):
    """List all advertisements for the current tenant."""
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id")
    with pg_conn() as conn:
        with conn.cursor() as cur:
            base = """SELECT id, ad_name, s3_link, rotation, fit_mode, display_duration, created_at, updated_at
                     FROM public.advertisement"""
            if tenant_id:
                if q:
                    cur.execute(base + " WHERE tenant_id = %s AND ad_name ILIKE %s ORDER BY id DESC LIMIT %s OFFSET %s", (tenant_id, f"%{q}%", limit, offset))
                else:
                    cur.execute(base + " WHERE tenant_id = %s ORDER BY id DESC LIMIT %s OFFSET %s", (tenant_id, limit, offset))
            else:
                if q:
                    cur.execute(base + " WHERE ad_name ILIKE %s ORDER BY id DESC LIMIT %s OFFSET %s", (f"%{q}%", limit, offset))
                else:
                    cur.execute(base + " ORDER BY id DESC LIMIT %s OFFSET %s", (limit, offset))
            rows = cur.fetchall()
            items = [{"id": r[0], "ad_name": r[1], "s3_link": r[2], "rotation": r[3],
                      "fit_mode": r[4], "display_duration": r[5], "created_at": r[6], "updated_at": r[7]} 
                     for r in rows]
            return {"count": len(items), "items": items, "limit": limit, "offset": offset, "query": q}

@app.get("/advertisement/{ad_name}")
def get_advertisement(ad_name: str, presign: bool = Query(True)):
    """Get a single advertisement by name."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, ad_name, s3_link, rotation, fit_mode, display_duration, created_at, updated_at
                FROM public.advertisement WHERE ad_name = %s LIMIT 1;
            """, (ad_name,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Advertisement not found")
            result = {"id": row[0], "ad_name": row[1], "s3_link": row[2], "rotation": row[3],
                      "fit_mode": row[4], "display_duration": row[5], "created_at": row[6], "updated_at": row[7]}
            
            if presign and result.get("s3_link"):
                try:
                    url, filename = ad_presign_get_object(result["s3_link"], AD_PRESIGN_EXPIRES)
                    result["presigned_url"] = url
                    result["presigned_expires_in"] = AD_PRESIGN_EXPIRES
                except Exception as e:
                    result["presign_error"] = str(e)
            
            return result

@app.get("/advertisement/{ad_name}/presign")
def presign_advertisement(ad_name: str, expires_in: int = Query(AD_PRESIGN_EXPIRES, ge=60, le=7 * 24 * 3600)):
    """Get presigned URL for an advertisement."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT s3_link FROM public.advertisement WHERE ad_name = %s LIMIT 1;", (ad_name,))
            row = cur.fetchone()
            if not row or not row[0]:
                raise HTTPException(status_code=404, detail="Advertisement not found")
            url, filename = ad_presign_get_object(row[0], expires_in)
            return {"ad_name": ad_name, "url": url, "expires_in": expires_in}

@app.post("/upload_advertisement")
async def upload_advertisement(
    file: UploadFile = File(...),
    ad_name: str = Form(...),
    overwrite: bool = Form(False),
    rotation: int = Form(0),
    fit_mode: str = Form("cover"),
    display_duration: int = Form(10),
    user: Dict = Depends(get_current_user),
):
    """Upload an image advertisement to S3 and create database record."""
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id")
    try:
        # Detect file extension
        ext = _detect_image_extension(file.filename or ad_name)
        key = _make_ad_s3_key(ad_name, ext)
        content_type = _get_content_type_for_image(ext)
        
        # Check if exists (if not overwriting)
        s3 = boto3.client("s3", region_name=AWS_REGION) if AWS_REGION else boto3.client("s3")
        if not overwrite:
            try:
                s3.head_object(Bucket=S3_BUCKET, Key=key)
                raise HTTPException(status_code=409, detail="Advertisement exists. Use overwrite=true.")
            except s3.exceptions.ClientError as e:
                if e.response['Error']['Code'] != '404':
                    raise
        
        # Upload to S3
        s3.upload_fileobj(
            file.file, 
            S3_BUCKET, 
            key, 
            ExtraArgs={"ContentType": content_type, "ACL": "private"}
        )
        
        s3_uri = _to_ad_s3_uri(key)
        
        # Upsert to database
        with pg_conn() as conn:
            with conn.cursor() as cur:
                if tenant_id:
                    cur.execute("""
                        INSERT INTO public.advertisement (ad_name, s3_link, rotation, fit_mode, display_duration, tenant_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (ad_name, s3_uri, rotation, fit_mode, display_duration, tenant_id))
                    if cur.rowcount == 0:
                        cur.execute("""
                            UPDATE public.advertisement SET s3_link = %s, rotation = %s,
                                fit_mode = %s, display_duration = %s, updated_at = NOW()
                            WHERE ad_name = %s AND tenant_id = %s RETURNING id;
                        """, (s3_uri, rotation, fit_mode, display_duration, ad_name, tenant_id))
                    else:
                        cur.execute("SELECT id FROM public.advertisement WHERE ad_name = %s AND tenant_id = %s ORDER BY id DESC LIMIT 1;", (ad_name, tenant_id))
                else:
                    cur.execute("""
                        INSERT INTO public.advertisement (ad_name, s3_link, rotation, fit_mode, display_duration)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (ad_name, s3_uri, rotation, fit_mode, display_duration))
                    if cur.rowcount == 0:
                        cur.execute("""
                            UPDATE public.advertisement SET s3_link = %s, rotation = %s,
                                fit_mode = %s, display_duration = %s, updated_at = NOW()
                            WHERE ad_name = %s RETURNING id;
                        """, (s3_uri, rotation, fit_mode, display_duration, ad_name))
                    else:
                        cur.execute("SELECT id FROM public.advertisement WHERE ad_name = %s ORDER BY id DESC LIMIT 1;", (ad_name,))
                new_id = cur.fetchone()[0]
                conn.commit()
        
        return {
            "id": new_id, 
            "ad_name": ad_name, 
            "s3_link": s3_uri, 
            "key": key,
            "rotation": rotation, 
            "fit_mode": fit_mode,
            "display_duration": display_duration, 
            "overwrote": overwrite
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")

@app.put("/advertisement/{ad_name}")
def update_advertisement(ad_name: str, patch: AdvertisementUpdate):
    """Update an advertisement's metadata."""
    sets, params = [], []
    if patch.ad_name is not None:
        sets.append("ad_name = %s"); params.append(patch.ad_name)
    if patch.s3_link is not None:
        sets.append("s3_link = %s"); params.append(patch.s3_link)
    if patch.rotation is not None:
        sets.append("rotation = %s"); params.append(patch.rotation)
    if patch.fit_mode is not None:
        sets.append("fit_mode = %s"); params.append(patch.fit_mode)
    if patch.display_duration is not None:
        sets.append("display_duration = %s"); params.append(patch.display_duration)
    
    if not sets:
        return get_advertisement(ad_name)
    
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                params.append(ad_name)
                cur.execute(f"UPDATE public.advertisement SET {', '.join(sets)}, updated_at = NOW() WHERE ad_name = %s RETURNING *;", params)
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Advertisement not found")
                conn.commit()
                return {"id": row[0], "ad_name": row[1], "s3_link": row[2], "rotation": row[3],
                        "fit_mode": row[4], "display_duration": row[5], "created_at": row[6], "updated_at": row[7]}
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/advertisement/{ad_name}/rotation")
def set_advertisement_rotation(ad_name: str, body: AdvertisementRotationUpdate):
    """Update advertisement rotation."""
    if body.rotation not in (0, 90, 180, 270):
        raise HTTPException(status_code=400, detail="Rotation must be 0, 90, 180, or 270")
    
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE public.advertisement SET rotation = %s, updated_at = NOW() WHERE ad_name = %s RETURNING *;", 
                           (body.rotation, ad_name))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Advertisement not found")
                conn.commit()
                return {"message": "Rotation updated", "item": {"id": row[0], "ad_name": row[1], "rotation": row[3]}}
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/advertisement/{ad_name}/fit_mode")
def set_advertisement_fit_mode(ad_name: str, body: AdvertisementFitModeUpdate):
    """Update advertisement fit mode."""
    if body.fit_mode not in ("cover", "contain", "fill", "none"):
        raise HTTPException(status_code=400, detail="fit_mode must be cover, contain, fill, or none")
    
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE public.advertisement SET fit_mode = %s, updated_at = NOW() WHERE ad_name = %s RETURNING *;", 
                           (body.fit_mode, ad_name))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Advertisement not found")
                conn.commit()
                return {"message": "Fit mode updated", "item": {"id": row[0], "ad_name": row[1], "fit_mode": row[4]}}
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))

@app.delete("/advertisement/{ad_name}")
def delete_advertisement(ad_name: str):
    """Delete an advertisement."""
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM public.advertisement WHERE ad_name = %s RETURNING id;", (ad_name,))
                rows = cur.fetchall()
                conn.commit()
                if not rows:
                    raise HTTPException(status_code=404, detail="Advertisement not found")
                return {"deleted_count": len(rows)}
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/advertisement/{ad_name}/groups")
def get_advertisement_groups(ad_name: str):
    """Get all groups where this advertisement/image is linked."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            # First get advertisement ID
            cur.execute("SELECT id FROM public.advertisement WHERE ad_name = %s LIMIT 1;", (ad_name,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Advertisement not found")
            aid = row[0]
            
            # Get groups from group_advertisement table
            cur.execute("""
                SELECT DISTINCT g.id, g.gname
                FROM public.group_advertisement ga
                JOIN public."group" g ON g.id = ga.gid
                WHERE ga.aid = %s
                ORDER BY g.gname;
            """, (aid,))
            groups = [{"id": r[0], "gname": r[1]} for r in cur.fetchall()]
            group_ids = {g["id"] for g in groups}
            
            # Also check device_layout for devices using this ad in their layout_config
            # Then find which groups those devices belong to
            cur.execute("""
                SELECT DISTINCT g.id, g.gname
                FROM public.device_layout dl
                JOIN public.device d ON d.id = dl.did
                JOIN public.device_assignment da ON da.did = d.id
                JOIN public."group" g ON g.id = da.gid
                WHERE dl.layout_config LIKE %s
                ORDER BY g.gname;
            """, (f'%"{ad_name}"%',))
            for r in cur.fetchall():
                if r[0] not in group_ids:
                    groups.append({"id": r[0], "gname": r[1]})
                    group_ids.add(r[0])
            
            # Also check device_video_shop_group for group associations
            cur.execute("""
                SELECT DISTINCT g.id, g.gname
                FROM public.device_layout dl
                JOIN public.device d ON d.id = dl.did
                JOIN public.device_video_shop_group dvsg ON dvsg.did = d.id
                JOIN public."group" g ON g.id = dvsg.gid
                WHERE dl.layout_config LIKE %s AND dvsg.gid IS NOT NULL
                ORDER BY g.gname;
            """, (f'%"{ad_name}"%',))
            for r in cur.fetchall():
                if r[0] not in group_ids:
                    groups.append({"id": r[0], "gname": r[1]})
                    group_ids.add(r[0])
            
            # Sort by gname
            groups.sort(key=lambda x: x["gname"])
            
            return {
                "ad_name": ad_name,
                "groups": groups,
                "group_count": len(groups)
            }


# --- Advertisement Names List (for dropdowns) ---
@app.get("/advertisement_names")
def list_advertisement_names():
    """List all advertisement names (for dropdowns)."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ad_name FROM public.advertisement ORDER BY ad_name;")
            return [r[0] for r in cur.fetchall()]


# ---------- Group Advertisement Linking ----------

class GroupAdvertisementsUpdateIn(BaseModel):
    ad_names: List[str]

class GroupAdvertisementsUpdateOut(BaseModel):
    gid: Optional[int]
    gname: Optional[str]
    aids: List[int]
    ad_names: List[str]
    inserted_count: int
    deleted_count: int
    updated_count: int


def _resolve_advertisement_ids(conn, ad_names: List[str]) -> List[int]:
    """Resolve advertisement names to IDs."""
    if not ad_names:
        return []
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, ad_name FROM public.advertisement WHERE ad_name = ANY(%s);",
            (ad_names,)
        )
        rows = cur.fetchall()
        name_to_id = {r[1]: r[0] for r in rows}
        result = []
        for name in ad_names:
            if name in name_to_id:
                result.append(name_to_id[name])
            else:
                raise HTTPException(status_code=404, detail=f"Advertisement '{name}' not found")
        return result


def _set_group_advertisements(conn, gid: int, aids: List[int]):
    """Link advertisements to a group via the group_advertisement table."""
    with conn.cursor() as cur:
        # Get current advertisements for this group
        cur.execute("SELECT aid FROM public.group_advertisement WHERE gid = %s;", (gid,))
        existing = set(r[0] for r in cur.fetchall())
        new_set = set(aids)
        
        to_insert = new_set - existing
        to_delete = existing - new_set
        
        inserted = 0
        deleted = 0
        
        # Insert new links
        for i, aid in enumerate(aids):
            if aid in to_insert:
                cur.execute(
                    "INSERT INTO public.group_advertisement (gid, aid, display_order) VALUES (%s, %s, %s) ON CONFLICT (gid, aid) DO NOTHING;",
                    (gid, aid, i)
                )
                inserted += cur.rowcount
        
        # Delete removed links
        if to_delete:
            cur.execute(
                "DELETE FROM public.group_advertisement WHERE gid = %s AND aid = ANY(%s);",
                (gid, list(to_delete))
            )
            deleted += cur.rowcount
        
        # Get group name and ad names
        cur.execute("SELECT gname FROM public.\"group\" WHERE id = %s;", (gid,))
        gname = cur.fetchone()[0]
        
        cur.execute("SELECT ad_name FROM public.advertisement WHERE id = ANY(%s);", (aids,))
        ad_names = [r[0] for r in cur.fetchall()]
        
        return inserted, deleted, 0, gname, ad_names


@app.post("/group/{gname}/advertisements", response_model=GroupAdvertisementsUpdateOut)
def set_group_advertisements_by_names(gname: str, body: GroupAdvertisementsUpdateIn):
    """Link advertisements to a group by names."""
    with pg_conn() as conn:
        try:
            aids = _resolve_advertisement_ids(conn, body.ad_names)
            gid = _resolve_group_id(conn, gname)
            ins, dele, upd, gname_res, ad_names = _set_group_advertisements(conn, gid, aids)
            conn.commit()
            return GroupAdvertisementsUpdateOut(
                gid=gid, gname=gname_res, aids=aids, ad_names=ad_names,
                inserted_count=ins, deleted_count=dele, updated_count=upd
            )
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/group/{gname}/advertisements")
def list_group_advertisements_by_name(gname: str):
    """List advertisements linked to a group."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            # Get group ID
            cur.execute("SELECT id FROM public.\"group\" WHERE gname = %s;", (gname,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Group '{gname}' not found")
            gid = row[0]
            
            # Get linked advertisements from group_advertisement table
            cur.execute("""
                SELECT a.id, a.ad_name, a.s3_link, a.rotation, a.fit_mode, a.display_duration
                FROM public.group_advertisement ga
                JOIN public.advertisement a ON ga.aid = a.id
                WHERE ga.gid = %s
                ORDER BY ga.display_order;
            """, (gid,))
            
            ads = []
            ad_names_set = set()
            for r in cur.fetchall():
                ads.append({
                    "id": r[0],
                    "ad_name": r[1],
                    "s3_link": r[2],
                    "rotation": r[3],
                    "fit_mode": r[4],
                    "display_duration": r[5],
                })
                ad_names_set.add(r[1])
            
            # Also check device_layout for devices in this group that have ads in their layout_config
            cur.execute("""
                SELECT DISTINCT dl.layout_config
                FROM public.device_layout dl
                JOIN public.device d ON d.id = dl.did
                LEFT JOIN public.device_assignment da ON da.did = d.id
                LEFT JOIN public.device_video_shop_group dvsg ON dvsg.did = d.id
                WHERE (da.gid = %s OR dvsg.gid = %s) AND dl.layout_config IS NOT NULL;
            """, (gid, gid))
            
            # Parse layout_configs to find ad_names
            found_ad_names = set()
            for row in cur.fetchall():
                if row[0]:
                    try:
                        config = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                        if isinstance(config, list):
                            for slot in config:
                                if slot.get("ad_name") and slot.get("content_type") == "image":
                                    found_ad_names.add(slot["ad_name"])
                    except:
                        pass
            
            # Get details for any new ad_names found
            new_ad_names = found_ad_names - ad_names_set
            if new_ad_names:
                cur.execute("""
                    SELECT id, ad_name, s3_link, rotation, fit_mode, display_duration
                    FROM public.advertisement
                    WHERE ad_name = ANY(%s);
                """, (list(new_ad_names),))
                for r in cur.fetchall():
                    ads.append({
                        "id": r[0],
                        "ad_name": r[1],
                        "s3_link": r[2],
                        "rotation": r[3],
                        "fit_mode": r[4],
                        "display_duration": r[5],
                    })
            
            return {"gname": gname, "ad_names": [a["ad_name"] for a in ads], "advertisements": ads}


# ---------- Device Active Status Endpoints ----------
@app.get("/device/{mobile_id}/active-status")
def get_device_active_status(mobile_id: str):
    """
    Get the active status of a device.
    Returns is_active = True/False
    """
    try:
        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT is_active FROM public.device 
                    WHERE mobile_id = %s 
                    ORDER BY id DESC LIMIT 1;
                """, (mobile_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail=f"Device not found: {mobile_id}")
                return {"mobile_id": mobile_id, "is_active": row[0] if row[0] is not None else True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/device/{mobile_id}/active-status")
def set_device_active_status(mobile_id: str, body: DeviceActiveStatusIn):
    """
    Set the active status of a device.
    - When is_active = False: Device will show "Not Enrolled" screen
    - When is_active = True: Device works normally
    """
    try:
        with pg_conn() as conn:
            with conn.cursor() as cur:
                # Check device exists
                cur.execute("""
                    SELECT id FROM public.device 
                    WHERE mobile_id = %s 
                    ORDER BY id DESC LIMIT 1;
                """, (mobile_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail=f"Device not found: {mobile_id}")
                
                did = row[0]
                
                # Update is_active status and set needs_refresh to trigger app restart
                cur.execute("""
                    UPDATE public.device 
                    SET is_active = %s, needs_refresh = TRUE, updated_at = NOW() 
                    WHERE id = %s;
                """, (body.is_active, did))
                
                conn.commit()
                
                status_text = "activated" if body.is_active else "deactivated"
                return {
                    "mobile_id": mobile_id,
                    "is_active": body.is_active,
                    "message": f"Device {status_text} successfully"
                }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/device/{mobile_id}/refresh")
def trigger_device_refresh(mobile_id: str):
    """
    Trigger a refresh/restart of the device app.
    The device will restart on next heartbeat.
    """
    try:
        with pg_conn() as conn:
            with conn.cursor() as cur:
                # Check device exists
                cur.execute("""
                    SELECT id FROM public.device 
                    WHERE mobile_id = %s 
                    ORDER BY id DESC LIMIT 1;
                """, (mobile_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail=f"Device not found: {mobile_id}")
                
                did = row[0]
                
                # Set needs_refresh flag
                cur.execute("""
                    UPDATE public.device 
                    SET needs_refresh = TRUE, updated_at = NOW() 
                    WHERE id = %s;
                """, (did,))
                
                conn.commit()
                
                return {
                    "mobile_id": mobile_id,
                    "message": "Refresh signal sent. Device will restart on next heartbeat (within 10 seconds)."
                }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/device/{mobile_id}/unassign-from-group")
def unassign_device_from_group(mobile_id: str):
    """
    Unassign a device from its current group.
    This will:
    1. Remove the device from device_assignment table
    2. Remove all video links from device_video_shop_group table
    3. Clear the device_layout config
    4. Mark device for re-download (download_status = FALSE)
    """
    try:
        with pg_conn() as conn:
            with conn.cursor() as cur:
                # Get device ID
                cur.execute("""
                    SELECT id FROM public.device 
                    WHERE mobile_id = %s 
                    ORDER BY id DESC LIMIT 1;
                """, (mobile_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail=f"Device not found: {mobile_id}")
                
                did = row[0]
                
                # Get current group info before unassigning
                cur.execute("""
                    SELECT g.gname 
                    FROM public.device_assignment da
                    JOIN public."group" g ON g.id = da.gid
                    WHERE da.did = %s;
                """, (did,))
                group_row = cur.fetchone()
                old_group = group_row[0] if group_row else None
                
                # Delete from device_assignment
                cur.execute("DELETE FROM public.device_assignment WHERE did = %s;", (did,))
                deleted_assignment = cur.rowcount
                
                # Delete from device_video_shop_group (video links)
                cur.execute("DELETE FROM public.device_video_shop_group WHERE did = %s;", (did,))
                deleted_links = cur.rowcount
                
                # Clear device_layout config
                cur.execute("DELETE FROM public.device_layout WHERE did = %s;", (did,))
                deleted_layout = cur.rowcount
                
                # Mark device for re-download
                cur.execute("""
                    UPDATE public.device 
                    SET download_status = FALSE, updated_at = NOW() 
                    WHERE id = %s;
                """, (did,))
                
                conn.commit()
                
                message = f"Device unassigned"
                if old_group:
                    message = f"Device unassigned from group '{old_group}'"
                
                return {
                    "mobile_id": mobile_id,
                    "message": message,
                    "deleted_assignment": deleted_assignment > 0,
                    "deleted_video_links": deleted_links,
                    "deleted_layout": deleted_layout > 0,
                    "old_group": old_group
                }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════════════════
# MERGED CRUD ENDPOINTS — device, group, shop, video basic CRUD
# These were previously in separate services (device.py, group.py,
# shop_service.py, video_service.py). Merged here for single-container deploy.
# ═══════════════════════════════════════════════════════════════════════════

# --- Models for standalone CRUD ---
class StandaloneDeviceRequest(BaseModel):
    mobile_id: str
    download_status: bool = False

class StandaloneDeviceUpdate(BaseModel):
    mobile_id: Optional[str] = None
    download_status: Optional[bool] = None

class StandaloneGroupCreate(BaseModel):
    gname: str

class StandaloneGroupUpdate(BaseModel):
    gname: Optional[str] = None

class StandaloneShopCreate(BaseModel):
    shop_name: str

class StandaloneShopUpdate(BaseModel):
    shop_name: Optional[str] = None

class StandaloneVideoUpdate(BaseModel):
    video_name: Optional[str] = None
    s3_link: Optional[str] = None
    rotation: Optional[int] = None
    content_type: Optional[str] = None
    fit_mode: Optional[str] = None
    display_duration: Optional[int] = None

class StandaloneRotationUpdate(BaseModel):
    rotation: int

class StandaloneFitModeUpdate(BaseModel):
    fit_mode: str

# --- S3 config for video uploads ---
S3_VIDEO_PREFIX = os.getenv("S3_PREFIX", "myvideos").strip("/")
S3_VIDEO_FORCED_EXT = ".mp4"

def _video_slug(s: str) -> str:
    import re as _re
    s = s.strip()
    s = _re.sub(r"[^\w\-.]+", "-", s, flags=_re.UNICODE)
    return _re.sub(r"-{2,}", "-", s).strip("-").lower()

def _make_video_s3_key(video_name: str, tenant_slug: str = None) -> str:
    base = _video_slug(video_name)
    filename = f"{base}{S3_VIDEO_FORCED_EXT}"
    if tenant_slug:
        return f"tenants/{tenant_slug}/videos/{filename}"
    return f"{S3_VIDEO_PREFIX}/{filename}".strip("/") if S3_VIDEO_PREFIX else filename

def _to_video_s3_uri(key: str) -> str:
    return f"s3://{S3_BUCKET}/{key}"

def _detect_video_content_type(filename: str) -> str:
    from pathlib import Path as _P
    ext = _P(filename).suffix.lower()
    video_exts = {'.mp4', '.webm', '.mov', '.avi', '.mkv'}
    image_exts = {'.jpg', '.jpeg', '.png', '.gif', '.webp'}
    if ext in video_exts: return "video"
    if ext in image_exts: return "image"
    if ext in {'.html', '.htm'}: return "html"
    if ext == '.pdf': return "pdf"
    return "video"

def _video_s3_key_exists(key: str, bucket: str = None) -> bool:
    s3 = boto3.client("s3", region_name=AWS_REGION) if AWS_REGION else boto3.client("s3")
    b = bucket or S3_BUCKET
    try:
        s3.head_object(Bucket=b, Key=key)
        return True
    except Exception:
        return False

def _video_presign(bucket: str, key: str, expires: int = 3600) -> str:
    s3 = boto3.client("s3", region_name=AWS_REGION) if AWS_REGION else boto3.client("s3")
    return s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires)

def _parse_video_s3_link(s3_link: str):
    s3_link = (s3_link or "").strip()
    if not s3_link:
        return (S3_BUCKET, "")
    if s3_link.startswith("s3://"):
        rest = s3_link[len("s3://"):]
        parts = rest.split("/", 1)
        return (parts[0], parts[1]) if len(parts) == 2 else (parts[0], "")
    return (S3_BUCKET, s3_link)


# ═══════════════ DEVICE CRUD ═══════════════

@app.post("/insert_device")
def standalone_insert_device(req: StandaloneDeviceRequest, user: Dict = Depends(get_current_user)):
    mobile = (req.mobile_id or "").strip()
    if not mobile:
        raise HTTPException(status_code=400, detail="mobile_id is required")
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id") or 1
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO public.device (mobile_id, download_status, tenant_id) VALUES (%s, %s, %s) RETURNING id;",
                        (mobile, bool(req.download_status), tenant_id))
            new_id = cur.fetchone()[0]
        conn.commit()
    return {"id": new_id, "message": "Device inserted successfully"}

@app.get("/device/{mobile_id}")
def standalone_get_device(mobile_id: str = Path(...)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, mobile_id, download_status, created_at, updated_at FROM public.device WHERE mobile_id = %s ORDER BY id DESC LIMIT 1;", (mobile_id,))
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Device not found")
    return {"id": row[0], "mobile_id": row[1], "download_status": row[2], "created_at": row[3], "updated_at": row[4]}

@app.get("/devices")
def standalone_list_devices(
    q: Optional[str] = Query(None, description="Search mobile_id or device_name (ILIKE)"),
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    user: Dict = Depends(get_current_user),
):
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id")
    with pg_conn() as conn:
        with conn.cursor() as cur:
            # Build tenant filter
            tenant_filter = "d.tenant_id = %s" if tenant_id else "TRUE"
            tenant_params = [tenant_id] if tenant_id else []

            if q:
                cur.execute(f"SELECT COUNT(*) FROM public.device d WHERE {tenant_filter} AND (d.mobile_id ILIKE %s OR d.device_name ILIKE %s);",
                            tenant_params + [f"%{q}%", f"%{q}%"])
            else:
                cur.execute(f"SELECT COUNT(*) FROM public.device d WHERE {tenant_filter};", tenant_params)
            total = int(cur.fetchone()[0])

            base_sql = f"""
                SELECT d.id, d.mobile_id, d.download_status, d.created_at, d.updated_at,
                       d.resolution, d.device_name,
                       COALESCE(g1.gname, g2.gname) as group_name, d.is_active
                FROM public.device d
                LEFT JOIN public.device_assignment da ON da.did = d.id
                LEFT JOIN public."group" g1 ON g1.id = da.gid
                LEFT JOIN (
                    SELECT DISTINCT ON (dvsg.did) dvsg.did, g.gname
                    FROM public.device_video_shop_group dvsg
                    JOIN public."group" g ON g.id = dvsg.gid
                    WHERE dvsg.gid IS NOT NULL
                ) g2 ON g2.did = d.id AND g1.gname IS NULL
                WHERE {tenant_filter}
            """
            if q:
                cur.execute(base_sql + " AND (d.mobile_id ILIKE %s OR d.device_name ILIKE %s) ORDER BY d.id DESC LIMIT %s OFFSET %s;",
                            tenant_params + [f"%{q}%", f"%{q}%", limit, offset])
            else:
                cur.execute(base_sql + " ORDER BY d.id DESC LIMIT %s OFFSET %s;", tenant_params + [limit, offset])
            rows = cur.fetchall()

    items = [
        {"id": r[0], "mobile_id": r[1], "download_status": r[2], "created_at": r[3], "updated_at": r[4],
         "resolution": r[5] if len(r) > 5 else None, "device_name": r[6] if len(r) > 6 else None,
         "group_name": r[7] if len(r) > 7 else None,
         "is_active": r[8] if len(r) > 8 and r[8] is not None else True}
        for r in rows
    ]
    return {"items": items, "total": total, "count": len(items), "limit": limit, "offset": offset, "query": q}

@app.put("/device/{mobile_id}")
def standalone_update_device(mobile_id: str, patch: StandaloneDeviceUpdate):
    sets, params = [], []
    if patch.mobile_id is not None:
        sets.append("mobile_id = %s"); params.append(patch.mobile_id)
    if patch.download_status is not None:
        sets.append("download_status = %s"); params.append(patch.download_status)
    if not sets:
        return standalone_get_device(mobile_id)
    params.append(mobile_id)
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE public.device SET {', '.join(sets)} WHERE mobile_id = %s RETURNING id, mobile_id, download_status, created_at, updated_at;", params)
            row = cur.fetchone()
        conn.commit()
    if not row:
        raise HTTPException(status_code=404, detail="Device not found or no changes")
    return {"message": "Device updated", "item": {"id": row[0], "mobile_id": row[1], "download_status": row[2], "created_at": row[3], "updated_at": row[4]}}

@app.delete("/device/{mobile_id}")
def standalone_delete_device(mobile_id: str):
    # Check for links first
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM public.device WHERE mobile_id = %s ORDER BY id DESC;", (mobile_id,))
            device_ids = [r[0] for r in cur.fetchall()]
            if not device_ids:
                raise HTTPException(status_code=404, detail="Device not found")
            cur.execute("SELECT COUNT(*) FROM public.device_video_shop_group WHERE did = ANY(%s::bigint[]);", (device_ids,))
            linked_count = int(cur.fetchone()[0])
            if linked_count > 0:
                raise HTTPException(status_code=409, detail=f"Device '{mobile_id}' has {linked_count} link(s). Remove links first.")
            cur.execute("DELETE FROM public.device WHERE mobile_id = %s RETURNING id;", (mobile_id,))
            deleted = cur.fetchall()
        conn.commit()
    return {"deleted_count": len(deleted)}


# ═══════════════ GROUP CRUD ═══════════════

@app.post("/insert_group")
def standalone_insert_group(req: StandaloneGroupCreate, user: Dict = Depends(get_current_user)):
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id") or 1
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('INSERT INTO public."group" (gname, tenant_id) VALUES (%s, %s) RETURNING id;', (req.gname, tenant_id))
            new_id = cur.fetchone()[0]
        conn.commit()
    return {"id": new_id, "message": "Group inserted successfully"}

@app.get("/group/{gname}")
def standalone_get_group(gname: str = Path(...)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT id, gname, created_at, updated_at FROM public."group" WHERE gname = %s ORDER BY id DESC LIMIT 1;', (gname,))
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Group not found")
    return {"id": row[0], "gname": row[1], "created_at": row[2], "updated_at": row[3]}

@app.get("/groups")
def standalone_list_groups(
    q: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    user: Dict = Depends(get_current_user),
):
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id")
    with pg_conn() as conn:
        with conn.cursor() as cur:
            t_filter = 'tenant_id = %s AND' if tenant_id else ''
            t_params = [tenant_id] if tenant_id else []
            if q:
                cur.execute(f'SELECT id, gname, created_at, updated_at FROM public."group" WHERE {t_filter} gname ILIKE %s ORDER BY id DESC LIMIT %s OFFSET %s;',
                            t_params + [f"%{q}%", limit, offset])
            else:
                if tenant_id:
                    cur.execute(f'SELECT id, gname, created_at, updated_at FROM public."group" WHERE tenant_id = %s ORDER BY id DESC LIMIT %s OFFSET %s;',
                                (tenant_id, limit, offset))
                else:
                    cur.execute('SELECT id, gname, created_at, updated_at FROM public."group" ORDER BY id DESC LIMIT %s OFFSET %s;', (limit, offset))
            rows = cur.fetchall()
    items = [{"id": r[0], "gname": r[1], "created_at": r[2], "updated_at": r[3]} for r in rows]
    return {"count": len(items), "items": items, "limit": limit, "offset": offset, "query": q}

@app.put("/group/{gname}")
def standalone_update_group(gname: str, patch: StandaloneGroupUpdate):
    if not patch.gname:
        return standalone_get_group(gname)
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('UPDATE public."group" SET gname = %s WHERE gname = %s RETURNING id, gname, created_at, updated_at;', (patch.gname, gname))
            row = cur.fetchone()
        conn.commit()
    if not row:
        raise HTTPException(status_code=404, detail="Group not found or no changes")
    return {"message": "Group updated", "item": {"id": row[0], "gname": row[1], "created_at": row[2], "updated_at": row[3]}}

@app.delete("/group/{gname}")
def standalone_delete_group(gname: str, force: bool = Query(False)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT id FROM public."group" WHERE gname = %s ORDER BY id DESC LIMIT 1;', (gname,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Group not found")
            gid = row[0]
            # Check device attachments
            cur.execute("SELECT COUNT(*) FROM public.device_assignment WHERE gid = %s;", (gid,))
            dev_count = int(cur.fetchone()[0])
            if dev_count > 0 and not force:
                raise HTTPException(status_code=409, detail=f"Group has {dev_count} device(s) attached. Use ?force=true to delete.")
            if force:
                cur.execute("DELETE FROM public.device_assignment WHERE gid = %s;", (gid,))
                cur.execute("DELETE FROM public.device_video_shop_group WHERE gid = %s;", (gid,))
                cur.execute("DELETE FROM public.group_video WHERE gid = %s;", (gid,))
                cur.execute("DELETE FROM public.group_advertisement WHERE gid = %s;", (gid,))
            cur.execute("DELETE FROM public.group_video WHERE gid = %s;", (gid,))
            cur.execute('DELETE FROM public."group" WHERE gname = %s RETURNING id;', (gname,))
            deleted = cur.fetchall()
        conn.commit()
    return {"deleted_count": len(deleted), "devices_unassigned": dev_count if force else 0}

@app.post("/group/{gname}/unassign-devices")
def standalone_unassign_group_devices(gname: str):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT id FROM public."group" WHERE gname = %s ORDER BY id DESC LIMIT 1;', (gname,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Group not found")
            gid = row[0]
            cur.execute("DELETE FROM public.device_assignment WHERE gid = %s;", (gid,))
            a_count = cur.rowcount
            cur.execute("DELETE FROM public.device_video_shop_group WHERE gid = %s;", (gid,))
            l_count = cur.rowcount
        conn.commit()
    return {"unassigned_count": a_count + l_count, "message": f"Unassigned {a_count + l_count} device(s) from group '{gname}'"}


# ═══════════════ SHOP CRUD ═══════════════

@app.post("/insert_shop")
def standalone_insert_shop(req: StandaloneShopCreate, user: Dict = Depends(get_current_user)):
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id") or 1
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO public.shop (shop_name, tenant_id) VALUES (%s, %s) RETURNING id;", (req.shop_name, tenant_id))
            new_id = cur.fetchone()[0]
        conn.commit()
    return {"id": new_id, "message": "Shop inserted successfully"}

@app.get("/shop/{shop_name}")
def standalone_get_shop(shop_name: str = Path(...)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, shop_name, created_at, updated_at FROM public.shop WHERE shop_name = %s ORDER BY id DESC LIMIT 1;", (shop_name,))
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Shop not found")
    return {"id": row[0], "shop_name": row[1], "created_at": row[2], "updated_at": row[3]}

@app.get("/shops")
def standalone_list_shops(
    q: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    user: Dict = Depends(get_current_user),
):
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id")
    with pg_conn() as conn:
        with conn.cursor() as cur:
            if tenant_id:
                if q:
                    cur.execute("SELECT id, shop_name, created_at, updated_at FROM public.shop WHERE tenant_id = %s AND shop_name ILIKE %s ORDER BY id DESC LIMIT %s OFFSET %s;",
                                (tenant_id, f"%{q}%", limit, offset))
                else:
                    cur.execute("SELECT id, shop_name, created_at, updated_at FROM public.shop WHERE tenant_id = %s ORDER BY id DESC LIMIT %s OFFSET %s;", (tenant_id, limit, offset))
            else:
                if q:
                    cur.execute("SELECT id, shop_name, created_at, updated_at FROM public.shop WHERE shop_name ILIKE %s ORDER BY id DESC LIMIT %s OFFSET %s;",
                                (f"%{q}%", limit, offset))
                else:
                    cur.execute("SELECT id, shop_name, created_at, updated_at FROM public.shop ORDER BY id DESC LIMIT %s OFFSET %s;", (limit, offset))
            rows = cur.fetchall()
    items = [{"id": r[0], "shop_name": r[1], "created_at": r[2], "updated_at": r[3]} for r in rows]
    return {"count": len(items), "items": items, "limit": limit, "offset": offset, "query": q}

@app.put("/shop/{shop_name}")
def standalone_update_shop(shop_name: str, patch: StandaloneShopUpdate):
    if not patch.shop_name:
        return standalone_get_shop(shop_name)
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE public.shop SET shop_name = %s WHERE shop_name = %s RETURNING id, shop_name, created_at, updated_at;",
                        (patch.shop_name, shop_name))
            row = cur.fetchone()
        conn.commit()
    if not row:
        raise HTTPException(status_code=404, detail="Shop not found or no changes")
    return {"message": "Shop updated", "item": {"id": row[0], "shop_name": row[1], "created_at": row[2], "updated_at": row[3]}}

@app.delete("/shop/{shop_name}")
def standalone_delete_shop(shop_name: str):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM public.shop WHERE shop_name = %s RETURNING id;", (shop_name,))
            deleted = cur.fetchall()
        conn.commit()
    if not deleted:
        raise HTTPException(status_code=404, detail="Shop not found")
    return {"deleted_count": len(deleted)}


# ═══════════════ VIDEO CRUD ═══════════════

@app.post("/upload_video")
async def standalone_upload_video(
    file: UploadFile = File(...),
    video_name: str = Form(...),
    overwrite: bool = Form(False),
    rotation: int = Form(0),
    fit_mode: str = Form("cover"),
    display_duration: int = Form(10),
    user: Dict = Depends(get_current_user),
):
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id")
    key = _make_video_s3_key(video_name)
    if not overwrite and _video_s3_key_exists(key):
        raise HTTPException(status_code=409, detail="Video exists. Use overwrite=true.")
    content_type = _detect_video_content_type(file.filename or video_name)
    s3 = boto3.client("s3", region_name=AWS_REGION) if AWS_REGION else boto3.client("s3")
    from boto3.s3.transfer import TransferConfig
    cfg = TransferConfig(multipart_threshold=8*1024*1024, multipart_chunksize=16*1024*1024, max_concurrency=8, use_threads=True)
    s3.upload_fileobj(file.file, S3_BUCKET, key, ExtraArgs={"ACL": "private", "ServerSideEncryption": "AES256", "ContentType": "video/mp4"}, Config=cfg)
    s3_uri = _to_video_s3_uri(key)
    with pg_conn() as conn:
        with conn.cursor() as cur:
            if tenant_id:
                cur.execute("""
                    INSERT INTO public.video (video_name, s3_link, rotation, content_type, fit_mode, display_duration, tenant_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (video_name, s3_uri, rotation, content_type, fit_mode, display_duration, tenant_id))
                if cur.rowcount == 0:
                    cur.execute("""
                        UPDATE public.video SET s3_link = %s, rotation = %s,
                            content_type = %s, fit_mode = %s,
                            display_duration = %s, updated_at = NOW()
                        WHERE video_name = %s AND tenant_id = %s RETURNING id;
                    """, (s3_uri, rotation, content_type, fit_mode, display_duration, video_name, tenant_id))
                else:
                    cur.execute("SELECT id FROM public.video WHERE video_name = %s AND tenant_id = %s ORDER BY id DESC LIMIT 1;", (video_name, tenant_id))
            else:
                cur.execute("""
                    INSERT INTO public.video (video_name, s3_link, rotation, content_type, fit_mode, display_duration)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (video_name, s3_uri, rotation, content_type, fit_mode, display_duration))
                if cur.rowcount == 0:
                    cur.execute("""
                        UPDATE public.video SET s3_link = %s, rotation = %s,
                            content_type = %s, fit_mode = %s,
                            display_duration = %s, updated_at = NOW()
                        WHERE video_name = %s RETURNING id;
                    """, (s3_uri, rotation, content_type, fit_mode, display_duration, video_name))
                else:
                    cur.execute("SELECT id FROM public.video WHERE video_name = %s ORDER BY id DESC LIMIT 1;", (video_name,))
            new_id = cur.fetchone()[0]
        conn.commit()
    return {"id": new_id, "video_name": video_name, "s3_link": s3_uri, "key": key,
            "rotation": rotation, "content_type": content_type, "fit_mode": fit_mode,
            "display_duration": display_duration, "overwrote": overwrite}

@app.get("/videos")
def standalone_list_videos(
    q: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    user: Dict = Depends(get_current_user),
):
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id")
    with pg_conn() as conn:
        with conn.cursor() as cur:
            base = "SELECT id, video_name, s3_link, rotation, content_type, fit_mode, display_duration, created_at, updated_at FROM public.video"
            if tenant_id:
                if q:
                    cur.execute(base + " WHERE tenant_id = %s AND video_name ILIKE %s ORDER BY id DESC LIMIT %s OFFSET %s", (tenant_id, f"%{q}%", limit, offset))
                else:
                    cur.execute(base + " WHERE tenant_id = %s ORDER BY id DESC LIMIT %s OFFSET %s", (tenant_id, limit, offset))
            else:
                if q:
                    cur.execute(base + " WHERE video_name ILIKE %s ORDER BY id DESC LIMIT %s OFFSET %s", (f"%{q}%", limit, offset))
                else:
                    cur.execute(base + " ORDER BY id DESC LIMIT %s OFFSET %s", (limit, offset))
            rows = cur.fetchall()
    items = [{"id": r[0], "video_name": r[1], "s3_link": r[2], "rotation": r[3],
              "content_type": r[4], "fit_mode": r[5], "display_duration": r[6],
              "created_at": r[7], "updated_at": r[8]} for r in rows]
    return {"count": len(items), "items": items, "limit": limit, "offset": offset, "query": q}

@app.put("/video/{video_name}")
def standalone_update_video(video_name: str, patch: StandaloneVideoUpdate):
    sets, params = [], []
    if patch.video_name is not None: sets.append("video_name = %s"); params.append(patch.video_name)
    if patch.s3_link is not None: sets.append("s3_link = %s"); params.append(patch.s3_link)
    if patch.rotation is not None: sets.append("rotation = %s"); params.append(patch.rotation)
    if patch.content_type is not None: sets.append("content_type = %s"); params.append(patch.content_type)
    if patch.fit_mode is not None: sets.append("fit_mode = %s"); params.append(patch.fit_mode)
    if patch.display_duration is not None: sets.append("display_duration = %s"); params.append(patch.display_duration)
    if not sets:
        raise HTTPException(status_code=400, detail="No fields to update")
    params.append(video_name)
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE public.video SET {', '.join(sets)}, updated_at = NOW() WHERE video_name = %s RETURNING id, video_name, s3_link, rotation, content_type, fit_mode, display_duration, created_at, updated_at;", params)
            row = cur.fetchone()
        conn.commit()
    if not row:
        raise HTTPException(status_code=404, detail="Video not found")
    return {"message": "Updated", "item": {"id": row[0], "video_name": row[1], "s3_link": row[2], "rotation": row[3],
            "content_type": row[4], "fit_mode": row[5], "display_duration": row[6], "created_at": row[7], "updated_at": row[8]}}

@app.delete("/video/{video_name}")
def standalone_delete_video(video_name: str):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM public.video WHERE video_name = %s RETURNING id;", (video_name,))
            deleted = cur.fetchall()
        conn.commit()
    if not deleted:
        raise HTTPException(status_code=404, detail="Video not found")
    return {"deleted_count": len(deleted)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "device_video_shop_group:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8005")),
        reload=True,
    )
