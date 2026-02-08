# platform_api.py
# B2B Platform API endpoints.
# Mounted as a sub-router on the main FastAPI app under /platform.
# Works with the actual schema: company(id, name, slug, is_active, created_at, updated_at)
# and company_id FK on entity tables.

import json
import secrets
import string
import re
from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query, Depends, Header
from pydantic import BaseModel, Field

from tenant_context import (
    TenantContext, get_tenant_context, require_platform_user,
    hash_password, start_impersonation, stop_impersonation,
    active_sessions,
)

router = APIRouter()


# ── Pydantic Models ──

class CompanyCreateIn(BaseModel):
    slug: str = Field(..., min_length=2, max_length=50, pattern=r'^[a-z0-9][a-z0-9\-]*[a-z0-9]$')
    name: str = Field(..., min_length=1, max_length=255)
    admin_username: str = Field(..., min_length=3, max_length=100)
    admin_email: Optional[str] = None
    admin_full_name: Optional[str] = None

class CompanyUpdateIn(BaseModel):
    name: Optional[str] = None
    slug: Optional[str] = None
    is_active: Optional[bool] = None

class CompanyOut(BaseModel):
    id: int
    slug: str
    name: str
    is_active: bool
    status: str = "active"  # Computed from is_active for frontend compat
    created_at: datetime
    updated_at: datetime

class CompanyListOut(BaseModel):
    items: List[CompanyOut]
    total: int

class CompanyStatsOut(BaseModel):
    company_id: int
    slug: str
    name: str
    device_count: int
    user_count: int
    video_count: int
    advertisement_count: int
    group_count: int
    shop_count: int

class ImpersonateIn(BaseModel):
    company_slug: str

class PlatformDashboardOut(BaseModel):
    total_companies: int = 0
    active_companies: int = 0
    inactive_companies: int = 0
    total_devices: int = 0
    online_devices: int = 0
    offline_devices: int = 0
    total_videos: int = 0
    total_advertisements: int = 0
    total_users: int = 0
    total_groups: int = 0
    total_shops: int = 0
    total_storage_used_mb: float = 0
    companies: list = []


COMPANY_COLUMNS = "id, name, slug, is_active, created_at, updated_at"

def _row_to_company(r) -> CompanyOut:
    is_active = r[3]
    return CompanyOut(
        id=r[0], name=r[1], slug=r[2], is_active=is_active,
        status="active" if is_active else "suspended",
        created_at=r[4], updated_at=r[5],
    )


def _get_pg_conn():
    from device_video_shop_group import pg_conn
    return pg_conn


def _temp_password(length: int = 12) -> str:
    chars = string.ascii_letters + string.digits
    return ''.join(secrets.choice(chars) for _ in range(length))


# ══════════════════════════════════════════════════════════════
# COMPANY MANAGEMENT (Platform Users Only)
# ══════════════════════════════════════════════════════════════

@router.post("/companies", response_model=dict)
def create_company(body: CompanyCreateIn, ctx: TenantContext = Depends(require_platform_user)):
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM public.company WHERE slug = %s;", (body.slug,))
                if cur.fetchone():
                    raise HTTPException(status_code=400, detail=f"Slug '{body.slug}' already exists")

                # Check if admin username is already taken
                cur.execute("SELECT id FROM public.users WHERE username = %s;", (body.admin_username.lower(),))
                if cur.fetchone():
                    raise HTTPException(status_code=400, detail=f"Username '{body.admin_username.lower()}' already exists")

                # Create company
                cur.execute("""
                    INSERT INTO public.company (slug, name)
                    VALUES (%s, %s)
                    RETURNING id, created_at
                """, (body.slug, body.name))
                company_id, created_at = cur.fetchone()

                # Create admin user for this company
                temp_pw = _temp_password()
                cur.execute("""
                    INSERT INTO public.users
                        (username, email, password_hash, full_name, role, company_id)
                    VALUES (%s, %s, %s, %s, 'admin', %s)
                    RETURNING id
                """, (body.admin_username.lower(),
                      body.admin_email.lower() if body.admin_email else None,
                      hash_password(temp_pw), body.admin_full_name,
                      company_id))
                admin_id = cur.fetchone()[0]

            conn.commit()

            return {
                "company": {"id": company_id, "slug": body.slug, "name": body.name,
                            "is_active": True, "created_at": str(created_at)},
                "admin_user": {"id": admin_id, "username": body.admin_username.lower(),
                               "temp_password": temp_pw},
            }
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/companies", response_model=CompanyListOut)
def list_companies(
    q: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    ctx: TenantContext = Depends(require_platform_user),
):
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        with conn.cursor() as cur:
            wheres, params = [], []
            if q:
                wheres.append("(name ILIKE %s OR slug ILIKE %s)")
                params.extend([f"%{q}%", f"%{q}%"])
            where_sql = f"WHERE {' AND '.join(wheres)}" if wheres else ""

            cur.execute(f"SELECT COUNT(*) FROM public.company {where_sql};", params)
            total = cur.fetchone()[0]

            cur.execute(f"SELECT {COMPANY_COLUMNS} FROM public.company {where_sql} ORDER BY created_at DESC LIMIT %s OFFSET %s",
                        params + [limit, offset])
            return CompanyListOut(items=[_row_to_company(r) for r in cur.fetchall()], total=total)


@router.get("/companies/{slug}", response_model=CompanyOut)
def get_company(slug: str, ctx: TenantContext = Depends(require_platform_user)):
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {COMPANY_COLUMNS} FROM public.company WHERE slug = %s", (slug,))
            r = cur.fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="Company not found")
            return _row_to_company(r)


@router.put("/companies/{slug}", response_model=CompanyOut)
def update_company(slug: str, body: CompanyUpdateIn, ctx: TenantContext = Depends(require_platform_user)):
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                updates, params = [], []
                if body.name is not None:
                    updates.append("name = %s"); params.append(body.name)
                if body.slug is not None:
                    updates.append("slug = %s"); params.append(body.slug)
                if body.is_active is not None:
                    updates.append("is_active = %s"); params.append(body.is_active)
                if not updates:
                    return get_company(slug, ctx)
                params.append(slug)
                cur.execute(f"UPDATE public.company SET {', '.join(updates)}, updated_at = NOW() WHERE slug = %s RETURNING {COMPANY_COLUMNS}", params)
                r = cur.fetchone()
                if not r:
                    raise HTTPException(status_code=404, detail="Company not found")
            conn.commit()
            return _row_to_company(r)
        except HTTPException:
            conn.rollback(); raise
        except Exception as e:
            conn.rollback(); raise HTTPException(status_code=500, detail=str(e))


@router.post("/companies/{slug}/suspend")
def suspend_company(slug: str, ctx: TenantContext = Depends(require_platform_user)):
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE public.company SET is_active=FALSE, updated_at=NOW() WHERE slug=%s AND is_active=TRUE RETURNING id", (slug,))
                r = cur.fetchone()
                if not r:
                    raise HTTPException(status_code=404, detail="Company not found or already suspended")
            conn.commit()
            return {"message": f"Company '{slug}' suspended", "company_id": r[0]}
        except HTTPException:
            conn.rollback(); raise
        except Exception as e:
            conn.rollback(); raise HTTPException(status_code=500, detail=str(e))


@router.post("/companies/{slug}/reactivate")
def reactivate_company(slug: str, ctx: TenantContext = Depends(require_platform_user)):
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE public.company SET is_active=TRUE, updated_at=NOW() WHERE slug=%s AND is_active=FALSE RETURNING id", (slug,))
                r = cur.fetchone()
                if not r:
                    raise HTTPException(status_code=404, detail="Company not found or already active")
            conn.commit()
            return {"message": f"Company '{slug}' reactivated", "company_id": r[0]}
        except HTTPException:
            conn.rollback(); raise
        except Exception as e:
            conn.rollback(); raise HTTPException(status_code=500, detail=str(e))


@router.delete("/companies/{slug}")
def delete_company(slug: str, ctx: TenantContext = Depends(require_platform_user)):
    """Permanently delete a company and ALL its data. Irreversible."""
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id, slug, name FROM public.company WHERE slug=%s", (slug,))
                c = cur.fetchone()
                if not c:
                    raise HTTPException(status_code=404, detail="Company not found")
                cid = c[0]

                # Collect S3 keys before deleting rows
                s3_keys = []
                cur.execute("SELECT s3_link FROM public.video WHERE company_id=%s AND s3_link IS NOT NULL", (cid,))
                s3_keys.extend([r[0] for r in cur.fetchall()])
                cur.execute("SELECT s3_link FROM public.advertisement WHERE company_id=%s AND s3_link IS NOT NULL", (cid,))
                s3_keys.extend([r[0] for r in cur.fetchall()])

                # Delete from all company-scoped tables (order matters for FK constraints)
                tenant_tables = [
                    "device_advertisement_shop_group",
                    "device_video_shop_group",
                    "device_assignment",
                    "group_video",
                    "group_advertisement",
                    "device_layout",
                    "device_logs",
                    "device_temperature",
                    "device_online_history",
                    "count_history",
                    "user_permissions",
                    "device",
                    "video",
                    "advertisement",
                    "shop",
                    '"group"',
                ]
                counts = {}
                for tbl in tenant_tables:
                    try:
                        cur.execute("SAVEPOINT sp_del")
                        cur.execute(f"DELETE FROM public.{tbl} WHERE company_id=%s", (cid,))
                        counts[tbl.strip('"')] = cur.rowcount
                        cur.execute("RELEASE SAVEPOINT sp_del")
                    except Exception:
                        cur.execute("ROLLBACK TO SAVEPOINT sp_del")
                        counts[tbl.strip('"')] = 0

                # Delete users belonging to this company
                cur.execute("DELETE FROM public.users WHERE company_id=%s", (cid,))
                counts["users"] = cur.rowcount

                # Finally delete the company itself
                cur.execute("DELETE FROM public.company WHERE id=%s", (cid,))

            conn.commit()

            # Clean up S3 objects in background (best-effort)
            if s3_keys:
                try:
                    import boto3, os
                    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-2"))
                    bucket = os.getenv("S3_BUCKET")
                    if bucket:
                        for i in range(0, len(s3_keys), 1000):
                            batch = s3_keys[i:i+1000]
                            s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch]})
                except Exception as s3_err:
                    print(f"[WARN] S3 cleanup failed for company {slug}: {s3_err}", flush=True)

            return {"message": f"Company '{slug}' deleted permanently", "deleted_rows": counts}
        except HTTPException:
            conn.rollback(); raise
        except Exception as e:
            conn.rollback(); raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════
# COMPANY STATS
# ══════════════════════════════════════════════════════════════

@router.get("/companies/{slug}/stats", response_model=CompanyStatsOut)
def company_stats(slug: str, ctx: TenantContext = Depends(require_platform_user)):
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, slug, name FROM public.company WHERE slug=%s", (slug,))
            c = cur.fetchone()
            if not c:
                raise HTTPException(status_code=404, detail="Company not found")
            cid = c[0]

            stats = {"company_id": cid, "slug": c[1], "name": c[2]}
            for tbl, col, key in [
                ("device", "company_id", "device_count"),
                ("users", "company_id", "user_count"),
                ("video", "company_id", "video_count"),
                ("advertisement", "company_id", "advertisement_count"),
                ('"group"', "company_id", "group_count"),
                ("shop", "company_id", "shop_count"),
            ]:
                cur.execute(f"SELECT COUNT(*) FROM public.{tbl} WHERE {col}=%s", (cid,))
                stats[key] = cur.fetchone()[0]

            return CompanyStatsOut(**stats)


# ══════════════════════════════════════════════════════════════
# PLATFORM DASHBOARD
# ══════════════════════════════════════════════════════════════

@router.get("/dashboard", response_model=PlatformDashboardOut)
def platform_dashboard(ctx: TenantContext = Depends(require_platform_user)):
    """Aggregated platform dashboard with per-company breakdown."""
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        with conn.cursor() as cur:
            # Global company counts
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE is_active = TRUE) AS active,
                    COUNT(*) FILTER (WHERE is_active = FALSE) AS inactive
                FROM public.company
            """)
            r = cur.fetchone()
            totals = dict(total_companies=r[0], active_companies=r[1], inactive_companies=r[2])

            # Global device counts
            cur.execute("""
                SELECT COUNT(*), COUNT(*) FILTER (WHERE is_online = TRUE),
                       COUNT(*) FILTER (WHERE is_online = FALSE OR is_online IS NULL)
                FROM public.device
            """)
            r = cur.fetchone()
            totals["total_devices"] = r[0]
            totals["online_devices"] = r[1]
            totals["offline_devices"] = r[2]

            # Global entity counts
            cur.execute("SELECT COUNT(*) FROM public.video")
            totals["total_videos"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM public.advertisement")
            totals["total_advertisements"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM public.users")
            totals["total_users"] = cur.fetchone()[0]
            cur.execute('SELECT COUNT(*) FROM public."group"')
            totals["total_groups"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM public.shop")
            totals["total_shops"] = cur.fetchone()[0]

            # Approximate storage
            cur.execute("SELECT COUNT(*) FROM public.video WHERE s3_link IS NOT NULL")
            vid_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM public.advertisement WHERE s3_link IS NOT NULL")
            ad_count = cur.fetchone()[0]
            totals["total_storage_used_mb"] = round(vid_count * 50 + ad_count * 5, 1)

            # Per-company breakdown
            cur.execute("""
                SELECT
                    c.id, c.slug, c.name, c.is_active, c.created_at,
                    COALESCE(d.total, 0)   AS device_count,
                    COALESCE(d.online, 0)  AS devices_online,
                    COALESCE(d.offline, 0) AS devices_offline,
                    COALESCE(v.cnt, 0)     AS video_count,
                    COALESCE(ad.cnt, 0)    AS ad_count,
                    COALESCE(u.cnt, 0)     AS user_count,
                    COALESCE(g.cnt, 0)     AS group_count,
                    COALESCE(s.cnt, 0)     AS shop_count
                FROM public.company c
                LEFT JOIN (
                    SELECT company_id, COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE is_online = TRUE) AS online,
                           COUNT(*) FILTER (WHERE is_online = FALSE OR is_online IS NULL) AS offline
                    FROM public.device GROUP BY company_id
                ) d ON d.company_id = c.id
                LEFT JOIN (SELECT company_id, COUNT(*) AS cnt FROM public.video GROUP BY company_id) v ON v.company_id = c.id
                LEFT JOIN (SELECT company_id, COUNT(*) AS cnt FROM public.advertisement GROUP BY company_id) ad ON ad.company_id = c.id
                LEFT JOIN (SELECT company_id, COUNT(*) AS cnt FROM public.users GROUP BY company_id) u ON u.company_id = c.id
                LEFT JOIN (SELECT company_id, COUNT(*) AS cnt FROM public."group" GROUP BY company_id) g ON g.company_id = c.id
                LEFT JOIN (SELECT company_id, COUNT(*) AS cnt FROM public.shop GROUP BY company_id) s ON s.company_id = c.id
                ORDER BY c.created_at DESC
            """)
            companies = []
            for row in cur.fetchall():
                status = "active" if row[3] else "suspended"
                companies.append({
                    "id": row[0], "slug": row[1], "name": row[2], "status": status,
                    "created_at": str(row[4]),
                    "device_count": row[5], "devices_online": row[6], "devices_offline": row[7],
                    "video_count": row[8], "ad_count": row[9], "user_count": row[10],
                    "group_count": row[11], "shop_count": row[12],
                })

            totals["companies"] = companies
            return totals


# ══════════════════════════════════════════════════════════════
# IMPERSONATION
# ══════════════════════════════════════════════════════════════

@router.post("/impersonate")
def impersonate_company(body: ImpersonateIn, authorization: Optional[str] = Header(None),
                        ctx: TenantContext = Depends(require_platform_user)):
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, slug, name FROM public.company WHERE slug=%s AND is_active=TRUE", (body.company_slug,))
            r = cur.fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="Company not found or inactive")
            token = authorization.replace("Bearer ", "") if authorization and authorization.startswith("Bearer ") else authorization
            start_impersonation(token, r[0], r[1], r[2])
            return {"message": f"Now impersonating '{r[2]}'", "company_id": r[0], "company_slug": r[1]}


@router.post("/stop-impersonate")
def stop_impersonate(authorization: Optional[str] = Header(None),
                     ctx: TenantContext = Depends(require_platform_user)):
    token = authorization.replace("Bearer ", "") if authorization and authorization.startswith("Bearer ") else authorization
    stop_impersonation(token)
    return {"message": "Stopped impersonation"}
