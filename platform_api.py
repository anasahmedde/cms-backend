# platform_api.py
# B2B Platform API endpoints.
# Mounted as a sub-router on the main FastAPI app.

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
    require_permission, require_tenant_context,
    hash_password, create_session, invalidate_tenant_sessions,
    start_impersonation, stop_impersonation, log_audit,
    active_sessions,
)
from migrations.multitenant_schema import clone_roles_for_company

router = APIRouter()


# ── Pydantic Models ──

class CompanyCreateIn(BaseModel):
    slug: str = Field(..., min_length=2, max_length=50, pattern=r'^[a-z0-9][a-z0-9\-]*[a-z0-9]$')
    name: str = Field(..., min_length=1, max_length=255)
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    max_devices: int = 50
    max_users: int = 10
    max_storage_mb: int = 5120
    admin_username: str = Field(..., min_length=3, max_length=100)
    admin_email: Optional[str] = None
    admin_full_name: Optional[str] = None

class CompanyUpdateIn(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    max_devices: Optional[int] = None
    max_users: Optional[int] = None
    max_storage_mb: Optional[int] = None
    primary_color: Optional[str] = None
    accent_color: Optional[str] = None

class CompanyOut(BaseModel):
    id: int
    slug: str
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    logo_s3_key: Optional[str] = None
    max_devices: int
    max_users: int
    max_storage_mb: int
    status: str
    trial_ends_at: Optional[datetime] = None
    primary_color: Optional[str] = None
    accent_color: Optional[str] = None
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

class RoleCreateIn(BaseModel):
    name: str = Field(..., min_length=2, max_length=100)
    display_name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    permissions: List[str] = []

class RoleUpdateIn(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None
    permissions: Optional[List[str]] = None

class RoleOut(BaseModel):
    id: int
    tenant_id: Optional[int] = None
    name: str
    display_name: str
    description: Optional[str] = None
    is_system: bool
    permissions: list
    created_at: datetime
    updated_at: datetime

class ImpersonateIn(BaseModel):
    company_slug: str

class AuditLogOut(BaseModel):
    id: int
    tenant_id: Optional[int] = None
    user_id: int
    username: Optional[str] = None
    action: str
    resource_type: Optional[str] = None
    resource_id: Optional[int] = None
    details: Optional[dict] = None
    ip_address: Optional[str] = None
    created_at: datetime


COMPANY_COLUMNS = """id, slug, name, email, phone, address, logo_s3_key,
    max_devices, max_users, max_storage_mb, status, trial_ends_at,
    primary_color, accent_color, created_at, updated_at"""

def _row_to_company(r) -> CompanyOut:
    return CompanyOut(
        id=r[0], slug=r[1], name=r[2], email=r[3], phone=r[4],
        address=r[5], logo_s3_key=r[6], max_devices=r[7], max_users=r[8],
        max_storage_mb=r[9], status=r[10], trial_ends_at=r[11],
        primary_color=r[12], accent_color=r[13], created_at=r[14], updated_at=r[15],
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

                cur.execute(f"""
                    INSERT INTO public.company
                        (slug, name, email, phone, address, max_devices, max_users, max_storage_mb, created_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, created_at
                """, (body.slug, body.name, body.email, body.phone, body.address,
                      body.max_devices, body.max_users, body.max_storage_mb, ctx.user_id))
                company_id, created_at = cur.fetchone()

                role_map = clone_roles_for_company(conn, company_id)
                admin_role_id = role_map.get("company_admin")

                temp_pw = _temp_password()
                cur.execute("""
                    INSERT INTO public.users
                        (username, email, password_hash, full_name, role, user_type, tenant_id, role_id, must_change_password)
                    VALUES (%s, %s, %s, %s, 'admin', 'company', %s, %s, TRUE)
                    RETURNING id
                """, (body.admin_username.lower(),
                      body.admin_email.lower() if body.admin_email else None,
                      hash_password(temp_pw), body.admin_full_name,
                      company_id, admin_role_id))
                admin_id = cur.fetchone()[0]

            conn.commit()
            log_audit(conn, None, ctx.user_id, "company.create", "company", company_id,
                      {"slug": body.slug, "name": body.name})
            conn.commit()

            return {
                "company": {"id": company_id, "slug": body.slug, "name": body.name,
                            "status": "active", "created_at": str(created_at)},
                "admin_user": {"id": admin_id, "username": body.admin_username.lower(),
                               "temp_password": temp_pw, "must_change_password": True},
                "roles_created": list(role_map.keys()),
            }
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/companies", response_model=CompanyListOut)
def list_companies(
    status: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    ctx: TenantContext = Depends(require_platform_user),
):
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        with conn.cursor() as cur:
            wheres, params = [], []
            if status:
                wheres.append("status = %s"); params.append(status)
            if q:
                wheres.append("(name ILIKE %s OR slug ILIKE %s OR email ILIKE %s)")
                params.extend([f"%{q}%"] * 3)
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
                for f, c in [("name","name"),("email","email"),("phone","phone"),("address","address"),
                             ("max_devices","max_devices"),("max_users","max_users"),("max_storage_mb","max_storage_mb"),
                             ("primary_color","primary_color"),("accent_color","accent_color")]:
                    v = getattr(body, f, None)
                    if v is not None:
                        updates.append(f"{c} = %s"); params.append(v)
                if not updates:
                    return get_company(slug, ctx)
                params.append(slug)
                cur.execute(f"UPDATE public.company SET {', '.join(updates)}, updated_at = NOW() WHERE slug = %s RETURNING {COMPANY_COLUMNS}", params)
                r = cur.fetchone()
                if not r:
                    raise HTTPException(status_code=404, detail="Company not found")
            conn.commit()
            log_audit(conn, r[0], ctx.user_id, "company.update", "company", r[0])
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
                cur.execute("UPDATE public.company SET status='suspended', updated_at=NOW() WHERE slug=%s AND status='active' RETURNING id", (slug,))
                r = cur.fetchone()
                if not r:
                    raise HTTPException(status_code=404, detail="Company not found or not active")
                invalidate_tenant_sessions(r[0])
            conn.commit()
            log_audit(conn, r[0], ctx.user_id, "company.suspend", "company", r[0])
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
                cur.execute("UPDATE public.company SET status='active', updated_at=NOW() WHERE slug=%s AND status='suspended' RETURNING id", (slug,))
                r = cur.fetchone()
                if not r:
                    raise HTTPException(status_code=404, detail="Company not found or not suspended")
            conn.commit()
            log_audit(conn, r[0], ctx.user_id, "company.reactivate", "company", r[0])
            conn.commit()
            return {"message": f"Company '{slug}' reactivated", "company_id": r[0]}
        except HTTPException:
            conn.rollback(); raise
        except Exception as e:
            conn.rollback(); raise HTTPException(status_code=500, detail=str(e))


@router.delete("/companies/{slug}")
def delete_company(slug: str, ctx: TenantContext = Depends(require_platform_user)):
    """Permanently delete a company and ALL its data. Irreversible."""
    if not ctx.has_permission("platform.manage_companies") and not ctx.has_permission("company.full_access"):
        raise HTTPException(status_code=403, detail="Missing platform.manage_companies permission")
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
                cur.execute("SELECT s3_link FROM public.video WHERE tenant_id=%s AND s3_link IS NOT NULL", (cid,))
                s3_keys.extend([r[0] for r in cur.fetchall()])
                cur.execute("SELECT s3_link FROM public.advertisement WHERE tenant_id=%s AND s3_link IS NOT NULL", (cid,))
                s3_keys.extend([r[0] for r in cur.fetchall()])

                # Delete from all tenant-scoped tables (order matters for FK constraints)
                tenant_tables = [
                    "device_advertisement_shop_group",
                    "device_video_shop_group",
                    "device_assignment",
                    "group_video",
                    "group_advertisement",
                    "device_layout",
                    "device_logs",
                    "device_temperature",
                    "temperature",
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
                        cur.execute(f"DELETE FROM public.{tbl} WHERE tenant_id=%s", (cid,))
                        counts[tbl.strip('"')] = cur.rowcount
                        cur.execute("RELEASE SAVEPOINT sp_del")
                    except Exception:
                        cur.execute("ROLLBACK TO SAVEPOINT sp_del")
                        counts[tbl.strip('"')] = 0

                # Delete users belonging to this company
                cur.execute("DELETE FROM public.users WHERE tenant_id=%s", (cid,))
                counts["users"] = cur.rowcount

                # Delete roles for this company
                cur.execute("DELETE FROM public.role WHERE tenant_id=%s", (cid,))
                counts["roles"] = cur.rowcount

                # Delete audit logs
                cur.execute("DELETE FROM public.audit_log WHERE tenant_id=%s", (cid,))
                counts["audit_log"] = cur.rowcount

                # Invalidate all sessions
                invalidate_tenant_sessions(cid)

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
                        # S3 delete_objects accepts max 1000 keys per call
                        for i in range(0, len(s3_keys), 1000):
                            batch = s3_keys[i:i+1000]
                            s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch]})
                except Exception as s3_err:
                    print(f"[WARN] S3 cleanup failed for company {slug}: {s3_err}", flush=True)

            # Log deletion to audit (tenant_id=None since company is gone)
            log_audit(conn, None, ctx.user_id, "company.delete", "company", cid,
                      {"slug": slug, "name": c[2], "deleted_rows": counts})
            conn.commit()

            return {
                "message": f"Company '{c[2]}' ({slug}) permanently deleted",
                "company_id": cid,
                "deleted_rows": counts,
                "s3_keys_deleted": len(s3_keys),
            }
        except HTTPException:
            conn.rollback(); raise
        except Exception as e:
            conn.rollback(); raise HTTPException(status_code=500, detail=str(e))


@router.get("/companies/{slug}/stats", response_model=CompanyStatsOut)
def get_company_stats(slug: str, ctx: TenantContext = Depends(require_platform_user)):
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, slug, name FROM public.company WHERE slug=%s", (slug,))
            c = cur.fetchone()
            if not c:
                raise HTTPException(status_code=404, detail="Company not found")
            cid = c[0]
            counts = {}
            for tbl, key in [("device","device_count"),("users","user_count"),("video","video_count"),
                             ("advertisement","advertisement_count"),('"group"',"group_count"),("shop","shop_count")]:
                cur.execute(f"SELECT COUNT(*) FROM public.{tbl} WHERE tenant_id=%s", (cid,))
                counts[key] = cur.fetchone()[0]
            return CompanyStatsOut(company_id=cid, slug=c[1], name=c[2], **counts)


# ══════════════════════════════════════════════════════════════
# PLATFORM DASHBOARD  (aggregated metrics across all companies)
# ══════════════════════════════════════════════════════════════

class PlatformDashboardOut(BaseModel):
    total_companies: int
    active_companies: int
    suspended_companies: int
    trial_companies: int
    total_devices: int
    online_devices: int
    offline_devices: int
    total_videos: int
    total_advertisements: int
    total_users: int
    total_groups: int
    total_shops: int
    total_storage_used_mb: float
    companies: list  # per-company breakdown


@router.get("/dashboard", response_model=PlatformDashboardOut)
def platform_dashboard(ctx: TenantContext = Depends(require_platform_user)):
    """Aggregated platform dashboard with per-company breakdown."""
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        with conn.cursor() as cur:
            # Global company counts by status
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status='active') AS active,
                    COUNT(*) FILTER (WHERE status='suspended') AS suspended,
                    COUNT(*) FILTER (WHERE status='trial') AS trial
                FROM public.company
            """)
            r = cur.fetchone()
            totals = dict(total_companies=r[0], active_companies=r[1],
                          suspended_companies=r[2], trial_companies=r[3])

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
            cur.execute("SELECT COUNT(*) FROM public.users WHERE user_type='company'")
            totals["total_users"] = cur.fetchone()[0]
            cur.execute('SELECT COUNT(*) FROM public."group"')
            totals["total_groups"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM public.shop")
            totals["total_shops"] = cur.fetchone()[0]

            # Approximate storage: count S3 objects (we don't store file sizes, so estimate)
            cur.execute("SELECT COUNT(*) FROM public.video WHERE s3_link IS NOT NULL")
            vid_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM public.advertisement WHERE s3_link IS NOT NULL")
            ad_count = cur.fetchone()[0]
            # Rough estimate: avg 50MB per video, 5MB per ad
            totals["total_storage_used_mb"] = round(vid_count * 50 + ad_count * 5, 1)

            # Per-company breakdown
            cur.execute("""
                SELECT
                    c.id, c.slug, c.name, c.status, c.max_devices, c.max_users,
                    c.max_storage_mb, c.created_at,
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
                    SELECT tenant_id, COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE is_online = TRUE) AS online,
                           COUNT(*) FILTER (WHERE is_online = FALSE OR is_online IS NULL) AS offline
                    FROM public.device GROUP BY tenant_id
                ) d ON d.tenant_id = c.id
                LEFT JOIN (SELECT tenant_id, COUNT(*) AS cnt FROM public.video GROUP BY tenant_id) v ON v.tenant_id = c.id
                LEFT JOIN (SELECT tenant_id, COUNT(*) AS cnt FROM public.advertisement GROUP BY tenant_id) ad ON ad.tenant_id = c.id
                LEFT JOIN (SELECT tenant_id, COUNT(*) AS cnt FROM public.users WHERE user_type='company' GROUP BY tenant_id) u ON u.tenant_id = c.id
                LEFT JOIN (SELECT tenant_id, COUNT(*) AS cnt FROM public."group" GROUP BY tenant_id) g ON g.tenant_id = c.id
                LEFT JOIN (SELECT tenant_id, COUNT(*) AS cnt FROM public.shop GROUP BY tenant_id) s ON s.tenant_id = c.id
                ORDER BY c.created_at DESC
            """)
            companies = []
            for row in cur.fetchall():
                companies.append({
                    "id": row[0], "slug": row[1], "name": row[2], "status": row[3],
                    "max_devices": row[4], "max_users": row[5], "max_storage_mb": row[6],
                    "created_at": str(row[7]),
                    "device_count": row[8], "devices_online": row[9], "devices_offline": row[10],
                    "video_count": row[11], "ad_count": row[12], "user_count": row[13],
                    "group_count": row[14], "shop_count": row[15],
                })

            totals["companies"] = companies
            return totals


# ══════════════════════════════════════════════════════════════
# IMPERSONATION
# ══════════════════════════════════════════════════════════════

@router.post("/impersonate")
def impersonate_company(body: ImpersonateIn, authorization: Optional[str] = Header(None),
                        ctx: TenantContext = Depends(require_platform_user)):
    if not ctx.has_permission("platform.impersonate") and not ctx.has_permission("company.full_access"):
        raise HTTPException(status_code=403, detail="Missing impersonate permission")
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, slug, name FROM public.company WHERE slug=%s AND status='active'", (body.company_slug,))
            r = cur.fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="Company not found or not active")
    token = authorization.replace("Bearer ", "") if authorization and authorization.startswith("Bearer ") else authorization
    start_impersonation(token, r[0], r[1], r[2])
    return {"message": f"Now impersonating '{r[2]}'", "company": {"id": r[0], "slug": r[1], "name": r[2]}}


@router.post("/stop-impersonate")
def stop_impersonate(authorization: Optional[str] = Header(None),
                     ctx: TenantContext = Depends(require_platform_user)):
    token = authorization.replace("Bearer ", "") if authorization and authorization.startswith("Bearer ") else authorization
    stop_impersonation(token)
    return {"message": "Impersonation ended"}


# ══════════════════════════════════════════════════════════════
# ROLE MANAGEMENT
# ══════════════════════════════════════════════════════════════

@router.get("/roles", response_model=List[RoleOut])
def list_roles(ctx: TenantContext = Depends(get_tenant_context)):
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        with conn.cursor() as cur:
            if ctx.user_type == "platform" and ctx.active_tenant_id is None:
                cur.execute("SELECT id, tenant_id, name, display_name, description, is_system, permissions, created_at, updated_at FROM public.role WHERE tenant_id IS NULL ORDER BY id")
            else:
                tid = ctx.active_tenant_id or ctx.tenant_id
                cur.execute("SELECT id, tenant_id, name, display_name, description, is_system, permissions, created_at, updated_at FROM public.role WHERE tenant_id=%s ORDER BY id", (tid,))
            return [RoleOut(id=r[0], tenant_id=r[1], name=r[2], display_name=r[3], description=r[4],
                           is_system=r[5], permissions=r[6] if isinstance(r[6], list) else json.loads(r[6]) if r[6] else [],
                           created_at=r[7], updated_at=r[8]) for r in cur.fetchall()]


@router.post("/roles", response_model=RoleOut)
def create_role(body: RoleCreateIn, ctx: TenantContext = Depends(get_tenant_context)):
    if ctx.user_type == "platform" and ctx.active_tenant_id is None:
        tenant_id = None
        if not ctx.has_permission("platform.manage_roles"):
            raise HTTPException(status_code=403, detail="Missing platform.manage_roles")
    else:
        tenant_id = ctx.require_tenant()
        if not ctx.has_permission("manage_roles"):
            raise HTTPException(status_code=403, detail="Missing manage_roles")
    name = re.sub(r'[^a-z0-9_]', '_', body.name.lower().strip())
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO public.role (tenant_id, name, display_name, description, is_system, permissions)
                    VALUES (%s,%s,%s,%s,FALSE,%s) RETURNING id, tenant_id, name, display_name, description, is_system, permissions, created_at, updated_at""",
                    (tenant_id, name, body.display_name, body.description, json.dumps(body.permissions)))
                r = cur.fetchone()
            conn.commit()
            return RoleOut(id=r[0], tenant_id=r[1], name=r[2], display_name=r[3], description=r[4],
                           is_system=r[5], permissions=r[6] if isinstance(r[6], list) else json.loads(r[6]) if r[6] else [],
                           created_at=r[7], updated_at=r[8])
        except Exception as e:
            conn.rollback()
            if "role_unique" in str(e):
                raise HTTPException(status_code=400, detail=f"Role '{name}' already exists")
            raise HTTPException(status_code=500, detail=str(e))


@router.put("/roles/{role_id}", response_model=RoleOut)
def update_role(role_id: int, body: RoleUpdateIn, ctx: TenantContext = Depends(get_tenant_context)):
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT tenant_id, is_system FROM public.role WHERE id=%s", (role_id,))
                r = cur.fetchone()
                if not r:
                    raise HTTPException(status_code=404, detail="Role not found")
                if ctx.user_type != "platform" and r[0] != ctx.active_tenant_id:
                    raise HTTPException(status_code=403, detail="Access denied")
                updates, params = [], []
                if body.display_name is not None:
                    updates.append("display_name=%s"); params.append(body.display_name)
                if body.description is not None:
                    updates.append("description=%s"); params.append(body.description)
                if body.permissions is not None:
                    updates.append("permissions=%s"); params.append(json.dumps(body.permissions))
                if not updates:
                    raise HTTPException(status_code=400, detail="No changes")
                params.append(role_id)
                cur.execute(f"UPDATE public.role SET {','.join(updates)}, updated_at=NOW() WHERE id=%s RETURNING id, tenant_id, name, display_name, description, is_system, permissions, created_at, updated_at", params)
                r = cur.fetchone()
            conn.commit()
            return RoleOut(id=r[0], tenant_id=r[1], name=r[2], display_name=r[3], description=r[4],
                           is_system=r[5], permissions=r[6] if isinstance(r[6], list) else json.loads(r[6]) if r[6] else [],
                           created_at=r[7], updated_at=r[8])
        except HTTPException:
            conn.rollback(); raise
        except Exception as e:
            conn.rollback(); raise HTTPException(status_code=500, detail=str(e))


@router.delete("/roles/{role_id}")
def delete_role(role_id: int, ctx: TenantContext = Depends(get_tenant_context)):
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT tenant_id, is_system, name FROM public.role WHERE id=%s", (role_id,))
                r = cur.fetchone()
                if not r:
                    raise HTTPException(status_code=404, detail="Role not found")
                if r[1]:
                    raise HTTPException(status_code=400, detail="Cannot delete system roles")
                if ctx.user_type != "platform" and r[0] != ctx.active_tenant_id:
                    raise HTTPException(status_code=403, detail="Access denied")
                cur.execute("UPDATE public.users SET role_id=NULL WHERE role_id=%s", (role_id,))
                cur.execute("DELETE FROM public.role WHERE id=%s", (role_id,))
            conn.commit()
            return {"message": f"Role '{r[2]}' deleted"}
        except HTTPException:
            conn.rollback(); raise
        except Exception as e:
            conn.rollback(); raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════
# AUDIT LOG
# ══════════════════════════════════════════════════════════════

@router.get("/audit-log", response_model=List[AuditLogOut])
def get_audit_log(
    action: Optional[str] = Query(None),
    resource_type: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    ctx: TenantContext = Depends(get_tenant_context),
):
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        with conn.cursor() as cur:
            wheres, params = [], []
            if ctx.user_type != "platform":
                wheres.append("a.tenant_id=%s"); params.append(ctx.require_tenant())
            elif ctx.active_tenant_id is not None:
                wheres.append("a.tenant_id=%s"); params.append(ctx.active_tenant_id)
            if action:
                wheres.append("a.action=%s"); params.append(action)
            if resource_type:
                wheres.append("a.resource_type=%s"); params.append(resource_type)
            where_sql = f"WHERE {' AND '.join(wheres)}" if wheres else ""
            cur.execute(f"""SELECT a.id, a.tenant_id, a.user_id, u.username, a.action,
                a.resource_type, a.resource_id, a.details, a.ip_address::text, a.created_at
                FROM public.audit_log a LEFT JOIN public.users u ON u.id=a.user_id
                {where_sql} ORDER BY a.created_at DESC LIMIT %s OFFSET %s""", params + [limit, offset])
            return [AuditLogOut(id=r[0], tenant_id=r[1], user_id=r[2], username=r[3], action=r[4],
                               resource_type=r[5], resource_id=r[6],
                               details=r[7] if isinstance(r[7], dict) else json.loads(r[7]) if r[7] else None,
                               ip_address=r[8], created_at=r[9]) for r in cur.fetchall()]


# ══════════════════════════════════════════════════════════════
# COMPANY SELF-SERVICE
# ══════════════════════════════════════════════════════════════

@router.get("/my-company", response_model=CompanyOut)
def get_my_company(ctx: TenantContext = Depends(require_tenant_context)):
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {COMPANY_COLUMNS} FROM public.company WHERE id=%s", (ctx.active_tenant_id,))
            r = cur.fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="Company not found")
            return _row_to_company(r)


@router.get("/my-company/stats", response_model=CompanyStatsOut)
def get_my_company_stats(ctx: TenantContext = Depends(require_tenant_context)):
    pg_conn = _get_pg_conn()
    with pg_conn() as conn:
        with conn.cursor() as cur:
            tid = ctx.active_tenant_id
            cur.execute("SELECT slug, name FROM public.company WHERE id=%s", (tid,))
            c = cur.fetchone()
            if not c:
                raise HTTPException(status_code=404, detail="Company not found")
            counts = {}
            for tbl, key in [("device","device_count"),("users","user_count"),("video","video_count"),
                             ("advertisement","advertisement_count"),('"group"',"group_count"),("shop","shop_count")]:
                cur.execute(f"SELECT COUNT(*) FROM public.{tbl} WHERE tenant_id=%s", (tid,))
                counts[key] = cur.fetchone()[0]
            return CompanyStatsOut(company_id=tid, slug=c[0], name=c[1], **counts)
