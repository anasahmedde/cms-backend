# tenant_context.py
# Core multi-tenant authentication, authorization, and context resolution.
# This module replaces the simple get_current_user / require_permission
# pattern with a tenant-aware TenantContext system.

import json
import secrets
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from fastapi import HTTPException, Header, Depends


# ══════════════════════════════════════════════════════════════
# TENANT CONTEXT - resolved on every authenticated request
# ══════════════════════════════════════════════════════════════

@dataclass
class TenantContext:
    """
    The resolved identity + tenant for every authenticated request.
    Every DB query should use ctx.active_tenant_id for scoping.
    """
    user_id: int
    username: str
    full_name: Optional[str]
    user_type: str                      # "platform" or "company"
    tenant_id: Optional[int]            # None for platform users (unless impersonating)
    active_tenant_id: Optional[int]     # The tenant being operated on (always set for data ops)
    role_name: str
    role_id: Optional[int]
    permissions: List[str] = field(default_factory=list)
    company_slug: Optional[str] = None
    company_name: Optional[str] = None
    is_impersonating: bool = False

    def has_permission(self, perm: str) -> bool:
        """Check if user has a specific permission."""
        # Platform super_admins with company.full_access bypass all
        if self.user_type == "platform" and "company.full_access" in self.permissions:
            return True
        return perm in self.permissions

    def is_platform_user(self) -> bool:
        return self.user_type == "platform"

    def require_tenant(self) -> int:
        """Get active_tenant_id or raise 400 if not set (platform user not impersonating)."""
        if self.active_tenant_id is None:
            raise HTTPException(
                status_code=400,
                detail="No company context. Platform users must specify or impersonate a company."
            )
        return self.active_tenant_id


# ══════════════════════════════════════════════════════════════
# SESSION STORE
# ══════════════════════════════════════════════════════════════
# Process memory is the hot path; every write goes through to the
# auth_session table (token stored as SHA-256, never raw) so sessions survive
# deploys/restarts and work across replicas. On a memory miss the token is
# rehydrated from the DB.

import logging

_auth_logger = logging.getLogger("auth.sessions")

# Token -> session dict
active_sessions: Dict[str, Dict[str, Any]] = {}


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _session_to_json(session: Dict[str, Any]) -> str:
    doc = dict(session)
    exp = doc.get("expires_at")
    if isinstance(exp, datetime):
        doc["expires_at"] = exp.isoformat()
    return json.dumps(doc)


def _session_from_json(doc: Dict[str, Any]) -> Dict[str, Any]:
    session = dict(doc)
    exp = session.get("expires_at")
    if isinstance(exp, str):
        try:
            session["expires_at"] = datetime.fromisoformat(exp)
        except ValueError:
            session["expires_at"] = datetime.min
    return session


def persist_session(token: str, session: Dict[str, Any]) -> None:
    """Write-through a session to the DB. Failures are logged, never fatal —
    the in-memory session still works until the next restart."""
    try:
        from database import pg_conn
        exp = session.get("expires_at")
        exp_dt = exp if isinstance(exp, datetime) else datetime.now() + timedelta(hours=24)
        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO public.auth_session (token_hash, session, expires_at)
                    VALUES (%s, %s::jsonb, %s)
                    ON CONFLICT (token_hash)
                    DO UPDATE SET session = EXCLUDED.session,
                                  expires_at = EXCLUDED.expires_at,
                                  updated_at = NOW();
                """, (_token_hash(token), _session_to_json(session), exp_dt))
            conn.commit()
    except Exception as exc:
        _auth_logger.warning("session persist failed (memory-only until restart): %s", exc)


def _load_session_from_db(token: str) -> Optional[Dict[str, Any]]:
    try:
        from database import pg_conn
        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT session FROM public.auth_session WHERE token_hash = %s AND expires_at > NOW();",
                    (_token_hash(token),),
                )
                row = cur.fetchone()
        if not row:
            return None
        return _session_from_json(row[0] if isinstance(row[0], dict) else json.loads(row[0]))
    except Exception as exc:
        _auth_logger.warning("session load failed: %s", exc)
        return None


def destroy_session(token: str) -> None:
    """Remove a session from memory AND the DB (logout / forced invalidation)."""
    active_sessions.pop(token, None)
    try:
        from database import pg_conn
        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM public.auth_session WHERE token_hash = %s;", (_token_hash(token),))
            conn.commit()
    except Exception as exc:
        _auth_logger.warning("session DB delete failed: %s", exc)


def _resolve_session(token: str) -> Optional[Dict[str, Any]]:
    """Memory first, then DB rehydrate (post-deploy / other replica)."""
    session = active_sessions.get(token)
    if session is not None:
        return session
    session = _load_session_from_db(token)
    if session is not None:
        active_sessions[token] = session
    return session


def hash_password(password: str) -> str:
    return hashlib.sha256(f"digix_salt_{password}".encode()).hexdigest()


def verify_password(password: str, password_hash: str) -> bool:
    return hash_password(password) == password_hash


def generate_token() -> str:
    return secrets.token_urlsafe(32)


def invalidate_user_sessions(user_id: int):
    """Remove all active sessions for a specific user (memory + DB)."""
    tokens_to_remove = [
        token for token, session in active_sessions.items()
        if session.get("user_id") == user_id
    ]
    for token in tokens_to_remove:
        del active_sessions[token]
    try:
        from database import pg_conn
        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM public.auth_session WHERE (session->>'user_id')::bigint = %s;",
                    (user_id,),
                )
            conn.commit()
    except Exception as exc:
        _auth_logger.warning("user session DB invalidation failed for user %s: %s", user_id, exc)


def invalidate_tenant_sessions(tenant_id: int):
    """Remove all sessions for users of a specific tenant (memory + DB)."""
    tokens_to_remove = [
        token for token, session in active_sessions.items()
        if session.get("tenant_id") == tenant_id
    ]
    for token in tokens_to_remove:
        del active_sessions[token]
    try:
        from database import pg_conn
        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM public.auth_session WHERE (session->>'tenant_id')::bigint = %s;",
                    (tenant_id,),
                )
            conn.commit()
    except Exception as exc:
        _auth_logger.warning("tenant session DB invalidation failed for tenant %s: %s", tenant_id, exc)


def create_session(
    user_id: int,
    username: str,
    full_name: Optional[str],
    user_type: str,
    tenant_id: Optional[int],
    role_name: str,
    role_id: Optional[int],
    permissions: List[str],
    company_slug: Optional[str] = None,
    company_name: Optional[str] = None,
    hours: int = 24,
) -> str:
    """Create a new session and return the token."""
    token = generate_token()
    active_sessions[token] = {
        "user_id": user_id,
        "username": username,
        "full_name": full_name,
        "user_type": user_type,
        "tenant_id": tenant_id,
        "active_tenant_id": tenant_id,  # same as tenant_id initially
        "role_name": role_name,
        "role_id": role_id,
        "permissions": permissions,
        "company_slug": company_slug,
        "company_name": company_name,
        "is_impersonating": False,
        "expires_at": datetime.now() + timedelta(hours=hours),
    }
    persist_session(token, active_sessions[token])
    # Opportunistic cleanup of expired rows (cheap, indexed).
    try:
        from database import pg_conn
        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM public.auth_session WHERE expires_at <= NOW();")
            conn.commit()
    except Exception as exc:
        _auth_logger.warning("expired-session sweep failed: %s", exc)
    return token


# ══════════════════════════════════════════════════════════════
# FASTAPI DEPENDENCIES
# ══════════════════════════════════════════════════════════════

def get_tenant_context(authorization: Optional[str] = Header(None)) -> TenantContext:
    """
    FastAPI dependency: resolves the authenticated user + tenant context.
    Replaces the old get_current_user().
    
    Also checks company expiration - if company is expired, the user is logged out.
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Not authenticated")

    token = authorization.replace("Bearer ", "") if authorization.startswith("Bearer ") else authorization

    session = _resolve_session(token)
    if session is None:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    if datetime.now() > session.get("expires_at", datetime.min):
        destroy_session(token)
        raise HTTPException(status_code=401, detail="Token expired")

    # Check company expiration for company users (not platform users)
    tenant_id = session.get("tenant_id")
    user_type = session.get("user_type", "company")
    is_impersonating = session.get("is_impersonating", False)
    
    # Only check for company users, not platform users (unless impersonating)
    if user_type == "company" and tenant_id and not is_impersonating:
        from company_expiration_api import check_company_access
        company_access = check_company_access(tenant_id)
        if not company_access.get("accessible", True):
            # Company is expired/suspended - invalidate session and force logout
            destroy_session(token)
            status = company_access.get("status", "expired")
            if status == "suspended":
                raise HTTPException(
                    status_code=403, 
                    detail="Company account suspended. You have been logged out. Contact administrator."
                )
            else:
                raise HTTPException(
                    status_code=403, 
                    detail="Company subscription expired. You have been logged out. Contact administrator to renew."
                )

    return TenantContext(
        user_id=session["user_id"],
        username=session["username"],
        full_name=session.get("full_name"),
        user_type=session.get("user_type", "company"),
        tenant_id=session.get("tenant_id"),
        active_tenant_id=session.get("active_tenant_id") or session.get("tenant_id"),
        role_name=session.get("role_name") or session.get("role", "viewer"),
        role_id=session.get("role_id"),
        permissions=session.get("permissions", []),
        company_slug=session.get("company_slug"),
        company_name=session.get("company_name"),
        is_impersonating=session.get("is_impersonating", False),
    )


def get_current_user(authorization: Optional[str] = Header(None)) -> Dict:
    """
    BACKWARD COMPATIBLE: wraps get_tenant_context() but returns a dict
    so existing endpoints that use `user: Dict = Depends(get_current_user)`
    continue to work without code changes.
    
    This allows gradual migration of endpoints.
    """
    ctx = get_tenant_context(authorization)
    return {
        "user_id": ctx.user_id,
        "username": ctx.username,
        "full_name": ctx.full_name,
        "role": ctx.role_name,
        "permissions": ctx.permissions,
        "user_type": ctx.user_type,
        "tenant_id": ctx.tenant_id,
        "active_tenant_id": ctx.active_tenant_id,
        "company_slug": ctx.company_slug,
        "company_name": ctx.company_name,
        "is_impersonating": ctx.is_impersonating,
    }


def require_permission(permission: str):
    """
    FastAPI dependency factory: checks a specific permission.
    Returns a Dict (NOT TenantContext) for backward compatibility
    with all existing endpoints that do user.get("key").
    """
    def checker(authorization: Optional[str] = Header(None)) -> Dict:
        ctx = get_tenant_context(authorization)
        if ctx.has_permission(permission):
            pass  # authorized
        elif ctx.user_type == "platform" and "company.full_access" in ctx.permissions:
            pass  # super admin bypass
        else:
            raise HTTPException(status_code=403, detail=f"Permission denied: {permission}")
        # Return as dict for backward compat
        return {
            "user_id": ctx.user_id,
            "username": ctx.username,
            "full_name": ctx.full_name,
            "role": ctx.role_name,
            "permissions": ctx.permissions,
            "user_type": ctx.user_type,
            "tenant_id": ctx.tenant_id,
            "active_tenant_id": ctx.active_tenant_id,
            "company_slug": ctx.company_slug,
            "company_name": ctx.company_name,
            "is_impersonating": ctx.is_impersonating,
        }
    return checker


def require_platform_user(ctx: TenantContext = Depends(get_tenant_context)) -> TenantContext:
    """Only platform-level users (DIGIX staff)."""
    if ctx.user_type != "platform":
        raise HTTPException(status_code=403, detail="Platform access required")
    return ctx


def require_tenant_context(ctx: TenantContext = Depends(get_tenant_context)) -> TenantContext:
    """Require that there is an active tenant (company user or impersonating platform user)."""
    ctx.require_tenant()
    return ctx


# ══════════════════════════════════════════════════════════════
# IMPERSONATION
# ══════════════════════════════════════════════════════════════

def start_impersonation(token: str, company_id: int, company_slug: str, company_name: str):
    """Set a platform user's session to impersonate a company."""
    session = _resolve_session(token)
    if session is None:
        raise HTTPException(status_code=401, detail="Invalid token")

    if session["user_type"] != "platform":
        raise HTTPException(status_code=403, detail="Only platform users can impersonate")
    if "platform.impersonate" not in session.get("permissions", []) and \
       "company.full_access" not in session.get("permissions", []):
        raise HTTPException(status_code=403, detail="Missing impersonate permission")

    session["active_tenant_id"] = company_id
    session["company_slug"] = company_slug
    session["company_name"] = company_name
    session["is_impersonating"] = True
    persist_session(token, session)


def stop_impersonation(token: str):
    """Revert a platform user's session to their own context."""
    session = _resolve_session(token)
    if session is None:
        raise HTTPException(status_code=401, detail="Invalid token")

    session["active_tenant_id"] = session.get("tenant_id")  # back to None for platform
    session["company_slug"] = None
    session["company_name"] = None
    session["is_impersonating"] = False
    persist_session(token, session)


# ══════════════════════════════════════════════════════════════
# AUDIT LOGGING
# ══════════════════════════════════════════════════════════════

def require_perm(user: Dict, *perms: str) -> None:
    """Permission gate for dashboard mutations using the dict-style user from
    get_current_user: the caller must hold at least ONE of the given permission
    flags. Platform users with company.full_access bypass (same rule as
    TenantContext.has_permission). Pair with tenant-scoped SQL — this checks
    WHAT the user may do, that checks WHOSE data it touches. Never use on
    device-facing routes (heartbeat etc.) — those have no user."""
    user_perms = user.get("permissions") or []
    if user.get("user_type") == "platform" and "company.full_access" in user_perms:
        return
    if not any(p in user_perms for p in perms):
        raise HTTPException(status_code=403,
                            detail=f"Permission denied: {' or '.join(perms)} required")


def require_user_tenant(user: Dict) -> int:
    """The tenant a dashboard mutation is scoped to. Unlike the legacy
    `... or 1` fallback, a caller with NO tenant (platform user not
    impersonating) gets a 400 instead of silently operating on tenant 1."""
    tenant_id = user.get("active_tenant_id") or user.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400,
                            detail="No company context. Platform users must impersonate a company first.")
    return int(tenant_id)


def log_audit(conn, tenant_id, user_id, action, resource_type=None,
              resource_id=None, details=None, ip_address=None):
    """
    Fire-and-forget audit log entry. Uses provided connection.
    Should never break the main flow.
    """
    try:
        with conn.cursor() as cur:
            # Savepoint so a failed audit INSERT cannot abort the caller's
            # transaction (which would silently turn its commit into a rollback).
            cur.execute("SAVEPOINT audit_log_sp")
            try:
                cur.execute("""
                    INSERT INTO public.audit_log
                        (tenant_id, user_id, action, resource_type, resource_id, details, ip_address)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    tenant_id, user_id, action, resource_type, resource_id,
                    json.dumps(details) if details else None,
                    ip_address,
                ))
                cur.execute("RELEASE SAVEPOINT audit_log_sp")
            except Exception:
                cur.execute("ROLLBACK TO SAVEPOINT audit_log_sp")
    except Exception:
        pass  # Never let audit logging break the caller


# ══════════════════════════════════════════════════════════════
# LEGACY ROLE_PERMISSIONS (kept for backward compatibility
# during migration, eventually replaced by role table lookup)
# ══════════════════════════════════════════════════════════════

ROLE_PERMISSIONS = {
    "admin": [
        "view_dashboard", "manage_devices", "manage_groups", "manage_shops",
        "upload_videos", "manage_videos", "manage_links", "manage_users",
        "view_reports", "export_data", "manage_advertisements",
        "manage_roles", "manage_company_settings",
    ],
    "manager": [
        "view_dashboard", "manage_devices", "manage_groups", "manage_shops",
        "upload_videos", "manage_videos", "manage_links", "view_reports",
        "export_data", "manage_advertisements",
    ],
    "editor": [
        "view_dashboard", "upload_videos", "manage_videos",
        "manage_links", "view_reports", "manage_advertisements",
    ],
    "viewer": ["view_dashboard", "view_reports"],
}
