# company_expiration_api.py
# Location: cms-backend-staging/company_expiration_api.py
"""
Company Expiration API
======================
Endpoints for managing company expiration:
- Set/extend expiration date
- View expired companies
- Suspend/reactivate companies
- Check expiration status (used by login and device heartbeat)

Include this router in device_video_shop_group.py:
    from company_expiration_api import router as company_expiration_router
    app.include_router(company_expiration_router, tags=["Company Expiration"])
"""

from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from enum import Enum

from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from database import pg_conn
from tenant_context import get_current_user, require_platform_user, require_tenant_context, log_audit, TenantContext

router = APIRouter()


# ══════════════════════════════════════════════════════════════════════════════
# ENUMS AND MODELS
# ══════════════════════════════════════════════════════════════════════════════

class ExpirationStatus(str, Enum):
    ACTIVE = "active"
    GRACE_PERIOD = "grace_period"
    EXPIRED = "expired"
    SUSPENDED = "suspended"


class CompanyExpirationOut(BaseModel):
    company_id: int
    company_name: str
    company_slug: str
    expires_at: Optional[datetime]
    expiration_status: str
    grace_period_days: int
    days_until_expiration: Optional[int]
    days_since_expiration: Optional[int]
    is_accessible: bool  # Can users/devices access?
    device_count: int
    user_count: int


class SetExpirationIn(BaseModel):
    expires_at: datetime = Field(..., description="When the company expires")
    grace_period_days: int = Field(7, ge=0, le=30, description="Days of grace period after expiration")
    notify_email: Optional[str] = Field(None, description="Email to notify about expiration")
    notes: Optional[str] = Field(None, description="Notes about this expiration setting")


class ExtendExpirationIn(BaseModel):
    extend_days: int = Field(..., ge=1, le=365, description="Days to extend from current expiration or today")
    notes: Optional[str] = Field(None, description="Notes about this extension")


class SuspendCompanyIn(BaseModel):
    reason: str = Field(..., min_length=1, max_length=500, description="Reason for suspension")


# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def calculate_expiration_status(expires_at: Optional[datetime], grace_period_days: int, suspended_at: Optional[datetime]) -> tuple:
    """
    Calculate current expiration status and accessibility.
    Returns: (status, is_accessible, days_until, days_since)
    """
    if suspended_at:
        return "suspended", False, None, None
    
    if not expires_at:
        return "active", True, None, None
    
    now = datetime.now(expires_at.tzinfo) if expires_at.tzinfo else datetime.utcnow()
    
    if now < expires_at:
        # Not expired yet
        days_until = (expires_at - now).days
        return "active", True, days_until, None
    
    # Past expiration date - calculate total seconds for more precise comparison
    time_since = now - expires_at
    days_since = time_since.days
    
    # Grace period check: 
    # - If grace_period_days is 0, no grace period at all
    # - Otherwise, allow grace_period_days FULL days after expiration
    if grace_period_days > 0 and days_since < grace_period_days:
        # In grace period - still accessible but with warning
        return "grace_period", True, None, days_since
    
    # Fully expired (including when grace_period_days is 0)
    return "expired", False, None, days_since


def log_expiration_event(cur, company_id: int, event_type: str, 
                         old_expires_at=None, new_expires_at=None,
                         old_status=None, new_status=None,
                         performed_by=None, notes=None):
    """Log an expiration-related event."""
    cur.execute("""
        INSERT INTO public.company_expiration_log 
        (company_id, event_type, old_expires_at, new_expires_at, old_status, new_status, performed_by, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """, (company_id, event_type, old_expires_at, new_expires_at, old_status, new_status, performed_by, notes))


# ══════════════════════════════════════════════════════════════════════════════
# CHECK COMPANY ACCESS (Used by login and device heartbeat)
# ══════════════════════════════════════════════════════════════════════════════

def check_company_access(company_id: int) -> dict:
    """
    Check if a company is accessible (not expired/suspended).
    Called during login and device heartbeat.
    
    Returns:
        {
            "accessible": bool,
            "status": str,
            "message": str or None,
            "days_remaining": int or None
        }
    """
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT expires_at, grace_period_days, suspended_at, suspension_reason, name
                FROM public.company WHERE id = %s;
            """, (company_id,))
            row = cur.fetchone()
            
            if not row:
                return {"accessible": False, "status": "not_found", "message": "Company not found"}
            
            expires_at, grace_days, suspended_at, suspension_reason, company_name = row
            # Use 0 as default if grace_days is None, not 7
            effective_grace_days = grace_days if grace_days is not None else 7
            status, accessible, days_until, days_since = calculate_expiration_status(
                expires_at, effective_grace_days, suspended_at
            )
            
            if status == "suspended":
                return {
                    "accessible": False,
                    "status": "suspended",
                    "message": f"Company '{company_name}' has been suspended: {suspension_reason or 'Contact administrator'}",
                    "days_remaining": None
                }
            
            if status == "expired":
                return {
                    "accessible": False,
                    "status": "expired",
                    "message": f"Company '{company_name}' subscription has expired. Please contact administrator.",
                    "days_remaining": None
                }
            
            if status == "grace_period":
                return {
                    "accessible": True,
                    "status": "grace_period",
                    "message": f"Warning: Company subscription expired {days_since} days ago. Grace period ends in {effective_grace_days - days_since} days.",
                    "days_remaining": effective_grace_days - days_since
                }
            
            # Active
            return {
                "accessible": True,
                "status": "active",
                "message": None,
                "days_remaining": days_until
            }


@router.get("/company/{company_id}/access-status")
def get_company_access_status(company_id: int):
    """
    Check if company is accessible (for login/device checks).
    This endpoint doesn't require authentication - used by login flow.
    """
    return check_company_access(company_id)


@router.get("/company/expiration-status")
def get_my_company_expiration_status(user: TenantContext = Depends(require_tenant_context)):
    """
    Get expiration status for the currently logged-in user's company.
    Used by frontend to show warning banners to company users.
    
    Returns:
        - status: active, grace_period, expired, suspended
        - days_remaining: days until expiration (if active) or days of grace left (if grace_period)
        - expires_at: expiration date
        - message: human-readable message
    """
    tenant_id = user.tenant_id
    if not tenant_id:
        # Platform user or no company - return "active" with no expiration
        return {
            "status": "active",
            "days_remaining": None,
            "expires_at": None,
            "message": None
        }
    
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT expires_at, grace_period_days, suspended_at, name
                FROM public.company WHERE id = %s;
            """, (tenant_id,))
            row = cur.fetchone()
            
            if not row:
                return {
                    "status": "active",
                    "days_remaining": None,
                    "expires_at": None,
                    "message": None
                }
            
            expires_at, grace_days, suspended_at, company_name = row
            effective_grace = grace_days if grace_days is not None else 7
            
            status, accessible, days_until, days_since = calculate_expiration_status(
                expires_at, effective_grace, suspended_at
            )
            
            # Calculate days_remaining based on status
            if status == "active" and days_until is not None:
                days_remaining = days_until
                message = f"Your subscription expires in {days_until} days" if days_until <= 30 else None
            elif status == "grace_period":
                days_remaining = effective_grace - (days_since or 0)
                message = f"Your subscription has expired. You have {days_remaining} days remaining in the grace period."
            elif status == "expired":
                days_remaining = None
                message = "Your subscription has expired. Please contact your administrator."
            elif status == "suspended":
                days_remaining = None
                message = "Your company account has been suspended. Please contact your administrator."
            else:
                days_remaining = None
                message = None
            
            return {
                "status": status,
                "days_remaining": days_remaining,
                "expires_at": expires_at.isoformat() if expires_at else None,
                "message": message,
                "company_name": company_name
            }


# ══════════════════════════════════════════════════════════════════════════════
# SUPER ADMIN: LIST COMPANIES BY STATUS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/platform/companies/expiration", response_model=List[CompanyExpirationOut])
def list_companies_expiration(
    status: Optional[str] = Query(None, description="Filter by status: active, grace_period, expired, suspended, all"),
    user: TenantContext = Depends(require_platform_user)
):
    """
    List all companies with their expiration status.
    Platform admin only.
    """
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    c.id, c.name, c.slug, c.expires_at, c.expiration_status,
                    c.grace_period_days, c.suspended_at, c.suspension_reason,
                    (SELECT COUNT(*) FROM public.device d WHERE d.tenant_id = c.id) as device_count,
                    (SELECT COUNT(*) FROM public.users u WHERE u.tenant_id = c.id) as user_count
                FROM public.company c
                ORDER BY 
                    CASE c.expiration_status 
                        WHEN 'expired' THEN 1 
                        WHEN 'suspended' THEN 2
                        WHEN 'grace_period' THEN 3
                        ELSE 4 
                    END,
                    c.expires_at ASC NULLS LAST;
            """)
            rows = cur.fetchall()
    
    result = []
    for row in rows:
        cid, name, slug, expires_at, exp_status, grace_days, suspended_at, susp_reason, dev_count, usr_count = row
        
        # Calculate actual status
        actual_status, is_accessible, days_until, days_since = calculate_expiration_status(
            expires_at, grace_days if grace_days is not None else 7, suspended_at
        )
        
        # Update status in DB if changed
        if actual_status != exp_status:
            with pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE public.company SET expiration_status = %s, updated_at = NOW()
                        WHERE id = %s;
                    """, (actual_status, cid))
                conn.commit()
        
        # Filter by status if requested
        if status and status != "all" and actual_status != status:
            continue
        
        result.append(CompanyExpirationOut(
            company_id=cid,
            company_name=name,
            company_slug=slug,
            expires_at=expires_at,
            expiration_status=actual_status,
            grace_period_days=grace_days if grace_days is not None else 7,
            days_until_expiration=days_until,
            days_since_expiration=days_since,
            is_accessible=is_accessible,
            device_count=dev_count,
            user_count=usr_count
        ))
    
    return result


@router.get("/platform/companies/expired", response_model=List[CompanyExpirationOut])
def list_expired_companies(user: TenantContext = Depends(require_platform_user)):
    """
    List only expired and suspended companies.
    Platform admin only.
    """
    return list_companies_expiration(status="expired", user=user) + \
           list_companies_expiration(status="suspended", user=user)


@router.get("/platform/companies/expiring-soon", response_model=List[CompanyExpirationOut])
def list_companies_expiring_soon(
    days: int = Query(30, ge=1, le=90, description="Companies expiring within N days"),
    user: TenantContext = Depends(require_platform_user)
):
    """
    List companies expiring within N days.
    Platform admin only.
    """
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    c.id, c.name, c.slug, c.expires_at, c.expiration_status,
                    c.grace_period_days, c.suspended_at,
                    (SELECT COUNT(*) FROM public.device d WHERE d.tenant_id = c.id) as device_count,
                    (SELECT COUNT(*) FROM public.users u WHERE u.tenant_id = c.id) as user_count
                FROM public.company c
                WHERE c.expires_at IS NOT NULL
                  AND c.expires_at > NOW()
                  AND c.expires_at <= NOW() + INTERVAL '%s days'
                  AND c.suspended_at IS NULL
                ORDER BY c.expires_at ASC;
            """ % days)
            rows = cur.fetchall()
    
    result = []
    for row in rows:
        cid, name, slug, expires_at, exp_status, grace_days, suspended_at, dev_count, usr_count = row
        
        actual_status, is_accessible, days_until, days_since = calculate_expiration_status(
            expires_at, grace_days if grace_days is not None else 7, suspended_at
        )
        
        result.append(CompanyExpirationOut(
            company_id=cid,
            company_name=name,
            company_slug=slug,
            expires_at=expires_at,
            expiration_status=actual_status,
            grace_period_days=grace_days if grace_days is not None else 7,
            days_until_expiration=days_until,
            days_since_expiration=days_since,
            is_accessible=is_accessible,
            device_count=dev_count,
            user_count=usr_count
        ))
    
    return result


# ══════════════════════════════════════════════════════════════════════════════
# SET/UPDATE COMPANY EXPIRATION
# ══════════════════════════════════════════════════════════════════════════════

@router.put("/platform/company/{company_id}/expiration")
def set_company_expiration(
    company_id: int,
    body: SetExpirationIn,
    user: TenantContext = Depends(require_platform_user)
):
    """
    Set expiration date for a company.
    Platform admin only.
    If the new expiration causes the company to be expired, all users will be logged out.
    """
    from tenant_context import invalidate_tenant_sessions
    
    with pg_conn() as conn:
        with conn.cursor() as cur:
            # Get current values
            cur.execute("""
                SELECT expires_at, expiration_status, name FROM public.company WHERE id = %s;
            """, (company_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Company not found")
            
            old_expires_at, old_status, company_name = row
            
            # Calculate new status
            new_status, new_accessible, _, _ = calculate_expiration_status(body.expires_at, body.grace_period_days, None)
            
            # Update company
            cur.execute("""
                UPDATE public.company 
                SET expires_at = %s, 
                    grace_period_days = %s,
                    expiration_status = %s,
                    expiration_notify_email = COALESCE(%s, expiration_notify_email),
                    expiration_notified_at = NULL,
                    suspended_at = NULL,
                    suspension_reason = NULL,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING expires_at, expiration_status, grace_period_days;
            """, (body.expires_at, body.grace_period_days, new_status, body.notify_email, company_id))
            
            result = cur.fetchone()
            
            # Log the event
            log_expiration_event(
                cur, company_id, "expiration_set",
                old_expires_at=old_expires_at, new_expires_at=body.expires_at,
                old_status=old_status, new_status=new_status,
                performed_by=user.user_id, notes=body.notes
            )
            
        conn.commit()
    
    # If the new status makes the company inaccessible, logout all users
    message = f"Expiration set for {company_name}"
    if not new_accessible:
        invalidate_tenant_sessions(company_id)
        message += " - All users have been logged out"
    
    return {
        "company_id": company_id,
        "company_name": company_name,
        "expires_at": result[0],
        "expiration_status": result[1],
        "grace_period_days": result[2],
        "users_logged_out": not new_accessible,
        "message": message
    }


@router.post("/platform/company/{company_id}/extend")
def extend_company_expiration(
    company_id: int,
    body: ExtendExpirationIn,
    user: TenantContext = Depends(require_platform_user)
):
    """
    Extend company expiration by N days.
    If already expired, extends from today.
    Platform admin only.
    """
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT expires_at, expiration_status, grace_period_days, name 
                FROM public.company WHERE id = %s;
            """, (company_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Company not found")
            
            old_expires_at, old_status, grace_days, company_name = row
            
            # Calculate new expiration
            from datetime import timezone
            now = datetime.now(timezone.utc)
            
            if old_expires_at and old_expires_at > now:
                # Extend from current expiration
                new_expires_at = old_expires_at + timedelta(days=body.extend_days)
            else:
                # Expired or no expiration - extend from today
                new_expires_at = now + timedelta(days=body.extend_days)
            
            new_status, _, _, _ = calculate_expiration_status(new_expires_at, grace_days if grace_days is not None else 7, None)
            
            cur.execute("""
                UPDATE public.company 
                SET expires_at = %s,
                    expiration_status = %s,
                    expiration_notified_at = NULL,
                    suspended_at = NULL,
                    suspension_reason = NULL,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING expires_at, expiration_status;
            """, (new_expires_at, new_status, company_id))
            
            result = cur.fetchone()
            
            log_expiration_event(
                cur, company_id, "renewed",
                old_expires_at=old_expires_at, new_expires_at=new_expires_at,
                old_status=old_status, new_status=new_status,
                performed_by=user.user_id, notes=body.notes
            )
            
        conn.commit()
    
    return {
        "company_id": company_id,
        "company_name": company_name,
        "old_expires_at": old_expires_at,
        "new_expires_at": result[0],
        "expiration_status": result[1],
        "extended_days": body.extend_days,
        "message": f"Extended {company_name} by {body.extend_days} days"
    }


@router.delete("/platform/company/{company_id}/expiration")
def remove_company_expiration(
    company_id: int,
    user: TenantContext = Depends(require_platform_user)
):
    """
    Remove expiration date (make company never expire).
    Platform admin only.
    """
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT expires_at, expiration_status, name FROM public.company WHERE id = %s;
            """, (company_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Company not found")
            
            old_expires_at, old_status, company_name = row
            
            cur.execute("""
                UPDATE public.company 
                SET expires_at = NULL,
                    expiration_status = 'active',
                    expiration_notified_at = NULL,
                    suspended_at = NULL,
                    suspension_reason = NULL,
                    updated_at = NOW()
                WHERE id = %s;
            """, (company_id,))
            
            log_expiration_event(
                cur, company_id, "expiration_removed",
                old_expires_at=old_expires_at, new_expires_at=None,
                old_status=old_status, new_status="active",
                performed_by=user.user_id, notes="Expiration removed - never expires"
            )
            
        conn.commit()
    
    return {
        "company_id": company_id,
        "company_name": company_name,
        "expires_at": None,
        "expiration_status": "active",
        "message": f"{company_name} will never expire"
    }


# ══════════════════════════════════════════════════════════════════════════════
# SUSPEND/REACTIVATE COMPANY
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/platform/company/{company_id}/suspend")
def suspend_company(
    company_id: int,
    body: SuspendCompanyIn,
    user: TenantContext = Depends(require_platform_user)
):
    """
    Manually suspend a company (immediate block, regardless of expiration).
    Platform admin only.
    All users of the company will be immediately logged out.
    """
    from tenant_context import invalidate_tenant_sessions
    
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT expiration_status, name FROM public.company WHERE id = %s;
            """, (company_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Company not found")
            
            old_status, company_name = row
            
            cur.execute("""
                UPDATE public.company 
                SET expiration_status = 'suspended',
                    suspended_at = NOW(),
                    suspension_reason = %s,
                    updated_at = NOW()
                WHERE id = %s;
            """, (body.reason, company_id))
            
            # Count affected users
            cur.execute("""
                SELECT COUNT(*) FROM public.users WHERE tenant_id = %s;
            """, (company_id,))
            user_count = cur.fetchone()[0]
            
            log_expiration_event(
                cur, company_id, "suspended",
                old_status=old_status, new_status="suspended",
                performed_by=user.user_id, notes=body.reason
            )
            
        conn.commit()
    
    # Immediately invalidate all sessions for this company's users
    invalidate_tenant_sessions(company_id)
    
    return {
        "company_id": company_id,
        "company_name": company_name,
        "expiration_status": "suspended",
        "reason": body.reason,
        "affected_users": user_count,
        "message": f"{company_name} has been suspended. {user_count} user(s) have been logged out."
    }


@router.post("/platform/company/{company_id}/reactivate")
def reactivate_company(
    company_id: int,
    extend_days: int = Query(None, ge=1, le=365, description="Optionally extend by N days"),
    user: TenantContext = Depends(require_platform_user)
):
    """
    Reactivate a suspended or expired company.
    Platform admin only.
    """
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT expires_at, expiration_status, grace_period_days, name 
                FROM public.company WHERE id = %s;
            """, (company_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Company not found")
            
            old_expires_at, old_status, grace_days, company_name = row
            
            # Calculate new expiration
            from datetime import timezone
            now = datetime.now(timezone.utc)
            
            if extend_days:
                new_expires_at = now + timedelta(days=extend_days)
            elif old_expires_at and old_expires_at > now:
                new_expires_at = old_expires_at
            else:
                # Default: extend 30 days from now
                new_expires_at = now + timedelta(days=30)
            
            new_status, _, _, _ = calculate_expiration_status(new_expires_at, grace_days if grace_days is not None else 7, None)
            
            cur.execute("""
                UPDATE public.company 
                SET expires_at = %s,
                    expiration_status = %s,
                    suspended_at = NULL,
                    suspension_reason = NULL,
                    expiration_notified_at = NULL,
                    updated_at = NOW()
                WHERE id = %s;
            """, (new_expires_at, new_status, company_id))
            
            log_expiration_event(
                cur, company_id, "reactivated",
                old_expires_at=old_expires_at, new_expires_at=new_expires_at,
                old_status=old_status, new_status=new_status,
                performed_by=user.user_id, 
                notes=f"Reactivated with {extend_days or 30} days extension"
            )
            
        conn.commit()
    
    return {
        "company_id": company_id,
        "company_name": company_name,
        "expires_at": new_expires_at,
        "expiration_status": new_status,
        "message": f"{company_name} has been reactivated"
    }


# ══════════════════════════════════════════════════════════════════════════════
# COMPANY EXPIRATION HISTORY
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/platform/company/{company_id}/expiration-history")
def get_company_expiration_history(
    company_id: int,
    limit: int = Query(50, ge=1, le=200),
    user: TenantContext = Depends(require_platform_user)
):
    """
    Get expiration history for a company.
    Platform admin only.
    """
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT cel.id, cel.event_type, cel.old_expires_at, cel.new_expires_at,
                       cel.old_status, cel.new_status, cel.performed_by, cel.performed_at,
                       cel.notes, u.email as performed_by_email
                FROM public.company_expiration_log cel
                LEFT JOIN public.users u ON u.id = cel.performed_by
                WHERE cel.company_id = %s
                ORDER BY cel.performed_at DESC
                LIMIT %s;
            """, (company_id, limit))
            rows = cur.fetchall()
    
    return {
        "company_id": company_id,
        "history": [
            {
                "id": r[0],
                "event_type": r[1],
                "old_expires_at": r[2],
                "new_expires_at": r[3],
                "old_status": r[4],
                "new_status": r[5],
                "performed_by": r[6],
                "performed_at": r[7],
                "notes": r[8],
                "performed_by_email": r[9]
            }
            for r in rows
        ]
    }
