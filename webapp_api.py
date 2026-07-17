# webapp_api.py
# Location: cms-backend/webapp_api.py
"""
Web-App (Linux Mini-PC player) API
==================================
Brand-new, isolated endpoints for the Linux web player + camera gender counting.

IMPORTANT: This router is fully additive and mounted under the /webapp prefix.
It does NOT modify or share any route with the Android app's existing /device/*
endpoints — the Android app is guaranteed unaffected. Content/playlist for the
web player is served by the EXISTING device endpoints (reused read-only).

Include in device_video_shop_group.py:
    from webapp_api import router as webapp_router, ensure_webapp_schema
    app.include_router(webapp_router, prefix="/webapp", tags=["WebApp"])
    # inside startup migration block:  ensure_webapp_schema(conn)
"""
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from database import pg_conn

router = APIRouter()


# ══════════════════════════════════════════════════════════════════════════════
# SCHEMA (idempotent — safe to run on every startup, invisible to the Android app)
# ══════════════════════════════════════════════════════════════════════════════

def ensure_webapp_schema(conn):
    with conn.cursor() as cur:
        # Optional per-device feature flag (Android never reads this column).
        cur.execute("""
            ALTER TABLE public.device
            ADD COLUMN IF NOT EXISTS gender_counting_enabled BOOLEAN DEFAULT FALSE;
        """)
        # Gender count history — brand new table, only the web app touches it.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.gender_count_history (
                id          SERIAL PRIMARY KEY,
                device_id   INTEGER,
                mobile_id   VARCHAR(64) NOT NULL,
                tenant_id   INTEGER,
                male        INTEGER NOT NULL DEFAULT 0,
                female      INTEGER NOT NULL DEFAULT 0,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_gender_hist_mobile_time
            ON public.gender_count_history (mobile_id, created_at);
        """)
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# MODELS
# ══════════════════════════════════════════════════════════════════════════════

class GenderCountIn(BaseModel):
    male: int = Field(0, ge=0)
    female: int = Field(0, ge=0)
    timestamp: Optional[int] = None  # client epoch ms (optional; server time is authoritative)


class GenderEnabledIn(BaseModel):
    enabled: bool = False


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _resolve_device(cur, mobile_id: str):
    """Return (device_id, tenant_id) for a mobile_id, or (None, None)."""
    cur.execute(
        "SELECT id, tenant_id FROM public.device WHERE mobile_id = %s ORDER BY id DESC LIMIT 1;",
        (mobile_id,),
    )
    row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


# ══════════════════════════════════════════════════════════════════════════════
# DEVICE-FACING (called by the Linux web player — no auth, like the Android device endpoints)
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/device/{mobile_id}/config")
def webapp_config(mobile_id: str):
    """Web-player heartbeat/config. Tells the player whether gender counting is on."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT is_muted, resolution, gender_counting_enabled
                FROM public.device WHERE mobile_id = %s ORDER BY id DESC LIMIT 1;
            """, (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            return {
                "mobile_id": mobile_id,
                "is_muted": bool(row[0]) if row[0] is not None else False,
                "resolution": row[1],
                "gender_counting_enabled": bool(row[2]) if row[2] is not None else False,
            }


@router.post("/device/{mobile_id}/gender-count")
def post_gender_count(mobile_id: str, body: GenderCountIn):
    """Web player posts the delta counts since its last successful post."""
    if body.male == 0 and body.female == 0:
        return {"ok": True, "stored": 0}
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                device_id, tenant_id = _resolve_device(cur, mobile_id)
                if device_id is None:
                    conn.rollback()
                    raise HTTPException(status_code=404, detail="Device not found")
                cur.execute("""
                    INSERT INTO public.gender_count_history
                        (device_id, mobile_id, tenant_id, male, female)
                    VALUES (%s, %s, %s, %s, %s);
                """, (device_id, mobile_id, tenant_id, body.male, body.female))
            conn.commit()
            return {"ok": True, "male": body.male, "female": body.female}
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD-FACING (called by cms-frontend)
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/device/{mobile_id}/gender-summary")
def gender_summary(mobile_id: str):
    """Totals for today, this month, and all-time."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  COALESCE(SUM(male)   FILTER (WHERE created_at::date = CURRENT_DATE), 0),
                  COALESCE(SUM(female) FILTER (WHERE created_at::date = CURRENT_DATE), 0),
                  COALESCE(SUM(male)   FILTER (WHERE date_trunc('month', created_at) = date_trunc('month', now())), 0),
                  COALESCE(SUM(female) FILTER (WHERE date_trunc('month', created_at) = date_trunc('month', now())), 0),
                  COALESCE(SUM(male), 0),
                  COALESCE(SUM(female), 0)
                FROM public.gender_count_history WHERE mobile_id = %s;
            """, (mobile_id,))
            r = cur.fetchone() or (0, 0, 0, 0, 0, 0)
            return {
                "mobile_id": mobile_id,
                "today":   {"male": r[0], "female": r[1], "total": r[0] + r[1]},
                "month":   {"male": r[2], "female": r[3], "total": r[2] + r[3]},
                "alltime": {"male": r[4], "female": r[5], "total": r[4] + r[5]},
            }


@router.get("/device/{mobile_id}/gender-series")
def gender_series(mobile_id: str, range: str = "24h"):
    """Time-bucketed series for charting. range = 24h | 7d | 30d."""
    if range == "24h":
        bucket, since = "hour", "24 hours"
    elif range == "7d":
        bucket, since = "day", "7 days"
    else:
        bucket, since = "day", "30 days"
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT date_trunc('{bucket}', created_at) AS bucket,
                       COALESCE(SUM(male), 0), COALESCE(SUM(female), 0)
                FROM public.gender_count_history
                WHERE mobile_id = %s AND created_at >= now() - INTERVAL '{since}'
                GROUP BY bucket ORDER BY bucket;
            """, (mobile_id,))
            rows = cur.fetchall()
            return {
                "mobile_id": mobile_id,
                "range": range,
                "series": [
                    {"t": b.isoformat(), "male": m, "female": f} for (b, m, f) in rows
                ],
            }


@router.post("/device/{mobile_id}/gender-enabled")
def set_gender_enabled(mobile_id: str, body: GenderEnabledIn):
    """Toggle the camera gender-counting feature for a device (from the dashboard)."""
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE public.device SET gender_counting_enabled = %s WHERE mobile_id = %s RETURNING id;",
                    (body.enabled, mobile_id),
                )
                if not cur.fetchone():
                    conn.rollback()
                    raise HTTPException(status_code=404, detail="Device not found")
            conn.commit()
            return {"mobile_id": mobile_id, "gender_counting_enabled": body.enabled}
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))
