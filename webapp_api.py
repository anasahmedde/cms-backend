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
import os
import json
from datetime import datetime
from typing import Optional, Dict, Any

import boto3
from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel, Field

from database import pg_conn

router = APIRouter()

# ---- S3 (same bucket/region convention as the rest of the app) ----------------
S3_BUCKET = os.getenv("S3_BUCKET", "digix-videos")
AWS_REGION = os.getenv("AWS_REGION")


def _s3():
    return boto3.client("s3", region_name=AWS_REGION) if AWS_REGION else boto3.client("s3")


def presign_s3(uri, expires=604800):  # 7-day presigned GET URL (device caches locally anyway)
    """Turn an s3://bucket/key URI into a presigned GET URL. Pass-through for plain URLs."""
    if not uri:
        return None
    if not uri.startswith("s3://"):
        return uri
    rest = uri[len("s3://"):]
    bucket, _, key = rest.partition("/")
    try:
        return _s3().generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires)
    except Exception:
        return None


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
        # Device-specific header (text) + footer (image via S3 link). Android never
        # breaks if these are absent; they default to disabled.
        cur.execute("ALTER TABLE public.device ADD COLUMN IF NOT EXISTS header_enabled   BOOLEAN DEFAULT FALSE;")
        cur.execute("ALTER TABLE public.device ADD COLUMN IF NOT EXISTS header_text      TEXT    DEFAULT NULL;")
        cur.execute("ALTER TABLE public.device ADD COLUMN IF NOT EXISTS footer_enabled   BOOLEAN DEFAULT FALSE;")
        cur.execute("ALTER TABLE public.device ADD COLUMN IF NOT EXISTS footer_image_url TEXT    DEFAULT NULL;")  # s3://bucket/key
        # All header/footer styling (colors, font, size, rotation, fit, ...) as one
        # JSON blob — new style options never need another migration.
        cur.execute("ALTER TABLE public.device ADD COLUMN IF NOT EXISTS header_footer_style TEXT DEFAULT NULL;")

        # Header/footer is primarily a GROUP setting. A device inherits its group's
        # header/footer unless header_footer_override = TRUE, in which case the
        # device's own columns above are used instead.
        cur.execute("ALTER TABLE public.device ADD COLUMN IF NOT EXISTS header_footer_override BOOLEAN DEFAULT FALSE;")
        cur.execute('ALTER TABLE public."group" ADD COLUMN IF NOT EXISTS header_enabled      BOOLEAN DEFAULT FALSE;')
        cur.execute('ALTER TABLE public."group" ADD COLUMN IF NOT EXISTS header_text         TEXT    DEFAULT NULL;')
        cur.execute('ALTER TABLE public."group" ADD COLUMN IF NOT EXISTS footer_enabled      BOOLEAN DEFAULT FALSE;')
        cur.execute('ALTER TABLE public."group" ADD COLUMN IF NOT EXISTS footer_image_url    TEXT    DEFAULT NULL;')
        cur.execute('ALTER TABLE public."group" ADD COLUMN IF NOT EXISTS header_footer_style TEXT    DEFAULT NULL;')
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


# ══════════════════════════════════════════════════════════════════════════════
# HEADER (image via S3) + FOOTER (text)
# ══════════════════════════════════════════════════════════════════════════════

class HeaderFooterIn(BaseModel):
    header_enabled: Optional[bool] = None
    header_text: Optional[str] = None
    footer_enabled: Optional[bool] = None
    style: Optional[Dict[str, Any]] = None  # {"header": {...}, "footer": {...}}
    header_footer_override: Optional[bool] = None  # device only: ignore group config


def _parse_style(raw):
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _hf(he, ht, fe, fu, style):
    return {
        "header_enabled": bool(he) if he else False,
        "header_text": ht,
        "footer_enabled": bool(fe) if fe else False,
        "footer_image_url": presign_s3(fu),
        "header_footer_style": style,
    }


_HF_EMPTY = {"header_enabled": False, "header_text": None,
             "footer_enabled": False, "footer_image_url": None,
             "header_footer_style": None}


def device_group_hf(cur, did):
    """The header/footer config of the device's GROUP (or None if it has no group)."""
    cur.execute("""
        SELECT g.id, g.gname, g.header_enabled, g.header_text, g.footer_enabled,
               g.footer_image_url, g.header_footer_style
        FROM public.device_assignment da
        JOIN public."group" g ON g.id = da.gid
        WHERE da.did = %s LIMIT 1;
    """, (did,))
    row = cur.fetchone()
    if not row:
        cur.execute("""
            SELECT g.id, g.gname, g.header_enabled, g.header_text, g.footer_enabled,
                   g.footer_image_url, g.header_footer_style
            FROM public.device_video_shop_group l
            JOIN public."group" g ON g.id = l.gid
            WHERE l.did = %s LIMIT 1;
        """, (did,))
        row = cur.fetchone()
    if not row:
        return None
    return {
        "gid": row[0],
        "gname": row[1],
        "header_enabled": bool(row[2]) if row[2] else False,
        "header_text": row[3],
        "footer_enabled": bool(row[4]) if row[4] else False,
        "footer_image_url": presign_s3(row[5]),
        "style": _parse_style(row[6]),
    }


def resolve_header_footer(cur, did):
    """Effective header/footer for a device.

    Header/footer is a GROUP setting: a device inherits its group's config.
    If the device has header_footer_override = TRUE, its own columns win instead.
    """
    cur.execute("""
        SELECT header_footer_override, header_enabled, header_text,
               footer_enabled, footer_image_url, header_footer_style
        FROM public.device WHERE id = %s;
    """, (did,))
    d = cur.fetchone()
    if d and d[0]:                      # per-device override
        return _hf(d[1], d[2], d[3], d[4], d[5])

    # Otherwise inherit from the device's group (device_assignment is UNIQUE per did)
    cur.execute("""
        SELECT g.header_enabled, g.header_text, g.footer_enabled,
               g.footer_image_url, g.header_footer_style
        FROM public.device_assignment da
        JOIN public."group" g ON g.id = da.gid
        WHERE da.did = %s LIMIT 1;
    """, (did,))
    row = cur.fetchone()
    if not row:                         # fall back to the video-link table
        cur.execute("""
            SELECT g.header_enabled, g.header_text, g.footer_enabled,
                   g.footer_image_url, g.header_footer_style
            FROM public.device_video_shop_group l
            JOIN public."group" g ON g.id = l.gid
            WHERE l.did = %s LIMIT 1;
        """, (did,))
        row = cur.fetchone()

    if row:
        return _hf(row[0], row[1], row[2], row[3], row[4])
    return dict(_HF_EMPTY)


@router.get("/device/{mobile_id}/header-footer")
def get_header_footer(mobile_id: str):
    """Current header/footer config (footer image returned as a presigned URL)."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, header_enabled, header_text, footer_enabled, footer_image_url,
                       header_footer_style, header_footer_override
                FROM public.device WHERE mobile_id = %s ORDER BY id DESC LIMIT 1;
            """, (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            # What the device would actually show (its own override, or the group's),
            # plus the group's own config so the dashboard knows whether the group
            # even has a header/footer to inherit.
            effective = resolve_header_footer(cur, row[0])
            return {
                "mobile_id": mobile_id,
                "header_footer_override": bool(row[6]) if row[6] is not None else False,
                "header_enabled": bool(row[1]) if row[1] is not None else False,
                "header_text": row[2],
                "footer_enabled": bool(row[3]) if row[3] is not None else False,
                "footer_image_url": presign_s3(row[4]),
                "style": _parse_style(row[5]),
                "effective": {**effective, "style": _parse_style(effective.get("header_footer_style"))},
                "group": device_group_hf(cur, row[0]),
            }


@router.post("/device/{mobile_id}/header-footer")
def set_header_footer(mobile_id: str, body: HeaderFooterIn):
    """Set the header/footer flags, header text, and styling (from the dashboard)."""
    sets, vals = [], []
    if body.header_enabled is not None:
        sets.append("header_enabled = %s"); vals.append(body.header_enabled)
    if body.header_text is not None:
        sets.append("header_text = %s"); vals.append(body.header_text or None)
    if body.footer_enabled is not None:
        sets.append("footer_enabled = %s"); vals.append(body.footer_enabled)
    if body.style is not None:
        sets.append("header_footer_style = %s"); vals.append(json.dumps(body.style))
    if body.header_footer_override is not None:
        sets.append("header_footer_override = %s"); vals.append(body.header_footer_override)
    if not sets:
        return {"mobile_id": mobile_id, "updated": False}
    vals.append(mobile_id)
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE public.device SET {', '.join(sets)} WHERE mobile_id = %s RETURNING id;",
                    tuple(vals),
                )
                if not cur.fetchone():
                    conn.rollback()
                    raise HTTPException(status_code=404, detail="Device not found")
            conn.commit()
            return {"mobile_id": mobile_id, "updated": True}
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))


@router.post("/device/{mobile_id}/footer-image")
def upload_footer_image(mobile_id: str, file: UploadFile = File(...)):
    """Upload the footer image to S3, store the s3:// link on the device."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            device_id, _ = _resolve_device(cur, mobile_id)
            if device_id is None:
                raise HTTPException(status_code=404, detail="Device not found")

    fname = file.filename or "footer.jpg"
    ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else "jpg"
    if ext not in ("jpg", "jpeg", "png", "webp", "gif"):
        ext = "jpg"
    key = f"footers/{mobile_id}.{ext}"
    try:
        _s3().upload_fileobj(
            file.file, S3_BUCKET, key,
            ExtraArgs={"ContentType": file.content_type or "image/jpeg", "ACL": "private"},
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 upload failed: {e}")

    uri = f"s3://{S3_BUCKET}/{key}"
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE public.device SET footer_image_url = %s WHERE mobile_id = %s;",
                    (uri, mobile_id),
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))
    return {"mobile_id": mobile_id, "footer_image_url": presign_s3(uri)}


# ══════════════════════════════════════════════════════════════════════════════
# GROUP-LEVEL HEADER / FOOTER (the primary place it is configured)
# Devices in a group inherit this unless they set header_footer_override.
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/group/{gid}/header-footer")
def get_group_header_footer(gid: int):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT header_enabled, header_text, footer_enabled,
                       footer_image_url, header_footer_style
                FROM public."group" WHERE id = %s;
            """, (gid,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Group not found")
            return {
                "gid": gid,
                "header_enabled": bool(row[0]) if row[0] is not None else False,
                "header_text": row[1],
                "footer_enabled": bool(row[2]) if row[2] is not None else False,
                "footer_image_url": presign_s3(row[3]),
                "style": _parse_style(row[4]),
            }


@router.post("/group/{gid}/header-footer")
def set_group_header_footer(gid: int, body: HeaderFooterIn):
    """Set header/footer for a whole group — every device in it inherits this."""
    sets, vals = [], []
    if body.header_enabled is not None:
        sets.append("header_enabled = %s"); vals.append(body.header_enabled)
    if body.header_text is not None:
        sets.append("header_text = %s"); vals.append(body.header_text or None)
    if body.footer_enabled is not None:
        sets.append("footer_enabled = %s"); vals.append(body.footer_enabled)
    if body.style is not None:
        sets.append("header_footer_style = %s"); vals.append(json.dumps(body.style))
    if not sets:
        return {"gid": gid, "updated": False}
    vals.append(gid)
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f'UPDATE public."group" SET {", ".join(sets)} WHERE id = %s RETURNING id;',
                    tuple(vals),
                )
                if not cur.fetchone():
                    conn.rollback()
                    raise HTTPException(status_code=404, detail="Group not found")
            conn.commit()
            return {"gid": gid, "updated": True}
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))


@router.post("/group/{gid}/footer-image")
def upload_group_footer_image(gid: int, file: UploadFile = File(...)):
    """Upload the group's footer image to S3."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT id FROM public."group" WHERE id = %s;', (gid,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Group not found")

    fname = file.filename or "footer.jpg"
    ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else "jpg"
    if ext not in ("jpg", "jpeg", "png", "webp", "gif"):
        ext = "jpg"
    key = f"footers/group-{gid}.{ext}"
    try:
        _s3().upload_fileobj(
            file.file, S3_BUCKET, key,
            ExtraArgs={"ContentType": file.content_type or "image/jpeg", "ACL": "private"},
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 upload failed: {e}")

    uri = f"s3://{S3_BUCKET}/{key}"
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute('UPDATE public."group" SET footer_image_url = %s WHERE id = %s;', (uri, gid))
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))
    return {"gid": gid, "footer_image_url": presign_s3(uri)}
