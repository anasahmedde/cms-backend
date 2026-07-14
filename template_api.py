# template_api.py
"""
Screen Templates API
====================
Platform-designed screen layouts (zones) linked to companies, with tenant-scoped
zone content (shop level, per-device override) and device-facing resolvers.

Fully additive: a company with no template linked is untouched — the resolver
returns 404 and players keep today's behavior.

Include in device_video_shop_group.py:
    from template_api import router as template_router
    app.include_router(template_router, tags=["Screen Templates"])
    # inside startup migration block:
    from migrations.template_schema import ensure_template_schema
    ensure_template_schema(conn)

Route map (mounted WITHOUT prefix; full paths declared here):
  Platform (require_platform_user):
    POST   /platform/templates                       create draft
    GET    /platform/templates                       list (+linked company counts)
    GET    /platform/templates/{tid}                 detail (+linked companies)
    PUT    /platform/templates/{tid}                 update draft fields/zones
    DELETE /platform/templates/{tid}                 delete (blocked while linked)
    POST   /platform/templates/{tid}/publish         validate + snapshot + version++
    POST   /platform/templates/{tid}/duplicate       copy as new draft
    GET    /platform/templates/{tid}/versions        publish history
    POST   /platform/templates/{tid}/rollback/{ver}  restore snapshot (as new publish)
    GET    /platform/templates/{tid}/preview         resolved zones with sample data
    PUT    /platform/companies/{cid}/template        link/unlink {template_id|null}
  Company dashboard (require_tenant_context):
    GET    /company/template                                    linked template + zones
    GET    /company/template-content                            company-wide default content
    PUT    /company/template-content/{zone_key}                 upsert payload (scope=company)
    POST   /company/template-content/{zone_key}/media           upload image/video
    GET    /shop/{shop_id}/template-content                     all zone content for shop
    PUT    /shop/{shop_id}/template-content/{zone_key}          upsert payload
    POST   /shop/{shop_id}/template-content/{zone_key}/media    upload image/video
    GET    /device-config/{device_id}/template-content           device overrides
    PUT    /device-config/{device_id}/template-content/{zone_key}
    DELETE /device-config/{device_id}/template-content/{zone_key}
    POST   /device-config/{device_id}/template-content/{zone_key}/media
  Players (no auth — keyed by mobile_id, consistent with device routes):
    GET    /device/{mobile_id}/template
    GET    /webapp/device/{mobile_id}/template
"""
import io
import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from database import pg_conn
from tenant_context import (
    TenantContext,
    log_audit,
    require_platform_user,
    require_tenant_context,
)

logger = logging.getLogger("template_api")

router = APIRouter()

# Standalone web player (Linux mini-PC browser / kiosk). Read once at import.
_PLAYER_HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "player.html")
try:
    with open(_PLAYER_HTML_PATH, "r", encoding="utf-8") as _f:
        _PLAYER_HTML = _f.read()
except OSError as _e:  # pragma: no cover - only if the asset is missing
    _PLAYER_HTML = None
    logger.error("player.html not found at %s: %s", _PLAYER_HTML_PATH, _e)

S3_BUCKET = os.getenv("S3_BUCKET", "digix-videos")
AWS_REGION = os.getenv("AWS_REGION")
CONTENT_PRESIGN_EXPIRES = int(os.getenv("TEMPLATE_PRESIGN_EXPIRES", str(7 * 24 * 3600)))

ORIENTATIONS = ("landscape", "portrait")
TEMPLATE_STATUSES = ("draft", "published", "archived")
ZONE_TYPES = ("text", "media", "playlist", "qr", "clock", "ticker")
BINDING_SOURCES = ("static", "content", "company.name", "shop.name", "device.name", "device.playlist")
CONTENT_SCOPES = ("company", "shop", "device")
QR_MODES = ("image", "link", "media")
HEX_COLOR_RE = re.compile(r"^#[0-9a-fA-F]{3,8}$")
ZONE_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9_\-]{0,63}$")
MEDIA_EXTENSIONS = {
    "jpg": "image", "jpeg": "image", "png": "image", "gif": "image", "webp": "image",
    "mp4": "video",
}


# ══════════════════════════════════════════════════════════════════════════════
# ZONE VALIDATION (pure functions — covered by tests/)
# ══════════════════════════════════════════════════════════════════════════════

def validate_zones(zones: Any) -> List[str]:
    """Validate a template's zones definition. Returns a list of error strings."""
    errors: List[str] = []
    if not isinstance(zones, list):
        return ["zones must be a list"]
    seen_keys = set()
    for i, zone in enumerate(zones):
        where = f"zones[{i}]"
        if not isinstance(zone, dict):
            errors.append(f"{where}: must be an object")
            continue
        key = zone.get("key")
        if not isinstance(key, str) or not ZONE_KEY_RE.match(key):
            errors.append(f"{where}: 'key' must be a slug (a-z, 0-9, _, -), got {key!r}")
        elif key in seen_keys:
            errors.append(f"{where}: duplicate key {key!r}")
        else:
            seen_keys.add(key)
        ztype = zone.get("type")
        if ztype not in ZONE_TYPES:
            errors.append(f"{where}: 'type' must be one of {ZONE_TYPES}, got {ztype!r}")
        for dim in ("x", "y", "w", "h"):
            v = zone.get(dim)
            if not isinstance(v, (int, float)) or isinstance(v, bool):
                errors.append(f"{where}: '{dim}' must be a number (percent)")
        if not errors or all(isinstance(zone.get(d), (int, float)) and not isinstance(zone.get(d), bool)
                             for d in ("x", "y", "w", "h")):
            x, y, w, h = (float(zone.get(d, -1)) for d in ("x", "y", "w", "h"))
            if not (0 <= x <= 100 and 0 <= y <= 100):
                errors.append(f"{where}: x/y must be within 0-100")
            if not (0 < w <= 100 and 0 < h <= 100):
                errors.append(f"{where}: w/h must be within (0, 100]")
            if x + w > 100.001 or y + h > 100.001:
                errors.append(f"{where}: zone extends past the canvas (x+w={x + w}, y+h={y + h})")
        z = zone.get("z", 1)
        if not isinstance(z, int) or isinstance(z, bool):
            errors.append(f"{where}: 'z' must be an integer")
        style = zone.get("style", {})
        if not isinstance(style, dict):
            errors.append(f"{where}: 'style' must be an object")
        else:
            for color_field in ("bg_color", "text_color"):
                cv = style.get(color_field)
                if cv is not None and (not isinstance(cv, str) or not HEX_COLOR_RE.match(cv)):
                    errors.append(f"{where}: style.{color_field} must be a hex color like #0a1628")
        binding = zone.get("binding", {})
        if not isinstance(binding, dict):
            errors.append(f"{where}: 'binding' must be an object")
            continue
        source = binding.get("source", "static")
        if source not in BINDING_SOURCES:
            errors.append(f"{where}: binding.source must be one of {BINDING_SOURCES}, got {source!r}")
            continue
        if ztype == "playlist" and source != "device.playlist":
            errors.append(f"{where}: playlist zones must bind to 'device.playlist'")
        if ztype != "playlist" and source == "device.playlist":
            errors.append(f"{where}: only playlist zones may bind to 'device.playlist'")
        if ztype == "qr" and source != "content":
            errors.append(f"{where}: qr zones must bind to 'content'")
        if source == "content" and binding.get("scope", "shop") not in CONTENT_SCOPES:
            errors.append(f"{where}: binding.scope must be one of {CONTENT_SCOPES}")
    return errors


def validate_content_payload(zone_type: str, payload: Any) -> List[str]:
    """Validate a zone-content payload for a given zone type. Returns error strings."""
    errors: List[str] = []
    if not isinstance(payload, dict):
        return ["payload must be an object"]
    # Background can be a color, a gradient, or an image (URL or uploaded S3).
    bg_keys = {"bg_color", "bg_gradient", "bg_image_url", "bg_image_s3"}
    media_keys = {"media_s3", "media_url", "media_type"}
    allowed = {
        "text": {"text", "text_color"} | bg_keys,
        "media": media_keys,
        "qr": {"qr_mode", "qr_link", "qr_generated_s3"} | media_keys,
        "ticker": {"text", "text_color"} | bg_keys,
        "clock": {"text_color", "format"} | bg_keys,
        "playlist": set(),
    }.get(zone_type)
    if allowed is None:
        return [f"unknown zone type {zone_type!r}"]
    unknown = set(payload.keys()) - allowed
    if unknown:
        errors.append(f"unknown payload keys for {zone_type} zone: {sorted(unknown)}")
    text = payload.get("text")
    if text is not None and (not isinstance(text, str) or len(text) > 5000):
        errors.append("text must be a string of at most 5000 characters")
    for color_field in ("bg_color", "text_color"):
        cv = payload.get(color_field)
        if cv is not None and (not isinstance(cv, str) or not HEX_COLOR_RE.match(cv)):
            errors.append(f"{color_field} must be a hex color like #0a1628")
    grad = payload.get("bg_gradient")
    if grad is not None:
        errors.extend(_validate_gradient(grad))
    for url_field in ("media_url", "bg_image_url"):
        uv = payload.get(url_field)
        if uv is not None and (not isinstance(uv, str) or len(uv) > 2048 or not uv.startswith(("http://", "https://"))):
            errors.append(f"{url_field} must be an http(s) URL of at most 2048 characters")
    for s3_field in ("media_s3", "bg_image_s3", "qr_generated_s3"):
        sv = payload.get(s3_field)
        if sv is not None and (not isinstance(sv, str) or not sv.startswith("s3://")):
            errors.append(f"{s3_field} must be an s3:// URI")
    if zone_type == "qr":
        mode = payload.get("qr_mode")
        if mode is not None and mode not in QR_MODES:
            errors.append(f"qr_mode must be one of {QR_MODES}")
        link = payload.get("qr_link")
        if link is not None:
            if not isinstance(link, str) or len(link) > 2048 or not link.startswith(("http://", "https://")):
                errors.append("qr_link must be an http(s) URL of at most 2048 characters")
        if mode == "link" and not link:
            errors.append("qr_mode 'link' requires qr_link")
    return errors


def _validate_gradient(grad: Any) -> List[str]:
    if not isinstance(grad, dict):
        return ["bg_gradient must be an object like {stops:[..], angle:int}"]
    errs = []
    stops = grad.get("stops")
    if not isinstance(stops, list) or not (2 <= len(stops) <= 4):
        errs.append("bg_gradient.stops must be a list of 2–4 hex colors")
    else:
        for s in stops:
            if not isinstance(s, str) or not HEX_COLOR_RE.match(s):
                errs.append(f"bg_gradient stop {s!r} must be a hex color")
    angle = grad.get("angle", 135)
    if isinstance(angle, bool) or not isinstance(angle, (int, float)) or not (0 <= angle <= 360):
        errs.append("bg_gradient.angle must be a number 0–360")
    return errs


# ── Raw value parsing (for bulk import cells; the UI sends structured payloads) ──

IMAGE_EXT_RE = re.compile(r"\.(png|jpe?g|gif|webp|svg|bmp)(\?|#|$)", re.I)
VIDEO_EXT_RE = re.compile(r"\.(mp4|webm|mov|m4v|mkv)(\?|#|$)", re.I)
GRADIENT_ANGLE_RE = re.compile(r"@\s*(\d{1,3})")


def resolve_media_value(cur, tenant_id: int, value: str) -> Dict[str, Any]:
    """
    A raw media value → structured fields. Accepts an http(s) URL, an s3:// URI,
    or the NAME of a video / advertisement already in the tenant's library
    (auto-resolved to its stored S3 object). Raises ValueError if a bare name
    matches nothing.
    """
    v = (value or "").strip()
    if not v:
        return {}
    if v.startswith(("http://", "https://")):
        return {"media_url": v, "media_type": "video" if VIDEO_EXT_RE.search(v) else "image"}
    if v.startswith("s3://"):
        return {"media_s3": v, "media_type": "video" if VIDEO_EXT_RE.search(v) else "image"}
    cur.execute("SELECT s3_link, COALESCE(content_type, 'video') FROM public.video "
                "WHERE video_name = %s AND tenant_id = %s LIMIT 1;", (v, tenant_id))
    row = cur.fetchone()
    if row:
        return {"media_s3": row[0], "media_type": "image" if row[1] == "image" else "video"}
    cur.execute("SELECT s3_link FROM public.advertisement WHERE ad_name = %s AND tenant_id = %s LIMIT 1;",
                (v, tenant_id))
    row = cur.fetchone()
    if row:
        return {"media_s3": row[0], "media_type": "image"}
    raise ValueError(f"'{v}' is not a URL and no video/advertisement has that name")


def parse_bg_value(cur, tenant_id: int, value: str) -> Dict[str, Any]:
    """A raw background value → {bg_color} | {bg_gradient} | {bg_image_url|bg_image_s3}."""
    v = (value or "").strip()
    if not v:
        return {}
    hexes = re.findall(r"#[0-9a-fA-F]{3,8}", v)
    if len(hexes) >= 2:
        m = GRADIENT_ANGLE_RE.search(v)
        angle = max(0, min(360, int(m.group(1)))) if m else 135
        return {"bg_gradient": {"stops": hexes[:4], "angle": angle}}
    if len(hexes) == 1 and re.fullmatch(r"\s*#[0-9a-fA-F]{3,8}\s*", v):
        return {"bg_color": hexes[0]}
    mv = resolve_media_value(cur, tenant_id, v)  # image URL / s3 / library name
    if mv.get("media_url"):
        return {"bg_image_url": mv["media_url"]}
    if mv.get("media_s3"):
        return {"bg_image_s3": mv["media_s3"]}
    return {}


def parse_qr_value(cur, tenant_id: int, value: str) -> Dict[str, Any]:
    """A raw QR value → generate-from-link, or show an image/video (URL/s3/name)."""
    v = (value or "").strip()
    if not v:
        return {}
    # An http(s) link WITHOUT an image extension = a link to encode into a QR.
    if v.startswith(("http://", "https://")) and not IMAGE_EXT_RE.search(v):
        return {"qr_mode": "link", "qr_link": v}
    mv = resolve_media_value(cur, tenant_id, v)
    if mv.get("media_type") == "video":
        return {"qr_mode": "media", **mv}
    return {"qr_mode": "image", **mv}


def resolve_zone(zone: Dict, entity: Dict[str, Optional[str]],
                 content: Dict[str, Dict], presign) -> Dict:
    """
    Resolve one template zone into what a player renders. Pure given its inputs.

    entity  — {"company.name": .., "shop.name": .., "device.name": ..}
    content — {zone_key: payload} already collapsed device→shop→company
    presign — callable(s3_uri) -> https URL (injected for testability)
    """
    out = {
        "key": zone.get("key"),
        "type": zone.get("type"),
        "x": zone.get("x"), "y": zone.get("y"),
        "w": zone.get("w"), "h": zone.get("h"),
        "z": zone.get("z", 1),
        "style": zone.get("style", {}),
    }
    source = (zone.get("binding") or {}).get("source", "static")
    if source == "static":
        out["content"] = {"text": zone.get("content", {}).get("text") if isinstance(zone.get("content"), dict) else None}
    elif source == "device.playlist":
        out["content"] = {"playlist": True}
    elif source in ("company.name", "shop.name", "device.name"):
        out["content"] = {"text": entity.get(source) or ""}
    elif source == "content":
        payload = dict(content.get(zone.get("key"), {}))
        resolved: Dict[str, Any] = {}

        # A media value may be an uploaded S3 object (presign it) OR an external
        # URL (pass through as-is). This lets content reference any public image/
        # video URL, or a video/advertisement already in the library (stored as s3://).
        def media_url_of(s3_key, ext_url):
            if ext_url:
                return ext_url
            if s3_key:
                return presign(s3_key)
            return None

        if zone.get("type") == "qr":
            mode = payload.get("qr_mode") or ("link" if payload.get("qr_link") else "image")
            resolved["qr_mode"] = mode
            if mode == "link":
                url = media_url_of(payload.get("qr_generated_s3"), None)
                mtype = "image"
            else:
                url = media_url_of(payload.get("media_s3"), payload.get("media_url"))
                mtype = payload.get("media_type", "image") if mode == "media" else "image"
            if url:
                resolved["media_url"] = url
                resolved["media_type"] = mtype
        elif zone.get("type") == "media":
            url = media_url_of(payload.get("media_s3"), payload.get("media_url"))
            if url:
                resolved["media_url"] = url
                resolved["media_type"] = payload.get("media_type", "image")

        # Shared text/style fields (text + ticker + any zone with a background).
        for k in ("text", "text_color", "bg_color", "format"):
            if payload.get(k) is not None:
                resolved[k] = payload[k]
        # Background can also be a gradient or an image (precedence image > gradient > color).
        if payload.get("bg_gradient") is not None:
            resolved["bg_gradient"] = payload["bg_gradient"]
        bg_img = media_url_of(payload.get("bg_image_s3"), payload.get("bg_image_url"))
        if bg_img:
            resolved["bg_image"] = bg_img
        out["content"] = resolved
    return out


# ══════════════════════════════════════════════════════════════════════════════
# S3 + QR HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _s3_client():
    return boto3.client("s3", region_name=AWS_REGION) if AWS_REGION else boto3.client("s3")


def _parse_s3_uri(uri: str):
    if not uri or not uri.startswith("s3://"):
        return None, None
    rest = uri[len("s3://"):]
    bucket, _, key = rest.partition("/")
    return bucket or None, key or None


def presign_content(uri: str, expires: int = CONTENT_PRESIGN_EXPIRES) -> Optional[str]:
    bucket, key = _parse_s3_uri(uri)
    if not bucket:
        return None
    try:
        return _s3_client().generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires
        )
    except Exception as e:
        logger.error("presign failed for %s: %s", uri, e)
        return None


def _upload_bytes(key: str, data: bytes, content_type: str) -> str:
    _s3_client().put_object(
        Bucket=S3_BUCKET, Key=key, Body=data,
        ACL="private", ServerSideEncryption="AES256", ContentType=content_type,
    )
    return f"s3://{S3_BUCKET}/{key}"


def make_qr_png(link: str) -> bytes:
    """Generate a PNG QR code for a link. Runs once at content-save time."""
    import qrcode  # deferred import: keeps module importable without the dep at tooling time

    qr = qrcode.QRCode(border=2, box_size=10, error_correction=qrcode.constants.ERROR_CORRECT_M)
    qr.add_data(link)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _tenant_slug(cur, tenant_id: int) -> str:
    cur.execute("SELECT slug FROM public.company WHERE id = %s;", (tenant_id,))
    row = cur.fetchone()
    return row[0] if row and row[0] else f"tenant-{tenant_id}"


def _content_media_key(slug: str, scope: str, target_id: int, zone_key: str, ext: str) -> str:
    return f"tenants/{slug}/template-content/{scope}/{target_id}/{zone_key}.{ext}"


def _content_qr_key(slug: str, scope: str, target_id: int, zone_key: str) -> str:
    return f"tenants/{slug}/template-content/{scope}/{target_id}/{zone_key}-qr.png"


# ══════════════════════════════════════════════════════════════════════════════
# MODELS
# ══════════════════════════════════════════════════════════════════════════════

class TemplateCreateIn(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    description: Optional[str] = Field(None, max_length=1000)
    orientation: str = Field("landscape", pattern="^(landscape|portrait)$")
    design_width: int = Field(1920, ge=1, le=10000)
    design_height: int = Field(1080, ge=1, le=10000)
    zones: List[Dict[str, Any]] = Field(default_factory=list)


class TemplateUpdateIn(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=120)
    description: Optional[str] = Field(None, max_length=1000)
    orientation: Optional[str] = Field(None, pattern="^(landscape|portrait)$")
    design_width: Optional[int] = Field(None, ge=1, le=10000)
    design_height: Optional[int] = Field(None, ge=1, le=10000)
    zones: Optional[List[Dict[str, Any]]] = None


class CompanyTemplateLinkIn(BaseModel):
    template_id: Optional[int] = None  # null unlinks


class ZoneContentIn(BaseModel):
    payload: Dict[str, Any] = Field(default_factory=dict)


# ══════════════════════════════════════════════════════════════════════════════
# SHARED QUERIES
# ══════════════════════════════════════════════════════════════════════════════

TEMPLATE_COLS = ("id, name, description, orientation, design_width, design_height, "
                 "zones, status, version, published_at, created_by, created_at, updated_at")
# Qualified variant for JOIN queries (avoids ambiguous column references).
TEMPLATE_COLS_ST = ", ".join(f"st.{c.strip()}" for c in TEMPLATE_COLS.split(","))


def _template_row_to_dict(row) -> Dict:
    zones = row[6]
    if isinstance(zones, str):
        zones = json.loads(zones)
    return {
        "id": row[0], "name": row[1], "description": row[2],
        "orientation": row[3], "design_width": row[4], "design_height": row[5],
        "zones": zones, "status": row[7], "version": row[8],
        "published_at": row[9].isoformat() if row[9] else None,
        "created_by": row[10],
        "created_at": row[11].isoformat() if row[11] else None,
        "updated_at": row[12].isoformat() if row[12] else None,
    }


def _get_template(cur, tid: int) -> Dict:
    cur.execute(f"SELECT {TEMPLATE_COLS} FROM public.screen_template WHERE id = %s;", (tid,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Template not found")
    return _template_row_to_dict(row)


def _linked_companies(cur, tid: int) -> List[Dict]:
    cur.execute("""
        SELECT id, slug, name FROM public.company
        WHERE template_id = %s ORDER BY name;
    """, (tid,))
    return [{"id": r[0], "slug": r[1], "name": r[2]} for r in cur.fetchall()]


def _company_template_stamp(cur, tenant_id: int):
    """(template_version, template_stamp) for a company, or (None, None).

    The stamp changes when the template is re-published OR any zone content of
    the tenant changes — players re-fetch /template when the stamp differs.
    """
    cur.execute("""
        SELECT st.id, st.version
        FROM public.company c
        JOIN public.screen_template st ON st.id = c.template_id AND st.status = 'published'
        WHERE c.id = %s;
    """, (tenant_id,))
    row = cur.fetchone()
    if not row:
        return None, None
    cur.execute("""
        SELECT COALESCE((MAX(EXTRACT(EPOCH FROM updated_at)) * 1000)::bigint, 0)
        FROM public.template_zone_content WHERE tenant_id = %s;
    """, (tenant_id,))
    content_epoch = cur.fetchone()[0] or 0
    return row[1], f"{row[0]}.{row[1]}.{content_epoch}"


def heartbeat_template_fields(cur, tenant_id) -> Dict[str, Any]:
    """Fields merged into the device heartbeat response. Never raises."""
    try:
        if not tenant_id:
            return {"template_version": None, "template_stamp": None}
        version, stamp = _company_template_stamp(cur, tenant_id)
        return {"template_version": version, "template_stamp": stamp}
    except Exception as e:
        logger.warning("heartbeat template fields skipped (tenant %s): %s", tenant_id, e)
        return {"template_version": None, "template_stamp": None}


# ══════════════════════════════════════════════════════════════════════════════
# PLATFORM — TEMPLATE CRUD
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/platform/templates")
def create_template(body: TemplateCreateIn, ctx: TenantContext = Depends(require_platform_user)):
    errors = validate_zones(body.zones)
    if errors:
        raise HTTPException(status_code=422, detail={"zone_errors": errors})
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO public.screen_template
                    (name, description, orientation, design_width, design_height, zones, created_by)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
                RETURNING {TEMPLATE_COLS};
            """, (body.name, body.description, body.orientation,
                  body.design_width, body.design_height, json.dumps(body.zones), ctx.user_id))
            tpl = _template_row_to_dict(cur.fetchone())
            log_audit(conn, None, ctx.user_id, "template.create", "screen_template", tpl["id"],
                      details={"name": body.name})
        conn.commit()
    return tpl


@router.get("/platform/templates")
def list_templates(status: Optional[str] = None, ctx: TenantContext = Depends(require_platform_user)):
    if status is not None and status not in TEMPLATE_STATUSES:
        raise HTTPException(status_code=422, detail=f"status must be one of {TEMPLATE_STATUSES}")
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT {TEMPLATE_COLS},
                       (SELECT COUNT(*) FROM public.company c WHERE c.template_id = t.id) AS linked
                FROM public.screen_template t
                WHERE (%s::text IS NULL OR status = %s)
                ORDER BY updated_at DESC;
            """, (status, status))
            items = []
            for row in cur.fetchall():
                d = _template_row_to_dict(row[:-1])
                d["linked_companies"] = row[-1]
                items.append(d)
    return {"items": items, "count": len(items)}


@router.get("/platform/templates/{tid}")
def get_template(tid: int, ctx: TenantContext = Depends(require_platform_user)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            tpl = _get_template(cur, tid)
            tpl["companies"] = _linked_companies(cur, tid)
    return tpl


@router.put("/platform/templates/{tid}")
def update_template(tid: int, body: TemplateUpdateIn, ctx: TenantContext = Depends(require_platform_user)):
    if body.zones is not None:
        errors = validate_zones(body.zones)
        if errors:
            raise HTTPException(status_code=422, detail={"zone_errors": errors})
    fields, values = [], []
    for col in ("name", "description", "orientation", "design_width", "design_height"):
        v = getattr(body, col)
        if v is not None:
            fields.append(f"{col} = %s")
            values.append(v)
    if body.zones is not None:
        fields.append("zones = %s::jsonb")
        values.append(json.dumps(body.zones))
    if not fields:
        raise HTTPException(status_code=422, detail="Nothing to update")
    with pg_conn() as conn:
        with conn.cursor() as cur:
            _get_template(cur, tid)  # 404 guard
            values.append(tid)
            cur.execute(f"""
                UPDATE public.screen_template
                SET {", ".join(fields)}, updated_at = NOW()
                WHERE id = %s
                RETURNING {TEMPLATE_COLS};
            """, values)
            tpl = _template_row_to_dict(cur.fetchone())
            log_audit(conn, None, ctx.user_id, "template.update", "screen_template", tid)
        conn.commit()
    return tpl


@router.delete("/platform/templates/{tid}")
def delete_template(tid: int, ctx: TenantContext = Depends(require_platform_user)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            _get_template(cur, tid)
            linked = _linked_companies(cur, tid)
            if linked:
                raise HTTPException(
                    status_code=409,
                    detail={"message": "Template is linked to companies — unlink first",
                            "companies": linked},
                )
            cur.execute("DELETE FROM public.screen_template WHERE id = %s;", (tid,))
            log_audit(conn, None, ctx.user_id, "template.delete", "screen_template", tid)
        conn.commit()
    return {"deleted": tid}


@router.post("/platform/templates/{tid}/publish")
def publish_template(tid: int, ctx: TenantContext = Depends(require_platform_user)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            tpl = _get_template(cur, tid)
            if not tpl["zones"]:
                raise HTTPException(status_code=422, detail="Cannot publish a template with no zones")
            errors = validate_zones(tpl["zones"])
            if errors:
                raise HTTPException(status_code=422, detail={"zone_errors": errors})
            new_version = tpl["version"] + 1
            cur.execute(f"""
                UPDATE public.screen_template
                SET status = 'published', version = %s, published_at = NOW(), updated_at = NOW()
                WHERE id = %s
                RETURNING {TEMPLATE_COLS};
            """, (new_version, tid))
            tpl = _template_row_to_dict(cur.fetchone())
            cur.execute("""
                INSERT INTO public.screen_template_version
                    (template_id, version, orientation, design_width, design_height, zones, published_by)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s);
            """, (tid, new_version, tpl["orientation"], tpl["design_width"],
                  tpl["design_height"], json.dumps(tpl["zones"]), ctx.user_id))
            tpl["companies"] = _linked_companies(cur, tid)
            log_audit(conn, None, ctx.user_id, "template.publish", "screen_template", tid,
                      details={"version": new_version, "linked_companies": len(tpl["companies"])})
        conn.commit()
    return tpl


@router.post("/platform/templates/{tid}/duplicate")
def duplicate_template(tid: int, ctx: TenantContext = Depends(require_platform_user)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            src = _get_template(cur, tid)
            cur.execute(f"""
                INSERT INTO public.screen_template
                    (name, description, orientation, design_width, design_height, zones, created_by)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
                RETURNING {TEMPLATE_COLS};
            """, (f"Copy of {src['name']}"[:120], src["description"], src["orientation"],
                  src["design_width"], src["design_height"], json.dumps(src["zones"]), ctx.user_id))
            tpl = _template_row_to_dict(cur.fetchone())
            log_audit(conn, None, ctx.user_id, "template.duplicate", "screen_template", tpl["id"],
                      details={"source": tid})
        conn.commit()
    return tpl


@router.get("/platform/templates/{tid}/versions")
def template_versions(tid: int, ctx: TenantContext = Depends(require_platform_user)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            _get_template(cur, tid)
            cur.execute("""
                SELECT version, published_by, published_at
                FROM public.screen_template_version
                WHERE template_id = %s ORDER BY version DESC;
            """, (tid,))
            versions = [{"version": r[0], "published_by": r[1],
                         "published_at": r[2].isoformat() if r[2] else None}
                        for r in cur.fetchall()]
    return {"template_id": tid, "versions": versions}


@router.post("/platform/templates/{tid}/rollback/{version}")
def rollback_template(tid: int, version: int, ctx: TenantContext = Depends(require_platform_user)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            tpl = _get_template(cur, tid)
            cur.execute("""
                SELECT orientation, design_width, design_height, zones
                FROM public.screen_template_version
                WHERE template_id = %s AND version = %s;
            """, (tid, version))
            snap = cur.fetchone()
            if not snap:
                raise HTTPException(status_code=404, detail=f"No snapshot for version {version}")
            zones = snap[3] if not isinstance(snap[3], str) else json.loads(snap[3])
            new_version = tpl["version"] + 1
            cur.execute(f"""
                UPDATE public.screen_template
                SET orientation = %s, design_width = %s, design_height = %s,
                    zones = %s::jsonb, status = 'published', version = %s,
                    published_at = NOW(), updated_at = NOW()
                WHERE id = %s
                RETURNING {TEMPLATE_COLS};
            """, (snap[0], snap[1], snap[2], json.dumps(zones), new_version, tid))
            tpl = _template_row_to_dict(cur.fetchone())
            cur.execute("""
                INSERT INTO public.screen_template_version
                    (template_id, version, orientation, design_width, design_height, zones, published_by)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s);
            """, (tid, new_version, snap[0], snap[1], snap[2], json.dumps(zones), ctx.user_id))
            log_audit(conn, None, ctx.user_id, "template.rollback", "screen_template", tid,
                      details={"restored_version": version, "new_version": new_version})
        conn.commit()
    return tpl


SAMPLE_ENTITY = {"company.name": "Sample Company", "shop.name": "Sample Shop", "device.name": "Sample Device"}


@router.get("/platform/templates/{tid}/preview")
def preview_template(tid: int, ctx: TenantContext = Depends(require_platform_user)):
    """Resolved zones with placeholder data — feeds the designer's live preview."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            tpl = _get_template(cur, tid)
    zones = [resolve_zone(z, SAMPLE_ENTITY, {}, lambda uri: None) for z in tpl["zones"]]
    return {"template_id": tid, "version": tpl["version"], "orientation": tpl["orientation"],
            "design_width": tpl["design_width"], "design_height": tpl["design_height"],
            "zones": zones, "sample": True}


# ══════════════════════════════════════════════════════════════════════════════
# PLATFORM — COMPANY LINK
# ══════════════════════════════════════════════════════════════════════════════

@router.put("/platform/companies/{cid}/template")
def link_company_template(cid: int, body: CompanyTemplateLinkIn,
                          ctx: TenantContext = Depends(require_platform_user)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM public.company WHERE id = %s;", (cid,))
            company = cur.fetchone()
            if not company:
                raise HTTPException(status_code=404, detail="Company not found")
            if body.template_id is not None:
                cur.execute("SELECT status FROM public.screen_template WHERE id = %s;", (body.template_id,))
                trow = cur.fetchone()
                if not trow:
                    raise HTTPException(status_code=404, detail="Template not found")
                if trow[0] != "published":
                    raise HTTPException(status_code=422, detail="Only published templates can be linked")
            cur.execute("""
                UPDATE public.company
                SET template_id = %s,
                    template_linked_at = CASE WHEN %s::bigint IS NULL THEN NULL ELSE NOW() END,
                    updated_at = NOW()
                WHERE id = %s;
            """, (body.template_id, body.template_id, cid))
            action = "template.link" if body.template_id is not None else "template.unlink"
            log_audit(conn, cid, ctx.user_id, action, "company", cid,
                      details={"template_id": body.template_id})
        conn.commit()
    return {"company_id": cid, "template_id": body.template_id}


# ══════════════════════════════════════════════════════════════════════════════
# COMPANY DASHBOARD — TEMPLATE VIEW + ZONE CONTENT
# ══════════════════════════════════════════════════════════════════════════════

def _tenant_template(cur, tenant_id: int) -> Optional[Dict]:
    cur.execute(f"""
        SELECT {TEMPLATE_COLS_ST}
        FROM public.screen_template st
        JOIN public.company c ON c.template_id = st.id
        WHERE c.id = %s AND st.status = 'published';
    """, (tenant_id,))
    row = cur.fetchone()
    return _template_row_to_dict(row) if row else None


def _content_zone_or_422(template: Dict, zone_key: str) -> Dict:
    zone = next((z for z in template["zones"] if z.get("key") == zone_key), None)
    if zone is None:
        raise HTTPException(status_code=422, detail=f"Zone {zone_key!r} does not exist in the linked template")
    if (zone.get("binding") or {}).get("source") != "content":
        raise HTTPException(status_code=422, detail=f"Zone {zone_key!r} does not take editable content")
    return zone


def _shop_of_tenant(cur, shop_id: int, tenant_id: int):
    cur.execute("SELECT id, shop_name FROM public.shop WHERE id = %s AND tenant_id = %s;",
                (shop_id, tenant_id))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Shop not found")
    return row


def _device_of_tenant(cur, device_id: int, tenant_id: int):
    cur.execute("SELECT id, device_name FROM public.device WHERE id = %s AND tenant_id = %s;",
                (device_id, tenant_id))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Device not found")
    return row


@router.get("/company/template")
def company_template(ctx: TenantContext = Depends(require_tenant_context)):
    """The company's linked template (or template: null) — drives the dashboard content UI."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            tpl = _tenant_template(cur, ctx.active_tenant_id)
    if not tpl:
        return {"template": None}
    content_zones = [z for z in tpl["zones"] if (z.get("binding") or {}).get("source") == "content"]
    return {"template": tpl, "content_zones": content_zones}


def _get_scope_content(cur, tenant_id: int, scope: str, target_col: Optional[str], target_id: Optional[int]):
    if scope == "company":
        cur.execute("""
            SELECT zone_key, payload, updated_at FROM public.template_zone_content
            WHERE tenant_id = %s AND scope = 'company';
        """, (tenant_id,))
    else:
        cur.execute(f"""
            SELECT zone_key, payload, updated_at FROM public.template_zone_content
            WHERE tenant_id = %s AND scope = %s AND {target_col} = %s;
        """, (tenant_id, scope, target_id))
    out = {}
    for zone_key, payload, updated_at in cur.fetchall():
        if isinstance(payload, str):
            payload = json.loads(payload)
        out[zone_key] = {"payload": payload,
                         "updated_at": updated_at.isoformat() if updated_at else None}
    return out


def _upsert_zone_content(conn, cur, tenant_id: int, zone: Dict, zone_key: str, scope: str,
                         shop_id: Optional[int], device_id: Optional[int],
                         payload: Dict, user_id: Optional[int]) -> Dict:
    errors = validate_content_payload(zone["type"], payload)
    if errors:
        raise HTTPException(status_code=422, detail={"payload_errors": errors})

    # QR link mode: generate the PNG once, store alongside the payload.
    if zone["type"] == "qr" and (payload.get("qr_mode") == "link" or
                                 (payload.get("qr_link") and not payload.get("qr_mode"))):
        payload["qr_mode"] = "link"
        slug = _tenant_slug(cur, tenant_id)
        target_id = device_id if scope == "device" else (shop_id if scope == "shop" else tenant_id)
        try:
            png = make_qr_png(payload["qr_link"])
            payload["qr_generated_s3"] = _upload_bytes(
                _content_qr_key(slug, scope, target_id, zone_key), png, "image/png")
        except Exception as e:
            logger.error("QR generation failed (tenant %s zone %s): %s", tenant_id, zone_key, e)
            raise HTTPException(status_code=502, detail="QR code generation/upload failed")

    cur.execute("""
        INSERT INTO public.template_zone_content
            (tenant_id, zone_key, scope, shop_id, device_id, payload, updated_by)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
        ON CONFLICT (tenant_id, zone_key, scope, COALESCE(shop_id, 0), COALESCE(device_id, 0))
        DO UPDATE SET payload = EXCLUDED.payload, updated_by = EXCLUDED.updated_by, updated_at = NOW()
        RETURNING payload;
    """, (tenant_id, zone_key, scope, shop_id, device_id, json.dumps(payload), user_id))
    saved = cur.fetchone()[0]
    if isinstance(saved, str):
        saved = json.loads(saved)
    log_audit(conn, tenant_id, user_id, "template.content.update", "template_zone_content",
              None, details={"zone_key": zone_key, "scope": scope,
                             "shop_id": shop_id, "device_id": device_id})
    return saved


async def _upload_zone_media(conn, cur, tenant_id: int, zone: Dict, zone_key: str, scope: str,
                             shop_id: Optional[int], device_id: Optional[int],
                             file: UploadFile, user_id: Optional[int]) -> Dict:
    ext = (file.filename or "").rsplit(".", 1)[-1].lower()
    media_type = MEDIA_EXTENSIONS.get(ext)
    if not media_type:
        raise HTTPException(status_code=422,
                            detail=f"Unsupported file type .{ext} — allowed: {sorted(MEDIA_EXTENSIONS)}")
    if zone["type"] not in ("media", "qr"):
        raise HTTPException(status_code=422, detail=f"Zone {zone_key!r} does not accept media uploads")
    data = await file.read()
    if not data:
        raise HTTPException(status_code=422, detail="Empty file")
    slug = _tenant_slug(cur, tenant_id)
    target_id = device_id if scope == "device" else (shop_id if scope == "shop" else tenant_id)
    content_type = f"{media_type}/{'jpeg' if ext == 'jpg' else ext}" if media_type == "image" else "video/mp4"
    try:
        uri = _upload_bytes(_content_media_key(slug, scope, target_id, zone_key, ext), data, content_type)
    except Exception as e:
        logger.error("media upload failed (tenant %s zone %s): %s", tenant_id, zone_key, e)
        raise HTTPException(status_code=502, detail="Media upload to storage failed")

    # Merge into the existing payload (keeps text/colors/qr_link already set).
    cur.execute("""
        SELECT payload FROM public.template_zone_content
        WHERE tenant_id = %s AND zone_key = %s AND scope = %s
          AND COALESCE(shop_id, 0) = COALESCE(%s::bigint, 0)
          AND COALESCE(device_id, 0) = COALESCE(%s::bigint, 0);
    """, (tenant_id, zone_key, scope, shop_id, device_id))
    row = cur.fetchone()
    payload = row[0] if row else {}
    if isinstance(payload, str):
        payload = json.loads(payload)
    payload["media_s3"] = uri
    payload["media_type"] = media_type
    if zone["type"] == "qr" and payload.get("qr_mode") != "link":
        payload["qr_mode"] = "media" if media_type == "video" else payload.get("qr_mode", "image")
    return _upsert_zone_content(conn, cur, tenant_id, zone, zone_key, scope,
                                shop_id, device_id, payload, user_id)


@router.get("/company/template-content")
def company_template_content(ctx: TenantContext = Depends(require_tenant_context)):
    """Company-wide default zone content (scope='company' — the resolver's lowest-precedence layer)."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            tpl = _tenant_template(cur, ctx.active_tenant_id)
            content = _get_scope_content(cur, ctx.active_tenant_id, "company", None, None)
    zones = [z for z in (tpl["zones"] if tpl else [])
             if (z.get("binding") or {}).get("source") == "content"]
    return {"template_linked": tpl is not None,
            "content_zones": zones, "content": content}


@router.put("/company/template-content/{zone_key}")
def put_company_zone_content(zone_key: str, body: ZoneContentIn,
                             ctx: TenantContext = Depends(require_tenant_context)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            tpl = _tenant_template(cur, ctx.active_tenant_id)
            if not tpl:
                raise HTTPException(status_code=422, detail="Company has no linked template")
            zone = _content_zone_or_422(tpl, zone_key)
            saved = _upsert_zone_content(conn, cur, ctx.active_tenant_id, zone, zone_key,
                                         "company", None, None, body.payload, ctx.user_id)
        conn.commit()
    return {"zone_key": zone_key, "payload": saved}


@router.post("/company/template-content/{zone_key}/media")
async def upload_company_zone_media(zone_key: str, file: UploadFile = File(...),
                                    ctx: TenantContext = Depends(require_tenant_context)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            tpl = _tenant_template(cur, ctx.active_tenant_id)
            if not tpl:
                raise HTTPException(status_code=422, detail="Company has no linked template")
            zone = _content_zone_or_422(tpl, zone_key)
            saved = await _upload_zone_media(conn, cur, ctx.active_tenant_id, zone, zone_key,
                                             "company", None, None, file, ctx.user_id)
        conn.commit()
    return {"zone_key": zone_key, "payload": saved}


@router.get("/shop/{shop_id}/template-content")
def shop_template_content(shop_id: int, ctx: TenantContext = Depends(require_tenant_context)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            _shop_of_tenant(cur, shop_id, ctx.active_tenant_id)
            tpl = _tenant_template(cur, ctx.active_tenant_id)
            content = _get_scope_content(cur, ctx.active_tenant_id, "shop", "shop_id", shop_id)
    zones = [z for z in (tpl["zones"] if tpl else [])
             if (z.get("binding") or {}).get("source") == "content"]
    return {"shop_id": shop_id, "template_linked": tpl is not None,
            "content_zones": zones, "content": content}


@router.put("/shop/{shop_id}/template-content/{zone_key}")
def put_shop_zone_content(shop_id: int, zone_key: str, body: ZoneContentIn,
                          ctx: TenantContext = Depends(require_tenant_context)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            _shop_of_tenant(cur, shop_id, ctx.active_tenant_id)
            tpl = _tenant_template(cur, ctx.active_tenant_id)
            if not tpl:
                raise HTTPException(status_code=422, detail="Company has no linked template")
            zone = _content_zone_or_422(tpl, zone_key)
            saved = _upsert_zone_content(conn, cur, ctx.active_tenant_id, zone, zone_key,
                                         "shop", shop_id, None, body.payload, ctx.user_id)
        conn.commit()
    return {"shop_id": shop_id, "zone_key": zone_key, "payload": saved}


@router.post("/shop/{shop_id}/template-content/{zone_key}/media")
async def upload_shop_zone_media(shop_id: int, zone_key: str, file: UploadFile = File(...),
                                 ctx: TenantContext = Depends(require_tenant_context)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            _shop_of_tenant(cur, shop_id, ctx.active_tenant_id)
            tpl = _tenant_template(cur, ctx.active_tenant_id)
            if not tpl:
                raise HTTPException(status_code=422, detail="Company has no linked template")
            zone = _content_zone_or_422(tpl, zone_key)
            saved = await _upload_zone_media(conn, cur, ctx.active_tenant_id, zone, zone_key,
                                             "shop", shop_id, None, file, ctx.user_id)
        conn.commit()
    return {"shop_id": shop_id, "zone_key": zone_key, "payload": saved}


@router.get("/device-config/{device_id}/template-content")
def device_template_content(device_id: int, ctx: TenantContext = Depends(require_tenant_context)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            _device_of_tenant(cur, device_id, ctx.active_tenant_id)
            tpl = _tenant_template(cur, ctx.active_tenant_id)
            content = _get_scope_content(cur, ctx.active_tenant_id, "device", "device_id", device_id)
    zones = [z for z in (tpl["zones"] if tpl else [])
             if (z.get("binding") or {}).get("source") == "content"]
    return {"device_id": device_id, "template_linked": tpl is not None,
            "content_zones": zones, "content": content}


@router.put("/device-config/{device_id}/template-content/{zone_key}")
def put_device_zone_content(device_id: int, zone_key: str, body: ZoneContentIn,
                            ctx: TenantContext = Depends(require_tenant_context)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            _device_of_tenant(cur, device_id, ctx.active_tenant_id)
            tpl = _tenant_template(cur, ctx.active_tenant_id)
            if not tpl:
                raise HTTPException(status_code=422, detail="Company has no linked template")
            zone = _content_zone_or_422(tpl, zone_key)
            saved = _upsert_zone_content(conn, cur, ctx.active_tenant_id, zone, zone_key,
                                         "device", None, device_id, body.payload, ctx.user_id)
        conn.commit()
    return {"device_id": device_id, "zone_key": zone_key, "payload": saved}


@router.delete("/device-config/{device_id}/template-content/{zone_key}")
def delete_device_zone_content(device_id: int, zone_key: str,
                               ctx: TenantContext = Depends(require_tenant_context)):
    """Remove a device override — the device falls back to shop/company content."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            _device_of_tenant(cur, device_id, ctx.active_tenant_id)
            cur.execute("""
                DELETE FROM public.template_zone_content
                WHERE tenant_id = %s AND zone_key = %s AND scope = 'device' AND device_id = %s
                RETURNING id;
            """, (ctx.active_tenant_id, zone_key, device_id))
            deleted = cur.fetchone() is not None
            if deleted:
                log_audit(conn, ctx.active_tenant_id, ctx.user_id, "template.content.delete",
                          "template_zone_content", None,
                          details={"zone_key": zone_key, "device_id": device_id})
        conn.commit()
    return {"device_id": device_id, "zone_key": zone_key, "deleted": deleted}


@router.post("/device-config/{device_id}/template-content/{zone_key}/media")
async def upload_device_zone_media(device_id: int, zone_key: str, file: UploadFile = File(...),
                                   ctx: TenantContext = Depends(require_tenant_context)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            _device_of_tenant(cur, device_id, ctx.active_tenant_id)
            tpl = _tenant_template(cur, ctx.active_tenant_id)
            if not tpl:
                raise HTTPException(status_code=422, detail="Company has no linked template")
            zone = _content_zone_or_422(tpl, zone_key)
            saved = await _upload_zone_media(conn, cur, ctx.active_tenant_id, zone, zone_key,
                                             "device", None, device_id, file, ctx.user_id)
        conn.commit()
    return {"device_id": device_id, "zone_key": zone_key, "payload": saved}


# ══════════════════════════════════════════════════════════════════════════════
# PLAYER-FACING RESOLVER (no auth — mobile_id keyed, like all device routes)
# ══════════════════════════════════════════════════════════════════════════════

def _collapse_content(cur, tenant_id: int, shop_id: Optional[int], device_id: int) -> Dict[str, Dict]:
    """zone_key -> payload with device > shop > company precedence."""
    cur.execute("""
        SELECT zone_key, scope, payload FROM public.template_zone_content
        WHERE tenant_id = %s
          AND (scope = 'company'
               OR (scope = 'shop' AND shop_id = COALESCE(%s::bigint, -1))
               OR (scope = 'device' AND device_id = %s));
    """, (tenant_id, shop_id, device_id))
    rank = {"company": 0, "shop": 1, "device": 2}
    best: Dict[str, tuple] = {}
    for zone_key, scope, payload in cur.fetchall():
        if isinstance(payload, str):
            payload = json.loads(payload)
        if zone_key not in best or rank[scope] > best[zone_key][0]:
            best[zone_key] = (rank[scope], payload)
    return {k: v[1] for k, v in best.items()}


@router.get("/player", response_class=HTMLResponse)
@router.get("/webapp/player", response_class=HTMLResponse)
def web_player():
    """
    Self-contained screen-template renderer for a browser/kiosk on a Linux
    player. Open with ?device=<mobile_id> (e.g. /player?device=DGX123). The page
    resolves and renders the device's template and plays its playlist zone from
    the existing content pipeline; degrades to a cached render when offline.
    """
    if _PLAYER_HTML is None:
        raise HTTPException(status_code=500, detail="Player asset unavailable")
    return HTMLResponse(content=_PLAYER_HTML)


@router.get("/device/{mobile_id}/template")
@router.get("/webapp/device/{mobile_id}/template")
def device_template(mobile_id: str):
    """
    The resolved template a player renders. 404 when the device's company has
    no published template linked — players fall back to today's behavior.
    """
    from company_expiration_api import check_company_access

    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, tenant_id, is_active, device_name
                FROM public.device WHERE mobile_id = %s ORDER BY id DESC LIMIT 1;
            """, (mobile_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Device not found")
            did, tenant_id, is_active, device_name = row
            if is_active is False:
                raise HTTPException(status_code=404, detail="Device is deactivated")
            if tenant_id:
                access = check_company_access(tenant_id)
                if not access["accessible"]:
                    raise HTTPException(status_code=403, detail=access["message"])
            tpl = _tenant_template(cur, tenant_id) if tenant_id else None
            if not tpl:
                raise HTTPException(status_code=404, detail="No template linked")

            cur.execute("""
                SELECT da.sid, s.shop_name, c.name
                FROM public.device d
                LEFT JOIN public.device_assignment da ON da.did = d.id
                LEFT JOIN public.shop s ON s.id = da.sid
                LEFT JOIN public.company c ON c.id = d.tenant_id
                WHERE d.id = %s;
            """, (did,))
            arow = cur.fetchone() or (None, None, None)
            shop_id, shop_name, company_name = arow
            if shop_id is None:
                # Fallback: shop via the content-link table (device may predate device_assignment).
                cur.execute("""
                    SELECT l.sid, s.shop_name FROM public.device_video_shop_group l
                    JOIN public.shop s ON s.id = l.sid
                    WHERE l.did = %s ORDER BY l.id DESC LIMIT 1;
                """, (did,))
                lrow = cur.fetchone()
                if lrow:
                    shop_id, shop_name = lrow

            entity = {"company.name": company_name, "shop.name": shop_name, "device.name": device_name}
            content = _collapse_content(cur, tenant_id, shop_id, did)
            _, stamp = _company_template_stamp(cur, tenant_id)

    zones = [resolve_zone(z, entity, content, presign_content) for z in tpl["zones"]]
    return {
        "mobile_id": mobile_id,
        "template_id": tpl["id"],
        "version": tpl["version"],
        "template_stamp": stamp,
        "orientation": tpl["orientation"],
        "design_width": tpl["design_width"],
        "design_height": tpl["design_height"],
        "zones": zones,
    }
