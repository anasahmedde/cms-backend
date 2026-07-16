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
    GET    /company/template/design                             company's own editable template (fork-on-write)
    PUT    /company/template/design                             save company template (forks on first write)
    POST   /company/template/design/publish                     publish + re-link the company to its own copy
    GET    /company/template-content                            company-wide default content
    PUT    /company/template-content/{zone_key}                 upsert payload (scope=company)
    POST   /company/template-content/{zone_key}/media           upload image/video
    GET    /shop/{shop_id}/template-content                     all zone content for shop
    PUT    /shop/{shop_id}/template-content/{zone_key}          upsert payload
    POST   /shop/{shop_id}/template-content/{zone_key}/media    upload image/video
    GET    /group/{group_id}/template-content                   all zone content for group
    PUT    /group/{group_id}/template-content/{zone_key}        upsert payload (scope=group)
    DELETE /group/{group_id}/template-content/{zone_key}
    POST   /group/{group_id}/template-content/{zone_key}/media  upload image/video
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
CONTENT_SCOPES = ("company", "shop", "group", "device")
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
            # Designer-set backgrounds (same schema as content payloads; the
            # resolver folds them into resolved content so players need nothing new).
            if style.get("bg_gradient") is not None:
                errors.extend(f"{where}: style.{e}" for e in _validate_gradient(style["bg_gradient"]))
            biu = style.get("bg_image_url")
            if biu is not None and (not isinstance(biu, str)
                                    or not biu.startswith(("http://", "https://"))
                                    or len(biu) > 2048):
                errors.append(f"{where}: style.bg_image_url must be an http(s) URL (max 2048 chars)")
            # Media-library background (s3:// object; presigned by the resolver).
            bis = style.get("bg_image_s3")
            if bis is not None and (not isinstance(bis, str) or not bis.startswith("s3://")):
                errors.append(f"{where}: style.bg_image_s3 must be an s3:// URI")
            # Typography / layout knobs the players already consume.
            fsv = style.get("font_size_vh")
            if fsv is not None and (isinstance(fsv, bool) or not isinstance(fsv, (int, float)) or not (1 <= fsv <= 100)):
                errors.append(f"{where}: style.font_size_vh must be a number 1-100")
            if style.get("align") is not None and style["align"] not in ("left", "center", "right"):
                errors.append(f"{where}: style.align must be left|center|right")
            if style.get("valign") is not None and style["valign"] not in ("top", "middle", "bottom"):
                errors.append(f"{where}: style.valign must be top|middle|bottom")
            if style.get("direction") is not None and style["direction"] not in ("ltr", "rtl"):
                errors.append(f"{where}: style.direction must be ltr|rtl")
            if style.get("bold") is not None and not isinstance(style["bold"], bool):
                errors.append(f"{where}: style.bold must be a boolean")
            if style.get("fit_mode") is not None and style["fit_mode"] not in ("cover", "contain"):
                errors.append(f"{where}: style.fit_mode must be cover|contain")
            ts = style.get("ticker_speed")
            if ts is not None and (isinstance(ts, bool) or not isinstance(ts, (int, float)) or not (1 <= ts <= 100)):
                errors.append(f"{where}: style.ticker_speed must be a number 1-100")
            pp = style.get("padding_pct")
            if pp is not None and (isinstance(pp, bool) or not isinstance(pp, (int, float)) or not (0 <= pp <= 40)):
                errors.append(f"{where}: style.padding_pct must be a number 0-40")
        # Multiple positioned text items inside a text/ticker zone (designer-composed).
        zcontent = zone.get("content")
        if isinstance(zcontent, dict) and zcontent.get("runs") is not None:
            if ztype not in ("text", "ticker"):
                errors.append(f"{where}: content.runs is only valid on text/ticker zones")
            else:
                errors.extend(f"{where}: {e}" for e in _validate_text_runs(zcontent["runs"]))
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
    media_keys = {"media_s3", "media_url", "media_type", "fit_mode"}
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
    fm = payload.get("fit_mode")
    if fm is not None and fm not in ("cover", "contain", "fill", "none"):
        errors.append("fit_mode must be cover, contain, fill, or none")
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


def _validate_text_runs(runs: Any) -> List[str]:
    """Validate a text/ticker zone's content.runs — multiple positioned,
    individually-styled text items placed inside the zone. Positions/size are
    percentages relative to the zone (resolution-independent)."""
    errs: List[str] = []
    if not isinstance(runs, list):
        return ["content.runs must be a list"]
    if len(runs) > 40:
        errs.append("content.runs may have at most 40 items")
    for i, r in enumerate(runs):
        at = f"content.runs[{i}]"
        if not isinstance(r, dict):
            errs.append(f"{at}: must be an object"); continue
        if not isinstance(r.get("text"), str) or len(r.get("text", "")) > 2000:
            errs.append(f"{at}.text must be a string of at most 2000 chars")
        for k in ("x", "y"):
            v = r.get(k)
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not (0 <= v <= 100):
                errs.append(f"{at}.{k} must be a number 0-100 (percent of the zone)")
        for k in ("w", "font_size_vh"):
            v = r.get(k)
            if v is not None and (isinstance(v, bool) or not isinstance(v, (int, float)) or not (0 < v <= 100)):
                errs.append(f"{at}.{k} must be a number in (0, 100]")
        tc = r.get("text_color")
        if tc is not None and (not isinstance(tc, str) or not HEX_COLOR_RE.match(tc)):
            errs.append(f"{at}.text_color must be a hex color")
        if r.get("bold") is not None and not isinstance(r.get("bold"), bool):
            errs.append(f"{at}.bold must be a boolean")
        if r.get("align") is not None and r.get("align") not in ("left", "center", "right"):
            errs.append(f"{at}.align must be left|center|right")
    return errs


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
    # Not found — suggest the closest library names so a typo is a one-edit fix.
    import difflib
    cur.execute("SELECT video_name FROM public.video WHERE tenant_id = %s;", (tenant_id,))
    names = [r[0] for r in cur.fetchall() if r[0]]
    cur.execute("SELECT ad_name FROM public.advertisement WHERE tenant_id = %s;", (tenant_id,))
    names += [r[0] for r in cur.fetchall() if r[0]]
    close = difflib.get_close_matches(v, names, n=2, cutoff=0.6)
    hint = f" — did you mean {' or '.join(repr(c) for c in close)}?" if close else ""
    raise ValueError(f"'{v}' is not a URL and no video/advertisement has that name{hint}")


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
        "style": dict(zone.get("style") or {}),
    }
    source = (zone.get("binding") or {}).get("source", "static")
    zc = zone.get("content") if isinstance(zone.get("content"), dict) else {}
    if source == "static":
        out["content"] = {"text": zc.get("text")}
        # Designer-composed multiple text items ride through as-is; percentages
        # are relative to the zone, so players render them at any resolution.
        if isinstance(zc.get("runs"), list) and zc["runs"]:
            out["content"]["runs"] = zc["runs"]
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
        # Per-content fit (cover = fill+crop, contain = show whole). Folded into
        # style, which every player already reads (style.fit_mode), so a per-image
        # fit works with no player change and overrides the zone's designer default.
        if payload.get("fit_mode") in ("cover", "contain", "fill", "none"):
            out["style"]["fit_mode"] = payload["fit_mode"]
        out["content"] = resolved

    # Designer-set zone backgrounds (style.bg_gradient / style.bg_image_url)
    # fold into resolved content — the players already render content-level
    # backgrounds for every zone type, so no player change is needed. Tenant
    # content, when present, wins.
    style = out.get("style") or {}
    if isinstance(out.get("content"), dict):
        c = out["content"]
        if c.get("bg_gradient") is None and style.get("bg_gradient") is not None:
            c["bg_gradient"] = style["bg_gradient"]
        # A designer-picked media-library background (s3://) is presigned here so
        # players get an https URL — same as an uploaded content background.
        if not c.get("bg_image") and style.get("bg_image_s3"):
            signed = presign(style["bg_image_s3"])
            if signed:
                c["bg_image"] = signed
        if not c.get("bg_image") and style.get("bg_image_url"):
            c["bg_image"] = style["bg_image_url"]
        # Solid color too — the Android renderer draws content-level backgrounds;
        # a designer-set style.bg_color must reach it the same way.
        if (c.get("bg_color") is None and c.get("bg_gradient") is None
                and not c.get("bg_image") and style.get("bg_color")):
            c["bg_color"] = style["bg_color"]
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


def _s3_bucket_key(link: str):
    """Parse an S3 reference stored in ANY historical form → (bucket, key).
    Accepts s3://bucket/key, an S3 https URL (virtual-hosted or path-style), or a
    bare key (assumes S3_BUCKET). Mirrors the tolerance the rest of the app already
    has (video_service._parse_s3_link) so a media-library item's s3_link resolves
    no matter how it was stored. Returns (None, None) for a non-S3 URL (that belongs
    in media_url, not media_s3)."""
    s = (link or "").strip()
    if not s:
        return None, None
    if s.startswith("s3://"):
        return _parse_s3_uri(s)
    if s.startswith(("http://", "https://")):
        from urllib.parse import urlparse
        u = urlparse(s)
        host, path = u.netloc, (u.path or "").lstrip("/")
        if "amazonaws" not in host:        # not an S3 URL — don't guess a bucket
            return None, None
        if ".s3" in host:                  # virtual-hosted: bucket.s3.<region>.amazonaws.com/key
            return (host.split(".s3", 1)[0] or None), (path or None)
        first, _, rest = path.partition("/")  # path-style: s3.<region>.amazonaws.com/bucket/key
        return (first or None), (rest or None)
    return S3_BUCKET, s                     # bare key


def _canonical_s3_ref(link: str) -> str:
    """Normalize any S3 reference to canonical s3://bucket/key; leave it unchanged
    if it isn't parseable as S3 (so validation still rejects a genuine non-S3 value)."""
    bucket, key = _s3_bucket_key(link)
    return f"s3://{bucket}/{key}" if bucket and key else link


def presign_content(uri: str, expires: int = CONTENT_PRESIGN_EXPIRES,
                    response_content_type: Optional[str] = None) -> Optional[str]:
    bucket, key = _s3_bucket_key(uri)
    if not bucket or not key:
        return None
    params = {"Bucket": bucket, "Key": key}
    # Override the object's stored Content-Type on the response — repairs images
    # that were uploaded to the video stack with a video/mp4 content-type (they'd
    # otherwise fail to render in a browser <img>).
    if response_content_type:
        params["ResponseContentType"] = response_content_type
    try:
        return _s3_client().generate_presigned_url("get_object", Params=params, ExpiresIn=expires)
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
    """Companies using this template — linked directly, or through their own
    customized copy (a fork carries source_template_id back to the original)."""
    cur.execute("""
        SELECT c.id, c.slug, c.name, (ct.owner_tenant_id IS NOT NULL) AS customized
        FROM public.company c
        JOIN public.screen_template ct ON ct.id = c.template_id
        WHERE ct.id = %s OR (ct.source_template_id = %s AND ct.owner_tenant_id IS NOT NULL)
        ORDER BY c.name;
    """, (tid, tid))
    return [{"id": r[0], "slug": r[1], "name": r[2], "customized": r[3]}
            for r in cur.fetchall()]


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
    # Max updated_at across BOTH content tables — a group-scope edit must bump
    # the stamp too, or devices in that group never re-fetch the new content.
    cur.execute("""
        SELECT COALESCE((MAX(EXTRACT(EPOCH FROM updated_at)) * 1000)::bigint, 0) FROM (
            SELECT updated_at FROM public.template_zone_content WHERE tenant_id = %s
            UNION ALL
            SELECT updated_at FROM public.template_zone_group_content WHERE tenant_id = %s
        ) AS all_content;
    """, (tenant_id, tenant_id))
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
            # "linked" must see THROUGH company forks: a company that published a
            # customized copy is linked to the fork row (hidden below), not to the
            # original — counting template_id alone shows "not linked" while the
            # template is in active (customized) use.
            cur.execute(f"""
                SELECT {TEMPLATE_COLS},
                       (SELECT COUNT(*) FROM public.company c WHERE c.template_id = t.id) AS linked,
                       (SELECT COUNT(*) FROM public.company c
                          JOIN public.screen_template f ON f.id = c.template_id
                          WHERE f.source_template_id = t.id
                            AND f.owner_tenant_id IS NOT NULL) AS customized
                FROM public.screen_template t
                WHERE (%s::text IS NULL OR status = %s)
                  AND owner_tenant_id IS NULL   -- hide company-forked private copies
                ORDER BY updated_at DESC;
            """, (status, status))
            items = []
            for row in cur.fetchall():
                d = _template_row_to_dict(row[:-2])
                d["linked_companies"] = row[-2] + row[-1]
                d["customized_companies"] = row[-1]
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
            # Only DIRECT links block deletion. Companies on a customized copy
            # keep their own fork row (FK sets the fork's source to NULL), so
            # deleting the original can't break them — and blocking on them
            # would create an undeletable template with no way to "unlink".
            linked = [c for c in _linked_companies(cur, tid) if not c["customized"]]
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
                cur.execute("SELECT status, owner_tenant_id FROM public.screen_template WHERE id = %s;",
                            (body.template_id,))
                trow = cur.fetchone()
                if not trow:
                    raise HTTPException(status_code=404, detail="Template not found")
                if trow[0] != "published":
                    raise HTTPException(status_code=422, detail="Only published templates can be linked")
                # A company-owned (forked) template may only be linked back to the
                # company that owns it — never shared to another tenant.
                if trow[1] is not None and trow[1] != cid:
                    raise HTTPException(status_code=422,
                                        detail="That template is a company-private copy and can't be linked to another company")
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


def _group_of_tenant(cur, group_id: int, tenant_id: int):
    # Groups are tenant-scoped via tenant_id (the column every other group query
    # uses — company_id from an older migration is unpopulated by the create
    # path). Never let one company read/write another company's group content.
    cur.execute('SELECT id, gname FROM public."group" WHERE id = %s AND tenant_id = %s;',
                (group_id, tenant_id))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Group not found")
    return row


def _device_of_tenant(cur, device_id: int, tenant_id: int):
    cur.execute("SELECT id, device_name, reported_resolution FROM public.device"
                " WHERE id = %s AND tenant_id = %s;",
                (device_id, tenant_id))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Device not found")
    return row


def _design_size_of(tpl: Optional[Dict]) -> Dict[str, Any]:
    """Template design canvas size — lets the dashboard translate a zone's %
    geometry into concrete pixels ("make this image 1152×486") wherever content
    is authored."""
    return {"design_width": tpl["design_width"] if tpl else None,
            "design_height": tpl["design_height"] if tpl else None}


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


@router.get("/company/template/preview")
def company_template_preview(scope: str = "company",
                             shop_id: Optional[int] = None,
                             device_id: Optional[int] = None,
                             group_id: Optional[int] = None,
                             ctx: TenantContext = Depends(require_tenant_context)):
    """Resolved + presigned zones for a WYSIWYG dashboard preview — what a screen
    actually renders at the given scope (company default / a location / a group /
    a screen). Media is presigned so the dashboard can show the real image/video."""
    tenant_id = ctx.active_tenant_id
    with pg_conn() as conn:
        with conn.cursor() as cur:
            tpl = _tenant_template(cur, tenant_id)
            if not tpl:
                return {"template": None, "zones": []}
            shop_name = device_name = None
            eff_shop = None
            eff_group = group_id if scope == "group" else None
            if scope == "device" and device_id:
                cur.execute("SELECT device_name FROM public.device WHERE id = %s AND tenant_id = %s;",
                            (device_id, tenant_id))
                r = cur.fetchone(); device_name = r[0] if r else None
                # The device's location + group, so those layers resolve too.
                cur.execute("""SELECT da.sid, s.shop_name, da.gid FROM public.device_assignment da
                               LEFT JOIN public.shop s ON s.id = da.sid WHERE da.did = %s LIMIT 1;""", (device_id,))
                sr = cur.fetchone()
                if sr: eff_shop, shop_name, eff_group = sr[0], sr[1], sr[2]
            elif scope == "shop" and shop_id:
                cur.execute("SELECT shop_name FROM public.shop WHERE id = %s AND tenant_id = %s;",
                            (shop_id, tenant_id))
                r = cur.fetchone(); shop_name = r[0] if r else None
                eff_shop = shop_id
            content = _collapse_content(
                cur, tenant_id,
                eff_shop if scope in ("shop", "device") else None,
                device_id if scope == "device" else -1,
                eff_group)
            entity = {"company.name": ctx.company_name, "shop.name": shop_name, "device.name": device_name}
    zones = [resolve_zone(z, entity, content, presign_content) for z in tpl["zones"]]
    # Repair browser rendering: an image uploaded to the video stack carries a
    # video/mp4 content-type, which a preview <img> refuses. Re-presign S3-backed
    # image media with an explicit image content-type so the preview shows it.
    for rz in zones:
        rc = rz.get("content") or {}
        if rz.get("type") == "media" and rc.get("media_type") == "image" and rc.get("media_url"):
            s3ref = (content.get(rz.get("key")) or {}).get("media_s3")
            if s3ref:
                fixed = presign_content(s3ref, response_content_type="image/jpeg")
                if fixed:
                    rc["media_url"] = fixed
    return {
        "template": {"name": tpl["name"], "version": tpl["version"], "orientation": tpl["orientation"],
                     "design_width": tpl["design_width"], "design_height": tpl["design_height"]},
        "zones": zones,
    }


# ── Company-scoped designer ────────────────────────────────────────────────
# A company admin can open the SAME designer for their own template. Editing is
# always done on a company-private copy (owner_tenant_id = the tenant): on the
# first write we fork the company's currently-linked platform template into an
# owned draft. The company's live screens keep using their linked template until
# the company publishes the fork — at which point we re-link them. This keeps
# every company's layout independent and can never touch the shared platform
# template or another tenant's copy.

def _require_company_settings(ctx: TenantContext) -> int:
    """Gate + resolve the active tenant for company-designer writes."""
    if not ctx.has_permission("manage_company_settings"):
        raise HTTPException(status_code=403, detail="Permission denied: manage_company_settings")
    if ctx.active_tenant_id is None:
        raise HTTPException(status_code=400, detail="No company context")
    return ctx.active_tenant_id


def _company_owned_template(cur, tenant_id: int) -> Optional[Dict]:
    """The tenant's own editable template (draft or published), or None."""
    cur.execute(f"""
        SELECT {TEMPLATE_COLS} FROM public.screen_template
        WHERE owner_tenant_id = %s ORDER BY id DESC LIMIT 1;
    """, (tenant_id,))
    row = cur.fetchone()
    return _template_row_to_dict(row) if row else None


def _fork_company_template(conn, cur, tenant_id: int, user_id: Optional[int]) -> Dict:
    """Return the tenant-OWNED editable template, forking it from the company's
    currently-linked published template on first use. Idempotent: a tenant has at
    most one owned template. Does NOT re-link the company (that happens on publish),
    so live screens are untouched until the company publishes."""
    owned = _company_owned_template(cur, tenant_id)
    if owned:
        return owned
    src = _tenant_template(cur, tenant_id)
    if not src:
        raise HTTPException(
            status_code=409,
            detail="No template is linked to your company yet — ask your platform administrator to link one before editing.")
    # Conflict-safe against a concurrent first-write: the partial-unique index on
    # owner_tenant_id lets the loser's INSERT no-op; it then re-reads the winner's
    # fork. Guarantees exactly one owned template per tenant (no orphan / lost edits).
    # source_template_id keeps the lineage to the platform original so the
    # platform UI still reports this company as linked after publish re-points
    # company.template_id at the fork.
    cur.execute(f"""
        INSERT INTO public.screen_template
            (name, description, orientation, design_width, design_height, zones,
             status, version, owner_tenant_id, source_template_id, created_by)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, 'draft', 0, %s, %s, %s)
        ON CONFLICT (owner_tenant_id) WHERE owner_tenant_id IS NOT NULL DO NOTHING
        RETURNING {TEMPLATE_COLS};
    """, (src["name"], src["description"], src["orientation"], src["design_width"],
          src["design_height"], json.dumps(src["zones"]), tenant_id, src["id"], user_id))
    row = cur.fetchone()
    if row is None:
        existing = _company_owned_template(cur, tenant_id)
        if existing:
            return existing
        raise HTTPException(status_code=500, detail="Could not create an editable template copy")
    fork = _template_row_to_dict(row)
    log_audit(conn, tenant_id, user_id, "template.company_fork", "screen_template", fork["id"],
              details={"source_template_id": src["id"]})
    return fork


@router.get("/company/template/design")
def company_template_design(ctx: TenantContext = Depends(require_tenant_context)):
    """The template a company admin edits in the designer: their owned copy if one
    exists, otherwise the linked platform template as a read-only starting point."""
    tenant_id = _require_company_settings(ctx)
    with pg_conn() as conn:
        with conn.cursor() as cur:
            owned = _company_owned_template(cur, tenant_id)
            if owned:
                return {"template": owned, "owned": True}
            linked = _tenant_template(cur, tenant_id)
            return {"template": linked, "owned": False}


@router.put("/company/template/design")
def update_company_template_design(body: TemplateUpdateIn,
                                   ctx: TenantContext = Depends(require_tenant_context)):
    """Save the company's own template (forking on first write). Mirrors the
    platform PUT shape so the shared designer component is drop-in."""
    tenant_id = _require_company_settings(ctx)
    if body.zones is not None:
        errors = validate_zones(body.zones)
        if errors:
            raise HTTPException(status_code=422, detail={"zone_errors": errors})
    with pg_conn() as conn:
        with conn.cursor() as cur:
            tpl = _fork_company_template(conn, cur, tenant_id, ctx.user_id)
            fields, values = [], []
            for col in ("name", "description", "orientation", "design_width", "design_height"):
                v = getattr(body, col)
                if v is not None:
                    fields.append(f"{col} = %s")
                    values.append(v)
            if body.zones is not None:
                fields.append("zones = %s::jsonb")
                values.append(json.dumps(body.zones))
            if fields:
                values.extend([tpl["id"], tenant_id])
                # Double-scope: id AND owner — never touch a row we don't own.
                cur.execute(f"""
                    UPDATE public.screen_template
                    SET {", ".join(fields)}, updated_at = NOW()
                    WHERE id = %s AND owner_tenant_id = %s
                    RETURNING {TEMPLATE_COLS};
                """, values)
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Editable template not found")
                tpl = _template_row_to_dict(row)
                log_audit(conn, tenant_id, ctx.user_id, "template.company_update",
                          "screen_template", tpl["id"])
        conn.commit()
    tpl["owned"] = True
    return tpl


@router.post("/company/template/design/publish")
def publish_company_template_design(ctx: TenantContext = Depends(require_tenant_context)):
    """Publish the company's own template and (re)link the company to it, so its
    screens switch to the company-edited layout on their next heartbeat."""
    tenant_id = _require_company_settings(ctx)
    with pg_conn() as conn:
        with conn.cursor() as cur:
            tpl = _fork_company_template(conn, cur, tenant_id, ctx.user_id)
            if not tpl["zones"]:
                raise HTTPException(status_code=422, detail="Cannot publish a template with no zones")
            errors = validate_zones(tpl["zones"])
            if errors:
                raise HTTPException(status_code=422, detail={"zone_errors": errors})
            new_version = tpl["version"] + 1
            cur.execute(f"""
                UPDATE public.screen_template
                SET status = 'published', version = %s, published_at = NOW(), updated_at = NOW()
                WHERE id = %s AND owner_tenant_id = %s
                RETURNING {TEMPLATE_COLS};
            """, (new_version, tpl["id"], tenant_id))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Editable template not found")
            published = _template_row_to_dict(row)
            cur.execute("""
                INSERT INTO public.screen_template_version
                    (template_id, version, orientation, design_width, design_height, zones, published_by)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s);
            """, (tpl["id"], new_version, published["orientation"], published["design_width"],
                  published["design_height"], json.dumps(published["zones"]), ctx.user_id))
            cur.execute("""
                UPDATE public.company
                SET template_id = %s, template_linked_at = NOW(), updated_at = NOW()
                WHERE id = %s;
            """, (tpl["id"], tenant_id))
            log_audit(conn, tenant_id, ctx.user_id, "template.company_publish",
                      "screen_template", tpl["id"], details={"version": new_version})
        conn.commit()
    published["companies"] = [{"id": tenant_id, "name": ctx.company_name}]
    published["owned"] = True
    return published


def _get_scope_content(cur, tenant_id: int, scope: str, target_col: Optional[str], target_id: Optional[int]):
    if scope == "group":
        # Group content has its own table (see migration note).
        cur.execute("""
            SELECT zone_key, payload, updated_at FROM public.template_zone_group_content
            WHERE tenant_id = %s AND group_id = %s;
        """, (tenant_id, target_id))
    elif scope == "company":
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


def _scope_target_id(scope: str, tenant_id: int, shop_id: Optional[int],
                     device_id: Optional[int], group_id: Optional[int]) -> int:
    """The id used in the S3 key path for this scope's uploaded media/QR."""
    if scope == "device":
        return device_id
    if scope == "shop":
        return shop_id
    if scope == "group":
        return group_id
    return tenant_id


def _upsert_zone_content(conn, cur, tenant_id: int, zone: Dict, zone_key: str, scope: str,
                         shop_id: Optional[int], device_id: Optional[int],
                         payload: Dict, user_id: Optional[int],
                         group_id: Optional[int] = None) -> Dict:
    # A media-library item's s3_link may be stored as s3://, an S3 https URL, or a
    # bare key. Canonicalize the S3 refs the user picked so a library image/video
    # both passes validation (strict s3://) and resolves to a presigned URL on the
    # player — the "library pick doesn't show on the device" bug.
    for _f in ("media_s3", "bg_image_s3"):
        v = payload.get(_f)
        if isinstance(v, str) and v and not v.startswith("s3://"):
            payload[_f] = _canonical_s3_ref(v)
    errors = validate_content_payload(zone["type"], payload)
    if errors:
        raise HTTPException(status_code=422, detail={"payload_errors": errors})

    # QR link mode: generate the PNG once, store alongside the payload.
    if zone["type"] == "qr" and (payload.get("qr_mode") == "link" or
                                 (payload.get("qr_link") and not payload.get("qr_mode"))):
        payload["qr_mode"] = "link"
        slug = _tenant_slug(cur, tenant_id)
        target_id = _scope_target_id(scope, tenant_id, shop_id, device_id, group_id)
        try:
            png = make_qr_png(payload["qr_link"])
            payload["qr_generated_s3"] = _upload_bytes(
                _content_qr_key(slug, scope, target_id, zone_key), png, "image/png")
        except Exception as e:
            logger.error("QR generation failed (tenant %s zone %s): %s", tenant_id, zone_key, e)
            raise HTTPException(status_code=502, detail="QR code generation/upload failed")

    if scope == "group":
        # Group content: separate table keyed by group_id (see migration note).
        cur.execute("""
            INSERT INTO public.template_zone_group_content
                (tenant_id, zone_key, group_id, payload, updated_by)
            VALUES (%s, %s, %s, %s::jsonb, %s)
            ON CONFLICT (tenant_id, zone_key, group_id)
            DO UPDATE SET payload = EXCLUDED.payload, updated_by = EXCLUDED.updated_by, updated_at = NOW()
            RETURNING payload;
        """, (tenant_id, zone_key, group_id, json.dumps(payload), user_id))
    else:
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
                             "shop_id": shop_id, "device_id": device_id, "group_id": group_id})
    return saved


async def _upload_zone_media(conn, cur, tenant_id: int, zone: Dict, zone_key: str, scope: str,
                             shop_id: Optional[int], device_id: Optional[int],
                             file: UploadFile, user_id: Optional[int],
                             group_id: Optional[int] = None) -> Dict:
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
    target_id = _scope_target_id(scope, tenant_id, shop_id, device_id, group_id)
    content_type = f"{media_type}/{'jpeg' if ext == 'jpg' else ext}" if media_type == "image" else "video/mp4"
    try:
        uri = _upload_bytes(_content_media_key(slug, scope, target_id, zone_key, ext), data, content_type)
    except Exception as e:
        logger.error("media upload failed (tenant %s zone %s): %s", tenant_id, zone_key, e)
        raise HTTPException(status_code=502, detail="Media upload to storage failed")

    # Merge into the existing payload (keeps text/colors/qr_link already set).
    if scope == "group":
        cur.execute("""
            SELECT payload FROM public.template_zone_group_content
            WHERE tenant_id = %s AND zone_key = %s AND group_id = %s;
        """, (tenant_id, zone_key, group_id))
    else:
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
                                shop_id, device_id, payload, user_id, group_id=group_id)


@router.get("/company/template-content")
def company_template_content(ctx: TenantContext = Depends(require_tenant_context)):
    """Company-wide default zone content (scope='company' — the resolver's lowest-precedence layer)."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            tpl = _tenant_template(cur, ctx.active_tenant_id)
            content = _get_scope_content(cur, ctx.active_tenant_id, "company", None, None)
    zones = [z for z in (tpl["zones"] if tpl else [])
             if (z.get("binding") or {}).get("source") == "content"]
    return {"template_linked": tpl is not None, **_design_size_of(tpl),
            "content_zones": zones, "content": content}


@router.get("/company/template-content/overrides")
def company_content_overrides(ctx: TenantContext = Depends(require_tenant_context)):
    """Per-zone summary of MORE-SPECIFIC content overrides (location + screen) that
    shadow the company-wide default. Content resolves screen > location > company, so
    a box pinned on a location/screen makes a company edit look like it 'didn't update'
    — this lets the dashboard surface and clear those overrides."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.zone_key, 'shop' AS scope, s.id, s.shop_name
                FROM public.template_zone_content c
                JOIN public.shop s ON s.id = c.shop_id
                WHERE c.tenant_id = %s AND c.scope = 'shop'
                UNION ALL
                SELECT c.zone_key, 'device', d.id, d.device_name
                FROM public.template_zone_content c
                JOIN public.device d ON d.id = c.device_id
                WHERE c.tenant_id = %s AND c.scope = 'device'
                UNION ALL
                SELECT gc.zone_key, 'group', g.id, g.gname
                FROM public.template_zone_group_content gc
                JOIN public."group" g ON g.id = gc.group_id
                WHERE gc.tenant_id = %s;
            """, (ctx.active_tenant_id, ctx.active_tenant_id, ctx.active_tenant_id))
            out = {}
            bucket = {"shop": "shops", "device": "devices", "group": "groups"}
            for zone_key, scope, tid, name in cur.fetchall():
                e = out.setdefault(zone_key, {"shops": [], "devices": [], "groups": []})
                e[bucket[scope]].append({"id": tid, "name": name})
    return {"overrides": out}


@router.delete("/company/template-content/{zone_key}/overrides")
def clear_company_zone_overrides(zone_key: str,
                                 ctx: TenantContext = Depends(require_tenant_context)):
    """Remove ALL location + screen overrides for one zone so the company-wide
    default takes effect everywhere. Bumps the content stamp → screens refetch."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM public.template_zone_content
                WHERE tenant_id = %s AND zone_key = %s AND scope IN ('shop', 'device')
                RETURNING id;
            """, (ctx.active_tenant_id, zone_key))
            cleared = len(cur.fetchall())
            cur.execute("""
                DELETE FROM public.template_zone_group_content
                WHERE tenant_id = %s AND zone_key = %s
                RETURNING id;
            """, (ctx.active_tenant_id, zone_key))
            cleared += len(cur.fetchall())
            if cleared:
                log_audit(conn, ctx.active_tenant_id, ctx.user_id, "template.content.clear_overrides",
                          "template_zone_content", None, details={"zone_key": zone_key, "cleared": cleared})
        conn.commit()
    return {"zone_key": zone_key, "cleared": cleared}


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
            **_design_size_of(tpl), "content_zones": zones, "content": content}


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


@router.get("/group/{group_id}/template-content")
def group_template_content(group_id: int, ctx: TenantContext = Depends(require_tenant_context)):
    """Group-wide zone content — applies to every device in the group regardless
    of location. Resolves screen > group > location > company."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            _group_of_tenant(cur, group_id, ctx.active_tenant_id)
            tpl = _tenant_template(cur, ctx.active_tenant_id)
            content = _get_scope_content(cur, ctx.active_tenant_id, "group", None, group_id)
    zones = [z for z in (tpl["zones"] if tpl else [])
             if (z.get("binding") or {}).get("source") == "content"]
    return {"group_id": group_id, "template_linked": tpl is not None,
            **_design_size_of(tpl), "content_zones": zones, "content": content}


@router.put("/group/{group_id}/template-content/{zone_key}")
def put_group_zone_content(group_id: int, zone_key: str, body: ZoneContentIn,
                           ctx: TenantContext = Depends(require_tenant_context)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            _group_of_tenant(cur, group_id, ctx.active_tenant_id)
            tpl = _tenant_template(cur, ctx.active_tenant_id)
            if not tpl:
                raise HTTPException(status_code=422, detail="Company has no linked template")
            zone = _content_zone_or_422(tpl, zone_key)
            saved = _upsert_zone_content(conn, cur, ctx.active_tenant_id, zone, zone_key,
                                         "group", None, None, body.payload, ctx.user_id,
                                         group_id=group_id)
        conn.commit()
    return {"group_id": group_id, "zone_key": zone_key, "payload": saved}


@router.delete("/group/{group_id}/template-content/{zone_key}")
def delete_group_zone_content(group_id: int, zone_key: str,
                              ctx: TenantContext = Depends(require_tenant_context)):
    """Remove a group's content for one zone — its devices fall back to location/company."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            _group_of_tenant(cur, group_id, ctx.active_tenant_id)
            cur.execute("""
                DELETE FROM public.template_zone_group_content
                WHERE tenant_id = %s AND zone_key = %s AND group_id = %s
                RETURNING id;
            """, (ctx.active_tenant_id, zone_key, group_id))
            deleted = cur.fetchone() is not None
            if deleted:
                log_audit(conn, ctx.active_tenant_id, ctx.user_id, "template.content.delete",
                          "template_zone_group_content", None,
                          details={"zone_key": zone_key, "group_id": group_id})
        conn.commit()
    return {"group_id": group_id, "zone_key": zone_key, "deleted": deleted}


@router.post("/group/{group_id}/template-content/{zone_key}/media")
async def upload_group_zone_media(group_id: int, zone_key: str, file: UploadFile = File(...),
                                  ctx: TenantContext = Depends(require_tenant_context)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            _group_of_tenant(cur, group_id, ctx.active_tenant_id)
            tpl = _tenant_template(cur, ctx.active_tenant_id)
            if not tpl:
                raise HTTPException(status_code=422, detail="Company has no linked template")
            zone = _content_zone_or_422(tpl, zone_key)
            saved = await _upload_zone_media(conn, cur, ctx.active_tenant_id, zone, zone_key,
                                             "group", None, None, file, ctx.user_id,
                                             group_id=group_id)
        conn.commit()
    return {"group_id": group_id, "zone_key": zone_key, "payload": saved}


@router.get("/device-config/{device_id}/template-content")
def device_template_content(device_id: int, ctx: TenantContext = Depends(require_tenant_context)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            dev = _device_of_tenant(cur, device_id, ctx.active_tenant_id)
            tpl = _tenant_template(cur, ctx.active_tenant_id)
            content = _get_scope_content(cur, ctx.active_tenant_id, "device", "device_id", device_id)
    zones = [z for z in (tpl["zones"] if tpl else [])
             if (z.get("binding") or {}).get("source") == "content"]
    # reported_resolution ("1920x1080", from the device heartbeat) lets the
    # dashboard show zone pixel sizes for THIS screen, not just the design canvas.
    return {"device_id": device_id, "template_linked": tpl is not None,
            **_design_size_of(tpl), "reported_resolution": dev[2],
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

def _collapse_content(cur, tenant_id: int, shop_id: Optional[int], device_id: int,
                      group_id: Optional[int] = None) -> Dict[str, Dict]:
    """zone_key -> payload with screen > group > location > company precedence.

    Group content lives in its own table (template_zone_group_content) and is
    resolved for the device's single group (device_assignment.gid), overriding
    the location default but yielding to a per-screen override.
    """
    rank = {"company": 0, "shop": 1, "group": 2, "device": 3}
    best: Dict[str, tuple] = {}

    def consider(zone_key, scope, payload):
        if isinstance(payload, str):
            payload = json.loads(payload)
        if zone_key not in best or rank[scope] > best[zone_key][0]:
            best[zone_key] = (rank[scope], payload)

    cur.execute("""
        SELECT zone_key, scope, payload FROM public.template_zone_content
        WHERE tenant_id = %s
          AND (scope = 'company'
               OR (scope = 'shop' AND shop_id = COALESCE(%s::bigint, -1))
               OR (scope = 'device' AND device_id = %s));
    """, (tenant_id, shop_id, device_id))
    for zone_key, scope, payload in cur.fetchall():
        consider(zone_key, scope, payload)

    if group_id is not None:
        cur.execute("""
            SELECT zone_key, payload FROM public.template_zone_group_content
            WHERE tenant_id = %s AND group_id = %s;
        """, (tenant_id, group_id))
        for zone_key, payload in cur.fetchall():
            consider(zone_key, "group", payload)

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
                SELECT da.sid, s.shop_name, c.name, da.gid
                FROM public.device d
                LEFT JOIN public.device_assignment da ON da.did = d.id
                LEFT JOIN public.shop s ON s.id = da.sid
                LEFT JOIN public.company c ON c.id = d.tenant_id
                WHERE d.id = %s;
            """, (did,))
            arow = cur.fetchone() or (None, None, None, None)
            shop_id, shop_name, company_name, group_id = arow
            if shop_id is None or group_id is None:
                # Fallback: shop/group via the content-link table (device may predate device_assignment).
                cur.execute("""
                    SELECT l.sid, s.shop_name, l.gid FROM public.device_video_shop_group l
                    LEFT JOIN public.shop s ON s.id = l.sid
                    WHERE l.did = %s ORDER BY l.id DESC LIMIT 1;
                """, (did,))
                lrow = cur.fetchone()
                if lrow:
                    if shop_id is None:
                        shop_id, shop_name = lrow[0], lrow[1]
                    if group_id is None:
                        group_id = lrow[2]

            entity = {"company.name": company_name, "shop.name": shop_name, "device.name": device_name}
            content = _collapse_content(cur, tenant_id, shop_id, did, group_id)
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
