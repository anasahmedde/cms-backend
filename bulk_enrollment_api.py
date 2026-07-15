# bulk_enrollment_api.py
"""
Bulk device enrollment via CSV/XLSX
===================================
Super-admin (impersonating) or company-admin downloads a template file, fills
rows, and uploads. The flow is validate -> commit:

  GET  /bulk-devices/template.csv            download the CSV template
  GET  /bulk-devices/template.xlsx           download the XLSX template
  POST /bulk-devices/validate                (multipart) parse + validate; writes
                                             NOTHING; returns a job_id + preview
  POST /bulk-devices/commit                  {job_id}: create shops/groups/devices
  GET  /bulk-devices/jobs/{job_id}           job status + report
  GET  /bulk-devices/pending                 list pending (unclaimed) devices
  POST /bulk-devices/claim                   {device_id, mobile_id}: bind a real
                                             ANDROID_ID to a pending row

Rows WITH a device_id (the ANDROID_ID) auto-enroll the moment that physical
device polls GET /device/{id}/online (which returns 200 once the row exists).
Rows WITHOUT one become pending; an installer reads the ANDROID_ID shown on the
device's unenrolled screen and claims the row from the dashboard.

Tenant-safety (per the audit): tenant_id always comes from the session, never the
file; a device_id already used under ANOTHER tenant is rejected (global mobile_id
lookups ignore tenant); max_devices is enforced here (it is enforced nowhere else).
Content links (group_video copy) are intentionally NOT created here to avoid the
known tenant_id=1 default leak on device_video_shop_group — content flows through
the normal group pipeline / template playlist.
"""
import csv
import io
import json
import logging
import re
import secrets
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from database import pg_conn
from tenant_context import TenantContext, log_audit, require_tenant_context
import template_api as tpl_api

logger = logging.getLogger("bulk_enrollment_api")
router = APIRouter()

BASE_COLUMNS = ["device_name", "shop_name", "group_name", "device_id", "resolution", "notes"]
COLUMNS = BASE_COLUMNS  # back-compat alias
REQUIRED = ["device_name", "shop_name"]
RESOLUTION_RE = re.compile(r"^\d{2,5}x\d{2,5}$")
MOBILE_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")
PENDING_PREFIX = "pending:"
MAX_ROWS = 20000
COMMIT_BATCH = 500

BASE_EXAMPLE = [
    ["Main Entrance Screen", "Shop Karachi 12", "North Region", "a1b2c3d4e5f6a7b8", "1080x1920", "landscape TV at door"],
    ["Checkout Screen", "Shop Karachi 12", "North Region", "", "", "device id unknown - will be claimed on site"],
]
BASE_INSTRUCTIONS = [
    "DIGIX bulk device import — fill one row per screen, then upload this file.",
    "device_name (required): a friendly name for the screen.",
    "shop_name (required): the shop this screen belongs to; created automatically if new.",
    "group_name (optional): a group for the screen; created automatically if new.",
    "device_id (optional): the device's ANDROID_ID. If you know it, the screen auto-enrolls when it powers on. Leave blank to create a 'pending' screen you claim on site.",
    "resolution (optional): e.g. 1080x1920. Auto-detected from the device if blank.",
    "notes (optional): free text, not shown on screens.",
]
# Per-zone example + guidance shown for the content columns (device-level content).
_ZONE_HELP = {
    "media": ("https://example.com/promo.jpg",
              "an image/video URL, an s3:// path, or the NAME of a video/advertisement already uploaded"),
    "qr": ("https://menu.example.com/shop12",
           "a LINK to turn into a QR code, OR an image URL / s3:// path / library name to show as-is"),
    "text": ("50% OFF this week", "the text to show on this screen"),
    "bg": ("#111827  (or  #0a1628 -> #f59e0b @135  for a gradient, or an image URL)",
           "a #hex color, a gradient like '#0a1628 -> #f59e0b @135', or an image URL/name"),
}


def content_columns_for(zones) -> list:
    """[(header, zone_key, field, zone_type)] for each content-bound zone."""
    cols = []
    for z in zones or []:
        if (z.get("binding") or {}).get("source") != "content":
            continue
        key, zt = z.get("key"), z.get("type")
        if zt in ("text", "ticker"):
            cols.append((f"content.{key}.text", key, "text", zt))
            cols.append((f"content.{key}.bg", key, "bg", zt))
        elif zt == "clock":
            cols.append((f"content.{key}.bg", key, "bg", zt))
        elif zt == "media":
            cols.append((f"content.{key}.media", key, "media", zt))
        elif zt == "qr":
            cols.append((f"content.{key}.qr", key, "qr", zt))
    return cols


def _tenant_content_zones(cur, tenant_id: int):
    tpl = tpl_api._tenant_template(cur, tenant_id)
    if not tpl:
        return [], None
    zones = [z for z in tpl["zones"] if (z.get("binding") or {}).get("source") == "content"]
    return zones, tpl


def _template_bundle(tenant_id: int):
    """Columns, example rows, and instructions for the tenant's linked template."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            content_cols, tpl = _tenant_content_zones(cur, tenant_id)
    ccols = content_columns_for(content_cols)
    headers = BASE_COLUMNS + [h for (h, _, _, _) in ccols]
    example = [list(r) + ["" for _ in ccols] for r in BASE_EXAMPLE]
    if ccols and example:
        for i, (_h, _k, field, _zt) in enumerate(ccols):
            example[0][len(BASE_COLUMNS) + i] = _ZONE_HELP.get(field, ("", ""))[0]
    instructions = list(BASE_INSTRUCTIONS)
    if ccols:
        instructions.append("")
        instructions.append(f"Screen content (from the linked template '{tpl['name']}') — filled PER SCREEN; blank = inherit the shop's content:")
        for (h, _k, field, _zt) in ccols:
            instructions.append(f"{h}: {_ZONE_HELP.get(field, ('', 'content'))[1]}")
    else:
        instructions.append("")
        instructions.append("(No screen template is linked to this company yet, so there are no content columns.)")
    return headers, example, instructions, ccols


def _csv_bytes(headers, example, instructions) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    for line in instructions:
        w.writerow(["# " + line] if line else [])
    w.writerow(headers)
    for row in example:
        w.writerow(row)
    return buf.getvalue().encode("utf-8")


# ─────────────────────────── models ───────────────────────────

class CommitIn(BaseModel):
    job_id: int
    auto_create: bool = True  # create missing shops/groups automatically


class ClaimIn(BaseModel):
    device_id: int                      # the pending device row's id
    mobile_id: str = Field(min_length=1, max_length=64)  # the real ANDROID_ID


# ─────────────────────────── template download ───────────────────────────

@router.get("/bulk-devices/template.csv")
def template_csv(ctx: TenantContext = Depends(require_tenant_context)):
    headers, example, instructions, _ = _template_bundle(ctx.active_tenant_id)
    return StreamingResponse(
        io.BytesIO(_csv_bytes(headers, example, instructions)), media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="digix-devices-template.csv"'},
    )


@router.get("/bulk-devices/template.xlsx")
def template_xlsx(ctx: TenantContext = Depends(require_tenant_context)):
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill
    except ImportError:
        raise HTTPException(status_code=501, detail="XLSX export unavailable; use the CSV template")
    headers, example, instructions, ccols = _template_bundle(ctx.active_tenant_id)
    wb = Workbook()
    ws = wb.active
    ws.title = "Devices"
    header_fill = PatternFill("solid", fgColor="0A1628")
    content_fill = PatternFill("solid", fgColor="1E3A5F")
    header_font = Font(color="FFFFFF", bold=True)
    ws.append(headers)
    for i, cell in enumerate(ws[1]):
        cell.fill = content_fill if i >= len(BASE_COLUMNS) else header_fill
        cell.font = header_font
    for row in example:
        ws.append(row)
    for i in range(len(headers)):
        ws.column_dimensions[_col_letter(i + 1)].width = 30 if i >= len(BASE_COLUMNS) else [26, 22, 18, 22, 12, 30][i]
    info = wb.create_sheet("Instructions")
    for line in instructions:
        info.append([line])
    info.column_dimensions["A"].width = 120
    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return StreamingResponse(
        out, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="digix-devices-template.xlsx"'},
    )


def _col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# ─────────────────────────── parsing ───────────────────────────

def _parse_upload(filename: str, data: bytes) -> List[Dict[str, str]]:
    """Return a list of {column: value} dicts. Skips comment/blank/header rows."""
    name = (filename or "").lower()
    if name.endswith(".xlsx"):
        return _parse_xlsx(data)
    return _parse_csv(data)


def _norm_header(cells: List[str]) -> Optional[List[str]]:
    lowered = [str(c or "").strip().lower() for c in cells]
    if "device_name" in lowered and "shop_name" in lowered:
        return lowered
    return None


def _rows_from(header: List[str], data_rows: List[List[str]]) -> List[Dict[str, str]]:
    base_idx = {col: header.index(col) for col in BASE_COLUMNS if col in header}
    # Any header of the form content.<zone>.<field> is a template content column.
    content_idx = {h: i for i, h in enumerate(header) if h.startswith("content.")}
    out = []
    for cells in data_rows:
        if not any(str(c or "").strip() for c in cells):
            continue

        def cell(i):
            return str(cells[i]).strip() if (i is not None and i < len(cells) and cells[i] is not None) else ""

        rec = {col: cell(base_idx.get(col)) for col in BASE_COLUMNS}
        rec["_content"] = {h: cell(i) for h, i in content_idx.items() if cell(i)}
        out.append(rec)
    return out


def _parse_csv(data: bytes) -> List[Dict[str, str]]:
    text = data.decode("utf-8-sig", errors="replace")
    reader = list(csv.reader(io.StringIO(text)))
    header = None
    body: List[List[str]] = []
    for cells in reader:
        if not cells:
            continue
        if cells[0].strip().startswith("#"):
            continue
        if header is None:
            h = _norm_header(cells)
            if h:
                header = h
                continue
            # first non-comment row isn't a header -> assume canonical order
            header = COLUMNS[:]
            body.append(cells)
            continue
        body.append(cells)
    if header is None:
        raise HTTPException(status_code=422, detail="No data rows found in the file")
    return _rows_from(header, body)


def _parse_xlsx(data: bytes) -> List[Dict[str, str]]:
    try:
        from openpyxl import load_workbook
    except ImportError:
        raise HTTPException(status_code=501, detail="XLSX upload unavailable; save as CSV and re-upload")
    wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    ws = wb.active
    rows = [[("" if c is None else c) for c in r] for r in ws.iter_rows(values_only=True)]
    header = None
    body = []
    for cells in rows:
        strs = [str(c).strip() for c in cells]
        if strs and strs[0].startswith("#"):
            continue
        if header is None:
            h = _norm_header(strs)
            if h:
                header = h
                continue
            continue
        body.append(strs)
    if header is None:
        raise HTTPException(status_code=422, detail="No header row (device_name, shop_name, …) found in the sheet")
    return _rows_from(header, body)


# ─────────────────────────── validation ───────────────────────────

def _parse_row_content(cur, tenant_id, content_cols, raw_content):
    """Parse a row's content cells → ({zone_key: payload}, [errors]). content_cols
    is [(header, zone_key, field, zone_type)]. Needs a DB cursor for name lookups."""
    payloads: Dict[str, Dict[str, Any]] = {}
    ztypes: Dict[str, str] = {}
    errors: List[str] = []
    for (header, key, field, zt) in content_cols:
        val = (raw_content or {}).get(header, "").strip()
        if not val:
            continue
        ztypes[key] = zt
        pl = payloads.setdefault(key, {})
        try:
            if field == "text":
                pl["text"] = val
            elif field == "bg":
                pl.update(tpl_api.parse_bg_value(cur, tenant_id, val))
            elif field == "media":
                pl.update(tpl_api.resolve_media_value(cur, tenant_id, val))
            elif field == "qr":
                pl.update(tpl_api.parse_qr_value(cur, tenant_id, val))
        except ValueError as e:
            errors.append(f"{header}: {e}")
    for key, pl in payloads.items():
        for msg in tpl_api.validate_content_payload(ztypes[key], pl):
            errors.append(f"content.{key}: {msg}")
    return payloads, errors


def _content_summary(payload) -> str:
    """Short human-readable value for a content payload (for the change preview)."""
    if not payload:
        return ""
    if payload.get("text"):
        return (payload["text"][:40])
    if payload.get("qr_link"):
        return payload["qr_link"]
    if payload.get("media_url"):
        return payload["media_url"].split("?")[0].split("/")[-1]
    if payload.get("media_s3"):
        return payload["media_s3"].split("/")[-1]
    return "set"


def _current_states(cur, tenant_id: int, mobile_ids) -> Dict[str, Dict[str, Any]]:
    """{mobile_id: {id, name, shop, group, content:{zone_key: payload}}} for existing
    devices in this tenant — the baseline a re-upload is diffed against."""
    out: Dict[str, Dict[str, Any]] = {}
    ids = [m for m in (mobile_ids or set()) if m]
    if not ids:
        return out
    cur.execute("""
        SELECT d.id, d.mobile_id, d.device_name, s.shop_name, g.gname
        FROM public.device d
        LEFT JOIN public.device_assignment da ON da.did = d.id
        LEFT JOIN public.shop s ON s.id = da.sid
        LEFT JOIN public."group" g ON g.id = da.gid
        WHERE d.tenant_id = %s AND d.mobile_id = ANY(%s);
    """, (tenant_id, ids))
    did_to_mid: Dict[int, str] = {}
    for did, mid, name, shop, grp in cur.fetchall():
        out[mid] = {"id": did, "name": name or "", "shop": shop or "", "group": grp or "", "content": {}}
        did_to_mid[did] = mid
    if did_to_mid:
        cur.execute("""
            SELECT device_id, zone_key, payload FROM public.template_zone_content
            WHERE tenant_id = %s AND scope = 'device' AND device_id = ANY(%s);
        """, (tenant_id, list(did_to_mid.keys())))
        for did, zk, payload in cur.fetchall():
            if isinstance(payload, str):
                payload = json.loads(payload)
            mid = did_to_mid.get(did)
            if mid:
                out[mid]["content"][zk] = payload
    return out


def _row_changes(device_name: str, shop_name: str, group_name: str,
                 row_content: Dict, current: Dict) -> List[Dict[str, str]]:
    """What a re-upload row changes on an EXISTING device: [{field, from, to}].
    A blank cell means 'leave as-is' — only a non-blank value that differs counts."""
    changes: List[Dict[str, str]] = []

    def diff(field, new_val, cur_val):
        if new_val and new_val != (cur_val or ""):
            changes.append({"field": field, "from": cur_val or "", "to": new_val})

    diff("name", device_name, current.get("name"))
    diff("location", shop_name, current.get("shop"))
    diff("group", group_name, current.get("group"))
    cur_content = current.get("content") or {}
    for zk, payload in (row_content or {}).items():
        cur_p = cur_content.get(zk) or {}
        # Subset comparison: only the fields the sheet sets. The stored payload may
        # carry extra server-added keys (e.g. qr_generated_s3) that the sheet never
        # provides — a full-dict compare would flag every QR/media as changed.
        if payload and any(cur_p.get(k) != v for k, v in payload.items()):
            changes.append({"field": f"content.{zk}",
                            "from": _content_summary(cur_p),
                            "to": _content_summary(payload)})
    return changes


def validate_rows(rows: List[Dict[str, str]], existing_device_count: int, max_devices: int,
                  mobile_ids_this_tenant: set, mobile_ids_other_tenant: set,
                  content_cols=None, cur=None, tenant_id=None) -> Dict[str, Any]:
    """Pure validation. Returns preview {rows, errors, summary}. Writes nothing."""
    errors: List[Dict[str, Any]] = []
    seen_ids: Dict[str, int] = {}
    shops, groups = set(), set()
    valid_new = 0        # devices that will be created
    with_id = 0
    pending = 0
    skipped = 0          # already exist in this tenant (idempotent re-run)
    updated = 0          # existing devices with field/content changes
    unchanged = 0        # existing devices the sheet leaves as-is
    # Baseline for the change preview (existing devices only).
    current_states = (_current_states(cur, tenant_id, mobile_ids_this_tenant)
                      if (cur is not None and tenant_id is not None) else {})

    normalized: List[Dict[str, Any]] = []
    for i, rec in enumerate(rows):
        rownum = i + 1
        row_errors = []
        for col in REQUIRED:
            if not rec.get(col):
                row_errors.append(f"{col} is required")
        dev_id = rec.get("device_id", "").strip()
        if dev_id:
            if not MOBILE_ID_RE.match(dev_id):
                row_errors.append("device_id has invalid characters")
            elif dev_id in seen_ids:
                row_errors.append(f"duplicate device_id (also on row {seen_ids[dev_id]})")
            elif dev_id in mobile_ids_other_tenant:
                row_errors.append("device_id already belongs to another company")
            if dev_id and dev_id.startswith(PENDING_PREFIX):
                row_errors.append("device_id must be a real device id, not a placeholder")
        res = rec.get("resolution", "").strip()
        if res and not RESOLUTION_RE.match(res):
            row_errors.append("resolution must look like 1080x1920")

        # Template content cells (only when the company has a linked template).
        row_content = {}
        if content_cols and cur is not None:
            row_content, content_errs = _parse_row_content(cur, tenant_id, content_cols, rec.get("_content"))
            row_errors.extend(content_errs)

        will = "error"
        row_changes: List[Dict[str, str]] = []
        if not row_errors:
            if dev_id:
                seen_ids[dev_id] = rownum
                if dev_id in mobile_ids_this_tenant:
                    # Existing screen — diff the row against its current state so the
                    # operator sees exactly what a re-upload will change (and only that).
                    row_changes = _row_changes(
                        rec.get("device_name", "").strip(), rec.get("shop_name", "").strip(),
                        rec.get("group_name", "").strip(), row_content, current_states.get(dev_id, {}))
                    if row_changes:
                        will = "update"
                        updated += 1
                    else:
                        will = "unchanged"
                        unchanged += 1
                else:
                    will = "create"
                    valid_new += 1
                    with_id += 1
            else:
                will = "pending"
                valid_new += 1
                pending += 1
            if rec.get("shop_name"):
                shops.add(rec["shop_name"])
            if rec.get("group_name"):
                groups.add(rec["group_name"])
        else:
            for e in row_errors:
                errors.append({"row": rownum, "reason": e})

        normalized.append({
            "row": rownum,
            "device_name": rec.get("device_name", ""),
            "shop_name": rec.get("shop_name", ""),
            "group_name": rec.get("group_name", ""),
            "device_id": dev_id,
            "resolution": res,
            "action": will,
            "content": row_content,
            "changes": row_changes,
        })

    after = existing_device_count + valid_new
    quota_ok = max_devices <= 0 or after <= max_devices
    if not quota_ok:
        errors.append({"row": 0, "reason": f"Quota exceeded: {existing_device_count} existing + {valid_new} new = {after} > limit {max_devices}"})

    return {
        "rows": normalized,
        "errors": errors,
        "summary": {
            "total_rows": len(rows),
            "will_create": with_id,
            "will_pending": pending,
            "will_skip": skipped,
            "will_update": updated,
            "will_unchanged": unchanged,
            "error_rows": len({e["row"] for e in errors if e["row"] > 0}),
            "new_shops": sorted(shops),
            "new_groups": sorted(groups),
            "content_columns": [h for (h, _, _, _) in (content_cols or [])],
            "quota": {"existing": existing_device_count, "new": valid_new, "after": after,
                      "max": max_devices, "ok": quota_ok},
            "valid": len(errors) == 0,
        },
    }


# ─────────────────────────── endpoints ───────────────────────────

@router.post("/bulk-devices/validate")
async def bulk_validate(file: UploadFile = File(...), ctx: TenantContext = Depends(require_tenant_context)):
    tenant_id = ctx.active_tenant_id
    data = await file.read()
    if not data:
        raise HTTPException(status_code=422, detail="Empty file")
    rows = _parse_upload(file.filename, data)
    if not rows:
        raise HTTPException(status_code=422, detail="No device rows found")
    if len(rows) > MAX_ROWS:
        raise HTTPException(status_code=422, detail=f"Too many rows ({len(rows)}); max {MAX_ROWS} per import")

    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.device WHERE tenant_id = %s;", (tenant_id,))
            existing = cur.fetchone()[0]
            cur.execute("SELECT COALESCE(max_devices, 0) FROM public.company WHERE id = %s;", (tenant_id,))
            row = cur.fetchone()
            max_devices = row[0] if row else 0
            # Which device_ids in the file already exist, and in which tenant?
            ids = [r["device_id"].strip() for r in rows if r.get("device_id", "").strip()]
            mine, other = set(), set()
            if ids:
                cur.execute("SELECT mobile_id, tenant_id FROM public.device WHERE mobile_id = ANY(%s);", (ids,))
                for mid, tid in cur.fetchall():
                    (mine if tid == tenant_id else other).add(mid)

            # Content columns come from the company's linked template (if any).
            content_zones, _tpl = _tenant_content_zones(cur, tenant_id)
            content_cols = content_columns_for(content_zones)

            preview = validate_rows(rows, existing, max_devices, mine, other,
                                    content_cols=content_cols, cur=cur, tenant_id=tenant_id)

            cur.execute("""
                INSERT INTO public.bulk_import_job
                    (tenant_id, uploaded_by, filename, status, total_rows, error_rows, payload, report)
                VALUES (%s, %s, %s, 'validated', %s, %s, %s::jsonb, %s::jsonb)
                RETURNING id;
            """, (tenant_id, ctx.user_id, file.filename, preview["summary"]["total_rows"],
                  preview["summary"]["error_rows"],
                  _json(preview["rows"]), _json(preview["summary"])))
            job_id = cur.fetchone()[0]
        conn.commit()
    return {"job_id": job_id, **preview}


@router.post("/bulk-devices/commit")
def bulk_commit(body: CommitIn, ctx: TenantContext = Depends(require_tenant_context)):
    tenant_id = ctx.active_tenant_id
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT status, payload, report FROM public.bulk_import_job
                    WHERE id = %s AND tenant_id = %s;
                """, (body.job_id, tenant_id))
                job = cur.fetchone()
                if not job:
                    raise HTTPException(status_code=404, detail="Import job not found")
                if job[0] == "committed":
                    raise HTTPException(status_code=409, detail="This import was already committed")
                rows = job[1]
                summary = job[2] if isinstance(job[2], dict) else {}
                if not summary.get("valid", False):
                    raise HTTPException(status_code=422, detail="This import has validation errors — fix and re-upload")

                # Re-check quota at commit time (another import may have run since).
                cur.execute("SELECT COUNT(*) FROM public.device WHERE tenant_id = %s;", (tenant_id,))
                existing = cur.fetchone()[0]
                cur.execute("SELECT COALESCE(max_devices, 0) FROM public.company WHERE id = %s;", (tenant_id,))
                max_devices = (cur.fetchone() or [0])[0]
                will_add = sum(1 for r in rows if r["action"] in ("create", "pending"))
                if max_devices > 0 and existing + will_add > max_devices:
                    raise HTTPException(status_code=409, detail=f"Quota exceeded at commit: {existing}+{will_add} > {max_devices}")

                # 1) upsert shops + groups, build name->id maps
                shop_ids = _upsert_named(cur, "shop", "shop_name",
                                         {r["shop_name"] for r in rows if r["shop_name"]}, tenant_id)
                group_ids = _upsert_named(cur, '"group"', "gname",
                                          {r["group_name"] for r in rows if r["group_name"]}, tenant_id)

                # Content zones of the linked template, for writing per-device content.
                content_zones, _tpl = _tenant_content_zones(cur, tenant_id)
                zone_map = {z["key"]: z for z in content_zones}

                created = skipped = pending = updated = 0
                content_written = 0

                def write_row_content(did, row, only_zones=None):
                    """Per-device template content (device-scope override). only_zones
                    limits the write to the zones the change-preview flagged, so a
                    re-upload doesn't rewrite (and re-stamp) unchanged content."""
                    nonlocal content_written
                    for zone_key, payload in (row.get("content") or {}).items():
                        if only_zones is not None and zone_key not in only_zones:
                            continue
                        zone = zone_map.get(zone_key)
                        if not zone or not payload:
                            continue
                        # MERGE into the existing device-scope payload — the write is a
                        # full replace, so writing only the sheet's sub-fields would wipe
                        # a blank-left sibling column (e.g. .bg) or a UI-only field
                        # (text_color / fit_mode / clock format). Mirrors _upload_zone_media.
                        cur.execute("""
                            SELECT payload FROM public.template_zone_content
                            WHERE tenant_id = %s AND zone_key = %s AND scope = 'device' AND device_id = %s;
                        """, (tenant_id, zone_key, did))
                        prev = cur.fetchone()
                        merged = {}
                        if prev and prev[0]:
                            merged = dict(prev[0] if isinstance(prev[0], dict) else json.loads(prev[0]))
                        merged.update(dict(payload))
                        tpl_api._upsert_zone_content(conn, cur, tenant_id, zone, zone_key,
                                                     "device", None, did, merged, ctx.user_id)
                        content_written += 1

                # 2) create devices row-by-row (safe + idempotent); assignment per device
                for r in rows:
                    if r["action"] == "error":
                        continue
                    if r["action"] in ("update", "unchanged", "skip"):
                        # Existing screen in this tenant. Apply ONLY the fields the
                        # change-preview flagged (name / location / group / content) —
                        # a blank cell leaves that value as-is, and unchanged rows are
                        # a no-op. (This replaces the old content-only "skip".)
                        mid = r.get("device_id")
                        chg = {c["field"] for c in (r.get("changes") or [])}
                        if not mid or not chg:
                            skipped += 1
                            continue
                        cur.execute("SELECT id FROM public.device WHERE tenant_id = %s AND mobile_id = %s;",
                                    (tenant_id, mid))
                        hit = cur.fetchone()
                        if not hit:
                            skipped += 1
                            continue
                        did = hit[0]
                        if "name" in chg and r.get("device_name"):
                            cur.execute("UPDATE public.device SET device_name = %s WHERE id = %s;",
                                        (r["device_name"], did))
                        loc_c, grp_c = ("location" in chg), ("group" in chg)
                        if loc_c or grp_c:
                            gid = group_ids.get(r["group_name"]) if r.get("group_name") else None
                            sid = shop_ids.get(r["shop_name"]) if r.get("shop_name") else None
                            cur.execute("""
                                INSERT INTO public.device_assignment (did, gid, sid, tenant_id)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (did) DO UPDATE SET
                                    gid = CASE WHEN %s THEN EXCLUDED.gid ELSE public.device_assignment.gid END,
                                    sid = CASE WHEN %s THEN EXCLUDED.sid ELSE public.device_assignment.sid END,
                                    updated_at = NOW();
                            """, (did, gid, sid, tenant_id, grp_c, loc_c))
                        changed_zones = {c["field"].split(".", 1)[1] for c in (r.get("changes") or [])
                                         if c["field"].startswith("content.")}
                        if changed_zones:
                            write_row_content(did, r, only_zones=changed_zones)
                        updated += 1
                        continue
                    if r["action"] == "pending":
                        code = _pending_code()
                        mobile_id = PENDING_PREFIX + code
                        activation = code
                    else:  # create
                        mobile_id = r["device_id"]
                        activation = None
                        # Guard: it may exist by now (idempotent re-run) — still
                        # refresh its content from the sheet.
                        cur.execute("SELECT id FROM public.device WHERE tenant_id = %s AND mobile_id = %s;",
                                    (tenant_id, mobile_id))
                        existing_row = cur.fetchone()
                        if existing_row:
                            skipped += 1
                            if r.get("content"):
                                write_row_content(existing_row[0], r)
                                updated += 1
                            continue
                    cur.execute("""
                        INSERT INTO public.device
                            (mobile_id, download_status, is_online, is_active, last_online_at,
                             resolution, device_name, tenant_id, activation_code)
                        VALUES (%s, FALSE, FALSE, TRUE, NOW(), %s, %s, %s, %s)
                        RETURNING id;
                    """, (mobile_id, r["resolution"] or None, r["device_name"] or None, tenant_id, activation))
                    did = cur.fetchone()[0]
                    gid = group_ids.get(r["group_name"]) if r["group_name"] else None
                    sid = shop_ids.get(r["shop_name"]) if r["shop_name"] else None
                    if sid or gid:
                        cur.execute("""
                            INSERT INTO public.device_assignment (did, gid, sid, tenant_id)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (did) DO UPDATE SET gid = EXCLUDED.gid, sid = EXCLUDED.sid, updated_at = NOW();
                        """, (did, gid, sid, tenant_id))
                    write_row_content(did, r)

                    if r["action"] == "pending":
                        pending += 1
                    else:
                        created += 1

                report = {"created": created, "pending": pending, "skipped": skipped,
                          "content_updated_existing": updated,
                          "content_written": content_written,
                          "shops": len(shop_ids), "groups": len(group_ids)}
                cur.execute("""
                    UPDATE public.bulk_import_job
                    SET status = 'committed', created_rows = %s, skipped_rows = %s,
                        report = %s::jsonb, finished_at = NOW()
                    WHERE id = %s;
                """, (created + pending, skipped, _json(report), body.job_id))
                log_audit(conn, tenant_id, ctx.user_id, "devices.bulk_import", "device", None,
                          details=report)
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            logger.error("bulk commit failed (tenant %s job %s): %s", tenant_id, body.job_id, e)
            raise HTTPException(status_code=500, detail="Bulk import failed; nothing was created")
    return {"job_id": body.job_id, "committed": True, **report}


@router.get("/bulk-devices/jobs/{job_id}")
def bulk_job(job_id: int, ctx: TenantContext = Depends(require_tenant_context)):
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, filename, status, total_rows, created_rows, skipped_rows,
                       error_rows, report, created_at, finished_at
                FROM public.bulk_import_job WHERE id = %s AND tenant_id = %s;
            """, (job_id, ctx.active_tenant_id))
            r = cur.fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="Import job not found")
    return {"id": r[0], "filename": r[1], "status": r[2], "total_rows": r[3],
            "created_rows": r[4], "skipped_rows": r[5], "error_rows": r[6],
            "report": r[7], "created_at": r[8].isoformat() if r[8] else None,
            "finished_at": r[9].isoformat() if r[9] else None}


@router.get("/bulk-devices/pending")
def bulk_pending(ctx: TenantContext = Depends(require_tenant_context)):
    """Devices created without a device_id, awaiting an ANDROID_ID (claim)."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT d.id, d.device_name, d.activation_code, d.resolution,
                       s.shop_name, g.gname, d.created_at
                FROM public.device d
                LEFT JOIN public.device_assignment da ON da.did = d.id
                LEFT JOIN public.shop s ON s.id = da.sid
                LEFT JOIN public."group" g ON g.id = da.gid
                WHERE d.tenant_id = %s AND d.mobile_id LIKE %s
                ORDER BY d.created_at DESC;
            """, (ctx.active_tenant_id, PENDING_PREFIX + "%"))
            items = [{"id": r[0], "device_name": r[1], "code": r[2], "resolution": r[3],
                      "shop_name": r[4], "group_name": r[5],
                      "created_at": r[6].isoformat() if r[6] else None} for r in cur.fetchall()]
    return {"items": items, "count": len(items)}


@router.post("/bulk-devices/claim")
def bulk_claim(body: ClaimIn, ctx: TenantContext = Depends(require_tenant_context)):
    """Bind a real ANDROID_ID to a pending device row. The physical device then
    auto-enrolls on its next GET /device/{id}/online poll."""
    tenant_id = ctx.active_tenant_id
    mobile_id = body.mobile_id.strip()
    if not MOBILE_ID_RE.match(mobile_id) or mobile_id.startswith(PENDING_PREFIX):
        raise HTTPException(status_code=422, detail="Invalid device id")
    with pg_conn() as conn:
        try:
            with conn.cursor() as cur:
                # Reject if this ANDROID_ID is already used in ANY tenant
                # (global heartbeat lookups ignore tenant).
                cur.execute("SELECT tenant_id FROM public.device WHERE mobile_id = %s;", (mobile_id,))
                clash = cur.fetchone()
                if clash:
                    raise HTTPException(status_code=409,
                                        detail="That device id is already registered"
                                               + (" to this company" if clash[0] == tenant_id else " to another company"))
                cur.execute("""
                    UPDATE public.device
                    SET mobile_id = %s, activation_code = NULL, activated_at = NOW(), updated_at = NOW()
                    WHERE id = %s AND tenant_id = %s AND mobile_id LIKE %s
                    RETURNING id, device_name;
                """, (mobile_id, body.device_id, tenant_id, PENDING_PREFIX + "%"))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Pending device not found (already claimed?)")
                log_audit(conn, tenant_id, ctx.user_id, "device.claim", "device", body.device_id,
                          details={"mobile_id": mobile_id})
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception as e:
            conn.rollback()
            logger.error("claim failed (tenant %s): %s", tenant_id, e)
            raise HTTPException(status_code=500, detail="Claim failed")
    return {"device_id": body.device_id, "mobile_id": mobile_id, "claimed": True}


# ─────────────────────────── helpers ───────────────────────────

def _json(obj) -> str:
    import json
    return json.dumps(obj)


def _pending_code() -> str:
    # short, unambiguous (no O/0/I/1) human code for the pending device
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(secrets.choice(alphabet) for _ in range(6))


def _upsert_named(cur, table: str, name_col: str, names: set, tenant_id: int) -> Dict[str, int]:
    """Idempotent upsert of shops/groups; returns {name: id}. Uses the per-tenant
    unique index so ON CONFLICT ... DO UPDATE ... RETURNING yields the id either way."""
    out: Dict[str, int] = {}
    conflict = "(tenant_id, shop_name)" if name_col == "shop_name" else "(tenant_id, gname)"
    for name in names:
        cur.execute(f"""
            INSERT INTO public.{table} ({name_col}, tenant_id) VALUES (%s, %s)
            ON CONFLICT {conflict} DO UPDATE SET {name_col} = EXCLUDED.{name_col}
            RETURNING id;
        """, (name, tenant_id))
        out[name] = cur.fetchone()[0]
    return out
