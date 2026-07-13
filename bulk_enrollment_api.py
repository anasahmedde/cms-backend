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
import logging
import re
import secrets
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from database import pg_conn
from tenant_context import TenantContext, log_audit, require_tenant_context

logger = logging.getLogger("bulk_enrollment_api")
router = APIRouter()

COLUMNS = ["device_name", "shop_name", "group_name", "device_id", "resolution", "notes"]
REQUIRED = ["device_name", "shop_name"]
RESOLUTION_RE = re.compile(r"^\d{2,5}x\d{2,5}$")
MOBILE_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")
PENDING_PREFIX = "pending:"
MAX_ROWS = 20000
COMMIT_BATCH = 500

EXAMPLE_ROWS = [
    ["Main Entrance Screen", "Shop Karachi 12", "North Region", "a1b2c3d4e5f6a7b8", "1080x1920", "landscape TV at door"],
    ["Checkout Screen", "Shop Karachi 12", "North Region", "", "", "device id unknown - will be claimed on site"],
]
INSTRUCTIONS = [
    "DIGIX bulk device import — fill one row per screen, then upload this file.",
    "device_name (required): a friendly name for the screen.",
    "shop_name (required): the shop this screen belongs to; created automatically if new.",
    "group_name (optional): a group for the screen; created automatically if new.",
    "device_id (optional): the device's ANDROID_ID. If you know it, the screen auto-enrolls when it powers on. Leave blank to create a 'pending' screen you claim on site.",
    "resolution (optional): e.g. 1080x1920. Auto-detected from the device if blank.",
    "notes (optional): free text, not shown on screens.",
]


# ─────────────────────────── models ───────────────────────────

class CommitIn(BaseModel):
    job_id: int
    auto_create: bool = True  # create missing shops/groups automatically


class ClaimIn(BaseModel):
    device_id: int                      # the pending device row's id
    mobile_id: str = Field(min_length=1, max_length=64)  # the real ANDROID_ID


# ─────────────────────────── template download ───────────────────────────

def _csv_bytes() -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    for line in INSTRUCTIONS:
        w.writerow(["# " + line])
    w.writerow(COLUMNS)
    for row in EXAMPLE_ROWS:
        w.writerow(row)
    return buf.getvalue().encode("utf-8")


@router.get("/bulk-devices/template.csv")
def template_csv(ctx: TenantContext = Depends(require_tenant_context)):
    return StreamingResponse(
        io.BytesIO(_csv_bytes()), media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="digix-devices-template.csv"'},
    )


@router.get("/bulk-devices/template.xlsx")
def template_xlsx(ctx: TenantContext = Depends(require_tenant_context)):
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill
    except ImportError:
        raise HTTPException(status_code=501, detail="XLSX export unavailable; use the CSV template")
    wb = Workbook()
    ws = wb.active
    ws.title = "Devices"
    header_fill = PatternFill("solid", fgColor="0A1628")
    header_font = Font(color="FFFFFF", bold=True)
    ws.append(COLUMNS)
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
    for row in EXAMPLE_ROWS:
        ws.append(row)
    widths = [26, 22, 18, 22, 12, 30]
    for i, wdt in enumerate(widths, start=1):
        ws.column_dimensions[chr(64 + i)].width = wdt
    info = wb.create_sheet("Instructions")
    for line in INSTRUCTIONS:
        info.append([line])
    info.column_dimensions["A"].width = 110
    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return StreamingResponse(
        out, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="digix-devices-template.xlsx"'},
    )


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
    idx = {col: header.index(col) for col in COLUMNS if col in header}
    out = []
    for cells in data_rows:
        if not any(str(c or "").strip() for c in cells):
            continue
        rec = {}
        for col in COLUMNS:
            i = idx.get(col)
            rec[col] = str(cells[i]).strip() if (i is not None and i < len(cells) and cells[i] is not None) else ""
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

def validate_rows(rows: List[Dict[str, str]], existing_device_count: int, max_devices: int,
                  mobile_ids_this_tenant: set, mobile_ids_other_tenant: set) -> Dict[str, Any]:
    """Pure validation. Returns preview {rows, errors, summary}. Writes nothing."""
    errors: List[Dict[str, Any]] = []
    seen_ids: Dict[str, int] = {}
    shops, groups = set(), set()
    valid_new = 0        # devices that will be created
    with_id = 0
    pending = 0
    skipped = 0          # already exist in this tenant (idempotent re-run)

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

        will = "error"
        if not row_errors:
            if dev_id:
                seen_ids[dev_id] = rownum
                if dev_id in mobile_ids_this_tenant:
                    will = "skip"      # already enrolled in this tenant
                    skipped += 1
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
            "error_rows": len({e["row"] for e in errors if e["row"] > 0}),
            "new_shops": sorted(shops),
            "new_groups": sorted(groups),
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

            preview = validate_rows(rows, existing, max_devices, mine, other)

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

                created = skipped = pending = 0
                # 2) create devices row-by-row (safe + idempotent); assignment per device
                for r in rows:
                    if r["action"] == "error":
                        continue
                    if r["action"] == "skip":
                        skipped += 1
                        continue
                    if r["action"] == "pending":
                        code = _pending_code()
                        mobile_id = PENDING_PREFIX + code
                        activation = code
                    else:  # create
                        mobile_id = r["device_id"]
                        activation = None
                        # Guard: skip if it now exists in this tenant (idempotent re-run)
                        cur.execute("SELECT id FROM public.device WHERE tenant_id = %s AND mobile_id = %s;",
                                    (tenant_id, mobile_id))
                        if cur.fetchone():
                            skipped += 1
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
                    if r["action"] == "pending":
                        pending += 1
                    else:
                        created += 1

                report = {"created": created, "pending": pending, "skipped": skipped,
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
