# reports_api.py — fleet-wide report aggregates for the dashboard Reports page.
#
# One request per report instead of one request per screen: each endpoint
# returns {summary, series, screens} for the caller's WHOLE tenant scope
# (optionally narrowed to a location/group) in a single SQL pass.
#
# Gating: every endpoint needs view_reports; temperature/footfall additionally
# honor company.features server-side — a disabled feature 403s here even if a
# stale UI (or a hand-typed URL) asks, so "disabled means not visible" cannot
# be bypassed. Reads only; device-facing routes are untouched.
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from database import pg_conn
from tenant_context import get_current_user, require_perm, require_user_tenant

router = APIRouter()


def _range_or_422(start_date: Optional[str], end_date: Optional[str]):
    """Parse the report window; default = last 7 days, end inclusive."""
    try:
        end = date.fromisoformat(end_date) if end_date else date.today()
        start = date.fromisoformat(start_date) if start_date else end - timedelta(days=6)
    except ValueError:
        raise HTTPException(status_code=422, detail="Dates must be YYYY-MM-DD")
    if start > end:
        raise HTTPException(status_code=422, detail="start_date is after end_date")
    if (end - start).days > 400:
        raise HTTPException(status_code=422, detail="Range too large — max 400 days")
    return start, end


def _require_feature(cur, tenant_id: int, key: str) -> None:
    """company.features gate, mirroring the dashboard's featureOn (missing = off)."""
    cur.execute("SELECT COALESCE(features, '{}'::jsonb) ->> %s FROM public.company WHERE id = %s;",
                (key, tenant_id))
    row = cur.fetchone()
    if not row or str(row[0]).lower() != "true":
        raise HTTPException(status_code=403,
                            detail=f"The {key} feature is disabled for this company")


_SCOPE_SQL = """
    SELECT d.id, d.mobile_id, d.device_name, s.shop_name, g.gname, d.is_online
    FROM public.device d
    LEFT JOIN public.device_assignment da ON da.did = d.id
    LEFT JOIN public.shop s ON s.id = da.sid
    LEFT JOIN public."group" g ON g.id = da.gid
    WHERE d.tenant_id = %s AND d.is_active IS NOT FALSE
      AND (%s::bigint IS NULL OR da.sid = %s)
      AND (%s::bigint IS NULL OR da.gid = %s)
"""


def _scope_devices(cur, tenant_id: int, shop_id: Optional[int], group_id: Optional[int]) -> List[Dict]:
    cur.execute(_SCOPE_SQL, (tenant_id, shop_id, shop_id, group_id, group_id))
    return [{"id": r[0], "mobile_id": r[1], "device_name": r[2],
             "shop_name": r[3], "gname": r[4], "is_online": bool(r[5])}
            for r in cur.fetchall()]


def _base(cur, user: Dict, feature: Optional[str], start_date, end_date, shop_id, group_id):
    require_perm(user, "view_reports")
    tenant_id = require_user_tenant(user)
    if feature:
        _require_feature(cur, tenant_id, feature)
    start, end = _range_or_422(start_date, end_date)
    devices = _scope_devices(cur, tenant_id, shop_id, group_id)
    return tenant_id, start, end, devices


@router.get("/reports/uptime")
def report_uptime(
    start_date: Optional[str] = Query(None), end_date: Optional[str] = Query(None),
    shop_id: Optional[int] = Query(None), group_id: Optional[int] = Query(None),
    user: Dict = Depends(get_current_user),
):
    """Per-screen online/offline totals + fleet summary, from the online-history
    event stream. A screen with no events in the window reports no_data (its
    uptime is unknown, not 0%)."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            tenant_id, start, end, devices = _base(cur, user, None, start_date, end_date, shop_id, group_id)
            if not devices:
                return {"range": {"start": str(start), "end": str(end)},
                        "summary": _uptime_summary([], 0), "screens": []}
            ids = [d["id"] for d in devices]
            # Pair each event with the next one per device; an open final session
            # runs to the window end (or now, whichever is earlier).
            cur.execute("""
                WITH ev AS (
                    SELECT did, event_type, event_at,
                           LEAD(event_at) OVER (PARTITION BY did ORDER BY event_at) AS next_at
                    FROM public.device_online_history
                    WHERE did = ANY(%s::bigint[])
                      AND event_at >= %s::date
                      AND event_at < (%s::date + interval '1 day')
                )
                SELECT did,
                       COALESCE(SUM(EXTRACT(EPOCH FROM (
                           LEAST(COALESCE(next_at, NOW()), (%s::date + interval '1 day')) - event_at
                       ))) FILTER (WHERE event_type = 'online'), 0)  AS online_s,
                       COALESCE(SUM(EXTRACT(EPOCH FROM (
                           LEAST(COALESCE(next_at, NOW()), (%s::date + interval '1 day')) - event_at
                       ))) FILTER (WHERE event_type = 'offline'), 0) AS offline_s,
                       COUNT(*) FILTER (WHERE event_type = 'online')  AS online_events,
                       COUNT(*) FILTER (WHERE event_type = 'offline') AS offline_events
                FROM ev
                GROUP BY did;
            """, (ids, start, end, end, end))
            stats = {r[0]: {"online_seconds": int(r[1]), "offline_seconds": int(r[2]),
                            "online_events": int(r[3]), "offline_events": int(r[4])}
                     for r in cur.fetchall()}

    screens = []
    for d in devices:
        st = stats.get(d["id"])
        if st:
            total = st["online_seconds"] + st["offline_seconds"]
            pct = round(100.0 * st["online_seconds"] / total, 1) if total > 0 else None
            screens.append({**d, **st, "online_percentage": pct, "no_data": False})
        else:
            screens.append({**d, "online_seconds": 0, "offline_seconds": 0,
                            "online_events": 0, "offline_events": 0,
                            "online_percentage": None, "no_data": True})
    screens.sort(key=lambda s: (s["no_data"], s["online_percentage"] if s["online_percentage"] is not None else 101))
    return {"range": {"start": str(start), "end": str(end)},
            "summary": _uptime_summary(screens, len(devices)), "screens": screens}


def _uptime_summary(screens: List[Dict], total: int) -> Dict:
    reporting = [s for s in screens if not s.get("no_data")]
    online_s = sum(s["online_seconds"] for s in reporting)
    offline_s = sum(s["offline_seconds"] for s in reporting)
    fleet_pct = round(100.0 * online_s / (online_s + offline_s), 1) if (online_s + offline_s) > 0 else None
    with_pct = [s for s in reporting if s["online_percentage"] is not None]
    worst = min(with_pct, key=lambda s: s["online_percentage"], default=None)
    return {
        "screens_total": total,
        "screens_reporting": len(reporting),
        "fleet_online_percentage": fleet_pct,
        "total_online_seconds": online_s,
        "total_offline_seconds": offline_s,
        "worst": {"mobile_id": worst["mobile_id"], "device_name": worst["device_name"],
                  "online_percentage": worst["online_percentage"]} if worst else None,
    }


@router.get("/reports/temperature")
def report_temperature(
    start_date: Optional[str] = Query(None), end_date: Optional[str] = Query(None),
    shop_id: Optional[int] = Query(None), group_id: Optional[int] = Query(None),
    user: Dict = Depends(get_current_user),
):
    """Avg/min/max temperature series across the scope + per-screen stats.
    Buckets by hour for windows of up to 2 days, by day otherwise. 403 when the
    company's temperature feature is disabled."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            tenant_id, start, end, devices = _base(cur, user, "temperature", start_date, end_date, shop_id, group_id)
            bucket = "hour" if (end - start).days <= 2 else "day"
            series, per_screen = [], {}
            if devices:
                ids = [d["id"] for d in devices]
                cur.execute("""
                    SELECT date_trunc(%s, logged_at) AS b,
                           ROUND(AVG(value)::numeric, 1), ROUND(MIN(value)::numeric, 1),
                           ROUND(MAX(value)::numeric, 1), COUNT(*)
                    FROM public.device_logs
                    WHERE did = ANY(%s::bigint[]) AND log_type = 'temperature' AND value IS NOT NULL
                      AND logged_at >= %s::date AND logged_at < (%s::date + interval '1 day')
                    GROUP BY b ORDER BY b;
                """, (bucket, ids, start, end))
                series = [{"t": r[0].isoformat(), "avg": float(r[1]), "min": float(r[2]),
                           "max": float(r[3]), "readings": int(r[4])} for r in cur.fetchall()]
                cur.execute("""
                    SELECT DISTINCT ON (did) did, value, logged_at,
                           AVG(value) OVER (PARTITION BY did),
                           MIN(value) OVER (PARTITION BY did),
                           MAX(value) OVER (PARTITION BY did),
                           COUNT(*)  OVER (PARTITION BY did)
                    FROM public.device_logs
                    WHERE did = ANY(%s::bigint[]) AND log_type = 'temperature' AND value IS NOT NULL
                      AND logged_at >= %s::date AND logged_at < (%s::date + interval '1 day')
                    ORDER BY did, logged_at DESC;
                """, (ids, start, end))
                per_screen = {r[0]: {"last": round(float(r[1]), 1), "last_at": r[2].isoformat(),
                                     "avg": round(float(r[3]), 1), "min": round(float(r[4]), 1),
                                     "max": round(float(r[5]), 1), "readings": int(r[6])}
                              for r in cur.fetchall()}

    screens = []
    for d in devices:
        st = per_screen.get(d["id"])
        screens.append({**d, **(st or {}), "no_data": st is None})
    screens.sort(key=lambda s: (s["no_data"], -(s.get("max") or -999)))
    reporting = [s for s in screens if not s["no_data"]]
    hottest = max(reporting, key=lambda s: s["max"], default=None)
    summary = {
        "screens_total": len(devices),
        "screens_reporting": len(reporting),
        "avg": round(sum(s["avg"] * s["readings"] for s in reporting) /
                     max(1, sum(s["readings"] for s in reporting)), 1) if reporting else None,
        "peak": {"value": hottest["max"], "mobile_id": hottest["mobile_id"],
                 "device_name": hottest["device_name"]} if hottest else None,
        "low": round(min(s["min"] for s in reporting), 1) if reporting else None,
        "readings": sum(s["readings"] for s in reporting),
    }
    return {"range": {"start": str(start), "end": str(end)}, "bucket": bucket,
            "summary": summary, "series": series, "screens": screens}


@router.get("/reports/footfall")
def report_footfall(
    grain: str = Query("daily", pattern="^(daily|monthly)$"),
    start_date: Optional[str] = Query(None), end_date: Optional[str] = Query(None),
    shop_id: Optional[int] = Query(None), group_id: Optional[int] = Query(None),
    user: Dict = Depends(get_current_user),
):
    """Footfall totals across the scope (per day or per month) + per-screen
    totals. 403 when the company's footfall feature is disabled."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            tenant_id, start, end, devices = _base(cur, user, "footfall", start_date, end_date, shop_id, group_id)
            series, per_screen = [], {}
            # count_history is created lazily by the first device count report —
            # a fresh install without it just means "no data yet", not a 500.
            cur.execute("SELECT to_regclass('public.count_history') IS NOT NULL;")
            table_exists = bool(cur.fetchone()[0])
            if devices and table_exists:
                ids = [d["id"] for d in devices]
                cur.execute("""
                    SELECT period_date, SUM(count_value)::bigint
                    FROM public.count_history
                    WHERE did = ANY(%s::bigint[]) AND period_type = %s
                      AND period_date >= %s::date AND period_date <= %s::date
                    GROUP BY period_date ORDER BY period_date;
                """, (ids, grain, start, end))
                series = [{"t": r[0].isoformat(), "total": int(r[1])} for r in cur.fetchall()]
                cur.execute("""
                    SELECT did, SUM(count_value)::bigint, MAX(count_value)::bigint, COUNT(*)
                    FROM public.count_history
                    WHERE did = ANY(%s::bigint[]) AND period_type = %s
                      AND period_date >= %s::date AND period_date <= %s::date
                    GROUP BY did;
                """, (ids, grain, start, end))
                per_screen = {r[0]: {"total": int(r[1]), "best": int(r[2]), "periods": int(r[3])}
                              for r in cur.fetchall()}

    screens = []
    for d in devices:
        st = per_screen.get(d["id"])
        screens.append({**d, **(st or {}), "no_data": st is None})
    screens.sort(key=lambda s: (s["no_data"], -(s.get("total") or 0)))
    reporting = [s for s in screens if not s["no_data"]]
    total = sum(s["total"] for s in reporting)
    best_period = max(series, key=lambda p: p["total"], default=None)
    top = max(reporting, key=lambda s: s["total"], default=None)
    summary = {
        "screens_total": len(devices),
        "screens_reporting": len(reporting),
        "total": total,
        "per_period_avg": round(total / len(series), 1) if series else None,
        "best_period": best_period,
        "top_screen": {"mobile_id": top["mobile_id"], "device_name": top["device_name"],
                       "total": top["total"]} if top else None,
    }
    return {"range": {"start": str(start), "end": str(end)}, "grain": grain,
            "summary": summary, "series": series, "screens": screens}
