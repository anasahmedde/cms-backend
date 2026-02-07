import os
from typing import Optional, List, Dict, Any, Tuple

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import psycopg2
from psycopg2 import pool, errorcodes


# =========================
# Env / Settings
# =========================
load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET", "markhorvideo")
DEFAULT_EXPIRES_IN = 604800  # 7 days

# --- Postgres ---
DB_HOST = os.getenv("PGHOST", "172.31.17.177")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_NAME = os.getenv("PGDATABASE", "dgx")
DB_USER = os.getenv("PGUSER", "app_user")
DB_PASS = os.getenv("PGPASSWORD", "strongpassword")
DB_MIN_CONN = int(os.getenv("PG_MIN_CONN", "1"))
DB_MAX_CONN = int(os.getenv("PG_MAX_CONN", "5"))


# =========================
# API (create app FIRST)
# =========================
app = FastAPI(title="Device Service", version="1.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================
# PG Pool Helpers
# =========================
pg_pool: Optional[pool.SimpleConnectionPool] = None


def pg_init_pool():
    global pg_pool
    if pg_pool is None:
        pg_pool = psycopg2.pool.SimpleConnectionPool(
            DB_MIN_CONN,
            DB_MAX_CONN,
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            connect_timeout=5,
        )


def pg_get_conn():
    if pg_pool is None:
        pg_init_pool()
    return pg_pool.getconn()


def pg_put_conn(conn):
    if pg_pool:
        pg_pool.putconn(conn)


def pg_close_pool():
    global pg_pool
    if pg_pool:
        pg_pool.closeall()
        pg_pool = None


# =========================
# Lightweight migration
# =========================
def ensure_schema():
    """
    Creates public.device if it doesn't exist and adds a helpful index.
    """
    ddl_device = """
    CREATE TABLE IF NOT EXISTS public.device (
        id BIGSERIAL PRIMARY KEY,
        mobile_id TEXT NOT NULL,
        download_status BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    ddl_updated_at_fn = """
    CREATE OR REPLACE FUNCTION public.set_updated_at()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """
    ddl_device_trigger = """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger WHERE tgname = 'trg_device_set_updated_at'
        ) THEN
            CREATE TRIGGER trg_device_set_updated_at
            BEFORE UPDATE ON public.device
            FOR EACH ROW
            EXECUTE FUNCTION public.set_updated_at();
        END IF;
    END;
    $$;
    """
    ddl_device_index = """
    CREATE INDEX IF NOT EXISTS idx_device_mobile_id
    ON public.device (mobile_id);
    """

    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute(ddl_device)
            cur.execute(ddl_updated_at_fn)
            cur.execute(ddl_device_trigger)
            cur.execute(ddl_device_index)
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            pg_put_conn(conn)


# =========================
# Models
# =========================
class DeviceRequest(BaseModel):
    mobile_id: str
    download_status: bool = False


class DeviceUpdate(BaseModel):
    mobile_id: Optional[str] = None
    download_status: Optional[bool] = None


# =========================
# Data access helpers
# =========================
def insert_device(mobile_id: str, download_status: bool) -> int:
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.device (mobile_id, download_status)
                VALUES (%s, %s)
                RETURNING id;
                """,
                (mobile_id, download_status),
            )
            new_id = cur.fetchone()[0]
        conn.commit()
        return new_id
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            pg_put_conn(conn)


def fetch_one_by_mobile(mobile_id: str) -> Optional[Dict[str, Any]]:
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, mobile_id, download_status, created_at, updated_at
                FROM public.device
                WHERE mobile_id = %s
                ORDER BY id DESC
                LIMIT 1;
                """,
                (mobile_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "mobile_id": row[1],
                "download_status": row[2],
                "created_at": row[3],
                "updated_at": row[4],
            }
    finally:
        if conn:
            pg_put_conn(conn)


def fetch_device_ids_by_mobile(mobile_id: str) -> List[int]:
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM public.device
                WHERE mobile_id = %s
                ORDER BY id DESC;
                """,
                (mobile_id,),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        if conn:
            pg_put_conn(conn)


def _dvsg_exists(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema='public' AND table_name='device_video_shop_group'
            );
            """
        )
        return bool(cur.fetchone()[0])


def fetch_link_summary_for_mobile(mobile_id: str, limit: int = 10) -> Dict[str, Any]:
    """
    Return how many links exist in device_video_shop_group
    + most recent linked rows (joined with names).
    NOTE: returns JSON-safe values (datetimes converted to ISO strings).
    """
    device_ids = fetch_device_ids_by_mobile(mobile_id)
    if not device_ids:
        return {"linked_count": 0, "recent_links": [], "device_ids": []}

    conn = None
    try:
        conn = pg_get_conn()

        # If dvsg table isn't present, don't crash
        if not _dvsg_exists(conn):
            return {"linked_count": 0, "recent_links": [], "device_ids": device_ids}

        with conn.cursor() as cur:
            # Count links across all rows with this mobile_id (in case duplicates exist)
            cur.execute(
                """
                SELECT COUNT(*)
                FROM public.device_video_shop_group
                WHERE did = ANY(%s::bigint[]);
                """,
                (device_ids,),
            )
            linked_count = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT
                    l.id AS link_id,
                    l.did,
                    d.mobile_id,
                    l.gid,
                    g.gname,
                    l.sid,
                    s.shop_name,
                    l.vid,
                    v.video_name,
                    l.created_at,
                    l.updated_at
                FROM public.device_video_shop_group l
                LEFT JOIN public.device d ON d.id = l.did
                LEFT JOIN public."group" g ON g.id = l.gid
                LEFT JOIN public.shop s ON s.id = l.sid
                LEFT JOIN public.video v ON v.id = l.vid
                WHERE l.did = ANY(%s::bigint[])
                ORDER BY l.updated_at DESC NULLS LAST, l.id DESC
                LIMIT %s;
                """,
                (device_ids, limit),
            )
            rows = cur.fetchall()

        recent_links = []
        for r in rows:
            recent_links.append(
                {
                    "link_id": r[0],
                    "did": r[1],
                    "mobile_id": r[2],
                    "gid": r[3],
                    "gname": r[4],
                    "sid": r[5],
                    "shop_name": r[6],
                    "vid": r[7],
                    "video_name": r[8],
                    "created_at": r[9],
                    "updated_at": r[10],
                }
            )

        payload = {"linked_count": linked_count, "recent_links": recent_links, "device_ids": device_ids}

        # IMPORTANT: make it JSON-safe for HTTPException.detail too
        return jsonable_encoder(payload)

    except Exception:
        return {"linked_count": 0, "recent_links": [], "device_ids": device_ids}
    finally:
        if conn:
            pg_put_conn(conn)


def fetch_total(q: Optional[str]) -> int:
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            if q:
                cur.execute(
                    "SELECT COUNT(*) FROM public.device WHERE mobile_id ILIKE %s OR device_name ILIKE %s;",
                    (f"%{q}%", f"%{q}%"),
                )
            else:
                cur.execute("SELECT COUNT(*) FROM public.device;")
            return int(cur.fetchone()[0])
    finally:
        if conn:
            pg_put_conn(conn)


def fetch_many(q: Optional[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            if q:
                cur.execute(
                    """
                    SELECT d.id, d.mobile_id, d.download_status, d.created_at, d.updated_at, d.resolution, d.device_name,
                           COALESCE(g1.gname, g2.gname) as group_name
                    FROM public.device d
                    LEFT JOIN public.device_assignment da ON da.did = d.id
                    LEFT JOIN public."group" g1 ON g1.id = da.gid
                    LEFT JOIN (
                        SELECT DISTINCT ON (dvsg.did) dvsg.did, g.gname
                        FROM public.device_video_shop_group dvsg
                        JOIN public."group" g ON g.id = dvsg.gid
                        WHERE dvsg.gid IS NOT NULL
                    ) g2 ON g2.did = d.id AND g1.gname IS NULL
                    WHERE d.mobile_id ILIKE %s OR d.device_name ILIKE %s
                    ORDER BY d.id DESC
                    LIMIT %s OFFSET %s;
                    """,
                    (f"%{q}%", f"%{q}%", limit, offset),
                )
            else:
                cur.execute(
                    """
                    SELECT d.id, d.mobile_id, d.download_status, d.created_at, d.updated_at, d.resolution, d.device_name,
                           COALESCE(g1.gname, g2.gname) as group_name
                    FROM public.device d
                    LEFT JOIN public.device_assignment da ON da.did = d.id
                    LEFT JOIN public."group" g1 ON g1.id = da.gid
                    LEFT JOIN (
                        SELECT DISTINCT ON (dvsg.did) dvsg.did, g.gname
                        FROM public.device_video_shop_group dvsg
                        JOIN public."group" g ON g.id = dvsg.gid
                        WHERE dvsg.gid IS NOT NULL
                    ) g2 ON g2.did = d.id AND g1.gname IS NULL
                    ORDER BY d.id DESC
                    LIMIT %s OFFSET %s;
                    """,
                    (limit, offset),
                )
            rows = cur.fetchall()
            return [
                {
                    "id": r[0],
                    "mobile_id": r[1],
                    "download_status": r[2],
                    "created_at": r[3],
                    "updated_at": r[4],
                    "resolution": r[5] if len(r) > 5 else None,
                    "device_name": r[6] if len(r) > 6 else None,
                    "group_name": r[7] if len(r) > 7 else None,
                }
                for r in rows
            ]
    finally:
        if conn:
            pg_put_conn(conn)


def update_by_mobile(mobile_id: str, patch: DeviceUpdate) -> Optional[Dict[str, Any]]:
    sets = []
    params: List[Any] = []
    if patch.mobile_id is not None:
        sets.append("mobile_id = %s")
        params.append(patch.mobile_id)
    if patch.download_status is not None:
        sets.append("download_status = %s")
        params.append(patch.download_status)

    if not sets:
        return fetch_one_by_mobile(mobile_id)

    params.append(mobile_id)
    sql = f"""
        UPDATE public.device
        SET {", ".join(sets)}
        WHERE mobile_id = %s
        RETURNING id, mobile_id, download_status, created_at, updated_at;
    """

    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        conn.commit()
        if not row:
            return None
        return {
            "id": row[0],
            "mobile_id": row[1],
            "download_status": row[2],
            "created_at": row[3],
            "updated_at": row[4],
        }
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            pg_put_conn(conn)


def delete_by_mobile(mobile_id: str) -> int:
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM public.device
                WHERE mobile_id = %s
                RETURNING id;
                """,
                (mobile_id,),
            )
            rows = cur.fetchall()
        conn.commit()
        return len(rows)
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            pg_put_conn(conn)


# =========================
# Startup / Shutdown
# =========================
@app.on_event("startup")
def on_startup():
    pg_init_pool()
    ensure_schema()


@app.on_event("shutdown")
def on_shutdown():
    pg_close_pool()


# =========================
# Routes
# =========================
@app.get("/db_health")
def db_health():
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT current_user;")
            user = cur.fetchone()[0]
            cur.execute(
                """
                SELECT EXISTS (
                  SELECT 1
                  FROM information_schema.tables
                  WHERE table_schema='public' AND table_name='device'
                );
                """
            )
            has_device = cur.fetchone()[0]
        return {"ok": True, "db_user": user, "device_table_exists": has_device}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")
    finally:
        if conn:
            pg_put_conn(conn)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/insert_device")
def create_device(req: DeviceRequest):
    try:
        mobile = (req.mobile_id or "").strip()
        if not mobile:
            raise HTTPException(status_code=400, detail="mobile_id is required")

        new_id = insert_device(mobile, bool(req.download_status))
        return {"id": new_id, "message": "Device inserted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to insert device: {e}")


@app.get("/device/{mobile_id}")
def get_device(mobile_id: str = Path(..., description="Exact mobile_id to fetch")):
    row = fetch_one_by_mobile(mobile_id)
    if not row:
        raise HTTPException(status_code=404, detail="Device not found")
    return row


@app.get("/devices")
def list_devices(
    q: Optional[str] = Query(None, description="Search mobile_id (ILIKE %q%)"),
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    total = fetch_total(q)
    rows = fetch_many(q, limit, offset)
    return {
        "items": rows,
        "total": total,
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "query": q,
    }


@app.put("/device/{mobile_id}")
def update_device(mobile_id: str, patch: DeviceUpdate):
    updated = update_by_mobile(mobile_id, patch)
    if not updated:
        raise HTTPException(status_code=404, detail="Device not found or no changes")
    return {"message": "Device updated", "item": updated}


@app.delete("/device/{mobile_id}")
def delete_device(mobile_id: str):
    # 1) Pre-check links (so we return a clean 409 without triggering FK exception)
    summary = fetch_link_summary_for_mobile(mobile_id, limit=10)
    if int(summary.get("linked_count", 0)) > 0:
        payload = {
            "message": f"Device '{mobile_id}' is linked with other records. Remove links first, then delete the device.",
            "linked_count": summary.get("linked_count", 0),
            "device_ids": summary.get("device_ids", []),
            "recent_links": summary.get("recent_links", []),
        }
        raise HTTPException(status_code=409, detail=jsonable_encoder(payload))

    # 2) Attempt delete
    try:
        deleted = delete_by_mobile(mobile_id)
        if deleted == 0:
            raise HTTPException(status_code=404, detail="Device not found")
        return {"deleted_count": deleted}

    except psycopg2.IntegrityError as e:
        # FK violation safety net (race condition)
        if getattr(e, "pgcode", None) == errorcodes.FOREIGN_KEY_VIOLATION:
            summary = fetch_link_summary_for_mobile(mobile_id, limit=10)
            payload = {
                "message": f"Device '{mobile_id}' is linked with other records. Remove links first, then delete the device.",
                "linked_count": summary.get("linked_count", 0),
                "device_ids": summary.get("device_ids", []),
                "recent_links": summary.get("recent_links", []),
            }
            raise HTTPException(status_code=409, detail=jsonable_encoder(payload))

        raise HTTPException(status_code=500, detail=f"DB IntegrityError: {str(e)}")

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete device: {type(e).__name__}: {e}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "device:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=True,
    )

