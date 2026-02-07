import os
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool

# =========================
# Env / Settings
# =========================
load_dotenv()

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
app = FastAPI(title='Group Service', version='1.0.0')

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten in production
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
    Creates public."group" if it doesn't exist and adds updated_at trigger + index.
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

    ddl_group = """
    CREATE TABLE IF NOT EXISTS public."group" (
        id BIGSERIAL PRIMARY KEY,
        gname TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    ddl_group_trigger = """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger WHERE tgname = 'trg_group_set_updated_at'
        ) THEN
            CREATE TRIGGER trg_group_set_updated_at
            BEFORE UPDATE ON public."group"
            FOR EACH ROW
            EXECUTE FUNCTION public.set_updated_at();
        END IF;
    END;
    $$;
    """

    ddl_group_index = """
    CREATE INDEX IF NOT EXISTS idx_group_gname
    ON public."group" (gname);
    """

    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute(ddl_updated_at_fn)
            cur.execute(ddl_group)
            cur.execute(ddl_group_trigger)
            cur.execute(ddl_group_index)
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
class GroupCreate(BaseModel):
    gname: str

class GroupUpdate(BaseModel):
    gname: Optional[str] = None  # allow renaming

# =========================
# Data access helpers
# =========================
def insert_group(gname: str) -> int:
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public."group" (gname)
                VALUES (%s)
                RETURNING id;
                """,
                (gname,),
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

def fetch_one_by_gname(gname: str) -> Optional[Dict[str, Any]]:
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, gname, created_at, updated_at
                FROM public."group"
                WHERE gname = %s
                ORDER BY id DESC
                LIMIT 1;
                """,
                (gname,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "gname": row[1],
                "created_at": row[2],
                "updated_at": row[3],
            }
    finally:
        if conn:
            pg_put_conn(conn)

def fetch_groups(q: Optional[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            if q:
                cur.execute(
                    """
                    SELECT id, gname, created_at, updated_at
                    FROM public."group"
                    WHERE gname ILIKE %s
                    ORDER BY id DESC
                    LIMIT %s OFFSET %s;
                    """,
                    (f"%{q}%", limit, offset),
                )
            else:
                cur.execute(
                    """
                    SELECT id, gname, created_at, updated_at
                    FROM public."group"
                    ORDER BY id DESC
                    LIMIT %s OFFSET %s;
                    """,
                    (limit, offset),
                )
            rows = cur.fetchall()
            return [
                {"id": r[0], "gname": r[1], "created_at": r[2], "updated_at": r[3]}
                for r in rows
            ]
    finally:
        if conn:
            pg_put_conn(conn)

def update_by_gname(current_gname: str, patch: GroupUpdate) -> Optional[Dict[str, Any]]:
    sets = []
    params: List[Any] = []

    if patch.gname is not None:
        sets.append('gname = %s')
        params.append(patch.gname)

    if not sets:
        return fetch_one_by_gname(current_gname)

    params.append(current_gname)

    sql = f"""
        UPDATE public."group"
        SET {", ".join(sets)}
        WHERE gname = %s
        RETURNING id, gname, created_at, updated_at;
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
        return {"id": row[0], "gname": row[1], "created_at": row[2], "updated_at": row[3]}
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            pg_put_conn(conn)

def get_group_attachments(gname: str) -> Dict[str, Any]:
    """Get videos, advertisements and devices attached to a group."""
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            # Get group ID
            cur.execute('SELECT id FROM public."group" WHERE gname = %s ORDER BY id DESC LIMIT 1;', (gname,))
            row = cur.fetchone()
            if not row:
                return {"gid": None, "videos": [], "advertisements": [], "devices": [], "video_count": 0, "advertisement_count": 0, "device_count": 0}
            gid = row[0]
            
            # Get videos from group_video table
            cur.execute("""
                SELECT DISTINCT v.id, v.video_name
                FROM public.group_video gv
                JOIN public.video v ON v.id = gv.vid
                WHERE gv.gid = %s
                ORDER BY v.video_name;
            """, (gid,))
            videos = [{"id": r[0], "video_name": r[1]} for r in cur.fetchall()]
            
            # Get advertisements from group_advertisement table
            cur.execute("""
                SELECT DISTINCT a.id, a.ad_name, a.s3_link, a.rotation, a.fit_mode, a.display_duration
                FROM public.group_advertisement ga
                JOIN public.advertisement a ON a.id = ga.aid
                WHERE ga.gid = %s
                ORDER BY a.ad_name;
            """, (gid,))
            advertisements = [{"id": r[0], "ad_name": r[1], "s3_link": r[2], "rotation": r[3], "fit_mode": r[4], "display_duration": r[5]} for r in cur.fetchall()]
            
            # Get devices from device_assignment table
            cur.execute("""
                SELECT d.id, d.mobile_id, d.device_name
                FROM public.device_assignment da
                JOIN public.device d ON d.id = da.did
                WHERE da.gid = %s
                ORDER BY d.device_name, d.mobile_id;
            """, (gid,))
            devices = [{"id": r[0], "mobile_id": r[1], "device_name": r[2]} for r in cur.fetchall()]
            
            # Also check device_video_shop_group for legacy device links
            cur.execute("""
                SELECT DISTINCT d.id, d.mobile_id, d.device_name
                FROM public.device_video_shop_group l
                JOIN public.device d ON d.id = l.did
                WHERE l.gid = %s
                ORDER BY d.device_name, d.mobile_id;
            """, (gid,))
            legacy_devices = [{"id": r[0], "mobile_id": r[1], "device_name": r[2]} for r in cur.fetchall()]
            
            # Merge devices (dedup by id)
            device_ids = {d["id"] for d in devices}
            for ld in legacy_devices:
                if ld["id"] not in device_ids:
                    devices.append(ld)
            
            return {
                "gid": gid,
                "videos": videos,
                "advertisements": advertisements,
                "devices": devices,
                "video_count": len(videos),
                "advertisement_count": len(advertisements),
                "device_count": len(devices)
            }
    except Exception as e:
        print(f"Error getting group attachments: {e}")
        return {"gid": None, "videos": [], "advertisements": [], "devices": [], "video_count": 0, "advertisement_count": 0, "device_count": 0}
    finally:
        if conn:
            pg_put_conn(conn)


def unassign_devices_from_group(gname: str) -> int:
    """Remove all device assignments from a group."""
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            # Get group ID
            cur.execute('SELECT id FROM public."group" WHERE gname = %s ORDER BY id DESC LIMIT 1;', (gname,))
            row = cur.fetchone()
            if not row:
                return 0
            gid = row[0]
            
            # Delete from device_assignment
            cur.execute("DELETE FROM public.device_assignment WHERE gid = %s;", (gid,))
            assignment_count = cur.rowcount
            
            # Delete from device_video_shop_group
            cur.execute("DELETE FROM public.device_video_shop_group WHERE gid = %s;", (gid,))
            link_count = cur.rowcount
            
            # Delete from group_video
            cur.execute("DELETE FROM public.group_video WHERE gid = %s;", (gid,))
            
        conn.commit()
        return assignment_count + link_count
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            pg_put_conn(conn)


def delete_by_gname(gname: str, force: bool = False) -> Dict[str, Any]:
    """
    Delete a group by name.
    If force=False and devices are attached, returns error with attachment info.
    If force=True, unassigns all devices first then deletes.
    """
    # Check for attachments first
    attachments = get_group_attachments(gname)
    
    if attachments["device_count"] > 0 and not force:
        return {
            "deleted": False,
            "error": "devices_attached",
            "message": f"Cannot delete group: {attachments['device_count']} device(s) attached",
            "devices": attachments["devices"],
            "videos": attachments["videos"],
            "device_count": attachments["device_count"],
            "video_count": attachments["video_count"]
        }
    
    conn = None
    try:
        conn = pg_get_conn()
        
        # If force, unassign devices first
        if force and attachments["device_count"] > 0:
            unassign_devices_from_group(gname)
        
        with conn.cursor() as cur:
            # Delete group_video entries
            if attachments["gid"]:
                cur.execute("DELETE FROM public.group_video WHERE gid = %s;", (attachments["gid"],))
            
            # Delete the group
            cur.execute(
                """
                DELETE FROM public."group"
                WHERE gname = %s
                RETURNING id;
                """,
                (gname,),
            )
            rows = cur.fetchall()
        conn.commit()
        
        if len(rows) == 0:
            return {"deleted": False, "error": "not_found", "message": "Group not found"}
        
        return {
            "deleted": True,
            "deleted_count": len(rows),
            "devices_unassigned": attachments["device_count"] if force else 0
        }
    except Exception as e:
        if conn:
            conn.rollback()
        return {"deleted": False, "error": "db_error", "message": str(e)}
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
            cur.execute("""
                SELECT EXISTS (
                  SELECT 1
                  FROM information_schema.tables
                  WHERE table_schema='public' AND table_name='group'
                );
            """)
            has_group = cur.fetchone()[0]
        return {"ok": True, "db_user": user, "group_table_exists": has_group}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")
    finally:
        if conn:
            pg_put_conn(conn)

@app.get("/health")
def health():
    return {"status": "ok"}

# ---- Insert
@app.post("/insert_group")
def create_group(req: GroupCreate):
    try:
        new_id = insert_group(req.gname)
        return {"id": new_id, "message": "Group inserted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to insert group: {e}")

# ---- Get one by name
@app.get("/group/{gname}")
def get_group(gname: str = Path(..., description='Exact group name (gname) to fetch')):
    row = fetch_one_by_gname(gname)
    if not row:
        raise HTTPException(status_code=404, detail="Group not found")
    return row

# ---- Bulk list with pagination + optional search
@app.get("/groups")
def list_groups(
    q: Optional[str] = Query(None, description='Search gname (ILIKE %q%)'),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    rows = fetch_groups(q, limit, offset)
    return {"count": len(rows), "items": rows, "limit": limit, "offset": offset, "query": q}

# ---- Update by name (partial; allows renaming gname)
@app.put("/group/{gname}")
def update_group(gname: str, patch: GroupUpdate):
    updated = update_by_gname(gname, patch)
    if not updated:
        raise HTTPException(status_code=404, detail="Group not found or no changes")
    return {"message": "Group updated", "item": updated}

# ---- Delete by name
@app.delete("/group/{gname}")
def delete_group(gname: str, force: bool = Query(False, description="Force delete and unassign all devices")):
    result = delete_by_gname(gname, force=force)
    
    if not result.get("deleted"):
        if result.get("error") == "devices_attached":
            raise HTTPException(
                status_code=409,
                detail={
                    "message": result["message"],
                    "error": "devices_attached",
                    "devices": result["devices"],
                    "videos": result["videos"],
                    "device_count": result["device_count"],
                    "video_count": result["video_count"]
                }
            )
        elif result.get("error") == "not_found":
            raise HTTPException(status_code=404, detail="Group not found")
        else:
            raise HTTPException(status_code=500, detail=result.get("message", "Delete failed"))
    
    return {"deleted_count": result.get("deleted_count", 1), "devices_unassigned": result.get("devices_unassigned", 0)}


# ---- Get group attachments (videos and devices)
@app.get("/group/{gname}/attachments")
def get_group_attachments_route(gname: str):
    """Get videos and devices attached to a group."""
    attachments = get_group_attachments(gname)
    if attachments["gid"] is None:
        raise HTTPException(status_code=404, detail="Group not found")
    return attachments


# ---- Unassign all devices from group
@app.post("/group/{gname}/unassign-devices")
def unassign_devices_route(gname: str):
    """Remove all device assignments from a group."""
    attachments = get_group_attachments(gname)
    if attachments["gid"] is None:
        raise HTTPException(status_code=404, detail="Group not found")
    
    count = unassign_devices_from_group(gname)
    return {"unassigned_count": count, "message": f"Unassigned {count} device(s) from group '{gname}'"}

# Optional: run with `python group_service.py` in dev
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "group_service:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=True,
    )

