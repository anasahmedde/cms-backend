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
app = FastAPI(title="Shop Service", version="1.0.0")

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
    Creates public.shop if it doesn't exist, adds updated_at trigger + index on shop_name.
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

    ddl_shop = """
    CREATE TABLE IF NOT EXISTS public.shop (
        id BIGSERIAL PRIMARY KEY,
        shop_name TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    ddl_shop_trigger = """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger WHERE tgname = 'trg_shop_set_updated_at'
        ) THEN
            CREATE TRIGGER trg_shop_set_updated_at
            BEFORE UPDATE ON public.shop
            FOR EACH ROW
            EXECUTE FUNCTION public.set_updated_at();
        END IF;
    END;
    $$;
    """

    ddl_shop_index = """
    CREATE INDEX IF NOT EXISTS idx_shop_shop_name
    ON public.shop (shop_name);
    """

    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute(ddl_updated_at_fn)
            cur.execute(ddl_shop)
            cur.execute(ddl_shop_trigger)
            cur.execute(ddl_shop_index)
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
class ShopCreate(BaseModel):
    shop_name: str

class ShopUpdate(BaseModel):
    shop_name: Optional[str] = None  # allow renaming

# =========================
# Data access helpers
# =========================
def insert_shop(shop_name: str) -> int:
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.shop (shop_name)
                VALUES (%s)
                RETURNING id;
                """,
                (shop_name,),
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

def fetch_one_by_name(shop_name: str) -> Optional[Dict[str, Any]]:
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, shop_name, created_at, updated_at
                FROM public.shop
                WHERE shop_name = %s
                ORDER BY id DESC
                LIMIT 1;
                """,
                (shop_name,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "shop_name": row[1],
                "created_at": row[2],
                "updated_at": row[3],
            }
    finally:
        if conn:
            pg_put_conn(conn)

def fetch_shops(q: Optional[str], limit: int, offset: int) -> List[Dict[str, Any]]:
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            if q:
                cur.execute(
                    """
                    SELECT id, shop_name, created_at, updated_at
                    FROM public.shop
                    WHERE shop_name ILIKE %s
                    ORDER BY id DESC
                    LIMIT %s OFFSET %s;
                    """,
                    (f"%{q}%", limit, offset),
                )
            else:
                cur.execute(
                    """
                    SELECT id, shop_name, created_at, updated_at
                    FROM public.shop
                    ORDER BY id DESC
                    LIMIT %s OFFSET %s;
                    """,
                    (limit, offset),
                )
            rows = cur.fetchall()
            return [
                {"id": r[0], "shop_name": r[1], "created_at": r[2], "updated_at": r[3]}
                for r in rows
            ]
    finally:
        if conn:
            pg_put_conn(conn)

def update_by_name(current_name: str, patch: ShopUpdate) -> Optional[Dict[str, Any]]:
    sets = []
    params: List[Any] = []

    if patch.shop_name is not None:
        sets.append("shop_name = %s")
        params.append(patch.shop_name)

    if not sets:
        return fetch_one_by_name(current_name)

    params.append(current_name)

    sql = f"""
        UPDATE public.shop
        SET {", ".join(sets)}
        WHERE shop_name = %s
        RETURNING id, shop_name, created_at, updated_at;
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
        return {"id": row[0], "shop_name": row[1], "created_at": row[2], "updated_at": row[3]}
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            pg_put_conn(conn)

def delete_by_name(shop_name: str) -> int:
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM public.shop
                WHERE shop_name = %s
                RETURNING id;
                """,
                (shop_name,),
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
            cur.execute("""
                SELECT EXISTS (
                  SELECT 1
                  FROM information_schema.tables
                  WHERE table_schema='public' AND table_name='shop'
                );
            """)
            has_shop = cur.fetchone()[0]
        return {"ok": True, "db_user": user, "shop_table_exists": has_shop}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")
    finally:
        if conn:
            pg_put_conn(conn)

@app.get("/health")
def health():
    return {"status": "ok"}

# ---- Insert
@app.post("/insert_shop")
def create_shop(req: ShopCreate):
    try:
        new_id = insert_shop(req.shop_name)
        return {"id": new_id, "message": "Shop inserted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to insert shop: {e}")

# ---- Get one by name
@app.get("/shop/{shop_name}")
def get_shop(shop_name: str = Path(..., description="Exact shop_name to fetch")):
    row = fetch_one_by_name(shop_name)
    if not row:
        raise HTTPException(status_code=404, detail="Shop not found")
    return row

# ---- Bulk list with pagination + optional search
@app.get("/shops")
def list_shops(
    q: Optional[str] = Query(None, description="Search shop_name (ILIKE %q%)"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    rows = fetch_shops(q, limit, offset)
    return {"count": len(rows), "items": rows, "limit": limit, "offset": offset, "query": q}

# ---- Update by name (partial; allows renaming shop_name)
@app.put("/shop/{shop_name}")
def update_shop(shop_name: str, patch: ShopUpdate):
    updated = update_by_name(shop_name, patch)
    if not updated:
        raise HTTPException(status_code=404, detail="Shop not found or no changes")
    return {"message": "Shop updated", "item": updated}

# ---- Delete by name
@app.delete("/shop/{shop_name}")
def delete_shop(shop_name: str):
    deleted = delete_by_name(shop_name)
    if deleted == 0:
        raise HTTPException(status_code=404, detail="Shop not found")
    return {"deleted_count": deleted}

# Optional: run with `python shop_service.py` in dev
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "shop_service:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=True,
    )

