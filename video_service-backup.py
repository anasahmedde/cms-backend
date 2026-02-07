# video_service.py
# Updated with rotation, fit_mode, content_type, display_duration support
# + FIX: add /video/{video_name}/presign and include presigned_url in /video/{video_name}
# Run: uvicorn video_service:app --host 0.0.0.0 --port 8003 --reload

import os
import re
from pathlib import Path
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Query, Path as FPath, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import BotoCoreError, ClientError

load_dotenv()

# --- S3 ---
S3_BUCKET = os.getenv("S3_BUCKET")
if not S3_BUCKET:
    raise RuntimeError("S3_BUCKET env var is required")

S3_PREFIX = os.getenv("S3_PREFIX", "myvideos").strip("/")
AWS_REGION = os.getenv("AWS_REGION")
FORCED_EXT = ".mp4"

# NEW: presign expiry (seconds)
PRESIGN_EXPIRES = int(os.getenv("PRESIGN_EXPIRES", "3600"))

TRANSFER_CFG = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,
    multipart_chunksize=16 * 1024 * 1024,
    max_concurrency=8,
    use_threads=True,
)

# --- Postgres ---
DB_HOST = os.getenv("PGHOST", "172.31.17.177")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_NAME = os.getenv("PGDATABASE", "dgx")
DB_USER = os.getenv("PGUSER", "app_user")
DB_PASS = os.getenv("PGPASSWORD", "strongpassword")
DB_MIN_CONN = int(os.getenv("PG_MIN_CONN", "1"))
DB_MAX_CONN = int(os.getenv("PG_MAX_CONN", "5"))

app = FastAPI(title="Video Service", version="2.0.1")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- S3 helpers ---
def get_s3_client():
    kw = {}
    if AWS_REGION:
        kw["region_name"] = AWS_REGION
    return boto3.client("s3", **kw)

def _slug(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^\w\-.]+", "-", s, flags=re.UNICODE)
    return re.sub(r"-{2,}", "-", s).strip("-").lower()

def _make_s3_key(video_name: str) -> str:
    base = _slug(video_name)
    filename = f"{base}{FORCED_EXT}"
    return f"{S3_PREFIX}/{filename}".strip("/") if S3_PREFIX else filename

def _to_s3_uri(key: str) -> str:
    return f"s3://{S3_BUCKET}/{key}"

def _detect_content_type(filename: str) -> str:
    ext = Path(filename).suffix.lower()
    video_exts = {'.mp4', '.webm', '.mov', '.avi', '.mkv'}
    image_exts = {'.jpg', '.jpeg', '.png', '.gif', '.webp'}
    if ext in video_exts:
        return "video"
    elif ext in image_exts:
        return "image"
    elif ext in {'.html', '.htm'}:
        return "html"
    elif ext == '.pdf':
        return "pdf"
    return "video"

def s3_key_exists(key: str, bucket: Optional[str] = None) -> bool:
    s3 = get_s3_client()
    b = bucket or S3_BUCKET
    try:
        s3.head_object(Bucket=b, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise

def s3_upload_fileobj(fileobj, key: str, content_type: Optional[str] = None):
    s3 = get_s3_client()
    extra = {"ACL": "private", "ServerSideEncryption": "AES256"}
    if content_type:
        extra["ContentType"] = content_type
    s3.upload_fileobj(fileobj, S3_BUCKET, key, ExtraArgs=extra, Config=TRANSFER_CFG)

# NEW: parse stored s3_link into (bucket, key)
def _parse_s3_link(s3_link: str) -> (str, str):
    """
    Supports:
      - s3://bucket/key
      - https://bucket.s3.region.amazonaws.com/key
      - https://s3.region.amazonaws.com/bucket/key
      - raw key: myvideos/test9.mp4   (assumes S3_BUCKET)
    """
    s3_link = (s3_link or "").strip()
    if not s3_link:
        return (S3_BUCKET, "")

    if s3_link.startswith("s3://"):
        # s3://bucket/key...
        rest = s3_link[len("s3://"):]
        parts = rest.split("/", 1)
        if len(parts) == 1:
            return (parts[0], "")
        return (parts[0], parts[1])

    if s3_link.startswith("http://") or s3_link.startswith("https://"):
        u = urlparse(s3_link)
        host = u.netloc
        path = (u.path or "").lstrip("/")

        # virtual-hosted: bucket.s3...
        if ".s3" in host:
            bucket = host.split(".s3", 1)[0]
            return (bucket, path)

        # path-style: s3.../bucket/key
        # if first path segment looks like bucket
        p = path.split("/", 1)
        if len(p) == 2 and p[0]:
            return (p[0], p[1])

        return (S3_BUCKET, path)

    # raw key
    return (S3_BUCKET, s3_link)

# NEW: presign helper
def presign_get_object(bucket: str, key: str, expires: int = PRESIGN_EXPIRES) -> str:
    s3 = get_s3_client()
    try:
        return s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Presign failed: {e}")

# --- PG Pool ---
pg_pool: Optional[pool.SimpleConnectionPool] = None

def pg_init_pool():
    global pg_pool
    if pg_pool is None:
        pg_pool = psycopg2.pool.SimpleConnectionPool(
            DB_MIN_CONN, DB_MAX_CONN,
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS, connect_timeout=5,
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

# --- Schema ---
def ensure_schema():
    ddl = """
    CREATE TABLE IF NOT EXISTS public.video (
        id BIGSERIAL PRIMARY KEY,
        video_name TEXT NOT NULL,
        s3_link TEXT NOT NULL,
        rotation INT NOT NULL DEFAULT 0,
        content_type TEXT NOT NULL DEFAULT 'video',
        fit_mode TEXT NOT NULL DEFAULT 'cover',
        display_duration INT NOT NULL DEFAULT 10,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE UNIQUE INDEX IF NOT EXISTS uq_video_video_name ON public.video (video_name);
    DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_name='video' AND column_name='rotation') THEN
            ALTER TABLE public.video ADD COLUMN rotation INT NOT NULL DEFAULT 0;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_name='video' AND column_name='content_type') THEN
            ALTER TABLE public.video ADD COLUMN content_type TEXT NOT NULL DEFAULT 'video';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_name='video' AND column_name='fit_mode') THEN
            ALTER TABLE public.video ADD COLUMN fit_mode TEXT NOT NULL DEFAULT 'cover';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                       WHERE table_name='video' AND column_name='display_duration') THEN
            ALTER TABLE public.video ADD COLUMN display_duration INT NOT NULL DEFAULT 10;
        END IF;
    END; $$;
    """
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            pg_put_conn(conn)

# --- Pydantic models ---
class VideoUpdate(BaseModel):
    video_name: Optional[str] = None
    s3_link: Optional[str] = None
    rotation: Optional[int] = None
    content_type: Optional[str] = None
    fit_mode: Optional[str] = None
    display_duration: Optional[int] = None

class RotationUpdate(BaseModel):
    rotation: int

class FitModeUpdate(BaseModel):
    fit_mode: str

# --- Data access ---
def upsert_video(video_name, s3_link, rotation=0, content_type="video", fit_mode="cover", display_duration=10):
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO public.video (video_name, s3_link, rotation, content_type, fit_mode, display_duration)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (video_name)
                DO UPDATE SET s3_link = EXCLUDED.s3_link, rotation = EXCLUDED.rotation,
                    content_type = EXCLUDED.content_type, fit_mode = EXCLUDED.fit_mode,
                    display_duration = EXCLUDED.display_duration, updated_at = NOW()
                RETURNING id;
            """, (video_name, s3_link, rotation, content_type, fit_mode, display_duration))
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

def fetch_one(video_name):
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, video_name, s3_link, rotation, content_type, fit_mode, display_duration, created_at, updated_at
                FROM public.video WHERE video_name = %s LIMIT 1;
            """, (video_name,))
            row = cur.fetchone()
            if not row:
                return None
            return {"id": row[0], "video_name": row[1], "s3_link": row[2], "rotation": row[3],
                    "content_type": row[4], "fit_mode": row[5], "display_duration": row[6],
                    "created_at": row[7], "updated_at": row[8]}
    finally:
        if conn:
            pg_put_conn(conn)

def fetch_videos(q, limit, offset):
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            sql = """SELECT id, video_name, s3_link, rotation, content_type, fit_mode, display_duration, created_at, updated_at
                     FROM public.video"""
            if q:
                cur.execute(sql + " WHERE video_name ILIKE %s ORDER BY id DESC LIMIT %s OFFSET %s", (f"%{q}%", limit, offset))
            else:
                cur.execute(sql + " ORDER BY id DESC LIMIT %s OFFSET %s", (limit, offset))
            return [{"id": r[0], "video_name": r[1], "s3_link": r[2], "rotation": r[3],
                     "content_type": r[4], "fit_mode": r[5], "display_duration": r[6],
                     "created_at": r[7], "updated_at": r[8]} for r in cur.fetchall()]
    finally:
        if conn:
            pg_put_conn(conn)

def update_video(name, patch):
    sets, params = [], []
    if patch.video_name is not None:
        sets.append("video_name = %s"); params.append(patch.video_name)
    if patch.s3_link is not None:
        sets.append("s3_link = %s"); params.append(patch.s3_link)
    if patch.rotation is not None:
        sets.append("rotation = %s"); params.append(patch.rotation)
    if patch.content_type is not None:
        sets.append("content_type = %s"); params.append(patch.content_type)
    if patch.fit_mode is not None:
        sets.append("fit_mode = %s"); params.append(patch.fit_mode)
    if patch.display_duration is not None:
        sets.append("display_duration = %s"); params.append(patch.display_duration)
    if not sets:
        return fetch_one(name)
    params.append(name)
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute(f"UPDATE public.video SET {', '.join(sets)}, updated_at = NOW() WHERE video_name = %s RETURNING *;", params)
            row = cur.fetchone()
        conn.commit()
        return {"id": row[0], "video_name": row[1], "s3_link": row[2], "rotation": row[3],
                "content_type": row[4], "fit_mode": row[5], "display_duration": row[6],
                "created_at": row[7], "updated_at": row[8]} if row else None
    except:
        if conn: conn.rollback()
        raise
    finally:
        if conn: pg_put_conn(conn)

def update_rotation(name, rotation):
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute("UPDATE public.video SET rotation = %s, updated_at = NOW() WHERE video_name = %s RETURNING *;", (rotation, name))
            row = cur.fetchone()
        conn.commit()
        return {"id": row[0], "video_name": row[1], "s3_link": row[2], "rotation": row[3],
                "content_type": row[4], "fit_mode": row[5], "display_duration": row[6],
                "created_at": row[7], "updated_at": row[8]} if row else None
    except:
        if conn: conn.rollback()
        raise
    finally:
        if conn: pg_put_conn(conn)

def update_fit_mode(name, fit_mode):
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute("UPDATE public.video SET fit_mode = %s, updated_at = NOW() WHERE video_name = %s RETURNING *;", (fit_mode, name))
            row = cur.fetchone()
        conn.commit()
        return {"id": row[0], "video_name": row[1], "s3_link": row[2], "rotation": row[3],
                "content_type": row[4], "fit_mode": row[5], "display_duration": row[6],
                "created_at": row[7], "updated_at": row[8]} if row else None
    except:
        if conn: conn.rollback()
        raise
    finally:
        if conn: pg_put_conn(conn)

def delete_video(name):
    conn = None
    try:
        conn = pg_get_conn()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM public.video WHERE video_name = %s RETURNING id;", (name,))
            rows = cur.fetchall()
        conn.commit()
        return len(rows)
    except:
        if conn: conn.rollback()
        raise
    finally:
        if conn: pg_put_conn(conn)

# --- Startup/Shutdown ---
@app.on_event("startup")
def on_startup():
    pg_init_pool()
    ensure_schema()

@app.on_event("shutdown")
def on_shutdown():
    pg_close_pool()

# --- Routes ---
@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/upload_video")
async def upload_video(
    file: UploadFile = File(...),
    video_name: str = Form(...),
    overwrite: bool = Form(False),
    rotation: int = Form(0),
    fit_mode: str = Form("cover"),
    display_duration: int = Form(10),
):
    try:
        key = _make_s3_key(video_name)
        if not overwrite and s3_key_exists(key):
            raise HTTPException(status_code=409, detail="Video exists. Use overwrite=true.")
        content_type = _detect_content_type(file.filename or video_name)
        s3_upload_fileobj(file.file, key, content_type="video/mp4")
        s3_uri = _to_s3_uri(key)
        new_id = upsert_video(video_name, s3_uri, rotation, content_type, fit_mode, display_duration)
        return {"id": new_id, "video_name": video_name, "s3_link": s3_uri, "key": key,
                "rotation": rotation, "content_type": content_type, "fit_mode": fit_mode,
                "display_duration": display_duration, "overwrote": overwrite}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")

# UPDATED: return DB row + presigned_url (and fall back to S3 if DB row missing)
@app.get("/video/{video_name}")
def get_video(video_name: str, presign: bool = Query(True)):
    row = fetch_one(video_name)

    # If DB has record, presign from its s3_link
    if row:
        bucket, key = _parse_s3_link(row.get("s3_link"))
        if not key:
            # safety fallback
            key = _make_s3_key(video_name)
            bucket = S3_BUCKET

        if not s3_key_exists(key, bucket=bucket):
            raise HTTPException(status_code=404, detail="Video record exists but S3 object not found")

        if presign:
            row["presigned_url"] = presign_get_object(bucket, key, expires=PRESIGN_EXPIRES)
            row["presigned_expires_in"] = PRESIGN_EXPIRES
            row["s3_bucket"] = bucket
            row["s3_key"] = key
        return row

    # DB missing: fallback to conventional key
    key = _make_s3_key(video_name)
    if not s3_key_exists(key, bucket=S3_BUCKET):
        raise HTTPException(status_code=404, detail="Video not found")

    url = presign_get_object(S3_BUCKET, key, expires=PRESIGN_EXPIRES)
    return {
        "id": None,
        "video_name": video_name,
        "s3_link": _to_s3_uri(key),
        "rotation": 0,
        "content_type": "video",
        "fit_mode": "cover",
        "display_duration": 10,
        "created_at": None,
        "updated_at": None,
        "presigned_url": url,
        "presigned_expires_in": PRESIGN_EXPIRES,
        "s3_bucket": S3_BUCKET,
        "s3_key": key,
        "note": "Returned via S3 fallback (no DB row found).",
    }

# NEW: this is what your frontend is calling: /video/{name}/presign
@app.get("/video/{video_name}/presign")
def presign_video(
    video_name: str,
    expires_in: int = Query(PRESIGN_EXPIRES, ge=60, le=7 * 24 * 3600),
):
    row = fetch_one(video_name)

    if row:
        bucket, key = _parse_s3_link(row.get("s3_link"))
        if not key:
            key = _make_s3_key(video_name)
            bucket = S3_BUCKET
    else:
        bucket = S3_BUCKET
        key = _make_s3_key(video_name)

    if not s3_key_exists(key, bucket=bucket):
        raise HTTPException(status_code=404, detail="Video not found")

    url = presign_get_object(bucket, key, expires=expires_in)
    return {"video_name": video_name, "url": url, "expires_in": expires_in, "bucket": bucket, "key": key}

@app.get("/videos")
def list_videos(q: Optional[str] = Query(None), limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0)):
    rows = fetch_videos(q, limit, offset)
    return {"count": len(rows), "items": rows, "limit": limit, "offset": offset, "query": q}

@app.put("/video/{video_name}")
def update_video_route(video_name: str, patch: VideoUpdate):
    updated = update_video(video_name, patch)
    if not updated:
        raise HTTPException(status_code=404, detail="Video not found")
    return {"message": "Updated", "item": updated}

@app.post("/video/{video_name}/rotation")
def set_rotation(video_name: str, body: RotationUpdate):
    if body.rotation not in (0, 90, 180, 270):
        raise HTTPException(status_code=400, detail="Rotation must be 0, 90, 180, or 270")
    updated = update_rotation(video_name, body.rotation)
    if not updated:
        raise HTTPException(status_code=404, detail="Video not found")
    return {"message": "Rotation updated", "item": updated}

@app.post("/video/{video_name}/fit_mode")
def set_fit_mode(video_name: str, body: FitModeUpdate):
    if body.fit_mode not in ("cover", "contain", "fill", "none"):
        raise HTTPException(status_code=400, detail="fit_mode must be cover, contain, fill, or none")
    updated = update_fit_mode(video_name, body.fit_mode)
    if not updated:
        raise HTTPException(status_code=404, detail="Video not found")
    return {"message": "Fit mode updated", "item": updated}

@app.delete("/video/{video_name}")
def delete_video_route(video_name: str):
    deleted = delete_video(video_name)
    if deleted == 0:
        raise HTTPException(status_code=404, detail="Video not found")
    return {"deleted_count": deleted}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("video_service:app", host="0.0.0.0", port=int(os.getenv("PORT", "8003")), reload=True)

