# migrations/auth_session_schema.py
"""
Durable auth sessions. Additive + idempotent.

Sessions used to live only in the process dict (tenant_context.active_sessions),
so every ECS deploy/restart logged every user out, and >1 replica meant a 401
lottery. Sessions are now written through to this table (token stored as a
SHA-256 hash, never raw) and lazily rehydrated into process memory on a miss.

Table:
  auth_session(token_hash PK, session JSONB, expires_at, created_at, updated_at)
"""


def ensure_auth_session_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.auth_session (
                token_hash TEXT PRIMARY KEY,
                session JSONB NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_auth_session_expires
            ON public.auth_session (expires_at);
        """)
    conn.commit()
