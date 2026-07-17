# migrations/reported_resolution_schema.py
"""
Device-reported screen resolution provenance. Additive + idempotent.

device.resolution stays the admin-effective value (the heartbeat and the
enrollment poll only backfill it when NULL — unchanged). These columns record
what the hardware actually reports, updated on EVERY heartbeat/enrollment poll
that carries a resolution, so the dashboard can show "detected vs configured"
drift without ever overwriting admin intent.

Columns added:
  device.reported_resolution    — last resolution self-reported by the player
  device.resolution_reported_at — when it was last reported
"""


def ensure_reported_resolution_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE public.device
            ADD COLUMN IF NOT EXISTS reported_resolution TEXT;
        """)
        cur.execute("""
            ALTER TABLE public.device
            ADD COLUMN IF NOT EXISTS resolution_reported_at TIMESTAMPTZ;
        """)
    conn.commit()
