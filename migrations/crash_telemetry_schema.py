# migrations/crash_telemetry_schema.py
"""
Fleet crash telemetry — the player's process supervisor (v10.2.0+) reports
crashes through the heartbeat so a crash-looping screen/cohort is visible in
the dashboard within a minute (the abort signal for staged rollouts).

Additive + idempotent, rolling-deploy safe.
"""


def ensure_crash_telemetry_schema(conn):
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE public.device ADD COLUMN IF NOT EXISTS crash_count_total INT NOT NULL DEFAULT 0;")
        cur.execute("ALTER TABLE public.device ADD COLUMN IF NOT EXISTS last_crash_at TIMESTAMPTZ;")
        cur.execute("ALTER TABLE public.device ADD COLUMN IF NOT EXISTS last_crash_msg TEXT;")
    conn.commit()
