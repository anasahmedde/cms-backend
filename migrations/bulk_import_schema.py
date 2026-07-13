# migrations/bulk_import_schema.py
"""
Bulk device-enrollment jobs. Additive + idempotent.

A validate call stores the normalized+validated rows (payload) with status
'validated'; commit reads that payload, creates shops/groups/devices, and writes
a result report. Pending devices (rows with no mobile_id) are created with a
placeholder mobile_id 'pending:<code>' and the code mirrored into the existing
(previously unused) device.activation_code column so the dashboard can list them.
"""


def ensure_bulk_import_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.bulk_import_job (
                id            BIGSERIAL PRIMARY KEY,
                tenant_id     BIGINT NOT NULL REFERENCES public.company(id) ON DELETE CASCADE,
                uploaded_by   BIGINT,
                filename      TEXT,
                status        VARCHAR(16) NOT NULL DEFAULT 'validated',
                total_rows    INT NOT NULL DEFAULT 0,
                created_rows  INT NOT NULL DEFAULT 0,
                skipped_rows  INT NOT NULL DEFAULT 0,
                error_rows    INT NOT NULL DEFAULT 0,
                payload       JSONB NOT NULL DEFAULT '[]'::jsonb,
                report        JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                finished_at   TIMESTAMPTZ
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_bulk_job_tenant
            ON public.bulk_import_job (tenant_id, created_at DESC);
        """)
    conn.commit()
