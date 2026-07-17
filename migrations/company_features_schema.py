# migrations/company_features_schema.py
"""
Per-company feature flags. Additive + idempotent.

company.features JSONB — e.g. {"temperature": true, "footfall": false,
"gender": false}. Missing keys mean OFF: hardware-dependent analytics
(BLE temperature probes, reed-switch footfall counters, camera gender
counting) only make sense for companies that installed the peripherals,
so everything defaults to disabled and the dashboard hides those surfaces.
"""


def ensure_company_features_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE public.company
            ADD COLUMN IF NOT EXISTS features JSONB NOT NULL DEFAULT '{}'::jsonb;
        """)
    conn.commit()
