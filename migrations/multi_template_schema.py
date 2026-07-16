# migrations/multi_template_schema.py
"""
Multi-template — a company can use several screen templates at once
(mixed-resolution fleets: landscape TVs + portrait totems).

Fully additive and idempotent (safe on every startup and during rolling
deploys: old tasks never reference these columns/tables).

Model:
  company.template_id             — stays: the company's DEFAULT template
  public.company_template         — the SET of extra templates linked to a company
  "group".template_id             — group-level template override (nullable)
  device.template_id              — screen-level template override (nullable)

Resolution (mirrors content precedence): screen > group > company default.
No FK constraints: a dangling id simply falls through to the next level at
resolve time, which keeps deletes/unlinks safe with old+new tasks running.
"""


def ensure_multi_template_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.company_template (
                company_id  BIGINT NOT NULL,
                template_id BIGINT NOT NULL,
                linked_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (company_id, template_id)
            );
        """)
        cur.execute('ALTER TABLE public."group" ADD COLUMN IF NOT EXISTS template_id BIGINT;')
        cur.execute("ALTER TABLE public.device ADD COLUMN IF NOT EXISTS template_id BIGINT;")
    conn.commit()
