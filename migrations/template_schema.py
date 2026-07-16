# migrations/template_schema.py
"""
Screen Templates schema — platform-designed screen layouts linked to companies.

Fully additive and idempotent (safe on every startup, safe during rolling
deploys: old tasks never reference these tables/columns).

Tables:
  screen_template          — platform-owned layout definitions (zones JSONB)
  screen_template_version  — immutable snapshots created on every publish
  template_zone_content    — tenant-scoped content for zones bound to 'content'
                             (scope: company | shop | device, device overrides shop)
  template_zone_group_content — group-scoped content (own table; screen > group
                             > location > company at resolve time)

Columns added:
  company.template_id / company.template_linked_at — the company→template link
  device.app_version — player APK/webapp version reported by the heartbeat
"""


def ensure_template_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.screen_template (
                id            BIGSERIAL PRIMARY KEY,
                name          TEXT NOT NULL,
                description   TEXT,
                orientation   VARCHAR(10) NOT NULL DEFAULT 'landscape',
                design_width  INT NOT NULL DEFAULT 1920,
                design_height INT NOT NULL DEFAULT 1080,
                zones         JSONB NOT NULL DEFAULT '[]'::jsonb,
                status        VARCHAR(10) NOT NULL DEFAULT 'draft',
                version       INT NOT NULL DEFAULT 0,
                published_at  TIMESTAMPTZ,
                created_by    BIGINT,
                created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        # owner_tenant_id: NULL = platform-owned (shared, edited in the platform
        # designer). Non-NULL = a company-private copy a company admin forked and
        # edits in the company designer. Company writes are ALWAYS scoped to a row
        # they own, so one tenant can never mutate another tenant's (or the shared
        # platform) template.
        cur.execute("""
            ALTER TABLE public.screen_template
            ADD COLUMN IF NOT EXISTS owner_tenant_id BIGINT
                REFERENCES public.company(id) ON DELETE CASCADE;
        """)
        # PARTIAL UNIQUE: at most one company-private template per tenant. This is
        # what makes the fork-on-write path safe under concurrency — two racing
        # first-writes can't create two forks (the second INSERT conflicts and the
        # code re-reads the winner). NULL (platform-owned) rows are unconstrained.
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_screen_template_owner
            ON public.screen_template (owner_tenant_id)
            WHERE owner_tenant_id IS NOT NULL;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.screen_template_version (
                id            BIGSERIAL PRIMARY KEY,
                template_id   BIGINT NOT NULL REFERENCES public.screen_template(id) ON DELETE CASCADE,
                version       INT NOT NULL,
                orientation   VARCHAR(10),
                design_width  INT,
                design_height INT,
                zones         JSONB NOT NULL,
                published_by  BIGINT,
                published_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (template_id, version)
            );
        """)
        cur.execute("""
            ALTER TABLE public.company
            ADD COLUMN IF NOT EXISTS template_id BIGINT REFERENCES public.screen_template(id) ON DELETE SET NULL;
        """)
        cur.execute("""
            ALTER TABLE public.company
            ADD COLUMN IF NOT EXISTS template_linked_at TIMESTAMPTZ;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.template_zone_content (
                id         BIGSERIAL PRIMARY KEY,
                tenant_id  BIGINT NOT NULL REFERENCES public.company(id) ON DELETE CASCADE,
                zone_key   TEXT NOT NULL,
                scope      VARCHAR(10) NOT NULL,
                shop_id    BIGINT REFERENCES public.shop(id) ON DELETE CASCADE,
                device_id  BIGINT REFERENCES public.device(id) ON DELETE CASCADE,
                payload    JSONB NOT NULL DEFAULT '{}'::jsonb,
                updated_by BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT tzc_scope_check CHECK (
                    (scope = 'company' AND shop_id IS NULL AND device_id IS NULL) OR
                    (scope = 'shop'    AND shop_id IS NOT NULL AND device_id IS NULL) OR
                    (scope = 'device'  AND device_id IS NOT NULL AND shop_id IS NULL)
                )
            );
        """)
        # One content row per (tenant, zone, scope, target). COALESCE lets the
        # unique index cover NULL shop/device ids.
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_tzc_unique
            ON public.template_zone_content
               (tenant_id, zone_key, scope, COALESCE(shop_id, 0), COALESCE(device_id, 0));
        """)
        # Heartbeat change-detection reads MAX(updated_at) per tenant.
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tzc_tenant_updated
            ON public.template_zone_content (tenant_id, updated_at);
        """)
        cur.execute("""
            ALTER TABLE public.device
            ADD COLUMN IF NOT EXISTS app_version TEXT;
        """)
        # Group-scoped zone content lives in its OWN table (not a 4th scope in
        # template_zone_content) on purpose: adding group_id to that table would
        # force a widened unique index, and old tasks still running during a
        # rolling deploy do ON CONFLICT on the exact 5-column signature — a
        # reshaped index would break them. A separate additive table keeps the
        # existing table/index byte-for-byte unchanged, so old tasks are safe.
        # Precedence at resolve time: screen > group > location > company.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.template_zone_group_content (
                id         BIGSERIAL PRIMARY KEY,
                tenant_id  BIGINT NOT NULL REFERENCES public.company(id) ON DELETE CASCADE,
                zone_key   TEXT NOT NULL,
                group_id   BIGINT NOT NULL REFERENCES public."group"(id) ON DELETE CASCADE,
                payload    JSONB NOT NULL DEFAULT '{}'::jsonb,
                updated_by BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        # One content row per (tenant, zone, group).
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_tzgc_unique
            ON public.template_zone_group_content (tenant_id, zone_key, group_id);
        """)
        # Heartbeat change-detection reads MAX(updated_at) per tenant.
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tzgc_tenant_updated
            ON public.template_zone_group_content (tenant_id, updated_at);
        """)
    conn.commit()
