# migrations/multi_tenant.py
"""
Multi-tenant migration: adds company table and company_id FK to all entity tables.
Designed to be fully idempotent — safe to run multiple times even if
the company table was partially created in a previous run.
Existing data gets company_id = NULL (super-admin scope).
"""


def ensure_multi_tenant_schema(conn):
    """
    Add multi-tenant (company) isolation to the DIGIX CMS schema.
    Must be called AFTER ensure_dvsg_schema().
    """

    # ── 1. Company table (minimal CREATE, then ALTER for extra cols) ──
    ddl_company_table = """
    CREATE TABLE IF NOT EXISTS public.company (
        id          BIGSERIAL PRIMARY KEY,
        name        TEXT NOT NULL,
        slug        TEXT NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    # Idempotent column adds + unique constraints
    ddl_company_cols = """
    DO $$
    DECLARE t text;
    BEGIN
      -- is_active
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='company' AND column_name='is_active';
      IF NOT FOUND THEN
        ALTER TABLE public.company ADD COLUMN is_active BOOLEAN NOT NULL DEFAULT TRUE;
      END IF;

      -- unique on name (idempotent)
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'company_name_key'
      ) THEN
        BEGIN
          ALTER TABLE public.company ADD CONSTRAINT company_name_key UNIQUE (name);
        EXCEPTION
          WHEN duplicate_table THEN NULL;
          WHEN duplicate_object THEN NULL;
          WHEN unique_violation THEN NULL;
        END;
      END IF;

      -- unique on slug (idempotent)
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'company_slug_key'
      ) THEN
        BEGIN
          ALTER TABLE public.company ADD CONSTRAINT company_slug_key UNIQUE (slug);
        EXCEPTION
          WHEN duplicate_table THEN NULL;
          WHEN duplicate_object THEN NULL;
          WHEN unique_violation THEN NULL;
        END;
      END IF;
    END $$;
    """

    ddl_company_indexes = """
    CREATE INDEX IF NOT EXISTS idx_company_slug ON public.company(slug);
    CREATE INDEX IF NOT EXISTS idx_company_is_active ON public.company(is_active);
    """

    ddl_company_trigger = """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_company_updated_at') THEN
            CREATE TRIGGER trg_company_updated_at
            BEFORE UPDATE ON public.company
            FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
        END IF;
    END $$;
    """

    # ── 2. Add company_id to entity tables ────────────────────────────
    ddl_add_company_id = """
    DO $$
    DECLARE t text;
    BEGIN
      -- users.company_id
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='users' AND column_name='company_id';
      IF NOT FOUND THEN
        ALTER TABLE public.users ADD COLUMN company_id BIGINT REFERENCES public.company(id) ON DELETE SET NULL;
      END IF;
      CREATE INDEX IF NOT EXISTS idx_users_company_id ON public.users(company_id);

      -- device.company_id
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='company_id';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN company_id BIGINT REFERENCES public.company(id) ON DELETE SET NULL;
      END IF;
      CREATE INDEX IF NOT EXISTS idx_device_company_id ON public.device(company_id);

      -- video.company_id
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='video' AND column_name='company_id';
      IF NOT FOUND THEN
        ALTER TABLE public.video ADD COLUMN company_id BIGINT REFERENCES public.company(id) ON DELETE SET NULL;
      END IF;
      CREATE INDEX IF NOT EXISTS idx_video_company_id ON public.video(company_id);

      -- shop.company_id
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='shop' AND column_name='company_id';
      IF NOT FOUND THEN
        ALTER TABLE public.shop ADD COLUMN company_id BIGINT REFERENCES public.company(id) ON DELETE SET NULL;
      END IF;
      CREATE INDEX IF NOT EXISTS idx_shop_company_id ON public.shop(company_id);

      -- group.company_id
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='group' AND column_name='company_id';
      IF NOT FOUND THEN
        ALTER TABLE public."group" ADD COLUMN company_id BIGINT REFERENCES public.company(id) ON DELETE SET NULL;
      END IF;
      CREATE INDEX IF NOT EXISTS idx_group_company_id ON public."group"(company_id);

      -- advertisement.company_id
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='advertisement' AND column_name='company_id';
      IF NOT FOUND THEN
        ALTER TABLE public.advertisement ADD COLUMN company_id BIGINT REFERENCES public.company(id) ON DELETE SET NULL;
      END IF;
      CREATE INDEX IF NOT EXISTS idx_advertisement_company_id ON public.advertisement(company_id);

    END $$;
    """

    # ── 3. Update unique constraints to be company-scoped ─────────────
    ddl_company_scoped_unique = """
    DO $$
    BEGIN
      -- video: drop old global unique, add company-scoped
      IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'video_video_name_key'
                 AND conrelid = 'public.video'::regclass) THEN
        ALTER TABLE public.video DROP CONSTRAINT video_video_name_key;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_video_company_name') THEN
        CREATE UNIQUE INDEX idx_video_company_name ON public.video(company_id, video_name) WHERE company_id IS NOT NULL;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_video_null_company_name') THEN
        CREATE UNIQUE INDEX idx_video_null_company_name ON public.video(video_name) WHERE company_id IS NULL;
      END IF;

      -- shop
      IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'shop_shop_name_key'
                 AND conrelid = 'public.shop'::regclass) THEN
        ALTER TABLE public.shop DROP CONSTRAINT shop_shop_name_key;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_shop_company_name') THEN
        CREATE UNIQUE INDEX idx_shop_company_name ON public.shop(company_id, shop_name) WHERE company_id IS NOT NULL;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_shop_null_company_name') THEN
        CREATE UNIQUE INDEX idx_shop_null_company_name ON public.shop(shop_name) WHERE company_id IS NULL;
      END IF;

      -- group
      IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'group_gname_key'
                 AND conrelid = 'public."group"'::regclass) THEN
        ALTER TABLE public."group" DROP CONSTRAINT group_gname_key;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_group_company_gname') THEN
        CREATE UNIQUE INDEX idx_group_company_gname ON public."group"(company_id, gname) WHERE company_id IS NOT NULL;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_group_null_company_gname') THEN
        CREATE UNIQUE INDEX idx_group_null_company_gname ON public."group"(gname) WHERE company_id IS NULL;
      END IF;

      -- advertisement
      IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'advertisement_ad_name_key'
                 AND conrelid = 'public.advertisement'::regclass) THEN
        ALTER TABLE public.advertisement DROP CONSTRAINT advertisement_ad_name_key;
      END IF;
      IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'uq_advertisement_ad_name') THEN
        DROP INDEX public.uq_advertisement_ad_name;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_advertisement_company_adname') THEN
        CREATE UNIQUE INDEX idx_advertisement_company_adname ON public.advertisement(company_id, ad_name) WHERE company_id IS NOT NULL;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_advertisement_null_company_adname') THEN
        CREATE UNIQUE INDEX idx_advertisement_null_company_adname ON public.advertisement(ad_name) WHERE company_id IS NULL;
      END IF;
    END $$;
    """

    # ── 4. Add is_super_admin flag to users ───────────────────────────
    ddl_super_admin = """
    DO $$
    DECLARE t text;
    BEGIN
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='users' AND column_name='is_super_admin';
      IF NOT FOUND THEN
        ALTER TABLE public.users ADD COLUMN is_super_admin BOOLEAN NOT NULL DEFAULT FALSE;
        UPDATE public.users SET is_super_admin = TRUE WHERE username = 'admin';
      END IF;
    END $$;
    """

    # ── Execute all (order matters) ───────────────────────────────────
    with conn.cursor() as cur:
        # 1. Create company table first (base structure)
        cur.execute(ddl_company_table)
        # 2. Add missing columns to company table
        cur.execute(ddl_company_cols)
        # 3. Indexes (safe now that is_active exists)
        cur.execute(ddl_company_indexes)
        # 4. Trigger
        cur.execute(ddl_company_trigger)
        # 5. Add company_id FK to all entity tables
        cur.execute(ddl_add_company_id)
        # 6. Switch unique constraints to company-scoped
        cur.execute(ddl_company_scoped_unique)
        # 7. Super admin flag
        cur.execute(ddl_super_admin)

    conn.commit()
