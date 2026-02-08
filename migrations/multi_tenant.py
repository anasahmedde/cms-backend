# migrations/multi_tenant.py
"""
Multi-tenant migration: adds company table and company_id FK to all entity tables.
Designed to be idempotent — safe to run multiple times.
Existing data gets company_id = NULL (super-admin scope).
"""


def ensure_multi_tenant_schema(conn):
    """
    Add multi-tenant (company) isolation to the DIGIX CMS schema.
    Must be called AFTER ensure_dvsg_schema().
    """

    # ── 1. Company table ──────────────────────────────────────────────
    ddl_company_table = """
    CREATE TABLE IF NOT EXISTS public.company (
        id          BIGSERIAL PRIMARY KEY,
        name        TEXT NOT NULL UNIQUE,
        slug        TEXT NOT NULL UNIQUE,
        is_active   BOOLEAN NOT NULL DEFAULT TRUE,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
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
    # Each block is idempotent: checks IF NOT FOUND before ALTER.
    # company_id is NULLABLE — NULL means super-admin / legacy data.

    ddl_add_company_id = """
    DO $$
    DECLARE t text;
    BEGIN
      -- users.company_id
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='users' AND column_name='company_id';
      IF NOT FOUND THEN
        ALTER TABLE public.users ADD COLUMN company_id BIGINT REFERENCES public.company(id) ON DELETE SET NULL;
        CREATE INDEX IF NOT EXISTS idx_users_company_id ON public.users(company_id);
      END IF;

      -- device.company_id
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='company_id';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN company_id BIGINT REFERENCES public.company(id) ON DELETE SET NULL;
        CREATE INDEX IF NOT EXISTS idx_device_company_id ON public.device(company_id);
      END IF;

      -- video.company_id
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='video' AND column_name='company_id';
      IF NOT FOUND THEN
        ALTER TABLE public.video ADD COLUMN company_id BIGINT REFERENCES public.company(id) ON DELETE SET NULL;
        CREATE INDEX IF NOT EXISTS idx_video_company_id ON public.video(company_id);
      END IF;

      -- shop.company_id
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='shop' AND column_name='company_id';
      IF NOT FOUND THEN
        ALTER TABLE public.shop ADD COLUMN company_id BIGINT REFERENCES public.company(id) ON DELETE SET NULL;
        CREATE INDEX IF NOT EXISTS idx_shop_company_id ON public.shop(company_id);
      END IF;

      -- group.company_id
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='group' AND column_name='company_id';
      IF NOT FOUND THEN
        ALTER TABLE public."group" ADD COLUMN company_id BIGINT REFERENCES public.company(id) ON DELETE SET NULL;
        CREATE INDEX IF NOT EXISTS idx_group_company_id ON public."group"(company_id);
      END IF;

      -- advertisement.company_id
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='advertisement' AND column_name='company_id';
      IF NOT FOUND THEN
        ALTER TABLE public.advertisement ADD COLUMN company_id BIGINT REFERENCES public.company(id) ON DELETE SET NULL;
        CREATE INDEX IF NOT EXISTS idx_advertisement_company_id ON public.advertisement(company_id);
      END IF;

    END $$;
    """

    # ── 3. Update unique constraints to be company-scoped ─────────────
    # video_name, shop_name, gname, ad_name must be unique PER company,
    # not globally.  We replace the old UNIQUE with a partial unique index.

    ddl_company_scoped_unique = """
    DO $$
    BEGIN
      -- video: unique (company_id, video_name)
      -- Drop old global unique if it exists
      IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'video_video_name_key') THEN
        ALTER TABLE public.video DROP CONSTRAINT video_video_name_key;
      END IF;
      -- Drop the original CREATE TABLE unique
      IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'video_video_name_key1'
                   OR conrelid = 'public.video'::regclass AND contype = 'u'
                   AND (SELECT array_agg(a.attname) FROM pg_attribute a WHERE a.attrelid = conrelid AND a.attnum = ANY(conkey)) = ARRAY['video_name']::name[]) THEN
        BEGIN
          ALTER TABLE public.video DROP CONSTRAINT IF EXISTS video_video_name_key1;
        EXCEPTION WHEN undefined_object THEN NULL;
        END;
      END IF;
      -- Create company-scoped unique indexes (two: one for NULL company, one for non-NULL)
      DROP INDEX IF EXISTS idx_video_company_name;
      DROP INDEX IF EXISTS idx_video_null_company_name;
      CREATE UNIQUE INDEX IF NOT EXISTS idx_video_company_name ON public.video(company_id, video_name) WHERE company_id IS NOT NULL;
      CREATE UNIQUE INDEX IF NOT EXISTS idx_video_null_company_name ON public.video(video_name) WHERE company_id IS NULL;

      -- shop: unique (company_id, shop_name)
      IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'shop_shop_name_key') THEN
        ALTER TABLE public.shop DROP CONSTRAINT shop_shop_name_key;
      END IF;
      DROP INDEX IF EXISTS idx_shop_company_name;
      DROP INDEX IF EXISTS idx_shop_null_company_name;
      CREATE UNIQUE INDEX IF NOT EXISTS idx_shop_company_name ON public.shop(company_id, shop_name) WHERE company_id IS NOT NULL;
      CREATE UNIQUE INDEX IF NOT EXISTS idx_shop_null_company_name ON public.shop(shop_name) WHERE company_id IS NULL;

      -- group: unique (company_id, gname)
      IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'group_gname_key') THEN
        ALTER TABLE public."group" DROP CONSTRAINT group_gname_key;
      END IF;
      DROP INDEX IF EXISTS idx_group_company_gname;
      DROP INDEX IF EXISTS idx_group_null_company_gname;
      CREATE UNIQUE INDEX IF NOT EXISTS idx_group_company_gname ON public."group"(company_id, gname) WHERE company_id IS NOT NULL;
      CREATE UNIQUE INDEX IF NOT EXISTS idx_group_null_company_gname ON public."group"(gname) WHERE company_id IS NULL;

      -- advertisement: unique (company_id, ad_name)
      IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'advertisement_ad_name_key') THEN
        ALTER TABLE public.advertisement DROP CONSTRAINT advertisement_ad_name_key;
      END IF;
      IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'uq_advertisement_ad_name') THEN
        DROP INDEX public.uq_advertisement_ad_name;
      END IF;
      DROP INDEX IF EXISTS idx_advertisement_company_adname;
      DROP INDEX IF EXISTS idx_advertisement_null_company_adname;
      CREATE UNIQUE INDEX IF NOT EXISTS idx_advertisement_company_adname ON public.advertisement(company_id, ad_name) WHERE company_id IS NOT NULL;
      CREATE UNIQUE INDEX IF NOT EXISTS idx_advertisement_null_company_adname ON public.advertisement(ad_name) WHERE company_id IS NULL;
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
        -- Mark existing 'admin' user as super admin
        UPDATE public.users SET is_super_admin = TRUE WHERE username = 'admin';
      END IF;
    END $$;
    """

    # ── Execute all ───────────────────────────────────────────────────
    with conn.cursor() as cur:
        cur.execute(ddl_company_table)
        cur.execute(ddl_company_trigger)
        cur.execute(ddl_add_company_id)
        cur.execute(ddl_company_scoped_unique)
        cur.execute(ddl_super_admin)

    conn.commit()
