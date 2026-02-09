# migrations/multitenant_schema.py

"""
B2B Multi-Tenant Migration - Milestone 2
-----------------------------------------
Adds:
  1. company table (tenants)
  2. role table (per-tenant + platform roles)
  3. audit_log table
  4. tenant_id column to ALL data tables
  5. Updates unique constraints to be per-tenant
  6. Modifies users table for multi-tenancy
  7. Seeds platform roles and default company

This migration is fully idempotent - safe to run multiple times.
"""

import json


def ensure_multitenant_schema(conn):
    """
    Apply multi-tenant schema on top of existing dvsg schema.
    Must run AFTER ensure_dvsg_schema().
    """

    # ══════════════════════════════════════════════════════════════
    # 1. COMPANY TABLE (tenants)
    # ══════════════════════════════════════════════════════════════
    ddl_company = """
    CREATE TABLE IF NOT EXISTS public.company (
        id              BIGSERIAL PRIMARY KEY,
        slug            VARCHAR(50) NOT NULL UNIQUE,
        name            VARCHAR(255) NOT NULL,
        email           VARCHAR(255),
        phone           VARCHAR(50),
        address         TEXT,
        logo_s3_key     TEXT,

        -- Quotas
        max_devices     INT NOT NULL DEFAULT 50,
        max_users       INT NOT NULL DEFAULT 10,
        max_storage_mb  BIGINT NOT NULL DEFAULT 5120,

        -- Status: active, suspended, trial, cancelled
        status          VARCHAR(20) NOT NULL DEFAULT 'active',
        trial_ends_at   TIMESTAMPTZ,

        -- Branding (white-label prep)
        primary_color   VARCHAR(7) DEFAULT '#0a1628',
        accent_color    VARCHAR(7) DEFAULT '#f59e0b',

        -- Metadata
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_by      BIGINT
    );

    CREATE INDEX IF NOT EXISTS idx_company_slug ON public.company(slug);
    CREATE INDEX IF NOT EXISTS idx_company_status ON public.company(status);
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

    # ══════════════════════════════════════════════════════════════
    # 2. ROLE TABLE
    # ══════════════════════════════════════════════════════════════
    ddl_role = """
    CREATE TABLE IF NOT EXISTS public.role (
        id              BIGSERIAL PRIMARY KEY,
        tenant_id       BIGINT,
        name            VARCHAR(100) NOT NULL,
        display_name    VARCHAR(255) NOT NULL,
        description     TEXT,
        is_system       BOOLEAN NOT NULL DEFAULT FALSE,
        permissions     JSONB NOT NULL DEFAULT '[]',
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT role_company_fk FOREIGN KEY (tenant_id)
            REFERENCES public.company(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_role_tenant_id ON public.role(tenant_id);
    """

    ddl_role_unique = """
    DO $$
    BEGIN
      -- Unique role name per tenant
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'role_unique_per_tenant'
      ) THEN
        BEGIN
          ALTER TABLE public.role ADD CONSTRAINT role_unique_per_tenant UNIQUE(tenant_id, name);
        EXCEPTION WHEN duplicate_table THEN NULL;
        END;
      END IF;
    END $$;

    -- Unique platform-level roles (tenant_id IS NULL)
    CREATE UNIQUE INDEX IF NOT EXISTS role_unique_platform
        ON public.role(name) WHERE tenant_id IS NULL;
    """

    ddl_role_trigger = """
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_role_updated_at') THEN
        CREATE TRIGGER trg_role_updated_at
        BEFORE UPDATE ON public.role
        FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
      END IF;
    END $$;
    """

    # ══════════════════════════════════════════════════════════════
    # 3. AUDIT LOG TABLE
    # ══════════════════════════════════════════════════════════════
    ddl_audit_log = """
    CREATE TABLE IF NOT EXISTS public.audit_log (
        id              BIGSERIAL PRIMARY KEY,
        tenant_id       BIGINT,
        user_id         BIGINT NOT NULL,
        action          VARCHAR(100) NOT NULL,
        resource_type   VARCHAR(50),
        resource_id     BIGINT,
        details         JSONB,
        ip_address      INET,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT audit_log_company_fk FOREIGN KEY (tenant_id)
            REFERENCES public.company(id) ON DELETE SET NULL
    );

    CREATE INDEX IF NOT EXISTS idx_audit_log_tenant ON public.audit_log(tenant_id);
    CREATE INDEX IF NOT EXISTS idx_audit_log_user ON public.audit_log(user_id);
    CREATE INDEX IF NOT EXISTS idx_audit_log_action ON public.audit_log(action);
    CREATE INDEX IF NOT EXISTS idx_audit_log_created ON public.audit_log(created_at);
    CREATE INDEX IF NOT EXISTS idx_audit_log_tenant_time ON public.audit_log(tenant_id, created_at DESC);
    """

    # ══════════════════════════════════════════════════════════════
    # 4. SEED DEFAULT COMPANY (id=1) for existing data migration
    # ══════════════════════════════════════════════════════════════
    ddl_seed_default_company = """
    INSERT INTO public.company (id, slug, name, email, status)
    SELECT 1, 'default', 'Default Company', 'admin@digix.local', 'active'
    WHERE NOT EXISTS (SELECT 1 FROM public.company WHERE id = 1);

    -- Ensure sequence is ahead of manual insert
    SELECT setval('company_id_seq', GREATEST(1, (SELECT COALESCE(MAX(id), 1) FROM public.company)));
    """

    # ══════════════════════════════════════════════════════════════
    # 5. ADD tenant_id TO ALL DATA TABLES
    #    Pattern: ADD IF NOT EXISTS → backfill → SET NOT NULL → FK → INDEX
    # ══════════════════════════════════════════════════════════════

    ddl_add_tenant_id = """
    DO $$
    DECLARE t text;
    BEGIN
      -- ─── device ───
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='tenant_id';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN tenant_id BIGINT;
      END IF;
      UPDATE public.device SET tenant_id = 1 WHERE tenant_id IS NULL;
      ALTER TABLE public.device ALTER COLUMN tenant_id SET DEFAULT 1;

      -- ─── video ───
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='video' AND column_name='tenant_id';
      IF NOT FOUND THEN
        ALTER TABLE public.video ADD COLUMN tenant_id BIGINT;
      END IF;
      UPDATE public.video SET tenant_id = 1 WHERE tenant_id IS NULL;
      ALTER TABLE public.video ALTER COLUMN tenant_id SET DEFAULT 1;

      -- ─── shop ───
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='shop' AND column_name='tenant_id';
      IF NOT FOUND THEN
        ALTER TABLE public.shop ADD COLUMN tenant_id BIGINT;
      END IF;
      UPDATE public.shop SET tenant_id = 1 WHERE tenant_id IS NULL;
      ALTER TABLE public.shop ALTER COLUMN tenant_id SET DEFAULT 1;

      -- ─── group ───
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='group' AND column_name='tenant_id';
      IF NOT FOUND THEN
        ALTER TABLE public."group" ADD COLUMN tenant_id BIGINT;
      END IF;
      UPDATE public."group" SET tenant_id = 1 WHERE tenant_id IS NULL;
      ALTER TABLE public."group" ALTER COLUMN tenant_id SET DEFAULT 1;

      -- ─── advertisement ───
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='advertisement' AND column_name='tenant_id';
      IF NOT FOUND THEN
        ALTER TABLE public.advertisement ADD COLUMN tenant_id BIGINT;
      END IF;
      UPDATE public.advertisement SET tenant_id = 1 WHERE tenant_id IS NULL;
      ALTER TABLE public.advertisement ALTER COLUMN tenant_id SET DEFAULT 1;

      -- ─── device_video_shop_group ───
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device_video_shop_group' AND column_name='tenant_id';
      IF NOT FOUND THEN
        ALTER TABLE public.device_video_shop_group ADD COLUMN tenant_id BIGINT;
      END IF;
      UPDATE public.device_video_shop_group SET tenant_id = 1 WHERE tenant_id IS NULL;
      ALTER TABLE public.device_video_shop_group ALTER COLUMN tenant_id SET DEFAULT 1;

      -- ─── device_layout ───
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device_layout' AND column_name='tenant_id';
      IF NOT FOUND THEN
        ALTER TABLE public.device_layout ADD COLUMN tenant_id BIGINT;
      END IF;
      UPDATE public.device_layout SET tenant_id = 1 WHERE tenant_id IS NULL;
      ALTER TABLE public.device_layout ALTER COLUMN tenant_id SET DEFAULT 1;

      -- ─── device_logs ───
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device_logs' AND column_name='tenant_id';
      IF NOT FOUND THEN
        ALTER TABLE public.device_logs ADD COLUMN tenant_id BIGINT;
      END IF;
      UPDATE public.device_logs SET tenant_id = 1 WHERE tenant_id IS NULL;
      ALTER TABLE public.device_logs ALTER COLUMN tenant_id SET DEFAULT 1;

      -- ─── device_temperature ───
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='device_temperature') THEN
        SELECT 1 INTO t FROM information_schema.columns
         WHERE table_schema='public' AND table_name='device_temperature' AND column_name='tenant_id';
        IF NOT FOUND THEN
          ALTER TABLE public.device_temperature ADD COLUMN tenant_id BIGINT;
        END IF;
        UPDATE public.device_temperature SET tenant_id = 1 WHERE tenant_id IS NULL;
        ALTER TABLE public.device_temperature ALTER COLUMN tenant_id SET DEFAULT 1;
      END IF;

      -- ─── temperature (alternate table name) ───
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='temperature') THEN
        SELECT 1 INTO t FROM information_schema.columns
         WHERE table_schema='public' AND table_name='temperature' AND column_name='tenant_id';
        IF NOT FOUND THEN
          ALTER TABLE public.temperature ADD COLUMN tenant_id BIGINT;
        END IF;
        UPDATE public.temperature SET tenant_id = 1 WHERE tenant_id IS NULL;
        ALTER TABLE public.temperature ALTER COLUMN tenant_id SET DEFAULT 1;
      END IF;

      -- ─── device_online_history ───
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device_online_history' AND column_name='tenant_id';
      IF NOT FOUND THEN
        ALTER TABLE public.device_online_history ADD COLUMN tenant_id BIGINT;
      END IF;
      UPDATE public.device_online_history SET tenant_id = 1 WHERE tenant_id IS NULL;
      ALTER TABLE public.device_online_history ALTER COLUMN tenant_id SET DEFAULT 1;

      -- ─── group_video ───
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='group_video' AND column_name='tenant_id';
      IF NOT FOUND THEN
        ALTER TABLE public.group_video ADD COLUMN tenant_id BIGINT;
      END IF;
      UPDATE public.group_video SET tenant_id = 1 WHERE tenant_id IS NULL;
      ALTER TABLE public.group_video ALTER COLUMN tenant_id SET DEFAULT 1;

      -- ─── group_advertisement ───
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='group_advertisement' AND column_name='tenant_id';
      IF NOT FOUND THEN
        ALTER TABLE public.group_advertisement ADD COLUMN tenant_id BIGINT;
      END IF;
      UPDATE public.group_advertisement SET tenant_id = 1 WHERE tenant_id IS NULL;
      ALTER TABLE public.group_advertisement ALTER COLUMN tenant_id SET DEFAULT 1;

      -- ─── device_advertisement_shop_group ───
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device_advertisement_shop_group' AND column_name='tenant_id';
      IF NOT FOUND THEN
        ALTER TABLE public.device_advertisement_shop_group ADD COLUMN tenant_id BIGINT;
      END IF;
      UPDATE public.device_advertisement_shop_group SET tenant_id = 1 WHERE tenant_id IS NULL;
      ALTER TABLE public.device_advertisement_shop_group ALTER COLUMN tenant_id SET DEFAULT 1;

      -- ─── device_assignment ───
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device_assignment' AND column_name='tenant_id';
      IF NOT FOUND THEN
        ALTER TABLE public.device_assignment ADD COLUMN tenant_id BIGINT;
      END IF;
      UPDATE public.device_assignment SET tenant_id = 1 WHERE tenant_id IS NULL;
      ALTER TABLE public.device_assignment ALTER COLUMN tenant_id SET DEFAULT 1;

      -- ─── count_history (may or may not exist) ───
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='count_history') THEN
        SELECT 1 INTO t FROM information_schema.columns
         WHERE table_schema='public' AND table_name='count_history' AND column_name='tenant_id';
        IF NOT FOUND THEN
          ALTER TABLE public.count_history ADD COLUMN tenant_id BIGINT;
        END IF;
        UPDATE public.count_history SET tenant_id = 1 WHERE tenant_id IS NULL;
        ALTER TABLE public.count_history ALTER COLUMN tenant_id SET DEFAULT 1;
      END IF;

    END $$;
    """

    # ══════════════════════════════════════════════════════════════
    # 6. FOREIGN KEYS for tenant_id → company(id)
    # ══════════════════════════════════════════════════════════════
    ddl_tenant_fks = """
    DO $$
    BEGIN
      -- device
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'device_tenant_fk') THEN
        ALTER TABLE public.device ADD CONSTRAINT device_tenant_fk
          FOREIGN KEY (tenant_id) REFERENCES public.company(id) ON DELETE RESTRICT;
      END IF;

      -- video
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'video_tenant_fk') THEN
        ALTER TABLE public.video ADD CONSTRAINT video_tenant_fk
          FOREIGN KEY (tenant_id) REFERENCES public.company(id) ON DELETE RESTRICT;
      END IF;

      -- shop
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'shop_tenant_fk') THEN
        ALTER TABLE public.shop ADD CONSTRAINT shop_tenant_fk
          FOREIGN KEY (tenant_id) REFERENCES public.company(id) ON DELETE RESTRICT;
      END IF;

      -- group
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'group_tenant_fk') THEN
        ALTER TABLE public."group" ADD CONSTRAINT group_tenant_fk
          FOREIGN KEY (tenant_id) REFERENCES public.company(id) ON DELETE RESTRICT;
      END IF;

      -- advertisement
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'advertisement_tenant_fk') THEN
        ALTER TABLE public.advertisement ADD CONSTRAINT advertisement_tenant_fk
          FOREIGN KEY (tenant_id) REFERENCES public.company(id) ON DELETE RESTRICT;
      END IF;

      -- device_video_shop_group
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dvsg_tenant_fk') THEN
        ALTER TABLE public.device_video_shop_group ADD CONSTRAINT dvsg_tenant_fk
          FOREIGN KEY (tenant_id) REFERENCES public.company(id) ON DELETE RESTRICT;
      END IF;

      -- device_assignment
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'device_assignment_tenant_fk') THEN
        ALTER TABLE public.device_assignment ADD CONSTRAINT device_assignment_tenant_fk
          FOREIGN KEY (tenant_id) REFERENCES public.company(id) ON DELETE RESTRICT;
      END IF;

    END $$;
    """

    # ══════════════════════════════════════════════════════════════
    # 7. TENANT INDEXES
    # ══════════════════════════════════════════════════════════════
    ddl_tenant_indexes = """
    CREATE INDEX IF NOT EXISTS idx_device_tenant ON public.device(tenant_id);
    CREATE INDEX IF NOT EXISTS idx_video_tenant ON public.video(tenant_id);
    CREATE INDEX IF NOT EXISTS idx_shop_tenant ON public.shop(tenant_id);
    CREATE INDEX IF NOT EXISTS idx_group_tenant ON public."group"(tenant_id);
    CREATE INDEX IF NOT EXISTS idx_advertisement_tenant ON public.advertisement(tenant_id);
    CREATE INDEX IF NOT EXISTS idx_dvsg_tenant ON public.device_video_shop_group(tenant_id);
    CREATE INDEX IF NOT EXISTS idx_device_layout_tenant ON public.device_layout(tenant_id);
    CREATE INDEX IF NOT EXISTS idx_device_logs_tenant ON public.device_logs(tenant_id);
    CREATE INDEX IF NOT EXISTS idx_device_online_history_tenant ON public.device_online_history(tenant_id);
    CREATE INDEX IF NOT EXISTS idx_group_video_tenant ON public.group_video(tenant_id);
    CREATE INDEX IF NOT EXISTS idx_group_advertisement_tenant ON public.group_advertisement(tenant_id);
    CREATE INDEX IF NOT EXISTS idx_dasg_tenant ON public.device_advertisement_shop_group(tenant_id);
    CREATE INDEX IF NOT EXISTS idx_device_assignment_tenant ON public.device_assignment(tenant_id);
    """

    ddl_tenant_indexes_optional = """
    DO $$
    BEGIN
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='device_temperature') THEN
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_device_temperature_tenant ON public.device_temperature(tenant_id)';
      END IF;
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='temperature') THEN
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_temperature_tenant ON public.temperature(tenant_id)';
      END IF;
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='count_history') THEN
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_count_history_tenant ON public.count_history(tenant_id)';
      END IF;
    END $$;
    """

    # ══════════════════════════════════════════════════════════════
    # 8. UPDATE UNIQUE CONSTRAINTS to be per-tenant
    # ══════════════════════════════════════════════════════════════
    ddl_update_unique_constraints = """
    DO $$
    BEGIN
      -- ─── device: mobile_id unique per tenant ───
      DROP INDEX IF EXISTS idx_device_mobile_id;
      -- Drop old unique constraint if exists
      IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'device_mobile_id_key') THEN
        ALTER TABLE public.device DROP CONSTRAINT device_mobile_id_key;
      END IF;

      -- ─── video: video_name unique per tenant ───
      IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'video_video_name_key') THEN
        ALTER TABLE public.video DROP CONSTRAINT video_video_name_key;
      END IF;
      -- Also drop the original inline UNIQUE from CREATE TABLE
      IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'video_video_name_key') THEN
        DROP INDEX IF EXISTS video_video_name_key;
      END IF;

      -- ─── shop: shop_name unique per tenant ───
      IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'shop_shop_name_key') THEN
        ALTER TABLE public.shop DROP CONSTRAINT shop_shop_name_key;
      END IF;

      -- ─── group: gname unique per tenant ───
      IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'group_gname_key') THEN
        ALTER TABLE public."group" DROP CONSTRAINT group_gname_key;
      END IF;

      -- ─── advertisement: ad_name unique per tenant ───
      IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'advertisement_ad_name_key') THEN
        ALTER TABLE public.advertisement DROP CONSTRAINT advertisement_ad_name_key;
      END IF;
    END $$;

    -- Drop old unique indexes
    DROP INDEX IF EXISTS uq_advertisement_ad_name;
    DROP INDEX IF EXISTS dvsg_unique_with_gid;
    DROP INDEX IF EXISTS dvsg_unique_no_gid;
    DROP INDEX IF EXISTS dasg_unique_with_gid;
    DROP INDEX IF EXISTS dasg_unique_no_gid;

    -- Drop leftover indexes from previous company_id-based migration attempts
    DROP INDEX IF EXISTS idx_shop_null_company_name;
    DROP INDEX IF EXISTS idx_shop_company_name;
    DROP INDEX IF EXISTS idx_video_null_company_name;
    DROP INDEX IF EXISTS idx_video_company_name;
    DROP INDEX IF EXISTS idx_group_null_company_name;
    DROP INDEX IF EXISTS idx_group_company_name;
    DROP INDEX IF EXISTS idx_device_null_company_name;
    DROP INDEX IF EXISTS idx_device_company_name;
    DROP INDEX IF EXISTS idx_advertisement_null_company_name;
    DROP INDEX IF EXISTS idx_advertisement_company_name;

    -- Create new composite unique indexes (per-tenant)
    CREATE UNIQUE INDEX IF NOT EXISTS idx_device_tenant_mobile
        ON public.device(tenant_id, mobile_id);

    CREATE UNIQUE INDEX IF NOT EXISTS idx_video_tenant_name
        ON public.video(tenant_id, video_name);

    CREATE UNIQUE INDEX IF NOT EXISTS idx_shop_tenant_name
        ON public.shop(tenant_id, shop_name);

    CREATE UNIQUE INDEX IF NOT EXISTS idx_group_tenant_gname
        ON public."group"(tenant_id, gname);

    CREATE UNIQUE INDEX IF NOT EXISTS idx_advertisement_tenant_name
        ON public.advertisement(tenant_id, ad_name);

    CREATE UNIQUE INDEX IF NOT EXISTS dvsg_unique_with_gid_tenant
        ON public.device_video_shop_group (tenant_id, did, vid, sid, gid)
        WHERE (gid IS NOT NULL);

    CREATE UNIQUE INDEX IF NOT EXISTS dvsg_unique_no_gid_tenant
        ON public.device_video_shop_group (tenant_id, did, vid, sid)
        WHERE (gid IS NULL);

    CREATE UNIQUE INDEX IF NOT EXISTS dasg_unique_with_gid_tenant
        ON public.device_advertisement_shop_group (tenant_id, did, aid, sid, gid)
        WHERE (gid IS NOT NULL);

    CREATE UNIQUE INDEX IF NOT EXISTS dasg_unique_no_gid_tenant
        ON public.device_advertisement_shop_group (tenant_id, did, aid, sid)
        WHERE (gid IS NULL);
    """

    # ══════════════════════════════════════════════════════════════
    # 9. MODIFY USERS TABLE for multi-tenancy
    # ══════════════════════════════════════════════════════════════
    ddl_users_multitenant = """
    DO $$
    DECLARE t text;
    BEGIN
      -- tenant_id (nullable: NULL = platform user)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='users' AND column_name='tenant_id';
      IF NOT FOUND THEN
        ALTER TABLE public.users ADD COLUMN tenant_id BIGINT;
      END IF;

      -- user_type: 'platform' or 'company'
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='users' AND column_name='user_type';
      IF NOT FOUND THEN
        ALTER TABLE public.users ADD COLUMN user_type VARCHAR(20) NOT NULL DEFAULT 'company';
      END IF;

      -- role_id: links to role table
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='users' AND column_name='role_id';
      IF NOT FOUND THEN
        ALTER TABLE public.users ADD COLUMN role_id BIGINT;
      END IF;

      -- must_change_password: force password change on first login
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='users' AND column_name='must_change_password';
      IF NOT FOUND THEN
        ALTER TABLE public.users ADD COLUMN must_change_password BOOLEAN NOT NULL DEFAULT FALSE;
      END IF;

      -- Add FK for tenant_id -> company
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'users_tenant_fk') THEN
        ALTER TABLE public.users ADD CONSTRAINT users_tenant_fk
          FOREIGN KEY (tenant_id) REFERENCES public.company(id) ON DELETE CASCADE;
      END IF;

      -- Add FK for role_id -> role
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'users_role_fk') THEN
        ALTER TABLE public.users ADD CONSTRAINT users_role_fk
          FOREIGN KEY (role_id) REFERENCES public.role(id) ON DELETE SET NULL;
      END IF;

    END $$;

    CREATE INDEX IF NOT EXISTS idx_users_tenant ON public.users(tenant_id);
    CREATE INDEX IF NOT EXISTS idx_users_user_type ON public.users(user_type);
    CREATE INDEX IF NOT EXISTS idx_users_role_id ON public.users(role_id);
    """

    # Update users unique constraints to be per-tenant
    ddl_users_unique_update = """
    DO $$
    BEGIN
      -- Drop old global unique constraints
      IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'users_username_key') THEN
        ALTER TABLE public.users DROP CONSTRAINT users_username_key;
      END IF;
      IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'users_email_key') THEN
        ALTER TABLE public.users DROP CONSTRAINT users_email_key;
      END IF;
    END $$;

    -- Platform users: username globally unique
    CREATE UNIQUE INDEX IF NOT EXISTS idx_users_platform_username
        ON public.users(username) WHERE tenant_id IS NULL;

    -- Company users: username unique per tenant
    CREATE UNIQUE INDEX IF NOT EXISTS idx_users_tenant_username
        ON public.users(tenant_id, username) WHERE tenant_id IS NOT NULL;

    -- Platform users: email globally unique
    CREATE UNIQUE INDEX IF NOT EXISTS idx_users_platform_email
        ON public.users(email) WHERE tenant_id IS NULL AND email IS NOT NULL;

    -- Company users: email unique per tenant
    CREATE UNIQUE INDEX IF NOT EXISTS idx_users_tenant_email
        ON public.users(tenant_id, email) WHERE tenant_id IS NOT NULL AND email IS NOT NULL;
    """

    # ══════════════════════════════════════════════════════════════
    # 10. DEVICE ACTIVATION CODE (for multi-tenant device onboarding)
    # ══════════════════════════════════════════════════════════════
    ddl_device_activation = """
    DO $$
    DECLARE t text;
    BEGIN
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='activation_code';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN activation_code VARCHAR(10);
      END IF;

      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='activated_at';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN activated_at TIMESTAMPTZ;
      END IF;
    END $$;

    CREATE INDEX IF NOT EXISTS idx_device_activation_code
        ON public.device(activation_code) WHERE activation_code IS NOT NULL;
    """

    # ══════════════════════════════════════════════════════════════
    # 11. SEED DATA: Platform roles + migrate existing admin
    # ══════════════════════════════════════════════════════════════
    ddl_seed_platform_roles = """
    -- Super Admin
    INSERT INTO public.role (tenant_id, name, display_name, description, is_system, permissions)
    SELECT NULL, 'super_admin', 'Super Admin', 'Full platform access', TRUE,
           '["platform.manage_companies","platform.manage_users","platform.view_analytics","platform.manage_settings","platform.impersonate","platform.manage_roles","company.full_access"]'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.role WHERE tenant_id IS NULL AND name = 'super_admin');

    -- Platform Manager
    INSERT INTO public.role (tenant_id, name, display_name, description, is_system, permissions)
    SELECT NULL, 'platform_manager', 'Platform Manager', 'Onboard companies, view analytics', TRUE,
           '["platform.manage_companies","platform.view_analytics","platform.manage_support","company.view"]'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.role WHERE tenant_id IS NULL AND name = 'platform_manager');

    -- Support Agent
    INSERT INTO public.role (tenant_id, name, display_name, description, is_system, permissions)
    SELECT NULL, 'support_agent', 'Support Agent', 'Read-only troubleshooting access', TRUE,
           '["platform.view_analytics","company.view","company.view_devices","company.view_content"]'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.role WHERE tenant_id IS NULL AND name = 'support_agent');
    """

    ddl_migrate_existing_admin = """
    -- Migrate existing 'admin' user to platform super_admin
    UPDATE public.users
    SET user_type = 'platform',
        tenant_id = NULL,
        role_id = (SELECT id FROM public.role WHERE tenant_id IS NULL AND name = 'super_admin' LIMIT 1)
    WHERE username = 'admin' AND user_type = 'company';

    -- Migrate other existing users to default company (id=1)
    UPDATE public.users
    SET tenant_id = 1
    WHERE tenant_id IS NULL AND username != 'admin' AND user_type = 'company';
    """

    # ══════════════════════════════════════════════════════════════
    # 12. CREATE DEFAULT ROLES FOR DEFAULT COMPANY (id=1)
    # ══════════════════════════════════════════════════════════════
    ddl_seed_default_company_roles = """
    INSERT INTO public.role (tenant_id, name, display_name, description, is_system, permissions)
    SELECT 1, 'company_admin', 'Company Admin', 'Full access within company', TRUE,
           '["view_dashboard","manage_devices","manage_groups","manage_shops","upload_videos","manage_videos","manage_advertisements","manage_links","manage_users","manage_roles","view_reports","export_data","manage_company_settings"]'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.role WHERE tenant_id = 1 AND name = 'company_admin');

    INSERT INTO public.role (tenant_id, name, display_name, description, is_system, permissions)
    SELECT 1, 'content_manager', 'Content Manager', 'Upload and manage content', TRUE,
           '["view_dashboard","upload_videos","manage_videos","manage_advertisements","manage_links","manage_groups","view_reports"]'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.role WHERE tenant_id = 1 AND name = 'content_manager');

    INSERT INTO public.role (tenant_id, name, display_name, description, is_system, permissions)
    SELECT 1, 'device_manager', 'Device Manager', 'Manage devices and groups', TRUE,
           '["view_dashboard","manage_devices","manage_groups","manage_shops","manage_links","view_reports"]'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.role WHERE tenant_id = 1 AND name = 'device_manager');

    INSERT INTO public.role (tenant_id, name, display_name, description, is_system, permissions)
    SELECT 1, 'reports_viewer', 'Reports Viewer', 'View reports and analytics', TRUE,
           '["view_dashboard","view_reports","export_data"]'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.role WHERE tenant_id = 1 AND name = 'reports_viewer');

    INSERT INTO public.role (tenant_id, name, display_name, description, is_system, permissions)
    SELECT 1, 'viewer', 'Viewer', 'Read-only dashboard access', TRUE,
           '["view_dashboard"]'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.role WHERE tenant_id = 1 AND name = 'viewer');
    """

    # ══════════════════════════════════════════════════════════════
    # EXECUTE ALL
    # ══════════════════════════════════════════════════════════════
    with conn.cursor() as cur:
        # 1. New tables
        cur.execute(ddl_company)
        cur.execute(ddl_company_trigger)
        cur.execute(ddl_role)
        cur.execute(ddl_role_unique)
        cur.execute(ddl_role_trigger)
        cur.execute(ddl_audit_log)

        # 2. Seed default company BEFORE adding tenant_id columns
        cur.execute(ddl_seed_default_company)

        # 3. Add tenant_id to all data tables (backfills with 1)
        cur.execute(ddl_add_tenant_id)

        # 4. Foreign keys
        cur.execute(ddl_tenant_fks)

        # 5. Indexes
        cur.execute(ddl_tenant_indexes)
        cur.execute(ddl_tenant_indexes_optional)

        # 6. Update unique constraints to be per-tenant
        cur.execute(ddl_update_unique_constraints)

        # 7. Modify users table
        cur.execute(ddl_users_multitenant)
        cur.execute(ddl_users_unique_update)

        # 8. Device activation code
        cur.execute(ddl_device_activation)

        # 9. Seed roles
        cur.execute(ddl_seed_platform_roles)
        cur.execute(ddl_migrate_existing_admin)
        cur.execute(ddl_seed_default_company_roles)

    conn.commit()


# ══════════════════════════════════════════════════════════════
# HELPER: Clone default roles to a new company
# ══════════════════════════════════════════════════════════════

DEFAULT_COMPANY_ROLES = [
    {
        "name": "company_admin",
        "display_name": "Company Admin",
        "description": "Full access within company",
        "is_system": True,
        "permissions": [
            "view_dashboard", "manage_devices", "manage_groups", "manage_shops",
            "upload_videos", "manage_videos", "manage_advertisements", "manage_links",
            "manage_users", "manage_roles", "view_reports", "export_data",
            "manage_company_settings"
        ]
    },
    {
        "name": "content_manager",
        "display_name": "Content Manager",
        "description": "Upload and manage content",
        "is_system": True,
        "permissions": [
            "view_dashboard", "upload_videos", "manage_videos", "manage_advertisements",
            "manage_links", "manage_groups", "view_reports"
        ]
    },
    {
        "name": "device_manager",
        "display_name": "Device Manager",
        "description": "Manage devices and groups",
        "is_system": True,
        "permissions": [
            "view_dashboard", "manage_devices", "manage_groups", "manage_shops",
            "manage_links", "view_reports"
        ]
    },
    {
        "name": "reports_viewer",
        "display_name": "Reports Viewer",
        "description": "View reports and analytics",
        "is_system": True,
        "permissions": ["view_dashboard", "view_reports", "export_data"]
    },
    {
        "name": "viewer",
        "display_name": "Viewer",
        "description": "Read-only dashboard access",
        "is_system": True,
        "permissions": ["view_dashboard"]
    },
]


def clone_roles_for_company(conn, company_id: int) -> dict:
    """
    Clone default roles into a new company. Returns dict mapping role_name -> role_id.
    """
    role_map = {}
    with conn.cursor() as cur:
        for role_def in DEFAULT_COMPANY_ROLES:
            cur.execute("""
                INSERT INTO public.role (tenant_id, name, display_name, description, is_system, permissions)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, name) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    description = EXCLUDED.description,
                    permissions = EXCLUDED.permissions
                RETURNING id
            """, (
                company_id,
                role_def["name"],
                role_def["display_name"],
                role_def["description"],
                role_def["is_system"],
                json.dumps(role_def["permissions"]),
            ))
            role_map[role_def["name"]] = cur.fetchone()[0]
    return role_map
