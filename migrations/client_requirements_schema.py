# migrations/client_requirements_schema.py
# Location: cms-backend-staging/migrations/client_requirements_schema.py
"""
Client Requirements Migration
=============================
Implements:
1. Storage management (80% limit) - Device reports storage, server manages allocation
2. Device status duration tracking - How long online/offline
3. Device expiration & scheduling system - Auto-deactivate devices
4. Content approval workflow - Two-factor content updates

This migration is fully idempotent - safe to run multiple times.
"""


def ensure_client_requirements_schema(conn):
    """
    Apply client requirements schema changes.
    Must run AFTER ensure_multitenant_schema().
    """

    # ══════════════════════════════════════════════════════════════
    # 1. DEVICE STORAGE TRACKING
    # ══════════════════════════════════════════════════════════════
    # Device reports its storage capacity and usage
    # Server uses this to manage 80% storage limit
    
    ddl_device_storage = """
    DO $$
    DECLARE t text;
    BEGIN
      -- total_storage_bytes: Total device storage capacity
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='total_storage_bytes';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN total_storage_bytes BIGINT DEFAULT 0;
      END IF;

      -- used_storage_bytes: Currently used storage
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='used_storage_bytes';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN used_storage_bytes BIGINT DEFAULT 0;
      END IF;

      -- available_storage_bytes: Available for content (calculated: total * 0.8 - used)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='available_storage_bytes';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN available_storage_bytes BIGINT DEFAULT 0;
      END IF;

      -- storage_limit_percent: Configurable limit (default 80%)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='storage_limit_percent';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN storage_limit_percent INT DEFAULT 80;
      END IF;

      -- last_storage_report_at: When device last reported storage
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='last_storage_report_at';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN last_storage_report_at TIMESTAMPTZ;
      END IF;
    END $$;
    """

    # ══════════════════════════════════════════════════════════════
    # 2. DEVICE STATUS DURATION TRACKING
    # ══════════════════════════════════════════════════════════════
    # Track cumulative online/offline time and current session duration
    
    ddl_device_status_duration = """
    DO $$
    DECLARE t text;
    BEGIN
      -- current_status_since: When current online/offline status started
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='current_status_since';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN current_status_since TIMESTAMPTZ DEFAULT NOW();
      END IF;

      -- total_online_seconds: Cumulative online time (updated when going offline)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='total_online_seconds';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN total_online_seconds BIGINT DEFAULT 0;
      END IF;

      -- total_offline_seconds: Cumulative offline time (updated when going online)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='total_offline_seconds';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN total_offline_seconds BIGINT DEFAULT 0;
      END IF;

      -- last_online_duration_seconds: Duration of last online session
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='last_online_duration_seconds';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN last_online_duration_seconds BIGINT DEFAULT 0;
      END IF;

      -- last_offline_duration_seconds: Duration of last offline period
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='last_offline_duration_seconds';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN last_offline_duration_seconds BIGINT DEFAULT 0;
      END IF;
    END $$;
    """

    # ══════════════════════════════════════════════════════════════
    # 3. DEVICE EXPIRATION & SCHEDULING SYSTEM
    # ══════════════════════════════════════════════════════════════
    
    # 3a. Add expiration fields to device table
    ddl_device_expiration = """
    DO $$
    DECLARE t text;
    BEGIN
      -- expires_at: When device should be auto-deactivated
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='expires_at';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN expires_at TIMESTAMPTZ DEFAULT NULL;
      END IF;

      -- expiration_action: What to do on expiration: 'deactivate', 'delete', 'notify_only'
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='expiration_action';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN expiration_action VARCHAR(20) DEFAULT 'deactivate';
      END IF;

      -- expiration_notified_at: When expiration notification was sent
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='expiration_notified_at';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN expiration_notified_at TIMESTAMPTZ DEFAULT NULL;
      END IF;

      -- deactivated_at: When device was deactivated
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='deactivated_at';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN deactivated_at TIMESTAMPTZ DEFAULT NULL;
      END IF;

      -- deactivation_reason: Why device was deactivated
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='deactivation_reason';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN deactivation_reason VARCHAR(100) DEFAULT NULL;
      END IF;
    END $$;
    
    CREATE INDEX IF NOT EXISTS idx_device_expires_at ON public.device(expires_at) WHERE expires_at IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_device_expiration_active ON public.device(expires_at, is_active) WHERE expires_at IS NOT NULL AND is_active = TRUE;
    """

    # 3b. Device Schedule Table - For recurring schedules
    ddl_device_schedule = """
    CREATE TABLE IF NOT EXISTS public.device_schedule (
        id              BIGSERIAL PRIMARY KEY,
        tenant_id       BIGINT NOT NULL,
        
        -- Target: specific device, all devices in company, or devices in group
        device_id       BIGINT,  -- NULL means company-wide or group-wide
        group_id        BIGINT,  -- NULL means specific device or company-wide
        
        -- Schedule type
        schedule_type   VARCHAR(30) NOT NULL,  -- 'expiration', 'maintenance', 'content_refresh', 'custom'
        
        -- Timing
        scheduled_at    TIMESTAMPTZ NOT NULL,  -- When to execute
        repeat_type     VARCHAR(20),           -- NULL, 'daily', 'weekly', 'monthly', 'yearly'
        repeat_interval INT DEFAULT 1,         -- e.g., every 2 weeks
        repeat_until    TIMESTAMPTZ,           -- When to stop repeating
        
        -- Action
        action          VARCHAR(30) NOT NULL,  -- 'deactivate', 'activate', 'refresh_content', 'notify', 'delete'
        action_params   JSONB,                 -- Additional parameters for the action
        
        -- Notification
        notify_days_before INT DEFAULT 7,      -- Notify X days before
        notify_email    TEXT,                  -- Email to notify
        notify_sent_at  TIMESTAMPTZ,           -- When notification was sent
        
        -- Status
        status          VARCHAR(20) NOT NULL DEFAULT 'active',  -- 'active', 'paused', 'completed', 'cancelled'
        last_executed_at TIMESTAMPTZ,
        next_execution_at TIMESTAMPTZ,
        execution_count INT DEFAULT 0,
        
        -- Audit
        created_by      BIGINT NOT NULL,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        
        -- Constraints
        CONSTRAINT device_schedule_tenant_fk FOREIGN KEY (tenant_id)
            REFERENCES public.company(id) ON DELETE CASCADE,
        CONSTRAINT device_schedule_device_fk FOREIGN KEY (device_id)
            REFERENCES public.device(id) ON DELETE CASCADE,
        CONSTRAINT device_schedule_group_fk FOREIGN KEY (group_id)
            REFERENCES public."group"(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_device_schedule_tenant ON public.device_schedule(tenant_id);
    CREATE INDEX IF NOT EXISTS idx_device_schedule_device ON public.device_schedule(device_id);
    CREATE INDEX IF NOT EXISTS idx_device_schedule_next ON public.device_schedule(next_execution_at) WHERE status = 'active';
    CREATE INDEX IF NOT EXISTS idx_device_schedule_status ON public.device_schedule(status);
    """

    ddl_device_schedule_trigger = """
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_device_schedule_updated_at') THEN
        CREATE TRIGGER trg_device_schedule_updated_at
        BEFORE UPDATE ON public.device_schedule
        FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
      END IF;
    END $$;
    """

    # 3c. Schedule Execution Log
    ddl_schedule_execution_log = """
    CREATE TABLE IF NOT EXISTS public.schedule_execution_log (
        id              BIGSERIAL PRIMARY KEY,
        schedule_id     BIGINT NOT NULL,
        executed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        status          VARCHAR(20) NOT NULL,  -- 'success', 'failed', 'skipped'
        devices_affected INT DEFAULT 0,
        error_message   TEXT,
        details         JSONB,
        
        CONSTRAINT schedule_log_schedule_fk FOREIGN KEY (schedule_id)
            REFERENCES public.device_schedule(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_schedule_log_schedule ON public.schedule_execution_log(schedule_id);
    CREATE INDEX IF NOT EXISTS idx_schedule_log_executed ON public.schedule_execution_log(executed_at);
    """

    # ══════════════════════════════════════════════════════════════
    # 4. CONTENT APPROVAL WORKFLOW (Two-Factor Content Update)
    # ══════════════════════════════════════════════════════════════
    
    # 4a. Company settings for approval workflow
    ddl_company_approval_settings = """
    DO $$
    DECLARE t text;
    BEGIN
      -- require_content_approval: Enable/disable approval workflow
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='company' AND column_name='require_content_approval';
      IF NOT FOUND THEN
        ALTER TABLE public.company ADD COLUMN require_content_approval BOOLEAN DEFAULT FALSE;
      END IF;

      -- auto_approve_roles: Roles that don't need approval (JSON array)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='company' AND column_name='auto_approve_roles';
      IF NOT FOUND THEN
        ALTER TABLE public.company ADD COLUMN auto_approve_roles JSONB DEFAULT '["admin", "manager"]';
      END IF;

      -- approval_notify_email: Email to notify for pending approvals
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='company' AND column_name='approval_notify_email';
      IF NOT FOUND THEN
        ALTER TABLE public.company ADD COLUMN approval_notify_email TEXT;
      END IF;
    END $$;
    """

    # 4b. Content Change Request Table
    ddl_content_change_request = """
    CREATE TABLE IF NOT EXISTS public.content_change_request (
        id              BIGSERIAL PRIMARY KEY,
        tenant_id       BIGINT NOT NULL,
        
        -- Request type
        request_type    VARCHAR(30) NOT NULL,  -- 'video_assign', 'video_remove', 'video_upload', 'bulk_assign', 'group_update'
        
        -- What's being changed
        target_type     VARCHAR(30) NOT NULL,  -- 'device', 'group', 'shop'
        target_id       BIGINT NOT NULL,       -- device_id, group_id, or shop_id
        target_name     VARCHAR(255),          -- device_name, group_name, shop_name for display
        
        -- Change details
        change_data     JSONB NOT NULL,        -- The actual change to apply
        /* Example change_data:
           For video_assign: {"video_ids": [1, 2, 3], "video_names": ["ad1.mp4", "ad2.mp4"]}
           For video_remove: {"video_ids": [1], "video_names": ["old_ad.mp4"]}
           For group_update: {"add_videos": [1, 2], "remove_videos": [3]}
        */
        
        -- Requester
        requested_by    BIGINT NOT NULL,
        requested_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        request_note    TEXT,                  -- Optional note from requester
        
        -- Approval
        status          VARCHAR(20) NOT NULL DEFAULT 'pending',  -- 'pending', 'approved', 'rejected', 'cancelled', 'expired'
        reviewed_by     BIGINT,
        reviewed_at     TIMESTAMPTZ,
        review_note     TEXT,                  -- Optional note from reviewer
        
        -- Execution
        executed_at     TIMESTAMPTZ,
        execution_error TEXT,
        
        -- Expiration
        expires_at      TIMESTAMPTZ,           -- Auto-reject after this time
        
        -- Audit
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        
        -- Constraints
        CONSTRAINT ccr_tenant_fk FOREIGN KEY (tenant_id)
            REFERENCES public.company(id) ON DELETE CASCADE,
        CONSTRAINT ccr_requested_by_fk FOREIGN KEY (requested_by)
            REFERENCES public.users(id) ON DELETE SET NULL,
        CONSTRAINT ccr_reviewed_by_fk FOREIGN KEY (reviewed_by)
            REFERENCES public.users(id) ON DELETE SET NULL
    );

    CREATE INDEX IF NOT EXISTS idx_ccr_tenant ON public.content_change_request(tenant_id);
    CREATE INDEX IF NOT EXISTS idx_ccr_status ON public.content_change_request(status);
    CREATE INDEX IF NOT EXISTS idx_ccr_pending ON public.content_change_request(tenant_id, status) WHERE status = 'pending';
    CREATE INDEX IF NOT EXISTS idx_ccr_requested_by ON public.content_change_request(requested_by);
    CREATE INDEX IF NOT EXISTS idx_ccr_expires ON public.content_change_request(expires_at) WHERE status = 'pending';
    """

    ddl_content_change_request_trigger = """
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_ccr_updated_at') THEN
        CREATE TRIGGER trg_ccr_updated_at
        BEFORE UPDATE ON public.content_change_request
        FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
      END IF;
    END $$;
    """

    # 4c. Approval notification preferences per user
    ddl_user_approval_prefs = """
    DO $$
    DECLARE t text;
    BEGIN
      -- can_approve_content: Whether user can approve content changes
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='users' AND column_name='can_approve_content';
      IF NOT FOUND THEN
        ALTER TABLE public.users ADD COLUMN can_approve_content BOOLEAN DEFAULT FALSE;
      END IF;

      -- notify_on_approval_request: Get notified when approval needed
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='users' AND column_name='notify_on_approval_request';
      IF NOT FOUND THEN
        ALTER TABLE public.users ADD COLUMN notify_on_approval_request BOOLEAN DEFAULT FALSE;
      END IF;
    END $$;
    """

    # ══════════════════════════════════════════════════════════════
    # EXECUTE ALL DDL
    # ══════════════════════════════════════════════════════════════
    
    with conn.cursor() as cur:
        # 1. Device storage tracking
        cur.execute(ddl_device_storage)
        
        # 2. Device status duration tracking
        cur.execute(ddl_device_status_duration)
        
        # 3. Device expiration & scheduling
        cur.execute(ddl_device_expiration)
        cur.execute(ddl_device_schedule)
        cur.execute(ddl_device_schedule_trigger)
        cur.execute(ddl_schedule_execution_log)
        
        # 4. Content approval workflow
        cur.execute(ddl_company_approval_settings)
        cur.execute(ddl_content_change_request)
        cur.execute(ddl_content_change_request_trigger)
        cur.execute(ddl_user_approval_prefs)
        
    conn.commit()
    print("[MIGRATION] Client requirements schema applied successfully")
