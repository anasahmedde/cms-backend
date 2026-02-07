# migrations/dvsg_schema.py

"""
Schema / migration for device_video_shop_group and related bits.
Enhanced with: rotation, fit_mode, content_type, display_duration, display_order,
last_online_at, counter reset dates, and device_logs table.

NEW:
- device_temperature table for time-series temperature reporting (line graph)
"""

def ensure_dvsg_schema(conn):
    """
    Ensure the schema for device_video_shop_group and related triggers/indexes exists.
    Expects an open psycopg2 connection.
    """
    ddl_fn = """
    CREATE OR REPLACE FUNCTION public.set_updated_at()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """

    # ── Base tables that other migrations depend on ──
    ddl_base_device = """
    CREATE TABLE IF NOT EXISTS public.device (
        id BIGSERIAL PRIMARY KEY,
        mobile_id TEXT NOT NULL,
        download_status BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_device_mobile_id ON public.device (mobile_id);
    """

    ddl_base_video = """
    CREATE TABLE IF NOT EXISTS public.video (
        id BIGSERIAL PRIMARY KEY,
        video_name TEXT NOT NULL UNIQUE,
        s3_link TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    ddl_base_shop = """
    CREATE TABLE IF NOT EXISTS public.shop (
        id BIGSERIAL PRIMARY KEY,
        shop_name TEXT NOT NULL UNIQUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    ddl_base_group = """
    CREATE TABLE IF NOT EXISTS public."group" (
        id BIGSERIAL PRIMARY KEY,
        gname TEXT NOT NULL UNIQUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    ddl_device_cols = """
    DO $$
    DECLARE t text;
    BEGIN
      -- connection
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='connection';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN connection BOOLEAN NOT NULL DEFAULT FALSE;
      END IF;

      -- screen
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='screen';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN screen TEXT;
      END IF;

      -- download_status
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='download_status';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN download_status BOOLEAN NOT NULL DEFAULT FALSE;
      END IF;

      -- is_online
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='is_online';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN is_online BOOLEAN NOT NULL DEFAULT FALSE;
      END IF;

      -- temperature (DOUBLE PRECISION) - latest snapshot value on device table
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='temperature';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN temperature DOUBLE PRECISION DEFAULT 0;
      END IF;

      -- daily_count (INT)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='daily_count';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN daily_count INT NOT NULL DEFAULT 0;
      END IF;

      -- monthly_count (INT)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='monthly_count';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN monthly_count INT NOT NULL DEFAULT 0;
      END IF;

      ----------------------------------------------------------------
      -- NEW FIELDS FOR ENHANCED REQUIREMENTS
      ----------------------------------------------------------------

      -- last_online_at (for 1-minute online threshold)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='last_online_at';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN last_online_at TIMESTAMPTZ;
      END IF;

      -- last_daily_reset (for counter reset logic)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='last_daily_reset';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN last_daily_reset DATE DEFAULT CURRENT_DATE;
      END IF;

      -- last_monthly_reset (for counter reset logic)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='last_monthly_reset';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN last_monthly_reset DATE DEFAULT DATE_TRUNC('month', CURRENT_DATE)::DATE;
      END IF;

      -- resolution (screen resolution e.g. 1920x1080)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='resolution';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN resolution TEXT DEFAULT NULL;
      END IF;

      -- device_name (friendly name for the device)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='device_name';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN device_name TEXT DEFAULT NULL;
      END IF;

      -- is_active (for deactivating devices without deleting them)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='is_active';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN is_active BOOLEAN NOT NULL DEFAULT TRUE;
      END IF;

      -- needs_refresh (flag to tell app to restart/refresh)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device' AND column_name='needs_refresh';
      IF NOT FOUND THEN
        ALTER TABLE public.device ADD COLUMN needs_refresh BOOLEAN NOT NULL DEFAULT FALSE;
      END IF;

    END $$;
    """

    ddl_video_cols = """
    DO $$
    DECLARE t text;
    BEGIN
      -- Rename legacy column names if they exist
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='video' AND column_name='name';
      IF FOUND THEN
        ALTER TABLE public.video RENAME COLUMN name TO video_name;
      END IF;

      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='video' AND column_name='s3key';
      IF FOUND THEN
        ALTER TABLE public.video RENAME COLUMN s3key TO s3_link;
      END IF;

      -- Add UNIQUE constraint on video_name if not exists
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'video_video_name_key'
      ) THEN
        BEGIN
          ALTER TABLE public.video ADD CONSTRAINT video_video_name_key UNIQUE (video_name);
        EXCEPTION WHEN duplicate_table THEN NULL;
        END;
      END IF;

      -- rotation (0, 90, 180, 270)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='video' AND column_name='rotation';
      IF NOT FOUND THEN
        ALTER TABLE public.video ADD COLUMN rotation INT NOT NULL DEFAULT 0;
      END IF;

      -- content_type (video, image, html, pdf)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='video' AND column_name='content_type';
      IF NOT FOUND THEN
        ALTER TABLE public.video ADD COLUMN content_type TEXT NOT NULL DEFAULT 'video';
      END IF;

      -- display_duration (seconds for non-video content)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='video' AND column_name='display_duration';
      IF NOT FOUND THEN
        ALTER TABLE public.video ADD COLUMN display_duration INT DEFAULT 10;
      END IF;

      -- fit_mode (contain, cover, fill, none)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='video' AND column_name='fit_mode';
      IF NOT FOUND THEN
        ALTER TABLE public.video ADD COLUMN fit_mode TEXT NOT NULL DEFAULT 'cover';
      END IF;

      -- resolution (e.g. 1920x1080, 1280x720)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='video' AND column_name='resolution';
      IF NOT FOUND THEN
        ALTER TABLE public.video ADD COLUMN resolution TEXT DEFAULT NULL;
      END IF;
    END $$;
    """

    ddl_shop_cols = """
    DO $$
    DECLARE t text;
    BEGIN
      -- Rename legacy column name if it exists (shop)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='shop' AND column_name='name';
      IF FOUND THEN
        ALTER TABLE public.shop RENAME COLUMN name TO shop_name;
      END IF;

      -- Add UNIQUE constraint on shop_name if not exists
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'shop_shop_name_key'
      ) THEN
        BEGIN
          ALTER TABLE public.shop ADD CONSTRAINT shop_shop_name_key UNIQUE (shop_name);
        EXCEPTION WHEN duplicate_table THEN NULL;
        END;
      END IF;

      -- Rename legacy column name if it exists (group)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='group' AND column_name='name';
      IF FOUND THEN
        ALTER TABLE public."group" RENAME COLUMN name TO gname;
      END IF;

      -- Add UNIQUE constraint on gname if not exists
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'group_gname_key'
      ) THEN
        BEGIN
          ALTER TABLE public."group" ADD CONSTRAINT group_gname_key UNIQUE (gname);
        EXCEPTION WHEN duplicate_table THEN NULL;
        END;
      END IF;
    END $$;
    """

    ddl_link_table = """
    CREATE TABLE IF NOT EXISTS public.device_video_shop_group (
      id          BIGSERIAL PRIMARY KEY,
      did         BIGINT     NOT NULL,
      vid         BIGINT     NOT NULL,
      sid         BIGINT     NOT NULL,
      gid         BIGINT,  -- NULL -> No group
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      dl_status   BOOLEAN NOT NULL DEFAULT FALSE,
      dl_at       TIMESTAMPTZ,
      dl_error    TEXT,
      CONSTRAINT dvsg_device_fk FOREIGN KEY (did) REFERENCES public.device(id) ON DELETE RESTRICT,
      CONSTRAINT dvsg_video_fk  FOREIGN KEY (vid) REFERENCES public.video(id)  ON DELETE RESTRICT,
      CONSTRAINT dvsg_shop_fk   FOREIGN KEY (sid) REFERENCES public.shop(id)   ON DELETE RESTRICT,
      CONSTRAINT dvsg_group_fk  FOREIGN KEY (gid) REFERENCES public."group"(id) ON DELETE RESTRICT
    );
    """

    ddl_link_cols = """
    DO $$
    DECLARE t text;
    BEGIN
      -- display_order (for multiple videos per screen)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device_video_shop_group' AND column_name='display_order';
      IF NOT FOUND THEN
        ALTER TABLE public.device_video_shop_group ADD COLUMN display_order INT NOT NULL DEFAULT 0;
      END IF;

      -- per-device rotation override (NULL = use video default)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device_video_shop_group' AND column_name='device_rotation';
      IF NOT FOUND THEN
        ALTER TABLE public.device_video_shop_group ADD COLUMN device_rotation INT DEFAULT NULL;
      END IF;

      -- per-device resolution override (NULL = use video default)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device_video_shop_group' AND column_name='device_resolution';
      IF NOT FOUND THEN
        ALTER TABLE public.device_video_shop_group ADD COLUMN device_resolution TEXT DEFAULT NULL;
      END IF;

      -- grid position for collage layout (0=full, 1=top-left, 2=top-right, 3=bottom-left, 4=bottom-right)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device_video_shop_group' AND column_name='grid_position';
      IF NOT FOUND THEN
        ALTER TABLE public.device_video_shop_group ADD COLUMN grid_position INT NOT NULL DEFAULT 0;
      END IF;

      -- grid size percentage (width_percent, height_percent) as JSON e.g. {"w":50,"h":50}
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='device_video_shop_group' AND column_name='grid_size';
      IF NOT FOUND THEN
        ALTER TABLE public.device_video_shop_group ADD COLUMN grid_size TEXT DEFAULT NULL;
      END IF;
    END $$;
    """

    # Device layout configuration table
    ddl_device_layout = """
    CREATE TABLE IF NOT EXISTS public.device_layout (
      id          BIGSERIAL PRIMARY KEY,
      did         BIGINT NOT NULL UNIQUE,
      layout_mode VARCHAR(20) NOT NULL DEFAULT 'single',
      layout_config TEXT DEFAULT NULL,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CONSTRAINT device_layout_device_fk FOREIGN KEY (did) REFERENCES public.device(id) ON DELETE CASCADE
    );
    """

    ddl_device_layout_indexes = """
    CREATE INDEX IF NOT EXISTS idx_device_layout_did ON public.device_layout(did);
    """

    ddl_logs_table = """
    CREATE TABLE IF NOT EXISTS public.device_logs (
      id         BIGSERIAL PRIMARY KEY,
      did        BIGINT NOT NULL,
      log_type   TEXT NOT NULL,  -- 'temperature', 'door_open'
      value      DOUBLE PRECISION,
      logged_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CONSTRAINT device_logs_device_fk FOREIGN KEY (did) REFERENCES public.device(id) ON DELETE CASCADE
    );
    """

    ddl_logs_indexes = """
    CREATE INDEX IF NOT EXISTS idx_device_logs_did ON public.device_logs(did);
    CREATE INDEX IF NOT EXISTS idx_device_logs_type ON public.device_logs(log_type);
    CREATE INDEX IF NOT EXISTS idx_device_logs_logged_at ON public.device_logs(logged_at);
    CREATE INDEX IF NOT EXISTS idx_device_logs_did_type_time ON public.device_logs(did, log_type, logged_at);
    """

    # ------------------------------------------------------------------
    # NEW: device_temperature table for time-series reporting
    # ------------------------------------------------------------------
    ddl_temperature_table = """
    CREATE TABLE IF NOT EXISTS public.device_temperature (
      id          BIGSERIAL PRIMARY KEY,
      did         BIGINT NOT NULL,
      temperature DOUBLE PRECISION NOT NULL,
      recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CONSTRAINT device_temperature_device_fk FOREIGN KEY (did) REFERENCES public.device(id) ON DELETE CASCADE
    );
    """

    ddl_temperature_indexes = """
    CREATE INDEX IF NOT EXISTS idx_device_temperature_did ON public.device_temperature(did);
    CREATE INDEX IF NOT EXISTS idx_device_temperature_recorded_at ON public.device_temperature(recorded_at);
    CREATE INDEX IF NOT EXISTS idx_device_temperature_did_recorded_at ON public.device_temperature(did, recorded_at DESC);
    """

    # ------------------------------------------------------------------
    # NEW: device_online_history table for uptime tracking
    # ------------------------------------------------------------------
    ddl_online_history_table = """
    CREATE TABLE IF NOT EXISTS public.device_online_history (
      id          BIGSERIAL PRIMARY KEY,
      did         BIGINT NOT NULL,
      event_type  TEXT NOT NULL,  -- 'online' or 'offline'
      event_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CONSTRAINT device_online_history_device_fk FOREIGN KEY (did) REFERENCES public.device(id) ON DELETE CASCADE
    );
    """

    ddl_online_history_indexes = """
    CREATE INDEX IF NOT EXISTS idx_device_online_history_did ON public.device_online_history(did);
    CREATE INDEX IF NOT EXISTS idx_device_online_history_event_at ON public.device_online_history(event_at);
    CREATE INDEX IF NOT EXISTS idx_device_online_history_did_event_at ON public.device_online_history(did, event_at DESC);
    """

    drop_old_uniq = """
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname='device_video_shop_group_uniq'
          AND conrelid='public.device_video_shop_group'::regclass
      ) THEN
        ALTER TABLE public.device_video_shop_group
          DROP CONSTRAINT device_video_shop_group_uniq;
      END IF;
    END $$;
    """

    ddl_unique_indexes = """
    CREATE UNIQUE INDEX IF NOT EXISTS dvsg_unique_with_gid
      ON public.device_video_shop_group (did, vid, sid, gid)
      WHERE (gid IS NOT NULL);

    CREATE UNIQUE INDEX IF NOT EXISTS dvsg_unique_no_gid
      ON public.device_video_shop_group (did, vid, sid)
      WHERE (gid IS NULL);
    """

    ddl_trigger = """
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_dvsg_updated_at') THEN
        CREATE TRIGGER trg_dvsg_updated_at
        BEFORE UPDATE ON public.device_video_shop_group
        FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
      END IF;
    END $$;
    """

    ddl_indexes = """
    CREATE INDEX IF NOT EXISTS idx_dvsg_did        ON public.device_video_shop_group(did);
    CREATE INDEX IF NOT EXISTS idx_dvsg_vid        ON public.device_video_shop_group(vid);
    CREATE INDEX IF NOT EXISTS idx_dvsg_sid        ON public.device_video_shop_group(sid);
    CREATE INDEX IF NOT EXISTS idx_dvsg_gid        ON public.device_video_shop_group(gid);
    CREATE INDEX IF NOT EXISTS idx_dvsg_did_status ON public.device_video_shop_group(did, dl_status);
    CREATE INDEX IF NOT EXISTS idx_dvsg_display_order ON public.device_video_shop_group(display_order);
    """

    # ------------------------------------------------------------------
    # User Management Tables
    # ------------------------------------------------------------------
    ddl_users_table = """
    CREATE TABLE IF NOT EXISTS public.users (
      id          BIGSERIAL PRIMARY KEY,
      username    VARCHAR(100) NOT NULL UNIQUE,
      email       VARCHAR(255) UNIQUE,
      password_hash VARCHAR(255) NOT NULL,
      full_name   VARCHAR(255),
      role        VARCHAR(50) NOT NULL DEFAULT 'viewer',
      is_active   BOOLEAN NOT NULL DEFAULT TRUE,
      created_by  BIGINT,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_login  TIMESTAMPTZ,
      CONSTRAINT users_created_by_fk FOREIGN KEY (created_by) REFERENCES public.users(id) ON DELETE SET NULL
    );
    """

    ddl_users_indexes = """
    CREATE INDEX IF NOT EXISTS idx_users_username ON public.users(username);
    CREATE INDEX IF NOT EXISTS idx_users_email ON public.users(email);
    CREATE INDEX IF NOT EXISTS idx_users_role ON public.users(role);
    CREATE INDEX IF NOT EXISTS idx_users_is_active ON public.users(is_active);
    """

    ddl_user_permissions_table = """
    CREATE TABLE IF NOT EXISTS public.user_permissions (
      id          BIGSERIAL PRIMARY KEY,
      user_id     BIGINT NOT NULL,
      permission  VARCHAR(100) NOT NULL,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CONSTRAINT user_permissions_user_fk FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE,
      CONSTRAINT user_permissions_unique UNIQUE (user_id, permission)
    );
    """

    ddl_user_permissions_indexes = """
    CREATE INDEX IF NOT EXISTS idx_user_permissions_user_id ON public.user_permissions(user_id);
    CREATE INDEX IF NOT EXISTS idx_user_permissions_permission ON public.user_permissions(permission);
    """

    # Default admin user (password: admin123) - SHA256 hash of "digix_salt_admin123"
    ddl_default_admin = """
    INSERT INTO public.users (username, email, password_hash, full_name, role)
    SELECT 'admin', 'admin@digix.local', '08527c0d38476f4736e3da616b8a0d86a45f0f6c4ce281e3e6213d19c32c052e', 'Administrator', 'admin'
    WHERE NOT EXISTS (SELECT 1 FROM public.users WHERE username = 'admin');
    """

    # ------------------------------------------------------------------
    # NEW: group_video table for storing group-video associations
    # independent of device assignments
    # ------------------------------------------------------------------
    ddl_group_video_table = """
    CREATE TABLE IF NOT EXISTS public.group_video (
        id BIGSERIAL PRIMARY KEY,
        gid BIGINT NOT NULL REFERENCES public."group"(id) ON DELETE CASCADE,
        vid BIGINT NOT NULL REFERENCES public.video(id) ON DELETE CASCADE,
        display_order INT NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(gid, vid)
    );
    """

    ddl_group_video_indexes = """
    CREATE INDEX IF NOT EXISTS idx_group_video_gid ON public.group_video(gid);
    CREATE INDEX IF NOT EXISTS idx_group_video_vid ON public.group_video(vid);
    """

    ddl_group_video_trigger = """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_group_video_updated_at') THEN
            CREATE TRIGGER trg_group_video_updated_at
            BEFORE UPDATE ON public.group_video
            FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
        END IF;
    END $$;
    """

    # ------------------------------------------------------------------
    # NEW: advertisement table for image advertisements
    # ------------------------------------------------------------------
    ddl_advertisement_table = """
    CREATE TABLE IF NOT EXISTS public.advertisement (
        id BIGSERIAL PRIMARY KEY,
        ad_name TEXT NOT NULL UNIQUE,
        s3_link TEXT NOT NULL,
        rotation INT NOT NULL DEFAULT 0,
        fit_mode TEXT NOT NULL DEFAULT 'cover',
        display_duration INT NOT NULL DEFAULT 10,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE UNIQUE INDEX IF NOT EXISTS uq_advertisement_ad_name ON public.advertisement (ad_name);
    """

    ddl_advertisement_trigger = """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_advertisement_updated_at') THEN
            CREATE TRIGGER trg_advertisement_updated_at
            BEFORE UPDATE ON public.advertisement
            FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
        END IF;
    END $$;
    """

    # ------------------------------------------------------------------
    # NEW: group_advertisement table for group-advertisement associations
    # ------------------------------------------------------------------
    ddl_group_advertisement_table = """
    CREATE TABLE IF NOT EXISTS public.group_advertisement (
        id BIGSERIAL PRIMARY KEY,
        gid BIGINT NOT NULL REFERENCES public."group"(id) ON DELETE CASCADE,
        aid BIGINT NOT NULL REFERENCES public.advertisement(id) ON DELETE CASCADE,
        display_order INT NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(gid, aid)
    );
    """

    ddl_group_advertisement_indexes = """
    CREATE INDEX IF NOT EXISTS idx_group_advertisement_gid ON public.group_advertisement(gid);
    CREATE INDEX IF NOT EXISTS idx_group_advertisement_aid ON public.group_advertisement(aid);
    """

    ddl_group_advertisement_trigger = """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_group_advertisement_updated_at') THEN
            CREATE TRIGGER trg_group_advertisement_updated_at
            BEFORE UPDATE ON public.group_advertisement
            FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
        END IF;
    END $$;
    """

    # ------------------------------------------------------------------
    # NEW: device_advertisement_shop_group table for device-advertisement links
    # ------------------------------------------------------------------
    ddl_device_advertisement_link_table = """
    CREATE TABLE IF NOT EXISTS public.device_advertisement_shop_group (
        id BIGSERIAL PRIMARY KEY,
        did BIGINT NOT NULL REFERENCES public.device(id) ON DELETE CASCADE,
        aid BIGINT NOT NULL REFERENCES public.advertisement(id) ON DELETE CASCADE,
        sid BIGINT NOT NULL REFERENCES public.shop(id) ON DELETE RESTRICT,
        gid BIGINT REFERENCES public."group"(id) ON DELETE RESTRICT,
        display_order INT NOT NULL DEFAULT 0,
        device_rotation INT DEFAULT NULL,
        grid_position INT NOT NULL DEFAULT 0,
        grid_size TEXT DEFAULT NULL,
        dl_status BOOLEAN NOT NULL DEFAULT FALSE,
        dl_at TIMESTAMPTZ,
        dl_error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    ddl_device_advertisement_link_indexes = """
    CREATE INDEX IF NOT EXISTS idx_dasg_did ON public.device_advertisement_shop_group(did);
    CREATE INDEX IF NOT EXISTS idx_dasg_aid ON public.device_advertisement_shop_group(aid);
    CREATE INDEX IF NOT EXISTS idx_dasg_sid ON public.device_advertisement_shop_group(sid);
    CREATE INDEX IF NOT EXISTS idx_dasg_gid ON public.device_advertisement_shop_group(gid);
    CREATE UNIQUE INDEX IF NOT EXISTS dasg_unique_with_gid ON public.device_advertisement_shop_group (did, aid, sid, gid) WHERE (gid IS NOT NULL);
    CREATE UNIQUE INDEX IF NOT EXISTS dasg_unique_no_gid ON public.device_advertisement_shop_group (did, aid, sid) WHERE (gid IS NULL);
    """

    ddl_device_advertisement_link_trigger = """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_dasg_updated_at') THEN
            CREATE TRIGGER trg_dasg_updated_at
            BEFORE UPDATE ON public.device_advertisement_shop_group
            FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
        END IF;
    END $$;
    """

    # ------------------------------------------------------------------
    # NEW: device_assignment table for tracking device-group-shop
    # associations independently of video links
    # ------------------------------------------------------------------
    ddl_device_assignment_table = """
    CREATE TABLE IF NOT EXISTS public.device_assignment (
        id BIGSERIAL PRIMARY KEY,
        did BIGINT NOT NULL REFERENCES public.device(id) ON DELETE CASCADE,
        gid BIGINT REFERENCES public."group"(id) ON DELETE SET NULL,
        sid BIGINT REFERENCES public.shop(id) ON DELETE SET NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(did)
    );
    """

    ddl_device_assignment_indexes = """
    CREATE INDEX IF NOT EXISTS idx_device_assignment_did ON public.device_assignment(did);
    CREATE INDEX IF NOT EXISTS idx_device_assignment_gid ON public.device_assignment(gid);
    CREATE INDEX IF NOT EXISTS idx_device_assignment_sid ON public.device_assignment(sid);
    """

    ddl_device_assignment_trigger = """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_device_assignment_updated_at') THEN
            CREATE TRIGGER trg_device_assignment_updated_at
            BEFORE UPDATE ON public.device_assignment
            FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
        END IF;
    END $$;
    """

    with conn.cursor() as cur:
        cur.execute(ddl_fn)

        # Create base tables first (idempotent - IF NOT EXISTS)
        cur.execute(ddl_base_device)
        cur.execute(ddl_base_video)
        cur.execute(ddl_base_shop)
        cur.execute(ddl_base_group)

        # Then run ALTER TABLE migrations
        cur.execute(ddl_device_cols)
        cur.execute(ddl_video_cols)
        cur.execute(ddl_shop_cols)
        cur.execute(ddl_link_table)
        cur.execute(ddl_link_cols)
        cur.execute(ddl_logs_table)
        cur.execute(ddl_logs_indexes)

        # NEW
        cur.execute(ddl_temperature_table)
        cur.execute(ddl_temperature_indexes)
        cur.execute(ddl_online_history_table)
        cur.execute(ddl_online_history_indexes)

        cur.execute(drop_old_uniq)
        cur.execute(ddl_unique_indexes)
        cur.execute(ddl_trigger)
        cur.execute(ddl_indexes)

        # User Management
        cur.execute(ddl_users_table)
        cur.execute(ddl_users_indexes)
        cur.execute(ddl_user_permissions_table)
        cur.execute(ddl_user_permissions_indexes)
        cur.execute(ddl_default_admin)

        # Device Layout
        cur.execute(ddl_device_layout)
        cur.execute(ddl_device_layout_indexes)

        # Group-Video associations (independent of devices)
        cur.execute(ddl_group_video_table)
        cur.execute(ddl_group_video_indexes)
        cur.execute(ddl_group_video_trigger)

        # Device-Group-Shop assignments (independent of video links)
        cur.execute(ddl_device_assignment_table)
        cur.execute(ddl_device_assignment_indexes)
        cur.execute(ddl_device_assignment_trigger)

        # Advertisement tables
        cur.execute(ddl_advertisement_table)
        cur.execute(ddl_advertisement_trigger)
        cur.execute(ddl_group_advertisement_table)
        cur.execute(ddl_group_advertisement_indexes)
        cur.execute(ddl_group_advertisement_trigger)
        cur.execute(ddl_device_advertisement_link_table)
        cur.execute(ddl_device_advertisement_link_indexes)
        cur.execute(ddl_device_advertisement_link_trigger)

    conn.commit()
