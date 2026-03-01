# migrations/company_expiration_schema.py
# Location: cms-backend-staging/migrations/company_expiration_schema.py
"""
Company Expiration Schema
=========================
Adds expiration functionality to companies:
- expires_at: When company subscription/license expires
- expiration_status: active, expired, grace_period
- grace_period_days: Days after expiration before full block
- expiration_notified_at: When we sent expiration warning

When company expires:
- All users cannot login
- All devices show "Not Enrolled" 
- Company appears in "Expired" section in super admin dashboard

This migration is fully idempotent - safe to run multiple times.
"""


def ensure_company_expiration_schema(conn):
    """
    Apply company expiration schema changes.
    Must run AFTER ensure_multitenant_schema().
    """

    # ══════════════════════════════════════════════════════════════
    # COMPANY EXPIRATION FIELDS
    # ══════════════════════════════════════════════════════════════
    
    ddl_company_expiration = """
    DO $$
    DECLARE t text;
    BEGIN
      -- expires_at: When company subscription expires
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='company' AND column_name='expires_at';
      IF NOT FOUND THEN
        ALTER TABLE public.company ADD COLUMN expires_at TIMESTAMPTZ DEFAULT NULL;
      END IF;

      -- expiration_status: 'active', 'grace_period', 'expired', 'suspended'
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='company' AND column_name='expiration_status';
      IF NOT FOUND THEN
        ALTER TABLE public.company ADD COLUMN expiration_status VARCHAR(20) DEFAULT 'active';
      END IF;

      -- grace_period_days: Days after expiration before full block (default 7 days)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='company' AND column_name='grace_period_days';
      IF NOT FOUND THEN
        ALTER TABLE public.company ADD COLUMN grace_period_days INT DEFAULT 7;
      END IF;

      -- expiration_notified_at: When we sent expiration warning email
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='company' AND column_name='expiration_notified_at';
      IF NOT FOUND THEN
        ALTER TABLE public.company ADD COLUMN expiration_notified_at TIMESTAMPTZ DEFAULT NULL;
      END IF;

      -- expiration_notify_email: Email to notify about expiration (defaults to company admin)
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='company' AND column_name='expiration_notify_email';
      IF NOT FOUND THEN
        ALTER TABLE public.company ADD COLUMN expiration_notify_email TEXT DEFAULT NULL;
      END IF;

      -- suspended_at: When company was manually suspended
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='company' AND column_name='suspended_at';
      IF NOT FOUND THEN
        ALTER TABLE public.company ADD COLUMN suspended_at TIMESTAMPTZ DEFAULT NULL;
      END IF;

      -- suspension_reason: Why company was suspended
      SELECT 1 INTO t FROM information_schema.columns
       WHERE table_schema='public' AND table_name='company' AND column_name='suspension_reason';
      IF NOT FOUND THEN
        ALTER TABLE public.company ADD COLUMN suspension_reason TEXT DEFAULT NULL;
      END IF;
    END $$;
    
    -- Indexes for efficient queries
    CREATE INDEX IF NOT EXISTS idx_company_expires_at ON public.company(expires_at) WHERE expires_at IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_company_expiration_status ON public.company(expiration_status);
    CREATE INDEX IF NOT EXISTS idx_company_expired ON public.company(expires_at, expiration_status) 
        WHERE expires_at IS NOT NULL AND expiration_status != 'active';
    """

    # ══════════════════════════════════════════════════════════════
    # COMPANY EXPIRATION HISTORY LOG
    # ══════════════════════════════════════════════════════════════
    
    ddl_company_expiration_log = """
    CREATE TABLE IF NOT EXISTS public.company_expiration_log (
        id              BIGSERIAL PRIMARY KEY,
        company_id      BIGINT NOT NULL,
        
        -- What happened
        event_type      VARCHAR(30) NOT NULL,  -- 'expired', 'renewed', 'suspended', 'reactivated', 'grace_started', 'warning_sent'
        
        -- Details
        old_expires_at  TIMESTAMPTZ,
        new_expires_at  TIMESTAMPTZ,
        old_status      VARCHAR(20),
        new_status      VARCHAR(20),
        
        -- Who did it
        performed_by    BIGINT,  -- user_id (NULL for system actions)
        performed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        
        -- Additional info
        notes           TEXT,
        
        CONSTRAINT cel_company_fk FOREIGN KEY (company_id)
            REFERENCES public.company(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_cel_company ON public.company_expiration_log(company_id);
    CREATE INDEX IF NOT EXISTS idx_cel_event_type ON public.company_expiration_log(event_type);
    CREATE INDEX IF NOT EXISTS idx_cel_performed_at ON public.company_expiration_log(performed_at);
    """

    # ══════════════════════════════════════════════════════════════
    # EXECUTE ALL DDL
    # ══════════════════════════════════════════════════════════════
    
    with conn.cursor() as cur:
        cur.execute(ddl_company_expiration)
        cur.execute(ddl_company_expiration_log)
        
    conn.commit()
    print("[MIGRATION] Company expiration schema applied successfully")
