"""
Migration: Add wipe_pending column to device table
Adds remote wipe functionality — device checks this flag on heartbeat
and wipes local content when set to TRUE.

NOTE: This migration was already applied manually via Adminer.
      This file exists for documentation and idempotent re-run safety.
"""

WIPE_PENDING_MIGRATION_SQL = """
-- ============================================================================
-- SQL MIGRATION: Add wipe_pending column to device table
-- ============================================================================

-- Add wipe_pending column to device table for remote wipe functionality
ALTER TABLE public.device ADD COLUMN IF NOT EXISTS wipe_pending BOOLEAN DEFAULT FALSE;

-- Create index for faster lookup (only index rows where wipe is pending)
CREATE INDEX IF NOT EXISTS idx_device_wipe_pending ON public.device (wipe_pending) WHERE wipe_pending = TRUE;
"""


async def run_migration(conn):
    """Apply wipe_pending migration. Safe to run multiple times (IF NOT EXISTS)."""
    await conn.execute(WIPE_PENDING_MIGRATION_SQL)


def run_migration_sync(cursor):
    """Synchronous version for psycopg2 cursor."""
    cursor.execute(WIPE_PENDING_MIGRATION_SQL)
