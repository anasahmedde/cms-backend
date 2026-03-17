"""
Migration: Add device_resolution_cache table

Stores screen resolutions reported by Android devices BEFORE they are
enrolled in the CMS.  The enrollment check (GET /device/{id}/online)
fires every 15 s even while the device is unregistered, so by the time
an admin opens the "Add Device" modal and types the Mobile ID, the
resolution is already cached here and can be pre-filled automatically.

On device creation the cache row is consumed and written to device.resolution.
"""

DEVICE_RESOLUTION_CACHE_SQL = """
CREATE TABLE IF NOT EXISTS public.device_resolution_cache (
    mobile_id   TEXT        PRIMARY KEY,
    resolution  TEXT        NOT NULL,
    reported_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


async def run_migration(conn):
    """Apply migration. Safe to run multiple times (IF NOT EXISTS)."""
    await conn.execute(DEVICE_RESOLUTION_CACHE_SQL)


def run_migration_sync(cursor):
    """Synchronous version for psycopg2 cursor."""
    cursor.execute(DEVICE_RESOLUTION_CACHE_SQL)
