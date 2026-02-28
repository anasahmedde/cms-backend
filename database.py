# database.py
# Location: cms-backend-staging/database.py
# Proper Connection Pool Management for DIGIX CMS

import os
import asyncio
from contextlib import asynccontextmanager, contextmanager
from typing import Optional, AsyncGenerator, Generator
from datetime import datetime

import asyncpg
from asyncpg import Pool, Connection
import psycopg2
from psycopg2.pool import ThreadedConnectionPool, PoolError
from dotenv import load_dotenv

load_dotenv()

# ===========================================
# Configuration
# ===========================================
DB_CONFIG = {
    "host": os.getenv("PGHOST", "172.31.17.177"),
    "port": int(os.getenv("PGPORT", "5432")),
    "database": os.getenv("PGDATABASE", "dgx"),
    "user": os.getenv("PGUSER", "app_user"),
    "password": os.getenv("PGPASSWORD", "strongpassword"),
}

POOL_CONFIG = {
    "min_size": int(os.getenv("PG_MIN_CONN", "2")),
    "max_size": int(os.getenv("PG_MAX_CONN", "20")),
    "max_inactive_connection_lifetime": 300,  # 5 minutes
    "command_timeout": 60,  # 60 second query timeout
}


# ===========================================
# Async Pool (for WebSocket and async endpoints)
# ===========================================
class AsyncDatabasePool:
    """
    Async connection pool using asyncpg.
    Best for WebSocket handlers and async endpoints.
    """
    
    _pool: Optional[Pool] = None
    _lock = asyncio.Lock()
    
    @classmethod
    async def get_pool(cls) -> Pool:
        """Get or create the async connection pool."""
        if cls._pool is None:
            async with cls._lock:
                if cls._pool is None:
                    cls._pool = await asyncpg.create_pool(
                        host=DB_CONFIG["host"],
                        port=DB_CONFIG["port"],
                        database=DB_CONFIG["database"],
                        user=DB_CONFIG["user"],
                        password=DB_CONFIG["password"],
                        min_size=POOL_CONFIG["min_size"],
                        max_size=POOL_CONFIG["max_size"],
                        max_inactive_connection_lifetime=POOL_CONFIG["max_inactive_connection_lifetime"],
                        command_timeout=POOL_CONFIG["command_timeout"],
                    )
                    print(f"[DB] Async pool created: min={POOL_CONFIG['min_size']}, max={POOL_CONFIG['max_size']}")
        return cls._pool
    
    @classmethod
    async def close_pool(cls):
        """Close the async pool gracefully."""
        if cls._pool is not None:
            await cls._pool.close()
            cls._pool = None
            print("[DB] Async pool closed")
    
    @classmethod
    @asynccontextmanager
    async def connection(cls) -> AsyncGenerator[Connection, None]:
        """
        Get a connection from the pool.
        Usage:
            async with AsyncDatabasePool.connection() as conn:
                result = await conn.fetch("SELECT * FROM device")
        """
        pool = await cls.get_pool()
        async with pool.acquire() as conn:
            yield conn
    
    @classmethod
    @asynccontextmanager
    async def transaction(cls) -> AsyncGenerator[Connection, None]:
        """
        Get a connection with automatic transaction management.
        Commits on success, rolls back on exception.
        """
        pool = await cls.get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                yield conn
    
    @classmethod
    async def get_pool_stats(cls) -> dict:
        """Get current pool statistics."""
        if cls._pool is None:
            return {"status": "not_initialized"}
        
        return {
            "status": "active",
            "size": cls._pool.get_size(),
            "min_size": cls._pool.get_min_size(),
            "max_size": cls._pool.get_max_size(),
            "free_size": cls._pool.get_idle_size(),
            "used_connections": cls._pool.get_size() - cls._pool.get_idle_size(),
        }


# ===========================================
# Sync Pool (for existing sync endpoints)
# ===========================================
class SyncDatabasePool:
    """
    Synchronous connection pool using psycopg2.
    For backward compatibility with existing sync endpoints.
    """
    
    _pool: Optional[ThreadedConnectionPool] = None
    
    @classmethod
    def get_pool(cls) -> ThreadedConnectionPool:
        """Get or create the sync connection pool."""
        if cls._pool is None:
            cls._pool = ThreadedConnectionPool(
                POOL_CONFIG["min_size"],
                POOL_CONFIG["max_size"],
                host=DB_CONFIG["host"],
                port=DB_CONFIG["port"],
                dbname=DB_CONFIG["database"],
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
                connect_timeout=10,
            )
            print(f"[DB] Sync pool created: min={POOL_CONFIG['min_size']}, max={POOL_CONFIG['max_size']}")
        return cls._pool
    
    @classmethod
    def close_pool(cls):
        """Close the sync pool."""
        if cls._pool is not None:
            cls._pool.closeall()
            cls._pool = None
            print("[DB] Sync pool closed")
    
    @classmethod
    @contextmanager
    def connection(cls) -> Generator:
        """
        Get a connection from the pool.
        Usage:
            with SyncDatabasePool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM device")
        """
        pool = cls.get_pool()
        conn = None
        try:
            conn = pool.getconn()
            yield conn
        except PoolError:
            raise Exception("Database connection pool exhausted. Try again later.")
        finally:
            if conn is not None:
                try:
                    pool.putconn(conn)
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass
    
    @classmethod
    def get_pool_stats(cls) -> dict:
        """Get pool statistics (limited for psycopg2)."""
        if cls._pool is None:
            return {"status": "not_initialized"}
        
        return {
            "status": "active",
            "min_size": POOL_CONFIG["min_size"],
            "max_size": POOL_CONFIG["max_size"],
        }


# ===========================================
# Health Check
# ===========================================
async def check_database_health() -> dict:
    """Check database connectivity and pool status."""
    result = {
        "timestamp": datetime.now().isoformat(),
        "async_pool": {},
        "sync_pool": {},
        "database_reachable": False,
    }
    
    # Check async pool
    try:
        async with AsyncDatabasePool.connection() as conn:
            row = await conn.fetchrow("SELECT 1 as health, NOW() as server_time")
            result["database_reachable"] = True
            result["server_time"] = str(row["server_time"])
        result["async_pool"] = await AsyncDatabasePool.get_pool_stats()
    except Exception as e:
        result["async_pool"] = {"error": str(e)}
    
    # Check sync pool
    try:
        result["sync_pool"] = SyncDatabasePool.get_pool_stats()
    except Exception as e:
        result["sync_pool"] = {"error": str(e)}
    
    return result


# ===========================================
# Backward Compatibility Layer
# ===========================================
# These functions provide drop-in replacements for the old pg_conn() function

@contextmanager
def pg_conn():
    """
    Drop-in replacement for the old pg_conn() context manager.
    Use this in existing sync endpoints without any code changes.
    """
    with SyncDatabasePool.connection() as conn:
        yield conn


def pg_init_pool():
    """Initialize the sync pool (called at startup)."""
    SyncDatabasePool.get_pool()


def pg_close_pool():
    """Close the sync pool (called at shutdown)."""
    SyncDatabasePool.close_pool()


# ===========================================
# FastAPI Lifecycle Events
# ===========================================
async def startup_db_pools():
    """Call this in FastAPI startup event."""
    # Initialize both pools
    await AsyncDatabasePool.get_pool()
    SyncDatabasePool.get_pool()
    print("[DB] All connection pools initialized")


async def shutdown_db_pools():
    """Call this in FastAPI shutdown event."""
    await AsyncDatabasePool.close_pool()
    SyncDatabasePool.close_pool()
    print("[DB] All connection pools closed")
