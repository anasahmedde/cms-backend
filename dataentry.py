#!/usr/bin/env python3
"""
populate_temperature_data.py

This script populates random temperature data for devices in the DGX system.
It creates realistic temperature readings that vary throughout the day.

Usage:
    python populate_temperature_data.py --device-id <mobile_id> --hours 24
    python populate_temperature_data.py --all-devices --hours 48
    python populate_temperature_data.py --device-id <mobile_id> --days 7 --interval 300

Requirements:
    pip install psycopg2-binary python-dotenv

Environment Variables (or use .env file):
    PGHOST - PostgreSQL host (default: localhost)
    PGPORT - PostgreSQL port (default: 5432)
    PGDATABASE - Database name (default: dgx)
    PGUSER - Database user (default: app_user)
    PGPASSWORD - Database password (default: strongpassword)
"""

import os
import sys
import random
import argparse
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("Error: psycopg2 is required. Install with: pip install psycopg2-binary")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv is optional

# Database configuration
DB_CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "dgx"),
    "user": os.getenv("PGUSER", "app_user"),
    "password": os.getenv("PGPASSWORD", "strongpassword"),
}


def get_connection():
    """Create and return a database connection."""
    return psycopg2.connect(**DB_CONFIG)


def get_device_id(conn, mobile_id: str) -> Optional[int]:
    """Get the device ID from mobile_id."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM public.device WHERE mobile_id = %s ORDER BY id DESC LIMIT 1;",
            (mobile_id,)
        )
        row = cur.fetchone()
        return row[0] if row else None


def get_all_device_ids(conn) -> List[Tuple[int, str]]:
    """Get all device IDs and mobile_ids."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, mobile_id FROM public.device ORDER BY id;")
        return cur.fetchall()


def generate_temperature(hour: int, base_temp: float = 22.0, variation: float = 5.0) -> float:
    """
    Generate a realistic temperature based on time of day.
    
    Temperature tends to be:
    - Cooler in early morning (4-7 AM)
    - Warming up during day (8 AM - 2 PM)
    - Hottest in afternoon (2-5 PM)
    - Cooling down in evening (5-10 PM)
    - Cool at night (10 PM - 4 AM)
    """
    # Base daily pattern (sinusoidal with peak at 3 PM / hour 15)
    import math
    daily_pattern = math.sin((hour - 6) * math.pi / 12) * (variation * 0.6)
    
    # Random noise
    noise = random.gauss(0, variation * 0.2)
    
    # Occasional spikes (simulate door opening, etc.)
    spike = 0
    if random.random() < 0.05:  # 5% chance of a spike
        spike = random.choice([-3, -2, 2, 3, 4, 5])
    
    temp = base_temp + daily_pattern + noise + spike
    
    # Clamp to reasonable range
    return round(max(15.0, min(35.0, temp)), 2)


def generate_temperature_data(
    device_id: int,
    hours: int = 24,
    interval_seconds: int = 300,  # 5 minutes default
    base_temp: float = 22.0,
    variation: float = 5.0,
) -> List[Tuple[int, str, float, datetime]]:
    """
    Generate temperature data points for a device.
    
    Args:
        device_id: The device ID
        hours: Number of hours of data to generate
        interval_seconds: Seconds between readings
        base_temp: Base temperature in Celsius
        variation: Temperature variation range
    
    Returns:
        List of (device_id, log_type, value, logged_at) tuples
    """
    data = []
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=hours)
    current_time = start_time
    
    while current_time <= end_time:
        hour = current_time.hour
        temp = generate_temperature(hour, base_temp, variation)
        data.append((device_id, "temperature", temp, current_time))
        current_time += timedelta(seconds=interval_seconds)
    
    return data


def insert_temperature_data(conn, data: List[Tuple[int, str, float, datetime]]) -> int:
    """
    Insert temperature data into device_logs table.
    
    Returns:
        Number of rows inserted
    """
    if not data:
        return 0
    
    with conn.cursor() as cur:
        # Ensure the device_logs table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.device_logs (
                id BIGSERIAL PRIMARY KEY,
                did BIGINT NOT NULL,
                log_type TEXT NOT NULL,
                value NUMERIC,
                logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        
        # Create index if not exists
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_device_logs_did_type_time 
            ON public.device_logs (did, log_type, logged_at DESC);
        """)
        
        # Insert data
        execute_values(
            cur,
            """
            INSERT INTO public.device_logs (did, log_type, value, logged_at)
            VALUES %s
            """,
            data,
            template="(%s, %s, %s, %s)"
        )
        
        conn.commit()
        return len(data)


def clear_temperature_data(conn, device_id: int, hours: Optional[int] = None) -> int:
    """
    Clear temperature data for a device.
    
    Args:
        conn: Database connection
        device_id: The device ID
        hours: If provided, only clear data from the last N hours
    
    Returns:
        Number of rows deleted
    """
    with conn.cursor() as cur:
        if hours:
            cur.execute(
                """
                DELETE FROM public.device_logs 
                WHERE did = %s 
                AND log_type = 'temperature'
                AND logged_at >= NOW() - INTERVAL '%s hours'
                """,
                (device_id, hours)
            )
        else:
            cur.execute(
                "DELETE FROM public.device_logs WHERE did = %s AND log_type = 'temperature'",
                (device_id,)
            )
        deleted = cur.rowcount
        conn.commit()
        return deleted


def main():
    parser = argparse.ArgumentParser(
        description="Populate random temperature data for DGX devices",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Add 24 hours of data for a specific device
  python populate_temperature_data.py --device-id abc123 --hours 24

  # Add 7 days of data with 10-minute intervals
  python populate_temperature_data.py --device-id abc123 --days 7 --interval 600

  # Add data for all devices
  python populate_temperature_data.py --all-devices --hours 48

  # Clear and regenerate data
  python populate_temperature_data.py --device-id abc123 --hours 24 --clear

  # Custom temperature range (cooler environment)
  python populate_temperature_data.py --device-id abc123 --hours 24 --base-temp 18 --variation 3

  # List all devices
  python populate_temperature_data.py --list-devices
        """
    )
    
    # Device selection
    device_group = parser.add_mutually_exclusive_group()
    device_group.add_argument(
        "--device-id", "-d",
        type=str,
        help="Mobile ID of the device to populate data for"
    )
    device_group.add_argument(
        "--all-devices", "-a",
        action="store_true",
        help="Populate data for all devices"
    )
    device_group.add_argument(
        "--list-devices", "-l",
        action="store_true",
        help="List all available devices"
    )
    
    # Time range
    time_group = parser.add_mutually_exclusive_group()
    time_group.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Number of hours of data to generate (default: 24)"
    )
    time_group.add_argument(
        "--days",
        type=int,
        help="Number of days of data to generate"
    )
    
    # Data options
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Interval between readings in seconds (default: 300 = 5 minutes)"
    )
    parser.add_argument(
        "--base-temp",
        type=float,
        default=22.0,
        help="Base temperature in Celsius (default: 22.0)"
    )
    parser.add_argument(
        "--variation",
        type=float,
        default=5.0,
        help="Temperature variation range (default: 5.0)"
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear existing temperature data before inserting new data"
    )
    
    # Database options
    parser.add_argument(
        "--host",
        type=str,
        help="Database host (overrides PGHOST env var)"
    )
    parser.add_argument(
        "--port",
        type=int,
        help="Database port (overrides PGPORT env var)"
    )
    parser.add_argument(
        "--database",
        type=str,
        help="Database name (overrides PGDATABASE env var)"
    )
    parser.add_argument(
        "--user",
        type=str,
        help="Database user (overrides PGUSER env var)"
    )
    parser.add_argument(
        "--password",
        type=str,
        help="Database password (overrides PGPASSWORD env var)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )
    
    args = parser.parse_args()
    
    # Update DB config with command line args
    if args.host:
        DB_CONFIG["host"] = args.host
    if args.port:
        DB_CONFIG["port"] = args.port
    if args.database:
        DB_CONFIG["dbname"] = args.database
    if args.user:
        DB_CONFIG["user"] = args.user
    if args.password:
        DB_CONFIG["password"] = args.password
    
    # Calculate hours
    hours = args.hours
    if args.days:
        hours = args.days * 24
    
    try:
        conn = get_connection()
        print(f"Connected to database: {DB_CONFIG['dbname']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}")
        
        # List devices mode
        if args.list_devices:
            devices = get_all_device_ids(conn)
            if not devices:
                print("No devices found in database.")
            else:
                print(f"\nFound {len(devices)} device(s):\n")
                print(f"{'ID':<10} {'Mobile ID':<40}")
                print("-" * 50)
                for did, mobile_id in devices:
                    print(f"{did:<10} {mobile_id:<40}")
            conn.close()
            return
        
        # Validate we have a device selection
        if not args.device_id and not args.all_devices:
            parser.error("Please specify --device-id, --all-devices, or --list-devices")
        
        # Get devices to process
        if args.all_devices:
            devices = get_all_device_ids(conn)
            if not devices:
                print("No devices found in database.")
                conn.close()
                return
            print(f"Processing {len(devices)} device(s)...")
        else:
            did = get_device_id(conn, args.device_id)
            if not did:
                print(f"Error: Device with mobile_id '{args.device_id}' not found.")
                conn.close()
                sys.exit(1)
            devices = [(did, args.device_id)]
        
        total_inserted = 0
        total_cleared = 0
        
        for did, mobile_id in devices:
            if args.verbose:
                print(f"\nProcessing device: {mobile_id} (ID: {did})")
            
            # Clear existing data if requested
            if args.clear:
                cleared = clear_temperature_data(conn, did, hours)
                total_cleared += cleared
                if args.verbose:
                    print(f"  Cleared {cleared} existing temperature records")
            
            # Generate new data
            data = generate_temperature_data(
                device_id=did,
                hours=hours,
                interval_seconds=args.interval,
                base_temp=args.base_temp,
                variation=args.variation,
            )
            
            # Insert data
            inserted = insert_temperature_data(conn, data)
            total_inserted += inserted
            
            if args.verbose:
                print(f"  Inserted {inserted} temperature records")
                if data:
                    temps = [d[2] for d in data]
                    print(f"  Temperature range: {min(temps):.1f}°C - {max(temps):.1f}°C")
                    print(f"  Average: {sum(temps)/len(temps):.1f}°C")
        
        # Summary
        print(f"\n{'='*50}")
        print(f"Summary:")
        print(f"  Devices processed: {len(devices)}")
        print(f"  Time range: {hours} hours ({hours/24:.1f} days)")
        print(f"  Interval: {args.interval} seconds ({args.interval/60:.1f} minutes)")
        if args.clear:
            print(f"  Records cleared: {total_cleared}")
        print(f"  Records inserted: {total_inserted}")
        print(f"  Base temperature: {args.base_temp}°C")
        print(f"  Variation: ±{args.variation}°C")
        
        conn.close()
        print("\nDone!")
        
    except psycopg2.OperationalError as e:
        print(f"Database connection error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
