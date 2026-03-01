#!/usr/bin/env python3
"""
Test script for company expiration logic.
Run this on your server to verify the expiration checking works correctly.

Usage:
    python test_expiration.py <company_id>
    
Example:
    python test_expiration.py 2  # Test company ID 2 (digix)
"""

import sys
import os
from datetime import datetime, timezone

# Add the backend directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_expiration(company_id: int):
    """Test the company expiration check for a given company."""
    
    # Import the functions
    from company_expiration_api import check_company_access, calculate_expiration_status
    from database import pg_conn
    
    print(f"\n{'='*60}")
    print(f"Testing Company Expiration for Company ID: {company_id}")
    print(f"{'='*60}\n")
    
    # First, get raw company data from database
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, slug, name, expires_at, grace_period_days, 
                       expiration_status, suspended_at, status
                FROM public.company WHERE id = %s;
            """, (company_id,))
            row = cur.fetchone()
            
            if not row:
                print(f"ERROR: Company ID {company_id} not found!")
                return
            
            cid, slug, name, expires_at, grace_days, exp_status, suspended_at, status = row
            
            print(f"Company: {name} ({slug})")
            print(f"  - Database status: {status}")
            print(f"  - Database expiration_status: {exp_status}")
            print(f"  - expires_at: {expires_at}")
            print(f"  - grace_period_days: {grace_days}")
            print(f"  - suspended_at: {suspended_at}")
            print()
            
            # Calculate current time
            now = datetime.now(timezone.utc)
            print(f"Current time (UTC): {now}")
            
            if expires_at:
                # Make sure expires_at is timezone-aware for comparison
                if expires_at.tzinfo is None:
                    expires_at_aware = expires_at.replace(tzinfo=timezone.utc)
                else:
                    expires_at_aware = expires_at
                    
                diff = now - expires_at_aware
                print(f"Time since expiration: {diff}")
                print(f"Days since expiration: {diff.days}")
            print()
    
    # Now test the check_company_access function
    print("Testing check_company_access():")
    result = check_company_access(company_id)
    print(f"  Result: {result}")
    print()
    
    # Test calculate_expiration_status directly
    effective_grace = grace_days if grace_days is not None else 7
    print(f"Testing calculate_expiration_status(expires_at={expires_at}, grace_days={effective_grace}, suspended_at={suspended_at}):")
    calc_status, accessible, days_until, days_since = calculate_expiration_status(
        expires_at, effective_grace, suspended_at
    )
    print(f"  calc_status: {calc_status}")
    print(f"  accessible: {accessible}")
    print(f"  days_until: {days_until}")
    print(f"  days_since: {days_since}")
    print()
    
    # Summary
    print(f"{'='*60}")
    if result["accessible"]:
        print(f"✅ COMPANY IS ACCESSIBLE - Users CAN login, devices CAN work")
    else:
        print(f"🚫 COMPANY IS BLOCKED - Users CANNOT login, devices show enrollment screen")
    print(f"{'='*60}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_expiration.py <company_id>")
        print("Example: python test_expiration.py 2")
        sys.exit(1)
    
    try:
        company_id = int(sys.argv[1])
        test_expiration(company_id)
    except ValueError:
        print(f"Error: '{sys.argv[1]}' is not a valid company ID")
        sys.exit(1)
