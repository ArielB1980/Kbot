import os
import sys
from datetime import datetime, timezone
from sqlalchemy import text

# Robustly find project root relative to this script
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)

if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from src.storage.db import get_db
except ImportError as e:
    print(f"❌ Could not import src modules: {e}")
    sys.exit(1)

def check_freshness():
    print("Connecting to Database...")
    try:
        db = get_db()
        # Ensure engine is created
        if not db.engine:
            print("❌ Database engine not initialized")
            return
            
        with db.engine.connect() as conn:
            print("\n--- GLOBAL FRESHNESS CHECK ---")
            
            # 1. Latest Heartbeat
            row = conn.execute(text("""
                SELECT timestamp, symbol, details 
                FROM system_events 
                WHERE event_type = 'DECISION_TRACE' 
                ORDER BY timestamp DESC 
                LIMIT 1
            """)).mappings().first()
            
            # Check DB time to avoid clock skew
            db_now = conn.execute(text("SELECT NOW()")).scalar()
            
            if row:
                timestamp = row['timestamp']
                symbol = row['symbol']
                
                # Normalize Timezones
                # 1. Convert DB Now to Aware UTC if naive
                if db_now.tzinfo is None:
                    db_now = db_now.replace(tzinfo=timezone.utc)
                
                # 2. Convert Event Timestamp to Aware UTC if naive
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                
                age = (db_now - timestamp).total_seconds() / 60
                
                print(f"Latest Trace: {timestamp}")
                print(f"Server Time:  {db_now}")
                print(f"Age:          {age:.1f} minutes")
                print(f"Symbol:       {symbol}")
                
                if age > 15:
                    print(f"❌ CRITICAL: System appears STALLED (No traces in {age:.1f} mins)")
                else:
                    print("✅ System appears ALIVE")
            else:
                print("❌ CRITICAL: No traces found in database whatsoever.")

            # 2. Check Trace Volume
            count = conn.execute(text("""
                SELECT COUNT(*) 
                FROM system_events 
                WHERE event_type = 'DECISION_TRACE' 
                AND timestamp > NOW() - INTERVAL '1 hour'
            """)).scalar()
            print(f"Traces in last hour: {count}")
            
            # 3. Check Recent Errors
            print("\n--- RECENT ERRORS (Last 2 Hours) ---")
            errors = conn.execute(text("""
                SELECT timestamp, details 
                FROM system_events 
                WHERE event_type IN ('ERROR', 'CRITICAL') 
                AND timestamp > NOW() - INTERVAL '2 hours'
                ORDER BY timestamp DESC
                LIMIT 5
            """)).mappings().all()
            
            if errors:
                for e in errors:
                    print(f"[{e['timestamp']}] {e['details']}")
            else:
                print("✅ No database errors logged in last 2 hours.")

    except Exception as e:
        print(f"❌ Database Query Error: {e}")

if __name__ == "__main__":
    check_freshness()
