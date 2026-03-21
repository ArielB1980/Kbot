#!/usr/bin/env python3
"""
Check and optionally clear decision traces to force re-analysis.

THIS SCRIPT TARGETS THE 'system_events' TABLE.
It specifically looks for event_type='DECISION_TRACE'.
"""
import os
import sys
import json
from datetime import datetime, timedelta, timezone
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.storage.db import get_db
from src.monitoring.logger import get_logger

logger = get_logger(__name__)


def check_regime_distribution():
    """Check current regime distribution in decision traces."""
    print("\n📊 Current Regime Distribution in Database (Table: system_events):\n")
    print("=" * 60)
    
    db = get_db()
    with db.get_session() as session:
        # Check database type to determine JSON syntax
        is_postgres = "postgres" in str(db.engine.url)
        
        try:
            # We filter by event_type = 'DECISION_TRACE'
            # Since 'details' is a String column containing JSON, we need to handle parsing.
            # To be safe across DBs/versions, we'll fetch ID/Details and parse in Python
            # unless the dataset is huge. For 300 coins * ~1000 events max history, 
            # fetching just the latest ones is better.
            
            # Let's query distinct latest traces per symbol first
            # But deep grouping is simpler:
            
            if is_postgres:
                # Optimized Postgres Query
                query = text("""
                    SELECT 
                        CAST(details AS JSON)->>'regime' as regime,
                        COUNT(*) as count,
                        MAX(timestamp) as latest_update
                    FROM system_events
                    WHERE event_type = 'DECISION_TRACE'
                    GROUP BY 1
                    ORDER BY 2 DESC;
                """)
                result = session.execute(query)
                rows = result.fetchall()
            else:
                # Fallback: Fetch latest 1000 traces and aggregage in Python
                # (For SQLite or other DBs if JSON operators fail)
                print("⚠️  Non-Postgres DB detected or fallback mode. Fetching recent events...")
                query = text("""
                    SELECT details, timestamp 
                    FROM system_events 
                    WHERE event_type = 'DECISION_TRACE'
                    ORDER BY timestamp DESC
                    LIMIT 2000;
                """)
                result = session.execute(query)
                rows_raw = result.fetchall()
                
                # Aggregate manually
                counts = {}
                latest_dates = {}
                for row in rows_raw:
                    try:
                        d = json.loads(row[0])
                        regime = d.get('regime', 'unknown')
                        counts[regime] = counts.get(regime, 0) + 1
                        ts = row[1]
                        if regime not in latest_dates or ts > latest_dates[regime]:
                            latest_dates[regime] = ts
                    except:
                        pass
                
                rows = []
                for r, c in counts.items():
                    rows.append((r, c, latest_dates[r]))
                rows.sort(key=lambda x: x[1], reverse=True)
            
            if not rows:
                print("❌ No 'DECISION_TRACE' events found in system_events")
                return
            
            total = sum(r[1] for r in rows)
            
            for regime, count, latest in rows:
                percentage = (count / total) * 100
                print(f"  {str(regime):20s}: {count:5d} ({percentage:5.1f}%)  Latest: {latest}")
            
            print("=" * 60)
            print(f"  {'TOTAL':20s}: {total:5d} (100.0%)\n")
            
            # Check age of traces
            age_query = text("""
                SELECT 
                    MIN(timestamp) as oldest,
                    MAX(timestamp) as newest,
                    COUNT(*) as total
                FROM system_events
                WHERE event_type = 'DECISION_TRACE';
            """)
            
            age_res = session.execute(age_query).fetchone()
            if age_res:
                oldest, newest, total = age_res
                print(f"📅 Trace Age:")
                print(f"  Oldest: {oldest}")
                print(f"  Newest: {newest}")
                print(f"  Total:  {total} traces\n")
                
        except Exception as e:
            print(f"❌ Error querying database: {e}")


def clear_old_traces(hours=1, dry_run=True):
    """
    Clear decision traces older than specified hours.
    
    Args:
        hours: Clear traces older than this many hours
        dry_run: If True, only show what would be deleted
    """
    db = get_db()
    with db.get_session() as session:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        # Count what would be deleted
        count_query = text("""
            SELECT COUNT(*) 
            FROM system_events 
            WHERE event_type = 'DECISION_TRACE' 
            AND timestamp < :cutoff
        """)
        count = session.execute(count_query, {"cutoff": cutoff}).scalar()
        
        if dry_run:
            print(f"\n🔍 DRY RUN: Would delete {count} traces older than {hours}h")
            print(f"   Cutoff time: {cutoff}")
            print("\n   Run with --execute to actually delete\n")
        else:
            print(f"\n🗑️  Deleting {count} traces older than {hours}h...")
            delete_query = text("""
                DELETE FROM system_events 
                WHERE event_type = 'DECISION_TRACE' 
                AND timestamp < :cutoff
            """)
            session.execute(delete_query, {"cutoff": cutoff})
            session.commit()
            print(f"   ✅ Deleted {count} traces\n")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Check and clear decision traces')
    parser.add_argument('--clear', type=int, metavar='HOURS',
                        help='Clear traces older than HOURS (default: 1)')
    parser.add_argument('--execute', action='store_true',
                        help='Actually execute the clear (default is dry-run)')
    
    args = parser.parse_args()
    
    # Always show current distribution
    check_regime_distribution()
    
    # Optionally clear old traces
    if args.clear is not None:
        hours = args.clear if args.clear > 0 else 1
        clear_old_traces(hours=hours, dry_run=not args.execute)
        
        if args.execute:
            print("✅ Traces cleared. New analysis will generate fresh regime classifications.\n")
            print("📊 Updated distribution:\n")
            check_regime_distribution()


if __name__ == '__main__':
    main()
