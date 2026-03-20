#!/usr/bin/env python3
"""
View coin reviews in a readable format.

Shows recent decision traces (coin reviews) from the database.
"""
import sqlite3
import json
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional

def view_coin_reviews(
    symbol: Optional[str] = None,
    hours: int = 1,
    limit: int = 50,
    show_details: bool = False
):
    """View coin reviews from database."""
    conn = sqlite3.connect('trading.db')
    cursor = conn.cursor()
    
    # Build query
    time_threshold = datetime.now(timezone.utc) - timedelta(hours=hours)
    
    if symbol:
        query = '''
            SELECT timestamp, symbol, details 
            FROM system_events 
            WHERE event_type = 'DECISION_TRACE'
            AND symbol = ?
            AND timestamp > ?
            ORDER BY timestamp DESC
            LIMIT ?
        '''
        params = (symbol, time_threshold.isoformat(), limit)
    else:
        query = '''
            SELECT timestamp, symbol, details 
            FROM system_events 
            WHERE event_type = 'DECISION_TRACE'
            AND timestamp > ?
            ORDER BY timestamp DESC
            LIMIT ?
        '''
        params = (time_threshold.isoformat(), limit)
    
    cursor.execute(query, params)
    traces = cursor.fetchall()
    
    if not traces:
        print(f"❌ No coin reviews found in the last {hours} hour(s)")
        if symbol:
            print(f"   (filtered for symbol: {symbol})")
        return
    
    print(f"📊 Coin Reviews (Last {hours} hour(s), showing {len(traces)} most recent)")
    print("=" * 80)
    print()
    
    for trace in traces:
        timestamp = trace[0]
        sym = trace[1]
        details_str = trace[2]
        
        try:
            details = json.loads(details_str) if details_str else {}
        except:
            details = {}
        
        signal = details.get('signal', 'no_signal')
        regime = details.get('regime', 'unknown')
        status = details.get('status', 'unknown')
        setup_quality = details.get('setup_quality', 0.0)
        spot_price = details.get('spot_price', 0.0)
        candle_count = details.get('candle_count', 0)
        
        # Format output
        signal_icon = "✅" if signal != "no_signal" else "⚪"
        print(f"{signal_icon} {timestamp} | {sym}")
        print(f"   Signal: {signal} | Regime: {regime} | Status: {status}")
        
        if show_details:
            print(f"   Price: ${spot_price:.6f} | Quality: {setup_quality:.1f} | Candles: {candle_count}")
            if details.get('score_breakdown'):
                scores = details['score_breakdown']
                print(f"   Scores: SMC={scores.get('smc', 0):.1f}, "
                      f"Fib={scores.get('fib', 0):.1f}, "
                      f"HTF={scores.get('htf', 0):.1f}, "
                      f"ADX={scores.get('adx', 0):.1f}")
        
        print()
    
    conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="View coin reviews")
    parser.add_argument("--symbol", "-s", help="Filter by symbol (e.g., BTC/USD)")
    parser.add_argument("--hours", type=int, default=1, help="Hours to look back (default: 1)")
    parser.add_argument("--limit", "-l", type=int, default=50, help="Max reviews to show (default: 50)")
    parser.add_argument("--details", "-d", action="store_true", help="Show detailed information")
    
    args = parser.parse_args()
    
    view_coin_reviews(
        symbol=args.symbol,
        hours=args.hours,
        limit=args.limit,
        show_details=args.details
    )
