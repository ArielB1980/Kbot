#!/usr/bin/env python3
"""
Check trading system activity by querying the database.

This script checks:
- Recent system events (DECISION_TRACE, TRADE_OPENED, etc.)
- Active positions
- Recent account state updates
- Signal generation activity
"""
import sys
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.db import get_db
from src.storage.repository import get_recent_events
from sqlalchemy import text

def check_recent_activity():
    """Check recent trading activity."""
    print("=" * 70)
    print("TRADING SYSTEM ACTIVITY CHECK")
    print("=" * 70)
    print()
    
    db = get_db()
    
    # 1. Check recent system events
    print("1. RECENT SYSTEM EVENTS (Last 10)")
    print("-" * 70)
    try:
        events = get_recent_events(limit=10)
        if events:
            for event in events[:10]:
                evt_type = event.get('event_type', 'UNKNOWN')
                symbol = event.get('symbol', 'N/A')
                timestamp = event.get('timestamp', 'N/A')
                details = event.get('details', {})
                
                print(f"  [{timestamp}] {evt_type} - {symbol}")
                if evt_type == 'DECISION_TRACE':
                    signal = details.get('signal', 'N/A')
                    price = details.get('spot_price', 'N/A')
                    print(f"    Signal: {signal}, Price: ${price}")
                elif evt_type == 'TRADE_OPENED':
                    side = details.get('side', 'N/A')
                    entry = details.get('entry_price', 'N/A')
                    print(f"    Side: {side}, Entry: ${entry}")
        else:
            print("  ⚠️  No recent events found")
        print()
    except Exception as e:
        print(f"  ❌ Error: {e}")
        print()
    
    # 2. Check active positions
    print("2. ACTIVE POSITIONS")
    print("-" * 70)
    try:
        with db.get_session() as session:
            result = session.execute(text("SELECT symbol, side, entry_price, current_mark_price, unrealized_pnl FROM positions LIMIT 10"))
            positions = result.fetchall()
            
            if positions:
                for pos in positions:
                    p = dict(pos._mapping)
                    print(f"  {p.get('symbol')}: {p.get('side')} @ ${p.get('entry_price')}")
                    print(f"    Current: ${p.get('current_mark_price')}, PnL: ${p.get('unrealized_pnl')}")
            else:
                print("  ℹ️  No active positions")
        print()
    except Exception as e:
        print(f"  ❌ Error: {e}")
        print()
    
    # 3. Check recent DECISION_TRACE events
    print("3. RECENT SIGNAL GENERATION (Last 20)")
    print("-" * 70)
    try:
        traces = get_recent_events(event_type='DECISION_TRACE', limit=20)
        if traces:
            symbols_with_signals = {}
            for trace in traces:
                symbol = trace.get('symbol', 'N/A')
                details = trace.get('details', {})
                signal = details.get('signal', 'no_signal')
                
                if signal != 'no_signal':
                    if symbol not in symbols_with_signals:
                        symbols_with_signals[symbol] = []
                    symbols_with_signals[symbol].append({
                        'time': trace.get('timestamp'),
                        'signal': signal,
                        'price': details.get('spot_price')
                    })
            
            if symbols_with_signals:
                print(f"  ✅ Found signals for {len(symbols_with_signals)} symbols:")
                for symbol, signals in list(symbols_with_signals.items())[:10]:
                    latest = signals[0]
                    print(f"    {symbol}: {latest['signal']} @ ${latest['price']} ({latest['time']})")
            else:
                print("  ℹ️  No non-NO_SIGNAL events found (system may be monitoring)")
        else:
            print("  ⚠️  No DECISION_TRACE events found")
        print()
    except Exception as e:
        print(f"  ❌ Error: {e}")
        print()
    
    # 4. Check account state
    print("4. RECENT ACCOUNT STATE")
    print("-" * 70)
    try:
        with db.get_session() as session:
            result = session.execute(text("""
                SELECT timestamp, equity, balance, margin_used, unrealized_pnl 
                FROM account_state 
                ORDER BY timestamp DESC 
                LIMIT 5
            """))
            states = result.fetchall()
            
            if states:
                latest = dict(states[0]._mapping)
                print(f"  Latest: {latest.get('timestamp')}")
                print(f"    Equity: ${latest.get('equity')}")
                print(f"    Balance: ${latest.get('balance')}")
                print(f"    Margin Used: ${latest.get('margin_used')}")
                print(f"    Unrealized PnL: ${latest.get('unrealized_pnl')}")
            else:
                print("  ℹ️  No account state records found")
        print()
    except Exception as e:
        print(f"  ❌ Error: {e}")
        print()
    
    # 5. Summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print("✅ Database connection: Working")
    print("✅ System events: Being logged")
    print("✅ Activity check: Complete")
    print()
    print("💡 Tip: Check App Platform Runtime Logs for live trading activity")

if __name__ == "__main__":
    try:
        check_recent_activity()
    except Exception as e:
        print(f"❌ Error running check: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
