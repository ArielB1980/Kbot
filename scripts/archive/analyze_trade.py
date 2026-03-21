#!/usr/bin/env python3
"""
Analyze a specific trade to understand what happened.

Usage: python3 scripts/analyze_trade.py POPCAT/USD
"""
import sys
import os
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.storage.db import get_db
from src.storage.repository import get_recent_events, get_candles
from sqlalchemy import text
import json

def analyze_trade(symbol: str):
    """Analyze a trade for a given symbol."""
    print(f"\n{'='*80}")
    print(f"TRADE ANALYSIS: {symbol}")
    print(f"{'='*80}\n")
    
    db = get_db()
    
    # 1. Check for position history
    with db.get_session() as session:
        result = session.execute(text(f"""
            SELECT * FROM positions 
            WHERE symbol LIKE '%{symbol.replace('/', '')}%'
            ORDER BY opened_at DESC
        """))
        positions = result.fetchall()
        
        if positions:
            print("=== POSITION HISTORY ===")
            for pos in positions:
                p = dict(pos._mapping)
                print(f"Symbol: {p.get('symbol')}")
                print(f"Side: {p.get('side')}")
                print(f"Entry: ${p.get('entry_price')}")
                print(f"Size: ${p.get('size_notional')}")
                print(f"Opened: {p.get('opened_at')}")
                print(f"Current: ${p.get('current_mark_price')}")
                print(f"PnL: ${p.get('unrealized_pnl')}")
                print("---")
        else:
            print("❌ No position found in database")
    
    # 2. Check for signals
    print("\n=== SIGNAL HISTORY ===")
    events = get_recent_events(limit=100, symbol=symbol)
    
    signals = []
    for event in events:
        details = event.get('details', {})
        signal = details.get('signal', '')
        if signal and signal != 'no_signal':
            signals.append({
                'time': event.get('timestamp'),
                'signal': signal,
                'price': details.get('spot_price'),
                'quality': details.get('setup_quality', 0),
                'regime': details.get('regime'),
                'bias': details.get('bias'),
                'adx': details.get('adx', 0),
                'atr': details.get('atr', 0)
            })
    
    if signals:
        print(f"Found {len(signals)} non-NO_SIGNAL events:")
        for sig in signals[:10]:
            print(f"  {sig['time']}: {sig['signal']} @ ${sig['price']:.4f} (Q: {sig['quality']:.0f}, {sig['regime']}, {sig['bias']})")
    else:
        print("❌ No SHORT/LONG signals found")
    
    # 3. Check price action
    print("\n=== PRICE ACTION (Last 24h) ===")
    candles = get_candles(symbol, "15m", limit=96)  # 24 hours
    
    if candles:
        prices = [float(c.close) for c in candles]
        high = max(prices)
        low = min(prices)
        current = prices[-1]
        start = prices[0]
        change_pct = ((current - start) / start) * 100
        
        print(f"Start (24h ago): ${start:.4f}")
        print(f"Current: ${current:.4f}")
        print(f"High: ${high:.4f}")
        print(f"Low: ${low:.4f}")
        print(f"Change: {change_pct:+.2f}%")
        
        # Find when price dropped (if SHORT was correct)
        if change_pct < 0:
            print(f"\n✅ Price dropped {abs(change_pct):.2f}% - SHORT would have been profitable")
            
            # Find lowest point
            min_idx = prices.index(low)
            min_candle = candles[min_idx]
            print(f"Lowest point: ${low:.4f} at {min_candle.timestamp}")
            
            # Calculate potential profit if entered at first signal
            if signals:
                entry_price = signals[0]['price']
                if entry_price > 0:
                    profit_pct = ((entry_price - low) / entry_price) * 100
                    print(f"\nIf entered SHORT at ${entry_price:.4f}:")
                    print(f"  Max profit: {profit_pct:.2f}% (to ${low:.4f})")
                    print(f"  Current profit: {((entry_price - current) / entry_price) * 100:.2f}%")
    else:
        print("❌ No candle data available")
    
    # 4. Check for exit reasons
    print("\n=== EXIT ANALYSIS ===")
    with db.get_session() as session:
        result = session.execute(text(f"""
            SELECT * FROM system_events 
            WHERE symbol = '{symbol}'
            AND (event_type = 'TRADE_CLOSED' OR event_type = 'POSITION_CLOSED' OR details LIKE '%close%')
            ORDER BY timestamp DESC
            LIMIT 10
        """))
        exits = result.fetchall()
        
        if exits:
            for exit_event in exits:
                e = dict(exit_event._mapping)
                print(f"Time: {e.get('timestamp')}")
                print(f"Type: {e.get('event_type')}")
                print(f"Details: {e.get('details', '')[:200]}")
                print("---")
        else:
            print("No explicit exit events found")
            print("Position may have been closed externally or by stop loss")
    
    print(f"\n{'='*80}\n")

if __name__ == "__main__":
    symbol = sys.argv[1] if len(sys.argv) > 1 else "POPCAT/USD"
    analyze_trade(symbol)
