#!/usr/bin/env python3
"""
Monitor trade execution: signals, entry orders, SL, and TP placement.
"""
import sys
import re
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

def monitor_logs():
    """Monitor logs for trade execution events."""
    log_file = Path(__file__).parent.parent / "logs" / "run.log"
    
    if not log_file.exists():
        print(f"Log file not found: {log_file}")
        return
    
    print("=" * 80)
    print("TRADE EXECUTION MONITOR")
    print("=" * 80)
    print(f"Monitoring: {log_file}")
    print("=" * 80)
    print()
    
    # Track recent signals and their execution status
    recent_signals = {}  # symbol -> {signal_type, timestamp, entry_order, sl_order, tp_orders}
    
    try:
        with open(log_file, 'r') as f:
            # Go to end of file
            f.seek(0, 2)
            
            while True:
                line = f.readline()
                if not line:
                    import time
                    time.sleep(1)
                    continue
                
                # Parse JSON log line
                try:
                    import json
                    log_entry = json.loads(line.strip())
                except:
                    continue
                
                event = log_entry.get('event', '')
                symbol = log_entry.get('symbol', '')
                timestamp = log_entry.get('timestamp', '')
                
                # Signal generated
                if '"Signal generated"' in event or event == 'Signal generated':
                    signal_type = log_entry.get('signal_type', 'unknown')
                    entry = log_entry.get('entry', '')
                    stop = log_entry.get('stop', '')
                    
                    recent_signals[symbol] = {
                        'signal_type': signal_type,
                        'timestamp': timestamp,
                        'entry_price': entry,
                        'stop_price': stop,
                        'entry_order': None,
                        'sl_order': None,
                        'tp_orders': [],
                        'status': 'signal_generated'
                    }
                    
                    print(f"[{timestamp}] 🎯 SIGNAL: {symbol} {signal_type.upper()}")
                    print(f"   Entry: ${entry} | Stop: ${stop}")
                    print()
                
                # Entry order submitted
                elif '"Entry order submitted"' in event or event == 'Entry order submitted':
                    order_id = log_entry.get('order_id', '')
                    entry_price = log_entry.get('entry_price', '')
                    
                    if symbol in recent_signals:
                        recent_signals[symbol]['entry_order'] = order_id
                        recent_signals[symbol]['status'] = 'entry_placed'
                    
                    print(f"[{timestamp}] ✅ ENTRY ORDER: {symbol}")
                    print(f"   Order ID: {order_id} | Entry Price: ${entry_price}")
                    print()
                
                # Entry order placed (alternative log format)
                elif '"Entry order placed"' in event or event == 'Entry order placed':
                    order_id = log_entry.get('order_id', '')
                    
                    if symbol in recent_signals:
                        recent_signals[symbol]['entry_order'] = order_id
                        recent_signals[symbol]['status'] = 'entry_placed'
                    
                    print(f"[{timestamp}] ✅ ENTRY ORDER PLACED: {symbol}")
                    print(f"   Order ID: {order_id}")
                    print()
                
                # Stop loss placed
                elif '"Protective SL placed"' in event or '"Stop loss order placed"' in event or 'Stop loss' in event.lower():
                    sl_id = log_entry.get('order_id', log_entry.get('sl_order_id', ''))
                    
                    if symbol in recent_signals:
                        recent_signals[symbol]['sl_order'] = sl_id
                        if recent_signals[symbol]['status'] == 'entry_placed':
                            recent_signals[symbol]['status'] = 'sl_placed'
                    
                    print(f"[{timestamp}] 🛡️  STOP LOSS PLACED: {symbol}")
                    print(f"   SL Order ID: {sl_id}")
                    print()
                
                # TP ladder placed
                elif '"TP ladder placed"' in event or '"TP ladder updated"' in event:
                    tp_count = log_entry.get('tp_count', 0)
                    tp_ids = log_entry.get('tp_ids', [])
                    
                    if symbol in recent_signals:
                        recent_signals[symbol]['tp_orders'] = tp_ids
                        if recent_signals[symbol]['status'] in ['entry_placed', 'sl_placed']:
                            recent_signals[symbol]['status'] = 'tp_placed'
                    
                    print(f"[{timestamp}] 🎯 TAKE PROFIT LADDER PLACED: {symbol}")
                    print(f"   TP Count: {tp_count} | TP IDs: {tp_ids}")
                    print()
                
                # Failed to submit entry order
                elif '"Failed to submit entry order"' in event:
                    error = log_entry.get('error', '')
                    
                    if symbol in recent_signals:
                        recent_signals[symbol]['status'] = 'entry_failed'
                        recent_signals[symbol]['error'] = error
                    
                    print(f"[{timestamp}] ❌ ENTRY ORDER FAILED: {symbol}")
                    print(f"   Error: {error}")
                    print()
                
                # Instrument specs not found
                elif '"Instrument specs not found"' in event or '"Instrument specs for' in event:
                    requested = log_entry.get('requested_symbol', symbol)
                    similar = log_entry.get('similar_symbols', [])
                    
                    print(f"[{timestamp}] ⚠️  INSTRUMENT LOOKUP FAILED: {requested}")
                    if similar:
                        print(f"   Similar symbols: {similar[:5]}")
                    print()
                
                # Auction opened position
                elif '"Auction: Opened position"' in event:
                    print(f"[{timestamp}] 📊 AUCTION: Position opened for {symbol}")
                    print()
                
                # Summary check every 10 signals
                if len(recent_signals) > 0 and len([s for s in recent_signals.values() if s['status'] == 'signal_generated']) >= 10:
                    print("\n" + "=" * 80)
                    print("EXECUTION SUMMARY (Last 10 Signals)")
                    print("=" * 80)
                    for sym, data in list(recent_signals.items())[-10:]:
                        status_icon = {
                            'signal_generated': '🎯',
                            'entry_placed': '✅',
                            'sl_placed': '🛡️',
                            'tp_placed': '🎯',
                            'entry_failed': '❌'
                        }.get(data['status'], '❓')
                        
                        print(f"{status_icon} {sym:15} {data['signal_type']:6} -> {data['status']}")
                        if data.get('error'):
                            print(f"   Error: {data['error'][:100]}")
                    print("=" * 80 + "\n")
                    
                    # Keep only last 20 signals
                    if len(recent_signals) > 20:
                        oldest = min(recent_signals.keys(), key=lambda k: recent_signals[k]['timestamp'])
                        del recent_signals[oldest]
    
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped.")
        if recent_signals:
            print("\nFinal Summary:")
            for sym, data in recent_signals.items():
                print(f"  {sym}: {data['status']}")

if __name__ == "__main__":
    monitor_logs()
