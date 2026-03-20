#!/usr/bin/env python3
"""
Analyze server logs to extract signals and trading activity.

This script can work with logs from:
1. DigitalOcean API (if you set DIGITALOCEAN_ACCESS_TOKEN)
2. Pasted log text from the web console
3. Local log files
"""
import sys
import json
import re
from datetime import datetime, timedelta
from collections import defaultdict

def analyze_logs(log_text):
    """Analyze log text and extract key information."""

    signals_found = []
    signals_rejected = []
    errors = []
    positions = set()

    for line in log_text.split('\n'):
        if not line.strip():
            continue

        try:
            # Try to parse as JSON
            if line.startswith('{'):
                log_entry = json.loads(line)
            elif '{' in line:
                # Extract JSON from line
                json_start = line.index('{')
                log_entry = json.loads(line[json_start:])
            else:
                continue

            event = log_entry.get('event', '')

            # Track signals found
            if 'SIGNAL FOUND' in event:
                signals_found.append({
                    'time': log_entry.get('timestamp'),
                    'event': event,
                    'symbol': log_entry.get('symbol'),
                    'signal_type': log_entry.get('signal_type'),
                    'entry': log_entry.get('entry'),
                    'stop': log_entry.get('stop')
                })

            # Track signal rejections
            elif 'NO SIGNAL' in event or 'Rejected' in event:
                # Extract symbol from event text
                match = re.search(r'for ([A-Z]+/USD)', event)
                if match:
                    symbol = match.group(1)
                    signals_rejected.append({
                        'time': log_entry.get('timestamp'),
                        'symbol': symbol,
                        'event': event[:200]  # First 200 chars
                    })

            # Track errors
            elif log_entry.get('level') in ['error', 'critical']:
                if 'Kraken' not in event and '503' not in event:  # Skip Kraken API errors
                    errors.append({
                        'time': log_entry.get('timestamp'),
                        'event': event,
                        'error': log_entry.get('error', '')
                    })

            # Track active positions
            if 'Active Portfolio' in event:
                symbols = log_entry.get('symbols', [])
                positions.update(symbols)

        except (json.JSONDecodeError, ValueError):
            continue

    # Print analysis
    print("="*80)
    print("SERVER LOG ANALYSIS")
    print("="*80)

    print(f"\n📊 SIGNALS FOUND: {len(signals_found)}")
    print("-"*80)
    for sig in signals_found[-10:]:  # Last 10
        print(f"  {sig['time']}: {sig['event']}")

    print(f"\n❌ SIGNALS REJECTED: {len(signals_rejected)}")
    print("-"*80)
    rejection_reasons = defaultdict(int)
    for rej in signals_rejected[-20:]:  # Last 20
        # Extract rejection reason
        if 'Reason=' in rej['event']:
            reason = rej['event'].split('Reason=')[1].split('\\n')[0][:50]
            rejection_reasons[reason] += 1
            print(f"  {rej['symbol']}: {reason}")

    print(f"\n📈 ACTIVE POSITIONS: {len(positions)}")
    print("-"*80)
    for pos in sorted(positions):
        print(f"  {pos}")

    print(f"\n⚠️  ERRORS (non-API): {len(errors)}")
    print("-"*80)
    for err in errors[-10:]:  # Last 10
        print(f"  {err['time']}: {err['event']}")

    print(f"\n🎯 REJECTION SUMMARY:")
    print("-"*80)
    for reason, count in sorted(rejection_reasons.items(), key=lambda x: -x[1])[:5]:
        print(f"  {count:3d}x: {reason}")

    print("\n" + "="*80)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Read from file
        with open(sys.argv[1], 'r') as f:
            log_text = f.read()
    else:
        # Read from stdin
        print("Paste your logs (Ctrl+D when done):")
        log_text = sys.stdin.read()

    analyze_logs(log_text)
