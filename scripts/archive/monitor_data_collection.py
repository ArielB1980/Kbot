#!/usr/bin/env python3
"""
Monitor data collection progress for live trading.

Shows:
- Coins with sufficient candles (50+)
- Coins still collecting data
- Progress over time
"""
import sys
import os
import time
from datetime import datetime, timezone

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.storage.db import get_db
from src.storage.repository import CandleModel
from src.config.config import load_config
from src.dashboard.utils import _get_monitored_symbols
from sqlalchemy import func

def check_data_collection():
    """Check current data collection status."""
    config = load_config()
    all_symbols = _get_monitored_symbols(config)
    
    db = get_db()
    with db.get_session() as session:
        # Get candle counts by symbol for 15m timeframe (required for analysis)
        results = session.query(
            CandleModel.symbol,
            func.count(CandleModel.id).label('count'),
            func.max(CandleModel.timestamp).label('latest')
        ).filter(
            CandleModel.timeframe == '15m'
        ).group_by(
            CandleModel.symbol
        ).all()
        
        candle_counts = {symbol: count for symbol, count, _ in results}
        latest_times = {symbol: latest for symbol, _, latest in results}
    
    # Categorize coins
    sufficient_data = []  # 50+ candles
    collecting_data = []  # 1-49 candles
    no_data = []  # 0 candles
    
    for symbol in all_symbols:
        count = candle_counts.get(symbol, 0)
        latest = latest_times.get(symbol)
        
        if count >= 50:
            sufficient_data.append({
                'symbol': symbol,
                'count': count,
                'latest': latest
            })
        elif count > 0:
            collecting_data.append({
                'symbol': symbol,
                'count': count,
                'latest': latest
            })
        else:
            no_data.append(symbol)
    
    return {
        'total': len(all_symbols),
        'sufficient': sufficient_data,
        'collecting': collecting_data,
        'no_data': no_data
    }

def print_status(stats):
    """Print formatted status."""
    print("\n" + "="*80)
    print("DATA COLLECTION STATUS")
    print("="*80)
    print(f"Analysis time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()
    
    total = stats['total']
    sufficient = len(stats['sufficient'])
    collecting = len(stats['collecting'])
    no_data = len(stats['no_data'])
    
    print(f"Total monitored coins: {total}")
    print()
    print(f"✅ Sufficient data (50+ candles): {sufficient:4d} coins ({sufficient/total*100:.1f}%)")
    print(f"🟡 Collecting data (1-49 candles): {collecting:4d} coins ({collecting/total*100:.1f}%)")
    print(f"⚪ No data yet:                  {no_data:4d} coins ({no_data/total*100:.1f}%)")
    print()
    
    if sufficient > 0:
        print("Coins ready for analysis (sample of 10):")
        for coin in stats['sufficient'][:10]:
            latest_str = ""
            if coin['latest']:
                if isinstance(coin['latest'], str):
                    try:
                        latest = datetime.fromisoformat(coin['latest'].replace('Z', '+00:00'))
                        latest_str = f" (latest: {latest.strftime('%Y-%m-%d %H:%M')})"
                    except:
                        pass
                else:
                    latest_str = f" (latest: {coin['latest'].strftime('%Y-%m-%d %H:%M')})"
            print(f"  {coin['symbol']:20s}: {coin['count']:4d} candles{latest_str}")
        if len(stats['sufficient']) > 10:
            print(f"  ... and {len(stats['sufficient']) - 10} more")
        print()
    
    if collecting > 0:
        print("Coins collecting data (sample of 10):")
        for coin in stats['collecting'][:10]:
            latest_str = ""
            if coin['latest']:
                if isinstance(coin['latest'], str):
                    try:
                        latest = datetime.fromisoformat(coin['latest'].replace('Z', '+00:00'))
                        latest_str = f" (latest: {latest.strftime('%Y-%m-%d %H:%M')})"
                    except:
                        pass
                else:
                    latest_str = f" (latest: {coin['latest'].strftime('%Y-%m-%d %H:%M')})"
            print(f"  {coin['symbol']:20s}: {coin['count']:4d} candles{latest_str}")
        if len(stats['collecting']) > 10:
            print(f"  ... and {len(stats['collecting']) - 10} more")
        print()
    
    # Progress estimate
    if collecting > 0 or no_data > 0:
        print("Progress:")
        progress_pct = (sufficient / total) * 100
        bar_length = 50
        filled = int(bar_length * progress_pct / 100)
        bar = "█" * filled + "░" * (bar_length - filled)
        print(f"  [{bar}] {progress_pct:.1f}%")
        print()
    
    print("="*80)

def main():
    """Main monitoring function."""
    try:
        stats = check_data_collection()
        print_status(stats)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
