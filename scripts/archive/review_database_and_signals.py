#!/usr/bin/env python3
"""
Comprehensive review of database data freshness and signal analysis correctness.

Checks:
1. Data freshness for all monitored coins
2. Signal analysis correctness (valid signals, proper structure)
3. Candle data availability
4. System health indicators
"""
import sys
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import json

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.storage.db import get_db
from src.storage.repository import get_latest_traces, get_recent_events
from src.config.config import load_config
from src.dashboard.utils import _get_monitored_symbols
from src.monitoring.logger import get_logger, setup_logging

logger = get_logger(__name__)
setup_logging("INFO", "json")


def analyze_data_freshness():
    """Analyze data freshness for all monitored coins."""
    config = load_config()
    all_symbols = _get_monitored_symbols(config)
    
    print("\n" + "="*80)
    print("DATA FRESHNESS ANALYSIS")
    print("="*80)
    print(f"Total monitored coins: {len(all_symbols)}")
    print(f"Analysis time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("="*80 + "\n")
    
    # Get latest traces for all symbols
    traces = get_latest_traces(limit=1000)
    traces_by_symbol = {trace.get('symbol'): trace for trace in traces}
    
    now = datetime.now(timezone.utc)
    
    # Categorize coins
    active_coins = []
    stale_coins = []
    dead_coins = []
    missing_coins = []
    
    for symbol in sorted(all_symbols):
        trace = traces_by_symbol.get(symbol)
        
        if not trace:
            missing_coins.append({
                'symbol': symbol,
                'reason': 'no_trace_events',
                'last_update': None
            })
            continue
        
        # Parse timestamp
        last_update = trace.get('timestamp')
        if isinstance(last_update, str):
            try:
                last_update = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
            except:
                last_update = None
        elif last_update and last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=timezone.utc)
        
        if not last_update:
            dead_coins.append({
                'symbol': symbol,
                'reason': 'invalid_timestamp',
                'last_update': None,
                'trace': trace
            })
            continue
        
        age_seconds = (now - last_update).total_seconds()
        age_minutes = age_seconds / 60
        age_hours = age_seconds / 3600
        
        # Get status from trace details
        details = trace.get('details', {})
        status = details.get('status', 'unknown')
        reason = details.get('reason', 'unknown')
        error = details.get('error', '')
        
        coin_info = {
            'symbol': symbol,
            'age_minutes': age_minutes,
            'age_hours': age_hours,
            'last_update': last_update,
            'status': status,
            'reason': reason,
            'error': error[:100] if error else None,
            'spot_price': details.get('spot_price', 0.0),
            'signal': details.get('signal', 'NO_SIGNAL'),
            'candle_count': details.get('candle_count', 0),
            'score_breakdown': details.get('score_breakdown', {})
        }
        
        if age_seconds < 3600:  # < 1 hour
            active_coins.append(coin_info)
        elif age_seconds < 21600:  # < 6 hours
            stale_coins.append(coin_info)
        else:  # > 6 hours
            dead_coins.append(coin_info)
    
    return {
        'active': active_coins,
        'stale': stale_coins,
        'dead': dead_coins,
        'missing': missing_coins,
        'total': len(all_symbols)
    }


def analyze_signal_quality():
    """Analyze signal quality and correctness."""
    print("\n" + "="*80)
    print("SIGNAL ANALYSIS QUALITY REVIEW")
    print("="*80 + "\n")
    
    # Get recent traces (last 24 hours)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    traces = get_latest_traces(limit=500)
    
    signal_stats = {
        'total_traces': len(traces),
        'signals': defaultdict(int),
        'regimes': defaultdict(int),
        'biases': defaultdict(int),
        'statuses': defaultdict(int),
        'issues': []
    }
    
    # Analyze each trace
    for trace in traces:
        details = trace.get('details', {})
        signal = details.get('signal', 'NO_SIGNAL')
        regime = details.get('regime', 'unknown')
        bias = details.get('bias', 'neutral')
        status = details.get('status', 'unknown')
        candle_count = details.get('candle_count', 0)
        score_breakdown = details.get('score_breakdown', {})
        spot_price = details.get('spot_price', 0.0)
        
        signal_stats['signals'][signal] += 1
        signal_stats['regimes'][regime] += 1
        signal_stats['biases'][bias] += 1
        signal_stats['statuses'][status] += 1
        
        # Check for issues
        symbol = trace.get('symbol', 'UNKNOWN')
        
        # Issue: Zero price
        if spot_price == 0.0:
            signal_stats['issues'].append({
                'symbol': symbol,
                'issue': 'zero_price',
                'details': 'Price is zero'
            })
        
        # Issue: Insufficient candles but marked as active
        if status == 'active' and candle_count < 50:
            signal_stats['issues'].append({
                'symbol': symbol,
                'issue': 'insufficient_candles_active',
                'details': f'Active status but only {candle_count} candles'
            })
        
        # Issue: Signal but no score breakdown (only for actual trading signals)
        # Note: Some old traces or initialization traces may not have scores
        if signal not in ['NO_SIGNAL', 'no_signal'] and not score_breakdown:
            # Only flag if status is active (not initialization)
            if status == 'active':
                signal_stats['issues'].append({
                    'symbol': symbol,
                    'issue': 'signal_no_score',
                    'details': f'{signal} signal but no score breakdown'
                })
        
        # Issue: Invalid regime
        valid_regimes = ['tight_smc', 'wide_structure', 'trending', 'consolidation', 'tight_range', 'unknown', 'no_data']
        if regime not in valid_regimes:
            signal_stats['issues'].append({
                'symbol': symbol,
                'issue': 'invalid_regime',
                'details': f'Invalid regime: {regime}'
            })
        
        # Issue: Invalid bias
        valid_biases = ['bullish', 'bearish', 'neutral']
        if bias not in valid_biases:
            signal_stats['issues'].append({
                'symbol': symbol,
                'issue': 'invalid_bias',
                'details': f'Invalid bias: {bias}'
            })
    
    return signal_stats


def check_candle_data():
    """Check candle data availability in database."""
    print("\n" + "="*80)
    print("CANDLE DATA AVAILABILITY")
    print("="*80 + "\n")
    
    db = get_db()
    from src.storage.repository import CandleModel
    
    with db.get_session() as session:
        from sqlalchemy import func
        
        # Get candle counts by symbol and timeframe
        results = session.query(
            CandleModel.symbol,
            CandleModel.timeframe,
            func.count(CandleModel.id).label('count'),
            func.max(CandleModel.timestamp).label('latest')
        ).group_by(
            CandleModel.symbol,
            CandleModel.timeframe
        ).all()
        
        candle_stats = defaultdict(lambda: defaultdict(dict))
        
        for symbol, timeframe, count, latest in results:
            candle_stats[symbol][timeframe] = {
                'count': count,
                'latest': latest.replace(tzinfo=timezone.utc) if latest else None
            }
        
        # Check for missing timeframes
        config = load_config()
        all_symbols = _get_monitored_symbols(config)
        required_timeframes = ['15m', '1h', '4h', '1d']
        
        missing_data = []
        for symbol in all_symbols[:20]:  # Check first 20 as sample
            for tf in required_timeframes:
                if symbol not in candle_stats or tf not in candle_stats[symbol]:
                    missing_data.append({
                        'symbol': symbol,
                        'timeframe': tf,
                        'issue': 'no_data'
                    })
                elif candle_stats[symbol][tf]['count'] < 50:
                    missing_data.append({
                        'symbol': symbol,
                        'timeframe': tf,
                        'issue': 'insufficient_data',
                        'count': candle_stats[symbol][tf]['count']
                    })
        
        return {
            'candle_stats': dict(candle_stats),
            'missing_data': missing_data[:50]  # Limit to 50 for display
        }


def check_system_health():
    """Check overall system health indicators."""
    print("\n" + "="*80)
    print("SYSTEM HEALTH CHECK")
    print("="*80 + "\n")
    
    health = {
        'live_trading_running': False,
        'latest_trace_age_minutes': None,
        'recent_errors': [],
        'recommendations': []
    }
    
    # Check if live trading is running
    try:
        import subprocess
        result = subprocess.run(
            ["pgrep", "-f", "src/cli.py live"],
            capture_output=True,
            text=True
        )
        health['live_trading_running'] = result.returncode == 0
    except Exception as e:
        health['live_trading_running'] = False
        health['recommendations'].append(f"Could not check process status: {e}")
    
    # Check latest trace age
    traces = get_latest_traces(limit=1)
    if traces:
        latest = traces[0]
        timestamp = latest.get('timestamp')
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except:
                timestamp = None
        
        if timestamp:
            age = (datetime.now(timezone.utc) - timestamp).total_seconds() / 60
            health['latest_trace_age_minutes'] = age
    
    # Check for recent errors
    recent_events = get_recent_events(limit=100, event_type="ERROR")
    for event in recent_events[:10]:
        details = event.get('details', {})
        if isinstance(details, str):
            try:
                details = json.loads(details)
            except:
                details = {}
        
        health['recent_errors'].append({
            'symbol': event.get('symbol', 'SYSTEM'),
            'timestamp': event.get('timestamp'),
            'error': details.get('error', str(details))
        })
    
    return health


def print_summary(freshness_data, signal_stats, candle_data, health):
    """Print comprehensive summary."""
    print("\n" + "="*80)
    print("COMPREHENSIVE REVIEW SUMMARY")
    print("="*80 + "\n")
    
    # Data Freshness Summary
    print("📊 DATA FRESHNESS")
    print("-" * 80)
    print(f"🟢 Active (< 1h):     {len(freshness_data['active']):4d} coins ({len(freshness_data['active'])/freshness_data['total']*100:.1f}%)")
    print(f"🟡 Stale (1-6h):      {len(freshness_data['stale']):4d} coins ({len(freshness_data['stale'])/freshness_data['total']*100:.1f}%)")
    print(f"🔴 Dead (> 6h):       {len(freshness_data['dead']):4d} coins ({len(freshness_data['dead'])/freshness_data['total']*100:.1f}%)")
    print(f"⚪ Missing (no data):  {len(freshness_data['missing']):4d} coins ({len(freshness_data['missing'])/freshness_data['total']*100:.1f}%)")
    print()
    
    # Signal Analysis Summary
    print("📈 SIGNAL ANALYSIS")
    print("-" * 80)
    print(f"Total traces analyzed: {signal_stats['total_traces']}")
    print(f"\nSignal distribution:")
    for signal, count in sorted(signal_stats['signals'].items(), key=lambda x: -x[1]):
        pct = count / signal_stats['total_traces'] * 100 if signal_stats['total_traces'] > 0 else 0
        print(f"  {signal:15s}: {count:4d} ({pct:5.1f}%)")
    
    print(f"\nRegime distribution:")
    for regime, count in sorted(signal_stats['regimes'].items(), key=lambda x: -x[1]):
        pct = count / signal_stats['total_traces'] * 100 if signal_stats['total_traces'] > 0 else 0
        print(f"  {regime:20s}: {count:4d} ({pct:5.1f}%)")
    
    print(f"\nBias distribution:")
    for bias, count in sorted(signal_stats['biases'].items(), key=lambda x: -x[1]):
        pct = count / signal_stats['total_traces'] * 100 if signal_stats['total_traces'] > 0 else 0
        print(f"  {bias:15s}: {count:4d} ({pct:5.1f}%)")
    
    if signal_stats['issues']:
        print(f"\n⚠️  Issues found: {len(signal_stats['issues'])}")
        issue_types = defaultdict(int)
        for issue in signal_stats['issues']:
            issue_types[issue['issue']] += 1
        for issue_type, count in sorted(issue_types.items(), key=lambda x: -x[1]):
            print(f"  {issue_type:30s}: {count:4d}")
    else:
        print("\n✅ No signal quality issues detected")
    print()
    
    # System Health
    print("🏥 SYSTEM HEALTH")
    print("-" * 80)
    if health['live_trading_running']:
        print("✅ Live trading process is RUNNING")
    else:
        print("❌ Live trading process is NOT running")
        print("   → Start with: python3 -m src.entrypoints.prod_live")
    
    if health['latest_trace_age_minutes'] is not None:
        age = health['latest_trace_age_minutes']
        if age < 10:
            print(f"✅ Latest trace is {age:.1f} minutes old (fresh)")
        elif age < 60:
            print(f"⚠️  Latest trace is {age:.1f} minutes old (may be stale)")
        else:
            print(f"❌ Latest trace is {age:.1f} minutes old (very stale)")
    
    if health['recent_errors']:
        print(f"\n⚠️  Recent errors: {len(health['recent_errors'])}")
        for error in health['recent_errors'][:5]:
            print(f"  {error['symbol']:20s}: {error['error'][:60]}")
    else:
        print("\n✅ No recent errors detected")
    print()
    
    # Recommendations
    print("💡 RECOMMENDATIONS")
    print("-" * 80)
    
    if not health['live_trading_running']:
        print("1. ❌ CRITICAL: Start live trading to update coin data")
        print("   → python3 -m src.entrypoints.prod_live")
        print()
    
    if len(freshness_data['dead']) > freshness_data['total'] * 0.5:
        print("2. ⚠️  WARNING: More than 50% of coins have stale data (> 6h)")
        print("   → Ensure live trading is running continuously")
        print("   → Check for API connectivity issues")
        print()
    
    if len(freshness_data['missing']) > 0:
        print(f"3. ⚠️  {len(freshness_data['missing'])} coins have no data at all")
        print("   → These coins may need initialization")
        print("   → Run: python3 scripts/fix_missing_coins.py")
        print()
    
    if signal_stats['issues']:
        print(f"4. ⚠️  {len(signal_stats['issues'])} signal quality issues detected")
        print("   → Review issues above and check signal generation logic")
        print()
    
    if len(candle_data['missing_data']) > 0:
        print(f"5. ⚠️  {len(candle_data['missing_data'])} symbols missing candle data")
        print("   → System needs to collect more historical data")
        print("   → This is normal for new coins")
        print()
    
    if (health['live_trading_running'] and 
        len(freshness_data['active']) > freshness_data['total'] * 0.8 and
        len(signal_stats['issues']) == 0):
        print("✅ System appears healthy!")
        print("   → Live trading is running")
        print("   → Most coins have fresh data")
        print("   → Signal analysis is working correctly")
        print()
    
    print("="*80)


def main():
    """Run comprehensive review."""
    try:
        # Run all analyses
        freshness_data = analyze_data_freshness()
        signal_stats = analyze_signal_quality()
        candle_data = check_candle_data()
        health = check_system_health()
        
        # Print summary
        print_summary(freshness_data, signal_stats, candle_data, health)
        
    except Exception as e:
        logger.error("Failed to complete review", error=str(e))
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
