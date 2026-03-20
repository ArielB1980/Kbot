#!/usr/bin/env python3
"""
Backtest CHZ/USD over the last 7 days to match the profitable trade.
"""
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.config.config import load_config
from src.backtest.backtest_engine import BacktestEngine
from src.monitoring.logger import setup_logging, get_logger

logger = get_logger(__name__)


async def run_chz_backtest():
    """Run backtest for CHZ/USD."""

    # Load config
    config = load_config("src/config/config.yaml")
    setup_logging("INFO", "json")

    print("\n" + "="*80)
    print("CHZ/USD BACKTEST - 7 Days (Jan 13-20)")
    print("="*80)
    print(f"Position Sizing: {config.risk.sizing_method}")
    print(f"Risk per Trade: {config.risk.risk_per_trade_pct * 100}%")
    print(f"Target Leverage: {config.risk.target_leverage}x")
    print(f"Starting Equity: $10,000")
    print("="*80)

    # Set backtest parameters - Last 7 days to capture the profitable CHZ trade
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=7)

    print(f"\nPeriod: {start_date.date()} to {end_date.date()}")
    print(f"Symbol: CHZ/USD")
    print(f"\nNote: Your live CHZ position shows +63% (~$6.26 profit)")
    print(f"      Backtest should capture similar signal if present")
    print("-"*80)

    # Run backtest
    engine = BacktestEngine(config, symbol="CHZ/USD")
    try:
        metrics = await engine.run(start_date=start_date, end_date=end_date)

        # Print results
        print(f"\n{'='*80}")
        print("BACKTEST RESULTS")
        print("="*80)
        print(f"Total Trades: {metrics.total_trades}")
        print(f"Winning Trades: {metrics.winning_trades}")
        print(f"Losing Trades: {metrics.losing_trades}")
        print(f"Win Rate: {metrics.win_rate:.2f}%")
        print("-"*80)
        print(f"Total PnL: ${float(metrics.total_pnl):,.2f}")
        print(f"Total Fees: ${float(metrics.total_fees):,.2f}")
        print(f"Net PnL: ${float(metrics.total_pnl - metrics.total_fees):,.2f}")
        print("-"*80)

        if metrics.winning_trades > 0:
            print(f"Avg Win: ${float(metrics.avg_win):,.2f}")
        if metrics.losing_trades > 0:
            print(f"Avg Loss: ${float(metrics.avg_loss):,.2f}")
        if metrics.profit_factor > 0:
            print(f"Profit Factor: {metrics.profit_factor:.2f}")

        print(f"Max Drawdown: ${float(metrics.max_drawdown):,.2f}")

        # Calculate return %
        starting_equity = Decimal("10000")
        return_pct = float((metrics.total_pnl / starting_equity) * 100)
        print("-"*80)
        print(f"Total Return: {return_pct:.2f}%")
        print("="*80)

        # Show equity curve summary
        if metrics.equity_curve:
            print(f"\nEquity Curve:")
            print(f"  Start: ${float(metrics.equity_curve[0]):,.2f}")
            print(f"  End: ${float(metrics.equity_curve[-1]):,.2f}")
            print(f"  Peak: ${float(metrics.peak_equity):,.2f}")

        return metrics

    except Exception as e:
        logger.error(f"Backtest failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        if getattr(engine, "client", None):
            await engine.client.close()


if __name__ == "__main__":
    asyncio.run(run_chz_backtest())
