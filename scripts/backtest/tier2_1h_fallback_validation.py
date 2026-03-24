"""
Tier 2 (1H structure fallback) validation — baseline vs Tier 2 enabled.

Compares baseline (structure_fallback_enabled=False) vs Tier 2 (True) on
6 symbols × 30d. Acceptance (design doc / KBO-13):

- Sharpe improvement ≥ +0.1 vs baseline (average across symbols)
- Drawdown increase ≤ 25% vs baseline worst per-symbol DD

Usage:
    uv run python scripts/backtest/tier2_1h_fallback_validation.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from dotenv import load_dotenv

load_dotenv(".env.local")

from src.backtest.backtest_engine import BacktestEngine
from src.config.config import load_config
from src.monitoring.logger import get_logger, setup_logging

logger = get_logger(__name__)

# Six symbols × 30d (design doc)
SYMBOLS = [
    "BTC/USD",
    "ETH/USD",
    "SOL/USD",
    "XRP/USD",
    "DOGE/USD",
    "AVAX/USD",
]
DAYS = 30

PROFILES = {
    "baseline": {
        "structure_fallback_enabled": False,
        "structure_fallback_score_premium": 15.0,
    },
    "tier2": {
        "structure_fallback_enabled": True,
        "structure_fallback_score_premium": 15.0,
    },
}


def apply_profile(config, profile_name: str):
    """Apply a parameter profile to strategy config."""
    params = PROFILES[profile_name]
    for key, val in params.items():
        setattr(config.strategy, key, val)
    # Align with current production Profile C–style gates (same as tier1 script)
    config.strategy.adx_threshold = 20.0
    config.strategy.min_score_tight_smc_aligned = 60.0
    config.strategy.min_score_tight_smc_neutral = 65.0
    config.strategy.min_score_wide_structure_aligned = 55.0
    config.strategy.min_score_wide_structure_neutral = 60.0
    config.strategy.ema_slope_bonus = 10.0
    config.strategy.decision_timeframes = ["4h"]
    return config


async def run_symbol(symbol: str, config, start: datetime, end: datetime) -> dict:
    """Run backtest for a single symbol."""
    engine = BacktestEngine(config, symbol=symbol)
    try:
        metrics = await engine.run(start_date=start, end_date=end)
        return {
            "symbol": symbol,
            "success": True,
            "trades": metrics.total_trades,
            "wins": metrics.winning_trades,
            "losses": metrics.losing_trades,
            "win_rate": metrics.win_rate,
            "pnl": float(metrics.total_pnl),
            "fees": float(metrics.total_fees),
            "max_dd": float(metrics.max_drawdown),
            "sharpe": metrics.sharpe_ratio,
            "profit_factor": metrics.profit_factor,
        }
    except Exception as e:
        logger.error("Backtest failed for %s: %s", symbol, e)
        return {"symbol": symbol, "success": False, "error": str(e)[:120]}
    finally:
        if getattr(engine, "client", None):
            await engine.client.close()


async def run_profile(profile_name: str) -> dict:
    """Run backtest suite for a profile."""
    config = load_config("src/config/config.yaml")
    config = apply_profile(config, profile_name)

    end = datetime.now(UTC)
    start = end - timedelta(days=DAYS)

    params = PROFILES[profile_name]
    print(f"\n{'=' * 70}")
    print(f"PROFILE: {profile_name.upper()}")
    print(
        f"  structure_fallback_enabled={params['structure_fallback_enabled']}, "
        f"premium={params['structure_fallback_score_premium']}"
    )
    print(f"  Period: {start.date()} to {end.date()} ({DAYS}d)")
    print(f"{'=' * 70}")

    results = []
    for sym in SYMBOLS:
        print(f"  {sym}...", end=" ", flush=True)
        r = await run_symbol(sym, config, start, end)
        results.append(r)
        if r["success"]:
            print(
                f"{r['trades']} trades, {r['win_rate']:.0f}% WR, "
                f"${r['pnl']:+,.2f}, DD={r['max_dd']:.1%}"
            )
        else:
            print(f"FAIL: {r.get('error', '?')[:50]}")
        await asyncio.sleep(1.0)

    ok = [r for r in results if r["success"]]
    return {
        "profile": profile_name,
        "symbols_ok": len(ok),
        "total_trades": sum(r["trades"] for r in ok),
        "total_pnl": sum(r["pnl"] for r in ok),
        "avg_win_rate": (sum(r["win_rate"] for r in ok) / len(ok) if ok else 0),
        "worst_dd": max((r["max_dd"] for r in ok), default=0.0),
        "avg_sharpe": (sum(r["sharpe"] for r in ok) / len(ok) if ok else 0.0),
        "avg_pf": (sum(r["profit_factor"] for r in ok) / len(ok) if ok else 0.0),
        "per_symbol": results,
    }


def print_comparison(base: dict, tier2: dict) -> None:
    """Print comparison and Tier 2 acceptance criteria."""
    print(f"\n{'=' * 70}")
    print("TIER 2 (1H FALLBACK) VALIDATION — COMPARISON")
    print(f"{'=' * 70}")
    print(f"{'Metric':<22} {'Baseline':>16} {'Tier 2':>16} {'Delta':>14}")
    print("-" * 70)

    sharpe_delta = tier2["avg_sharpe"] - base["avg_sharpe"]
    dd_base = base["worst_dd"]
    dd_t2 = tier2["worst_dd"]
    if dd_base > 1e-9:
        dd_rel_increase = (dd_t2 - dd_base) / dd_base
    else:
        dd_rel_increase = float("inf") if dd_t2 > 0 else 0.0

    rows = [
        ("Total Trades", base["total_trades"], tier2["total_trades"], None),
        ("Avg Win Rate", f"{base['avg_win_rate']:.1f}%", f"{tier2['avg_win_rate']:.1f}%",
         f"{tier2['avg_win_rate'] - base['avg_win_rate']:+.1f}%"),
        ("Total PnL", f"${base['total_pnl']:,.2f}", f"${tier2['total_pnl']:,.2f}",
         f"${tier2['total_pnl'] - base['total_pnl']:+,.2f}"),
        ("Worst Drawdown", f"{base['worst_dd']:.1%}", f"{tier2['worst_dd']:.1%}",
         f"{(dd_t2 - dd_base) * 100:+.1f} pp"),
        ("Avg Sharpe", f"{base['avg_sharpe']:.2f}", f"{tier2['avg_sharpe']:.2f}",
         f"{sharpe_delta:+.2f}"),
        ("Avg Profit Factor", f"{base['avg_pf']:.2f}", f"{tier2['avg_pf']:.2f}",
         f"{tier2['avg_pf'] - base['avg_pf']:+.2f}"),
    ]

    for label, bval, tval, delta in rows:
        if delta is not None:
            d = delta
        elif isinstance(bval, int) and isinstance(tval, int):
            d = f"{tval - bval:+d}"
        else:
            d = ""
        bstr = str(bval) if not isinstance(bval, str) else bval
        tstr = str(tval) if not isinstance(tval, str) else tval
        print(f"{label:<22} {bstr:>16} {tstr:>16} {d:>14}")

    print("=" * 70)

    # Acceptance: Sharpe ≥ +0.1 and DD increase ≤ 25% vs baseline worst DD
    sharpe_ok = sharpe_delta >= 0.1
    dd_ok = dd_rel_increase <= 0.25
    print("ACCEPTANCE (design doc):")
    print(f"  Sharpe delta ≥ +0.1     → {sharpe_delta:+.3f}  [{'PASS' if sharpe_ok else 'FAIL'}]")
    print(
        f"  DD increase ≤ 25%       → rel={dd_rel_increase:.1%}  [{'PASS' if dd_ok else 'FAIL'}]"
    )
    print("=" * 70)

    if sharpe_ok and dd_ok:
        print("VERDICT: TIER 2 MEETS ACCEPTANCE — consider canary enablement")
    elif sharpe_ok and not dd_ok:
        print("VERDICT: SHARPE OK BUT DD TOO HIGH — tune premium or narrow symbols")
    elif not sharpe_ok and dd_ok:
        print("VERDICT: DD OK BUT SHARPE GAIN INSUFFICIENT — needs more data or tuning")
    else:
        print("VERDICT: TIER 2 DOES NOT MEET GATES — keep disabled")
    print("=" * 70)


def setup_db_mock():
    """Mock DB so BacktestEngine uses API for candles."""
    mock_session = MagicMock()
    mock_session.query.return_value = mock_session
    mock_session.filter.return_value = mock_session
    mock_session.filter_by.return_value = mock_session
    mock_session.order_by.return_value = mock_session
    mock_session.limit.return_value = mock_session
    mock_session.offset.return_value = mock_session
    mock_session.all.return_value = []
    mock_session.first.return_value = None
    mock_session.one_or_none.return_value = None
    mock_session.count.return_value = 0
    mock_session.scalar.return_value = 0

    @contextmanager
    def _fake_session():
        yield mock_session

    mock_db = MagicMock()
    mock_db.get_session = _fake_session

    patcher = patch("src.storage.db._db_instance", mock_db)
    patcher.start()
    return patcher


async def main():
    setup_logging("WARNING", "json")
    db_patcher = setup_db_mock()
    print("Tier 2 (1H structure fallback) validation")
    print(f"Symbols ({len(SYMBOLS)}): {SYMBOLS}")
    print(f"Period: {DAYS} days")
    print("(DB mocked — fetching candles from Kraken API)")

    try:
        base = await run_profile("baseline")
        tier2 = await run_profile("tier2")
        print_comparison(base, tier2)
    finally:
        db_patcher.stop()


if __name__ == "__main__":
    asyncio.run(main())
