#!/usr/bin/env python3
"""Control test: evaluate March 12 c001 params against current data.

Runs the research evaluator with the exact parameter set from the March 12
c001 candidate (the only successful research run) to determine whether the
strategy still produces signals with today's data and candle coverage.
"""

import asyncio
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config.config import load_config
from src.research.evaluator import CandidateEvaluator, EvaluationSpec

# ------------------------------------------------------------------
# March 12 c001 parameters (best historical run: 118 trades, 75.2% WR,
# Sharpe 1.56, +1.19% return)
# ------------------------------------------------------------------
MARCH_12_PARAMS: dict[str, float] = {
    "strategy.adx_threshold": 25.0,
    "strategy.min_score_tight_smc_aligned": 65.0,
    "strategy.min_score_tight_smc_neutral": 55.0,  # Must be lower — EMA 200 unavailable → all signals are "neutral"
    "strategy.min_score_wide_structure_aligned": 60.0,
    "strategy.min_score_wide_structure_neutral": 50.0,
    "strategy.signal_cooldown_hours": 1.092298,
    "strategy.tight_smc_atr_stop_min": 0.161643,
    "strategy.wide_structure_atr_stop_min": 0.459619,
    "strategy.tight_smc_atr_stop_max": 0.638795,
    "strategy.wide_structure_atr_stop_max": 1.221116,
    "strategy.ema_slope_bonus": 7.456321,
    "strategy.fvg_min_size_pct": 0.002,
    "strategy.entry_zone_tolerance_pct": 2.5,
    "strategy.entry_zone_tolerance_atr_mult": 0.75,
    "strategy.bos_volume_threshold_mult": 1.5,
    "strategy.fib_proximity_bps": 120.0,  # Widened from 60 — was blocking all signals
    "strategy.fib_proximity_adaptive_scale": 0.5,
    "strategy.fib_proximity_max_bps": 160.0,  # Widened from 60
    "strategy.structure_fallback_score_premium": 10.0,
}

SYMBOLS = ("BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD", "ADA/USD", "LINK/USD")


def _build_evaluator(config, symbol: str) -> CandidateEvaluator:
    """Build a per-symbol evaluator (mirrors harness._build_evaluator)."""
    return CandidateEvaluator(
        base_config=config,
        spec=EvaluationSpec(
            symbols=(symbol,),
            lookback_days=120,
            starting_equity=Decimal("10000"),
            mode="replay",
            objective_mode="net_pnl_only",
            window_offsets_days=(0,),
            holdout_ratio=0.30,
            replay_data_dir="data/replay",
            replay_timeframes=("15m", "1h", "4h", "1d"),
            auto_backfill_data=True,
        ),
    )


async def main() -> None:
    config = load_config()

    print("=" * 70)
    print("CONTROL TEST — March 12 c001 params")
    print(f"Symbols: {', '.join(SYMBOLS)}")
    print("Lookback: 120 days")
    print(f"Started: {datetime.now(UTC).isoformat()}")
    print("=" * 70, flush=True)

    for symbol in SYMBOLS:
        print(f"\n--- Evaluating {symbol} ---", flush=True)
        try:
            evaluator = _build_evaluator(config, symbol)
            # Prepare replay data (backfill if needed)
            coverage = await evaluator.prepare_symbol_data(symbol)
            print(f"  Coverage: {coverage}", flush=True)

            outcome = await evaluator.evaluate(MARCH_12_PARAMS)
            m = outcome.metrics
            print(f"  Return:    {m.net_return_pct:+.2f}%")
            print(f"  Trades:    {m.trade_count}")
            print(f"  Win rate:  {m.win_rate_pct:.1f}%")
            print(f"  Sharpe:    {m.sharpe:.2f}")
            print(f"  Max DD:    {m.max_drawdown_pct:.2f}%")
            if m.rejection_reasons:
                print(f"  Rejected:  {m.rejection_reasons}")
            print(flush=True)
        except Exception as e:
            print(f"  ERROR: {e}", flush=True)

    print(f"\nFinished: {datetime.now(UTC).isoformat()}")


if __name__ == "__main__":
    asyncio.run(main())
