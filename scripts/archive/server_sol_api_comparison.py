#!/usr/bin/env python3
"""Run SOL mode comparison using Kraken API-backed backtest fetches."""

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.backtest.backtest_engine import BacktestEngine
from src.config.config import load_config
from src.monitoring.logger import setup_logging


def apply_mode(cfg, mode: str) -> None:
    cfg.strategy.require_ms_change_confirmation = False
    cfg.strategy.skip_reconfirmation_in_trends = True
    cfg.strategy.adx_threshold = 25.0
    cfg.strategy.entry_zone_tolerance_pct = 0.02
    cfg.strategy.min_score_tight_smc_aligned = 65.0
    cfg.strategy.min_score_wide_structure_aligned = 60.0

    if mode == "current_4h_runner":
        cfg.strategy.decision_timeframes = ["4h"]
        cfg.strategy.refinement_timeframes = ["1h", "15m"]
        cfg.strategy.ms_confirmation_candles = 1
        cfg.strategy.tight_smc_atr_stop_min = 0.15
        cfg.strategy.tight_smc_atr_stop_max = 0.30
        cfg.strategy.wide_structure_atr_stop_min = 0.50
        cfg.strategy.wide_structure_atr_stop_max = 0.60
        if cfg.multi_tp:
            cfg.multi_tp.runner_has_fixed_tp = False
    elif mode == "current_4h_fixed_tp3":
        cfg.strategy.decision_timeframes = ["4h"]
        cfg.strategy.refinement_timeframes = ["1h", "15m"]
        cfg.strategy.ms_confirmation_candles = 1
        cfg.strategy.tight_smc_atr_stop_min = 0.15
        cfg.strategy.tight_smc_atr_stop_max = 0.30
        cfg.strategy.wide_structure_atr_stop_min = 0.50
        cfg.strategy.wide_structure_atr_stop_max = 0.60
        if cfg.multi_tp:
            cfg.multi_tp.runner_has_fixed_tp = True
            cfg.multi_tp.runner_tp_r_multiple = 3.0
    elif mode == "legacy_1h_runner":
        cfg.strategy.decision_timeframes = ["1h"]
        cfg.strategy.refinement_timeframes = ["15m"]
        cfg.strategy.ms_confirmation_candles = 2
        cfg.strategy.tight_smc_atr_stop_min = 0.30
        cfg.strategy.tight_smc_atr_stop_max = 0.60
        cfg.strategy.wide_structure_atr_stop_min = 1.00
        cfg.strategy.wide_structure_atr_stop_max = 1.20
        if cfg.multi_tp:
            cfg.multi_tp.runner_has_fixed_tp = False
    else:
        raise ValueError(f"Unknown mode: {mode}")


async def run_mode(mode: str, start: datetime, end: datetime) -> dict:
    cfg = load_config("src/config/config.yaml")
    apply_mode(cfg, mode)
    engine = BacktestEngine(cfg, symbol="SOL/USD")
    try:
        m = await engine.run(start_date=start, end_date=end)
        return {
            "mode": mode,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "trades": m.total_trades,
            "wins": m.winning_trades,
            "losses": m.losing_trades,
            "win_rate": float(m.win_rate),
            "total_pnl": float(m.total_pnl),
            "fees": float(m.total_fees),
            "net_pnl": float(m.total_pnl - m.total_fees),
            "max_dd": float(m.max_drawdown),
        }
    except Exception as e:
        return {"mode": mode, "error": str(e)}
    finally:
        if getattr(engine, "client", None):
            await engine.client.close()


async def main() -> None:
    setup_logging("INFO", "json")
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30)
    modes = ["current_4h_runner", "current_4h_fixed_tp3", "legacy_1h_runner"]
    out = []
    for mode in modes:
        result = await run_mode(mode, start, end)
        out.append(result)
        print("MODE_RESULT:" + json.dumps(result))
        await asyncio.sleep(6)

    ranked = sorted(
        [r for r in out if "net_pnl" in r],
        key=lambda r: r["net_pnl"],
        reverse=True,
    )
    print("RANKING:" + json.dumps(ranked))


if __name__ == "__main__":
    asyncio.run(main())
