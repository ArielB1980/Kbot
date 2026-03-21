#!/usr/bin/env python3
"""
Server-only SOL backtest validation helper.

Runs:
1) Two-window sanity check (disjoint ranges)
2) Walk-forward validation (90d train -> 30d test)
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.backtest.backtest_engine import BacktestEngine
from src.config.config import load_config
from src.monitoring.logger import setup_logging


SYMBOL = "SOL/USD"


def _apply_mode(cfg, mode: str) -> None:
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


async def _run_once(mode: str, start: datetime, end: datetime) -> dict:
    cfg = load_config("src/config/config.yaml")
    _apply_mode(cfg, mode)

    engine = BacktestEngine(cfg, symbol=SYMBOL)
    try:
        m = await engine.run(start_date=start, end_date=end)
        return {
            "mode": mode,
            "trades": m.total_trades,
            "wins": m.winning_trades,
            "losses": m.losing_trades,
            "win_rate": float(m.win_rate),
            "total_pnl": float(m.total_pnl),
            "fees": float(m.total_fees),
            "net_pnl": float(m.total_pnl - m.total_fees),
            "max_dd": float(m.max_drawdown),
        }
    finally:
        if getattr(engine, "client", None):
            await engine.client.close()


async def _run_with_retry(mode: str, start: datetime, end: datetime, retries: int = 3) -> dict:
    for i in range(retries):
        try:
            return await _run_once(mode, start, end)
        except Exception as e:
            msg = str(e)
            if ("Too many requests" in msg or "Rate limit" in msg or "DDoSProtection" in msg) and i < retries - 1:
                await asyncio.sleep(10 * (i + 1))
                continue
            return {
                "mode": mode,
                "error": msg[:300],
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "total_pnl": 0.0,
                "fees": 0.0,
                "net_pnl": -1e18,
                "max_dd": 1.0,
            }


async def run_window_sanity() -> None:
    windows = [
        ("2025-09-01T00:00:00+00:00", "2025-10-01T00:00:00+00:00"),
        ("2026-01-20T00:00:00+00:00", "2026-02-20T00:00:00+00:00"),
    ]
    for start_s, end_s in windows:
        start = datetime.fromisoformat(start_s)
        end = datetime.fromisoformat(end_s)
        res = await _run_with_retry("current_4h_runner", start, end)
        print("WINDOW_RESULT:" + json.dumps({"start": start_s, "end": end_s, **res}))
        await asyncio.sleep(6)


async def run_walk_forward() -> None:
    now = datetime.now(timezone.utc)
    start_180 = now - timedelta(days=180)
    modes = ["current_4h_runner", "current_4h_fixed_tp3", "legacy_1h_runner"]

    folds = []
    for i in range(3):
        tr_start = start_180 + timedelta(days=30 * i)
        tr_end = tr_start + timedelta(days=90)
        te_start = tr_end
        te_end = te_start + timedelta(days=30)
        folds.append((f"fold_{i+1}", tr_start, tr_end, te_start, te_end))

    folds.append((
        "fold_4",
        now - timedelta(days=120),
        now - timedelta(days=30),
        now - timedelta(days=30),
        now,
    ))

    details = []
    for fold_name, tr_start, tr_end, te_start, te_end in folds:
        train = []
        test = []
        for mode in modes:
            train.append(await _run_with_retry(mode, tr_start, tr_end))
            await asyncio.sleep(6)
        selected = sorted(train, key=lambda r: r.get("net_pnl", -1e18), reverse=True)[0]["mode"]

        for mode in modes:
            test.append(await _run_with_retry(mode, te_start, te_end))
            await asyncio.sleep(6)

        best_test = sorted(test, key=lambda r: r.get("net_pnl", -1e18), reverse=True)[0]
        selected_test = next((r for r in test if r["mode"] == selected), None)
        fold_result = {
            "fold": fold_name,
            "train_range": [tr_start.isoformat(), tr_end.isoformat()],
            "test_range": [te_start.isoformat(), te_end.isoformat()],
            "selected_by_train": selected,
            "best_test_mode": best_test["mode"],
            "selected_test_net_pnl": selected_test["net_pnl"] if selected_test else None,
            "best_test_net_pnl": best_test["net_pnl"],
            "train_results": train,
            "test_results": test,
        }
        details.append(fold_result)
        print("FOLD_RESULT:" + json.dumps(fold_result))

    mode_total_test_net = {}
    selected_wins = 0
    selected_total_net = 0.0
    for item in details:
        if item["selected_by_train"] == item["best_test_mode"]:
            selected_wins += 1
        selected_total_net += float(item["selected_test_net_pnl"] or 0.0)
        for tr in item["test_results"]:
            mode_total_test_net[tr["mode"]] = mode_total_test_net.get(tr["mode"], 0.0) + float(tr["net_pnl"])

    final = {
        "symbol": SYMBOL,
        "walk_forward": "90d_train_30d_test_rolling",
        "folds": len(details),
        "selection_accuracy": f"{selected_wins}/{len(details)}",
        "selected_policy_total_test_net_pnl": selected_total_net,
        "mode_total_test_net_pnl": mode_total_test_net,
    }
    print("WALK_FORWARD_FINAL:" + json.dumps(final))


async def main() -> None:
    setup_logging("INFO", "json")
    await run_window_sanity()
    await run_walk_forward()


if __name__ == "__main__":
    asyncio.run(main())
