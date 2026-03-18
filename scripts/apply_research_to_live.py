#!/usr/bin/env python3
"""
Apply research best-by-symbol results to live trading config.

Reads a research run's best_by_symbol artifact (or state + leaderboard), filters to
sufficient results (non-baseline, min trade count), maps params to strategy/risk
symbol_overrides, and writes live_research_overrides.yaml so load_config merges them.

Optional promotion: with --promote-new-coins, symbols not in the current whitelist
that meet the promotion bar (accepted, min trades, drawdown/return gates) are
added to the whitelist.

Usage:
  apply_research_to_live.py --best-by-symbol PATH [--live-universe CSV] [--out PATH] [--min-trades N]
  apply_research_to_live.py --run-dir PATH   # uses run's artifacts and optional state
  apply_research_to_live.py --run-dir PATH --promote-new-coins  # add promotion-ready coins to whitelist

Live trading must restart to pick up config changes.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yaml

# StrategySymbolOverride fields (research only mutates strategy params)
STRATEGY_OVERRIDE_KEYS = frozenset({
    "adx_threshold",
    "fvg_min_size_pct",
    "entry_zone_tolerance_pct",
    "entry_zone_tolerance_atr_mult",
    "min_score_tight_smc_aligned",
    "min_score_wide_structure_aligned",
    "min_score_wide_structure_neutral",
    "signal_cooldown_hours",
    "tight_smc_atr_stop_min",
    "tight_smc_atr_stop_max",
    "wide_structure_atr_stop_min",
    "wide_structure_atr_stop_max",
})

DEFAULT_LIVE_UNIVERSE = [
    "BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD", "ADA/USD", "LINK/USD",
    "DOGE/USD", "AVAX/USD", "LTC/USD", "TRX/USD", "DOT/USD", "BCH/USD",
]

# Promotion gate (new coins to live): align with harness _promotion_gate
PROMOTION_MAX_DRAWDOWN_PCT = 35.0
PROMOTION_MIN_NET_RETURN_PCT = -10.0
PROMOTION_DRAWDOWN_RETURN_CAP_PCT = 20.0  # max drawdown when return < 2%
PROMOTION_WEAK_RETURN_PCT = 2.0


def _params_to_strategy_override(params: dict) -> dict:
    """Map research param keys (strategy.xyz) to StrategySymbolOverride dict (xyz only)."""
    out = {}
    for key, value in params.items():
        if not key.startswith("strategy."):
            continue
        attr = key.split(".", 1)[1]
        if attr not in STRATEGY_OVERRIDE_KEYS:
            continue
        try:
            out[attr] = float(value)
        except (TypeError, ValueError):
            continue
    return out


def load_best_by_symbol(path: Path) -> dict:
    """Load best_by_symbol from JSON (run_id_best_by_symbol.json style)."""
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("best_by_symbol") or {}


def load_best_from_run_dir(run_dir: Path) -> dict:
    """Load best_by_symbol from a run directory (artifacts + optional state)."""
    artifacts = run_dir / "artifacts"
    if not artifacts.is_dir():
        return {}
    best_files = sorted(artifacts.glob("*_best_by_symbol.json"))
    if not best_files:
        return {}
    return load_best_by_symbol(best_files[-1])


def _passes_promotion_gate(entry: dict, promotion_min_trades: int) -> bool:
    """True if entry is accepted and meets promotion bar (drawdown/return)."""
    if not entry.get("accepted", False):
        return False
    metrics = entry.get("metrics") or {}
    trade_count = int(metrics.get("trade_count") or 0)
    if trade_count < promotion_min_trades:
        return False
    if str(entry.get("candidate_id") or "").endswith("_baseline"):
        return False
    net_return = float(metrics.get("net_return_pct") or -999.0)
    max_dd = float(metrics.get("max_drawdown_pct") or 999.0)
    if max_dd > PROMOTION_MAX_DRAWDOWN_PCT:
        return False
    if net_return <= PROMOTION_MIN_NET_RETURN_PCT:
        return False
    if max_dd > PROMOTION_DRAWDOWN_RETURN_CAP_PCT and net_return < PROMOTION_WEAK_RETURN_PCT:
        return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply research results to live overrides")
    parser.add_argument("--best-by-symbol", type=Path, help="Path to *_best_by_symbol.json")
    parser.add_argument("--run-dir", type=Path, help="Run directory (artifacts/ inside)")
    parser.add_argument(
        "--live-universe",
        type=str,
        default=",".join(DEFAULT_LIVE_UNIVERSE),
        help="Comma-separated symbol list for assets.whitelist (default: 12-coin research set)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output YAML path (default: config dir / live_research_overrides.yaml)",
    )
    parser.add_argument(
        "--min-trades",
        type=int,
        default=2,
        help="Minimum trade_count to consider a non-baseline result sufficient (default: 2)",
    )
    parser.add_argument(
        "--promote-new-coins",
        action="store_true",
        help="Add promotion-ready symbols (accepted, promotion-min-trades, drawdown/return gate) to whitelist",
    )
    parser.add_argument(
        "--promotion-min-trades",
        type=int,
        default=10,
        help="Minimum trade_count for promoting a new symbol to live (default: 10)",
    )
    parser.add_argument(
        "--max-promotions",
        type=int,
        default=None,
        help="Max new coins to add per run (default: no limit)",
    )
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "src" / "config",
        help="Config directory for default --out",
    )
    args = parser.parse_args()

    if args.best_by_symbol:
        best_by_symbol = load_best_by_symbol(args.best_by_symbol)
    elif args.run_dir:
        best_by_symbol = load_best_from_run_dir(args.run_dir)
    else:
        parser.error("Provide --best-by-symbol or --run-dir")

    out_path = args.out or (args.config_dir / "live_research_overrides.yaml")

    # Base live universe: from --live-universe or, when promoting, from current file
    live_universe = [s.strip() for s in args.live_universe.split(",") if s.strip()]
    if args.promote_new_coins and out_path.exists():
        try:
            existing = yaml.safe_load(out_path.read_text()) or {}
            current_list = existing.get("assets", {}).get("whitelist") or []
            if current_list:
                live_universe = list(current_list)
        except Exception:
            pass

    # Optionally add new coins that meet promotion bar
    if args.promote_new_coins:
        current_set = set(live_universe)
        to_add: list[str] = []
        for symbol, entry in best_by_symbol.items():
            if not isinstance(entry, dict) or symbol in current_set:
                continue
            if not _passes_promotion_gate(entry, args.promotion_min_trades):
                continue
            to_add.append(symbol)
            current_set.add(symbol)
            if args.max_promotions is not None and len(to_add) >= args.max_promotions:
                break
        if to_add:
            live_universe = list(live_universe) + to_add
            print(f"Promoting {len(to_add)} new coin(s) to live: {to_add}")

    strategy_overrides: dict[str, dict] = {}

    for symbol, entry in best_by_symbol.items():
        if not isinstance(entry, dict):
            continue
        candidate_id = entry.get("candidate_id") or ""
        params = entry.get("params") or {}
        metrics = entry.get("metrics") or {}
        trade_count = int(metrics.get("trade_count") or 0)
        accepted = entry.get("accepted", False)

        # Only apply non-baseline candidates that meet minimum bar
        if str(candidate_id).endswith("_baseline"):
            continue
        if trade_count < args.min_trades:
            continue
        override = _params_to_strategy_override(params)
        if not override:
            continue
        strategy_overrides[symbol] = override

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Merge with existing overrides so we don't drop previous winners when this run has none
    existing_strategy = {}
    if out_path.exists():
        try:
            existing = yaml.safe_load(out_path.read_text()) or {}
            existing_strategy = existing.get("strategy", {}).get("symbol_overrides") or {}
        except Exception:
            pass
    merged_strategy = {**existing_strategy, **strategy_overrides}

    payload = {
        "strategy": {"symbol_overrides": merged_strategy},
        "risk": {"symbol_overrides": {}},
        "assets": {
            "mode": "whitelist",
            "whitelist": live_universe,
        },
    }
    with open(out_path, "w") as f:
        yaml.safe_dump(payload, f, default_flow_style=False, sort_keys=True)

    print(f"Wrote {len(merged_strategy)} symbol overrides and whitelist ({len(live_universe)} symbols) to {out_path}")
    if merged_strategy:
        for sym, ov in merged_strategy.items():
            print(f"  {sym}: {list(ov.keys())}")

    # Promotion-ready summary for logs: accepted >=10 trades in run
    live_set = set(live_universe)
    accepted_live: list[str] = []
    accepted_not_live: list[str] = []
    for symbol, entry in best_by_symbol.items():
        if not isinstance(entry, dict):
            continue
        if not entry.get("accepted", False):
            continue
        metrics = entry.get("metrics") or {}
        if int(metrics.get("trade_count") or 0) < 10:
            continue
        if symbol in live_set:
            accepted_live.append(symbol)
        else:
            accepted_not_live.append(symbol)
    if accepted_live or accepted_not_live:
        print("Promotion-ready summary: accepted (>=10 trades) in whitelist: " + (", ".join(sorted(accepted_live)) or "none"))
        if accepted_not_live:
            print("Promotion-ready summary: accepted (>=10 trades) not in whitelist (gate or max_promotions): " + ", ".join(sorted(accepted_not_live)))


if __name__ == "__main__":
    main()
