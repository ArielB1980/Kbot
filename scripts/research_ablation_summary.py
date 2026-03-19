"""Summarize 4-way ablation batch outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


CASES = ("baseline", "structure_relaxed", "weekly_relaxed", "both_relaxed")


def _load_candidates(run_dir: Path) -> list[dict[str, Any]]:
    leaderboard_files = sorted((run_dir / "artifacts").glob("*_leaderboard.json"))
    if not leaderboard_files:
        return []
    payload = json.loads(leaderboard_files[-1].read_text(encoding="utf-8"))
    candidates = payload.get("candidates") or []
    return [c for c in candidates if isinstance(c, dict)]


def _load_state(run_dir: Path) -> dict[str, Any]:
    state_file = run_dir / "state.json"
    if not state_file.exists():
        return {}
    payload = json.loads(state_file.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _load_log(run_dir: Path) -> str:
    log_file = run_dir / "research.log"
    if not log_file.exists():
        return ""
    return log_file.read_text(encoding="utf-8", errors="ignore")


def _summarize_case(batch_dir: Path, case: str) -> dict[str, Any]:
    run_dir = batch_dir / case
    candidates = _load_candidates(run_dir)
    state = _load_state(run_dir)
    log_text = _load_log(run_dir)

    max_trade_count = max((float(c.get("trade_count") or 0.0) for c in candidates), default=0.0)
    best_score = max((float(c.get("score") or -1e9) for c in candidates), default=None)
    max_net_return_pct = max(
        (float(c.get("net_return_pct")) for c in candidates if c.get("net_return_pct") is not None),
        default=None,
    )
    nonbaseline_candidates = sum(
        1
        for c in candidates
        if c.get("candidate_id") and not str(c.get("candidate_id")).endswith("_baseline")
    )
    accepted_candidates = sum(1 for c in candidates if c.get("accepted") is True)

    symbol_phases: dict[str, Any] = {}
    symbol_progress = state.get("symbol_progress") or {}
    if isinstance(symbol_progress, dict):
        for symbol, row in symbol_progress.items():
            if isinstance(row, dict):
                symbol_phases[symbol] = row.get("phase")

    return {
        "case": case,
        "phase": state.get("phase"),
        "completed": "{}/{}".format(
            len(state.get("completed_symbols") or []),
            state.get("total_symbols"),
        ),
        "max_trade_count": max_trade_count,
        "best_score": best_score,
        "max_net_return_pct": max_net_return_pct,
        "nonbaseline_candidates": nonbaseline_candidates,
        "accepted_candidates": accepted_candidates,
        "count_4h_structure_required": log_text.count("4H_STRUCTURE_REQUIRED"),
        "count_outside_weekly_zone": log_text.count("outside_weekly_zone"),
        "count_ablation_structure_bypass": log_text.count(
            "Ablation: bypassing decision structure gate in replay"
        ),
        "count_ablation_weekly_bypass": log_text.count(
            "Ablation: bypassing weekly-zone gate in replay"
        ),
        "count_waiting_structure_break": log_text.count(
            "waiting for structure break"
        ),
        "count_waiting_structure_confirmation": log_text.count(
            "waiting for confirmation"
        ),
        "count_waiting_reconfirmation": log_text.count(
            "waiting for reconfirmation"
        ),
        "count_adx_ranging_filter": log_text.count(
            "Ranging market: ADX"
        ),
        "count_score_reject": log_text.count(
            "Signal Rejected (Score)"
        ),
        "count_fib_gate_reject": log_text.count(
            "tight_smc entry not in OTE/Key Fib"
        ),
        "count_fib_missing_reject": log_text.count(
            "No Fib structure found for tight_smc"
        ),
        "count_direction_mismatch": log_text.count(
            "Signal direction mismatch"
        ),
        "count_low_conviction_gate": log_text.count(
            "ENTRY_BLOCKED_LOW_CONVICTION"
        ),
        "symbol_phases": symbol_phases,
    }


def summarize_batch(batch_dir: Path) -> dict[str, Any]:
    rows = [_summarize_case(batch_dir, case) for case in CASES]
    baseline = rows[0]
    deltas: list[dict[str, Any]] = []
    for row in rows[1:]:
        deltas.append(
            {
                "case": row["case"],
                "delta_max_trade_count": float(row["max_trade_count"] or 0.0)
                - float(baseline["max_trade_count"] or 0.0),
                "delta_best_score": float(row["best_score"] or 0.0)
                - float(baseline["best_score"] or 0.0),
                "delta_count_4h_structure_required": int(row["count_4h_structure_required"] or 0)
                - int(baseline["count_4h_structure_required"] or 0),
                "delta_count_outside_weekly_zone": int(row["count_outside_weekly_zone"] or 0)
                - int(baseline["count_outside_weekly_zone"] or 0),
            }
        )
    return {
        "batch_dir": str(batch_dir),
        "cases": rows,
        "deltas_vs_baseline": deltas,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-dir", required=True, type=Path)
    parser.add_argument("--out-file", type=Path, default=None)
    args = parser.parse_args()

    payload = summarize_batch(args.batch_dir)
    if args.out_file is not None:
        args.out_file.parent.mkdir(parents=True, exist_ok=True)
        args.out_file.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
