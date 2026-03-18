"""Campaign-level stop gate for continuous research.

Stops a campaign when either:
1) a meaningful result is found, or
2) repeated cycles strongly support the "not profitable" hypothesis.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class CycleStats:
    run_id: str
    ts: str
    completed_symbols: int
    total_symbols: int
    nonbaseline_best: int
    accepted_best: int
    avg_best_return_pct: float
    avg_best_trade_count: float
    counterfactual_uplift: float | None
    data_failure_ratio: float
    comparable: bool


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _tail_text(path: Path, max_bytes: int = 4_000_000) -> str:
    if not path.exists():
        return ""
    size = path.stat().st_size
    with path.open("rb") as f:
        if size > max_bytes:
            f.seek(size - max_bytes)
        raw = f.read()
    return raw.decode("utf-8", errors="replace")


def _cycle_stats(run_id: str, state_file: Path, artifacts_dir: Path, run_log: Path | None = None) -> CycleStats:
    state = _read_json(state_file)
    best_map = state.get("symbol_best_candidates") or {}
    completed = len(state.get("completed_symbols") or [])
    total = int(state.get("total_symbols") or 0)

    nonbaseline = 0
    accepted_best = 0
    returns: list[float] = []
    trades: list[int] = []
    best_file_candidates = sorted(artifacts_dir.glob("*_best_by_symbol.json"))
    if best_file_candidates:
        best_payload = _read_json(best_file_candidates[-1]).get("best_by_symbol") or {}
        for symbol, payload in best_payload.items():
            _ = symbol
            cid = str((payload or {}).get("candidate_id") or "")
            if cid and not cid.endswith("_baseline"):
                nonbaseline += 1
            if bool((payload or {}).get("accepted", False)):
                accepted_best += 1
            m = (payload or {}).get("metrics") or {}
            returns.append(float(m.get("net_return_pct") or 0.0))
            trades.append(int(m.get("trade_count") or 0))
    else:
        # Fallback from state only
        for cid in best_map.values():
            if not str(cid).endswith("_baseline"):
                nonbaseline += 1

    cf_uplift: float | None = None
    cf_has_utility: bool | None = None
    cf_single = artifacts_dir / "counterfactual_single.json"
    if cf_single.exists():
        report = (_read_json(cf_single).get("report") or {})
        raw = report.get("utility_uplift")
        if raw is not None:
            cf_uplift = float(raw)
        baseline_utility = report.get("baseline_utility")
        if baseline_utility is None:
            baseline_utility = report.get("baseline_utility_score")
        cf_has_utility = baseline_utility is not None

    avg_trades = (sum(trades) / len(trades)) if trades else 0.0
    has_trades = avg_trades > 0.0
    data_failure_ratio = 0.0
    if run_log is not None and run_log.exists():
        tail = _tail_text(run_log)
        if tail:
            data_failures = tail.count("reason=data_failure")
            safe_hold = tail.count("Kill switch SAFE_HOLD")
            if safe_hold > 0:
                data_failure_ratio = data_failures / safe_hold
    comparable = has_trades and (cf_has_utility is not False) and data_failure_ratio < 0.20

    return CycleStats(
        run_id=run_id,
        ts=datetime.now(timezone.utc).isoformat(),
        completed_symbols=completed,
        total_symbols=total,
        nonbaseline_best=nonbaseline,
        accepted_best=accepted_best,
        avg_best_return_pct=(sum(returns) / len(returns)) if returns else 0.0,
        avg_best_trade_count=avg_trades,
        counterfactual_uplift=cf_uplift,
        data_failure_ratio=data_failure_ratio,
        comparable=comparable,
    )


def _load_history(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    entries: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entries.append(json.loads(line))
    return entries


def _append_history(path: Path, stats: CycleStats) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(asdict(stats), sort_keys=True) + "\n")


def _decide(
    history: list[dict[str, Any]],
    *,
    meaningful_nonbaseline_min: int,
    meaningful_accepted_min: int,
    proof_window_cycles: int,
    allow_falsification_stop: bool,
) -> tuple[str, str]:
    """Return (decision, reason)."""
    if not history:
        return "continue", "no_history"

    latest = history[-1]
    if (
        int(latest.get("nonbaseline_best") or 0) >= meaningful_nonbaseline_min
        and int(latest.get("accepted_best") or 0) >= meaningful_accepted_min
    ):
        return "stop_meaningful_result", "meaningful_threshold_met"

    if len(history) < proof_window_cycles:
        return "continue", "insufficient_cycles_for_falsification"

    if not allow_falsification_stop:
        return "continue", "falsification_stop_disabled"

    window = history[-proof_window_cycles:]
    if any(not bool(x.get("comparable", False)) for x in window):
        return "continue", "falsification_window_non_comparable"
    all_baseline = all(int(x.get("nonbaseline_best") or 0) == 0 for x in window)
    no_accepted = all(int(x.get("accepted_best") or 0) == 0 for x in window)
    non_positive_uplift = all(
        (x.get("counterfactual_uplift") is None) or float(x.get("counterfactual_uplift") or 0.0) <= 0.0
        for x in window
    )
    full_or_near_full = all(
        int(x.get("total_symbols") or 0) > 0 and int(x.get("completed_symbols") or 0) >= int(x.get("total_symbols") or 0) * 0.9
        for x in window
    )
    if all_baseline and no_accepted and non_positive_uplift and full_or_near_full:
        return "stop_not_profitable_supported", "falsification_window_all_flat"
    return "continue", "campaign_still_informative"


def main() -> None:
    parser = argparse.ArgumentParser(description="Campaign stop gate for continuous research.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--state-file", required=True)
    parser.add_argument("--artifacts-dir", required=True)
    parser.add_argument("--run-log", default="")
    parser.add_argument("--history-file", required=True)
    parser.add_argument("--decision-file", required=True)
    parser.add_argument("--meaningful-nonbaseline-min", type=int, default=3)
    parser.add_argument("--meaningful-accepted-min", type=int, default=1)
    parser.add_argument("--proof-window-cycles", type=int, default=6)
    parser.add_argument("--allow-falsification-stop", action="store_true")
    args = parser.parse_args()

    state_file = Path(args.state_file)
    if not state_file.exists():
        return

    run_log = Path(args.run_log) if args.run_log else None
    stats = _cycle_stats(args.run_id, state_file=state_file, artifacts_dir=Path(args.artifacts_dir), run_log=run_log)
    _append_history(Path(args.history_file), stats)
    history = _load_history(Path(args.history_file))
    decision, reason = _decide(
        history,
        meaningful_nonbaseline_min=max(1, int(args.meaningful_nonbaseline_min)),
        meaningful_accepted_min=max(0, int(args.meaningful_accepted_min)),
        proof_window_cycles=max(2, int(args.proof_window_cycles)),
        allow_falsification_stop=bool(args.allow_falsification_stop),
    )

    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "run_id": args.run_id,
        "decision": decision,
        "reason": reason,
        "latest_cycle": asdict(stats),
    }
    decision_file = Path(args.decision_file)
    decision_file.parent.mkdir(parents=True, exist_ok=True)
    decision_file.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()

