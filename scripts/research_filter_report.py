"""Generate ranked blocker report from research run logs/state."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


KNOWN_BLOCKERS: tuple[tuple[str, str], ...] = (
    ("decision_structure_required", r"4H_STRUCTURE_REQUIRED"),
    ("outside_weekly_zone", r"outside_weekly_zone"),
    ("insufficient_signal", r"insufficient_signal\("),
    ("no_futures_ticker", r"no_futures_ticker"),
    ("data_failure", r"reason=data_failure|Kill switch SAFE_HOLD"),
    ("incomplete_window", r"incomplete_window"),
    ("paused_candle_health_high", r"paused_candle_health_ratio_high"),
    ("low_comparability_for_promotion", r"low_comparability_for_promotion"),
)


def _safe_read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="ignore")


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    except json.JSONDecodeError:
        return {}
    return {}


def _count_known_blockers(log_text: str) -> list[dict[str, Any]]:
    counts: list[dict[str, Any]] = []
    for blocker_id, pattern in KNOWN_BLOCKERS:
        count = len(re.findall(pattern, log_text))
        counts.append({"blocker": blocker_id, "count": int(count)})
    return sorted(counts, key=lambda row: row["count"], reverse=True)


def _top_reason_tokens(log_text: str, top_n: int = 20) -> list[dict[str, Any]]:
    reason_counts: Counter[str] = Counter()
    for match in re.finditer(r"reason=([A-Za-z0-9_\-\.]+)", log_text):
        reason_counts[match.group(1)] += 1
    top = reason_counts.most_common(top_n)
    return [{"reason": reason, "count": int(count)} for reason, count in top]


def _symbol_phase_summary(state: dict[str, Any]) -> dict[str, int]:
    summary: Counter[str] = Counter()
    symbol_progress = state.get("symbol_progress")
    if not isinstance(symbol_progress, dict):
        return {}
    for row in symbol_progress.values():
        if not isinstance(row, dict):
            continue
        phase = str(row.get("phase") or "unknown")
        summary[phase] += 1
    return dict(sorted(summary.items(), key=lambda kv: kv[1], reverse=True))


def build_report(*, run_id: str, state_path: Path, run_log_path: Path) -> dict[str, Any]:
    state = _load_state(state_path)
    log_text = _safe_read_text(run_log_path)
    blockers = _count_known_blockers(log_text)
    known_total = int(sum(row["count"] for row in blockers))
    ranked: list[dict[str, Any]] = []
    for row in blockers:
        count = int(row["count"])
        share = (count / known_total) if known_total > 0 else 0.0
        ranked.append(
            {
                "blocker": row["blocker"],
                "count": count,
                "share_of_known_blockers": round(share, 6),
            }
        )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "state_path": str(state_path),
        "run_log_path": str(run_log_path),
        "state_summary": {
            "phase": state.get("phase"),
            "completed_symbols": len(state.get("completed_symbols") or []),
            "total_symbols": int(state.get("total_symbols") or 0),
            "current_symbol": state.get("current_symbol"),
            "symbol_phase_counts": _symbol_phase_summary(state),
        },
        "known_blocker_total": known_total,
        "ranked_blockers": ranked,
        "top_reason_tokens": _top_reason_tokens(log_text),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True, help="Continuous run id (e.g. continuous_20260315_122414)")
    parser.add_argument("--state-file", required=True, type=Path, help="Path to state.json for the run")
    parser.add_argument("--run-log", required=True, type=Path, help="Path to research.log for the run")
    parser.add_argument("--out-file", required=True, type=Path, help="Output JSON path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(run_id=args.run_id, state_path=args.state_file, run_log_path=args.run_log)
    args.out_file.parent.mkdir(parents=True, exist_ok=True)
    args.out_file.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

    ranked = report["ranked_blockers"]
    if ranked:
        top = ranked[0]
        print(
            "filter_report run_id={} top_blocker={} count={} known_total={}".format(
                args.run_id,
                top["blocker"],
                top["count"],
                report["known_blocker_total"],
            )
        )
    else:
        print(f"filter_report run_id={args.run_id} top_blocker=none count=0 known_total=0")
    print(f"filter_report_file={args.out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
