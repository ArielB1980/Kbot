"""Build a step-by-step gate funnel from COUNTERFACTUAL_DECISION events."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.storage.repository import get_system_events_since


@dataclass(frozen=True)
class GateRule:
    stage: str
    token: str


GATE_RULES: tuple[GateRule, ...] = (
    GateRule("missing_15m_data", "ERROR: Missing 15m Data"),
    GateRule("missing_1h_data", "ERROR: Missing 1h Data"),
    GateRule("missing_4h_data", "ERROR: Missing 4H Data"),
    GateRule("decision_structure_required", "No valid 4H decision structure"),
    GateRule("weekly_zone_hard_reject", "Higher-TF hard reject: outside weekly zone"),
    GateRule("waiting_structure_break", "No 4H market structure change detected - waiting for structure break"),
    GateRule("waiting_structure_confirmation", "waiting for confirmation"),
    GateRule("waiting_reconfirmation", "waiting for reconfirmation"),
    GateRule("adx_ranging_filter", "Ranging market: ADX"),
    GateRule("fib_gate_reject", "tight_smc entry not in OTE/Key Fib"),
    GateRule("fib_missing_reject", "No Fib structure found for tight_smc"),
    GateRule("direction_mismatch", "Signal direction mismatch"),
    GateRule("score_gate_reject", "Score "),
    GateRule("conviction_gate_reject", "ENTRY_BLOCKED_LOW_CONVICTION"),
    GateRule("ablation_structure_bypass", "Ablation: bypass 4H structure-required gate"),
    GateRule("ablation_weekly_bypass", "Ablation: bypass higher-TF weekly-zone gate"),
)


def _classify_reason(reason: str) -> str:
    text = reason or ""
    for rule in GATE_RULES:
        if rule.token in text:
            if rule.stage == "score_gate_reject" and " < Threshold " not in text:
                continue
            return rule.stage
    return "other_or_pass"


def build_funnel(*, since_hours: int, symbols: list[str], limit: int) -> dict[str, Any]:
    since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
    events = get_system_events_since(
        since,
        event_types=["COUNTERFACTUAL_DECISION"],
        symbols=symbols or None,
        limit=limit,
    )
    overall: Counter[str] = Counter()
    by_symbol: dict[str, Counter[str]] = defaultdict(Counter)
    examples: dict[str, str] = {}

    for event in events:
        details = event.get("details") or {}
        reason = str(details.get("reason") or "")
        stage = _classify_reason(reason)
        overall[stage] += 1
        by_symbol[str(event.get("symbol") or "UNKNOWN")][stage] += 1
        if stage not in examples and reason:
            examples[stage] = reason[:300]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "since_hours": since_hours,
        "symbols": symbols,
        "event_count": len(events),
        "overall_stage_counts": dict(overall.most_common()),
        "by_symbol_stage_counts": {
            symbol: dict(counter.most_common())
            for symbol, counter in sorted(by_symbol.items())
        },
        "stage_examples": examples,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=int, default=6, help="Lookback window in hours")
    parser.add_argument(
        "--symbols",
        type=str,
        default="",
        help="Comma-separated symbols (empty means all symbols)",
    )
    parser.add_argument("--limit", type=int, default=200000, help="Maximum events to load")
    parser.add_argument("--out-file", type=Path, default=None, help="Optional output JSON file")
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    payload = build_funnel(since_hours=max(1, args.hours), symbols=symbols, limit=max(1000, args.limit))
    rendered = json.dumps(payload, indent=2, sort_keys=True)
    print(rendered)

    if args.out_file is not None:
        args.out_file.parent.mkdir(parents=True, exist_ok=True)
        args.out_file.write_text(rendered + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
