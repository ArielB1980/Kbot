"""Counterfactual Live Twin: deterministic decision-tape uplift estimator."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from src.config.config import Config
from src.storage.repository import get_system_events_since


@dataclass(frozen=True)
class TapeDecision:
    """Normalized decision record from system events."""

    decision_id: str
    symbol: str
    timestamp: datetime
    signal: str
    regime: str
    bias: str
    setup_quality: float
    is_tradable: bool
    order_placed: bool


def load_decision_tape(
    *,
    since_hours: int,
    symbols: tuple[str, ...] | None = None,
    limit: int = 200_000,
) -> list[TapeDecision]:
    """Load decisions/actions and stitch into a deterministic tape."""
    since = datetime.now(timezone.utc) - timedelta(hours=max(1, int(since_hours)))
    events = get_system_events_since(
        since,
        event_types=["COUNTERFACTUAL_DECISION", "COUNTERFACTUAL_ACTION", "DECISION_TRACE"],
        symbols=list(symbols) if symbols else None,
        limit=limit,
    )

    decisions: dict[str, dict[str, Any]] = {}
    for row in events:
        et = str(row.get("event_type") or "")
        details = dict(row.get("details") or {})
        decision_id = str(row.get("decision_id") or "")
        if not decision_id:
            # Fallback for legacy DECISION_TRACE events that predate explicit IDs.
            symbol = str(row.get("symbol") or "unknown")
            ts = row.get("timestamp")
            rid = row.get("id")
            decision_id = f"legacy:{symbol}:{rid}:{ts.isoformat() if ts else 'na'}"
        base = decisions.setdefault(
            decision_id,
            {
                "decision_id": decision_id,
                "symbol": str(row.get("symbol") or ""),
                "timestamp": row.get("timestamp"),
                "signal": str(details.get("signal") or "NO_SIGNAL"),
                "regime": str(details.get("regime") or ""),
                "bias": str(details.get("bias") or ""),
                "setup_quality": float(details.get("setup_quality") or 0.0),
                "is_tradable": bool(details.get("is_tradable", True)),
                "order_placed": bool(details.get("order_placed", False)),
            },
        )
        if et == "COUNTERFACTUAL_ACTION":
            base["order_placed"] = bool(details.get("order_placed", False))
        elif et in {"COUNTERFACTUAL_DECISION", "DECISION_TRACE"}:
            base["signal"] = str(details.get("signal") or base["signal"])
            base["regime"] = str(details.get("regime") or base["regime"])
            base["bias"] = str(details.get("bias") or base["bias"])
            base["setup_quality"] = float(details.get("setup_quality") or base["setup_quality"])
            base["is_tradable"] = bool(details.get("is_tradable", base["is_tradable"]))
            if "order_placed" in details:
                base["order_placed"] = bool(details.get("order_placed"))

    out: list[TapeDecision] = []
    for item in decisions.values():
        ts = item.get("timestamp")
        if not ts or not item.get("symbol"):
            continue
        out.append(
            TapeDecision(
                decision_id=str(item["decision_id"]),
                symbol=str(item["symbol"]),
                timestamp=ts,
                signal=str(item["signal"]),
                regime=str(item["regime"]),
                bias=str(item["bias"]),
                setup_quality=float(item["setup_quality"]),
                is_tradable=bool(item["is_tradable"]),
                order_placed=bool(item["order_placed"]),
            )
        )
    out.sort(key=lambda x: x.timestamp)
    return out


def evaluate_counterfactual_uplift(
    *,
    base_config: Config,
    candidate_params: dict[str, float],
    tape: list[TapeDecision],
) -> dict[str, Any]:
    """Estimate baseline vs candidate decision utility on identical opportunities."""

    def _thresholds(params: dict[str, float] | None = None) -> dict[str, float]:
        p = params or {}
        return {
            "tight_smc_aligned": float(p.get("strategy.min_score_tight_smc_aligned", base_config.strategy.min_score_tight_smc_aligned)),
            "tight_smc_neutral": float(p.get("strategy.min_score_tight_smc_neutral", base_config.strategy.min_score_tight_smc_neutral)),
            "wide_structure_aligned": float(p.get("strategy.min_score_wide_structure_aligned", base_config.strategy.min_score_wide_structure_aligned)),
            "wide_structure_neutral": float(p.get("strategy.min_score_wide_structure_neutral", base_config.strategy.min_score_wide_structure_neutral)),
        }

    def _threshold_for(d: TapeDecision, thresholds: dict[str, float]) -> float:
        regime = (d.regime or "").lower()
        aligned = (d.bias or "").lower() in {"bullish", "bearish"}
        if regime == "wide_structure":
            return thresholds["wide_structure_aligned" if aligned else "wide_structure_neutral"]
        return thresholds["tight_smc_aligned" if aligned else "tight_smc_neutral"]

    base_t = _thresholds()
    cand_t = _thresholds(candidate_params)

    baseline_open = 0
    candidate_open = 0
    overlap_opportunities = 0
    baseline_utility = 0.0
    candidate_utility = 0.0

    for d in tape:
        if d.signal == "NO_SIGNAL" or not d.is_tradable:
            continue
        overlap_opportunities += 1
        base_thr = _threshold_for(d, base_t)
        cand_thr = _threshold_for(d, cand_t)
        base_should_open = d.setup_quality >= base_thr
        cand_should_open = d.setup_quality >= cand_thr
        if base_should_open:
            baseline_open += 1
            baseline_utility += d.setup_quality - base_thr
        if cand_should_open:
            candidate_open += 1
            candidate_utility += d.setup_quality - cand_thr

    return {
        "samples": len(tape),
        "eligible_opportunities": overlap_opportunities,
        "baseline_open_count": baseline_open,
        "candidate_open_count": candidate_open,
        "delta_open_count": candidate_open - baseline_open,
        "baseline_utility_score": baseline_utility,
        "candidate_utility_score": candidate_utility,
        "utility_uplift": candidate_utility - baseline_utility,
        "thresholds": {"baseline": base_t, "candidate": cand_t},
    }


def evaluate_candidate_batch(
    *,
    base_config: Config,
    tape: list[TapeDecision],
    candidates: dict[str, dict[str, float]],
) -> list[dict[str, Any]]:
    """Evaluate many candidate parameter sets against the same decision tape."""
    rows: list[dict[str, Any]] = []
    for candidate_id, params in candidates.items():
        report = evaluate_counterfactual_uplift(
            base_config=base_config,
            candidate_params=params,
            tape=tape,
        )
        rows.append(
            {
                "candidate_id": candidate_id,
                "params": params,
                "utility_uplift": float(report["utility_uplift"]),
                "delta_open_count": int(report["delta_open_count"]),
                "candidate_open_count": int(report["candidate_open_count"]),
                "baseline_open_count": int(report["baseline_open_count"]),
                "eligible_opportunities": int(report["eligible_opportunities"]),
                "samples": int(report["samples"]),
                "report": report,
            }
        )
    rows.sort(
        key=lambda r: (
            float(r["utility_uplift"]),
            int(r["delta_open_count"]),
            int(r["candidate_open_count"]),
        ),
        reverse=True,
    )
    return rows
