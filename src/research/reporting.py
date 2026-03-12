"""Reporting output for sandbox autoresearch runs."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.research.models import CandidateResult


def write_leaderboard(path: Path, run_id: str, baseline_id: str, results: list[CandidateResult]) -> Path:
    """Write machine-readable leaderboard JSON."""
    payload = {
        "run_id": run_id,
        "baseline_candidate_id": baseline_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "candidates": [
            {
                "candidate_id": r.candidate_id,
                "params": r.params,
                "score": r.score,
                "accepted": r.accepted,
                "promoted": r.promoted,
                "metrics": {
                    "net_return_pct": r.metrics.net_return_pct,
                    "max_drawdown_pct": r.metrics.max_drawdown_pct,
                    "sharpe": r.metrics.sharpe,
                    "sortino": r.metrics.sortino,
                    "win_rate_pct": r.metrics.win_rate_pct,
                    "trade_count": r.metrics.trade_count,
                    "rejection_reasons": r.metrics.rejection_reasons,
                },
                "metadata": r.metadata,
            }
            for r in results
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def write_summary(path: Path, run_id: str, baseline: CandidateResult, best: CandidateResult) -> Path:
    """Write concise markdown summary for operator review."""
    delta_return = best.metrics.net_return_pct - baseline.metrics.net_return_pct
    delta_dd = best.metrics.max_drawdown_pct - baseline.metrics.max_drawdown_pct
    delta_sharpe = best.metrics.sharpe - baseline.metrics.sharpe
    delta_win = best.metrics.win_rate_pct - baseline.metrics.win_rate_pct
    content = (
        f"# Sandbox Autoresearch Summary ({run_id})\n\n"
        f"- Baseline: `{baseline.candidate_id}`\n"
        f"- Best Candidate: `{best.candidate_id}`\n"
        f"- Promotion Eligible: `{best.accepted}`\n\n"
        "## KPI Delta vs Baseline\n\n"
        f"- Net Return: `{delta_return:+.2f}%`\n"
        f"- Max Drawdown: `{delta_dd:+.2f}%`\n"
        f"- Sharpe: `{delta_sharpe:+.2f}`\n"
        f"- Win Rate: `{delta_win:+.2f}%`\n"
        f"- Trade Count: `{best.metrics.trade_count - baseline.metrics.trade_count:+d}`\n\n"
        "## Best Candidate Params\n\n"
        + "\n".join(f"- `{k}`: `{v}`" for k, v in sorted(best.params.items()))
        + "\n"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path

