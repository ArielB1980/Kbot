"""Domain models for sandbox autoresearch runs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


RunPhase = Literal["idle", "running", "paused", "finished", "failed", "stopped"]


@dataclass
class CandidateMetrics:
    """Unified KPI contract for candidate evaluation."""

    net_return_pct: float
    max_drawdown_pct: float
    sharpe: float
    sortino: float | None
    win_rate_pct: float
    trade_count: int
    rejection_reasons: list[str] = field(default_factory=list)


@dataclass
class CandidateResult:
    """Result payload for one evaluated candidate."""

    candidate_id: str
    params: dict[str, float]
    metrics: CandidateMetrics
    score: float
    accepted: bool
    promoted: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DecisionPrompt:
    """Represents a pending operator decision in Telegram."""

    token: str
    prompt_type: str
    message: str
    created_at: str
    expires_at: str
    resolved: bool = False
    resolution: Literal["approve", "reject", "timeout"] | None = None

