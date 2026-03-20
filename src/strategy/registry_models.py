"""
Strategy registry data models.

Defines the core types for strategy lifecycle management:
lifecycle states, strategy metadata, performance metrics,
validation criteria, and audit trail entries.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum


class StrategyLifecycle(StrEnum):
    """Strategy lifecycle states (ordered progression)."""

    DRAFT = "draft"
    BACKTESTED = "backtested"
    PAPER_TRADING = "paper_trading"
    LIVE = "live"
    RETIRED = "retired"


# Valid forward transitions between lifecycle states.
VALID_TRANSITIONS: dict[StrategyLifecycle, list[StrategyLifecycle]] = {
    StrategyLifecycle.DRAFT: [StrategyLifecycle.BACKTESTED, StrategyLifecycle.RETIRED],
    StrategyLifecycle.BACKTESTED: [
        StrategyLifecycle.PAPER_TRADING,
        StrategyLifecycle.DRAFT,
        StrategyLifecycle.RETIRED,
    ],
    StrategyLifecycle.PAPER_TRADING: [
        StrategyLifecycle.LIVE,
        StrategyLifecycle.BACKTESTED,
        StrategyLifecycle.RETIRED,
    ],
    StrategyLifecycle.LIVE: [
        StrategyLifecycle.PAPER_TRADING,
        StrategyLifecycle.RETIRED,
    ],
    StrategyLifecycle.RETIRED: [],
}


class StrategyType(StrEnum):
    """Broad strategy categories."""

    MOMENTUM = "momentum"
    MEAN_REVERSION = "mean_reversion"
    STAT_ARB = "stat_arb"
    SMC = "smc"
    TREND_FOLLOWING = "trend_following"
    OTHER = "other"


@dataclass
class PerformanceMetrics:
    """Snapshot of strategy performance for a given evaluation period."""

    sharpe_ratio: float = 0.0
    max_drawdown_pct: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    total_trades: int = 0
    total_pnl: float = 0.0
    regime_correlation: float = 0.0
    recovery_time_hours: float = 0.0
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class ValidationCriteria:
    """Pass/fail thresholds for a lifecycle stage gate."""

    min_sharpe: float = 0.5
    max_drawdown_pct: float = 20.0
    min_win_rate: float = 0.40
    min_profit_factor: float = 1.1
    min_trades: int = 30
    min_evaluation_days: int = 30


# Default criteria per stage gate.
DEFAULT_STAGE_CRITERIA: dict[StrategyLifecycle, ValidationCriteria] = {
    StrategyLifecycle.BACKTESTED: ValidationCriteria(
        min_sharpe=0.5,
        max_drawdown_pct=25.0,
        min_win_rate=0.35,
        min_profit_factor=1.0,
        min_trades=50,
        min_evaluation_days=90,
    ),
    StrategyLifecycle.PAPER_TRADING: ValidationCriteria(
        min_sharpe=0.8,
        max_drawdown_pct=15.0,
        min_win_rate=0.40,
        min_profit_factor=1.2,
        min_trades=30,
        min_evaluation_days=30,
    ),
    StrategyLifecycle.LIVE: ValidationCriteria(
        min_sharpe=1.0,
        max_drawdown_pct=10.0,
        min_win_rate=0.45,
        min_profit_factor=1.3,
        min_trades=20,
        min_evaluation_days=14,
    ),
}


@dataclass
class TransitionRecord:
    """Immutable audit entry for a lifecycle state change."""

    from_state: StrategyLifecycle
    to_state: StrategyLifecycle
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    reason: str = ""
    metrics_snapshot: PerformanceMetrics | None = None
    approved_by: str = ""


@dataclass
class StrategyRecord:
    """Complete strategy entry in the registry."""

    name: str
    strategy_type: StrategyType
    universe: list[str] = field(default_factory=list)
    parameters: dict[str, object] = field(default_factory=dict)
    lifecycle: StrategyLifecycle = StrategyLifecycle.DRAFT
    author: str = ""
    description: str = ""
    regime_applicability: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    # Performance snapshots keyed by lifecycle stage name.
    performance: dict[str, PerformanceMetrics] = field(default_factory=dict)

    # Ordered audit trail.
    transitions: list[TransitionRecord] = field(default_factory=list)

    # Custom stage-gate overrides (optional).
    custom_criteria: dict[str, ValidationCriteria] = field(default_factory=dict)
