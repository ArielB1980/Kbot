"""KPI and scoring utilities for sandbox autoresearch."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from math import sqrt
from typing import Iterable

from src.backtest.backtest_engine import BacktestMetrics
from src.research.models import CandidateMetrics


def _to_float(value: Decimal | float | int) -> float:
    """Normalize numeric values to float safely."""
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def compute_sortino_from_equity(equity_curve: Iterable[Decimal | float | int]) -> float | None:
    """Compute annualized Sortino ratio from a daily-equity series."""
    points = [_to_float(x) for x in equity_curve]
    if len(points) < 3:
        return None

    returns: list[float] = []
    for idx in range(1, len(points)):
        prev = points[idx - 1]
        if prev <= 0:
            continue
        returns.append((points[idx] - prev) / prev)
    if not returns:
        return None

    mean_return = sum(returns) / len(returns)
    downside = [min(0.0, r) for r in returns]
    downside_var = sum(d * d for d in downside) / len(downside)
    downside_dev = sqrt(downside_var)
    if downside_dev == 0:
        return None
    return (mean_return / downside_dev) * sqrt(365.0)


def metrics_from_backtest(metrics: BacktestMetrics, starting_equity: Decimal) -> CandidateMetrics:
    """Build the normalized KPI payload from backtest metrics."""
    start_eq = _to_float(starting_equity)
    pnl = _to_float(metrics.total_pnl)
    net_return_pct = (pnl / start_eq) * 100 if start_eq > 0 else 0.0
    drawdown_pct = _to_float(metrics.max_drawdown) * 100

    return CandidateMetrics(
        net_return_pct=net_return_pct,
        max_drawdown_pct=drawdown_pct,
        sharpe=float(metrics.sharpe_ratio),
        sortino=compute_sortino_from_equity(metrics.equity_curve),
        win_rate_pct=float(metrics.win_rate),
        trade_count=int(metrics.total_trades),
        rejection_reasons=[],
    )


@dataclass(frozen=True)
class ScoreWeights:
    """Weights for composite short-horizon candidate scoring."""

    return_weight: float = 1.0
    drawdown_weight: float = 0.8
    sharpe_weight: float = 0.35
    win_rate_weight: float = 0.1
    trade_count_weight: float = 0.01


def score_candidate(metrics: CandidateMetrics, weights: ScoreWeights | None = None) -> float:
    """Compute a composite score for ranking candidates."""
    w = weights or ScoreWeights()
    sortino_component = metrics.sortino if metrics.sortino is not None else metrics.sharpe
    return (
        metrics.net_return_pct * w.return_weight
        - metrics.max_drawdown_pct * w.drawdown_weight
        + (0.5 * metrics.sharpe + 0.5 * sortino_component) * w.sharpe_weight
        + metrics.win_rate_pct * w.win_rate_weight
        + metrics.trade_count * w.trade_count_weight
    )

