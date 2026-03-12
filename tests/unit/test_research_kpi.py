from decimal import Decimal

from src.backtest.backtest_engine import BacktestMetrics
from src.research.kpi import compute_sortino_from_equity, metrics_from_backtest, score_candidate
from src.research.models import CandidateMetrics


def test_compute_sortino_from_equity_returns_value_for_downside_series() -> None:
    equity = [Decimal("100"), Decimal("102"), Decimal("101"), Decimal("105"), Decimal("103")]
    sortino = compute_sortino_from_equity(equity)
    assert sortino is not None
    assert isinstance(sortino, float)


def test_metrics_from_backtest_includes_sortino_and_return() -> None:
    metrics = BacktestMetrics(
        total_pnl=Decimal("250"),
        max_drawdown=Decimal("0.05"),
        win_rate=55.0,
        total_trades=12,
        sharpe_ratio=1.2,
        equity_curve=[Decimal("10000"), Decimal("10100"), Decimal("10050"), Decimal("10250")],
    )
    normalized = metrics_from_backtest(metrics, Decimal("10000"))
    assert normalized.net_return_pct == 2.5
    assert normalized.max_drawdown_pct == 5.0
    assert normalized.trade_count == 12
    assert normalized.sortino is not None


def test_score_candidate_rewards_higher_return() -> None:
    weak = CandidateMetrics(
        net_return_pct=1.0,
        max_drawdown_pct=5.0,
        sharpe=0.5,
        sortino=0.7,
        win_rate_pct=50.0,
        trade_count=10,
    )
    strong = CandidateMetrics(
        net_return_pct=3.0,
        max_drawdown_pct=5.0,
        sharpe=0.5,
        sortino=0.7,
        win_rate_pct=50.0,
        trade_count=10,
    )
    assert score_candidate(strong) > score_candidate(weak)

