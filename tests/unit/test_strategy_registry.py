"""
Tests for the strategy registry: CRUD, lifecycle transitions,
validation pipeline, and auto-promotion/demotion.
"""

from pathlib import Path

import pytest

from src.strategy.registry import (
    DuplicateStrategyError,
    InvalidTransitionError,
    StrategyNotFoundError,
    StrategyRegistry,
    ValidationFailedError,
)
from src.strategy.registry_models import (
    PerformanceMetrics,
    StrategyLifecycle,
    StrategyType,
)


@pytest.fixture
def tmp_storage(tmp_path: Path) -> Path:
    """Provide a temporary directory for registry storage."""
    return tmp_path / "strategies"


@pytest.fixture
def registry(tmp_storage: Path) -> StrategyRegistry:
    """Provide a fresh registry backed by a temp directory."""
    return StrategyRegistry(tmp_storage)


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


class TestRegister:
    def test_register_creates_draft(self, registry: StrategyRegistry) -> None:
        rec = registry.register("alpha", StrategyType.MOMENTUM, author="alice")
        assert rec.name == "alpha"
        assert rec.lifecycle == StrategyLifecycle.DRAFT
        assert rec.strategy_type == StrategyType.MOMENTUM
        assert rec.author == "alice"

    def test_register_duplicate_raises(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        with pytest.raises(DuplicateStrategyError):
            registry.register("alpha", StrategyType.MOMENTUM)

    def test_register_with_all_fields(self, registry: StrategyRegistry) -> None:
        rec = registry.register(
            "beta",
            StrategyType.STAT_ARB,
            universe=["BTC/USDT", "ETH/USDT"],
            parameters={"lookback": 20, "threshold": 2.0},
            author="bob",
            description="Pair trading strategy",
            regime_applicability=["trending", "ranging"],
        )
        assert rec.universe == ["BTC/USDT", "ETH/USDT"]
        assert rec.parameters == {"lookback": 20, "threshold": 2.0}
        assert rec.description == "Pair trading strategy"
        assert rec.regime_applicability == ["trending", "ranging"]


class TestGet:
    def test_get_existing(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        rec = registry.get("alpha")
        assert rec.name == "alpha"

    def test_get_nonexistent_raises(self, registry: StrategyRegistry) -> None:
        with pytest.raises(StrategyNotFoundError):
            registry.get("nonexistent")


class TestListStrategies:
    def test_list_all(self, registry: StrategyRegistry) -> None:
        registry.register("a", StrategyType.MOMENTUM)
        registry.register("b", StrategyType.STAT_ARB)
        assert len(registry.list_strategies()) == 2

    def test_filter_by_lifecycle(self, registry: StrategyRegistry) -> None:
        registry.register("a", StrategyType.MOMENTUM)
        registry.register("b", StrategyType.STAT_ARB)
        drafts = registry.list_strategies(lifecycle=StrategyLifecycle.DRAFT)
        assert len(drafts) == 2
        live = registry.list_strategies(lifecycle=StrategyLifecycle.LIVE)
        assert len(live) == 0

    def test_filter_by_type(self, registry: StrategyRegistry) -> None:
        registry.register("a", StrategyType.MOMENTUM)
        registry.register("b", StrategyType.STAT_ARB)
        momentum = registry.list_strategies(strategy_type=StrategyType.MOMENTUM)
        assert len(momentum) == 1
        assert momentum[0].name == "a"


class TestUpdate:
    def test_update_fields(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        rec = registry.update(
            "alpha",
            parameters={"lookback": 50},
            description="Updated description",
        )
        assert rec.parameters == {"lookback": 50}
        assert rec.description == "Updated description"

    def test_update_nonexistent_raises(self, registry: StrategyRegistry) -> None:
        with pytest.raises(StrategyNotFoundError):
            registry.update("ghost", parameters={"x": 1})

    def test_update_persists(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        registry.update("alpha", universe=["SOL/USDT"])
        reloaded = registry.get("alpha")
        assert reloaded.universe == ["SOL/USDT"]


class TestDelete:
    def test_delete_existing(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        assert registry.delete("alpha") is True
        with pytest.raises(StrategyNotFoundError):
            registry.get("alpha")

    def test_delete_nonexistent(self, registry: StrategyRegistry) -> None:
        assert registry.delete("ghost") is False


# ---------------------------------------------------------------------------
# Lifecycle transitions
# ---------------------------------------------------------------------------


def _passing_backtest_metrics() -> PerformanceMetrics:
    """Metrics that pass default backtest criteria."""
    return PerformanceMetrics(
        sharpe_ratio=0.8,
        max_drawdown_pct=15.0,
        win_rate=0.45,
        profit_factor=1.3,
        total_trades=100,
        total_pnl=5000.0,
    )


def _passing_paper_metrics() -> PerformanceMetrics:
    """Metrics that pass default paper-trading criteria."""
    return PerformanceMetrics(
        sharpe_ratio=1.0,
        max_drawdown_pct=10.0,
        win_rate=0.50,
        profit_factor=1.4,
        total_trades=50,
        total_pnl=3000.0,
    )


def _passing_live_metrics() -> PerformanceMetrics:
    """Metrics that pass default live criteria."""
    return PerformanceMetrics(
        sharpe_ratio=1.2,
        max_drawdown_pct=8.0,
        win_rate=0.50,
        profit_factor=1.5,
        total_trades=30,
        total_pnl=2000.0,
    )


class TestTransition:
    def test_draft_to_backtested(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        rec = registry.transition(
            "alpha",
            StrategyLifecycle.BACKTESTED,
            metrics=_passing_backtest_metrics(),
            reason="Initial backtest passed",
        )
        assert rec.lifecycle == StrategyLifecycle.BACKTESTED
        assert len(rec.transitions) == 1
        assert rec.transitions[0].from_state == StrategyLifecycle.DRAFT
        assert rec.transitions[0].to_state == StrategyLifecycle.BACKTESTED

    def test_full_lifecycle_forward(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        registry.transition(
            "alpha", StrategyLifecycle.BACKTESTED, metrics=_passing_backtest_metrics()
        )
        registry.transition(
            "alpha", StrategyLifecycle.PAPER_TRADING, metrics=_passing_paper_metrics()
        )
        registry.transition("alpha", StrategyLifecycle.LIVE, metrics=_passing_live_metrics())
        rec = registry.transition("alpha", StrategyLifecycle.RETIRED, reason="EOL")
        assert rec.lifecycle == StrategyLifecycle.RETIRED
        assert len(rec.transitions) == 4

    def test_invalid_transition_raises(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        with pytest.raises(InvalidTransitionError):
            registry.transition("alpha", StrategyLifecycle.LIVE)

    def test_demotion_backtested_to_draft(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        registry.transition(
            "alpha", StrategyLifecycle.BACKTESTED, metrics=_passing_backtest_metrics()
        )
        rec = registry.transition("alpha", StrategyLifecycle.DRAFT, reason="Needs rework")
        assert rec.lifecycle == StrategyLifecycle.DRAFT

    def test_transition_nonexistent_raises(self, registry: StrategyRegistry) -> None:
        with pytest.raises(StrategyNotFoundError):
            registry.transition("ghost", StrategyLifecycle.BACKTESTED)

    def test_metrics_required_for_promotion(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        with pytest.raises(ValidationFailedError, match="Metrics required"):
            registry.transition("alpha", StrategyLifecycle.BACKTESTED)

    def test_failing_metrics_rejected(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        bad_metrics = PerformanceMetrics(
            sharpe_ratio=0.1,
            max_drawdown_pct=50.0,
            win_rate=0.2,
            profit_factor=0.5,
            total_trades=5,
        )
        with pytest.raises(ValidationFailedError, match="sharpe_ratio"):
            registry.transition("alpha", StrategyLifecycle.BACKTESTED, metrics=bad_metrics)

    def test_skip_validation(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        rec = registry.transition(
            "alpha",
            StrategyLifecycle.BACKTESTED,
            skip_validation=True,
            reason="Admin override",
        )
        assert rec.lifecycle == StrategyLifecycle.BACKTESTED

    def test_transition_stores_metrics(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        metrics = _passing_backtest_metrics()
        registry.transition("alpha", StrategyLifecycle.BACKTESTED, metrics=metrics)
        rec = registry.get("alpha")
        assert "backtested" in rec.performance
        assert rec.performance["backtested"].sharpe_ratio == metrics.sharpe_ratio

    def test_audit_trail_records_approver(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        registry.transition(
            "alpha",
            StrategyLifecycle.BACKTESTED,
            metrics=_passing_backtest_metrics(),
            approved_by="ceo",
        )
        rec = registry.get("alpha")
        assert rec.transitions[0].approved_by == "ceo"


# ---------------------------------------------------------------------------
# Record metrics (without state change)
# ---------------------------------------------------------------------------


class TestRecordMetrics:
    def test_record_metrics(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        m = PerformanceMetrics(sharpe_ratio=1.5, win_rate=0.6, total_trades=200)
        registry.record_metrics("alpha", "backtest_v2", m)
        rec = registry.get("alpha")
        assert "backtest_v2" in rec.performance
        assert rec.performance["backtest_v2"].total_trades == 200


# ---------------------------------------------------------------------------
# Auto-promotion / demotion
# ---------------------------------------------------------------------------


class TestAutoPromotion:
    def test_check_promotions_finds_eligible(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        registry.transition(
            "alpha", StrategyLifecycle.BACKTESTED, metrics=_passing_backtest_metrics()
        )
        # Record metrics that pass paper-trading criteria under current stage
        registry.record_metrics("alpha", "backtested", _passing_paper_metrics())
        suggestions = registry.check_promotions()
        assert ("alpha", StrategyLifecycle.PAPER_TRADING) in suggestions

    def test_check_promotions_skips_ineligible(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        registry.transition(
            "alpha", StrategyLifecycle.BACKTESTED, metrics=_passing_backtest_metrics()
        )
        # Record metrics that fail paper-trading criteria
        weak = PerformanceMetrics(
            sharpe_ratio=0.3,
            max_drawdown_pct=30.0,
            win_rate=0.2,
            profit_factor=0.8,
            total_trades=10,
        )
        registry.record_metrics("alpha", "backtested", weak)
        suggestions = registry.check_promotions()
        assert len(suggestions) == 0


class TestAutoDemotion:
    def test_check_demotions_flags_bad_live(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        registry.transition(
            "alpha", StrategyLifecycle.BACKTESTED, metrics=_passing_backtest_metrics()
        )
        registry.transition(
            "alpha", StrategyLifecycle.PAPER_TRADING, metrics=_passing_paper_metrics()
        )
        registry.transition("alpha", StrategyLifecycle.LIVE, metrics=_passing_live_metrics())
        # Record degraded live metrics
        bad_live = PerformanceMetrics(
            sharpe_ratio=0.2,
            max_drawdown_pct=20.0,
            win_rate=0.3,
            profit_factor=0.7,
            total_trades=50,
        )
        registry.record_metrics("alpha", "live", bad_live)
        suggestions = registry.check_demotions()
        assert ("alpha", StrategyLifecycle.PAPER_TRADING) in suggestions

    def test_check_demotions_skips_healthy_live(self, registry: StrategyRegistry) -> None:
        registry.register("alpha", StrategyType.MOMENTUM)
        registry.transition(
            "alpha", StrategyLifecycle.BACKTESTED, metrics=_passing_backtest_metrics()
        )
        registry.transition(
            "alpha", StrategyLifecycle.PAPER_TRADING, metrics=_passing_paper_metrics()
        )
        registry.transition("alpha", StrategyLifecycle.LIVE, metrics=_passing_live_metrics())
        suggestions = registry.check_demotions()
        assert len(suggestions) == 0


# ---------------------------------------------------------------------------
# Persistence round-trip
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_round_trip_preserves_data(self, tmp_storage: Path) -> None:
        reg1 = StrategyRegistry(tmp_storage)
        reg1.register(
            "alpha",
            StrategyType.STAT_ARB,
            universe=["BTC/USDT"],
            parameters={"window": 30},
            author="alice",
        )
        reg1.transition("alpha", StrategyLifecycle.BACKTESTED, metrics=_passing_backtest_metrics())

        # Reload from disk
        reg2 = StrategyRegistry(tmp_storage)
        rec = reg2.get("alpha")
        assert rec.lifecycle == StrategyLifecycle.BACKTESTED
        assert rec.strategy_type == StrategyType.STAT_ARB
        assert rec.universe == ["BTC/USDT"]
        assert rec.author == "alice"
        assert len(rec.transitions) == 1
        assert "backtested" in rec.performance

    def test_delete_removes_file(self, tmp_storage: Path) -> None:
        reg = StrategyRegistry(tmp_storage)
        reg.register("alpha", StrategyType.MOMENTUM)
        reg.delete("alpha")
        assert list(tmp_storage.glob("*.yaml")) == []
