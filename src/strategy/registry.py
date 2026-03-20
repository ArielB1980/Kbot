"""
Strategy registry: CRUD, lifecycle transitions, validation, and auto-promotion/demotion.

Central API for managing strategies through the full validation pipeline:
draft -> backtested -> paper_trading -> live -> retired.
"""

from datetime import UTC, datetime
from pathlib import Path

from src.strategy.registry_models import (
    DEFAULT_STAGE_CRITERIA,
    VALID_TRANSITIONS,
    PerformanceMetrics,
    StrategyLifecycle,
    StrategyRecord,
    StrategyType,
    TransitionRecord,
    ValidationCriteria,
)
from src.strategy.registry_storage import RegistryStorage


class RegistryError(Exception):
    """Base error for registry operations."""


class StrategyNotFoundError(RegistryError):
    """Strategy does not exist in the registry."""


class InvalidTransitionError(RegistryError):
    """Requested lifecycle transition is not allowed."""


class ValidationFailedError(RegistryError):
    """Strategy does not pass stage-gate criteria."""


class DuplicateStrategyError(RegistryError):
    """A strategy with this name already exists."""


class StrategyRegistry:
    """Manages strategy lifecycle, persistence, and validation."""

    def __init__(self, storage_dir: str | Path) -> None:
        self._storage = RegistryStorage(storage_dir)

    # -- CRUD -----------------------------------------------------------------

    def register(
        self,
        name: str,
        strategy_type: StrategyType,
        *,
        universe: list[str] | None = None,
        parameters: dict[str, object] | None = None,
        author: str = "",
        description: str = "",
        regime_applicability: list[str] | None = None,
    ) -> StrategyRecord:
        """Create a new strategy in DRAFT state.

        Raises:
            DuplicateStrategyError: If name already exists.
        """
        if self._storage.load(name) is not None:
            raise DuplicateStrategyError(f"Strategy '{name}' already exists")

        record = StrategyRecord(
            name=name,
            strategy_type=strategy_type,
            universe=universe or [],
            parameters=parameters or {},
            author=author,
            description=description,
            regime_applicability=regime_applicability or [],
        )
        self._storage.save(record)
        return record

    def get(self, name: str) -> StrategyRecord:
        """Retrieve a strategy by name.

        Raises:
            StrategyNotFoundError: If not found.
        """
        rec = self._storage.load(name)
        if rec is None:
            raise StrategyNotFoundError(f"Strategy '{name}' not found")
        return rec

    def list_strategies(
        self,
        *,
        lifecycle: StrategyLifecycle | None = None,
        strategy_type: StrategyType | None = None,
    ) -> list[StrategyRecord]:
        """List strategies, optionally filtered."""
        results = self._storage.list_all()
        if lifecycle is not None:
            results = [r for r in results if r.lifecycle == lifecycle]
        if strategy_type is not None:
            results = [r for r in results if r.strategy_type == strategy_type]
        return results

    def update(
        self,
        name: str,
        *,
        parameters: dict[str, object] | None = None,
        universe: list[str] | None = None,
        description: str | None = None,
        regime_applicability: list[str] | None = None,
    ) -> StrategyRecord:
        """Update mutable fields on a strategy.

        Raises:
            StrategyNotFoundError: If not found.
        """
        rec = self.get(name)
        if parameters is not None:
            rec.parameters = parameters
        if universe is not None:
            rec.universe = universe
        if description is not None:
            rec.description = description
        if regime_applicability is not None:
            rec.regime_applicability = regime_applicability
        rec.updated_at = datetime.now(UTC)
        self._storage.save(rec)
        return rec

    def delete(self, name: str) -> bool:
        """Remove a strategy from the registry."""
        return self._storage.delete(name)

    # -- Lifecycle transitions -----------------------------------------------

    def transition(
        self,
        name: str,
        target: StrategyLifecycle,
        *,
        reason: str = "",
        approved_by: str = "",
        metrics: PerformanceMetrics | None = None,
        skip_validation: bool = False,
    ) -> StrategyRecord:
        """Move a strategy to a new lifecycle state.

        Args:
            name: Strategy name.
            target: Desired new lifecycle state.
            reason: Human-readable reason for the transition.
            approved_by: Who approved this transition.
            metrics: Performance metrics to validate against stage gate and record.
            skip_validation: If True, bypass stage-gate checks (admin override).

        Raises:
            StrategyNotFoundError: If not found.
            InvalidTransitionError: If transition is not allowed.
            ValidationFailedError: If metrics fail stage-gate criteria.
        """
        rec = self.get(name)
        current = rec.lifecycle

        # Check transition validity.
        allowed = VALID_TRANSITIONS.get(current, [])
        if target not in allowed:
            raise InvalidTransitionError(
                f"Cannot transition '{name}' from {current.value} to {target.value}. "
                f"Allowed: {[s.value for s in allowed]}"
            )

        # Stage-gate validation for forward promotions.
        if not skip_validation and self._is_promotion(current, target):
            criteria = self._criteria_for(rec, target)
            if criteria is not None:
                if metrics is None:
                    raise ValidationFailedError(f"Metrics required for promotion to {target.value}")
                self._validate_metrics(metrics, criteria, target)

        # Record transition.
        tr = TransitionRecord(
            from_state=current,
            to_state=target,
            reason=reason,
            metrics_snapshot=metrics,
            approved_by=approved_by,
        )
        rec.transitions.append(tr)
        rec.lifecycle = target
        rec.updated_at = datetime.now(UTC)

        # Store metrics snapshot under the target stage name.
        if metrics is not None:
            rec.performance[target.value] = metrics

        self._storage.save(rec)
        return rec

    def record_metrics(
        self,
        name: str,
        stage: str,
        metrics: PerformanceMetrics,
    ) -> StrategyRecord:
        """Record a performance snapshot without changing lifecycle state."""
        rec = self.get(name)
        rec.performance[stage] = metrics
        rec.updated_at = datetime.now(UTC)
        self._storage.save(rec)
        return rec

    # -- Auto promotion / demotion -------------------------------------------

    def check_promotions(self) -> list[tuple[str, StrategyLifecycle]]:
        """Identify strategies eligible for promotion based on current metrics.

        Returns list of (strategy_name, suggested_target) tuples.
        Does NOT perform the transitions.
        """
        suggestions: list[tuple[str, StrategyLifecycle]] = []
        for rec in self._storage.list_all():
            if rec.lifecycle == StrategyLifecycle.RETIRED:
                continue
            allowed = VALID_TRANSITIONS.get(rec.lifecycle, [])
            for target in allowed:
                if not self._is_promotion(rec.lifecycle, target):
                    continue
                criteria = self._criteria_for(rec, target)
                if criteria is None:
                    continue
                latest = rec.performance.get(rec.lifecycle.value)
                if latest is None:
                    continue
                if self._meets_criteria(latest, criteria):
                    suggestions.append((rec.name, target))
                    break  # only suggest one promotion step at a time
        return suggestions

    def check_demotions(
        self,
        demotion_drawdown_pct: float = 15.0,
        demotion_sharpe_floor: float = 0.3,
    ) -> list[tuple[str, StrategyLifecycle]]:
        """Identify live strategies that should be demoted.

        Returns list of (strategy_name, suggested_target) tuples.
        Does NOT perform the transitions.
        """
        suggestions: list[tuple[str, StrategyLifecycle]] = []
        for rec in self._storage.list_all():
            if rec.lifecycle != StrategyLifecycle.LIVE:
                continue
            latest = rec.performance.get("live")
            if latest is None:
                continue
            if (
                latest.max_drawdown_pct > demotion_drawdown_pct
                or latest.sharpe_ratio < demotion_sharpe_floor
            ):
                suggestions.append((rec.name, StrategyLifecycle.PAPER_TRADING))
        return suggestions

    # -- Private helpers -----------------------------------------------------

    @staticmethod
    def _is_promotion(current: StrategyLifecycle, target: StrategyLifecycle) -> bool:
        """A promotion moves forward in the lifecycle ordering."""
        order = list(StrategyLifecycle)
        return order.index(target) > order.index(current)

    @staticmethod
    def _criteria_for(rec: StrategyRecord, target: StrategyLifecycle) -> ValidationCriteria | None:
        """Resolve criteria: custom override > defaults."""
        custom = rec.custom_criteria.get(target.value)
        if custom is not None:
            return custom
        return DEFAULT_STAGE_CRITERIA.get(target)

    @staticmethod
    def _meets_criteria(metrics: PerformanceMetrics, criteria: ValidationCriteria) -> bool:
        return (
            metrics.sharpe_ratio >= criteria.min_sharpe
            and metrics.max_drawdown_pct <= criteria.max_drawdown_pct
            and metrics.win_rate >= criteria.min_win_rate
            and metrics.profit_factor >= criteria.min_profit_factor
            and metrics.total_trades >= criteria.min_trades
        )

    @staticmethod
    def _validate_metrics(
        metrics: PerformanceMetrics,
        criteria: ValidationCriteria,
        target: StrategyLifecycle,
    ) -> None:
        """Raise ValidationFailedError with details if metrics fail."""
        failures: list[str] = []
        if metrics.sharpe_ratio < criteria.min_sharpe:
            failures.append(f"sharpe_ratio {metrics.sharpe_ratio:.2f} < {criteria.min_sharpe}")
        if metrics.max_drawdown_pct > criteria.max_drawdown_pct:
            failures.append(
                f"max_drawdown_pct {metrics.max_drawdown_pct:.1f}% > {criteria.max_drawdown_pct}%"
            )
        if metrics.win_rate < criteria.min_win_rate:
            failures.append(f"win_rate {metrics.win_rate:.2f} < {criteria.min_win_rate}")
        if metrics.profit_factor < criteria.min_profit_factor:
            failures.append(
                f"profit_factor {metrics.profit_factor:.2f} < {criteria.min_profit_factor}"
            )
        if metrics.total_trades < criteria.min_trades:
            failures.append(f"total_trades {metrics.total_trades} < {criteria.min_trades}")
        if failures:
            raise ValidationFailedError(f"Cannot promote to {target.value}: " + "; ".join(failures))
