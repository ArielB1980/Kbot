"""
Unified Position Manager (KBO-29).

Single entry point for all position management operations:
- State tracking (persistence layer, via PositionRegistry)
- Decision engine (action layer, via internal evaluation logic)

Replaces the previous two-object pattern:
    registry = get_position_registry()
    manager = PositionManagerV2(registry=registry, ...)

With a single unified interface:
    manager = PositionManager(...)
    manager.registry  # access state layer
    manager.evaluate_entry(...)  # decision layer
"""

from decimal import Decimal
from typing import Any

from src.domain.models import Side, Signal
from src.execution.instrument_specs import InstrumentSpecRegistry
from src.execution.position_manager_v2 import (
    DecisionTick,
    ManagementAction,
    PositionManagerV2,
)
from src.execution.position_state_machine import (
    ManagedPosition,
    OrderEvent,
    PositionRegistry,
    get_position_registry,
)
from src.monitoring.logger import get_logger

logger = get_logger(__name__)


class PositionManager:
    """Unified position management: state tracking + decision engine.

    Composes:
    - PositionRegistry (persistence/state layer) — single source of truth
    - Decision engine (action/evaluation layer) — produces ManagementActions

    The registry owns position state and enforces invariants.
    The decision engine evaluates market conditions and produces ordered action lists.
    """

    def __init__(
        self,
        registry: PositionRegistry | None = None,
        multi_tp_config: Any = None,
        instrument_spec_registry: InstrumentSpecRegistry | None = None,
        strategy_config: Any = None,
        institutional_memory: Any = None,
    ):
        """Initialize unified position manager.

        Args:
            registry: Position registry (uses singleton if not provided).
            multi_tp_config: Optional MultiTPConfig for runner mode settings.
            instrument_spec_registry: Optional registry for venue min_size.
            strategy_config: Strategy configuration.
            institutional_memory: Institutional memory manager.
        """
        self._registry = registry or get_position_registry()
        self._decision_engine = PositionManagerV2(
            registry=self._registry,
            multi_tp_config=multi_tp_config,
            instrument_spec_registry=instrument_spec_registry,
            strategy_config=strategy_config,
            institutional_memory=institutional_memory,
        )

    # ========== STATE LAYER (registry delegation) ==========

    @property
    def registry(self) -> PositionRegistry:
        """Access the underlying position registry."""
        return self._registry

    def can_open_position(self, symbol: str, side: Side) -> tuple[bool, str]:
        """Check if a new position can be opened for symbol/side."""
        return self._registry.can_open_position(symbol, side)

    def register_position(self, position: ManagedPosition) -> None:
        """Register a new position in the registry."""
        self._registry.register_position(position)

    def get_position(self, symbol: str) -> ManagedPosition | None:
        """Get active position for symbol."""
        return self._registry.get_position(symbol)

    def get_position_any_state(self, symbol: str) -> ManagedPosition | None:
        """Get position for symbol in any state."""
        return self._registry.get_position_any_state(symbol)

    def has_position(self, symbol: str) -> bool:
        """Check if symbol has an active position."""
        return self._registry.has_position(symbol)

    def get_all_active(self) -> list[ManagedPosition]:
        """Get all active (non-terminal) positions."""
        return self._registry.get_all_active()

    def get_all_positions(self) -> list[ManagedPosition]:
        """Get all positions (active and terminal)."""
        return self._registry.get_all()

    def get_closed_history(self, limit: int = 100) -> list[ManagedPosition]:
        """Get closed position history."""
        return self._registry.get_closed_history(limit)

    def apply_order_event_to_registry(self, symbol: str, event: OrderEvent) -> bool:
        """Apply an order event directly to the registry."""
        return self._registry.apply_order_event(symbol, event)

    def request_reversal(self, symbol: str, new_side: Side) -> bool:
        """Request a direction reversal for symbol."""
        return self._registry.request_reversal(symbol, new_side)

    def confirm_reversal_closed(self, symbol: str) -> Side | None:
        """Confirm reversal close completed."""
        return self._registry.confirm_reversal_closed(symbol)

    def hard_reset(self, reason: str) -> list[ManagedPosition]:
        """Hard reset all positions (startup when exchange is flat)."""
        return self._registry.hard_reset(reason)

    def cleanup_stale(self, max_age_hours: int = 24) -> int:
        """Clean up stale closed positions."""
        return self._registry.cleanup_stale(max_age_hours)

    def reconcile_with_exchange(self, exchange_positions: dict, exchange_orders: dict) -> list:
        """Run raw exchange reconciliation."""
        return self._registry.reconcile_with_exchange(exchange_positions, exchange_orders)

    # ========== DECISION LAYER (engine delegation) ==========

    def evaluate_entry(
        self,
        signal: Signal,
        entry_price: Decimal,
        stop_price: Decimal,
        tp1_price: Decimal | None,
        tp2_price: Decimal | None,
        final_target: Decimal | None,
        position_size: Decimal,
        trade_type: str = "signal",
        leverage: Decimal | None = None,
    ) -> tuple[ManagementAction, ManagedPosition | None]:
        """Evaluate a potential entry signal.

        Returns (action, position) where action is OPEN_POSITION or REJECT_ENTRY.
        """
        return self._decision_engine.evaluate_entry(
            signal=signal,
            entry_price=entry_price,
            stop_price=stop_price,
            tp1_price=tp1_price,
            tp2_price=tp2_price,
            final_target=final_target,
            position_size=position_size,
            trade_type=trade_type,
            leverage=leverage,
        )

    def evaluate_position(
        self,
        symbol: str,
        current_price: Decimal,
        current_atr: Decimal | None = None,
        premise_invalidated: bool = False,
        confirmation_condition_met: bool = False,
        confirmation_price: Decimal | None = None,
    ) -> list[ManagementAction]:
        """Per-tick position evaluation. Returns prioritized action list."""
        return self._decision_engine.evaluate_position(
            symbol=symbol,
            current_price=current_price,
            current_atr=current_atr,
            premise_invalidated=premise_invalidated,
            confirmation_condition_met=confirmation_condition_met,
            confirmation_price=confirmation_price,
        )

    def handle_order_event(self, symbol: str, event: OrderEvent) -> list[ManagementAction]:
        """Handle order event feedback. Returns follow-up actions."""
        return self._decision_engine.handle_order_event(symbol, event)

    def request_reversal_actions(
        self, symbol: str, new_side: Side, current_price: Decimal
    ) -> list[ManagementAction]:
        """Request reversal and get close actions."""
        return self._decision_engine.request_reversal(symbol, new_side, current_price)

    def reconcile(
        self,
        exchange_positions: dict,
        exchange_orders: dict,
        issues: list,
    ) -> list[ManagementAction]:
        """Translate reconciliation issues into corrective actions."""
        return self._decision_engine.reconcile(exchange_positions, exchange_orders, issues)

    def check_safety(self) -> list[ManagementAction]:
        """Run periodic safety checks."""
        return self._decision_engine.check_safety()

    # ========== METRICS & DIAGNOSTICS ==========

    @property
    def decision_history(self) -> list[DecisionTick]:
        """Access decision history."""
        return self._decision_engine.decision_history

    @property
    def exit_timeout_manager(self):
        """Access exit timeout manager."""
        return self._decision_engine.exit_timeout_manager

    @property
    def safety_config(self):
        """Access safety config."""
        return self._decision_engine.safety_config

    @property
    def metrics(self) -> dict:
        """Access decision metrics."""
        return self._decision_engine.metrics

    def get_decision_metrics(self) -> dict:
        """Get decision metrics summary."""
        return self._decision_engine.get_decision_metrics()

    def export_decision_history(self, limit: int = 1000) -> list[dict]:
        """Export decision history for analysis."""
        return self._decision_engine.export_decision_history(limit)

    # ========== PERSISTENCE ==========

    def registry_to_dict(self) -> dict:
        """Serialize registry state for persistence."""
        return self._registry.to_dict()

    @classmethod
    def from_registry_dict(
        cls,
        data: dict,
        multi_tp_config: Any = None,
        instrument_spec_registry: InstrumentSpecRegistry | None = None,
        strategy_config: Any = None,
        institutional_memory: Any = None,
    ) -> "PositionManager":
        """Restore from persisted registry state."""
        registry = PositionRegistry.from_dict(data)
        return cls(
            registry=registry,
            multi_tp_config=multi_tp_config,
            instrument_spec_registry=instrument_spec_registry,
            strategy_config=strategy_config,
            institutional_memory=institutional_memory,
        )
