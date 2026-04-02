"""
Backward-compatible imports for legacy position-manager modules.

Older runtime artifacts may still import these names from
`src.execution.position_manager_models`. Keep this shim to avoid
ModuleNotFoundError during mixed-version rollouts.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum

from src.execution.position_state_machine import (
    ExitReason,
    ManagedPosition,
    OrderEvent,
    OrderEventType,
    PositionRegistry,
    PositionState,
    check_invariant,
    get_position_registry,
)


class ActionType(Enum):
    """Position management action types."""

    OPEN_POSITION = "OPEN_POSITION"
    CLOSE_FULL = "CLOSE_FULL"
    CLOSE_PARTIAL = "CLOSE_PARTIAL"
    PLACE_STOP = "PLACE_STOP"
    UPDATE_STOP = "UPDATE_STOP"
    CANCEL_STOP = "CANCEL_STOP"
    PLACE_TP = "PLACE_TP"
    CANCEL_TP = "CANCEL_TP"
    FLATTEN_ORPHAN = "FLATTEN_ORPHAN"
    REJECT_ENTRY = "REJECT_ENTRY"
    NO_ACTION = "NO_ACTION"


@dataclass
class ManagementAction:
    """A single action produced by the position manager decision engine."""

    type: ActionType
    symbol: str
    reason: str = ""
    price: Decimal | None = None
    qty: Decimal | None = None
    order_id: str | None = None
    metadata: dict = field(default_factory=dict)


@dataclass
class DecisionTick:
    """Snapshot of a position-manager evaluation cycle for debugging."""

    timestamp: datetime
    symbol: str
    current_price: Decimal | None = None
    position_state: str | None = None
    position_id: str | None = None
    remaining_qty: Decimal | None = None
    current_stop: Decimal | None = None
    actions: list[ManagementAction] = field(default_factory=list)
    reason_codes: list[str] = field(default_factory=list)


__all__ = [
    "ActionType",
    "DecisionTick",
    "ExitReason",
    "ManagementAction",
    "ManagedPosition",
    "OrderEvent",
    "OrderEventType",
    "PositionRegistry",
    "PositionState",
    "check_invariant",
    "get_position_registry",
]
