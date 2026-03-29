"""
Backward-compatible imports for legacy position-manager modules.

Older runtime artifacts may still import these names from
`src.execution.position_manager_models`. Keep this shim to avoid
ModuleNotFoundError during mixed-version rollouts.
"""

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

__all__ = [
    "ExitReason",
    "ManagedPosition",
    "OrderEvent",
    "OrderEventType",
    "PositionRegistry",
    "PositionState",
    "check_invariant",
    "get_position_registry",
]
