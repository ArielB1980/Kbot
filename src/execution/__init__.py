"""
Execution module.

Contains order execution, position management, and state machine components.

ARCHITECTURE:
    ExecutionGateway (single entry point for all orders)
        │
        ├── PositionManagerV2 (decision engine)
        │       │
        │       └── PositionRegistry (single source of truth)
        │               │
        │               └── ManagedPosition (state machine)
        │
        ├── PositionPersistence (SQLite crash recovery)
        │
        └── ProductionSafety
                ├── AtomicStopReplacer (new-first stop replace)
                ├── ExitTimeoutManager (timeout + escalation)
                ├── ProtectionEnforcer (Invariant K)
                └── WriteAheadIntentLog (WAL for crash recovery)
"""

# Core State Machine
# Execution Gateway
from src.execution.execution_gateway import (
    ExecutionGateway,
    ExecutionResult,
    OrderPurpose,
    PendingOrder,
)

# Unified Position Manager (KBO-29)
from src.execution.position_manager import PositionManager

# Position Manager V2 (deprecated — use PositionManager)
from src.execution.position_manager_v2 import (
    ActionType,
    DecisionTick,
    ManagementAction,
    PositionManagerV2,
)

# Persistence
from src.execution.position_persistence import (
    PositionPersistence,
    persist_registry_state,
    recover_from_persistence,
)
from src.execution.position_state_machine import (
    ExitReason,
    FillRecord,
    InvariantViolation,
    ManagedPosition,
    OrderEvent,
    OrderEventType,
    PositionRegistry,
    PositionState,
    check_invariant,
    get_position_registry,
    reset_position_registry,
    set_position_registry,
)

# Production Safety
from src.execution.production_safety import (
    ActionIntent,
    ActionIntentStatus,
    AtomicStopReplacer,
    EventOrderingEnforcer,
    ExitEscalationLevel,
    ExitTimeoutManager,
    PositionProtectionMonitor,
    ProtectionEnforcer,
    SafetyConfig,
    StopReplaceContext,
    WriteAheadIntentLog,
)

__all__ = [
    # State Machine
    "ManagedPosition",
    "PositionState",
    "PositionRegistry",
    "ExitReason",
    "OrderEvent",
    "OrderEventType",
    "FillRecord",
    "InvariantViolation",
    "check_invariant",
    "get_position_registry",
    "reset_position_registry",
    "set_position_registry",
    # Unified Manager
    "PositionManager",
    # Manager V2 (deprecated)
    "PositionManagerV2",
    "ManagementAction",
    "ActionType",
    "DecisionTick",
    # Persistence
    "PositionPersistence",
    "recover_from_persistence",
    "persist_registry_state",
    # Gateway
    "ExecutionGateway",
    "ExecutionResult",
    "PendingOrder",
    "OrderPurpose",
    # Production Safety
    "SafetyConfig",
    "AtomicStopReplacer",
    "StopReplaceContext",
    "ProtectionEnforcer",
    "EventOrderingEnforcer",
    "WriteAheadIntentLog",
    "ActionIntent",
    "ActionIntentStatus",
    "ExitTimeoutManager",
    "ExitEscalationLevel",
    "PositionProtectionMonitor",
]
