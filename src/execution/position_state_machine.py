"""
Position State Machine - Production Grade.

This module enforces world-class position management with:
1. Single position per symbol (enforced)
2. Explicit position states including in-flight states
3. Hard stop attached to position (immutable floor)
4. TP-driven partials only (state transitions, not random orders)
5. Full close before direction change (reversal lock)
6. Invariant assertions (runtime safety)
7. Idempotent event handling
8. Crash recovery support

NO TRADE CAN EXIST OUTSIDE THIS STATE MACHINE.
"""
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN
from enum import Enum
from datetime import datetime, timezone
from typing import Optional, Dict, List, Tuple, Set
import threading
import hashlib
import json

from src.domain.models import Side, OrderType
from src.exceptions import OperationalError, DataError
from src.monitoring.logger import get_logger
from src.data.symbol_utils import normalize_symbol_for_position_match

logger = get_logger(__name__)


def _normalize_symbol(symbol: str) -> str:
    """Normalize symbol for consistent position lookup across formats.
    
    Handles: PF_TRXUSD, TRX/USD, TRX/USD:USD -> TRXUSD
    """
    return normalize_symbol_for_position_match(symbol)


# ============ INVARIANT CHECKING ============

class InvariantViolation(Exception):
    """Raised when a critical invariant is violated."""
    pass


def check_invariant(condition: bool, message: str) -> None:
    """Assert an invariant. Raises InvariantViolation if false."""
    if not condition:
        logger.critical(f"INVARIANT VIOLATION: {message}")
        raise InvariantViolation(message)


# ============ ENUMS ============

class PositionState(str, Enum):
    """
    Position lifecycle states.
    
    State Machine:
        PENDING → OPEN (entry filled)
        PENDING → CANCELLED (entry cancelled/rejected)
        OPEN → PROTECTED (stop moved to break-even or better)
        OPEN → PARTIAL (TP1 hit, partial close executed)
        OPEN → EXIT_PENDING (exit order sent)
        PROTECTED → PARTIAL (TP1 hit after BE protection)
        PROTECTED → EXIT_PENDING (exit order sent)
        PARTIAL → EXIT_PENDING (final exit order sent)
        EXIT_PENDING → CLOSED (exit filled)
        EXIT_PENDING → PARTIAL (partial exit fill, more remaining)
        any → ERROR (reconciliation mismatch)
        
    Terminal States: CLOSED, CANCELLED, ERROR
    In-Flight States: PENDING, EXIT_PENDING, CANCEL_PENDING
    """
    # Entry States
    PENDING = "pending"              # Entry order submitted, awaiting fill
    
    # Active States
    OPEN = "open"                    # Position filled, stop active, no protection yet
    PROTECTED = "protected"          # Stop moved to break-even or profit-lock
    PARTIAL = "partial"              # TP1/TP2 hit, partial close executed, runner active
    
    # In-Flight States (critical for production)
    EXIT_PENDING = "exit_pending"    # Exit order sent, awaiting fill
    CANCEL_PENDING = "cancel_pending" # Cancelling stale order
    
    # Terminal States
    CLOSED = "closed"                # Position fully closed
    CANCELLED = "cancelled"          # Entry never filled, cancelled
    ERROR = "error"                  # Reconciliation error - requires manual intervention
    ORPHANED = "orphaned"            # Exchange/registry mismatch


class ExitReason(str, Enum):
    """Reason for position exit."""
    STOP_LOSS = "stop_loss"
    TRAILING_STOP = "trailing_stop"
    TAKE_PROFIT_1 = "take_profit_1"
    TAKE_PROFIT_2 = "take_profit_2"
    TAKE_PROFIT_FINAL = "take_profit_final"
    PREMISE_INVALIDATION = "premise_invalidation"
    DIRECTION_REVERSAL = "direction_reversal"
    KILL_SWITCH = "kill_switch"
    MANUAL = "manual"
    TIME_BASED = "time_based"
    ABANDON_SHIP = "abandon_ship"
    RECONCILIATION = "reconciliation"
    ORPHAN_FLATTEN = "orphan_flatten"
    MANUAL_RESET = "manual_reset"


def normalize_exit_reason(
    reason: Optional[object],
    *,
    default: ExitReason = ExitReason.TIME_BASED,
) -> tuple[ExitReason, bool]:
    """
    Normalize potentially invalid exit reason input to a valid ExitReason enum.

    Returns (normalized_reason, was_normalized).
    """
    if isinstance(reason, ExitReason):
        return reason, False

    if isinstance(reason, str):
        raw = reason.strip()
        if raw:
            normalized = raw.lower().replace("-", "_").replace(" ", "_")
            aliases = {
                "strategic": ExitReason.TIME_BASED,
                "strategic_close": ExitReason.TIME_BASED,
                "auction_strategic_close": ExitReason.TIME_BASED,
                "unknown": ExitReason.MANUAL,
            }
            if normalized in aliases:
                return aliases[normalized], True
            try:
                return ExitReason(normalized), True
            except ValueError:
                enum_name = normalized.upper()
                if enum_name in ExitReason.__members__:
                    return ExitReason.__members__[enum_name], True

    return default, True


class OrderEventType(str, Enum):
    """Order event types for idempotent processing."""
    SUBMITTED = "submitted"
    ACKNOWLEDGED = "acknowledged"
    PARTIAL_FILL = "partial_fill"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    REPLACED = "replaced"


# ============ ORDER EVENT (for idempotent handling) ============

@dataclass(frozen=True)
class OrderEvent:
    """
    Immutable order event for idempotent processing.
    
    The same event applied twice MUST be a no-op.
    """
    order_id: str
    client_order_id: str
    event_type: OrderEventType
    event_seq: int  # Sequence number for ordering
    timestamp: datetime
    
    # Fill details (optional)
    fill_qty: Optional[Decimal] = None
    fill_price: Optional[Decimal] = None
    fill_id: Optional[str] = None
    
    # Error details (optional)
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    
    def event_hash(self) -> str:
        """Generate unique hash for deduplication."""
        data = f"{self.order_id}:{self.event_type.value}:{self.event_seq}"
        if self.fill_id:
            data += f":{self.fill_id}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]


# ============ FILL RECORD ============

@dataclass
class FillRecord:
    """Record of a single fill."""
    fill_id: str
    order_id: str
    side: Side  # LONG = buy, SHORT = sell
    qty: Decimal
    price: Decimal
    timestamp: datetime
    is_entry: bool  # True for entry fills, False for exit fills
    
    def __post_init__(self):
        check_invariant(self.qty > 0, f"Fill qty must be positive: {self.qty}")


# ============ MANAGED POSITION ============

@dataclass
class ManagedPosition:
    """
    Immutable position state container with production-grade invariants.
    
    CRITICAL DESIGN PRINCIPLES:
    1. initial_* fields are IMMUTABLE after acknowledgement
    2. State transitions are validated
    3. Size can only DECREASE (partials), never increase
    4. Stop can only move TOWARD profit, never away
    5. All events are idempotent (duplicate = no-op)
    """
    # Identity
    symbol: str
    side: Side
    position_id: str  # Unique identifier for this position instance
    
    # ========== IMMUTABLE ENTRY PARAMETERS (set at creation, never changed) ==========
    # Invariant C: These NEVER mutate after entry acknowledgement
    initial_size: Decimal
    initial_entry_price: Decimal  # Basis price for calculations
    initial_stop_price: Decimal   # HARD FLOOR - cannot be moved away from profit
    initial_tp1_price: Optional[Decimal]
    initial_tp2_price: Optional[Decimal]
    initial_final_target: Optional[Decimal]
    
    # Current State
    state: PositionState = PositionState.PENDING
    current_stop_price: Optional[Decimal] = None  # Starts at initial, can only improve
    
    # ========== FILL TRACKING (Invariant B) ==========
    entry_fills: List[FillRecord] = field(default_factory=list)
    exit_fills: List[FillRecord] = field(default_factory=list)
    
    # Computed from fills (cached for performance)
    _filled_entry_qty: Decimal = Decimal("0")
    _filled_exit_qty: Decimal = Decimal("0")
    _avg_entry_price: Optional[Decimal] = None
    
    # ========== ORDER TRACKING ==========
    entry_order_id: Optional[str] = None
    entry_client_order_id: Optional[str] = None
    stop_order_id: Optional[str] = None
    stop_client_order_id: Optional[str] = None
    tp1_order_id: Optional[str] = None
    tp2_order_id: Optional[str] = None
    pending_exit_order_id: Optional[str] = None  # For EXIT_PENDING state
    pending_exit_client_order_id: Optional[str] = None
    
    # ========== EVENT TRACKING (for idempotency) ==========
    processed_event_hashes: Set[str] = field(default_factory=set)
    
    # ========== STATE FLAGS ==========
    entry_acknowledged: bool = False  # Invariant C kicks in after this
    tp1_filled: bool = False
    tp2_filled: bool = False  # When TP1 order is skipped (e.g. below venue min), first fill may set tp2_filled; do not assume tp1_filled before tp2_filled in fill handling. check_tp2_hit (price-based) still requires tp1_filled for semantic order.
    break_even_triggered: bool = False
    trailing_active: bool = False
    peak_price: Optional[Decimal] = None  # For trailing stop calculation
    
    # ========== EXIT TRACKING ==========
    exit_reason: Optional[ExitReason] = None
    exit_time: Optional[datetime] = None
    
    # ========== TRADE RECORDING ==========
    trade_recorded: bool = False  # Set True once trade_recorder persists this position's trade
    trade_record_attempts: int = 0  # How many times recording was attempted (cap retries)
    
    # ========== METADATA ==========
    setup_type: Optional[str] = None
    regime: Optional[str] = None
    trade_type: Optional[str] = None  # "tight_smc" or "wide_structure"
    intent_confirmed: bool = False  # BE gate for tight: set on market confirmation (BOS/level), not entry ACK
    futures_symbol: Optional[str] = None  # Exchange symbol (e.g. X/USD:USD) for order placement
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    # ========== CONFIGURATION ==========
    partial_close_pct: Decimal = Decimal("0.5")  # % to close at TP1 (legacy)
    min_partial_for_be: Decimal = Decimal("0.3")  # Min fill % before BE allowed
    
    # Snapshot-based targets (set once on first entry fill, never mutated)
    entry_size_initial: Optional[Decimal] = None   # Filled qty when position opened
    tp1_qty_target: Optional[Decimal] = None      # Fixed TP1 close size
    tp2_qty_target: Optional[Decimal] = None      # Fixed TP2 close size

    # Runner mode configuration (populated from MultiTPConfig when enabled)
    tp1_close_pct: Decimal = Decimal("0.40")   # % of filled entry to close at TP1
    tp2_close_pct: Decimal = Decimal("0.40")   # % of filled entry to close at TP2
    runner_pct: Decimal = Decimal("0.20")       # Implicit remainder; no TP order in runner mode
    runner_mode: bool = False                    # True when runner_pct > 0 and runner_has_fixed_tp == False
    final_target_behavior: str = "tighten_trail"  # tighten_trail | close_partial | close_full
    tighten_trail_atr_mult: Decimal = Decimal("1.2")  # ATR mult after final target touch
    final_target_touched: bool = False           # Set True on first final target hit (one-time tighten)
    
    # Progressive trailing state: tracks the highest R-level tightening applied
    highest_r_tighten_level: int = -1  # Index into progressive_trail_levels; -1 = none applied yet
    current_trail_atr_mult: Optional[Decimal] = None  # Current effective ATR mult (set by progressive trail)
    
    def __post_init__(self):
        """Validate position parameters."""
        check_invariant(
            self.initial_size > 0,
            f"Invalid position size: {self.initial_size}"
        )
        check_invariant(
            self.initial_stop_price is not None,
            "Position MUST have initial stop price"
        )
        
        # Validate stop direction
        if self.side == Side.LONG:
            check_invariant(
                self.initial_stop_price < self.initial_entry_price,
                f"LONG stop ({self.initial_stop_price}) must be below entry ({self.initial_entry_price})"
            )
        else:
            check_invariant(
                self.initial_stop_price > self.initial_entry_price,
                f"SHORT stop ({self.initial_stop_price}) must be above entry ({self.initial_entry_price})"
            )
        
        # Validate runner mode pct invariant
        if self.runner_mode:
            total = self.tp1_close_pct + self.tp2_close_pct + self.runner_pct
            check_invariant(
                total <= Decimal("1.001"),
                f"TP pcts exceed 100%: tp1={self.tp1_close_pct} + tp2={self.tp2_close_pct} + runner={self.runner_pct} = {total}"
            )
        
        # Initialize current stop
        if self.current_stop_price is None:
            self.current_stop_price = self.initial_stop_price

    # ========== COMPUTED PROPERTIES ==========
    
    @property
    def filled_entry_qty(self) -> Decimal:
        """Total entry quantity filled."""
        return sum(f.qty for f in self.entry_fills)
    
    @property
    def filled_exit_qty(self) -> Decimal:
        """Total exit quantity filled."""
        return sum(f.qty for f in self.exit_fills)
    
    @property
    def remaining_qty(self) -> Decimal:
        """
        Remaining position quantity.
        
        Invariant B: remaining_qty = filled_entry_qty - filled_exit_qty
        and must never go negative.
        """
        remaining = self.filled_entry_qty - self.filled_exit_qty
        check_invariant(
            remaining >= 0,
            f"INVARIANT B VIOLATION: remaining_qty negative: {remaining}"
        )
        return remaining
    
    @property
    def avg_entry_price(self) -> Optional[Decimal]:
        """Volume-weighted average entry price.
        
        Falls back to initial_entry_price when fills have zero prices
        (common for orphaned/imported positions with synthetic fills).
        """
        if not self.entry_fills:
            return self.initial_entry_price if self.initial_entry_price and self.initial_entry_price > 0 else None
        total_value = sum(f.qty * f.price for f in self.entry_fills)
        total_qty = self.filled_entry_qty
        if total_qty == 0:
            return self.initial_entry_price if self.initial_entry_price and self.initial_entry_price > 0 else None
        vwap = total_value / total_qty
        if vwap == 0 and self.initial_entry_price and self.initial_entry_price > 0:
            return self.initial_entry_price
        return vwap
    
    @property
    def avg_exit_price(self) -> Optional[Decimal]:
        """Volume-weighted average exit price from exit fills."""
        if not self.exit_fills:
            return None
        total_value = sum(f.qty * f.price for f in self.exit_fills)
        total_qty = self.filled_exit_qty
        if total_qty == 0:
            return None
        return total_value / total_qty
    
    @property
    def is_terminal(self) -> bool:
        """Check if position is in a terminal state."""
        return self.state in (
            PositionState.CLOSED,
            PositionState.CANCELLED,
            PositionState.ERROR,
            PositionState.ORPHANED
        )
    
    @property
    def is_active(self) -> bool:
        """Check if position has exposure (non-zero remaining qty)."""
        return self.remaining_qty > 0 and not self.is_terminal
    
    # ========== IDEMPOTENT EVENT HANDLING ==========
    
    def _is_duplicate_event(self, event: OrderEvent) -> bool:
        """Check if event was already processed."""
        return event.event_hash() in self.processed_event_hashes
    
    def _mark_event_processed(self, event: OrderEvent) -> None:
        """Mark event as processed for idempotency."""
        self.processed_event_hashes.add(event.event_hash())
        self.updated_at = datetime.now(timezone.utc)

    def _matches_entry_event(self, event: OrderEvent) -> bool:
        """Match entry updates by exchange order id or client order id."""
        if self.entry_order_id and event.order_id == self.entry_order_id:
            return True
        if self.entry_client_order_id and event.client_order_id == self.entry_client_order_id:
            return True
        # Compatibility: some older states stored client id in entry_order_id.
        if self.entry_order_id and event.client_order_id == self.entry_order_id:
            return True
        return False

    def _matches_pending_exit_event(self, event: OrderEvent) -> bool:
        """Match pending full-exit updates by exchange/client id."""
        if self.pending_exit_order_id and event.order_id == self.pending_exit_order_id:
            return True
        if self.pending_exit_client_order_id and event.client_order_id == self.pending_exit_client_order_id:
            return True
        # Compatibility: pending_exit_order_id may contain client id in older states.
        if self.pending_exit_order_id and event.client_order_id == self.pending_exit_order_id:
            return True
        return False

    def _matches_stop_event(self, event: OrderEvent) -> bool:
        """Match stop order updates by exchange/client id."""
        if self.stop_order_id and event.order_id == self.stop_order_id:
            return True
        if self.stop_client_order_id and event.client_order_id == self.stop_client_order_id:
            return True
        # Compatibility: stop_order_id may contain client id in older states.
        if self.stop_order_id and event.client_order_id == self.stop_order_id:
            return True
        return False

    def _matches_tp1_event(self, event: OrderEvent) -> bool:
        """Match TP1 order fills by exchange/client id."""
        if self.tp1_order_id and event.order_id == self.tp1_order_id:
            return True
        if self.tp1_order_id and event.client_order_id == self.tp1_order_id:
            return True
        if event.client_order_id and event.client_order_id.startswith("tp1-"):
            return True
        return False

    def _matches_tp2_event(self, event: OrderEvent) -> bool:
        """Match TP2 order fills by exchange/client id."""
        if self.tp2_order_id and event.order_id == self.tp2_order_id:
            return True
        if self.tp2_order_id and event.client_order_id == self.tp2_order_id:
            return True
        if event.client_order_id and event.client_order_id.startswith("tp2-"):
            return True
        return False

    # ========== STATE TRANSITION METHODS ==========
    
    def apply_order_event(self, event: OrderEvent) -> bool:
        """
        Apply an order event to the position state.
        
        IDEMPOTENT: Duplicate events are no-ops.
        
        Returns:
            True if state was modified, False if duplicate/no-op
        """
        # Idempotency check
        if self._is_duplicate_event(event):
            logger.debug(f"Duplicate event ignored: {event.event_hash()}")
            return False
        
        self._mark_event_processed(event)
        
        # Route to appropriate handler
        if event.event_type == OrderEventType.ACKNOWLEDGED:
            return self._handle_acknowledged(event)
        elif event.event_type == OrderEventType.PARTIAL_FILL:
            return self._handle_partial_fill(event)
        elif event.event_type == OrderEventType.FILLED:
            return self._handle_fill(event)
        elif event.event_type in (OrderEventType.CANCELLED, OrderEventType.EXPIRED):
            return self._handle_cancel(event)
        elif event.event_type == OrderEventType.REJECTED:
            return self._handle_reject(event)
        
        return False
    
    def _handle_acknowledged(self, event: OrderEvent) -> bool:
        """Handle order acknowledgement. Locks immutable fields."""
        if self._matches_entry_event(event):
            self.entry_acknowledged = True
            logger.info(f"Entry acknowledged for {self.symbol}, immutables locked")
            return True
        return False
    
    def _handle_partial_fill(self, event: OrderEvent) -> bool:
        """Handle partial fill event."""
        if event.fill_qty is None or event.fill_price is None:
            logger.error(f"Partial fill missing qty/price: {event}")
            return False
        if event.fill_qty <= 0:
            logger.warning(f"Ignoring non-positive partial fill qty: {event.fill_qty}")
            return False
        
        return self._record_fill(event)
    
    def _handle_fill(self, event: OrderEvent) -> bool:
        """Handle complete fill event."""
        if event.fill_qty is None or event.fill_price is None:
            logger.error(f"Fill missing qty/price: {event}")
            return False
        if event.fill_qty <= 0:
            logger.warning(f"Ignoring non-positive fill qty: {event.fill_qty}")
            return False
        
        return self._record_fill(event)
    
    def _record_fill(self, event: OrderEvent) -> bool:
        """Record a fill and update state."""
        is_entry = self._matches_entry_event(event)
        is_tp1 = self._matches_tp1_event(event)
        is_tp2 = self._matches_tp2_event(event)
        is_exit = (
            self._matches_pending_exit_event(event)
            or self._matches_stop_event(event)
            or is_tp1
            or is_tp2
        )
        
        fill = FillRecord(
            fill_id=event.fill_id or f"{event.order_id}-{event.event_seq}",
            order_id=event.order_id,
            side=self.side if is_entry else (Side.SHORT if self.side == Side.LONG else Side.LONG),
            qty=event.fill_qty,
            price=event.fill_price,
            timestamp=event.timestamp,
            is_entry=is_entry
        )
        
        if is_entry:
            self.entry_fills.append(fill)
            self._snapshot_targets_on_entry_fill(event)
            self._update_state_after_entry_fill()
        elif is_exit:
            self.exit_fills.append(fill)
            if is_tp1:
                self.tp1_filled = True
            if is_tp2:
                # Invariant: if TP1 was skipped (no order), tp2_filled first is allowed. Else log if TP2 fill before TP1.
                if self.tp1_order_id is not None and not self.tp1_filled:
                    logger.warning(
                        "TP2 fill received while TP1 order exists and not filled; check event mapping",
                        symbol=self.symbol,
                        tp1_order_id=self.tp1_order_id,
                        tp2_order_id=event.order_id,
                        fill_id=event.fill_id,
                    )
                self.tp2_filled = True
            self._update_state_after_exit_fill(event)
        else:
            logger.warning(f"Unknown fill order_id: {event.order_id}")
            return False
        
        # Re-validate Invariant B
        _ = self.remaining_qty  # Triggers check
        
        return True
    
    def ensure_snapshot_targets(self) -> None:
        """Set entry_size_initial, tp1_qty_target, tp2_qty_target once from filled_entry_qty if not set."""
        if self.entry_size_initial is not None:
            return
        filled = self.filled_entry_qty
        if filled <= 0:
            return
        step = Decimal("0.0001")
        self.entry_size_initial = filled.quantize(step, rounding=ROUND_DOWN)
        self.tp1_qty_target = (self.entry_size_initial * self.tp1_close_pct).quantize(step, rounding=ROUND_DOWN)
        self.tp2_qty_target = (self.entry_size_initial * self.tp2_close_pct).quantize(step, rounding=ROUND_DOWN)
        logger.info(
            "Snapshot targets set",
            symbol=self.symbol,
            entry_size_initial=str(self.entry_size_initial),
            tp1_target=str(self.tp1_qty_target),
            tp2_target=str(self.tp2_qty_target),
        )

    def _snapshot_targets_on_entry_fill(self, _event: OrderEvent) -> None:
        """Called from _record_fill when entry fill is recorded."""
        self.ensure_snapshot_targets()

    def _update_state_after_entry_fill(self) -> None:
        """Update state after entry fill."""
        if self.state == PositionState.PENDING:
            self.state = PositionState.OPEN
            logger.info(
                "Position OPENED",
                symbol=self.symbol,
                side=self.side.value,
                filled_qty=str(self.filled_entry_qty),
                avg_price=str(self.avg_entry_price)
            )
    
    def _update_state_after_exit_fill(self, event: OrderEvent) -> None:
        """Update state after exit fill."""
        remaining = self.remaining_qty
        
        if remaining <= 0:
            # Fully closed — use final fill timestamp as the authoritative close time
            fill_time = event.timestamp if event.timestamp else None
            self._mark_closed(
                self.exit_reason or ExitReason.STOP_LOSS,
                exit_time=fill_time,
            )
        elif self.state == PositionState.EXIT_PENDING:
            # Partial exit fill
            if event.event_type == OrderEventType.FILLED:
                # Exit order fully filled but position not closed = error
                self.state = PositionState.ERROR
                logger.error(f"Exit order filled but position not closed: {self.symbol}")
            else:
                # Still more to fill
                pass
    
    def _handle_cancel(self, event: OrderEvent) -> bool:
        """Handle order cancellation."""
        if self._matches_entry_event(event) and self.state == PositionState.PENDING:
            if self.filled_entry_qty == 0:
                self.state = PositionState.CANCELLED
                logger.info(f"Entry cancelled for {self.symbol}")
            else:
                # Partial fill then cancel - position is open with partial qty
                self.state = PositionState.OPEN
        elif self._matches_pending_exit_event(event):
            # Exit cancelled - return to previous state
            if self.remaining_qty > 0:
                self.state = PositionState.PARTIAL if self.tp1_filled else PositionState.OPEN
                self.pending_exit_order_id = None
                self.pending_exit_client_order_id = None
        return True
    
    def _handle_reject(self, event: OrderEvent) -> bool:
        """Handle order rejection."""
        if self._matches_entry_event(event) and self.state == PositionState.PENDING:
            self.state = PositionState.CANCELLED
            logger.error(f"Entry rejected for {self.symbol}: {event.error_message}")
        return True
    
    # ========== STOP MANAGEMENT ==========
    
    def update_stop(self, new_stop_price: Decimal, order_id: Optional[str] = None) -> bool:
        """
        Update stop price.
        
        Invariant D: Stop can only move toward profit.
        - LONG: stop can only move UP
        - SHORT: stop can only move DOWN
        """
        if self.is_terminal:
            return False
        
        # Invariant C: Cannot modify after entry acknowledged (only improve)
        if self.entry_acknowledged:
            if not self._validate_stop_move(new_stop_price):
                return False
        
        old_stop = self.current_stop_price
        self.current_stop_price = new_stop_price
        if order_id:
            self.stop_order_id = order_id
        
        self.updated_at = datetime.now(timezone.utc)
        
        logger.info(
            "Stop updated",
            symbol=self.symbol,
            old=str(old_stop),
            new=str(new_stop_price)
        )
        return True
    
    def _validate_stop_move(self, new_stop: Decimal) -> bool:
        """
        Validate stop movement direction (Invariant D).
        
        - LONG: new stop must be >= current stop (moving up)
        - SHORT: new stop must be <= current stop (moving down)
        - Cannot move past initial stop (hard floor)
        """
        if new_stop is None:
            return False
        
        if self.side == Side.LONG:
            if new_stop < self.initial_stop_price:
                logger.error(f"Cannot move LONG stop below initial: {new_stop} < {self.initial_stop_price}")
                return False
            if self.current_stop_price and new_stop < self.current_stop_price:
                logger.error(f"Cannot move LONG stop down: {new_stop} < {self.current_stop_price}")
                return False
        else:  # SHORT
            if new_stop > self.initial_stop_price:
                logger.error(f"Cannot move SHORT stop above initial: {new_stop} > {self.initial_stop_price}")
                return False
            if self.current_stop_price and new_stop > self.current_stop_price:
                logger.error(f"Cannot move SHORT stop up: {new_stop} > {self.current_stop_price}")
                return False
        
        return True
    
    # ========== INTENT CONFIRMATION (market confirmation, not entry ACK) ==========
    
    def confirm_intent(self) -> bool:
        """
        Set intent_confirmed when market confirms (e.g. BOS/confirmation level crossed).
        Idempotent; returns True iff state changed.
        """
        if self.intent_confirmed:
            return False
        self.intent_confirmed = True
        self.updated_at = datetime.now(timezone.utc)
        logger.info(f"Intent confirmed for {self.symbol} (market confirmation)")
        return True
    
    # ========== BREAK-EVEN LOGIC (Conditional) ==========
    
    def should_trigger_break_even(self) -> bool:
        """
        Check if break-even should be triggered.
        
        Conditional requirements (prevents death by thousand stop-outs):
        1. tp1 filled with qty >= min_partial_for_be
        2. Have proven intent OR trade_type is "wide"
        """
        if self.break_even_triggered:
            return False
        
        if not self.tp1_filled:
            return False
        
        # Check minimum fill requirement
        tp1_fill_ratio = self.filled_exit_qty / self.initial_size if self.initial_size > 0 else Decimal("0")
        if tp1_fill_ratio < self.min_partial_for_be:
            logger.debug(
                f"BE skipped: TP1 fill ratio {tp1_fill_ratio} < min {self.min_partial_for_be}"
            )
            return False
        
        # For wide trades, earlier defense is OK
        if self.trade_type == "wide_structure":
            return True
        
        # For tight trades, require intent_confirmed (set when price crosses BOS/confirmation level or structure confirms)
        return bool(self.intent_confirmed)

    def activate_trailing_if_guard_passes(
        self,
        current_atr: Decimal,
        atr_min: Decimal = Decimal("0"),
    ) -> bool:
        """
        Activate trailing at TP1 when guard passes (e.g. ATR > threshold).
        Guard evaluated once at TP1; once trailing_active it stays on.
        Returns True iff trailing_active was set.
        """
        if self.trailing_active:
            return False
        if not self.tp1_filled:
            return False
        if current_atr <= 0:
            return False
        if atr_min > 0 and current_atr < atr_min:
            logger.debug(
                "Trailing guard not passed: ATR < min",
                symbol=self.symbol,
                atr=str(current_atr),
                atr_min=str(atr_min),
            )
            return False
        self.trailing_active = True
        logger.info(
            "Trailing activated at TP1 (guard passed)",
            symbol=self.symbol,
            atr=str(current_atr),
        )
        return True

    def trigger_break_even(self, be_price: Optional[Decimal] = None) -> bool:
        """Trigger break-even stop move."""
        if not self.should_trigger_break_even():
            return False
        
        target = be_price or self.avg_entry_price
        if target is None:
            return False
        
        if self.update_stop(target):
            self.break_even_triggered = True
            if self.state == PositionState.OPEN:
                self.state = PositionState.PROTECTED
            return True
        return False
    
    # ========== EXIT MANAGEMENT ==========
    
    def initiate_exit(
        self,
        reason: ExitReason | str | None,
        order_id: str,
        client_order_id: Optional[str] = None,
    ) -> bool:
        """
        Initiate exit - transition to EXIT_PENDING.
        """
        if self.is_terminal or self.state == PositionState.EXIT_PENDING:
            return False
        
        normalized_reason, was_normalized = normalize_exit_reason(reason)
        if was_normalized:
            logger.warning(
                "EXIT_REASON_INVALID_FALLBACK",
                symbol=self.symbol,
                position_id=self.position_id,
                input_reason=str(reason),
                normalized_reason=normalized_reason.value,
            )

        self.exit_reason = normalized_reason
        self.pending_exit_order_id = order_id
        self.pending_exit_client_order_id = client_order_id or order_id
        self.state = PositionState.EXIT_PENDING
        self.updated_at = datetime.now(timezone.utc)
        
        logger.info(
            "Exit initiated",
            symbol=self.symbol,
            reason=normalized_reason.value,
            order_id=order_id
        )
        return True
    
    # ========== CENTRALIZED CLOSE ==========
    
    def _mark_closed(self, reason: ExitReason, exit_time: Optional[datetime] = None) -> None:
        """
        Single entry point for transitioning to CLOSED.
        
        All code paths that close a position MUST call this method instead of
        setting ``self.state = PositionState.CLOSED`` directly.  This ensures:
        - exit_reason and exit_time are always set consistently
        - trade_recorded stays False (recording happens at the gateway layer)
        - No qty / price / fill mutation — pure state + timestamp update
        """
        self.state = PositionState.CLOSED
        if self.exit_reason is None:
            self.exit_reason = reason
        self.exit_time = exit_time or datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)
        logger.info(
            "Position CLOSED",
            symbol=self.symbol,
            exit_reason=self.exit_reason.value if self.exit_reason else "unknown",
            trade_recorded=self.trade_recorded,
        )
    
    def force_close(self, reason: ExitReason) -> None:
        """Force immediate close (for emergencies)."""
        self._mark_closed(reason)
    
    def mark_error(self, error_message: str) -> None:
        """Mark position as ERROR state."""
        self.state = PositionState.ERROR
        self.updated_at = datetime.now(timezone.utc)
        logger.error(f"Position marked ERROR: {self.symbol} - {error_message}")
    
    def mark_orphaned(self) -> None:
        """Mark position as ORPHANED (exchange mismatch)."""
        self.state = PositionState.ORPHANED
        if self.exit_reason is None:
            self.exit_reason = ExitReason.RECONCILIATION
        if self.exit_time is None:
            self.exit_time = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)
        logger.critical(f"Position marked ORPHANED: {self.symbol}")

    def reconcile_quantity_to_exchange(
        self,
        exchange_qty: Decimal,
        exchange_entry_price: Optional[Decimal],
        qty_epsilon: Decimal,
    ) -> Optional[str]:
        """
        Converge local remaining quantity to exchange truth.

        This prevents perpetual reconciliation loops when a fill/update was missed.
        Returns a short summary string when a mutation is applied.
        """
        if self.is_terminal:
            return None

        local_qty = self.remaining_qty
        delta = exchange_qty - local_qty
        if abs(delta) <= qty_epsilon:
            return None

        now = datetime.now(timezone.utc)
        reference_price = (
            exchange_entry_price
            if exchange_entry_price is not None and exchange_entry_price > 0
            else self.avg_entry_price or self.initial_entry_price
        )

        if delta > 0:
            # Exchange has larger exposure than local registry: add synthetic entry fill.
            fill_qty = delta
            fill = FillRecord(
                fill_id=f"reconcile-entry-{int(now.timestamp() * 1000)}-{len(self.entry_fills) + 1}",
                order_id="reconcile-sync",
                side=self.side,
                qty=fill_qty,
                price=reference_price,
                timestamp=now,
                is_entry=True,
            )
            self.entry_fills.append(fill)
            self._update_state_after_entry_fill()
            self.updated_at = now
            _ = self.remaining_qty  # Re-assert invariant B
            return (
                f"entry+{fill_qty} local={local_qty} exchange={exchange_qty} "
                f"price={reference_price}"
            )

        # delta < 0: exchange has smaller exposure, apply synthetic exit fill.
        fill_qty = min(abs(delta), local_qty)
        if fill_qty <= 0:
            return None
        fill = FillRecord(
            fill_id=f"reconcile-exit-{int(now.timestamp() * 1000)}-{len(self.exit_fills) + 1}",
            order_id="reconcile-sync",
            side=Side.SHORT if self.side == Side.LONG else Side.LONG,
            qty=fill_qty,
            price=reference_price,
            timestamp=now,
            is_entry=False,
        )
        self.exit_fills.append(fill)
        if self.remaining_qty <= qty_epsilon:
            self._mark_closed(ExitReason.RECONCILIATION, exit_time=now)
        self.updated_at = now
        _ = self.remaining_qty  # Re-assert invariant B
        return (
            f"exit+{fill_qty} local={local_qty} exchange={exchange_qty} "
            f"price={reference_price}"
        )
    
    # ========== PRICE CHECKS ==========
    
    def check_stop_hit(self, current_price: Decimal) -> bool:
        """Check if stop loss has been hit."""
        if self.is_terminal or self.state == PositionState.EXIT_PENDING:
            return False
        if self.current_stop_price is None:
            return False
        
        if self.side == Side.LONG:
            return current_price <= self.current_stop_price
        else:
            return current_price >= self.current_stop_price
    
    def check_tp1_hit(self, current_price: Decimal) -> bool:
        """Check if TP1 has been hit."""
        if self.tp1_filled or self.initial_tp1_price is None:
            return False
        if self.side == Side.LONG:
            return current_price >= self.initial_tp1_price
        else:
            return current_price <= self.initial_tp1_price
    
    def check_tp2_hit(self, current_price: Decimal) -> bool:
        """Check if TP2 level has been hit (price-based). Requires tp1_filled for semantic order; fill events may set tp2_filled first when TP1 order was skipped (dust)."""
        if self.tp2_filled or self.initial_tp2_price is None:
            return False
        if not self.tp1_filled:
            return False
        if self.side == Side.LONG:
            return current_price >= self.initial_tp2_price
        else:
            return current_price <= self.initial_tp2_price
    
    def check_final_target_hit(self, current_price: Decimal) -> bool:
        """Check if final target has been hit."""
        if self.initial_final_target is None:
            return False
        if self.side == Side.LONG:
            return current_price >= self.initial_final_target
        else:
            return current_price <= self.initial_final_target
    
    # ========== SERIALIZATION (for persistence) ==========
    
    def to_dict(self) -> Dict:
        """Serialize position for persistence."""
        return {
            "position_id": self.position_id,
            "symbol": self.symbol,
            "side": self.side.value,
            "state": self.state.value,
            "initial_size": str(self.initial_size),
            "initial_entry_price": str(self.initial_entry_price),
            "initial_stop_price": str(self.initial_stop_price),
            "initial_tp1_price": str(self.initial_tp1_price) if self.initial_tp1_price else None,
            "initial_tp2_price": str(self.initial_tp2_price) if self.initial_tp2_price else None,
            "initial_final_target": str(self.initial_final_target) if self.initial_final_target else None,
            "current_stop_price": str(self.current_stop_price) if self.current_stop_price else None,
            "entry_acknowledged": self.entry_acknowledged,
            "tp1_filled": self.tp1_filled,
            "tp2_filled": self.tp2_filled,
            "break_even_triggered": self.break_even_triggered,
            "trailing_active": self.trailing_active,
            "exit_reason": self.exit_reason.value if self.exit_reason else None,
            "setup_type": self.setup_type,
            "regime": self.regime,
            "trade_type": self.trade_type,
            "intent_confirmed": self.intent_confirmed,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "entry_order_id": self.entry_order_id,
            "stop_order_id": self.stop_order_id,
            "pending_exit_order_id": self.pending_exit_order_id,
            "pending_exit_client_order_id": self.pending_exit_client_order_id,
            "entry_fills": [
                {"fill_id": f.fill_id, "qty": str(f.qty), "price": str(f.price), "ts": f.timestamp.isoformat()}
                for f in self.entry_fills
            ],
            "exit_fills": [
                {"fill_id": f.fill_id, "qty": str(f.qty), "price": str(f.price), "ts": f.timestamp.isoformat()}
                for f in self.exit_fills
            ],
            "processed_event_hashes": list(self.processed_event_hashes),
            "trade_recorded": self.trade_recorded,
            "trade_record_attempts": self.trade_record_attempts,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "ManagedPosition":
        """Deserialize position from persistence."""
        pos = cls(
            symbol=data["symbol"],
            side=Side(data["side"]),
            position_id=data["position_id"],
            initial_size=Decimal(data["initial_size"]),
            initial_entry_price=Decimal(data["initial_entry_price"]),
            initial_stop_price=Decimal(data["initial_stop_price"]),
            initial_tp1_price=Decimal(data["initial_tp1_price"]) if data.get("initial_tp1_price") else None,
            initial_tp2_price=Decimal(data["initial_tp2_price"]) if data.get("initial_tp2_price") else None,
            initial_final_target=Decimal(data["initial_final_target"]) if data.get("initial_final_target") else None,
        )
        
        pos.state = PositionState(data["state"])
        pos.current_stop_price = Decimal(data["current_stop_price"]) if data.get("current_stop_price") else None
        pos.entry_acknowledged = data.get("entry_acknowledged", False)
        pos.tp1_filled = data.get("tp1_filled", False)
        pos.tp2_filled = data.get("tp2_filled", False)
        pos.break_even_triggered = data.get("break_even_triggered", False)
        pos.trailing_active = data.get("trailing_active", False)
        pos.exit_reason = ExitReason(data["exit_reason"]) if data.get("exit_reason") else None
        pos.setup_type = data.get("setup_type")
        pos.regime = data.get("regime")
        pos.trade_type = data.get("trade_type")
        pos.intent_confirmed = data.get("intent_confirmed", False)
        pos.created_at = datetime.fromisoformat(data["created_at"])
        pos.updated_at = datetime.fromisoformat(data["updated_at"])
        pos.entry_order_id = data.get("entry_order_id")
        pos.pending_exit_order_id = data.get("pending_exit_order_id")
        pos.pending_exit_client_order_id = data.get("pending_exit_client_order_id")
        pos.stop_order_id = data.get("stop_order_id")
        
        # Restore fills
        for f_data in data.get("entry_fills", []):
            pos.entry_fills.append(FillRecord(
                fill_id=f_data["fill_id"],
                order_id=pos.entry_order_id or "",
                side=pos.side,
                qty=Decimal(f_data["qty"]),
                price=Decimal(f_data["price"]),
                timestamp=datetime.fromisoformat(f_data["ts"]),
                is_entry=True
            ))
        
        for f_data in data.get("exit_fills", []):
            pos.exit_fills.append(FillRecord(
                fill_id=f_data["fill_id"],
                order_id="",
                side=Side.SHORT if pos.side == Side.LONG else Side.LONG,
                qty=Decimal(f_data["qty"]),
                price=Decimal(f_data["price"]),
                timestamp=datetime.fromisoformat(f_data["ts"]),
                is_entry=False
            ))
        
        pos.processed_event_hashes = set(data.get("processed_event_hashes", []))
        pos.trade_recorded = data.get("trade_recorded", False)
        pos.trade_record_attempts = data.get("trade_record_attempts", 0)
        
        return pos


# ============ POSITION REGISTRY ============

class PositionRegistry:
    """
    Single source of truth for all managed positions.
    
    ENFORCES:
    1. One position per symbol (Invariant A)
    2. Full close before direction change (Invariant E)
    3. Thread-safe, idempotent access
    """
    
    def __init__(self):
        self._positions: Dict[str, ManagedPosition] = {}
        self._lock = threading.RLock()
        self._pending_reversals: Dict[str, Side] = {}  # symbol -> pending new side
        self._closed_positions: List[ManagedPosition] = []  # History
        # Orphan hysteresis: require consecutive "missing on exchange" observations
        # before marking ORPHANED to avoid transient API/sync blips.
        self._orphan_miss_counts: Dict[str, int] = {}
        self._orphan_miss_threshold: int = 2
        # Additional ghost-killer grace windows for fast close/reconcile races.
        self._orphan_exit_pending_grace_seconds: int = 30
        # Exchange-side positions snapshot (updated by reconcile_with_exchange).
        # Used as defense-in-depth guard: even if the registry loses track of a
        # position, we still know the exchange has one and block duplicate entries.
        self._known_exchange_symbols: Set[str] = set()  # normalized symbols with live exposure
    
    # ========== INVARIANT A: Single position per symbol ==========
    
    def _check_invariant_a(self, symbol: str) -> None:
        """Invariant A: At most one non-terminal position per symbol (using normalized matching)."""
        target_norm = _normalize_symbol(symbol)
        positions = [
            p for p in self._positions.values()
            if _normalize_symbol(p.symbol) == target_norm and not p.is_terminal
        ]
        check_invariant(
            len(positions) <= 1,
            f"INVARIANT A VIOLATION: Multiple active positions for {symbol} (normalized: {target_norm})"
        )
    
    # ========== POSITION ACCESS ==========
    
    def _find_position_by_normalized(self, symbol: str) -> Optional[ManagedPosition]:
        """Find position by normalized symbol (handles PF_*, /, :USD formats).
        
        MUST be called under lock.
        """
        # Try exact match first
        pos = self._positions.get(symbol)
        if pos is not None:
            return pos
        
        # Search by normalized symbol
        target_norm = _normalize_symbol(symbol)
        for stored_symbol, pos in self._positions.items():
            if _normalize_symbol(stored_symbol) == target_norm:
                return pos
        return None
    
    def has_position(self, symbol: str) -> bool:
        """Check if symbol has an active position (handles symbol format variants)."""
        with self._lock:
            pos = self._find_position_by_normalized(symbol)
            return pos is not None and not pos.is_terminal
    
    def get_position(self, symbol: str) -> Optional[ManagedPosition]:
        """Get active position for symbol (handles symbol format variants)."""
        with self._lock:
            pos = self._find_position_by_normalized(symbol)
            if pos and not pos.is_terminal:
                return pos
            return None

    def get_position_any_state(self, symbol: str) -> Optional[ManagedPosition]:
        """Get position for symbol regardless of state (including terminal/closed)."""
        with self._lock:
            return self._find_position_by_normalized(symbol)
    
    def get_all_active(self) -> List[ManagedPosition]:
        """Get all active positions."""
        with self._lock:
            return [p for p in self._positions.values() if not p.is_terminal]
    
    def get_all(self) -> List[ManagedPosition]:
        """Get all positions including terminal."""
        with self._lock:
            return list(self._positions.values())
    
    # ========== HARD RESET (startup hygiene) ==========

    def hard_reset(self, reason: str) -> List[ManagedPosition]:
        """
        Close all active positions and clear the registry.

        Used at startup when the exchange is provably flat (0 positions,
        0 orders) to eliminate stale registry debris that would otherwise
        trigger orphan detection and the kill switch.

        Returns the list of positions that were closed so the caller can
        persist them.
        """
        with self._lock:
            closed: List[ManagedPosition] = []
            for pos in self._positions.values():
                pos.state = PositionState.CLOSED
                pos.exit_reason = pos.exit_reason or ExitReason.RECONCILIATION
                pos.updated_at = datetime.now(timezone.utc)
                self._closed_positions.append(pos)
                closed.append(pos)
            self._positions.clear()
            self._pending_reversals.clear()

        if closed:
            logger.critical(
                "STARTUP_REGISTRY_RESET: %d stale positions closed",
                len(closed),
                reason=reason,
                symbols=[p.symbol for p in closed],
            )
        return closed

    # ========== POSITION REGISTRATION ==========
    
    def can_open_position(self, symbol: str, side: Side) -> Tuple[bool, str]:
        """
        Check if a new position can be opened (Invariant E).
        
        Uses normalized symbol matching to handle format variants:
        - PF_TRXUSD, TRX/USD, TRX/USD:USD all refer to the same market
        
        Defense-in-depth: Also checks _known_exchange_symbols to block entries
        when the exchange has live exposure the registry lost track of.
        
        Returns:
            (allowed, reason)
        """
        with self._lock:
            # Use normalized lookup to catch format variants
            existing = self._find_position_by_normalized(symbol)
            
            if existing is None:
                # Defense-in-depth: Even if registry has no position, check if
                # the exchange has live exposure for this symbol. This prevents
                # the catastrophic compounding bug where a PENDING position gets
                # archived by STALE_ZERO_QTY, and new entries pile on every cycle.
                norm_key = _normalize_symbol(symbol)
                if norm_key in self._known_exchange_symbols:
                    return False, (
                        f"Exchange has live exposure for {symbol} but registry has no position. "
                        f"Blocking new entry to prevent duplicate/compounding exposure."
                    )
                return True, "No existing position"
            
            if existing.is_terminal:
                # Same defense-in-depth check for terminal positions
                norm_key = _normalize_symbol(symbol)
                if norm_key in self._known_exchange_symbols:
                    return False, (
                        f"Exchange has live exposure for {symbol} (registry position is terminal). "
                        f"Blocking new entry to prevent duplicate/compounding exposure."
                    )
                return True, "Previous position terminal"
            
            # Check if reversal is pending (use normalized key)
            target_norm = _normalize_symbol(symbol)
            reversal_pending = any(
                _normalize_symbol(s) == target_norm for s in self._pending_reversals
            )
            if reversal_pending:
                return False, f"Reversal pending for {symbol}, waiting for close confirmation"
            
            # Position exists and is active
            if existing.side == side:
                return False, f"Position already exists for {symbol} ({existing.state.value})"
            else:
                # Invariant E: Cannot open opposite until current is terminal
                return False, f"Must close existing {existing.side.value} position before opening {side.value}"
    
    def register_position(self, position: ManagedPosition) -> None:
        """
        Register a new position.
        
        IDEMPOTENT: If the SAME position (same position_id) is registered twice,
        treat as no-op (duplicate registration from concurrent tasks).
        
        Uses normalized symbol matching to detect conflicts across format variants.
        
        Raises:
            InvariantViolation if a DIFFERENT position tries to register for the same symbol
        """
        with self._lock:
            # Use normalized lookup to find existing position across format variants
            existing = self._find_position_by_normalized(position.symbol)
            
            if existing is not None:
                # IDEMPOTENT HANDLING: Same position (same position_id) registered twice
                # This handles duplicate registration from concurrent tasks
                if existing.position_id == position.position_id:
                    # Same position object - idempotent no-op
                    logger.debug(
                        "Duplicate position registration ignored (idempotent - same position_id)",
                        symbol=position.symbol,
                        position_id=position.position_id,
                        state=existing.state.value
                    )
                    return  # Idempotent - same position already registered
                
                # Different position trying to register for same symbol - check if allowed
                can_open, reason = self.can_open_position(position.symbol, position.side)
                if not can_open:
                    # This is a real conflict - raise invariant violation
                    check_invariant(False, f"Cannot register position: {reason}")
            
            # Archive old terminal position if exists (check both exact and normalized)
            old_pos = self._find_position_by_normalized(position.symbol)
            if old_pos and old_pos.is_terminal:
                self._closed_positions.append(old_pos)
                # Remove by the key it was stored under
                if old_pos.symbol in self._positions:
                    del self._positions[old_pos.symbol]
            
            self._positions[position.symbol] = position
            self._check_invariant_a(position.symbol)
            
            # Clear any pending reversal (by normalized key)
            target_norm = _normalize_symbol(position.symbol)
            to_remove = [s for s in self._pending_reversals if _normalize_symbol(s) == target_norm]
            for s in to_remove:
                del self._pending_reversals[s]
            
            logger.info(
                "Position registered",
                symbol=position.symbol,
                side=position.side.value,
                state=position.state.value,
                position_id=position.position_id
            )
    
    # ========== REVERSAL HANDLING (Invariant E) ==========
    
    def request_reversal(self, symbol: str, new_side: Side) -> bool:
        """
        Request to close position for direction reversal.
        
        This blocks new opens until close is confirmed.
        """
        with self._lock:
            pos = self.get_position(symbol)
            if pos is None:
                return False
            
            if pos.side == new_side:
                return False  # Not a reversal
            
            self._pending_reversals[symbol] = new_side
            logger.info(
                "Reversal requested",
                symbol=symbol,
                from_side=pos.side.value,
                to_side=new_side.value
            )
            return True
    
    def confirm_reversal_closed(self, symbol: str) -> Optional[Side]:
        """
        Confirm reversal close complete. Returns the new side to open.
        """
        with self._lock:
            pos = self.get_position(symbol)
            if pos is not None and not pos.is_terminal:
                return None  # Not yet closed
            
            new_side = self._pending_reversals.pop(symbol, None)
            if new_side:
                logger.info(f"Reversal confirmed for {symbol}, can now open {new_side.value}")
            return new_side
    
    # ========== ORDER EVENT DISPATCH ==========
    
    def apply_order_event(self, symbol: str, event: OrderEvent) -> bool:
        """
        Apply order event to position. Idempotent.
        """
        with self._lock:
            pos = self._positions.get(symbol)
            if pos is None:
                logger.warning(f"Order event for unknown position: {symbol}")
                return False
            
            return pos.apply_order_event(event)
    
    # ========== RECONCILIATION ==========
    
    def reconcile_with_exchange(
        self,
        exchange_positions: Dict[str, Dict],  # symbol -> {side, qty, entry_price}
        exchange_orders: List[Dict]  # [{order_id, symbol, side, status, ...}]
    ) -> List[Tuple[str, str]]:
        """
        Reconcile registry with exchange state.
        
        Uses symbol normalization to match positions across different formats:
        - Registry may have: ADA/USD, ADA/USD:USD, PF_ADAUSD
        - Exchange returns: PF_ADAUSD
        
        Returns:
            List of (symbol, issue) tuples for positions needing attention
        """
        from src.data.symbol_utils import normalize_symbol_for_position_match
        
        issues = []
        qty_epsilon = Decimal("0.0001")
        
        orphaned_symbols: list[str] = []
        
        # Build normalized lookup for exchange positions: normalized_key -> (original_symbol, pos_data)
        exchange_normalized: Dict[str, tuple[str, Dict]] = {}
        for ex_symbol, ex_pos in exchange_positions.items():
            norm_key = normalize_symbol_for_position_match(ex_symbol)
            exchange_normalized[norm_key] = (ex_symbol, ex_pos)
        
        # Update known exchange symbols for the duplicate-entry guard.
        # This is the defense-in-depth: even if the registry loses a position,
        # can_open_position() will still block new entries for symbols with
        # live exchange exposure.
        self._known_exchange_symbols = set(exchange_normalized.keys())
        
        # Track which normalized exchange positions we've matched (for phantom detection)
        matched_exchange_keys: set[str] = set()
        
        with self._lock:
            # Check for orphaned positions (registry has, exchange doesn't)
            for symbol, pos in self._positions.items():
                symbol_norm = normalize_symbol_for_position_match(symbol)
                if pos.is_terminal:
                    self._orphan_miss_counts.pop(symbol_norm, None)
                    continue
                
                # Try exact match first, then normalized match
                exchange_pos = exchange_positions.get(symbol)
                matched_key = None
                
                if exchange_pos is None:
                    # Try normalized matching
                    norm_key = normalize_symbol_for_position_match(symbol)
                    if norm_key in exchange_normalized:
                        matched_key = norm_key
                        _, exchange_pos = exchange_normalized[norm_key]
                else:
                    matched_key = normalize_symbol_for_position_match(symbol)
                
                if matched_key:
                    matched_exchange_keys.add(matched_key)
                
                if exchange_pos is None and pos.remaining_qty > 0:
                    now = datetime.now(timezone.utc)
                    age_since_update = (
                        max(0.0, (now - pos.updated_at).total_seconds())
                        if pos.updated_at is not None
                        else 999999.0
                    )
                    # Avoid false orphaning while close/replace events are still settling.
                    if (
                        pos.state in (PositionState.EXIT_PENDING, PositionState.CANCEL_PENDING)
                        and age_since_update <= self._orphan_exit_pending_grace_seconds
                    ):
                        self._orphan_miss_counts[symbol_norm] = 0
                        issues.append(
                            (
                                symbol,
                                f"MISSING_ON_EXCHANGE_EXIT_GRACE: age={age_since_update:.1f}s",
                            )
                        )
                        logger.info(
                            "Orphan grace active (exit pending)",
                            symbol=symbol,
                            state=pos.state.value,
                            age_seconds=round(age_since_update, 3),
                            grace_seconds=self._orphan_exit_pending_grace_seconds,
                            remaining_qty=str(pos.remaining_qty),
                        )
                        continue
                    miss_count = self._orphan_miss_counts.get(symbol_norm, 0) + 1
                    self._orphan_miss_counts[symbol_norm] = miss_count
                    if miss_count < self._orphan_miss_threshold:
                        issues.append(
                            (
                                symbol,
                                f"MISSING_ON_EXCHANGE_PENDING: miss_count={miss_count}/{self._orphan_miss_threshold}",
                            )
                        )
                        logger.warning(
                            "Orphan hysteresis pending",
                            symbol=symbol,
                            miss_count=miss_count,
                            threshold=self._orphan_miss_threshold,
                            remaining_qty=str(pos.remaining_qty),
                        )
                        continue
                    # If we are already in an explicit exit flow and the exchange still
                    # reports no position after grace + hysteresis, treat as confirmed close
                    # instead of escalating to ORPHANED.
                    if pos.state in (PositionState.EXIT_PENDING, PositionState.CANCEL_PENDING):
                        pos._mark_closed(ExitReason.RECONCILIATION)
                        orphaned_symbols.append(symbol)
                        issues.append(
                            (
                                symbol,
                                "CLOSED_ON_EXCHANGE_MISSING_AFTER_EXIT: "
                                f"miss_count={miss_count}",
                            )
                        )
                        logger.info(
                            "Exchange close confirmed after exit pending hysteresis",
                            symbol=symbol,
                            miss_count=miss_count,
                            state=pos.state.value,
                        )
                        continue
                    pos.mark_orphaned()
                    orphaned_symbols.append(symbol)
                    issues.append((symbol, "ORPHANED: Registry has position, exchange does not"))
                elif exchange_pos is None and pos.remaining_qty <= 0:
                    self._orphan_miss_counts.pop(symbol_norm, None)
                    # Position has no remaining qty and exchange has nothing - mark as closed
                    pos._mark_closed(ExitReason.RECONCILIATION)
                    orphaned_symbols.append(symbol)
                    issues.append((symbol, "STALE: Registry has empty position, marking closed"))
                elif exchange_pos is not None:
                    self._orphan_miss_counts.pop(symbol_norm, None)
                    # Verify qty matches
                    exchange_qty = Decimal(str(exchange_pos.get('qty', 0)))
                    if pos.remaining_qty <= qty_epsilon and exchange_qty > qty_epsilon:
                        # Exchange has quantity but registry shows zero.
                        # CRITICAL: If the position is PENDING, this is a race condition —
                        # a market order was filled on the exchange before the fill event
                        # reached the state machine. We MUST adopt the exchange qty into
                        # this position, NOT archive it. Archiving a PENDING position
                        # causes the system to lose track of the position and re-enter
                        # the same symbol every cycle, compounding exposure until halt.
                        if pos.state == PositionState.PENDING:
                            # Race condition: market order filled before fill event processed.
                            # Adopt exchange quantity into this position via synthetic fill.
                            exchange_entry_price_raw = exchange_pos.get("entry_price")
                            ref_price = pos.initial_entry_price
                            if exchange_entry_price_raw is not None:
                                try:
                                    ref_price = Decimal(str(exchange_entry_price_raw))
                                except (ValueError, TypeError, ArithmeticError):
                                    pass
                            now = datetime.now(timezone.utc)
                            fill = FillRecord(
                                fill_id=f"sync-adopt-{int(now.timestamp() * 1000)}-{len(pos.entry_fills) + 1}",
                                order_id=pos.entry_order_id or "sync-adopted",
                                side=pos.side,
                                qty=exchange_qty,
                                price=ref_price,
                                timestamp=now,
                                is_entry=True,
                            )
                            pos.entry_fills.append(fill)
                            pos._update_state_after_entry_fill()
                            pos.updated_at = datetime.now(timezone.utc)
                            logger.warning(
                                "PENDING position adopted exchange qty (race condition fix)",
                                symbol=symbol,
                                adopted_qty=str(exchange_qty),
                                entry_price=str(ref_price),
                                new_state=pos.state.value,
                            )
                            issues.append((symbol, f"PENDING_ADOPTED: Registry adopted {exchange_qty} from exchange (was PENDING)"))
                        else:
                            # Non-PENDING zero-qty position: truly stale from prior restart.
                            # Close and archive so startup import can adopt the live position.
                            pos._mark_closed(ExitReason.RECONCILIATION)
                            orphaned_symbols.append(symbol)
                            issues.append((symbol, f"STALE_ZERO_QTY: Registry {pos.remaining_qty} vs Exchange {exchange_qty}"))
                            logger.info(
                                "Stale zero-qty position archived",
                                symbol=symbol,
                                state=pos.state.value,
                                exchange_qty=str(exchange_qty),
                            )
                    elif abs(exchange_qty - pos.remaining_qty) > qty_epsilon:
                        exchange_entry_price_raw = exchange_pos.get("entry_price")
                        exchange_entry_price: Optional[Decimal] = None
                        if exchange_entry_price_raw is not None:
                            try:
                                exchange_entry_price = Decimal(str(exchange_entry_price_raw))
                            except (ValueError, TypeError, ArithmeticError):
                                exchange_entry_price = None

                        sync_summary = pos.reconcile_quantity_to_exchange(
                            exchange_qty=exchange_qty,
                            exchange_entry_price=exchange_entry_price,
                            qty_epsilon=qty_epsilon,
                        )
                        if sync_summary:
                            issues.append((symbol, f"QTY_SYNCED: {sync_summary}"))
                            if pos.state == PositionState.CLOSED:
                                orphaned_symbols.append(symbol)
                        else:
                            issues.append((symbol, f"QTY_MISMATCH: Registry {pos.remaining_qty} vs Exchange {exchange_qty}"))
            
            # Move orphaned positions to closed history (so they don't reappear)
            for symbol in orphaned_symbols:
                self._orphan_miss_counts.pop(normalize_symbol_for_position_match(symbol), None)
                pos = self._positions.pop(symbol, None)
                if pos:
                    self._closed_positions.append(pos)
                    logger.info("Orphaned position moved to closed history", symbol=symbol)
            
            # Check for phantom positions (exchange has, registry doesn't)
            # Use normalized keys to avoid false positives from format differences
            registry_normalized = {
                normalize_symbol_for_position_match(s): s 
                for s, p in self._positions.items() 
                if not p.is_terminal
            }
            
            for ex_symbol, exchange_pos in exchange_positions.items():
                norm_key = normalize_symbol_for_position_match(ex_symbol)
                if norm_key not in matched_exchange_keys and norm_key not in registry_normalized:
                    issues.append((ex_symbol, "PHANTOM: Exchange has position, registry does not"))
        
        return issues
    
    # ========== HISTORY ==========
    
    def get_closed_history(self, limit: int = 100) -> List[ManagedPosition]:
        """Get closed position history."""
        with self._lock:
            return self._closed_positions[-limit:]
    
    def cleanup_stale(self, max_age_hours: int = 24) -> int:
        """Remove very old closed positions from memory."""
        with self._lock:
            cutoff = datetime.now(timezone.utc)
            original_count = len(self._closed_positions)
            self._closed_positions = [
                p for p in self._closed_positions
                if p.exit_time and (cutoff - p.exit_time).total_seconds() < max_age_hours * 3600
            ]
            removed = original_count - len(self._closed_positions)
            if removed > 0:
                logger.info(f"Cleaned up {removed} stale closed positions")
            return removed
    
    # ========== PERSISTENCE ==========
    
    def to_dict(self) -> Dict:
        """Serialize registry for persistence."""
        with self._lock:
            return {
                "positions": {s: p.to_dict() for s, p in self._positions.items()},
                "pending_reversals": {s: side.value for s, side in self._pending_reversals.items()},
                "closed_positions": [p.to_dict() for p in self._closed_positions[-100:]]  # Keep last 100
            }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "PositionRegistry":
        """Deserialize registry from persistence."""
        registry = cls()
        
        for symbol, pos_data in data.get("positions", {}).items():
            registry._positions[symbol] = ManagedPosition.from_dict(pos_data)
        
        for symbol, side_str in data.get("pending_reversals", {}).items():
            registry._pending_reversals[symbol] = Side(side_str)
        
        for pos_data in data.get("closed_positions", []):
            registry._closed_positions.append(ManagedPosition.from_dict(pos_data))
        
        return registry


# ============ SINGLETON REGISTRY ============
_registry_instance: Optional[PositionRegistry] = None
_registry_lock = threading.Lock()


def get_position_registry() -> PositionRegistry:
    """Get the singleton PositionRegistry instance."""
    global _registry_instance
    if _registry_instance is None:
        with _registry_lock:
            if _registry_instance is None:
                _registry_instance = PositionRegistry()
    return _registry_instance


def reset_position_registry() -> None:
    """Reset the registry (for testing only)."""
    global _registry_instance
    with _registry_lock:
        _registry_instance = None


def set_position_registry(registry: PositionRegistry) -> None:
    """Set custom registry (for crash recovery)."""
    global _registry_instance
    with _registry_lock:
        _registry_instance = registry
