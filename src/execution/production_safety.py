"""
Production Safety Module.

Contains critical safety mechanisms that prevent real-world disasters:
1. Atomic stop replace (new-first, then cancel old)
2. EXIT_PENDING timeout + escalation
3. Event ordering constraints
4. Write-ahead persistence for intents
5. Invariant K: Always protected after first fill
"""
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Set
from enum import Enum
import asyncio

from src.execution.position_state_machine import (
    ManagedPosition,
    PositionState,
    check_invariant,
    InvariantViolation
)
from src.data.symbol_utils import position_symbol_matches_order
from src.domain.models import Side
from src.monitoring.logger import get_logger
from src.exceptions import OperationalError, DataError, InvariantError

logger = get_logger(__name__)


def _symbol_variants(position) -> List[str]:
    """
    Return a list of symbol format variants to try when fetching orders.
    Handles the PF_XXXUSD / XXX/USD / XXX/USD:USD mismatch.
    """
    variants = []
    sym = position.symbol
    fs = getattr(position, "futures_symbol", None)
    if fs and fs != sym:
        variants.append(fs)
    variants.append(sym)
    # If sym is like "TON/USD", also try "TON/USD:USD" and "PF_TONUSD"
    if "/" in sym and ":USD" not in sym:
        variants.append(sym + ":USD")
    base = sym.replace("/USD", "").replace("/", "").replace(":USD", "").upper()
    pf = f"PF_{base}USD"
    if pf not in variants:
        variants.append(pf)
    return variants


# ============ STOP ORDER STATUS CLASSIFICATION ============
# Kraken Futures stop orders go through:  untouched → entered_book → filled
# CCXT may or may not normalize these.  We classify explicitly to avoid
# false NAKED POSITION / kill-switch triggers on transitional states.

ALIVE_STOP_STATUSES = frozenset({
    "open",              # CCXT-normalized "active"
    "entered_book",      # Kraken Futures: triggered, resting on order book
    "untouched",         # Kraken Futures: conditional not yet triggered
    "new",               # Some exchanges: just placed
    "partiallyfilled",   # Partially filled, still working
    "partial",           # CCXT alias for partially filled
})

DEAD_STOP_STATUSES = frozenset({
    "canceled", "cancelled", "expired", "rejected",
})

FINAL_STOP_STATUSES = frozenset({
    "closed", "filled",  # Terminal — check filled qty to decide
})


# ============ CONFIGURATION ============

@dataclass
class SafetyConfig:
    """Safety mechanism configuration."""
    
    # Exit timeout settings
    exit_pending_timeout_seconds: int = 60
    exit_escalation_max_retries: int = 3
    exit_aggressive_after_seconds: int = 30
    
    # Stop replace settings
    stop_replace_ack_timeout_seconds: int = 10
    
    # Protection settings
    emergency_exit_on_stop_fail: bool = True
    # Grace window to avoid false "naked" alerts while stops are still attaching/visible
    stop_pending_grace_seconds: int = 90
    # Short grace specifically for freshly reconciled/adopted positions before
    # stop binding catches up in the same cycle.
    reconcile_fresh_grace_seconds: int = 3
    
    # Event ordering
    reject_stale_events: bool = True


# ============ EXIT ESCALATION STATES ============

class ExitEscalationLevel(str, Enum):
    """Levels of exit urgency."""
    NORMAL = "normal"           # Standard limit/market exit
    AGGRESSIVE = "aggressive"   # Wider price tolerance, IOC
    EMERGENCY = "emergency"     # Market + cancel all, flatten
    QUARANTINE = "quarantine"   # Give up, quarantine symbol


@dataclass
class ExitEscalationState:
    """Tracks exit escalation for a position."""
    position_id: str
    symbol: str
    started_at: datetime
    current_level: ExitEscalationLevel = ExitEscalationLevel.NORMAL
    retry_count: int = 0
    last_attempt_at: Optional[datetime] = None
    last_error: Optional[str] = None
    
    def should_escalate(self, config: SafetyConfig) -> bool:
        """Check if we should escalate to next level."""
        if self.current_level == ExitEscalationLevel.QUARANTINE:
            return False
        
        elapsed = (datetime.now(timezone.utc) - self.started_at).total_seconds()
        
        if elapsed > config.exit_pending_timeout_seconds:
            return True
        
        if self.retry_count >= config.exit_escalation_max_retries:
            return True
        
        return False
    
    def escalate(self) -> ExitEscalationLevel:
        """Move to next escalation level."""
        self.retry_count += 1
        
        if self.current_level == ExitEscalationLevel.NORMAL:
            self.current_level = ExitEscalationLevel.AGGRESSIVE
        elif self.current_level == ExitEscalationLevel.AGGRESSIVE:
            self.current_level = ExitEscalationLevel.EMERGENCY
        elif self.current_level == ExitEscalationLevel.EMERGENCY:
            self.current_level = ExitEscalationLevel.QUARANTINE
        
        logger.warning(
            f"Exit escalated to {self.current_level.value}",
            symbol=self.symbol,
            retry_count=self.retry_count
        )
        
        return self.current_level


# ============ ATOMIC STOP REPLACE ============

@dataclass
class StopReplaceContext:
    """Context for atomic stop replace operation."""
    position_id: str
    symbol: str
    old_stop_order_id: Optional[str]
    old_stop_price: Decimal
    new_stop_price: Decimal
    new_stop_order_id: Optional[str] = None
    new_stop_acked: bool = False
    old_stop_cancelled: bool = False
    failed: bool = False
    error: Optional[str] = None


class AtomicStopReplacer:
    """
    Ensures stop replacement is atomic (or fails safely).
    
    Protocol:
    1. Place NEW stop first
    2. Wait for ACK on new stop
    3. Only THEN cancel old stop
    4. If step 1 fails → keep old stop, do not advance state
    """
    
    def __init__(self, exchange_client, config: SafetyConfig):
        self.client = exchange_client
        self.config = config
        self._pending_replaces: Dict[str, StopReplaceContext] = {}
    
    async def replace_stop(
        self,
        position: ManagedPosition,
        new_stop_price: Decimal,
        generate_client_order_id: callable
    ) -> StopReplaceContext:
        """
        Execute atomic stop replacement.
        
        Returns context with success/failure details.
        """
        ctx = StopReplaceContext(
            position_id=position.position_id,
            symbol=position.symbol,
            old_stop_order_id=position.stop_order_id,
            old_stop_price=position.current_stop_price or position.initial_stop_price,
            new_stop_price=new_stop_price
        )
        
        self._pending_replaces[position.symbol] = ctx
        
        try:
            # Step 1: Place NEW stop first (reduce_only=True: protective exit, no dust)
            new_client_order_id = generate_client_order_id(position.position_id, "stop")
            stop_side = "sell" if position.side.value == "long" else "buy"
            exchange_symbol = getattr(position, "futures_symbol", None) or position.symbol
            
            result = await self.client.place_futures_order(
                symbol=exchange_symbol,
                side=stop_side,
                order_type="stop",
                size=position.remaining_qty,
                stop_price=new_stop_price,
                reduce_only=True,
                client_order_id=new_client_order_id,
            )
            
            ctx.new_stop_order_id = result.get("id")
            
            # Step 2: Wait for ACK (with timeout)
            ack_deadline = datetime.now(timezone.utc) + timedelta(
                seconds=self.config.stop_replace_ack_timeout_seconds
            )
            
            while datetime.now(timezone.utc) < ack_deadline:
                # Check if order is acknowledged/live
                # Check if order is acknowledged/live by looking at open orders
                try:
                    open_orders = await self.client.get_futures_open_orders()
                    if any(o.get("id") == ctx.new_stop_order_id for o in open_orders):
                        ctx.new_stop_acked = True
                        break
                except OperationalError as e:
                    # Transient API error — retry on next poll iteration
                    logger.debug("Stop ack poll: transient error, retrying", error=str(e))
                
                await asyncio.sleep(0.5)
            
            if not ctx.new_stop_acked:
                # New stop not acknowledged - keep old stop
                ctx.failed = True
                ctx.error = "New stop not acknowledged in time - keeping old stop"
                logger.error(ctx.error, symbol=position.symbol)
                
                # Try to cancel the new stop we placed
                try:
                    await self.client.cancel_futures_order(ctx.new_stop_order_id, exchange_symbol)
                except OperationalError as e:
                    logger.warning(
                        "Failed to cancel unacked new stop (may need manual cleanup)",
                        symbol=position.symbol,
                        new_stop_id=ctx.new_stop_order_id,
                        error=str(e),
                    )
                
                return ctx
            
            # Step 3: NOW cancel old stop (only after new is confirmed)
            if ctx.old_stop_order_id:
                try:
                    await self.client.cancel_futures_order(ctx.old_stop_order_id, exchange_symbol)
                    ctx.old_stop_cancelled = True
                except (OperationalError, DataError) as e:
                    # Old stop might have already triggered — that's OK
                    logger.warning(
                        "Old stop cancel failed (may have triggered)",
                        symbol=position.symbol,
                        old_stop_id=ctx.old_stop_order_id,
                        error=str(e),
                        error_type=type(e).__name__,
                    )
                    ctx.old_stop_cancelled = True  # Treat as success
            
            logger.info(
                "Atomic stop replace complete",
                symbol=position.symbol,
                old_price=str(ctx.old_stop_price),
                new_price=str(ctx.new_stop_price)
            )
            
            return ctx
            
        except InvariantError:
            raise  # Safety violation — must propagate
        except (OperationalError, DataError) as e:
            ctx.failed = True
            ctx.error = str(e)
            logger.error(
                "Atomic stop replace failed - KEEPING OLD STOP",
                symbol=position.symbol,
                error=str(e),
                error_type=type(e).__name__,
            )
            return ctx
        
        finally:
            del self._pending_replaces[position.symbol]


# ============ INVARIANT K: ALWAYS PROTECTED AFTER FIRST FILL ============

class ProtectionEnforcer:
    """
    Enforces Invariant K: Always protected after first fill.
    
    If filled_qty > 0 and position not closed:
        There MUST be a valid protective stop on exchange
        OR a guaranteed immediate market exit fallback
    """
    
    def __init__(self, exchange_client, config: SafetyConfig):
        self.client = exchange_client
        self.config = config
        self._protection_failures: Dict[str, int] = {}
    
    async def verify_protection(
        self,
        position: ManagedPosition,
        exchange_orders: List[Dict],
        *,
        log_violation: bool = True,
    ) -> bool:
        """
        Verify position has protective stop.
        
        Returns True if protected, False if naked.
        """
        if position.remaining_qty <= 0:
            return True  # No exposure, no protection needed
        
        if position.is_terminal:
            return True
        
        # Check for valid stop order on exchange
        has_stop = False
        for order in exchange_orders:
            order_symbol = str(order.get("symbol") or "")
            if not position_symbol_matches_order(position.symbol, order_symbol):
                continue

            # Check order type - look in both top-level 'type' and 'info.orderType' (Kraken puts it there)
            info = order.get("info") or {}
            otype = str(
                order.get("type") 
                or info.get("orderType") 
                or info.get("type") 
                or ""
            ).lower()
            
            # Use substring matching to catch variations (stop, stop_market, stp, stop-loss, etc.)
            is_stop_type = any(
                stop_term in otype 
                for stop_term in ('stop', 'stop-loss', 'stop_loss', 'stp')
            )
            # Exclude take-profit orders that might contain 'stop' substring
            if 'take_profit' in otype or 'take-profit' in otype:
                is_stop_type = False
            
            if not is_stop_type:
                continue

            # Defensive: if reduce-only is explicitly present and false, it's not protective.
            reduce_only_present = any(k in order for k in ("reduceOnly", "reduce_only"))
            reduce_only = bool(order.get("reduceOnly") or order.get("reduce_only") or False)
            if reduce_only_present and not reduce_only:
                continue

            order_status = str(order.get("status") or "").lower()
            if order_status not in ALIVE_STOP_STATUSES:
                continue

            has_stop = True
            break
        
        if not has_stop and log_violation:
            logger.critical(
                "INVARIANT K VIOLATION: Position has exposure but NO STOP!",
                symbol=position.symbol,
                remaining_qty=str(position.remaining_qty),
                expected_stop_id=getattr(position, "stop_order_id", None),
            )
        
        return has_stop
    
    async def emergency_exit_naked_position(
        self,
        position: ManagedPosition
    ) -> bool:
        """
        Emergency market exit for naked position.
        
        Called when stop placement fails after entry fill.
        """
        logger.critical(
            "EMERGENCY EXIT: Stop placement failed, exiting at market",
            symbol=position.symbol,
            qty=str(position.remaining_qty)
        )
        
        try:
            # reduce_only=True: emergency close, no dust
            close_side = "sell" if position.side.value == "long" else "buy"
            exchange_symbol = getattr(position, "futures_symbol", None) or position.symbol
            await self.client.place_futures_order(
                symbol=exchange_symbol,
                side=close_side,
                order_type="market",
                size=position.remaining_qty,
                reduce_only=True,
            )
            
            # Mark position as closed with emergency reason
            position.state = PositionState.CLOSED
            
            self._protection_failures[position.symbol] = \
                self._protection_failures.get(position.symbol, 0) + 1
            
            return True
            
        except InvariantError:
            raise  # Safety violation — propagate
        except (OperationalError, DataError) as e:
            logger.critical(
                "EMERGENCY EXIT FAILED - MANUAL INTERVENTION REQUIRED",
                symbol=position.symbol,
                error=str(e),
                error_type=type(e).__name__,
            )
            position.mark_orphaned()
            return False


# ============ EVENT ORDERING ENFORCER ============

class EventOrderingEnforcer:
    """
    Enforces event ordering constraints.
    
    - Maintains per-order last_event_seq
    - Rejects/queues older events
    - De-duplicates by fill_id (primary)
    """
    
    def __init__(self):
        # order_id -> last processed event_seq
        self._last_event_seq: Dict[str, int] = {}
        # fill_id -> True (for deduplication)
        self._processed_fill_ids: Set[str] = set()
    
    def should_process_event(
        self,
        order_id: str,
        event_seq: int,
        fill_id: Optional[str] = None
    ) -> bool:
        """
        Check if event should be processed.
        
        Returns False if:
        - Event is older than last processed for this order
        - Fill_id was already processed (duplicate fill)
        """
        # Fill ID deduplication (primary for fills)
        if fill_id and fill_id in self._processed_fill_ids:
            logger.debug(f"Duplicate fill_id ignored: {fill_id}")
            return False
        
        # Event sequence check
        last_seq = self._last_event_seq.get(order_id, 0)
        if event_seq <= last_seq:
            logger.debug(
                f"Stale event ignored: order={order_id}, seq={event_seq}, last={last_seq}"
            )
            return False
        
        return True
    
    def mark_processed(
        self,
        order_id: str,
        event_seq: int,
        fill_id: Optional[str] = None
    ) -> None:
        """Mark event as processed."""
        self._last_event_seq[order_id] = max(
            self._last_event_seq.get(order_id, 0),
            event_seq
        )
        
        if fill_id:
            self._processed_fill_ids.add(fill_id)
    
    def cleanup_old_orders(self, active_order_ids: Set[str]) -> None:
        """Clean up tracking for orders no longer active."""
        stale_orders = set(self._last_event_seq.keys()) - active_order_ids
        for order_id in stale_orders:
            del self._last_event_seq[order_id]


# ============ WRITE-AHEAD INTENT PERSISTENCE ============

class ActionIntentStatus(str, Enum):
    """Status of a persisted action intent."""
    PENDING = "pending"      # Intent recorded, not yet sent
    SENT = "sent"            # Sent to exchange, awaiting response
    ACKNOWLEDGED = "acknowledged"  # Exchange acknowledged
    COMPLETED = "completed"  # Fully processed
    FAILED = "failed"        # Failed, needs reconciliation


@dataclass
class ActionIntent:
    """
    Write-ahead intent for an action.
    
    Persisted BEFORE sending to exchange to prevent
    duplicate orders on crash/restart.
    """
    intent_id: str  # Same as client_order_id
    position_id: str
    action_type: str
    symbol: str
    side: str
    size: str  # Stored as string for persistence
    price: Optional[str]
    created_at: datetime
    status: ActionIntentStatus = ActionIntentStatus.PENDING
    exchange_order_id: Optional[str] = None
    error: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return {
            "intent_id": self.intent_id,
            "position_id": self.position_id,
            "action_type": self.action_type,
            "symbol": self.symbol,
            "side": self.side,
            "size": self.size,
            "price": self.price,
            "created_at": self.created_at.isoformat(),
            "status": self.status.value,
            "exchange_order_id": self.exchange_order_id,
            "error": self.error
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "ActionIntent":
        return cls(
            intent_id=data["intent_id"],
            position_id=data["position_id"],
            action_type=data["action_type"],
            symbol=data["symbol"],
            side=data["side"],
            size=data["size"],
            price=data.get("price"),
            created_at=datetime.fromisoformat(data["created_at"]),
            status=ActionIntentStatus(data["status"]),
            exchange_order_id=data.get("exchange_order_id"),
            error=data.get("error")
        )


class WriteAheadIntentLog:
    """
    Write-ahead log for action intents.
    
    Prevents duplicate orders on crash/restart by:
    1. Persisting intent BEFORE sending to exchange
    2. On restart, checking for pending intents and reconciling
    """
    
    def __init__(self, persistence):
        self.persistence = persistence
        self._pending_intents: Dict[str, ActionIntent] = {}
    
    def record_intent(self, intent: ActionIntent) -> None:
        """
        Record intent BEFORE executing.
        
        This is the WAL - if we crash after this but before
        exchange call, we'll detect on restart.
        """
        self._pending_intents[intent.intent_id] = intent
        
        # Persist to database
        self.persistence._conn.execute("""
            INSERT OR REPLACE INTO action_intents (
                intent_id, position_id, action_type, symbol, side, size, price,
                created_at, status, exchange_order_id, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            intent.intent_id,
            intent.position_id,
            intent.action_type,
            intent.symbol,
            intent.side,
            intent.size,
            intent.price,
            intent.created_at.isoformat(),
            intent.status.value,
            intent.exchange_order_id,
            intent.error
        ))
        self.persistence._conn.commit()
    
    def mark_sent(self, intent_id: str, exchange_order_id: str) -> None:
        """Mark intent as sent to exchange."""
        if intent_id in self._pending_intents:
            self._pending_intents[intent_id].status = ActionIntentStatus.SENT
            self._pending_intents[intent_id].exchange_order_id = exchange_order_id
            self._update_intent_status(intent_id, ActionIntentStatus.SENT, exchange_order_id)
    
    def mark_completed(self, intent_id: str) -> None:
        """Mark intent as completed."""
        if intent_id in self._pending_intents:
            del self._pending_intents[intent_id]
        self._update_intent_status(intent_id, ActionIntentStatus.COMPLETED)
    
    def mark_failed(self, intent_id: str, error: str) -> None:
        """Mark intent as failed."""
        if intent_id in self._pending_intents:
            self._pending_intents[intent_id].status = ActionIntentStatus.FAILED
            self._pending_intents[intent_id].error = error
        self._update_intent_status(intent_id, ActionIntentStatus.FAILED, error=error)
    
    def _update_intent_status(
        self,
        intent_id: str,
        status: ActionIntentStatus,
        exchange_order_id: Optional[str] = None,
        error: Optional[str] = None
    ) -> None:
        """Update intent status in persistence."""
        self.persistence._conn.execute("""
            UPDATE action_intents 
            SET status = ?, exchange_order_id = COALESCE(?, exchange_order_id), error = ?
            WHERE intent_id = ?
        """, (status.value, exchange_order_id, error, intent_id))
        self.persistence._conn.commit()
    
    def get_pending_intents(self) -> List[ActionIntent]:
        """Get all pending/sent intents for reconciliation."""
        cursor = self.persistence._conn.execute("""
            SELECT * FROM action_intents 
            WHERE status IN ('pending', 'sent')
            ORDER BY created_at
        """)
        
        intents = []
        for row in cursor:
            intents.append(ActionIntent(
                intent_id=row["intent_id"],
                position_id=row["position_id"],
                action_type=row["action_type"],
                symbol=row["symbol"],
                side=row["side"],
                size=row["size"],
                price=row["price"],
                created_at=datetime.fromisoformat(row["created_at"]),
                status=ActionIntentStatus(row["status"]),
                exchange_order_id=row["exchange_order_id"],
                error=row["error"]
            ))
        
        return intents
    
    async def reconcile_on_startup(
        self,
        exchange_client,
        registry
    ) -> Dict[str, str]:
        """
        Reconcile pending intents on startup.
        
        For each pending intent:
        - If exchange_order_id exists → check if order exists on exchange
        - If order exists → mark completed
        - If order doesn't exist and status=PENDING → re-evaluate (may need to retry)
        - If order doesn't exist and status=SENT → lost in limbo, reconcile
        
        Returns dict of {intent_id: resolution}
        """
        resolutions = {}
        pending = self.get_pending_intents()
        
        for intent in pending:
            if intent.status == ActionIntentStatus.PENDING:
                # Never sent - can safely ignore or retry
                logger.warning(
                    f"Found unsent intent on startup: {intent.intent_id}",
                    action_type=intent.action_type,
                    symbol=intent.symbol
                )
                self.mark_failed(intent.intent_id, "Startup: intent never sent")
                resolutions[intent.intent_id] = "cancelled_unsent"
                
            elif intent.status == ActionIntentStatus.SENT:
                # Was sent but we don't know outcome
                if intent.exchange_order_id:
                    # Cannot fetch single order, so we skip check or implement get_futures_order later
                    # For now, if we can't verify, we log warning.
                    logger.warning(f"Cannot verify intent {intent.intent_id} status (fetching single order not supported)")
                    resolutions[intent.intent_id] = "unknown"
                    continue
                else:
                    # Sent but no exchange_order_id - definitely lost
                    self.mark_failed(intent.intent_id, "No exchange_order_id")
                    resolutions[intent.intent_id] = "no_order_id"
        
        logger.info(
            "Intent reconciliation complete",
            total=len(pending),
            resolutions=resolutions
        )
        
        return resolutions


# ============ EXIT TIMEOUT MANAGER ============

class ExitTimeoutManager:
    """
    Manages EXIT_PENDING timeouts and escalation.
    
    If EXIT_PENDING lasts > timeout:
    - Escalate to aggressive exit
    - If retries exhausted → quarantine symbol
    """
    
    def __init__(self, config: SafetyConfig):
        self.config = config
        self._exit_states: Dict[str, ExitEscalationState] = {}
        self._quarantined_symbols: Set[str] = set()
    
    def start_exit_tracking(self, position: ManagedPosition) -> None:
        """Start tracking exit timeout for position."""
        if position.state == PositionState.EXIT_PENDING:
            self._exit_states[position.symbol] = ExitEscalationState(
                position_id=position.position_id,
                symbol=position.symbol,
                started_at=datetime.now(timezone.utc)
            )
    
    def is_quarantined(self, symbol: str) -> bool:
        """Check if symbol is quarantined."""
        return symbol in self._quarantined_symbols
    
    def check_timeouts(self) -> List[ExitEscalationState]:
        """
        Check all exit states for timeouts.
        
        Returns list of states that need escalation.
        """
        needs_escalation = []
        
        for symbol, state in self._exit_states.items():
            if state.should_escalate(self.config):
                needs_escalation.append(state)
        
        return needs_escalation
    
    def escalate(self, symbol: str) -> ExitEscalationLevel:
        """Escalate exit for symbol."""
        state = self._exit_states.get(symbol)
        if not state:
            return ExitEscalationLevel.NORMAL
        
        new_level = state.escalate()
        
        if new_level == ExitEscalationLevel.QUARANTINE:
            self._quarantined_symbols.add(symbol)
            logger.critical(
                f"SYMBOL QUARANTINED: {symbol}",
                reason="exit_timeout_exhausted"
            )
        
        return new_level
    
    def exit_completed(self, symbol: str) -> None:
        """Mark exit as completed, clear tracking."""
        if symbol in self._exit_states:
            del self._exit_states[symbol]
    
    def unquarantine(self, symbol: str) -> None:
        """Remove symbol from quarantine (manual intervention)."""
        self._quarantined_symbols.discard(symbol)
        logger.info(f"Symbol unquarantined: {symbol}")


# ============ POSITION PROTECTION MONITOR ============

class PositionProtectionMonitor:
    """
    Continuous monitoring for Invariant K.
    
    Runs periodically to verify all exposed positions have protection.
    When a stop loss fill is detected (expected behavior), the position
    is closed gracefully instead of triggering a false kill-switch.
    """
    
    def __init__(
        self,
        exchange_client,
        registry,
        protection_enforcer: ProtectionEnforcer,
        persistence=None,
    ):
        self.client = exchange_client
        self.registry = registry
        self.enforcer = protection_enforcer
        # Per-symbol semantic mismatch counters (reset daily via reset_semantic_counters)
        self._semantic_warning_counts: Dict[str, int] = {}
        self._semantic_error_counts: Dict[str, int] = {}
        self._reduceonly_missing_counts: Dict[str, int] = {}
        # Track which symbols have had a raw dump emitted (once per symbol per reset)
        self._raw_dump_emitted: Set[str] = set()
        self._counters_reset_at: datetime = datetime.now(timezone.utc)
        self.persistence = persistence
        self._running = False
        # Per-symbol snapshot of the most recent Layer 2 stop lookup outcome
        self._layer2_last_status: Dict[str, str] = {}

        # ── Layer 3 / self-heal counters ──
        # Readable from health_monitor to detect systemic instability.
        self.layer3_saves_total: int = 0

    async def check_all_positions(self) -> Dict[str, bool]:
        """
        Check all positions for protection.
        
        Multi-layer defense against false NAKED POSITION alerts:
        
        Layer 1: Verify position actually exists on exchange.
                 If exchange size = 0, position was closed (stop filled).
        Layer 2: If no stop order found but position exists, check whether
                 the known stop_order_id was recently filled (not just missing).
                 A filled stop is expected behavior, not a safety violation.
        Layer 3: Only flag as NAKED if the stop truly vanished without filling.
        
        Returns {symbol: is_protected}
        """
        from src.data.symbol_utils import normalize_symbol_for_position_match
        
        results = {}
        
        try:
            exchange_orders = await self.client.get_futures_open_orders()
        except InvariantError:
            raise  # Safety violation — propagate
        except (OperationalError, DataError) as e:
            logger.error(f"Failed to fetch orders for protection check: {e}", error_type=type(e).__name__)
            return results
        
        # Diagnostic: log what orders are visible so future incidents are 30-second diagnoses
        if exchange_orders:
            order_summary = [
                {
                    "id": str(o.get("id") or "")[:12],
                    "symbol": str(o.get("symbol") or ""),
                    "type": str(o.get("type") or (o.get("info") or {}).get("orderType") or ""),
                    "status": str(o.get("status") or ""),
                    "filled": o.get("filled"),
                    "amount": o.get("amount"),
                }
                for o in exchange_orders[:20]
            ]
            logger.debug(
                "Protection check: exchange orders visible",
                order_count=len(exchange_orders),
                orders=order_summary,
            )
        else:
            logger.debug("Protection check: no exchange orders visible")
        
        # CRITICAL: Fetch actual positions from exchange to verify they exist
        try:
            exchange_positions = await self.client.get_all_futures_positions()
        except InvariantError:
            raise  # Safety violation — propagate
        except (OperationalError, DataError) as e:
            logger.error(f"Failed to fetch positions for protection check: {e}", error_type=type(e).__name__)
            # If we can't verify positions, assume protected to avoid false positives
            return results
        
        # Build map of exchange positions by normalized symbol
        exchange_position_map = {}
        for pos in exchange_positions:
            pos_symbol = pos.get("symbol") or ""
            pos_size = abs(float(pos.get("contracts") or pos.get("size") or 0))
            if pos_size > 0:
                normalized = normalize_symbol_for_position_match(pos_symbol)
                exchange_position_map[normalized] = pos_size
        
        for position in self.registry.get_all_active():
            if position.remaining_qty > 0:
                now = datetime.now(timezone.utc)
                updated_age_seconds = max(0, int((now - position.updated_at).total_seconds()))
                # CRITICAL CHECK: Verify position actually exists on exchange
                normalized_sym = normalize_symbol_for_position_match(position.symbol)
                exchange_size = exchange_position_map.get(normalized_sym, 0)
                
                if exchange_size == 0:
                    # Position closed on exchange but registry not updated yet
                    # This is NOT a naked position - it's a closed position!
                    logger.info(
                        "Position closed on exchange (stop filled?), skipping protection check",
                        symbol=position.symbol,
                        registry_qty=str(position.remaining_qty),
                        exchange_qty=0
                    )
                    results[position.symbol] = True  # Treat as protected (closed)
                    continue

                # Fresh reconcile/adopt grace: avoid tripping safety while stop bind
                # runs in the same loop for newly updated positions.
                if (
                    updated_age_seconds <= self.enforcer.config.reconcile_fresh_grace_seconds
                    and not position.stop_order_id
                ):
                    logger.warning(
                        "INVARIANT_K_FRESH_RECONCILE_GRACE",
                        symbol=position.symbol,
                        qty=str(position.remaining_qty),
                        exchange_qty=exchange_size,
                        updated_age_seconds=updated_age_seconds,
                        grace_seconds=self.enforcer.config.reconcile_fresh_grace_seconds,
                    )
                    results[position.symbol] = True
                    continue
                
                layer1_protected = await self.enforcer.verify_protection(
                    position,
                    exchange_orders,
                    log_violation=False,
                )
                layer1_seen_orders_count = sum(
                    1
                    for o in exchange_orders
                    if position_symbol_matches_order(position.symbol, str(o.get("symbol") or ""))
                )
                
                is_protected = layer1_protected
                layer2_status = "not_run"
                layer3_attempted = False
                layer3_succeeded = False
                self._layer2_last_status[position.symbol] = "not_run"
                
                # ---- LAYER 2: Stop-fill verification before declaring naked ----
                # Check the specific order ID stored on the position.
                if not layer1_protected and position.stop_order_id:
                    layer2_protected = await self._check_stop_was_filled(
                        position, exchange_size
                    )
                    layer2_status = self._layer2_last_status.get(position.symbol, "unknown")
                    if layer2_protected:
                        logger.info(
                            "Protection check: Layer 1 missed stop but Layer 2 confirmed ALIVE by ID",
                            symbol=position.symbol,
                            stop_order_id=position.stop_order_id,
                        )
                    is_protected = layer2_protected
                
                # ---- LAYER 3: Broad stop search via recent orders ----
                # The position's stop_order_id may be stale (e.g. stop was replaced
                # by protection_ops but in-memory position wasn't updated).  Search
                # ALL recent orders for this symbol to find any alive stop we missed.
                if not is_protected:
                    layer3_attempted = True
                    layer3_protected = await self._check_any_stop_for_symbol(
                        position, exchange_orders
                    )
                    layer3_succeeded = layer3_protected
                    if layer3_protected:
                        logger.info(
                            "Protection check: Layer 3 found alive stop via broad search",
                            symbol=position.symbol,
                        )
                    is_protected = layer3_protected
                
                # ---- PENDING-GRACE OVERRIDE (reduces false-positive naked alerts) ----
                # Treat newly-updated/open positions as temporarily protected when stop
                # attach/visibility is still settling and Layer 2 is inconclusive.
                if not is_protected:
                    now = datetime.now(timezone.utc)
                    position_age_seconds = max(0, int((now - position.created_at).total_seconds()))
                    updated_age_seconds = max(0, int((now - position.updated_at).total_seconds()))
                    stop_order_id_present = bool(position.stop_order_id)
                    within_grace = updated_age_seconds <= self.enforcer.config.stop_pending_grace_seconds
                    layer2_ambiguous = layer2_status in {
                        "not_run",
                        "fetch_not_supported",
                        "no_order_data",
                    }
                    if within_grace and (not stop_order_id_present or layer2_ambiguous):
                        logger.warning(
                            "TEMP_PROTECTION_PENDING",
                            symbol=position.symbol,
                            qty=str(position.remaining_qty),
                            exchange_qty=exchange_size,
                            position_age_seconds=position_age_seconds,
                            updated_age_seconds=updated_age_seconds,
                            stop_order_id_present=stop_order_id_present,
                            stop_order_id=position.stop_order_id or "none",
                            layer1_seen_orders_count=layer1_seen_orders_count,
                            layer2_fetch_order_status=layer2_status,
                            self_heal_attempted=layer3_attempted,
                            self_heal_succeeded=layer3_succeeded,
                            grace_seconds=self.enforcer.config.stop_pending_grace_seconds,
                        )
                        results[position.symbol] = True
                        continue

                results[position.symbol] = is_protected
                
                if not is_protected:
                    # CRITICAL: Position is truly naked on exchange!
                    now = datetime.now(timezone.utc)
                    position_age_seconds = max(0, int((now - position.created_at).total_seconds()))
                    updated_age_seconds = max(0, int((now - position.updated_at).total_seconds()))
                    logger.critical(
                        "INVARIANT K VIOLATION: Position has exposure but NO STOP!",
                        symbol=position.symbol,
                        remaining_qty=str(position.remaining_qty),
                        exchange_qty=exchange_size,
                        expected_stop_id=position.stop_order_id or None,
                    )
                    logger.critical(
                        "NAKED POSITION DETECTED",
                        symbol=position.symbol,
                        qty=str(position.remaining_qty),
                        exchange_qty=exchange_size,
                        stop_order_id=position.stop_order_id or "none",
                        position_age_seconds=position_age_seconds,
                        updated_age_seconds=updated_age_seconds,
                        stop_order_id_present=bool(position.stop_order_id),
                        layer1_seen_orders_count=layer1_seen_orders_count,
                        layer2_fetch_order_status=layer2_status,
                        self_heal_attempted=layer3_attempted,
                        self_heal_succeeded=layer3_succeeded,
                    )
        
        return results

    async def _check_any_stop_for_symbol(
        self,
        position: ManagedPosition,
        exchange_orders: List[Dict],
    ) -> bool:
        """
        LAYER 3: Broad stop search with strict validation.

        The position's ``stop_order_id`` may be stale (e.g. stop was replaced
        by protection_ops / trailing-stop logic but the in-memory
        ``ManagedPosition`` was never updated).

        This layer re-scans ``exchange_orders`` looking for ANY alive stop
        order that matches the symbol.  To avoid binding to the wrong order,
        every candidate must pass ALL of:

          1. Symbol matches (normalized)
          2. reduceOnly == True
          3. Correct side (long position → sell stop, short → buy stop)
          4. Amount covers ≥ 25% of position remaining_qty
          5. Type is stop-like (has stopPrice/triggerPrice OR type contains stop/stp)
          6. NOT a take-profit order

        Returns True if a validated stop exists (alive or recently filled).
        """
        from src.data.symbol_utils import normalize_symbol_for_position_match

        pos_norm = normalize_symbol_for_position_match(position.symbol)
        expected_side = "sell" if position.side == Side.LONG else "buy"
        pos_qty = float(position.remaining_qty) if position.remaining_qty else 0.0
        min_coverage = pos_qty * 0.25  # Must cover at least 25%

        # 3a. Re-scan open orders with strict validation
        for order in exchange_orders:
            order_symbol = str(order.get("symbol") or "")
            if normalize_symbol_for_position_match(order_symbol) != pos_norm:
                continue

            info = order.get("info") or {}
            otype = str(
                order.get("type")
                or info.get("orderType")
                or info.get("type")
                or ""
            ).lower()

            # ── Check 6: Exclude take-profit orders ──
            if "take_profit" in otype or "take-profit" in otype:
                continue

            # ── Check 5: Must be stop-like ──
            has_stop_price = (
                order.get("stopPrice") is not None
                or order.get("triggerPrice") is not None
                or info.get("stopPrice") is not None
                or info.get("triggerPrice") is not None
            )
            is_stop_type = any(
                t in otype for t in ("stop", "stp", "stop_loss", "stop-loss")
            )
            if not (has_stop_price or is_stop_type):
                continue

            # ── Check 2: Must be reduceOnly ──
            reduce_only = (
                order.get("reduceOnly")
                or order.get("reduce_only")
                or info.get("reduceOnly")
                or info.get("reduce_only")
            )
            # If reduceOnly is explicitly False, skip.
            # If absent (None), allow — some exchanges don't populate it.
            reduce_only_present = any(
                k in order or k in info
                for k in ("reduceOnly", "reduce_only")
            )
            if reduce_only_present and not reduce_only:
                logger.debug(
                    "Layer 3: skipping non-reduceOnly stop candidate",
                    symbol=position.symbol,
                    order_id=str(order.get("id") or "")[:16],
                )
                continue

            # ── Check 3: Correct side ──
            order_side = str(order.get("side") or "").lower()
            if order_side and order_side != expected_side:
                continue

            # ── Check 4: Amount coverage ──
            order_amount = float(order.get("amount") or order.get("remaining") or 0)
            if order_amount < min_coverage and min_coverage > 0:
                logger.debug(
                    "Layer 3: stop candidate below 25%% coverage threshold",
                    symbol=position.symbol,
                    order_amount=order_amount,
                    position_qty=pos_qty,
                    min_coverage=min_coverage,
                )
                continue

            # ── All checks passed — this is a valid protective stop ──
            status = str(order.get("status") or "").lower()
            if status in ALIVE_STOP_STATUSES or status in FINAL_STOP_STATUSES:
                filled = float(order.get("filled") or 0)
                if status in ALIVE_STOP_STATUSES or filled > 0:
                    self.layer3_saves_total += 1
                    logger.info(
                        "Layer 3: found validated stop order via broad search",
                        symbol=position.symbol,
                        order_id=str(order.get("id") or "")[:16],
                        status=status,
                        filled=filled,
                        order_type=otype,
                        order_side=order_side,
                        order_amount=order_amount,
                        reduce_only=reduce_only,
                        layer3_saves_total=self.layer3_saves_total,
                    )
                    # Update stop_order_id so Layer 2 works next time.
                    # CRITICAL: Also persist to SQLite so the update survives
                    # restarts. Without persistence, a restart reverts to the
                    # stale ID and Layer 3 must re-discover every time.
                    new_id = order.get("id")
                    if new_id and status in ALIVE_STOP_STATUSES:
                        position.stop_order_id = str(new_id)
                        if self.persistence:
                            try:
                                self.persistence.save_position(position)
                            except (OperationalError, DataError, OSError) as persist_err:
                                logger.warning(
                                    "Layer 3: failed to persist stop_order_id update",
                                    symbol=position.symbol,
                                    error=str(persist_err),
                                    error_type=type(persist_err).__name__,
                                )
                        logger.info(
                            "Layer 3: updated stale stop_order_id",
                            symbol=position.symbol,
                            new_stop_order_id=str(new_id)[:16],
                            persisted=bool(self.persistence),
                        )
                    return True

        # 3b. If we still found nothing, try fetching the order by ID one more
        #     time with a broader symbol (futures_symbol).  Sometimes CCXT needs
        #     the exact exchange symbol, not the spot/unified one.
        # (Skipped if no fetch_order capability)
        fetch_order = getattr(self.client, "fetch_order", None)
        if fetch_order and position.stop_order_id:
            for sym_variant in _symbol_variants(position):
                try:
                    order_data = await fetch_order(position.stop_order_id, sym_variant)
                    if order_data:
                        status = str(order_data.get("status") or "").lower()
                        if status in ALIVE_STOP_STATUSES:
                            self.layer3_saves_total += 1
                            logger.info(
                                "Layer 3: stop found via fetch_order with variant symbol",
                                symbol=position.symbol,
                                variant=sym_variant,
                                status=status,
                                layer3_saves_total=self.layer3_saves_total,
                            )
                            return True
                        if status in FINAL_STOP_STATUSES and float(order_data.get("filled") or 0) > 0:
                            self.layer3_saves_total += 1
                            logger.info(
                                "Layer 3: stop was recently filled (variant symbol lookup)",
                                symbol=position.symbol,
                                variant=sym_variant,
                                status=status,
                                layer3_saves_total=self.layer3_saves_total,
                            )
                            return True
                except (OperationalError, DataError) as e:
                    logger.debug(
                        "Layer 3: fetch_order variant failed (trying next)",
                        symbol=position.symbol,
                        variant=sym_variant,
                        error=str(e),
                    )
                    continue

        return False

    async def _check_stop_was_filled(
        self,
        position: ManagedPosition,
        exchange_size: float,
    ) -> bool:
        """
        Before declaring a position naked, verify whether its stop order was
        filled (expected behavior) vs. vanished without filling (real danger).
        
        If the stop was filled, the position is being closed by the stop --
        which is the *intended* safety mechanism, not a violation.
        
        This is the kill-switch backstop: it must default to PROTECTED (True)
        for any ambiguous but non-dead status, because a false NAKED alarm
        triggers emergency close + kill switch — a worse outcome than briefly
        tolerating an uncertain but likely-alive stop.
        
        Returns True (protected / expected) or False (genuinely naked).
        """
        fetch_order = getattr(self.client, "fetch_order", None)
        if not fetch_order:
            self._layer2_last_status[position.symbol] = "fetch_not_supported"
            return False  # Can't verify, treat as naked

        stop_oid = position.stop_order_id
        # Use futures symbol for fetch_order if available
        sym = getattr(position, "futures_symbol", None) or position.symbol

        try:
            order_data = await fetch_order(stop_oid, sym)
        except (OperationalError, DataError) as e:
            self._layer2_last_status[position.symbol] = "fetch_error"
            logger.warning(
                "Stop-fill verification: fetch_order failed",
                symbol=position.symbol,
                stop_order_id=stop_oid,
                error=str(e),
                error_type=type(e).__name__,
            )
            return False  # Can't verify, treat as naked

        if not order_data:
            self._layer2_last_status[position.symbol] = "no_order_data"
            return False

        status = str(order_data.get("status") or "").lower()
        self._layer2_last_status[position.symbol] = f"status:{status or 'unknown'}"
        filled_raw = order_data.get("filled")
        filled = float(filled_raw if filled_raw is not None else 0)

        # ---- FINAL (closed/filled): check filled qty ----
        if status in FINAL_STOP_STATUSES:
            if filled > 0:
                # The stop loss was FILLED -- this is expected behavior!
                logger.info(
                    "Stop-fill verification: stop was FILLED (expected, not naked)",
                    symbol=position.symbol,
                    stop_order_id=stop_oid,
                    filled_qty=filled,
                    exchange_remaining=exchange_size,
                )
                # Gracefully close the position so it doesn't keep flagging
                self._close_position_from_stop_fill(position, filled, order_data)
                return True
            else:
                # Closed with 0 fills — ambiguous (e.g. self-trade prevention).
                # Fail-safe: warn but do NOT trigger kill switch.
                logger.warning(
                    "Stop-fill verification: stop CLOSED with 0 fills (ambiguous, treating as protected)",
                    symbol=position.symbol,
                    stop_order_id=stop_oid,
                    status=status,
                )
                return True

        # ---- DEAD (cancelled/expired/rejected): genuinely naked ----
        if status in DEAD_STOP_STATUSES:
            logger.warning(
                "Stop-fill verification: stop was CANCELLED/EXPIRED (genuinely naked)",
                symbol=position.symbol,
                stop_order_id=stop_oid,
                status=status,
            )
            return False

        # ---- ALIVE (open/entered_book/untouched/etc.): stop is working ----
        if status in ALIVE_STOP_STATUSES:
            logger.info(
                "Stop-fill verification: stop is ALIVE on exchange (protected)",
                symbol=position.symbol,
                stop_order_id=stop_oid,
                status=status,
                filled=filled,
            )
            # Non-blocking semantic check: is the stop *correct*, not just alive?
            self._warn_if_stop_semantically_wrong(position, order_data)
            return True

        # ---- UNKNOWN STATUS: fail-safe → treat as protected ----
        # This is the kill-switch backstop.  An unfamiliar but non-dead status
        # should NOT trigger emergency close.  Log loud so we investigate.
        logger.warning(
            "Stop-fill verification: UNKNOWN status (treating as PROTECTED to avoid false kill-switch)",
            symbol=position.symbol,
            stop_order_id=stop_oid,
            status=status,
            filled=filled,
        )
        return True

    def _warn_if_stop_semantically_wrong(
        self,
        position: ManagedPosition,
        order_data: dict,
    ) -> None:
        """
        Non-blocking validation: is this alive stop *substantively correct*?

        Checks:
          1. Side matches position direction (long→sell stop, short→buy stop)
          2. reduceOnly is True (or unknown → soft warn)
          3. Amount coverage: warn < 90%, error-level warn < 25%
          4. Type is stop-like (stopPrice present OR type contains stop/stp)

        Logs WARNING (or ERROR for critical mismatches) but does NOT change
        the protection verdict.  This gives observability for "alive but wrong"
        stops without introducing new kill-switch risk.
        """
        issues = []       # Standard warnings
        critical = []     # High-severity issues (logged at error level)

        # --- 1. Side check ---
        # Log all three values (position side, expected close side, order side)
        # so we can confirm venue semantics quickly.
        order_side = str(order_data.get("side") or "").lower()
        pos_side_str = "long" if position.side == Side.LONG else "short"
        expected_side = "sell" if position.side == Side.LONG else "buy"
        if order_side:
            if order_side != expected_side:
                critical.append(
                    f"side={order_side}, expected={expected_side} "
                    f"(position_side={pos_side_str})"
                )
        # If order_side is empty, we can't check — skip silently.

        # --- 2. reduceOnly check ---
        reduce_only = order_data.get("reduceOnly")
        if reduce_only is None:
            # Field missing from CCXT/Kraken response — soft warning so we learn
            issues.append("reduceOnly=missing (expected True; field not in response)")
        elif not reduce_only:
            critical.append("reduceOnly=False (expected True for protective stop)")

        # --- 3. Amount coverage (two-band) ---
        order_amount = order_data.get("amount")
        if order_amount is not None:
            try:
                amount = float(order_amount)
                remaining = float(position.remaining_qty)
                if remaining > 0:
                    coverage = amount / remaining
                    if coverage < 0.25:
                        critical.append(
                            f"amount={amount}, remaining_qty={remaining}, "
                            f"coverage={coverage:.0%} (basically ineffective, likely bug)"
                        )
                    elif coverage < 0.90:
                        issues.append(
                            f"amount={amount}, remaining_qty={remaining}, "
                            f"coverage={coverage:.0%} (partial, won't fully protect)"
                        )
            except (ValueError, TypeError):
                pass

        # --- 4. Type check (resilient to Kraken naming) ---
        # A legitimate stop can appear as type="market" with a stopPrice set,
        # so we accept either: type contains stop/stp, OR stopPrice is present.
        order_type = str(order_data.get("type") or "").lower()
        stop_price = order_data.get("stopPrice")
        has_stop_in_type = any(t in order_type for t in ("stop", "stp"))
        has_stop_price = stop_price is not None

        if order_type:
            if "take_profit" in order_type or "take-profit" in order_type:
                critical.append(f"type='{order_type}' appears to be TP, not SL")
            elif not has_stop_in_type and not has_stop_price:
                issues.append(
                    f"type='{order_type}', stopPrice={stop_price} "
                    "(expected stop variant or stopPrice set)"
                )

        # --- Emit logs + update counters ---
        stop_oid = str(order_data.get("id") or "")
        sym = position.symbol

        # Auto-reset counters daily
        now = datetime.now(timezone.utc)
        if (now - self._counters_reset_at).total_seconds() > 86400:
            self.reset_semantic_counters()

        # Structured quantity fields for quick triage (always included)
        qty_fields = {
            "stop_amount": order_data.get("amount"),
            "remaining_qty": float(position.remaining_qty),
            "stop_price": order_data.get("stopPrice"),
        }

        # Track reduceOnly=missing separately
        if any("reduceOnly=missing" in i for i in issues):
            self._reduceonly_missing_counts[sym] = self._reduceonly_missing_counts.get(sym, 0) + 1

        if critical:
            self._semantic_error_counts[sym] = self._semantic_error_counts.get(sym, 0) + 1
            error_count = self._semantic_error_counts[sym]

            logger.error(
                "Stop semantic CRITICAL mismatch: stop is ALIVE but likely NOT correct protection",
                symbol=sym,
                stop_order_id=stop_oid,
                position_side=pos_side_str,
                expected_close_side=expected_side,
                order_side=order_side or "unknown",
                critical_issues=critical,
                other_issues=issues if issues else None,
                error_count_today=error_count,
                **qty_fields,
            )

            # One-time raw CCXT dump per symbol for forensic diagnosis
            if sym not in self._raw_dump_emitted:
                self._raw_dump_emitted.add(sym)
                logger.warning(
                    "Stop semantic: raw order payload dump (first error for symbol today)",
                    symbol=sym,
                    stop_order_id=stop_oid,
                    raw_order_data=order_data,
                )

        elif issues:
            self._semantic_warning_counts[sym] = self._semantic_warning_counts.get(sym, 0) + 1

            logger.warning(
                "Stop semantic mismatch: stop is ALIVE but may not be correct protection",
                symbol=sym,
                stop_order_id=stop_oid,
                issues=issues,
                warning_count_today=self._semantic_warning_counts[sym],
                **qty_fields,
            )

    def reset_semantic_counters(self) -> None:
        """Reset daily semantic mismatch counters. Called automatically after 24h."""
        if self._semantic_error_counts or self._semantic_warning_counts:
            logger.info(
                "Stop semantic counters reset (daily)",
                error_counts=dict(self._semantic_error_counts) if self._semantic_error_counts else None,
                warning_counts=dict(self._semantic_warning_counts) if self._semantic_warning_counts else None,
                reduceonly_missing=dict(self._reduceonly_missing_counts) if self._reduceonly_missing_counts else None,
            )
        self._semantic_warning_counts.clear()
        self._semantic_error_counts.clear()
        self._reduceonly_missing_counts.clear()
        self._raw_dump_emitted.clear()
        self._counters_reset_at = datetime.now(timezone.utc)

    def get_semantic_counts(self) -> Dict[str, Dict[str, int]]:
        """Return current semantic mismatch counters (for monitoring/Telegram)."""
        return {
            "errors": dict(self._semantic_error_counts),
            "warnings": dict(self._semantic_warning_counts),
            "reduceonly_missing": dict(self._reduceonly_missing_counts),
        }
    
    def _close_position_from_stop_fill(
        self,
        position: ManagedPosition,
        filled_qty: float,
        order_data: dict,
    ) -> None:
        """
        Gracefully close a position whose stop loss was confirmed filled.

        Records a synthetic exit fill (so P&L / state machine are accurate),
        sets exit_reason = STOP_LOSS, and persists the change.
        """
        from src.execution.position_state_machine import (
            FillRecord, Side, ExitReason, PositionState,
        )

        if position.is_terminal:
            return  # Already closed

        avg_price = Decimal(str(order_data.get("average") or order_data.get("price") or 0))
        qty = Decimal(str(filled_qty))
        now = datetime.now(timezone.utc)

        fill = FillRecord(
            fill_id=f"stop-fill-detect-{int(now.timestamp() * 1000)}",
            order_id=position.stop_order_id or "unknown",
            side=Side.SHORT if position.side == Side.LONG else Side.LONG,
            qty=min(qty, position.remaining_qty),
            price=avg_price if avg_price > 0 else (position.initial_entry_price or Decimal("0")),
            timestamp=now,
            is_entry=False,
        )
        position.exit_fills.append(fill)

        if position.remaining_qty <= 0:
            position._mark_closed(ExitReason.STOP_LOSS, exit_time=now)

        position.updated_at = now

        logger.info(
            "Position closed via stop-fill detection",
            symbol=position.symbol,
            filled_qty=str(qty),
            avg_price=str(avg_price),
            remaining=str(position.remaining_qty),
            state=position.state.value,
        )

        # Persist if we have a persistence reference
        if self.persistence:
            try:
                self.persistence.save_position(position)
            except (OperationalError, DataError, OSError) as e:
                logger.warning(
                    "Failed to persist stop-fill closure",
                    symbol=position.symbol,
                    error=str(e),
                )
        
        # Record trade if position just closed
        if position.state == PositionState.CLOSED and not position.trade_recorded:
            try:
                from src.execution.trade_recorder import record_closed_trade
                from decimal import Decimal as _D
                # Use conservative defaults; config rates loaded by gateway
                trade = record_closed_trade(
                    position,
                    maker_fee_rate=_D("0.0002"),
                    taker_fee_rate=_D("0.0005"),
                )
                if trade and self.persistence:
                    self.persistence.save_position(position)  # save trade_recorded=True
            except (OperationalError, DataError, OSError, ImportError) as e:
                logger.warning(
                    "Failed to record trade after stop-fill closure",
                    symbol=position.symbol,
                    error=str(e),
                    error_type=type(e).__name__,
                )

    async def run_periodic_check(self, interval_seconds: int = 30) -> None:
        """Run periodic protection checks."""
        self._running = True
        
        while self._running:
            try:
                results = await self.check_all_positions()
                
                naked_count = sum(1 for v in results.values() if not v)
                if naked_count > 0:
                    logger.critical(
                        f"PROTECTION CHECK: {naked_count} naked positions detected!",
                        details=results
                    )
                else:
                    logger.debug(
                        f"Protection check passed: {len(results)} positions verified"
                    )
                    
            except InvariantError:
                raise  # Safety violation — must halt
            except (OperationalError, DataError) as e:
                logger.error(f"Protection check failed: {e}", error_type=type(e).__name__)
            
            await asyncio.sleep(interval_seconds)
    
    def stop(self) -> None:
        """Stop periodic checks."""
        self._running = False
