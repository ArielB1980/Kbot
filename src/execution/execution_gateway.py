"""
Execution Gateway - Single Order Flow Point.

CRITICAL: All order placement MUST flow through this gateway.

This ensures:
1. Every order has client_order_id linking to position_id
2. Order events are routed back to state machine
3. No bypass paths for order placement
4. Audit trail for all execution
"""
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Callable, Optional, Dict, List, Callable, Awaitable
from datetime import datetime, timezone
import asyncio
import os
import time

from src.execution.position_state_machine import (
    ManagedPosition,
    PositionRegistry,
    PositionState,
    OrderEvent,
    OrderEventType,
    ExitReason,
    normalize_exit_reason,
    get_position_registry
)
from src.execution.position_manager_v2 import (
    PositionManagerV2,
    ManagementAction,
    ActionType
)
from src.execution.position_persistence import PositionPersistence
from src.execution.production_safety import (
    SafetyConfig,
    AtomicStopReplacer,
    WriteAheadIntentLog,
    EventOrderingEnforcer,
    ActionIntent,
    ActionIntentStatus,
)
from src.domain.models import Side, OrderType
from src.monitoring.logger import get_logger
from src.exceptions import (
    OperationalError,
    DataError,
    InvariantError,
    CircuitOpenError,
)

logger = get_logger(__name__)


class OrderPurpose(str, Enum):
    """Purpose of an order for tracking."""
    ENTRY = "entry"
    EXIT_STOP = "exit_stop"
    EXIT_TP = "exit_tp"
    EXIT_MARKET = "exit_market"
    EXIT_REVERSAL = "exit_reversal"
    STOP_INITIAL = "stop_initial"
    STOP_UPDATE = "stop_update"


@dataclass
class PendingOrder:
    """Tracking record for a pending order."""
    client_order_id: str
    position_id: str
    symbol: str
    purpose: OrderPurpose
    side: Side
    size: Decimal
    price: Optional[Decimal]
    order_type: OrderType
    submitted_at: datetime
    exchange_order_id: Optional[str] = None
    status: str = "pending"
    last_event_seq: int = 0
    last_filled_qty: Decimal = Decimal("0")
    exchange_symbol: Optional[str] = None  # Futures symbol for fetch_order (e.g. X/USD:USD)
    

@dataclass
class ExecutionResult:
    """Result of an execution attempt."""
    success: bool
    client_order_id: str
    exchange_order_id: Optional[str] = None
    error: Optional[str] = None
    filled_qty: Decimal = Decimal("0")
    filled_price: Optional[Decimal] = None


def _normalize_pending_order_type(raw_order_type: str) -> OrderType:
    """Normalize exchange/raw order type strings to domain enum values."""
    order_type = (raw_order_type or "").strip().lower()
    if order_type in ("stop", "stop_loss", "stop-loss", "stp"):
        return OrderType.STOP_LOSS
    if order_type in ("take_profit", "take-profit", "tp"):
        return OrderType.TAKE_PROFIT
    if order_type == "limit":
        return OrderType.LIMIT
    return OrderType.MARKET


class _OrderRateLimiter:
    """Sliding-window token bucket for order rate limiting.

    Two windows: per-minute and per-10-seconds.
    Raises InvariantError if either limit is exceeded — this is a logic failure,
    not a transient condition.
    """

    def __init__(
        self,
        max_per_minute: int = 60,
        max_per_10s: int = 10,
    ):
        self.max_per_minute = max_per_minute
        self.max_per_10s = max_per_10s
        self._timestamps: list[float] = []  # monotonic timestamps of placed orders
        self.orders_blocked_total: int = 0

    def check_and_record(self) -> None:
        """Check rate limits and record a new order. Raises InvariantError if exceeded."""
        now = time.monotonic()
        # Prune timestamps older than 60s
        cutoff_60s = now - 60.0
        self._timestamps = [t for t in self._timestamps if t > cutoff_60s]

        # Check per-minute limit
        if len(self._timestamps) >= self.max_per_minute:
            self.orders_blocked_total += 1
            raise InvariantError(
                f"Order rate limit exceeded: {len(self._timestamps)} orders in last 60s "
                f"(max {self.max_per_minute}/min). Possible runaway loop."
            )

        # Check per-10s limit
        cutoff_10s = now - 10.0
        recent_10s = sum(1 for t in self._timestamps if t > cutoff_10s)
        if recent_10s >= self.max_per_10s:
            self.orders_blocked_total += 1
            raise InvariantError(
                f"Order rate limit exceeded: {recent_10s} orders in last 10s "
                f"(max {self.max_per_10s}/10s). Possible runaway loop."
            )

        # Record this order
        self._timestamps.append(now)

    @property
    def orders_last_minute(self) -> int:
        now = time.monotonic()
        cutoff = now - 60.0
        return sum(1 for t in self._timestamps if t > cutoff)

    @property
    def orders_last_10s(self) -> int:
        now = time.monotonic()
        cutoff = now - 10.0
        return sum(1 for t in self._timestamps if t > cutoff)


class ExecutionGateway:
    """
    Single point of order flow.
    
    ALL order placement must go through this gateway.
    This is enforced by architecture - no direct exchange calls allowed.
    
    Responsibilities:
    1. Submit orders to exchange
    2. Track pending orders
    3. Route order events to state machine
    4. Persist state changes
    5. Emit follow-up actions
    """
    
    def __init__(
        self,
        exchange_client,  # The actual exchange client (KrakenFuturesClient)
        registry: Optional[PositionRegistry] = None,
        position_manager: Optional[PositionManagerV2] = None,
        persistence: Optional[PositionPersistence] = None,
        safety_config: Optional[SafetyConfig] = None,
        use_safety: bool = True,
        on_partial_close: Optional[Callable[[str], None]] = None,
        instrument_spec_registry=None,
        on_trade_recorded: Optional[Callable] = None,
        startup_machine=None,
        maker_fee_bps: float = 2.0,
        taker_fee_bps: float = 5.0,
        funding_rate_daily_bps: float = 10.0,
    ):
        """
        Initialize the execution gateway.
        
        Args:
            exchange_client: Exchange API client
            registry: Position registry (uses singleton if not provided)
            position_manager: Position manager (creates new if not provided)
            persistence: Persistence layer (optional, creates default if not provided)
            safety_config: Config for AtomicStopReplacer, etc. (default SafetyConfig())
            use_safety: If True, wire AtomicStopReplacer, WAL, EventOrderingEnforcer
            instrument_spec_registry: Optional registry for venue min_size; used to guard partial closes
            on_trade_recorded: Optional async callback(position, trade) called after trade recording
        """
        self.client = exchange_client
        self.registry = registry or get_position_registry()
        self.position_manager = position_manager or PositionManagerV2(self.registry)
        self._instrument_spec_registry = instrument_spec_registry
        self.persistence = persistence or PositionPersistence()
        self._safety_config = safety_config or SafetyConfig()
        self._use_safety = use_safety

        self._on_partial_close = on_partial_close
        self._on_trade_recorded = on_trade_recorded
        self._startup_machine = startup_machine  # Optional P2.3 startup state machine
        self._stop_replacer: Optional[AtomicStopReplacer] = None
        self._wal: Optional[WriteAheadIntentLog] = None
        self._event_enforcer: Optional[EventOrderingEnforcer] = None
        if use_safety:
            self._stop_replacer = AtomicStopReplacer(exchange_client, self._safety_config)
            self._wal = WriteAheadIntentLog(self.persistence)
            self._event_enforcer = EventOrderingEnforcer()
        
        # Pending order tracking
        self._pending_orders: Dict[str, PendingOrder] = {}  # client_order_id -> PendingOrder
        self._order_id_map: Dict[str, str] = {}  # exchange_order_id -> client_order_id
        
        # Metrics
        self.metrics = {
            "orders_submitted": 0,
            "orders_filled": 0,
            "orders_placed": 0,
            "orders_cancelled": 0,
            "orders_rejected": 0,
            "events_processed": 0,
            "errors": 0,
            "trades_recorded_total": 0,
            "trade_record_failures_total": 0,
            "orders_blocked_by_rate_limit_total": 0,
        }

        # P0.2: Global order rate limiter — prevents runaway loops
        max_per_minute = 60
        max_per_10s = 10
        # Replay research can emit clustered synthetic orders in tight loops;
        # keep invariant checks, but widen limits to avoid false runaway trips.
        if os.getenv("REPLAY_RELAX_ORDER_RATE_LIMITER", "0") == "1":
            max_per_minute = 600
            max_per_10s = 120
        self._order_rate_limiter = _OrderRateLimiter(
            max_per_minute=max_per_minute,
            max_per_10s=max_per_10s,
        )

        # Fee config for trade recorder (bps → fraction, set at construction)
        self._maker_fee_rate: Optional[Decimal] = Decimal(str(maker_fee_bps)) / Decimal("10000")
        self._taker_fee_rate: Optional[Decimal] = Decimal(str(taker_fee_bps)) / Decimal("10000")
        self._funding_rate_daily_bps: Decimal = Decimal(str(funding_rate_daily_bps))

    def _wal_record_intent(
        self,
        action: ManagementAction,
        action_type: str,
        size: Optional[Decimal] = None,
        price: Optional[Decimal] = None,
    ) -> None:
        """Record write-ahead intent before exchange call. No-op if no WAL."""
        if not self._wal:
            return
        intent = ActionIntent(
            intent_id=action.client_order_id,
            position_id=action.position_id or "",
            action_type=action_type,
            symbol=action.symbol,
            side=action.side.value if action.side else "unknown",
            size=str(size if size is not None else action.size),
            price=str(price) if price is not None else (str(action.price) if action.price else None),
            created_at=datetime.now(timezone.utc),
        )
        self._wal.record_intent(intent)

    def _wal_mark_sent(self, intent_id: str, exchange_order_id: str) -> None:
        if self._wal:
            self._wal.mark_sent(intent_id, exchange_order_id)

    def _wal_mark_completed(self, intent_id: str) -> None:
        if self._wal:
            self._wal.mark_completed(intent_id)

    def _wal_mark_failed(self, intent_id: str, error: str) -> None:
        if self._wal:
            self._wal.mark_failed(intent_id, error)
    
    # ========== ENTRY-TIME LIQUIDITY SAFETY ==========
    
    # Configurable thresholds for entry-time liquidity check
    ENTRY_MAX_SPREAD_PCT = Decimal("0.005")  # 0.5% max spread at entry time
    ENTRY_MIN_DEPTH_RATIO = Decimal("2.0")  # Order book depth must be 2x order size
    PRICE_DRIFT_ALERT_PCT = Decimal("0.001")  # 0.1% threshold for SQLite vs Postgres entry price drift
    
    async def _check_entry_liquidity(
        self, 
        symbol: str, 
        side: Side, 
        size: Decimal
    ) -> tuple[bool, Optional[str]]:
        """
        Entry-time liquidity safety check.
        
        This is a critical safety net since we've removed OI as a gate.
        Checks real-time orderbook conditions before sending entry orders.
        
        Returns:
            (True, None) if liquidity is acceptable
            (False, reason) if entry should be blocked
        
        Checks performed:
        1. Effective spread: Must be <= 0.5% (prevents entering during wide spreads)
        2. Depth check: Order book depth must support the order size
        
        Note: We fail OPEN on errors to be safe. Reduce-only exits always proceed.
        """
        try:
            # Get real-time orderbook or ticker
            fetch_ticker = getattr(self.client, "fetch_ticker", None)
            if not fetch_ticker:
                # No ticker method - skip check and proceed
                return (True, None)
            
            ticker = await fetch_ticker(symbol)
            if not ticker:
                return (True, None)  # Can't verify - proceed with caution
            
            # Extract bid/ask
            bid = ticker.get("bid")
            ask = ticker.get("ask")
            
            if not bid or not ask or bid <= 0 or ask <= 0:
                # Can't calculate spread - proceed with caution
                logger.debug(
                    "Entry liquidity check: missing bid/ask, skipping",
                    symbol=symbol,
                    bid=bid,
                    ask=ask,
                )
                return (True, None)
            
            bid = Decimal(str(bid))
            ask = Decimal(str(ask))
            
            # Calculate effective spread
            mid = (bid + ask) / 2
            spread_pct = (ask - bid) / mid if mid > 0 else Decimal("1")
            
            # Check 1: Spread threshold
            if spread_pct > self.ENTRY_MAX_SPREAD_PCT:
                return (
                    False, 
                    f"Spread {spread_pct:.4%} exceeds max {self.ENTRY_MAX_SPREAD_PCT:.2%}"
                )
            
            # Check 2: Depth (if available from ticker)
            # Some exchanges provide bidVolume/askVolume in ticker
            bid_vol = ticker.get("bidVolume")
            ask_vol = ticker.get("askVolume")
            
            if bid_vol is not None and ask_vol is not None:
                bid_vol = Decimal(str(bid_vol))
                ask_vol = Decimal(str(ask_vol))
                
                # For longs, we hit the ask; for shorts, we hit the bid
                relevant_depth = ask_vol if side == Side.LONG else bid_vol
                
                if relevant_depth > 0 and size > 0:
                    depth_ratio = relevant_depth / size
                    if depth_ratio < self.ENTRY_MIN_DEPTH_RATIO:
                        return (
                            False,
                            f"Depth ratio {depth_ratio:.2f}x < min {self.ENTRY_MIN_DEPTH_RATIO}x"
                        )
            
            # Passed all checks
            logger.debug(
                "Entry liquidity check passed",
                symbol=symbol,
                spread=f"{spread_pct:.4%}",
                bid=str(bid),
                ask=str(ask),
            )
            return (True, None)
            
        except InvariantError:
            raise  # Safety violation — must propagate
        except (OperationalError, DataError) as e:
            # Log but don't block on transient/data errors — liquidity is a soft gate
            logger.warning(
                "Entry liquidity check error",
                symbol=symbol,
                error=str(e),
                error_type=type(e).__name__,
            )
            return (True, None)
    
    # ========== ORDER SUBMISSION ==========
    
    async def execute_action(
        self, action: ManagementAction, order_symbol: Optional[str] = None
    ) -> ExecutionResult:
        """
        Execute a management action.

        order_symbol: optional futures symbol (e.g. X/USD:USD) for exchange orders.
        When provided for OPEN_POSITION, used instead of action.symbol (spot) so
        Kraken Futures receives the correct unified symbol.
        """
        # P2.3: Gate order placement on startup readiness (if startup machine is wired)
        if self._startup_machine is not None:
            try:
                self._startup_machine.assert_ready()
            except AssertionError as e:
                logger.error(
                    "Order blocked: system not ready",
                    action_type=action.type.value,
                    symbol=action.symbol,
                    startup_phase=self._startup_machine.phase.value,
                    error=str(e),
                )
                return ExecutionResult(
                    success=False,
                    client_order_id=action.client_order_id,
                    error=f"System not ready: {e}"
                )
        # P0.2: Global order rate limit
        self._order_rate_limiter.check_and_record()
        try:
            if action.type == ActionType.OPEN_POSITION:
                return await self._execute_entry(action, order_symbol=order_symbol)
            
            elif action.type == ActionType.CLOSE_FULL:
                return await self._execute_close(action)
            
            elif action.type == ActionType.CLOSE_PARTIAL:
                return await self._execute_partial_close(action)
            
            elif action.type == ActionType.PLACE_STOP:
                return await self._execute_place_stop(action)
            
            elif action.type == ActionType.UPDATE_STOP:
                return await self._execute_update_stop(action)
            
            elif action.type == ActionType.CANCEL_STOP:
                return await self._execute_cancel_stop(action)
            
            elif action.type == ActionType.FLATTEN_ORPHAN:
                return await self._execute_flatten_orphan(action)
            
            elif action.type == ActionType.PLACE_TP:
                return await self._execute_place_tp(action)
            
            elif action.type == ActionType.CANCEL_TP:
                return await self._execute_cancel_tp(action)
            
            else:
                logger.warning(f"Unhandled action type: {action.type}")
                return ExecutionResult(
                    success=False,
                    client_order_id=action.client_order_id,
                    error=f"Unhandled action type: {action.type}"
                )
                
        except InvariantError:
            raise  # Safety violation — must propagate to kill switch
        except (OperationalError, DataError) as e:
            self.metrics["errors"] += 1
            logger.error(
                "Execution failed",
                action_type=action.type.value,
                symbol=action.symbol,
                error=str(e),
                error_type=type(e).__name__,
            )
            return ExecutionResult(
                success=False,
                client_order_id=action.client_order_id,
                error=str(e)
            )
    
    async def _execute_entry(
        self, action: ManagementAction, order_symbol: Optional[str] = None
    ) -> ExecutionResult:
        """
        Execute entry order with entry-time liquidity safety check.
        
        order_symbol: optional futures symbol (e.g. X/USD:USD) for exchange orders.
        
        ENTRY-TIME LIQUIDITY SAFETY:
        Before sending an order, we check effective spread at entry time.
        This protects against:
        - "looks liquid in stats, but not at this moment"
        - Spoofed volume
        - Stale orderbook data
        
        This is more reliable than Kraken's OI data.
        """
        self.metrics["orders_submitted"] += 1
        exchange_symbol = order_symbol if order_symbol is not None else action.symbol
        
        # ============================================================
        # ENTRY-TIME LIQUIDITY SAFETY CHECK
        # ============================================================
        # Check effective spread before sending order.
        # This guards against momentarily illiquid conditions.
        # ============================================================
        try:
            liquidity_ok, liquidity_reason = await self._check_entry_liquidity(
                exchange_symbol, action.side, action.size
            )
            if not liquidity_ok:
                logger.warning(
                    "Entry rejected by liquidity safety check",
                    symbol=exchange_symbol,
                    reason=liquidity_reason,
                    side=action.side.value,
                    size=str(action.size),
                )
                return ExecutionResult(
                    success=False,
                    client_order_id=action.client_order_id,
                    error=f"Liquidity check failed: {liquidity_reason}"
                )
        except InvariantError:
            raise  # Safety violation — must propagate
        except (OperationalError, DataError) as e:
            # Don't block entry on transient/data errors — log and proceed
            logger.warning(
                "Liquidity check failed with error, proceeding with entry",
                symbol=exchange_symbol,
                error=str(e),
                error_type=type(e).__name__,
            )

        # Track pending order (store exchange_symbol for order-status polling)
        pending = PendingOrder(
            client_order_id=action.client_order_id,
            position_id=action.position_id,
            symbol=action.symbol,
            purpose=OrderPurpose.ENTRY,
            side=action.side,
            size=action.size,
            price=action.price,
            order_type=action.order_type,
            submitted_at=datetime.now(timezone.utc),
            exchange_symbol=exchange_symbol,
        )
        self._pending_orders[action.client_order_id] = pending

        self._wal_record_intent(action, "open")

        # Submit to exchange (use futures symbol; action.symbol is spot)
        try:
            order_side = "buy" if action.side == Side.LONG else "sell"
            order_type_for_submit = action.order_type.value
            price_for_submit = (
                float(action.price)
                if action.price and action.order_type == OrderType.LIMIT
                else None
            )
            if os.getenv("REPLAY_FORCE_MARKET_ENTRY", "0") == "1":
                order_type_for_submit = "market"
                price_for_submit = None
                logger.info(
                    "Replay override: forcing market entry order type",
                    symbol=exchange_symbol,
                    original_order_type=action.order_type.value,
                )

            result = await self.client.create_order(
                symbol=exchange_symbol,
                type=order_type_for_submit,
                side=order_side,
                amount=float(action.size),
                price=price_for_submit,
                params={"clientOrderId": action.client_order_id},
                leverage=action.leverage,
            )
            
            exchange_order_id = result.get("id")
            pending.exchange_order_id = exchange_order_id
            pending.status = "submitted"
            self._order_id_map[exchange_order_id] = action.client_order_id

            # Canonicalize entry identifiers on first exchange acknowledgement path:
            # state machine should track exchange order id + stable client id.
            position = self.registry.get_position(action.symbol)
            if position and exchange_order_id:
                position.entry_order_id = exchange_order_id
                position.entry_client_order_id = action.client_order_id
                self.persistence.save_position(position)
            
            logger.info(
                "Entry order submitted",
                symbol=exchange_symbol,
                client_order_id=action.client_order_id,
                exchange_order_id=exchange_order_id
            )
            self._wal_mark_sent(action.client_order_id, exchange_order_id)

            # Replay exchange can return immediately filled market orders.
            # Process that snapshot now so state machine transitions happen
            # before the next reconcile pass.
            status = str(result.get("status", "")).lower()
            filled_now = Decimal(str(result.get("filled", 0) or 0))
            replay_force_market = os.getenv("REPLAY_FORCE_MARKET_ENTRY", "0") == "1"
            if replay_force_market and status in {"open", "submitted"} and filled_now <= 0:
                # Replay fail-open: treat forced market entry as filled to avoid
                # permanent pending-position churn when simulator order snapshots
                # lag the immediate fill path.
                result["status"] = "filled"
                result["filled"] = float(action.size)
                result["remaining"] = 0.0
                if result.get("average") in (None, 0, 0.0):
                    result["average"] = float(action.price) if action.price else 0.0
                status = "filled"
                filled_now = Decimal(str(result.get("filled", 0) or 0))
                logger.warning(
                    "Replay fail-open: synthesizing immediate fill for forced market entry",
                    symbol=exchange_symbol,
                    client_order_id=action.client_order_id,
                )
            if status in {"filled", "closed"} or filled_now > 0:
                if not result.get("clientOrderId"):
                    result["clientOrderId"] = action.client_order_id
                await self.process_order_update(result)
            
            # Log action to persistence
            self.persistence.log_action(
                action.position_id,
                "entry_submitted",
                {"client_order_id": action.client_order_id, "exchange_order_id": exchange_order_id}
            )
            
            return ExecutionResult(
                success=True,
                client_order_id=action.client_order_id,
                exchange_order_id=exchange_order_id
            )
            
        except InvariantError:
            raise  # Safety violation — must propagate
        except (OperationalError, DataError) as e:
            pending.status = "failed"
            self.metrics["orders_rejected"] += 1
            self._wal_mark_failed(action.client_order_id, str(e))
            logger.error(f"Entry order failed: {e}", error_type=type(e).__name__)
            return ExecutionResult(
                success=False,
                client_order_id=action.client_order_id,
                error=str(e)
            )
    
    async def _execute_close(self, action: ManagementAction) -> ExecutionResult:
        """Execute full position close."""
        normalized_exit_reason, was_normalized = normalize_exit_reason(action.exit_reason)
        if was_normalized:
            logger.warning(
                "EXIT_REASON_NORMALIZED",
                symbol=action.symbol,
                position_id=action.position_id,
                input_reason=str(action.exit_reason),
                normalized_reason=normalized_exit_reason.value,
                caller="_execute_close",
                action_type=action.type.value,
            )
        action.exit_reason = normalized_exit_reason

        self.metrics["orders_submitted"] += 1
        
        # Track pending order
        pending = PendingOrder(
            client_order_id=action.client_order_id,
            position_id=action.position_id,
            symbol=action.symbol,
            purpose=OrderPurpose.EXIT_MARKET if action.exit_reason != ExitReason.DIRECTION_REVERSAL else OrderPurpose.EXIT_REVERSAL,
            side=Side.SHORT if action.side == Side.LONG else Side.LONG,  # Opposite side to close
            size=action.size,
            price=None,
            order_type=OrderType.MARKET,
            submitted_at=datetime.now(timezone.utc)
        )
        self._pending_orders[action.client_order_id] = pending
        
        # Update position state to EXIT_PENDING
        position = self.registry.get_position(action.symbol)
        if position:
            position.initiate_exit(
                action.exit_reason,
                action.client_order_id,
                client_order_id=action.client_order_id,
            )
            self.persistence.save_position(position)
        exchange_symbol = (getattr(position, "futures_symbol", None) if position else None) or action.symbol
        
        self._wal_record_intent(action, "close")
        try:
            # Close via reduce-only market order (reduceOnly=True required: rounds up, no dust)
            close_side = "sell" if action.side == Side.LONG else "buy"
            
            result = await self.client.create_order(
                symbol=exchange_symbol,
                type="market",
                side=close_side,
                amount=float(action.size),
                params={
                    "clientOrderId": action.client_order_id,
                    "reduceOnly": True,
                }
            )
            
            exchange_order_id = result.get("id")
            pending.exchange_order_id = exchange_order_id
            pending.status = "submitted"
            self._order_id_map[exchange_order_id] = action.client_order_id
            self._wal_mark_sent(action.client_order_id, exchange_order_id)

            # Replace temporary client-id tracking with exchange id once available.
            if position and exchange_order_id:
                position.pending_exit_order_id = exchange_order_id
                position.pending_exit_client_order_id = action.client_order_id
                self.persistence.save_position(position)
            
            logger.info(
                "Close order submitted",
                symbol=exchange_symbol,
            reason=action.exit_reason.value if action.exit_reason else "unknown"
            )
            
            return ExecutionResult(
                success=True,
                client_order_id=action.client_order_id,
                exchange_order_id=exchange_order_id
            )
            
        except InvariantError:
            raise  # Safety violation — must propagate
        except (OperationalError, DataError) as e:
            pending.status = "failed"
            self._wal_mark_failed(action.client_order_id, str(e))
            logger.error(f"Close order failed: {e}", error_type=type(e).__name__)
            return ExecutionResult(
                success=False,
                client_order_id=action.client_order_id,
                error=str(e)
            )
    
    async def _execute_partial_close(self, action: ManagementAction) -> ExecutionResult:
        """Execute partial position close. Use position.futures_symbol for exchange when set."""
        position = self.registry.get_position(action.symbol)
        exchange_symbol = (getattr(position, "futures_symbol", None) if position else None) or action.symbol

        # Venue min guard: reject before sending to avoid ORDER_REJECTED_BY_VENUE
        if self._instrument_spec_registry:
            min_size = self._instrument_spec_registry.get_effective_min_size(exchange_symbol)
            if action.size < min_size:
                self.metrics["orders_rejected"] = self.metrics.get("orders_rejected", 0) + 1
                logger.warning(
                    "Partial close below venue min - skipping",
                    symbol=exchange_symbol,
                    size=str(action.size),
                    min_size=str(min_size),
                )
                return ExecutionResult(
                    success=False,
                    client_order_id=action.client_order_id,
                    error=f"Partial close size {action.size} below venue min {min_size} for {exchange_symbol}",
                )

        self.metrics["orders_submitted"] += 1
        pending = PendingOrder(
            client_order_id=action.client_order_id,
            position_id=action.position_id,
            symbol=action.symbol,
            purpose=OrderPurpose.EXIT_TP,
            side=Side.SHORT if action.side == Side.LONG else Side.LONG,
            size=action.size,
            price=None,
            order_type=OrderType.MARKET,
            submitted_at=datetime.now(timezone.utc)
        )
        self._pending_orders[action.client_order_id] = pending
        
        self._wal_record_intent(action, "partial_close")
        try:
            # reduceOnly=True required for exits: size rounds up, no dust
            close_side = "sell" if action.side == Side.LONG else "buy"
            
            result = await self.client.create_order(
                symbol=exchange_symbol,
                type="market",
                side=close_side,
                amount=float(action.size),
                params={
                    "clientOrderId": action.client_order_id,
                    "reduceOnly": True,
                }
            )
            
            exchange_order_id = result.get("id")
            pending.exchange_order_id = exchange_order_id
            self._order_id_map[exchange_order_id] = action.client_order_id
            self._wal_mark_sent(action.client_order_id, exchange_order_id)

            # Keep stop identifiers synchronized for event matching.
            if position and exchange_order_id:
                position.stop_order_id = exchange_order_id
                position.stop_client_order_id = action.client_order_id
                self.persistence.save_position(position)
            
            return ExecutionResult(
                success=True,
                client_order_id=action.client_order_id,
                exchange_order_id=exchange_order_id
            )
            
        except InvariantError:
            raise  # Safety violation — must propagate
        except (OperationalError, DataError) as e:
            self._wal_mark_failed(action.client_order_id, str(e))
            logger.error(f"Partial close failed: {e}", error_type=type(e).__name__)
            return ExecutionResult(
                success=False,
                client_order_id=action.client_order_id,
                error=str(e)
            )
    
    async def _execute_place_stop(self, action: ManagementAction) -> ExecutionResult:
        """Place stop loss order. Use position.futures_symbol for exchange when set."""
        self.metrics["orders_submitted"] += 1
        position = self.registry.get_position(action.symbol)
        exchange_symbol = (getattr(position, "futures_symbol", None) if position else None) or action.symbol
        
        pending = PendingOrder(
            client_order_id=action.client_order_id,
            position_id=action.position_id,
            symbol=action.symbol,
            purpose=OrderPurpose.STOP_INITIAL,
            side=Side.SHORT if action.side == Side.LONG else Side.LONG,  # Opposite to close
            size=action.size,
            price=action.price,
            order_type=OrderType.STOP_LOSS,
            submitted_at=datetime.now(timezone.utc)
        )
        self._pending_orders[action.client_order_id] = pending
        
        self._wal_record_intent(action, "place_stop", price=action.price)
        try:
            # reduceOnly=True required for protective exits: rounds up, no dust
            stop_side = "sell" if action.side == Side.LONG else "buy"
            
            result = await self.client.create_order(
                symbol=exchange_symbol,
                type="stop",
                side=stop_side,
                amount=float(action.size),
                price=float(action.price),
                params={
                    "clientOrderId": action.client_order_id,
                    "reduceOnly": True,
                    "stopPrice": float(action.price),
                }
            )
            
            exchange_order_id = result.get("id")
            pending.exchange_order_id = exchange_order_id
            self._order_id_map[exchange_order_id] = action.client_order_id
            self._wal_mark_sent(action.client_order_id, exchange_order_id)
            
            # Update position with stop order ID
            position = self.registry.get_position(action.symbol)
            if position:
                position.stop_order_id = exchange_order_id
                self.persistence.save_position(position)
            
            logger.info(
                "Stop order placed",
                symbol=exchange_symbol,
                price=str(action.price)
            )
            
            return ExecutionResult(
                success=True,
                client_order_id=action.client_order_id,
                exchange_order_id=exchange_order_id
            )
            
        except InvariantError:
            raise  # Safety violation — must propagate
        except (OperationalError, DataError) as e:
            self._wal_mark_failed(action.client_order_id, str(e))
            logger.error(f"Stop placement failed: {e}", error_type=type(e).__name__)
            return ExecutionResult(
                success=False,
                client_order_id=action.client_order_id,
                error=str(e)
            )
    
    async def _execute_update_stop(self, action: ManagementAction) -> ExecutionResult:
        """Update stop loss order. Uses AtomicStopReplacer (new-first then cancel old) when use_safety."""
        position = self.registry.get_position(action.symbol)
        if not position:
            return ExecutionResult(
                success=False,
                client_order_id=action.client_order_id,
                error="Position not found"
            )
        if not action.price:
            return ExecutionResult(
                success=False,
                client_order_id=action.client_order_id,
                error="Update stop requires price"
            )

        if self._stop_replacer:
            self._wal_record_intent(action, "update_stop", price=action.price)
            def _gen_cid(_pid: str, _: str) -> str:
                return action.client_order_id
            ctx = await self._stop_replacer.replace_stop(
                position, action.price, _gen_cid
            )
            if ctx.failed:
                self.metrics["errors"] += 1
                self._wal_mark_failed(action.client_order_id, ctx.error or "Atomic stop replace failed")
                return ExecutionResult(
                    success=False,
                    client_order_id=action.client_order_id,
                    error=ctx.error or "Atomic stop replace failed"
                )
            if not position.update_stop(action.price):
                return ExecutionResult(
                    success=False,
                    client_order_id=action.client_order_id,
                    error="Stop update rejected by state machine"
                )
            if ctx.new_stop_order_id:
                position.stop_order_id = ctx.new_stop_order_id
                position.stop_client_order_id = action.client_order_id
            self.persistence.save_position(position)
            self.metrics["orders_submitted"] += 1
            if ctx.old_stop_cancelled:
                self.metrics["orders_cancelled"] += 1
            self._wal_mark_sent(action.client_order_id, ctx.new_stop_order_id or "")
            return ExecutionResult(
                success=True,
                client_order_id=action.client_order_id,
                exchange_order_id=ctx.new_stop_order_id
            )
        else:
            # Legacy: cancel then place when use_safety=False
            if position.stop_order_id:
                try:
                    await self.client.cancel_order(position.stop_order_id, action.symbol)
                    self.metrics["orders_cancelled"] += 1
                except (OperationalError, DataError) as e:
                    logger.warning(f"Failed to cancel old stop: {e}", error_type=type(e).__name__)
            if not position.update_stop(action.price):
                return ExecutionResult(
                    success=False,
                    client_order_id=action.client_order_id,
                    error="Stop update rejected by state machine"
                )
            action.size = position.remaining_qty
            return await self._execute_place_stop(action)
    
    async def _execute_cancel_stop(self, action: ManagementAction) -> ExecutionResult:
        """Cancel stop loss order."""
        position = self.registry.get_position(action.symbol)
        if not position or not position.stop_order_id:
            return ExecutionResult(
                success=True,
                client_order_id=action.client_order_id,
                error="No stop to cancel"
            )
        self._wal_record_intent(action, "cancel_stop", size=position.remaining_qty)
        try:
            await self.client.cancel_order(position.stop_order_id, action.symbol)
            self.metrics["orders_cancelled"] += 1
            self._wal_mark_completed(action.client_order_id)
            return ExecutionResult(
                success=True,
                client_order_id=action.client_order_id
            )
        except InvariantError:
            raise  # Safety violation — must propagate
        except (OperationalError, DataError) as e:
            self._wal_mark_failed(action.client_order_id, str(e))
            return ExecutionResult(
                success=False,
                client_order_id=action.client_order_id,
                error=str(e)
            )
    
    async def _execute_place_tp(self, action: ManagementAction) -> ExecutionResult:
        """
        Place take-profit limit order.
        
        V3: Added to fix V2 state machine not placing TP orders after entry fill.
        TP orders are reduce-only limit orders at the target price.
        """
        position = self.registry.get_position(action.symbol)
        if not position:
            return ExecutionResult(
                success=False,
                client_order_id=action.client_order_id,
                error="Position not found"
            )
        
        if not action.price:
            return ExecutionResult(
                success=False,
                client_order_id=action.client_order_id,
                error="TP requires price"
            )
        
        if not action.size or action.size <= 0:
            return ExecutionResult(
                success=False,
                client_order_id=action.client_order_id,
                error="TP requires positive size"
            )

        # Venue min guard: reject before sending to avoid ORDER_REJECTED_BY_VENUE
        exchange_symbol = position.futures_symbol or action.symbol
        if self._instrument_spec_registry:
            min_size = self._instrument_spec_registry.get_effective_min_size(exchange_symbol)
            if action.size < min_size:
                self.metrics["orders_rejected"] = self.metrics.get("orders_rejected", 0) + 1
                logger.warning(
                    "TP size below venue min - skipping",
                    symbol=exchange_symbol,
                    size=str(action.size),
                    min_size=str(min_size),
                )
                return ExecutionResult(
                    success=False,
                    client_order_id=action.client_order_id,
                    error=f"TP size {action.size} below venue min {min_size} for {exchange_symbol}",
                )
        
        # Determine TP side (opposite of position side)
        tp_side = "sell" if position.side == Side.LONG else "buy"
        
        # Register pending order for fill tracking
        pending = PendingOrder(
            client_order_id=action.client_order_id,
            position_id=action.position_id,
            symbol=action.symbol,
            exchange_symbol=exchange_symbol,
            purpose=OrderPurpose.EXIT_TP,
            side=Side.SHORT if position.side == Side.LONG else Side.LONG,
            size=action.size,
            price=action.price,
            order_type=OrderType.LIMIT,
            submitted_at=datetime.now(timezone.utc),
        )
        self._pending_orders[action.client_order_id] = pending
        self.metrics["orders_submitted"] += 1
        
        self._wal_record_intent(action, "place_tp", price=action.price, size=action.size)
        
        try:
            result = await self.client.create_order(
                symbol=exchange_symbol,
                type="limit",
                side=tp_side,
                amount=float(action.size),
                price=float(action.price),
                params={
                    "clientOrderId": action.client_order_id,
                    "reduceOnly": True,
                }
            )
            
            exchange_order_id = result.get("id")
            pending.exchange_order_id = exchange_order_id
            self._order_id_map[exchange_order_id] = action.client_order_id
            self._wal_mark_sent(action.client_order_id, exchange_order_id)
            
            # Track TP order ID - update specific TP slot based on client order ID
            if action.client_order_id and action.client_order_id.startswith("tp1-"):
                position.tp1_order_id = exchange_order_id
            elif action.client_order_id and action.client_order_id.startswith("tp2-"):
                position.tp2_order_id = exchange_order_id
            self.persistence.save_position(position)
            
            self.metrics["orders_placed"] += 1
            logger.info(
                "TP order placed",
                symbol=exchange_symbol,
                price=str(action.price),
                size=str(action.size),
                order_id=exchange_order_id,
                client_order_id=action.client_order_id
            )
            
            return ExecutionResult(
                success=True,
                client_order_id=action.client_order_id,
                exchange_order_id=exchange_order_id
            )
            
        except InvariantError:
            raise  # Safety violation — must propagate
        except (OperationalError, DataError) as e:
            self._wal_mark_failed(action.client_order_id, str(e))
            logger.error(f"TP placement failed: {e}", error_type=type(e).__name__, symbol=action.symbol, price=str(action.price))
            return ExecutionResult(
                success=False,
                client_order_id=action.client_order_id,
                error=str(e)
            )
    
    async def _execute_cancel_tp(self, action: ManagementAction) -> ExecutionResult:
        """
        Cancel take-profit order.
        
        V3: Added to complement PLACE_TP handling.
        """
        if not action.client_order_id:
            return ExecutionResult(
                success=True,
                client_order_id=action.client_order_id or "unknown",
                error="No TP order ID to cancel"
            )
        
        self._wal_record_intent(action, "cancel_tp")
        
        try:
            # Use the client_order_id as the exchange order ID to cancel
            order_id_to_cancel = action.client_order_id
            await self.client.cancel_order(order_id_to_cancel, action.symbol)
            self.metrics["orders_cancelled"] += 1
            self._wal_mark_completed(action.client_order_id)
            
            # Clear the corresponding TP slot
            position = self.registry.get_position(action.symbol)
            if position:
                if position.tp1_order_id == order_id_to_cancel:
                    position.tp1_order_id = None
                elif position.tp2_order_id == order_id_to_cancel:
                    position.tp2_order_id = None
                self.persistence.save_position(position)
            
            logger.info("TP order cancelled", order_id=order_id_to_cancel, symbol=action.symbol)
            
            return ExecutionResult(
                success=True,
                client_order_id=action.client_order_id
            )
        except InvariantError:
            raise  # Safety violation — must propagate
        except (OperationalError, DataError) as e:
            self._wal_mark_failed(action.client_order_id, str(e))
            logger.warning(f"Failed to cancel TP order: {e}", error_type=type(e).__name__, order_id=action.client_order_id)
            return ExecutionResult(
                success=False,
                client_order_id=action.client_order_id,
                error=str(e)
            )
    
    async def _execute_flatten_orphan(self, action: ManagementAction) -> ExecutionResult:
        """Flatten orphan position on exchange."""
        logger.critical(f"FLATTENING ORPHAN POSITION: {action.symbol}")
        self._wal_record_intent(action, "flatten_orphan")
        try:
            # Use exchange's close_position command
            result = await self.client.close_position(action.symbol)
            oid = (result or {}).get("id") if isinstance(result, dict) else None
            if oid:
                self._wal_mark_sent(action.client_order_id, str(oid))
            else:
                self._wal_mark_completed(action.client_order_id)
            self.persistence.log_action(
                action.position_id or "unknown",
                "orphan_flattened",
                {"symbol": action.symbol, "result": str(result)}
            )
            return ExecutionResult(
                success=True,
                client_order_id=action.client_order_id
            )
        except InvariantError:
            raise  # Safety violation — must propagate
        except (OperationalError, DataError) as e:
            self._wal_mark_failed(action.client_order_id, str(e))
            logger.error(f"Failed to flatten orphan: {e}", error_type=type(e).__name__)
            return ExecutionResult(
                success=False,
                client_order_id=action.client_order_id,
                error=str(e)
            )
    
    # ========== TRADE RECORDING ==========
    
    def _get_fee_rates(self) -> tuple:
        """Return fee rates (bps → fraction). Uses values set at construction time."""
        if self._maker_fee_rate is None:
            self._maker_fee_rate = Decimal("0.0002")   # 2 bps default
            self._taker_fee_rate = Decimal("0.0005")   # 5 bps default
            self._funding_rate_daily_bps = Decimal("10")
        return self._maker_fee_rate, self._taker_fee_rate
    
    async def _maybe_record_trade(self, position: ManagedPosition) -> None:
        """
        Record a trade if the position is terminal and not yet recorded.
        
        This is the SINGLE convergence point for all close paths.
        Called after persistence.save_position() in every code path
        that can transition a position to CLOSED or ORPHANED.
        
        If exit fills are missing (common for orphaned positions whose stop
        was filled by the exchange), queries Kraken for the actual fill data
        before recording.
        
        After successful recording:
        1. Sends POSITION_CLOSED Telegram notification
        2. Fires on_trade_recorded callback (for risk manager, etc.)
        """
        terminal_states = (PositionState.CLOSED, PositionState.ORPHANED)
        if position.state not in terminal_states or position.trade_recorded:
            return
        
        MAX_RECORD_ATTEMPTS = 10
        position.trade_record_attempts += 1
        if position.trade_record_attempts > MAX_RECORD_ATTEMPTS:
            logger.critical(
                "TRADE_RECORD_EXHAUSTED: giving up after max attempts",
                position_id=position.position_id,
                symbol=position.symbol,
                attempts=position.trade_record_attempts,
            )
            position.trade_recorded = True
            try:
                from src.monitoring.alert_dispatcher import send_alert
                await send_alert(
                    "TRADE_RECORD_EXHAUSTED",
                    f"Trade recording permanently failed for {position.symbol} "
                    f"(position {position.position_id}) after {MAX_RECORD_ATTEMPTS} attempts. "
                    f"Manual intervention required.",
                    urgent=True,
                )
            except Exception:
                pass
            return
        
        # If exit fills are missing or incomplete, try to backfill from exchange
        # before recording. This is common for orphaned positions where the
        # exchange closed exposure but local fill events were partially missed.
        if position.filled_entry_qty > 0 and (
            not position.exit_fills or position.filled_exit_qty < position.filled_entry_qty
        ):
            await self._backfill_exit_fills(position)

        # Do not fabricate precise trade stats for orphaned positions when we
        # still lack complete exit fills after backfill. Emit a clear alert
        # without P&L instead of a misleading "entry == exit" summary.
        if (
            position.state == PositionState.ORPHANED
            and position.filled_entry_qty > 0
            and position.filled_exit_qty < position.filled_entry_qty
        ):
            missing_qty = position.filled_entry_qty - position.filled_exit_qty
            logger.warning(
                "ORPHANED_CLOSE_UNPRICED: skipping precise trade record",
                position_id=position.position_id,
                symbol=position.symbol,
                entry_qty=str(position.filled_entry_qty),
                exit_qty=str(position.filled_exit_qty),
                missing_qty=str(missing_qty),
            )
            try:
                from src.monitoring.alert_dispatcher import send_alert, fmt_size
                await send_alert(
                    "POSITION_CLOSED_UNPRICED",
                    f"⚠️ Position reconciled closed: {position.symbol}\n"
                    f"Side: {position.side.value.upper()}\n"
                    f"Entry size: {fmt_size(position.filled_entry_qty)}\n"
                    f"Known exited: {fmt_size(position.filled_exit_qty)}\n"
                    f"Unpriced remainder: {fmt_size(missing_qty)}\n"
                    "Reason: orphaned_exchange_reconcile (exact exit fills unavailable)",
                    urgent=True,
                )
            except Exception:
                pass
            position.trade_recorded = True
            self.persistence.save_position(position)
            return
        
        maker_rate, taker_rate = self._get_fee_rates()
        
        try:
            from src.execution.trade_recorder import record_closed_trade_async
            trade = await record_closed_trade_async(
                position,
                maker_fee_rate=maker_rate,
                taker_fee_rate=taker_rate,
                funding_rate_daily_bps=self._funding_rate_daily_bps,
            )
            if trade is None and position.trade_recorded:
                self.persistence.save_position(position)
                return
            if trade:
                self.metrics["trades_recorded_total"] += 1
                # Re-persist to save trade_recorded=True
                self.persistence.save_position(position)
                
                # ---- Send POSITION_CLOSED notification ----
                try:
                    from src.monitoring.alert_dispatcher import send_alert, fmt_price, fmt_size
                    
                    pnl_sign = "+" if trade.net_pnl >= 0 else ""
                    pnl_emoji = "\u2705" if trade.net_pnl >= 0 else "\u274c"
                    holding = trade.holding_period_hours
                    holding_str = f"{float(holding):.1f}h" if holding else "?"
                    exit_reason = trade.exit_reason or "unknown"
                    
                    await send_alert(
                        "POSITION_CLOSED",
                        f"{pnl_emoji} Position closed: {trade.symbol}\n"
                        f"Side: {trade.side.value.upper()}\n"
                        f"Size: {fmt_size(trade.size)}\n"
                        f"Entry: ${fmt_price(trade.entry_price)} \u2192 Exit: ${fmt_price(trade.exit_price)}\n"
                        f"Gross P&L: {pnl_sign}${float(trade.gross_pnl):.2f}\n"
                        f"Fees: ${float(trade.fees):.2f}\n"
                        f"Funding: ${float(trade.funding):.2f}\n"
                        f"Net P&L: {pnl_sign}${float(trade.net_pnl):.2f}\n"
                        f"Reason: {exit_reason}\n"
                        f"Duration: {holding_str}",
                        urgent=True,
                    )
                except (OperationalError, DataError, OSError, RuntimeError) as alert_err:
                    logger.warning(
                        "Failed to send POSITION_CLOSED alert (non-fatal)",
                        error=str(alert_err),
                        error_type=type(alert_err).__name__,
                    )
                
                # ---- Fire callback (risk manager update, etc.) ----
                if self._on_trade_recorded:
                    try:
                        import asyncio
                        result = self._on_trade_recorded(position, trade)
                        if asyncio.iscoroutine(result):
                            await result
                    except (OperationalError, DataError, RuntimeError) as cb_err:
                        logger.warning(
                            "on_trade_recorded callback failed (non-fatal)",
                            error=str(cb_err),
                            error_type=type(cb_err).__name__,
                        )
        except InvariantError:
            raise  # Safety violation — must propagate
        except (OperationalError, DataError) as e:
            self.metrics["trade_record_failures_total"] += 1
            logger.error(
                "TRADE_RECORD_FAILURE",
                position_id=position.position_id,
                symbol=position.symbol,
                error=str(e),
            )
        except Exception as e:
            self.metrics["trade_record_failures_total"] += 1
            logger.critical(
                "TRADE_RECORD_FAILURE_UNEXPECTED",
                position_id=position.position_id,
                symbol=position.symbol,
                error=str(e),
                error_type=type(e).__name__,
            )
            try:
                from src.monitoring.alert_dispatcher import send_alert
                await send_alert(
                    "TRADE_RECORD_FAILURE",
                    f"Failed to record trade for {position.symbol}: {type(e).__name__}: {e}",
                    urgent=True,
                )
            except Exception:
                pass
    
    async def _backfill_exit_fills(self, position: ManagedPosition) -> None:
        """
        Query Kraken for exit fill data when exit_fills are missing.
        
        For orphaned positions, the exchange filled a stop/TP order but
        the system missed the fill event. This method fetches the actual
        fill price and quantity from the exchange so P&L can be computed
        accurately instead of guessed.
        
        Strategy:
        1. Check stop_order_id, tp1_order_id, tp2_order_id via fetch_order
        2. For any filled orders, create FillRecord entries
        3. If no order IDs available, use fetch_my_trades as fallback
        """
        from src.execution.position_state_machine import FillRecord, Side
        
        symbol = position.symbol
        exit_side = Side.SHORT if position.side == Side.LONG else Side.LONG
        fills_added = 0
        
        # Try known order IDs first (stop, tp1, tp2)
        order_ids = []
        if position.stop_order_id:
            order_ids.append(("stop_loss", position.stop_order_id))
        if position.tp1_order_id:
            order_ids.append(("tp1", position.tp1_order_id))
        if position.tp2_order_id:
            order_ids.append(("tp2", position.tp2_order_id))
        
        for label, order_id in order_ids:
            try:
                order_data = await self.client.fetch_order(order_id, symbol)
                if not order_data:
                    continue
                
                status = order_data.get("status", "")
                filled = Decimal(str(order_data.get("filled", 0)))
                avg_price = order_data.get("average", 0)
                
                if status in ("closed", "filled") and filled > 0 and avg_price:
                    fill = FillRecord(
                        fill_id=f"backfill-{label}-{order_id[:12]}",
                        order_id=order_id,
                        side=exit_side,
                        qty=filled,
                        price=Decimal(str(avg_price)),
                        timestamp=datetime.now(timezone.utc),
                        is_entry=False,
                    )
                    position.exit_fills.append(fill)
                    fills_added += 1
                    logger.info(
                        "Backfilled exit fill from exchange",
                        symbol=symbol,
                        label=label,
                        order_id=order_id,
                        filled=str(filled),
                        price=str(avg_price),
                    )
            except Exception as e:
                logger.warning(
                    "Failed to fetch order for fill backfill",
                    symbol=symbol,
                    label=label,
                    order_id=order_id,
                    error=str(e),
                )
        
        # Fallback: if no fills found via order IDs, try fetch_my_trades
        if fills_added == 0:
            try:
                # Fetch recent trades for this symbol.
                # Accepts position symbols as either spot (e.g. AVAX/USD),
                # unified futures (AVAX/USD:USD), or venue ids (PF_AVAXUSD).
                ccxt_symbol = str(symbol or "").strip()
                if ccxt_symbol.startswith("PF_") and "/" not in ccxt_symbol:
                    base = ccxt_symbol.replace("PF_", "")
                    if base.endswith("USD"):
                        base = base[:-3]
                    ccxt_symbol = f"{base}/USD:USD"
                elif "/" in ccxt_symbol and not ccxt_symbol.endswith(":USD"):
                    ccxt_symbol = f"{ccxt_symbol}:USD"
                
                since_ms = None
                if position.entry_fills:
                    earliest = min(f.timestamp for f in position.entry_fills)
                    since_ms = int(earliest.timestamp() * 1000)
                
                trades = await self.client.get_futures_fills(
                    symbol=ccxt_symbol,
                    since=since_ms,
                    limit=50,
                )
                
                entry_qty = position.filled_entry_qty
                entry_side_str = "buy" if position.side == Side.LONG else "sell"
                exit_side_str = "sell" if position.side == Side.LONG else "buy"
                
                for t in trades:
                    t_side = (t.get("side", "") or "").lower()
                    t_amount = Decimal(str(t.get("amount", 0)))
                    t_price = Decimal(str(t.get("price", 0)))
                    
                    if t_side == exit_side_str and t_amount > 0 and t_price > 0:
                        fill = FillRecord(
                            fill_id=f"backfill-trade-{t.get('id', 'unknown')}",
                            order_id=t.get("order", "unknown"),
                            side=exit_side,
                            qty=t_amount,
                            price=t_price,
                            timestamp=datetime.now(timezone.utc),
                            is_entry=False,
                        )
                        position.exit_fills.append(fill)
                        fills_added += 1
                        logger.info(
                            "Backfilled exit fill from trade history",
                            symbol=symbol,
                            trade_id=t.get("id"),
                            amount=str(t_amount),
                            price=str(t_price),
                        )
            except Exception as e:
                logger.warning(
                    "Failed to fetch trade history for fill backfill",
                    symbol=symbol,
                    error=str(e),
                    error_type=type(e).__name__,
                )
        
        if fills_added > 0:
            logger.info(
                "Exit fills backfilled from exchange",
                symbol=symbol,
                fills_added=fills_added,
                total_exit_fills=len(position.exit_fills),
                avg_exit_price=str(position.avg_exit_price),
            )
            self.persistence.save_position(position)

    async def retry_unrecorded_trades(self) -> int:
        """
        Retry trade recording for closed positions that were never recorded.
        
        Called once during startup, after registry load + reconciliation.
        Prevents permanent TRADE_RECORDING_STALL when a restart interrupted
        recording or a previous recording attempt failed silently.
        
        Returns count of trades successfully recorded.
        """
        if not self.registry:
            return 0
        
        recorded = 0
        terminal_states = (PositionState.CLOSED, PositionState.ORPHANED)
        candidates = [
            pos for pos in self.registry.get_closed_history(limit=200)
            if pos.state in terminal_states and not pos.trade_recorded
        ]
        
        if not candidates:
            return 0
        
        logger.info(
            "Retrying trade recording for unrecorded closed positions",
            count=len(candidates),
        )
        
        for pos in candidates:
            try:
                await self._maybe_record_trade(pos)
                if pos.trade_recorded:
                    recorded += 1
            except InvariantError:
                raise
            except Exception as e:
                logger.error(
                    "Startup trade recording retry failed",
                    position_id=pos.position_id,
                    symbol=pos.symbol,
                    error=str(e),
                    error_type=type(e).__name__,
                )
        
        if recorded > 0:
            logger.info("Startup trade recording retry complete", recorded=recorded)
        
        return recorded
    
    # ========== ORDER EVENT HANDLING ==========
    
    async def process_order_update(self, order_data: Dict) -> List[ManagementAction]:
        """
        Process order update from exchange.
        
        This routes events to the state machine and returns follow-up actions.
        """
        self.metrics["events_processed"] += 1
        
        exchange_order_id = order_data.get("id")
        client_order_id = order_data.get("clientOrderId") or self._order_id_map.get(exchange_order_id)
        
        if not client_order_id:
            logger.warning(f"Unknown order update: {exchange_order_id}")
            return []
        
        pending = self._pending_orders.get(client_order_id)
        if not pending:
            logger.warning(f"No pending order for client_order_id: {client_order_id}")
            return []
        
        # Determine event type
        status = order_data.get("status", "").lower()
        # Handle None values: exchange may return null instead of 0
        filled_raw = order_data.get("filled")
        remaining_raw = order_data.get("remaining")
        filled = Decimal(str(filled_raw if filled_raw is not None else 0))
        remaining = Decimal(str(remaining_raw if remaining_raw is not None else 0))
        fill_delta = filled - pending.last_filled_qty
        if fill_delta < 0:
            logger.warning(
                "Order filled quantity moved backwards; resetting delta baseline",
                order_id=exchange_order_id,
                client_order_id=client_order_id,
                previous=str(pending.last_filled_qty),
                current=str(filled),
            )
            fill_delta = filled
        
        if status in {"closed", "filled"} and filled > 0:
            if fill_delta <= 0:
                pending.status = "filled"
                return []
            event_type = OrderEventType.FILLED
        elif fill_delta > 0 and remaining > 0:
            event_type = OrderEventType.PARTIAL_FILL
        elif status == "canceled":
            event_type = OrderEventType.CANCELLED
        elif status == "rejected":
            event_type = OrderEventType.REJECTED
        elif status == "open":
            event_type = OrderEventType.ACKNOWLEDGED
        else:
            return []  # No meaningful event
        
        next_seq = pending.last_event_seq + 1
        trades = order_data.get("trades") or []
        raw_fill_id = trades[0].get("id") if trades and isinstance(trades[0], dict) else None
        fill_id = str(raw_fill_id) if raw_fill_id is not None else None

        if self._event_enforcer and not self._event_enforcer.should_process_event(
            exchange_order_id, next_seq, fill_id
        ):
            return []

        event = OrderEvent(
            order_id=exchange_order_id,
            client_order_id=client_order_id,
            event_type=event_type,
            event_seq=next_seq,
            timestamp=datetime.now(timezone.utc),
            fill_qty=fill_delta if event_type in (OrderEventType.FILLED, OrderEventType.PARTIAL_FILL) else None,
            fill_price=Decimal(str(order_data.get("average") or 0)) if fill_delta > 0 else None,
            fill_id=fill_id,
        )
        
        follow_up = self.position_manager.handle_order_event(pending.symbol, event)
        if self._on_partial_close and event_type in (OrderEventType.FILLED, OrderEventType.PARTIAL_FILL):
            if client_order_id and (client_order_id.startswith("tp1-") or client_order_id.startswith("tp2-")):
                self._on_partial_close(pending.symbol)
        
        if self._event_enforcer:
            self._event_enforcer.mark_processed(exchange_order_id, next_seq, fill_id)
        pending.last_event_seq = next_seq
        
        if event_type == OrderEventType.FILLED:
            pending.status = "filled"
            pending.last_filled_qty = filled
            self.metrics["orders_filled"] += 1
            self._wal_mark_completed(client_order_id)
        elif event_type == OrderEventType.PARTIAL_FILL:
            pending.last_filled_qty = filled
        elif event_type == OrderEventType.CANCELLED:
            pending.status = "cancelled"
            self.metrics["orders_cancelled"] += 1
        elif event_type == OrderEventType.REJECTED:
            pending.status = "rejected"
            self.metrics["orders_rejected"] += 1
        
        position = self.registry.get_position_any_state(pending.symbol)
        if position:
            self.persistence.save_position(position)
            await self._maybe_record_trade(position)
        
        for action in follow_up:
            await self.execute_action(action)
        
        return follow_up

    async def poll_and_process_order_updates(self) -> int:
        """
        Poll order status for pending entry AND stop orders, process fills.

        Entry orders: detect fills → trigger PLACE_STOP (SL/TP).
        Stop orders:  detect fills → transition position to CLOSED.

        Without stop polling, the system cannot distinguish "stop filled
        (expected)" from "stop disappeared (danger)", which leads to false
        NAKED POSITION kill-switch triggers.

        Returns number of orders processed (fill/cancel/reject).
        """
        fetch_order = getattr(self.client, "fetch_order", None)
        if not fetch_order:
            return 0

        # Purposes we actively poll for status changes
        _POLLABLE_PURPOSES = {
            OrderPurpose.ENTRY,
            OrderPurpose.STOP_INITIAL,
            OrderPurpose.STOP_UPDATE,
            OrderPurpose.EXIT_STOP,
            OrderPurpose.EXIT_MARKET,
        }

        processed = 0
        for pending in list(self._pending_orders.values()):
            if pending.purpose not in _POLLABLE_PURPOSES:
                continue
            if pending.status not in ("pending", "submitted"):
                continue
            oid = pending.exchange_order_id
            if not oid:
                continue
            sym = pending.exchange_symbol or pending.symbol
            try:
                order_data = await fetch_order(oid, sym)
            except InvariantError:
                raise  # Safety violation — must propagate
            except (OperationalError, DataError) as e:
                logger.debug("poll order fetch failed", order_id=oid, symbol=sym, error=str(e), error_type=type(e).__name__)
                continue
            if not order_data:
                continue
            if not order_data.get("clientOrderId"):
                order_data["clientOrderId"] = pending.client_order_id
            try:
                follow_up = await self.process_order_update(order_data)
                if follow_up:
                    processed += 1
            except InvariantError:
                raise  # Safety violation — must propagate
            except (OperationalError, DataError) as e:
                logger.warning("process_order_update failed", order_id=oid, error=str(e), error_type=type(e).__name__)
        return processed
    
    # ========== EMERGENCY / BYPASS ORDERS ==========
    
    async def place_emergency_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        size,
        price=None,
        stop_price=None,
        reduce_only: bool = True,
        reason: str = "emergency",
    ) -> ExecutionResult:
        """
        Place an order through the gateway without a ManagementAction.
        
        Used by self-heal paths (missing stops, ShockGuard) that previously
        bypassed the gateway.  Enforces:
        - startup readiness (READY only)
        - circuit breaker (via client)
        - WAL logging
        - metrics
        
        Does NOT require a ManagedPosition or ManagementAction.
        """
        # Enforce startup gate
        if self._startup_machine is not None and not self._startup_machine.is_ready:
            logger.error(
                "Emergency order blocked: system not ready",
                symbol=symbol,
                side=side,
                order_type=order_type,
                reason=reason,
                phase=self._startup_machine.phase.value,
            )
            return ExecutionResult(
                success=False,
                client_order_id=f"emergency-{reason}",
                error=f"System not ready (phase={self._startup_machine.phase.value})",
            )

        # P0.2: Global order rate limit (emergency orders count too)
        self._order_rate_limiter.check_and_record()

        import uuid
        client_oid = f"emg-{reason}-{uuid.uuid4().hex[:8]}"
        self._wal_record_raw_intent(client_oid, reason, symbol=symbol, side=side, size=str(size))
        
        try:
            params: Dict = {"reduceOnly": reduce_only}
            if stop_price is not None:
                params["stopPrice"] = float(stop_price)
            
            result = await self.client.create_order(
                symbol=symbol,
                type=order_type,
                side=side,
                amount=float(size),
                price=float(price) if price else None,
                params=params,
            )
            
            exchange_oid = result.get("id")
            self.metrics["orders_placed"] += 1

            # Register in _pending_orders so poll_and_process_order_updates detects the fill.
            # Without this, the position stays in the registry until reconciliation marks it
            # orphaned -- losing the fill data and P&L for trade recording.
            purpose = OrderPurpose.EXIT_STOP if reason in ("emergency_stop", "missing_stop", "shock_guard") else OrderPurpose.EXIT_MARKET
            pending_order_type = _normalize_pending_order_type(order_type)

            pending = PendingOrder(
                client_order_id=client_oid,
                position_id=symbol,
                symbol=symbol,
                purpose=purpose,
                side=Side(side) if side in ("long", "short") else Side.SHORT,
                size=Decimal(str(size)),
                price=Decimal(str(stop_price)) if stop_price is not None else (Decimal(str(price)) if price is not None else None),
                order_type=pending_order_type,
                submitted_at=datetime.now(timezone.utc),
                exchange_order_id=exchange_oid,
                exchange_symbol=symbol,
            )
            self._pending_orders[client_oid] = pending
            if exchange_oid:
                self._order_id_map[exchange_oid] = client_oid

            logger.info(
                "Emergency order placed",
                symbol=symbol,
                side=side,
                order_type=order_type,
                size=str(size),
                reason=reason,
                exchange_order_id=exchange_oid,
                registered_for_polling=True,
            )
            return ExecutionResult(
                success=True,
                client_order_id=client_oid,
                exchange_order_id=exchange_oid,
            )

        except InvariantError:
            raise  # Safety violation — propagate
        except (OperationalError, DataError) as e:
            self.metrics["errors"] += 1
            logger.error(
                "Emergency order failed",
                symbol=symbol,
                reason=reason,
                error=str(e),
                error_type=type(e).__name__,
            )
            return ExecutionResult(
                success=False,
                client_order_id=client_oid,
                error=str(e),
            )
    
    def _wal_record_raw_intent(self, client_oid: str, reason: str, **kwargs) -> None:
        """Record a raw intent to WAL for emergency orders."""
        if self._wal:
            try:
                self._wal.record_intent(ActionIntent(
                    intent_id=client_oid,
                    position_id=kwargs.get("symbol", "unknown"),
                    action_type=reason,
                    symbol=kwargs.get("symbol", "unknown"),
                    side=kwargs.get("side", "unknown"),
                    size=str(kwargs.get("size", "0")),
                    price=str(kwargs.get("price")) if kwargs.get("price") is not None else None,
                    created_at=datetime.now(timezone.utc),
                    status=ActionIntentStatus.PENDING,
                ))
            except Exception as wal_err:
                logger.warning(
                    "WAL write failed for emergency order (proceeding without WAL)",
                    client_oid=client_oid,
                    reason=reason,
                    error=str(wal_err),
                )
    
    # ========== BATCH OPERATIONS ==========
    
    async def execute_actions(self, actions: List[ManagementAction]) -> List[ExecutionResult]:
        """Execute multiple actions in priority order."""
        # Sort by priority
        actions.sort(key=lambda a: a.priority, reverse=True)
        
        results = []
        for action in actions:
            result = await self.execute_action(action)
            results.append(result)
        
        return results
    
    async def sync_with_exchange(self) -> Dict:
        """
        Sync state with exchange.
        
        Returns dict of issues found.
        """
        # Get exchange state
        positions = await self.client.get_all_futures_positions()
        orders = await self.client.get_futures_open_orders()
        
        # Convert to reconciliation format
        exchange_positions = {
            p['symbol']: {
                'side': p.get('side', 'unknown'),
                'qty': str(p.get('contracts', p.get('size', 0))),
                'entry_price': str(p.get('entryPrice', 0))
            }
            for p in positions
            if float(p.get('contracts', p.get('size', 0))) != 0
        }
        
        # Reconcile (this marks orphaned positions and moves them to closed)
        issues = self.registry.reconcile_with_exchange(exchange_positions, orders)
        
        # Persist any orphaned positions (now in closed history)
        # This ensures they're saved as ORPHANED and won't be reloaded as ACTIVE
        orphaned_count = 0
        qty_synced_count = 0
        for symbol, issue in issues:
            if "ORPHANED" in issue:
                # Find in closed positions (just moved there by reconcile)
                for pos in self.registry._closed_positions:
                    if pos.symbol == symbol and pos.state.value == "orphaned":
                        self.persistence.save_position(pos)
                        orphaned_count += 1
                        break
            elif "QTY_SYNCED" in issue:
                pos = self.registry.get_position(symbol)
                if pos:
                    self.persistence.save_position(pos)
                    qty_synced_count += 1
                else:
                    # Reconciliation may close/move the position to history.
                    for closed_pos in self.registry._closed_positions:
                        if closed_pos.symbol == symbol:
                            self.persistence.save_position(closed_pos)
                            qty_synced_count += 1
                            break
        
        if orphaned_count > 0:
            logger.info("Persisted orphaned positions", count=orphaned_count)
        if qty_synced_count > 0:
            logger.warning("Persisted quantity-synced positions", count=qty_synced_count)
        
        # Record trades for any positions that reconciliation closed or orphaned
        recordable_issues = ("STALE", "QTY_SYNCED", "ORPHANED")
        for symbol, issue in issues:
            if any(tag in issue for tag in recordable_issues):
                for closed_pos in self.registry._closed_positions:
                    if closed_pos.symbol == symbol and closed_pos.state in (
                        PositionState.CLOSED, PositionState.ORPHANED
                    ):
                        await self._maybe_record_trade(closed_pos)
                        break
        
        # Auto-import phantom positions at runtime (not just startup).
        # Without this, a symbol-normalization miss or race condition can
        # leave an exchange position unprotected until the next restart.
        has_phantoms = any("PHANTOM" in issue for _, issue in issues)
        if has_phantoms:
            try:
                await self._import_phantom_positions()
            except InvariantError:
                raise
            except Exception as e:
                logger.error(
                    "Runtime phantom import failed",
                    error=str(e),
                    error_type=type(e).__name__,
                )
        
        # Get corrective actions
        actions = self.position_manager.reconcile(exchange_positions, orders, issues=issues)
        
        # Execute corrective actions
        for action in actions:
            await self.execute_action(action)
        
        return {
            "issues": issues,
            "actions_taken": len(actions),
            "exchange_positions": len(exchange_positions),
            "registry_positions": len(self.registry.get_all_active())
        }
    
    # ========== STARTUP / RECOVERY ==========
    
    async def startup(self) -> None:
        """
        Startup procedure.
        
        0. Registry hygiene: if exchange is flat, wipe stale registry
        1. Load persisted state from SQLite
        2. Sync with exchange (orphan/qty reconciliation)
        3. Import phantom positions
        4. Cross-validate with Postgres (P1.5 Option A: dual-DB fix)
        5. Persist reconciled state to SQLite (single source of truth after startup)
        """
        logger.info("ExecutionGateway starting up...")
        
        # 1. Load persisted registry from SQLite
        persisted_registry = self.persistence.load_registry()
        
        # Merge into current registry
        for pos in persisted_registry.get_all():
            if pos.symbol not in self.registry._positions:
                self.registry._positions[pos.symbol] = pos
        
        # 0. REGISTRY HYGIENE: if exchange is provably flat, wipe stale registry.
        # Exchange is always source of truth. If the exchange has zero
        # positions AND zero open orders but the registry carried forward
        # stale entries from a prior run, clear them now to prevent the
        # orphan → kill-switch cascade.
        active_count = len(self.registry.get_all_active())
        if active_count > 0:
            try:
                exchange_positions = await self.client.get_all_futures_positions()
                live_count = sum(
                    1 for p in exchange_positions
                    if float(p.get("contracts", p.get("size", 0))) != 0
                )
                open_orders = await self.client.get_futures_open_orders()
                order_count = len(open_orders) if open_orders else 0

                if live_count == 0 and order_count == 0:
                    closed = self.registry.hard_reset(
                        reason=f"exchange flat (0 positions, 0 orders) "
                               f"but registry had {active_count} stale entries"
                    )
                    for pos in closed:
                        self.persistence.save_position(pos)
                elif live_count == 0 and order_count > 0:
                    logger.warning(
                        "Registry hygiene: exchange has 0 positions but %d "
                        "open orders — skipping wipe (possible pending state)",
                        order_count,
                        registry_positions=active_count,
                    )
            except Exception as e:
                logger.error(
                    "Registry hygiene check failed — proceeding with normal reconciliation",
                    error=str(e),
                    error_type=type(e).__name__,
                )
        
        # 2. Sync with exchange (handles orphans, qty mismatches)
        sync_result = await self.sync_with_exchange()
        
        # 3. Auto-import phantom positions (exchange has, registry doesn't)
        await self._import_phantom_positions()
        
        # 4. Cross-validate with Postgres and enrich SQLite positions
        enriched = self._enrich_from_postgres()
        
        # 5. Persist fully reconciled state to SQLite
        self.persistence.save_registry(self.registry)
        
        logger.info(
            "ExecutionGateway startup complete",
            positions=len(self.registry.get_all_active()),
            issues=len(sync_result.get("issues", [])),
            pg_enriched=enriched,
        )
    
    def _enrich_from_postgres(self) -> int:
        """
        P1.5 Option A: Cross-validate SQLite positions against Postgres.
        
        For each active position in the registry, checks the Postgres `positions`
        table for supplementary data. If SQLite is missing key fields (entry price,
        stop, TP levels) that Postgres has, backfill them. Logs any discrepancies
        between the two databases.
        
        Returns the number of positions enriched.
        """
        enriched_count = 0
        try:
            from src.storage.repository import get_active_positions
            pg_positions = get_active_positions()
        except Exception as e:
            logger.warning(
                "Postgres enrichment skipped (DB unavailable or no positions)",
                error=str(e),
            )
            return 0
        
        # Build Postgres lookup by normalized symbol
        from src.data.symbol_utils import normalize_symbol_for_position_match
        pg_by_norm: dict = {}
        for pg_pos in pg_positions:
            norm = normalize_symbol_for_position_match(pg_pos.symbol)
            pg_by_norm[norm] = pg_pos
        
        for pos in self.registry.get_all_active():
            norm = normalize_symbol_for_position_match(pos.symbol)
            pg_pos = pg_by_norm.get(norm)
            if not pg_pos:
                continue
            
            changed = False
            
            # Backfill missing entry price from Postgres
            if (not pos.initial_entry_price or pos.initial_entry_price == 0) and pg_pos.entry_price:
                logger.info(
                    "ENRICH: backfilling entry price from Postgres",
                    symbol=pos.symbol,
                    pg_entry_price=str(pg_pos.entry_price),
                )
                pos.initial_entry_price = pg_pos.entry_price
                changed = True
            
            # Backfill missing stop price from Postgres
            if (not pos.current_stop_price or pos.current_stop_price == 0) and pg_pos.initial_stop_price:
                logger.info(
                    "ENRICH: backfilling stop price from Postgres",
                    symbol=pos.symbol,
                    pg_stop_price=str(pg_pos.initial_stop_price),
                )
                pos.current_stop_price = pg_pos.initial_stop_price
                if not pos.initial_stop_price:
                    pos.initial_stop_price = pg_pos.initial_stop_price
                changed = True
            
            # Backfill missing stop order ID from Postgres
            if not pos.stop_order_id and pg_pos.stop_loss_order_id:
                logger.info(
                    "ENRICH: backfilling stop_order_id from Postgres",
                    symbol=pos.symbol,
                    pg_stop_order_id=pg_pos.stop_loss_order_id,
                )
                pos.stop_order_id = pg_pos.stop_loss_order_id
                changed = True
            
            # Backfill missing TP prices from Postgres
            if (not pos.initial_tp1_price or pos.initial_tp1_price == 0) and pg_pos.tp1_price:
                pos.initial_tp1_price = pg_pos.tp1_price
                changed = True
            if (not pos.initial_tp2_price or pos.initial_tp2_price == 0) and pg_pos.tp2_price:
                pos.initial_tp2_price = pg_pos.tp2_price
                changed = True
            
            # Log any significant entry price discrepancy (SQLite vs Postgres)
            if pos.initial_entry_price and pg_pos.entry_price:
                sqlite_ep = pos.initial_entry_price
                pg_ep = pg_pos.entry_price
                if pg_ep > 0:
                    drift_pct = abs(sqlite_ep - pg_ep) / pg_ep
                    if drift_pct > self.PRICE_DRIFT_ALERT_PCT:
                        logger.warning(
                            "DRIFT: SQLite vs Postgres entry price mismatch >0.1%",
                            symbol=pos.symbol,
                            sqlite_entry=str(sqlite_ep),
                            pg_entry=str(pg_ep),
                            drift_pct=f"{float(drift_pct):.4%}",
                        )
            
            if changed:
                self.persistence.save_position(pos)
                enriched_count += 1
        
        if enriched_count > 0:
            logger.info(
                "Postgres enrichment complete",
                positions_enriched=enriched_count,
                pg_positions_available=len(pg_positions),
            )
        
        return enriched_count
    
    async def _import_phantom_positions(self) -> None:
        """
        Import positions that exist on exchange but not in registry.
        
        This handles the case where the bot was restarted and lost in-memory state.
        Also handles stale positions (remaining_qty=0) by replacing them.
        """
        from src.execution.position_state_machine import ManagedPosition, PositionState, FillRecord, Side
        from src.data.symbol_utils import normalize_symbol_for_position_match
        from datetime import datetime, timezone
        
        try:
            positions = await self.client.get_all_futures_positions()
            orders = await self.client.get_futures_open_orders()
        except InvariantError:
            raise  # Safety violation — must propagate
        except (OperationalError, DataError) as e:
            logger.error("Failed to fetch positions for phantom import", error=str(e), error_type=type(e).__name__)
            return
        
        imported = 0
        for pos in positions:
            symbol = pos.get("symbol", "")
            size = abs(float(pos.get("size", pos.get("contracts", 0))))
            
            if size == 0:
                continue
            
            # Check if already in registry - use normalized matching
            normalized_key = normalize_symbol_for_position_match(symbol)
            existing = self.registry.get_position(symbol)
            stale_symbols_to_remove = []
            
            # Also check for normalized matches across all registry positions
            for reg_symbol in list(self.registry._positions.keys()):
                if normalize_symbol_for_position_match(reg_symbol) == normalized_key:
                    reg_pos = self.registry._positions.get(reg_symbol)
                    if reg_pos and reg_pos.remaining_qty > 0:
                        # Found a valid existing position, skip import
                        existing = reg_pos
                        break
                    elif reg_pos and reg_pos.remaining_qty <= 0:
                        # Stale position, mark for removal
                        stale_symbols_to_remove.append(reg_symbol)
            
            if existing and existing.remaining_qty > 0:
                continue
            
            # Remove any stale positions with same normalized key
            for stale_symbol in stale_symbols_to_remove:
                logger.warning("Removing stale position before phantom import", 
                              stale_symbol=stale_symbol, new_symbol=symbol)
                with self.registry._lock:
                    stale_pos = self.registry._positions.pop(stale_symbol, None)
                    if stale_pos:
                        self.registry._closed_positions.append(stale_pos)
            
            # Need to import
            logger.warning("Importing phantom position from exchange", symbol=symbol, size=size)
            
            side_str = pos.get("side", "long").lower()
            side = Side.LONG if side_str == "long" else Side.SHORT
            raw_entry = pos.get("entryPrice", pos.get("entry_price", 0))
            try:
                entry_price = Decimal(str(raw_entry))
            except (ValueError, TypeError, ArithmeticError) as parse_err:
                logger.critical(
                    "PHANTOM_IMPORT_SKIPPED: unparseable entry price",
                    symbol=symbol,
                    raw_entry=str(raw_entry),
                    error=str(parse_err),
                )
                continue
            if entry_price <= 0:
                logger.critical(
                    "PHANTOM_IMPORT_SKIPPED: zero/negative entry price",
                    symbol=symbol,
                    entry_price=str(entry_price),
                )
                continue
            qty = Decimal(str(size))
            
            # Find stop order
            stop_order = None
            stop_price = None
            stop_id = None
            
            for order in orders:
                order_symbol = order.get("symbol", "")
                order_type = order.get("type", "").lower()
                is_reduce = order.get("reduceOnly", False)
                
                # Normalize symbols for matching
                sym1 = symbol.replace("PF_", "").replace("USD", "").upper()
                sym2 = order_symbol.replace("/USD:USD", "").replace("/USD", "").replace("_", "").upper()
                
                if sym1 in sym2 and "stop" in order_type and is_reduce:
                    stop_order = order
                    break
            
            if stop_order:
                raw_price = stop_order.get("price") or stop_order.get("stopPrice") or stop_order.get("triggerPrice") or 0
                try:
                    stop_price = Decimal(str(raw_price)) if raw_price else None
                except (ValueError, TypeError, ArithmeticError):
                    stop_price = None
                stop_id = stop_order.get("id")
            
            PHANTOM_DEFAULT_STOP_PCT = Decimal("0.02")  # 2% emergency stop for imported positions with no stop on exchange
            if not stop_price or stop_price == 0:
                logger.warning(
                    "PHANTOM_IMPORT: no stop order found on exchange — using default 2% emergency stop",
                    symbol=symbol,
                    entry_price=str(entry_price),
                )
                if side == Side.LONG:
                    stop_price = entry_price * (1 - PHANTOM_DEFAULT_STOP_PCT)
                else:
                    stop_price = entry_price * (1 + PHANTOM_DEFAULT_STOP_PCT)
            
            # Create position
            pid = f"pos-{symbol.replace('/', '')}-import-{int(datetime.now().timestamp())}"
            
            managed_pos = ManagedPosition(
                symbol=symbol,
                side=side,
                position_id=pid,
                initial_size=qty,
                initial_entry_price=entry_price,
                initial_stop_price=stop_price,
                initial_tp1_price=None,
                initial_tp2_price=None,
                initial_final_target=None,
            )
            
            managed_pos.entry_acknowledged = True
            managed_pos.intent_confirmed = True
            managed_pos.state = PositionState.PROTECTED if stop_id else PositionState.OPEN
            managed_pos.current_stop_price = stop_price
            managed_pos.stop_order_id = stop_id
            managed_pos.setup_type = "AUTO_IMPORT"
            managed_pos.trade_type = "UNKNOWN"
            
            # Add fill record
            fill = FillRecord(
                fill_id=f"import-fill-{pid}",
                order_id="AUTO_IMPORT",
                side=side,
                qty=qty,
                price=entry_price,
                timestamp=datetime.now(timezone.utc),
                is_entry=True,
            )
            managed_pos.entry_fills.append(fill)
            managed_pos.ensure_snapshot_targets()
            
            try:
                self.registry.register_position(managed_pos)
                self.persistence.save_position(managed_pos)
                imported += 1
                logger.info("Phantom position imported", symbol=symbol, qty=str(qty), stop=str(stop_price))
            except (OperationalError, DataError, KeyError, ValueError) as e:
                logger.critical(
                    "PHANTOM_IMPORT_FAILED: exchange position not tracked — manual check required",
                    symbol=symbol,
                    error=str(e),
                    error_type=type(e).__name__,
                )
                try:
                    from src.monitoring.alert_dispatcher import send_alert
                    import asyncio
                    await send_alert(
                        "PHANTOM_IMPORT_FAILED",
                        f"Failed to import exchange position {symbol}: {e}\n"
                        f"This position EXISTS on exchange but is NOT tracked by the bot. "
                        f"Manual intervention required.",
                        urgent=True,
                    )
                except Exception:
                    pass
        
        if imported > 0:
            logger.info("Phantom positions imported", count=imported)
    
    def get_metrics(self) -> Dict:
        """Get gateway metrics."""
        return {
            **self.metrics,
            "pending_orders": len(self._pending_orders),
            "active_positions": len(self.registry.get_all_active()),
            "manager_metrics": self.position_manager.metrics,
            "orders_blocked_by_rate_limit_total": self._order_rate_limiter.orders_blocked_total,
            "orders_per_minute_current": self._order_rate_limiter.orders_last_minute,
            "orders_per_10s_current": self._order_rate_limiter.orders_last_10s,
        }
