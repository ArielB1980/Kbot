"""
Position Manager v2 - Production Grade.

State-machine-driven position management with:
1. All decisions go through PositionRegistry (single source of truth)
2. Order events drive state transitions (not intent)
3. Idempotent event handling
4. Shadow mode support for comparison
"""
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import List, Optional, Dict, Tuple
from datetime import datetime, timezone
import uuid
import os

from src.execution.position_state_machine import (
    ManagedPosition,
    PositionState,
    PositionRegistry,
    ExitReason,
    OrderEvent,
    OrderEventType,
    get_position_registry,
    check_invariant
)
from src.execution.instrument_specs import InstrumentSpecRegistry
from src.data.symbol_utils import position_symbol_matches_order
from src.domain.models import Side, OrderType, Signal, SignalType
from src.monitoring.logger import get_logger

logger = get_logger(__name__)


class ActionType(str, Enum):
    """Types of management actions."""
    # Entry Actions
    OPEN_POSITION = "open_position"
    CANCEL_ENTRY = "cancel_entry"
    
    # Exit Actions
    CLOSE_FULL = "close_full"
    CLOSE_PARTIAL = "close_partial"
    
    # Stop Management
    PLACE_STOP = "place_stop"
    UPDATE_STOP = "update_stop"
    CANCEL_STOP = "cancel_stop"
    
    # TP Management
    PLACE_TP = "place_tp"
    CANCEL_TP = "cancel_tp"
    
    # Reconciliation
    FLATTEN_ORPHAN = "flatten_orphan"
    SYNC_STOP = "sync_stop"
    
    # State Updates
    STATE_UPDATE = "state_update"
    
    # Rejections
    REJECT_ENTRY = "reject_entry"
    REJECT_ACTION = "reject_action"
    
    # No Action
    NO_ACTION = "no_action"


@dataclass
class ManagementAction:
    """
    Action to be executed by the execution layer.
    
    The PositionManager only DECIDES - it does not execute.
    The Execution Gateway executes and reports back via OrderEvents.
    """
    type: ActionType
    symbol: str
    reason: str
    
    # For entries/exits
    side: Optional[Side] = None
    size: Optional[Decimal] = None
    price: Optional[Decimal] = None
    leverage: Optional[Decimal] = None
    order_type: OrderType = OrderType.MARKET
    
    # Order identification (for tracking)
    client_order_id: Optional[str] = None
    position_id: Optional[str] = None
    
    # Exit reason tracking
    exit_reason: Optional[ExitReason] = None
    
    # Priority (higher = execute first)
    priority: int = 0
    
    # Metadata for decision tracking
    decision_timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def __post_init__(self):
        if self.client_order_id is None:
            self.client_order_id = f"psm-{uuid.uuid4().hex[:12]}"


@dataclass
class DecisionTick:
    """
    Record of a single decision tick for metrics / debugging.
    """
    timestamp: datetime
    symbol: str
    current_price: Decimal
    position_state: Optional[str]
    position_id: Optional[str]
    remaining_qty: Optional[Decimal]
    current_stop: Optional[Decimal]
    actions: List[ManagementAction]
    reason_codes: List[str]
    
    def to_dict(self) -> Dict:
        return {
            "timestamp": self.timestamp.isoformat(),
            "symbol": self.symbol,
            "current_price": str(self.current_price),
            "position_state": self.position_state,
            "position_id": self.position_id,
            "remaining_qty": str(self.remaining_qty) if self.remaining_qty else None,
            "current_stop": str(self.current_stop) if self.current_stop else None,
            "actions": [{"type": a.type.value, "reason": a.reason} for a in self.actions],
            "reason_codes": self.reason_codes
        }


class PositionManagerV2:
    """
    State-Machine-Driven Position Manager.
    
    EXECUTION MODEL:
    1. Receive price update / order event
    2. Evaluate rules against current state
    3. Return prioritized list of actions
    4. Execution Gateway places orders with client_order_id linking to position_id
    5. Order events reported back via apply_order_event()
    6. State transitions are driven by acknowledged fills, not intent
    """
    
    def __init__(
        self,
        registry: Optional[PositionRegistry] = None,
        multi_tp_config=None,
        instrument_spec_registry: Optional[InstrumentSpecRegistry] = None,
    ):
        """
        Initialize with optional custom registry and multi-TP config.
        
        Args:
            registry: Position registry (uses singleton if not provided)
            multi_tp_config: Optional MultiTPConfig for runner mode settings
            instrument_spec_registry: Optional registry for venue min_size; used to guard partial closes
        """
        self.registry = registry or get_position_registry()
        self._multi_tp_config = multi_tp_config
        self._instrument_spec_registry = instrument_spec_registry
        
        # Decision history for metrics / debugging
        self.decision_history: List[DecisionTick] = []
        self.max_history = 10000
        
        # Safety Components
        from src.execution.production_safety import (
            ExitTimeoutManager, 
            SafetyConfig, 
            PositionProtectionMonitor, 
            ProtectionEnforcer
        )
        self.safety_config = SafetyConfig()
        self.exit_timeout_manager = ExitTimeoutManager(self.safety_config)
        # Note: PositionProtectionMonitor requires client/gateway, usually run externally or injected.
        # We'll leave monitor for the outer loop, but manage timeouts here.
        
        # Configuration
        
        # Configuration
        self.tp1_partial_pct = Decimal("0.5")
        self.tp2_partial_pct = Decimal("0.25")
        self.trailing_atr_multiple = Decimal("1.5")
        
        # Metrics
        self.metrics = {
            "opens": 0,
            "closes": 0,
            "reversals_attempted": 0,
            "stop_moves": 0,
            "blocked_duplicates": 0,
            "errors": 0
        }
    
    def _get_min_size_for_partial(self, symbol: str) -> Decimal:
        """
        Get venue minimum size for partial closes. Used to avoid ORDER_REJECTED_BY_VENUE.
        Uses get_effective_min_size (includes VENUE_MIN_OVERRIDES for known Kraken symbols).
        """
        if not self._instrument_spec_registry:
            return Decimal("1")
        return self._instrument_spec_registry.get_effective_min_size(symbol)
    
    # ========== ENTRY EVALUATION ==========
    
    def evaluate_entry(
        self,
        signal: Signal,
        entry_price: Decimal,
        stop_price: Decimal,
        tp1_price: Optional[Decimal],
        tp2_price: Optional[Decimal],
        final_target: Optional[Decimal],
        position_size: Decimal,
        trade_type: str = "tight_smc",
        leverage: Optional[Decimal] = None,
    ) -> Tuple[ManagementAction, Optional[ManagedPosition]]:
        """
        Evaluate whether a new position can be opened.
        
        Returns:
            (action, position) - Action to execute and the prepared position object
        """
        symbol = signal.symbol
        side = Side.LONG if signal.signal_type == SignalType.LONG else Side.SHORT
        
        # SAFETY CHECK: New entries enabled?
        if os.environ.get("TRADING_NEW_ENTRIES_ENABLED", "true").lower() != "true":
            return ManagementAction(
                type=ActionType.REJECT_ENTRY,
                symbol=symbol,
                reason="Global Switch: NEW_ENTRIES_ENABLED=False",
                side=side,
                priority=-1
            ), None
        
        # Check if position can be opened
        can_open, reason = self.registry.can_open_position(symbol, side)
        
        if not can_open:
            self.metrics["blocked_duplicates"] += 1
            logger.warning("Entry REJECTED", symbol=symbol, side=side.value, reason=reason)
            
            return ManagementAction(
                type=ActionType.REJECT_ENTRY,
                symbol=symbol,
                reason=reason,
                side=side,
                priority=-1
            ), None
        
        # Validate stop
        if stop_price is None:
            return ManagementAction(
                type=ActionType.REJECT_ENTRY,
                symbol=symbol,
                reason="NO STOP PRICE DEFINED",
                side=side,
                priority=-1
            ), None
        
        # Validate stop direction
        if side == Side.LONG and stop_price >= entry_price:
            return ManagementAction(
                type=ActionType.REJECT_ENTRY,
                symbol=symbol,
                reason=f"LONG stop ({stop_price}) must be below entry ({entry_price})",
                side=side,
                priority=-1
            ), None
        if side == Side.SHORT and stop_price <= entry_price:
            return ManagementAction(
                type=ActionType.REJECT_ENTRY,
                symbol=symbol,
                reason=f"SHORT stop ({stop_price}) must be above entry ({entry_price})",
                side=side,
                priority=-1
            ), None
        
        # Create position object (not registered until entry acknowledged)
        position_id = f"pos-{uuid.uuid4().hex[:12]}"
        client_order_id = f"entry-{position_id}"
        
        # Determine runner mode settings from multi_tp config
        mtp = self._multi_tp_config
        runner_mode = False
        tp1_close_pct = Decimal("0.40")
        tp2_close_pct = Decimal("0.40")
        runner_pct = Decimal("0.20")
        final_target_behavior = "tighten_trail"
        tighten_trail_atr_mult = Decimal("1.2")
        
        if mtp and getattr(mtp, "enabled", False):
            runner_has_fixed_tp = getattr(mtp, "runner_has_fixed_tp", False)
            runner_mode = not runner_has_fixed_tp and mtp.runner_pct > 0
            tp1_close_pct = Decimal(str(mtp.tp1_close_pct))
            tp2_close_pct = Decimal(str(mtp.tp2_close_pct))
            runner_pct = Decimal(str(mtp.runner_pct))
            final_target_behavior = getattr(mtp, "final_target_behavior", "tighten_trail")
            tighten_trail_atr_mult = Decimal(str(
                getattr(mtp, "tighten_trail_at_final_target_atr_mult", 1.2)
            ))
            
            # Regime-aware sizing: override pcts based on signal regime
            regime_sizing = getattr(mtp, "regime_runner_sizing_enabled", False)
            regime_overrides = getattr(mtp, "regime_runner_overrides", {})
            signal_regime = signal.regime if hasattr(signal, 'regime') else None
            
            if regime_sizing and runner_mode and signal_regime and signal_regime in regime_overrides:
                ov = regime_overrides[signal_regime]
                tp1_close_pct = Decimal(str(ov.get("tp1_close_pct", float(tp1_close_pct))))
                tp2_close_pct = Decimal(str(ov.get("tp2_close_pct", float(tp2_close_pct))))
                runner_pct = Decimal(str(ov.get("runner_pct", float(runner_pct))))
                logger.info(
                    "Regime-aware sizing for position",
                    symbol=symbol,
                    regime=signal_regime,
                    tp1_pct=str(tp1_close_pct),
                    tp2_pct=str(tp2_close_pct),
                    runner_pct=str(runner_pct),
                )
        
        position = ManagedPosition(
            symbol=symbol,
            side=side,
            position_id=position_id,
            initial_size=position_size,
            initial_entry_price=entry_price,
            initial_stop_price=stop_price,
            initial_tp1_price=tp1_price,
            initial_tp2_price=tp2_price,
            initial_final_target=final_target,
            setup_type=signal.setup_type.value if hasattr(signal, 'setup_type') else None,
            regime=signal.regime if hasattr(signal, 'regime') else None,
            trade_type=trade_type,
            runner_mode=runner_mode,
            tp1_close_pct=tp1_close_pct,
            tp2_close_pct=tp2_close_pct,
            runner_pct=runner_pct,
            final_target_behavior=final_target_behavior,
            tighten_trail_atr_mult=tighten_trail_atr_mult,
        )
        position.entry_order_id = client_order_id
        position.entry_client_order_id = client_order_id
        
        self.metrics["opens"] += 1
        logger.info(
            "Entry APPROVED",
            symbol=symbol,
            side=side.value,
            size=str(position_size),
            entry=str(entry_price),
            stop=str(stop_price),
            position_id=position_id
        )
        
        return ManagementAction(
            type=ActionType.OPEN_POSITION,
            symbol=symbol,
            reason="Entry criteria met",
            side=side,
            size=position_size,
            price=entry_price,
            leverage=leverage,
            client_order_id=client_order_id,
            position_id=position_id,
            priority=10
        ), position
    
    # ========== POSITION EVALUATION ==========
    
    def evaluate_position(
        self,
        symbol: str,
        current_price: Decimal,
        current_atr: Optional[Decimal] = None,
        premise_invalidated: bool = False,
        confirmation_condition_met: bool = False,
        confirmation_price: Optional[Decimal] = None
    ) -> List[ManagementAction]:
        """
        Evaluate all rules for an active position.
        
        RULE PRIORITY (highest to lowest):
        1. STOP HIT → Immediate close (ABSOLUTE)
        2. PREMISE INVALIDATION → Immediate close
        3. FINAL TARGET HIT → Behavior depends on config:
           - close_full (legacy): Full close
           - tighten_trail (default runner): Tighten trailing stop
           - close_partial: Close ~50% of runner
        4. TP2 HIT → Partial close
        5. TP1 HIT → Partial close + conditional BE
        6. TRAILING STOP UPDATE → Move stop toward profit
        7. NO ACTION
        
        Intent confirmation (BE gate for tight trades):
        Set intent_confirmed when confirmation_condition_met=True or when
        current_price crosses confirmation_price (BOS level, prior swing, etc.).
        Caller should compute this from structure/candles.
        
        Returns:
            Prioritized list of actions (execute in order)
        """
        actions: List[ManagementAction] = []
        reason_codes: List[str] = []
        
        position = self.registry.get_position(symbol)
        
        # No position or in-flight
        if position is None:
            return []
        
        if position.state in (PositionState.PENDING, PositionState.EXIT_PENDING, 
                              PositionState.CANCEL_PENDING):
            reason_codes.append(f"IN_FLIGHT:{position.state.value}")
            self._record_decision(symbol, current_price, position, actions, reason_codes)
            return []
        
        if position.is_terminal:
            return []
        
        # ========== INTENT CONFIRMATION (market confirmation, not entry ACK) ==========
        if not position.intent_confirmed:
            price_crossed = False
            if confirmation_price is not None:
                if position.side == Side.LONG:
                    price_crossed = current_price >= confirmation_price
                else:
                    price_crossed = current_price <= confirmation_price
            if confirmation_condition_met or price_crossed:
                if position.confirm_intent():
                    reason_codes.append("INTENT_CONFIRMED")
        
        # ========== RULE 2: STOP HIT (ABSOLUTE PRIORITY) ==========
        if position.check_stop_hit(current_price):
            reason_codes.append("STOP_HIT")
            exit_reason = ExitReason.TRAILING_STOP if position.trailing_active else ExitReason.STOP_LOSS
            
            logger.critical(
                "🛑 STOP HIT - IMMEDIATE EXIT",
                symbol=symbol,
                stop_price=str(position.current_stop_price),
                current_price=str(current_price)
            )
            
            client_order_id = f"exit-stop-{position.position_id}"
            
            actions.append(ManagementAction(
                type=ActionType.CLOSE_FULL,
                symbol=symbol,
                reason=f"Stop Hit ({position.current_stop_price})",
                side=position.side,
                size=position.remaining_qty,
                price=current_price,
                order_type=OrderType.MARKET,
                client_order_id=client_order_id,
                position_id=position.position_id,
                exit_reason=exit_reason,
                priority=100
            ))
            
            self._record_decision(symbol, current_price, position, actions, reason_codes)
            return actions
        
        # ========== RULE 3: PREMISE INVALIDATION ==========
        if premise_invalidated:
            reason_codes.append("PREMISE_INVALIDATED")
            client_order_id = f"exit-premise-{position.position_id}"
            
            actions.append(ManagementAction(
                type=ActionType.CLOSE_FULL,
                symbol=symbol,
                reason="Premise Invalidated",
                side=position.side,
                size=position.remaining_qty,
                order_type=OrderType.MARKET,
                client_order_id=client_order_id,
                position_id=position.position_id,
                exit_reason=ExitReason.PREMISE_INVALIDATION,
                priority=90
            ))
            
            self._record_decision(symbol, current_price, position, actions, reason_codes)
            return actions
        
        # ========== RULE 11: FINAL TARGET HIT ==========
        if position.check_final_target_hit(current_price):
            reason_codes.append("FINAL_TARGET_HIT")
            
            # Determine behavior: runner mode uses configurable behavior, legacy always closes full
            behavior = position.final_target_behavior if position.runner_mode else "close_full"
            
            if behavior == "close_full":
                # Legacy: close full position
                client_order_id = f"exit-final-{position.position_id}"
                actions.append(ManagementAction(
                    type=ActionType.CLOSE_FULL,
                    symbol=symbol,
                    reason=f"Final Target Hit ({position.initial_final_target})",
                    side=position.side,
                    size=position.remaining_qty,
                    order_type=OrderType.MARKET,
                    client_order_id=client_order_id,
                    position_id=position.position_id,
                    exit_reason=ExitReason.TAKE_PROFIT_FINAL,
                    priority=80
                ))
                self._record_decision(symbol, current_price, position, actions, reason_codes)
                return actions
            
            elif behavior == "tighten_trail":
                # Tighten trailing stop at final target, do NOT close
                if not position.final_target_touched:
                    position.final_target_touched = True
                    reason_codes.append("TIGHTEN_TRAIL_AT_FINAL")
                    logger.info(
                        "Final target touched - tightening trail (not closing)",
                        symbol=symbol,
                        final_target=str(position.initial_final_target),
                        current_price=str(current_price),
                    )
                    if position.trailing_active and current_atr:
                        tighter_mult = position.tighten_trail_atr_mult
                        new_trail = self._calculate_trailing_stop(
                            position, current_price, current_atr,
                            atr_mult_override=tighter_mult,
                        )
                        if new_trail and position._validate_stop_move(new_trail):
                            client_order_id = f"stop-tighten-final-{position.position_id}"
                            actions.append(ManagementAction(
                                type=ActionType.UPDATE_STOP,
                                symbol=symbol,
                                reason=f"Tighten trail at final target ({position.initial_final_target})",
                                side=position.side,
                                price=new_trail,
                                client_order_id=client_order_id,
                                position_id=position.position_id,
                                priority=75
                            ))
                            self.metrics["stop_moves"] += 1
                # Do NOT return early -- allow subsequent rules (trailing, etc.) to run
            
            elif behavior == "close_partial":
                # Close ~50% of remaining runner at final target.
                # Root cause fix: Kraken (and many venues) require min order size (often 1 contract);
                # sending 0.5 causes "amount must be greater than minimum amount precision of 1".
                if not position.final_target_touched:
                    position.final_target_touched = True
                    partial_size = position.remaining_qty * Decimal("0.5")
                    spec_symbol = position.futures_symbol or symbol
                    min_size = self._get_min_size_for_partial(spec_symbol)
                    # Only emit partial close if size meets venue minimum (per InstrumentSpec or 1).
                    # Skip dust partials to avoid ORDER_REJECTED_BY_VENUE / Partial close failed.
                    if partial_size > 0 and partial_size >= min_size:
                        client_order_id = f"exit-final-partial-{position.position_id}"
                        reason_codes.append("FINAL_TARGET_CLOSE_PARTIAL")
                        actions.append(ManagementAction(
                            type=ActionType.CLOSE_PARTIAL,
                            symbol=symbol,
                            reason=f"Final Target Partial Close ({position.initial_final_target})",
                            side=position.side,
                            size=partial_size,
                            order_type=OrderType.MARKET,
                            client_order_id=client_order_id,
                            position_id=position.position_id,
                            exit_reason=ExitReason.TAKE_PROFIT_FINAL,
                            priority=75
                        ))
                        logger.info(
                            "Final target touched - closing partial runner",
                            symbol=symbol,
                            partial_size=str(partial_size),
                        )
                # Do NOT return early -- allow subsequent rules to run
        
        # ========== RULE 10.5: PROGRESSIVE TRAILING (R-based tightening) ==========
        if (
            position.runner_mode
            and position.trailing_active
            and current_atr
            and self._multi_tp_config
            and getattr(self._multi_tp_config, 'progressive_trail_enabled', False)
        ):
            prog_levels = getattr(self._multi_tp_config, 'progressive_trail_levels', [])
            # Compute current R-multiple
            entry_ref = position.avg_entry_price or position.initial_entry_price
            if position.initial_stop_price and entry_ref:
                risk_per_unit = abs(entry_ref - position.initial_stop_price)
                if risk_per_unit > 0:
                    if position.side == Side.LONG:
                        current_r = (current_price - entry_ref) / risk_per_unit
                    else:
                        current_r = (entry_ref - current_price) / risk_per_unit
                    
                    # Check each level (sorted by r_threshold ascending)
                    sorted_levels = sorted(prog_levels, key=lambda x: x.get("r_threshold", 0))
                    for idx, level in enumerate(sorted_levels):
                        r_thresh = Decimal(str(level.get("r_threshold", 999)))
                        atr_m = Decimal(str(level.get("atr_mult", 2.0)))
                        
                        if current_r >= r_thresh and idx > position.highest_r_tighten_level:
                            # New R-level reached: tighten trail
                            position.highest_r_tighten_level = idx
                            position.current_trail_atr_mult = atr_m
                            reason_codes.append(f"PROGRESSIVE_TRAIL_{float(r_thresh):.0f}R")
                            
                            new_trail = self._calculate_trailing_stop(
                                position, current_price, current_atr,
                                atr_mult_override=atr_m,
                            )
                            if new_trail and position._validate_stop_move(new_trail):
                                client_order_id = f"stop-prog-trail-{float(r_thresh):.0f}r-{position.position_id}"
                                actions.append(ManagementAction(
                                    type=ActionType.UPDATE_STOP,
                                    symbol=symbol,
                                    reason=f"Progressive trail tighten at {float(r_thresh):.1f}R (ATR×{float(atr_m):.1f})",
                                    side=position.side,
                                    price=new_trail,
                                    client_order_id=client_order_id,
                                    position_id=position.position_id,
                                    priority=76  # Between final target (75) and TP2 (70)
                                ))
                                self.metrics["stop_moves"] += 1
                                logger.info(
                                    "Progressive trail tightened",
                                    symbol=symbol,
                                    r_level=f"{float(r_thresh):.1f}R",
                                    atr_mult=f"{float(atr_m):.1f}",
                                    new_stop=str(new_trail),
                                    current_price=str(current_price),
                                )
        
        # ========== RULE 10: TP2 HIT ==========
        if position.check_tp2_hit(current_price):
            if os.environ.get("TRADING_PARTIALS_ENABLED", "true").lower() != "true":
                reason_codes.append("TP2_HIT_IGNORED")
            else:
                reason_codes.append("TP2_HIT")
                if position.tp2_qty_target is not None:
                    partial_size = min(position.tp2_qty_target, position.remaining_qty)
                else:
                    partial_size = position.remaining_qty * self.tp2_partial_pct
                spec_symbol = position.futures_symbol or symbol
                min_size = self._get_min_size_for_partial(spec_symbol)
                if partial_size >= min_size:
                    client_order_id = f"exit-tp2-{position.position_id}"
                    actions.append(ManagementAction(
                        type=ActionType.CLOSE_PARTIAL,
                        symbol=symbol,
                        reason=f"TP2 Hit ({position.initial_tp2_price})",
                        side=position.side,
                        size=partial_size,
                        order_type=OrderType.MARKET,
                        client_order_id=client_order_id,
                        position_id=position.position_id,
                        exit_reason=ExitReason.TAKE_PROFIT_2,
                        priority=70
                    ))
                else:
                    reason_codes.append("TP2_HIT_SKIP_BELOW_MIN")
                    logger.debug(
                        "TP2 partial skip: size below venue min",
                        symbol=symbol,
                        partial_size=str(partial_size),
                        min_size=str(min_size),
                    )
        
        # ========== RULE 5: TP1 HIT ==========
        if position.check_tp1_hit(current_price):
            if os.environ.get("TRADING_PARTIALS_ENABLED", "true").lower() != "true":
                reason_codes.append("TP1_HIT_IGNORED")
            else:
                reason_codes.append("TP1_HIT")
                if position.tp1_qty_target is not None:
                    partial_size = min(position.tp1_qty_target, position.remaining_qty)
                else:
                    partial_size = position.remaining_qty * self.tp1_partial_pct
                spec_symbol = position.futures_symbol or symbol
                min_size = self._get_min_size_for_partial(spec_symbol)
                if partial_size >= min_size:
                    client_order_id = f"exit-tp1-{position.position_id}"
                    actions.append(ManagementAction(
                        type=ActionType.CLOSE_PARTIAL,
                        symbol=symbol,
                        reason=f"TP1 Hit ({position.initial_tp1_price})",
                        side=position.side,
                        size=partial_size,
                        order_type=OrderType.MARKET,
                        client_order_id=client_order_id,
                        position_id=position.position_id,
                        exit_reason=ExitReason.TAKE_PROFIT_1,
                        priority=60
                    ))
                    reason_codes.append("TP1_PARTIAL_QUEUED")
                else:
                    reason_codes.append("TP1_HIT_SKIP_BELOW_MIN")
                    logger.debug(
                        "TP1 partial skip: size below venue min",
                        symbol=symbol,
                        partial_size=str(partial_size),
                        min_size=str(min_size),
                    )
        
        # ========== TRAILING ACTIVATION (guard at TP1) ==========
        if position.tp1_filled and not position.trailing_active and current_atr:
            atr_min = Decimal("0")
            if self._multi_tp_config:
                atr_min = Decimal(str(getattr(self._multi_tp_config, "trailing_activation_atr_min", 0)))
            position.activate_trailing_if_guard_passes(current_atr, atr_min)

        # ========== RULE 9: TRAILING STOP ==========
        if (position.break_even_triggered or position.trailing_active) and current_atr:
            if os.environ.get("TRADING_TRAILING_ENABLED", "true").lower() == "true":
                # Use progressive trail ATR mult if set, otherwise default
                trail_override = position.current_trail_atr_mult if position.current_trail_atr_mult is not None else None
                new_trail = self._calculate_trailing_stop(position, current_price, current_atr, atr_mult_override=trail_override)
                
                if new_trail and position._validate_stop_move(new_trail):
                    pct_move = abs(new_trail - position.current_stop_price) / position.current_stop_price
                    if pct_move > Decimal("0.001"):  # 0.1% min move
                        reason_codes.append("TRAILING_UPDATE")
                        client_order_id = f"stop-trail-{position.position_id}"
                        
                        actions.append(ManagementAction(
                            type=ActionType.UPDATE_STOP,
                            symbol=symbol,
                            reason=f"Trailing Stop Update",
                            side=position.side,
                            price=new_trail,
                            client_order_id=client_order_id,
                            position_id=position.position_id,
                            priority=20
                        ))
                        self.metrics["stop_moves"] += 1
        
        # Sort by priority
        actions.sort(key=lambda a: a.priority, reverse=True)
        
        if not reason_codes:
            reason_codes.append("NO_ACTION")
        
        self._record_decision(symbol, current_price, position, actions, reason_codes)
        
        return actions
    
    def _calculate_trailing_stop(
        self,
        position: ManagedPosition,
        current_price: Decimal,
        current_atr: Decimal,
        atr_mult_override: Optional[Decimal] = None
    ) -> Optional[Decimal]:
        """Calculate trailing stop using ATR.
        
        Args:
            atr_mult_override: If provided, overrides self.trailing_atr_multiple.
                Used for tightening trail at final target.
        """
        mult = atr_mult_override if atr_mult_override is not None else self.trailing_atr_multiple
        trail_distance = current_atr * mult
        
        if position.side == Side.LONG:
            new_stop = current_price - trail_distance
            if new_stop <= position.current_stop_price:
                return None
            if position.break_even_triggered and position.avg_entry_price:
                if new_stop < position.avg_entry_price:
                    return None
            return new_stop
        else:
            new_stop = current_price + trail_distance
            if new_stop >= position.current_stop_price:
                return None
            if position.break_even_triggered and position.avg_entry_price:
                if new_stop > position.avg_entry_price:
                    return None
            return new_stop
    
    # ========== ORDER EVENT HANDLING ==========
    
    def handle_order_event(self, symbol: str, event: OrderEvent) -> List[ManagementAction]:
        """
        Handle order event and potentially trigger follow-up actions.
        
        This is the feedback loop from Execution Gateway.
        State transitions are driven by events, not intent.
        """
        result = self.registry.apply_order_event(symbol, event)
        if not result:
            return []  # Duplicate or N/A
        
        position = self.registry.get_position(symbol)
        if position is None:
            return []
        
        follow_up_actions: List[ManagementAction] = []
        
        # Handle entry acknowledgement → place stop
        if event.event_type == OrderEventType.ACKNOWLEDGED:
            if event.order_id == position.entry_order_id:
                # Entry ack → stop placement will happen after fill
                pass
        
        # Handle entry fill → place stop and TP orders
        if event.event_type in (OrderEventType.FILLED, OrderEventType.PARTIAL_FILL):
            if event.order_id == position.entry_order_id:
                # Entry filled (or partial) → ensure stop is placed
                if not position.stop_order_id:
                    client_order_id = f"stop-initial-{position.position_id}"
                    follow_up_actions.append(ManagementAction(
                        type=ActionType.PLACE_STOP,
                        symbol=symbol,
                        reason="Initial stop after entry fill",
                        side=position.side,
                        price=position.current_stop_price,
                        size=position.remaining_qty,
                        client_order_id=client_order_id,
                        position_id=position.position_id,
                        priority=100
                    ))
                
                # Place TP orders after entry fill
                # Use snapshot targets when available (avoids partial sizing drift)
                position.ensure_snapshot_targets()
                filled_entry = position.filled_entry_qty
                
                if position.initial_tp1_price and not position.tp1_order_id:
                    if position.tp1_qty_target is not None:
                        tp1_size = min(position.tp1_qty_target, position.remaining_qty)
                    elif position.runner_mode:
                        tp1_size = filled_entry * position.tp1_close_pct
                    else:
                        tp1_size = position.remaining_qty * position.partial_close_pct
                    # Safety: never exceed remaining
                    tp1_size = min(tp1_size, position.remaining_qty)
                    if tp1_size > 0:
                        tp1_client_id = f"tp1-{position.position_id}"
                        follow_up_actions.append(ManagementAction(
                            type=ActionType.PLACE_TP,
                            symbol=symbol,
                            reason="TP1 after entry fill",
                            side=position.side,
                            price=position.initial_tp1_price,
                            size=tp1_size,
                            client_order_id=tp1_client_id,
                            position_id=position.position_id,
                            priority=95
                        ))
                        logger.info(
                            "Queuing TP1 placement",
                            symbol=symbol,
                            price=str(position.initial_tp1_price),
                            size=str(tp1_size),
                            runner_mode=position.runner_mode,
                        )
                
                if position.initial_tp2_price and not position.tp2_order_id:
                    if position.tp2_qty_target is not None:
                        tp2_size = min(position.tp2_qty_target, position.remaining_qty)
                    elif position.runner_mode:
                        tp2_size = filled_entry * position.tp2_close_pct
                    else:
                        # Legacy: TP2 gets the remaining portion after TP1
                        tp2_size = position.remaining_qty * (Decimal("1") - position.partial_close_pct)
                    # Safety: never exceed remaining (account for TP1 that was just queued)
                    tp2_size = min(tp2_size, position.remaining_qty)
                    if tp2_size > 0:
                        tp2_client_id = f"tp2-{position.position_id}"
                        follow_up_actions.append(ManagementAction(
                            type=ActionType.PLACE_TP,
                            symbol=symbol,
                            reason="TP2 after entry fill",
                            side=position.side,
                            price=position.initial_tp2_price,
                            size=tp2_size,
                            client_order_id=tp2_client_id,
                            position_id=position.position_id,
                            priority=94
                        ))
                        logger.info(
                            "Queuing TP2 placement",
                            symbol=symbol,
                            price=str(position.initial_tp2_price),
                            size=str(tp2_size),
                            runner_mode=position.runner_mode,
                        )
        
        # Handle exit fill → check for BE trigger
        if event.event_type in (OrderEventType.FILLED, OrderEventType.PARTIAL_FILL):
            if event.order_id != position.entry_order_id:
                # Exit fill → check conditional BE
                if position.tp1_filled and position.should_trigger_break_even():
                    if position.trigger_break_even():
                        client_order_id = f"stop-be-{position.position_id}"
                        follow_up_actions.append(ManagementAction(
                            type=ActionType.UPDATE_STOP,
                            symbol=symbol,
                            reason="Break-even after TP1 fill (conditional)",
                            side=position.side,
                            price=position.avg_entry_price,
                            client_order_id=client_order_id,
                            position_id=position.position_id,
                            priority=90
                        ))
                        self.metrics["stop_moves"] += 1
        
        # Handle full close
        if position.is_terminal:
            self.metrics["closes"] += 1
            # Cancel any remaining TP orders if position closed
            if position.tp1_order_id and not position.tp1_filled:
                follow_up_actions.append(ManagementAction(
                    type=ActionType.CANCEL_TP,
                    symbol=symbol,
                    reason="Position closed, cancel TP",
                    client_order_id=position.tp1_order_id,
                    position_id=position.position_id,
                    priority=50
                ))
        
        return follow_up_actions
    
    # ========== REVERSAL HANDLING ==========
    
    def request_reversal(
        self,
        symbol: str,
        new_side: Side,
        current_price: Decimal
    ) -> List[ManagementAction]:
        """
        Request position close for direction reversal.
        """
        position = self.registry.get_position(symbol)
        if position is None:
            return []
        
        if position.side == new_side:
            return []  # Not a reversal
            
        # SAFETY CHECK: Reversals enabled?
        if os.environ.get("TRADING_REVERSALS_ENABLED", "true").lower() != "true":
            logger.warning("Reversal BLOCKED by Global Switch", symbol=symbol)
            return []
        
        # Register reversal intent
        self.registry.request_reversal(symbol, new_side)
        self.metrics["reversals_attempted"] += 1
        
        client_order_id = f"exit-reversal-{position.position_id}"
        
        return [ManagementAction(
            type=ActionType.CLOSE_FULL,
            symbol=symbol,
            reason=f"Direction reversal: {position.side.value} → {new_side.value}",
            side=position.side,
            size=position.remaining_qty,
            price=current_price,
            order_type=OrderType.MARKET,
            client_order_id=client_order_id,
            position_id=position.position_id,
            exit_reason=ExitReason.DIRECTION_REVERSAL,
            priority=95
        )]
    
    # ========== RECONCILIATION ==========
    
    @staticmethod
    def _has_live_reduce_only_stop_for_symbol(
        symbol: str,
        side: Side,
        exchange_orders: List[Dict],
    ) -> bool:
        """Return True when a live, stop-like reduce-only order exists for symbol."""
        expected_stop_side = "sell" if side == Side.LONG else "buy"
        for order in exchange_orders or []:
            order_symbol = str(order.get("symbol") or "")
            if not position_symbol_matches_order(symbol, order_symbol):
                continue
            info = order.get("info") or {}
            otype = str(
                order.get("type")
                or info.get("orderType")
                or info.get("type")
                or ""
            ).lower()
            if "take_profit" in otype or "take-profit" in otype:
                continue
            has_stop_shape = (
                order.get("stopPrice") is not None
                or order.get("triggerPrice") is not None
                or info.get("stopPrice") is not None
                or info.get("triggerPrice") is not None
                or any(t in otype for t in ("stop", "stp", "stop_loss", "stop-loss"))
            )
            if not has_stop_shape:
                continue
            order_side = str(order.get("side") or "").lower()
            if order_side and order_side != expected_stop_side:
                continue
            reduce_only_present = any(
                k in order or k in info for k in ("reduceOnly", "reduce_only")
            )
            reduce_only = (
                order.get("reduceOnly")
                or order.get("reduce_only")
                or info.get("reduceOnly")
                or info.get("reduce_only")
            )
            if reduce_only_present and not reduce_only:
                continue
            status = str(order.get("status") or "").lower()
            if status in {"open", "new", "untouched", "entered_book", "partiallyfilled", "partial"}:
                return True
        return False
    
    def reconcile(
        self,
        exchange_positions: Dict[str, Dict],
        exchange_orders: List[Dict],
        issues: Optional[List[Tuple[str, str]]] = None,
    ) -> List[ManagementAction]:
        """
        Reconcile with exchange and return corrective actions.
        """
        if issues is None:
            issues = self.registry.reconcile_with_exchange(exchange_positions, exchange_orders)
        actions: List[ManagementAction] = []
        stop_bind_enqueued: set[str] = set()
        
        for symbol, issue in issues:
            if "ORPHANED" in issue:
                # Registry thinks position exists, exchange doesn't
                # This is dangerous - position might have been liquidated
                pos = self.registry.get_position(symbol)
                if pos:
                    pos.mark_orphaned()
                    actions.append(ManagementAction(
                        type=ActionType.NO_ACTION,
                        symbol=symbol,
                        reason=f"ORPHANED: {issue}",
                        priority=0
                    ))
                    self.metrics["errors"] += 1
            
            elif "PHANTOM" in issue:
                # Exchange has position we don't know about.
                # DO NOT auto-flatten: these are likely real positions that survived
                # a restart but weren't persisted. Let _import_phantom_positions()
                # and production_takeover handle them safely with proper stop placement.
                logger.warning(
                    "PHANTOM position detected - deferring to import/takeover",
                    symbol=symbol,
                    issue=issue,
                )
                actions.append(ManagementAction(
                    type=ActionType.NO_ACTION,
                    symbol=symbol,
                    reason=f"PHANTOM: deferred to import/takeover - {issue}",
                    priority=0
                ))
                self.metrics["errors"] += 1
            
            elif "QTY_MISMATCH" in issue:
                # Qty mismatch - log but don't auto-correct
                logger.error(f"QTY MISMATCH: {symbol} - {issue}")
                self.metrics["errors"] += 1
            
            elif "PENDING_ADOPTED" in issue or "QTY_SYNCED" in issue:
                # Reconcile/adopt just mutated exposure. Ensure a protective stop is
                # explicitly bound in the same reconciliation cycle.
                pos = self.registry.get_position(symbol)
                if not pos or pos.is_terminal or pos.remaining_qty <= 0:
                    continue
                if pos.symbol in stop_bind_enqueued:
                    continue
                stop_already_live = self._has_live_reduce_only_stop_for_symbol(
                    pos.symbol, pos.side, exchange_orders
                )
                if stop_already_live:
                    continue
                if not pos.current_stop_price or pos.current_stop_price <= 0:
                    logger.warning(
                        "RECONCILE_STOP_BIND_SKIPPED_NO_STOP_PRICE",
                        symbol=pos.symbol,
                        position_id=pos.position_id,
                        issue=issue,
                    )
                    continue
                action = ManagementAction(
                    type=ActionType.PLACE_STOP,
                    symbol=pos.symbol,
                    reason=f"Reconcile stop bind after {issue.split(':', 1)[0]}",
                    side=pos.side,
                    size=pos.remaining_qty,
                    price=pos.current_stop_price,
                    client_order_id=f"stop-reconcile-{pos.position_id}",
                    position_id=pos.position_id,
                    priority=99,
                )
                actions.append(action)
                stop_bind_enqueued.add(pos.symbol)
                logger.warning(
                    "RECONCILE_ENSURE_STOP_BIND_QUEUED",
                    symbol=pos.symbol,
                    position_id=pos.position_id,
                    reason=issue,
                    stop_price=str(pos.current_stop_price),
                    qty=str(pos.remaining_qty),
                )
        
        return actions
    
    # ========== DECISION HISTORY ==========
    
    def _record_decision(
        self,
        symbol: str,
        current_price: Decimal,
        position: Optional[ManagedPosition],
        actions: List[ManagementAction],
        reason_codes: List[str]
    ) -> None:
        """Record decision tick for metrics / debugging."""
        tick = DecisionTick(
            timestamp=datetime.now(timezone.utc),
            symbol=symbol,
            current_price=current_price,
            position_state=position.state.value if position else None,
            position_id=position.position_id if position else None,
            remaining_qty=position.remaining_qty if position else None,
            current_stop=position.current_stop_price if position else None,
            actions=actions,
            reason_codes=reason_codes
        )
        
        self.decision_history.append(tick)
        
        # Trim history
        if len(self.decision_history) > self.max_history:
            self.decision_history = self.decision_history[-self.max_history:]
    
    def get_decision_metrics(self) -> Dict:
        """Get metrics from decision history (counts, state distribution)."""
        return {
            "total_decisions": len(self.decision_history),
            "metrics": self.metrics.copy(),
            "action_counts": self._count_actions(),
            "state_distribution": self._state_distribution()
        }
    
    def _count_actions(self) -> Dict[str, int]:
        """Count actions by type in history."""
        counts: Dict[str, int] = {}
        for tick in self.decision_history:
            for action in tick.actions:
                counts[action.type.value] = counts.get(action.type.value, 0) + 1
        return counts
    
    def _state_distribution(self) -> Dict[str, int]:
        """Count state occurrences in history."""
        states: Dict[str, int] = {}
        for tick in self.decision_history:
            if tick.position_state:
                states[tick.position_state] = states.get(tick.position_state, 0) + 1
        return states
    
    def export_decision_history(self, limit: int = 1000) -> List[Dict]:
        """Export decision history for analysis."""
        return [t.to_dict() for t in self.decision_history[-limit:]]

    # ========== SAFETY & MAINTENANCE ==========
    
    def check_safety(self) -> List[ManagementAction]:
        """
        Run periodic safety checks.
        
        1. Exit Timeouts & Escalation
        """
        from src.execution.production_safety import ExitEscalationLevel
        
        actions: List[ManagementAction] = []
        
        # 1. Update Exit Timeout States
        # Ensure we are tracking all pending exits
        for pos in self.registry.get_all_active():
            if pos.state == PositionState.EXIT_PENDING:
                self.exit_timeout_manager.start_exit_tracking(pos)
        
        # 2. Check Timeouts
        escalations = self.exit_timeout_manager.check_timeouts()
        
        for state in escalations:
            new_level = self.exit_timeout_manager.escalate(state.symbol)
            
            if new_level in (ExitEscalationLevel.AGGRESSIVE, ExitEscalationLevel.EMERGENCY):
                # Fetch position to get side (needed for ManagementAction, though simple close shouldn't need it if robust)
                pos = self.registry.get_position(state.symbol)
                side = pos.side if pos else Side.LONG # Fallback
                
                # Escalate to Market Close
                actions.append(ManagementAction(
                    type=ActionType.CLOSE_FULL,
                    symbol=state.symbol,
                    reason=f"Exit Timeout: Escalating to {new_level.value}",
                    side=side, 
                    order_type=OrderType.MARKET,
                    priority=200 # Higher than signal exits
                ))
                
            elif new_level == ExitEscalationLevel.QUARANTINE:
                logger.critical("QUARANTINING SYMBOL due to Exit Timeout", symbol=state.symbol)
                # Could emit a quarantine action if supported
        
        return actions
