"""
Position Manager v2 - Production Grade.

State-machine-driven position management with:
1. All decisions go through PositionRegistry (single source of truth)
2. Order events drive state transitions (not intent)
3. Idempotent event handling
4. Shadow mode support for comparison
"""
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
import os
import uuid

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
from src.execution.instrument_specs import InstrumentSpecRegistry
from src.data.symbol_utils import position_symbol_matches_order
from src.domain.models import OrderType, Side, Signal, SignalType
from src.monitoring.logger import get_logger
from src.monitoring.alert_dispatcher import send_alert_sync

# Re-export models so existing imports continue to work
from src.execution.position_manager_models import (  # noqa: F401
    ActionType,
    DecisionTick,
    ManagementAction,
)
from src.execution.position_evaluator import PositionEvaluatorMixin

logger = get_logger(__name__)


class PositionManagerV2(PositionEvaluatorMixin):
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
        strategy_config: Optional[Any] = None,
        institutional_memory: Optional[Any] = None,
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
        self._strategy_config = strategy_config
        self._institutional_memory = institutional_memory

        # Decision history for metrics / debugging
        self.decision_history: List[DecisionTick] = []
        self.max_history = 10000

        # Safety Components
        from src.execution.production_safety import (
            ExitTimeoutManager,
            SafetyConfig,
        )
        self.safety_config = SafetyConfig()
        self.exit_timeout_manager = ExitTimeoutManager(self.safety_config)

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
            "errors": 0,
        }

    def _get_min_size_for_partial(self, symbol: str) -> Decimal:
        """
        Get venue minimum size for partial closes. Used to avoid ORDER_REJECTED_BY_VENUE.
        Uses get_effective_min_size (includes VENUE_MIN_OVERRIDES for known Kraken symbols).
        """
        if not self._instrument_spec_registry:
            return Decimal("1")
        return self._instrument_spec_registry.get_effective_min_size(symbol)

    def _thesis_management_enabled(self, symbol: str) -> bool:
        cfg = self._strategy_config
        if cfg is None:
            return False
        if not bool(getattr(cfg, "memory_enabled", False)):
            return False
        if bool(getattr(cfg, "thesis_observe_only", True)):
            return False
        if not bool(getattr(cfg, "thesis_management_enabled", False)):
            return False
        canary = set(getattr(cfg, "thesis_canary_symbols", []) or [])
        return (not canary) or (symbol in canary)

    def _thesis_alerts_enabled(self) -> bool:
        cfg = self._strategy_config
        if cfg is None:
            return False
        if not bool(getattr(cfg, "memory_enabled", False)):
            return False
        return bool(getattr(cfg, "thesis_alerts_enabled", False))

    def _get_conviction_snapshot(self, symbol: str, current_price: Decimal) -> Optional[Dict[str, Any]]:
        if not self._institutional_memory:
            return None
        return self._institutional_memory.update_conviction_for_symbol(
            symbol,
            current_price=current_price,
            current_volume_avg=None,
            emit_log=False,
        )

    def _conviction_partial_factor(self, conviction: Optional[float]) -> Decimal:
        if conviction is None or self._strategy_config is None:
            return Decimal("1")
        neutral = float(getattr(self._strategy_config, "thesis_score_neutral_conviction", 60.0))
        if conviction >= neutral + 15.0:
            return Decimal(str(getattr(self._strategy_config, "thesis_partial_reduce_high_conviction_factor", 0.85)))
        if conviction <= neutral - 20.0:
            return Decimal(str(getattr(self._strategy_config, "thesis_partial_reduce_low_conviction_factor", 1.25)))
        return Decimal("1")

    def _conviction_trail_factor(self, conviction: Optional[float]) -> Decimal:
        if conviction is None or self._strategy_config is None:
            return Decimal("1")
        neutral = float(getattr(self._strategy_config, "thesis_score_neutral_conviction", 60.0))
        if conviction >= neutral + 15.0:
            return Decimal(str(getattr(self._strategy_config, "thesis_high_conviction_relax_factor", 1.1)))
        if conviction <= neutral - 20.0:
            return Decimal(str(getattr(self._strategy_config, "thesis_low_conviction_tighten_factor", 0.75)))
        return Decimal("1")

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
                priority=-1,
            ), None

        # Check if position can be opened
        if self._thesis_management_enabled(symbol):
            conviction_snapshot = self._get_conviction_snapshot(symbol, entry_price)
            conviction = float(conviction_snapshot["conviction"]) if conviction_snapshot else None
            if conviction is not None and self._institutional_memory.should_block_reentry(symbol, conviction):
                if self._thesis_alerts_enabled():
                    send_alert_sync(
                        "THESIS_REENTRY_BLOCKED",
                        (
                            f"[THESIS] {symbol} re-entry blocked at conviction {conviction:.1f}%\n"
                            f"Threshold: {float(getattr(self._strategy_config, 'thesis_reentry_block_threshold', 25.0)):.1f}%"
                        ),
                        rate_limit_key=f"THESIS_REENTRY_BLOCKED:{symbol}",
                        rate_limit_seconds=1800,
                    )
                return ManagementAction(
                    type=ActionType.REJECT_ENTRY,
                    symbol=symbol,
                    reason=f"Thesis re-entry blocked (conviction={conviction:.1f})",
                    side=side,
                    priority=-1,
                ), None

        can_open, reason = self.registry.can_open_position(symbol, side)

        if not can_open:
            self.metrics["blocked_duplicates"] += 1
            logger.warning("Entry REJECTED", symbol=symbol, side=side.value, reason=reason)

            return ManagementAction(
                type=ActionType.REJECT_ENTRY,
                symbol=symbol,
                reason=reason,
                side=side,
                priority=-1,
            ), None

        # Validate stop
        if stop_price is None:
            return ManagementAction(
                type=ActionType.REJECT_ENTRY,
                symbol=symbol,
                reason="NO STOP PRICE DEFINED",
                side=side,
                priority=-1,
            ), None

        # Validate stop direction
        if side == Side.LONG and stop_price >= entry_price:
            return ManagementAction(
                type=ActionType.REJECT_ENTRY,
                symbol=symbol,
                reason=f"LONG stop ({stop_price}) must be below entry ({entry_price})",
                side=side,
                priority=-1,
            ), None
        if side == Side.SHORT and stop_price <= entry_price:
            return ManagementAction(
                type=ActionType.REJECT_ENTRY,
                symbol=symbol,
                reason=f"SHORT stop ({stop_price}) must be above entry ({entry_price})",
                side=side,
                priority=-1,
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
            signal_regime = signal.regime if hasattr(signal, "regime") else None

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
            setup_type=signal.setup_type.value if hasattr(signal, "setup_type") else None,
            regime=signal.regime if hasattr(signal, "regime") else None,
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
            position_id=position_id,
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
            priority=10,
        ), position

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

        # Handle entry acknowledgement -> place stop
        if event.event_type == OrderEventType.ACKNOWLEDGED:
            if event.order_id == position.entry_order_id:
                # Entry ack -> stop placement will happen after fill
                pass

        # Handle entry fill -> place stop and TP orders
        if event.event_type in (OrderEventType.FILLED, OrderEventType.PARTIAL_FILL):
            if event.order_id == position.entry_order_id:
                self._handle_entry_fill(position, symbol, follow_up_actions)

        # Handle exit fill -> check for BE trigger
        if event.event_type in (OrderEventType.FILLED, OrderEventType.PARTIAL_FILL):
            if event.order_id != position.entry_order_id:
                # Exit fill -> check conditional BE
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
                            priority=90,
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
                    priority=50,
                ))

        return follow_up_actions

    def _handle_entry_fill(
        self,
        position: ManagedPosition,
        symbol: str,
        follow_up_actions: List[ManagementAction],
    ) -> None:
        """Queue stop and TP orders after an entry fill."""
        # Ensure stop is placed
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
                priority=100,
            ))

        # Place TP orders after entry fill
        position.ensure_snapshot_targets()
        filled_entry = position.filled_entry_qty

        if position.initial_tp1_price and not position.tp1_order_id:
            if position.tp1_qty_target is not None:
                tp1_size = min(position.tp1_qty_target, position.remaining_qty)
            elif position.runner_mode:
                tp1_size = filled_entry * position.tp1_close_pct
            else:
                tp1_size = position.remaining_qty * position.partial_close_pct
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
                    priority=95,
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
                tp2_size = position.remaining_qty * (Decimal("1") - position.partial_close_pct)
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
                    priority=94,
                ))
                logger.info(
                    "Queuing TP2 placement",
                    symbol=symbol,
                    price=str(position.initial_tp2_price),
                    size=str(tp2_size),
                    runner_mode=position.runner_mode,
                )

    # ========== REVERSAL HANDLING ==========

    def request_reversal(
        self,
        symbol: str,
        new_side: Side,
        current_price: Decimal,
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
            priority=95,
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
                pos = self.registry.get_position(symbol)
                if pos:
                    pos.mark_orphaned()
                    actions.append(ManagementAction(
                        type=ActionType.NO_ACTION,
                        symbol=symbol,
                        reason=f"ORPHANED: {issue}",
                        priority=0,
                    ))
                    self.metrics["errors"] += 1

            elif "PHANTOM" in issue:
                logger.warning(
                    "PHANTOM position detected - deferring to import/takeover",
                    symbol=symbol,
                    issue=issue,
                )
                actions.append(ManagementAction(
                    type=ActionType.NO_ACTION,
                    symbol=symbol,
                    reason=f"PHANTOM: deferred to import/takeover - {issue}",
                    priority=0,
                ))
                self.metrics["errors"] += 1

            elif "QTY_MISMATCH" in issue:
                logger.error(f"QTY MISMATCH: {symbol} - {issue}")
                self.metrics["errors"] += 1

            elif "PENDING_ADOPTED" in issue or "QTY_SYNCED" in issue:
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
        reason_codes: List[str],
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
            reason_codes=reason_codes,
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
            "state_distribution": self._state_distribution(),
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
        for pos in self.registry.get_all_active():
            if pos.state == PositionState.EXIT_PENDING:
                self.exit_timeout_manager.start_exit_tracking(pos)

        # 2. Check Timeouts
        escalations = self.exit_timeout_manager.check_timeouts()

        for state in escalations:
            new_level = self.exit_timeout_manager.escalate(state.symbol)

            if new_level in (ExitEscalationLevel.AGGRESSIVE, ExitEscalationLevel.EMERGENCY):
                pos = self.registry.get_position(state.symbol)
                side = pos.side if pos else Side.LONG  # Fallback

                actions.append(ManagementAction(
                    type=ActionType.CLOSE_FULL,
                    symbol=state.symbol,
                    reason=f"Exit Timeout: Escalating to {new_level.value}",
                    side=side,
                    order_type=OrderType.MARKET,
                    priority=200,  # Higher than signal exits
                ))

            elif new_level == ExitEscalationLevel.QUARANTINE:
                logger.critical("QUARANTINING SYMBOL due to Exit Timeout", symbol=state.symbol)

        return actions
