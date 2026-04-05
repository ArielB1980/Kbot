"""Position evaluation logic for PositionManagerV2.

Extracted from position_manager_v2.py to keep module sizes manageable.
Contains the evaluate_position rule engine and trailing stop calculation.
"""
from __future__ import annotations

import os
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional

from src.domain.models import OrderType, Side
from src.execution.position_manager_models import ActionType, ManagementAction
from src.execution.position_state_machine import (
    ExitReason,
    ManagedPosition,
    PositionState,
)
from src.monitoring.alert_dispatcher import send_alert_sync
from src.monitoring.logger import get_logger

if TYPE_CHECKING:
    from src.execution.position_manager_v2 import PositionManagerV2

logger = get_logger(__name__)


class PositionEvaluatorMixin:
    """Mixin providing evaluate_position and trailing-stop helpers for PositionManagerV2."""

    def evaluate_position(
        self: PositionManagerV2,
        symbol: str,
        current_price: Decimal,
        current_atr: Optional[Decimal] = None,
        premise_invalidated: bool = False,
        confirmation_condition_met: bool = False,
        confirmation_price: Optional[Decimal] = None,
    ) -> List[ManagementAction]:
        """
        Evaluate all rules for an active position.

        RULE PRIORITY (highest to lowest):
        1. STOP HIT -> Immediate close (ABSOLUTE)
        2. PREMISE INVALIDATION -> Immediate close
        3. FINAL TARGET HIT -> Behavior depends on config:
           - close_full (legacy): Full close
           - tighten_trail (default runner): Tighten trailing stop
           - close_partial: Close ~50% of runner
        4. TP2 HIT -> Partial close
        5. TP1 HIT -> Partial close + conditional BE
        6. TRAILING STOP UPDATE -> Move stop toward profit
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

        if position.state in (
            PositionState.PENDING,
            PositionState.EXIT_PENDING,
            PositionState.CANCEL_PENDING,
        ):
            reason_codes.append(f"IN_FLIGHT:{position.state.value}")
            self._record_decision(symbol, current_price, position, actions, reason_codes)
            return []

        if position.is_terminal:
            return []

        # ========== INTENT CONFIRMATION (market confirmation, not entry ACK) ==========
        conviction_snapshot: Optional[Dict[str, object]] = None
        conviction: Optional[float] = None
        if self._thesis_management_enabled(symbol):
            conviction_snapshot = self._get_conviction_snapshot(symbol, current_price)
            conviction = float(conviction_snapshot["conviction"]) if conviction_snapshot else None
            if conviction is not None:
                reason_codes.append(f"THESIS_CONVICTION:{conviction:.1f}")
                early_exit_threshold = float(
                    getattr(self._strategy_config, "thesis_early_exit_threshold", 35.0)
                )
                if conviction <= early_exit_threshold:
                    if self._thesis_alerts_enabled():
                        send_alert_sync(
                            "THESIS_EARLY_EXIT_TRIGGERED",
                            (
                                f"[THESIS] {symbol} early exit triggered at conviction {conviction:.1f}%\n"
                                f"Threshold: {early_exit_threshold:.1f}% | Position: {position.position_id}"
                            ),
                            rate_limit_key=f"THESIS_EARLY_EXIT_TRIGGERED:{symbol}",
                            rate_limit_seconds=1800,
                        )
                    client_order_id = f"exit-thesis-{position.position_id}"
                    actions.append(
                        ManagementAction(
                            type=ActionType.CLOSE_FULL,
                            symbol=symbol,
                            reason=f"Thesis conviction below threshold ({conviction:.1f})",
                            side=position.side,
                            size=position.remaining_qty,
                            order_type=OrderType.MARKET,
                            client_order_id=client_order_id,
                            position_id=position.position_id,
                            exit_reason=ExitReason.PREMISE_INVALIDATION,
                            priority=89,
                        )
                    )
                    self._record_decision(symbol, current_price, position, actions, reason_codes)
                    return actions

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
                current_price=str(current_price),
            )

            client_order_id = f"exit-stop-{position.position_id}"

            actions.append(
                ManagementAction(
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
                    priority=100,
                )
            )

            self._record_decision(symbol, current_price, position, actions, reason_codes)
            return actions

        # ========== RULE 3: PREMISE INVALIDATION ==========
        if premise_invalidated:
            reason_codes.append("PREMISE_INVALIDATED")
            client_order_id = f"exit-premise-{position.position_id}"

            actions.append(
                ManagementAction(
                    type=ActionType.CLOSE_FULL,
                    symbol=symbol,
                    reason="Premise Invalidated",
                    side=position.side,
                    size=position.remaining_qty,
                    order_type=OrderType.MARKET,
                    client_order_id=client_order_id,
                    position_id=position.position_id,
                    exit_reason=ExitReason.PREMISE_INVALIDATION,
                    priority=90,
                )
            )

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
                actions.append(
                    ManagementAction(
                        type=ActionType.CLOSE_FULL,
                        symbol=symbol,
                        reason=f"Final Target Hit ({position.initial_final_target})",
                        side=position.side,
                        size=position.remaining_qty,
                        order_type=OrderType.MARKET,
                        client_order_id=client_order_id,
                        position_id=position.position_id,
                        exit_reason=ExitReason.TAKE_PROFIT_FINAL,
                        priority=80,
                    )
                )
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
                        tighter_mult = tighter_mult * self._conviction_trail_factor(conviction)
                        new_trail = self._calculate_trailing_stop(
                            position,
                            current_price,
                            current_atr,
                            atr_mult_override=tighter_mult,
                        )
                        if new_trail and position._validate_stop_move(new_trail):
                            client_order_id = f"stop-tighten-final-{position.position_id}"
                            actions.append(
                                ManagementAction(
                                    type=ActionType.UPDATE_STOP,
                                    symbol=symbol,
                                    reason=f"Tighten trail at final target ({position.initial_final_target})",
                                    side=position.side,
                                    price=new_trail,
                                    client_order_id=client_order_id,
                                    position_id=position.position_id,
                                    priority=75,
                                )
                            )
                            self.metrics["stop_moves"] += 1
                # Do NOT return early -- allow subsequent rules (trailing, etc.) to run

            elif behavior == "close_partial":
                # Close ~50% of remaining runner at final target.
                if not position.final_target_touched:
                    position.final_target_touched = True
                    partial_size = position.remaining_qty * Decimal("0.5")
                    partial_size *= self._conviction_partial_factor(conviction)
                    spec_symbol = position.futures_symbol or symbol
                    min_size = self._get_min_size_for_partial(spec_symbol)
                    if partial_size > 0 and partial_size >= min_size:
                        client_order_id = f"exit-final-partial-{position.position_id}"
                        reason_codes.append("FINAL_TARGET_CLOSE_PARTIAL")
                        actions.append(
                            ManagementAction(
                                type=ActionType.CLOSE_PARTIAL,
                                symbol=symbol,
                                reason=f"Final Target Partial Close ({position.initial_final_target})",
                                side=position.side,
                                size=partial_size,
                                order_type=OrderType.MARKET,
                                client_order_id=client_order_id,
                                position_id=position.position_id,
                                exit_reason=ExitReason.TAKE_PROFIT_FINAL,
                                priority=75,
                            )
                        )
                        logger.info(
                            "Final target touched - closing partial runner",
                            symbol=symbol,
                            partial_size=str(partial_size),
                        )
                # Do NOT return early -- allow subsequent rules to run

        # ========== RULE 10.5: PROGRESSIVE TRAILING (R-based tightening) ==========
        self._evaluate_progressive_trailing(
            position, current_price, current_atr, conviction, actions, reason_codes
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
                partial_size *= self._conviction_partial_factor(conviction)
                spec_symbol = position.futures_symbol or symbol
                min_size = self._get_min_size_for_partial(spec_symbol)
                if partial_size >= min_size:
                    client_order_id = f"exit-tp2-{position.position_id}"
                    actions.append(
                        ManagementAction(
                            type=ActionType.CLOSE_PARTIAL,
                            symbol=symbol,
                            reason=f"TP2 Hit ({position.initial_tp2_price})",
                            side=position.side,
                            size=partial_size,
                            order_type=OrderType.MARKET,
                            client_order_id=client_order_id,
                            position_id=position.position_id,
                            exit_reason=ExitReason.TAKE_PROFIT_2,
                            priority=70,
                        )
                    )
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
                partial_size *= self._conviction_partial_factor(conviction)
                spec_symbol = position.futures_symbol or symbol
                min_size = self._get_min_size_for_partial(spec_symbol)
                if partial_size >= min_size:
                    client_order_id = f"exit-tp1-{position.position_id}"
                    actions.append(
                        ManagementAction(
                            type=ActionType.CLOSE_PARTIAL,
                            symbol=symbol,
                            reason=f"TP1 Hit ({position.initial_tp1_price})",
                            side=position.side,
                            size=partial_size,
                            order_type=OrderType.MARKET,
                            client_order_id=client_order_id,
                            position_id=position.position_id,
                            exit_reason=ExitReason.TAKE_PROFIT_1,
                            priority=60,
                        )
                    )
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
                atr_min = Decimal(
                    str(getattr(self._multi_tp_config, "trailing_activation_atr_min", 0))
                )
            position.activate_trailing_if_guard_passes(current_atr, atr_min)

        # ========== RULE 9: TRAILING STOP ==========
        if (position.break_even_triggered or position.trailing_active) and current_atr:
            if os.environ.get("TRADING_TRAILING_ENABLED", "true").lower() == "true":
                # Use progressive trail ATR mult if set, otherwise default
                trail_override = (
                    position.current_trail_atr_mult
                    if position.current_trail_atr_mult is not None
                    else None
                )
                trail_factor = self._conviction_trail_factor(conviction)
                if trail_override is not None:
                    trail_override = trail_override * trail_factor
                elif trail_factor != Decimal("1"):
                    trail_override = self.trailing_atr_multiple * trail_factor
                new_trail = self._calculate_trailing_stop(
                    position, current_price, current_atr, atr_mult_override=trail_override
                )

                if new_trail and position._validate_stop_move(new_trail):
                    pct_move = abs(new_trail - position.current_stop_price) / position.current_stop_price
                    if pct_move > Decimal("0.001"):  # 0.1% min move
                        reason_codes.append("TRAILING_UPDATE")
                        client_order_id = f"stop-trail-{position.position_id}"

                        actions.append(
                            ManagementAction(
                                type=ActionType.UPDATE_STOP,
                                symbol=symbol,
                                reason="Trailing Stop Update",
                                side=position.side,
                                price=new_trail,
                                client_order_id=client_order_id,
                                position_id=position.position_id,
                                priority=20,
                            )
                        )
                        self.metrics["stop_moves"] += 1

        # Sort by priority
        actions.sort(key=lambda a: a.priority, reverse=True)

        if not reason_codes:
            reason_codes.append("NO_ACTION")

        self._record_decision(symbol, current_price, position, actions, reason_codes)

        return actions

    def _evaluate_progressive_trailing(
        self: PositionManagerV2,
        position: ManagedPosition,
        current_price: Decimal,
        current_atr: Optional[Decimal],
        conviction: Optional[float],
        actions: List[ManagementAction],
        reason_codes: List[str],
    ) -> None:
        """Evaluate progressive trailing (R-based tightening) for runner positions."""
        if not (
            position.runner_mode
            and position.trailing_active
            and current_atr
            and self._multi_tp_config
            and getattr(self._multi_tp_config, "progressive_trail_enabled", False)
        ):
            return

        prog_levels = getattr(self._multi_tp_config, "progressive_trail_levels", [])
        # Compute current R-multiple
        entry_ref = position.avg_entry_price or position.initial_entry_price
        if not (position.initial_stop_price and entry_ref):
            return
        risk_per_unit = abs(entry_ref - position.initial_stop_price)
        if risk_per_unit <= 0:
            return
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
                    position,
                    current_price,
                    current_atr,
                    atr_mult_override=(atr_m * self._conviction_trail_factor(conviction)),
                )
                if new_trail and position._validate_stop_move(new_trail):
                    client_order_id = (
                        f"stop-prog-trail-{float(r_thresh):.0f}r-{position.position_id}"
                    )
                    actions.append(
                        ManagementAction(
                            type=ActionType.UPDATE_STOP,
                            symbol=position.symbol,
                            reason=f"Progressive trail tighten at {float(r_thresh):.1f}R (ATR×{float(atr_m):.1f})",
                            side=position.side,
                            price=new_trail,
                            client_order_id=client_order_id,
                            position_id=position.position_id,
                            priority=76,  # Between final target (75) and TP2 (70)
                        )
                    )
                    self.metrics["stop_moves"] += 1
                    logger.info(
                        "Progressive trail tightened",
                        symbol=position.symbol,
                        r_level=f"{float(r_thresh):.1f}R",
                        atr_mult=f"{float(atr_m):.1f}",
                        new_stop=str(new_trail),
                        current_price=str(current_price),
                    )

    def _calculate_trailing_stop(
        self: PositionManagerV2,
        position: ManagedPosition,
        current_price: Decimal,
        current_atr: Decimal,
        atr_mult_override: Optional[Decimal] = None,
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
