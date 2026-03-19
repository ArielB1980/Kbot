"""
Signal processing: risk validation, state machine entry, order placement.

Extracted from live_trading.py to reduce god-object size.
All functions receive a typed reference to the LiveTrading host.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Optional, TYPE_CHECKING

from src.exceptions import OperationalError, DataError
from src.domain.models import Signal, SignalType, Side
from src.execution.equity import calculate_effective_equity
from src.execution.position_manager_v2 import ActionType as ActionTypeV2
from src.monitoring.logger import get_logger

if TYPE_CHECKING:
    from src.live.live_trading import LiveTrading

logger = get_logger(__name__)


async def handle_signal(
    lt: "LiveTrading",
    signal: Signal,
    spot_price: Decimal,
    mark_price: Decimal,
    notional_override: Optional[Decimal] = None,
) -> dict:
    """
    Process signal through Position State Machine V2.

    Args:
        lt: LiveTrading host reference
        signal: Trading signal
        spot_price: Current spot price
        mark_price: Current futures mark price
        notional_override: When set (auction execution path), used as base
            notional in risk sizing and enables utilisation boost.

    Returns:
        dict with keys:
            - order_placed: bool
            - reason: str (human-readable reason for success/failure)
            - rejection_reasons: list[str] (if rejected)
    """
    lt.signals_since_emit += 1
    logger.info("New signal detected", type=signal.signal_type.value, symbol=signal.symbol)

    # Health gate: no new entries when candle health is insufficient
    if getattr(lt, "trade_paused", False):
        return {
            "order_placed": False,
            "reason": "TRADING PAUSED: candle health insufficient",
            "rejection_reasons": ["trade_paused"],
        }

    # Hardening gate: in DEGRADED/HALTED/EMERGENCY, block new entries.
    # Management actions remain handled elsewhere in the loop.
    if getattr(lt, "hardening", None) and not lt.hardening.is_trading_allowed():
        return {
            "order_placed": False,
            "reason": "TRADING_GATE_CLOSED: new entries suppressed by hardening state",
            "rejection_reasons": ["hardening_gate_closed"],
        }

    return await handle_signal_v2(lt, signal, spot_price, mark_price, notional_override=notional_override)


async def handle_signal_v2(
    lt: "LiveTrading",
    signal: Signal,
    spot_price: Decimal,
    mark_price: Decimal,
    notional_override: Optional[Decimal] = None,
) -> dict:
    """
    Process signal through Position State Machine V2.

    CRITICAL: All orders flow through ExecutionGateway.
    No direct exchange calls allowed.

    Args:
        notional_override: When set (auction execution path), used as base
            notional and enables utilisation boost. Auction already validated
            margin budget so skip_margin_check is set True.

    Returns:
        {"order_placed": bool, "reason": str | None}
    """
    import uuid

    decision_id = None
    if isinstance(getattr(signal, "meta_info", None), dict):
        decision_id = signal.meta_info.get("decision_id")
    cycle_id = getattr(lt, "_current_cycle_id", None)

    async def _emit_counterfactual_action(order_placed: bool, reason: Optional[str]) -> None:
        if not decision_id:
            return
        try:
            from src.storage.repository import async_record_event

            await async_record_event(
                event_type="COUNTERFACTUAL_ACTION",
                symbol=signal.symbol,
                decision_id=decision_id,
                details={
                    "order_placed": bool(order_placed),
                    "reason": reason,
                    "signal_type": signal.signal_type.value,
                    "cycle_id": cycle_id,
                },
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("Failed to record counterfactual action", symbol=signal.symbol, error=str(exc))

    async def _fail(reason: str) -> dict:
        await _emit_counterfactual_action(False, reason)
        return {"order_placed": False, "reason": reason}

    async def _ok() -> dict:
        await _emit_counterfactual_action(True, None)
        return {"order_placed": True, "reason": None}

    logger.info(
        "Processing signal via State Machine V2",
        symbol=signal.symbol,
        type=signal.signal_type.value,
    )

    # 1. Fetch Account Equity and Available Margin
    balance = await lt.client.get_futures_balance()
    base = getattr(lt.config.exchange, "base_currency", "USD")
    equity, available_margin, _ = await calculate_effective_equity(
        balance, base_currency=base, kraken_client=lt.client
    )
    if (
        getattr(lt, "_replay_relaxed_signal_gates", False)
        and equity <= 0
    ):
        replay_equity = Decimal(
            str(getattr(getattr(lt.config, "backtest", None), "starting_equity", Decimal("10000")))
        )
        equity = replay_equity
        if available_margin <= 0:
            available_margin = replay_equity
    if equity <= 0:
        logger.error("Insufficient equity for trading", equity=str(equity))
        return await _fail("Insufficient equity for trading")

    # 2. Risk Validation (Safety Gate)
    symbol_tier = lt.market_discovery.get_symbol_tier(signal.symbol) if lt.market_discovery else "C"
    if symbol_tier != "A":
        static_tier = lt._get_static_tier(signal.symbol)
        if static_tier == "A":
            logger.warning(
                "Tier downgrade detected",
                symbol=signal.symbol,
                static_tier=static_tier,
                dynamic_tier=symbol_tier,
                reason="Dynamic classification is authoritative",
            )

    decision = lt.risk_manager.validate_trade(
        signal,
        equity,
        spot_price,
        mark_price,
        available_margin=available_margin,
        notional_override=notional_override,
        skip_margin_check=(notional_override is not None),
        symbol_tier=symbol_tier,
    )

    if not decision.approved:
        reasons = getattr(decision, "rejection_reasons", []) or []
        detail = reasons[0] if reasons else "Trade rejected by Risk Manager"
        logger.warning("Trade rejected by Risk Manager", symbol=signal.symbol, reasons=reasons)
        return await _fail(f"Risk Manager rejected: {detail}")
    logger.info("Risk approved", symbol=signal.symbol, notional=str(decision.position_notional))

    # 3. Map to futures symbol
    futures_symbol = lt.futures_adapter.map_spot_to_futures(
        signal.symbol, futures_tickers=lt.latest_futures_tickers
    )

    # 3b. Enforce minimum position notional (venue min_size * price)
    if hasattr(lt, "instrument_spec_registry") and lt.instrument_spec_registry and mark_price > 0:
        spec = lt.instrument_spec_registry.get_spec(futures_symbol)
        skip_min_notional_check = (
            getattr(lt, "_replay_relaxed_signal_gates", False) and spec is None
        )
        if not skip_min_notional_check:
            min_size = lt.instrument_spec_registry.get_effective_min_size(futures_symbol)
            min_notional = min_size * mark_price
            if decision.position_notional < min_notional:
                logger.warning(
                    "Position notional below venue minimum - rejecting",
                    symbol=signal.symbol,
                    notional=str(decision.position_notional),
                    min_notional=str(min_notional),
                    min_size=str(min_size),
                )
                return await _fail(
                    f"Position notional {decision.position_notional} below venue min {min_notional}"
                )

    # 4. Generate entry plan to get TP levels
    step_size = None
    if hasattr(lt, "instrument_spec_registry") and lt.instrument_spec_registry:
        spec = lt.instrument_spec_registry.get_spec(futures_symbol)
        if spec and spec.size_step > 0:
            step_size = spec.size_step
    order_intent = lt.execution_engine.generate_entry_plan(
        signal, decision.position_notional, spot_price, mark_price, decision.leverage,
        step_size=step_size,
    )

    tps = order_intent.get("take_profits", [])
    tp1_price = tps[0]["price"] if len(tps) > 0 else None
    tp2_price = tps[1]["price"] if len(tps) > 1 else None
    # In runner mode (2 TPs), final_target comes from metadata (3.0R aspiration level).
    # In legacy mode (3+ TPs), final_target is the last TP price.
    metadata = order_intent.get("metadata", {})
    final_target = metadata.get("final_target_price")
    if final_target is None:
        final_target = tps[-1]["price"] if len(tps) > 2 else None

    # 5. Calculate position size in contracts
    position_size = Decimal(str(order_intent.get("size", 0)))
    if position_size <= 0:
        position_size = decision.position_notional / mark_price

    # 6. Evaluate entry via Position Manager V2
    action, position = lt.position_manager_v2.evaluate_entry(
        signal=signal,
        entry_price=mark_price,
        stop_price=order_intent["metadata"]["fut_sl"],
        tp1_price=tp1_price,
        tp2_price=tp2_price,
        final_target=final_target,
        position_size=position_size,
        trade_type=signal.regime if hasattr(signal, "regime") else "tight_smc",
        leverage=decision.leverage,
    )

    if action.type == ActionTypeV2.REJECT_ENTRY:
        logger.warning("Entry REJECTED by State Machine", symbol=signal.symbol, reason=action.reason)
        return await _fail(f"State Machine rejected: {action.reason or 'REJECT_ENTRY'}")
    logger.info(
        "State machine accepted entry",
        symbol=signal.symbol,
        client_order_id=action.client_order_id,
    )

    # 7. Handle opportunity cost replacement via V2
    if decision.should_close_existing and decision.close_symbol:
        logger.warning(
            "Opportunity cost replacement via V2",
            closing=decision.close_symbol,
            opening=signal.symbol,
        )

        close_actions = lt.position_manager_v2.request_reversal(
            decision.close_symbol,
            Side.LONG if signal.signal_type == SignalType.LONG else Side.SHORT,
            mark_price,
        )

        for close_action in close_actions:
            result = await lt.execution_gateway.execute_action(close_action)
            if not result.success:
                logger.error("Failed to close for replacement", error=result.error)
                return await _fail(f"Failed to close for replacement: {result.error}")

        lt.position_registry.confirm_reversal_closed(decision.close_symbol)

    # 8. Register position in state machine
    position.entry_order_id = action.client_order_id
    position.entry_client_order_id = action.client_order_id
    position.futures_symbol = futures_symbol

    try:
        lt.position_registry.register_position(position)
    except (OperationalError, DataError) as e:
        logger.error("Failed to register position", error=str(e), error_type=type(e).__name__)
        return await _fail(f"Failed to register position: {e}")

    # 9. Execute entry via Execution Gateway
    logger.info(
        "Submitting entry to gateway",
        symbol=futures_symbol,
        client_order_id=action.client_order_id,
    )
    result = await lt.execution_gateway.execute_action(action, order_symbol=futures_symbol)

    if not result.success:
        logger.error("Entry failed", error=result.error)
        position.mark_error(f"Entry failed: {result.error}")
        return await _fail(f"Entry failed: {result.error}")

    logger.info(
        "Entry order placed via V2",
        symbol=futures_symbol,
        client_order_id=action.client_order_id,
        exchange_order_id=result.exchange_order_id,
    )

    # 10. Persist position state
    if lt.position_persistence:
        lt.position_persistence.save_position(position)
        lt.position_persistence.log_action(
            position.position_id,
            "entry_submitted",
            {
                "signal_type": signal.signal_type.value,
                "entry_price": str(mark_price),
                "stop_price": str(position.initial_stop_price),
                "size": str(position_size),
            },
        )

    # Send thesis-aware open alert (urgent=True so opens are never rate-limited).
    try:
        from src.monitoring.alerting import send_alert_sync, fmt_price, fmt_size

        tp_line = ""
        if tp1_price:
            tp_line = f"TP1: ${fmt_price(tp1_price)}"
            if tp2_price:
                tp_line += f" | TP2: ${fmt_price(tp2_price)}"
            tp_line += "\n"

        thesis_line = "Thesis: unavailable"
        memory = getattr(lt, "institutional_memory_manager", None)
        if memory and memory.is_enabled_for_symbol(signal.symbol):
            thesis = memory.get_latest_thesis(signal.symbol)
            if thesis:
                thesis_line = (
                    f"Thesis: {thesis.daily_bias} bias | "
                    f"Zone ${fmt_price(thesis.weekly_zone_low)}-${fmt_price(thesis.weekly_zone_high)} | "
                    f"Conviction {float(thesis.current_conviction):.1f}% ({thesis.status})"
                )

        send_alert_sync(
            "THESIS_TRADE_OPENED",
            f"[THESIS] New {signal.signal_type.value.upper()} trade plan\n"
            f"Symbol: {signal.symbol}\n"
            f"{thesis_line}\n"
            f"Entry: ${fmt_price(mark_price)}\n"
            f"Stop: ${fmt_price(position.initial_stop_price)}\n"
            f"{tp_line}"
            f"Size: {fmt_size(position_size)} | Notional: ${float(decision.position_notional):.2f} ({decision.leverage}x)\n"
            f"Plan: {'Boosted sizing' if decision.utilisation_boost_applied else 'Base sizing'}",
            urgent=True,
        )
    except (OperationalError, ImportError, OSError):
        pass  # Alert failure must never block trading

    return await _ok()
