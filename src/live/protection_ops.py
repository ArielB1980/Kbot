"""
Position protection operations: TP backfill, SL reconciliation, orphan cleanup.

Extracted from live_trading.py to reduce god-object size.
All methods receive a typed reference to the LiveTrading host for shared state access.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional

from src.data.symbol_utils import exchange_position_side as _exchange_position_side
from src.domain.models import Position, Side
from src.exceptions import OperationalError, DataError
from src.monitoring.logger import get_logger

if TYPE_CHECKING:
    from src.live.live_trading import LiveTrading

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# TP Backfill / Reconciliation
# ---------------------------------------------------------------------------

async def reconcile_protective_orders(
    lt: "LiveTrading", raw_positions: List[Dict], current_prices: Dict[str, Decimal]
) -> None:
    """
    TP Backfill / Reconciliation loop that repairs positions missing TP coverage.
    Runs after position sync to ensure all open positions have proper TP ladder.
    """
    if not lt.config.execution.tp_backfill_enabled:
        return

    from src.storage.repository import get_active_position, save_position, async_record_event

    skipped_not_protected: List[str] = []
    for pos_data in raw_positions:
        symbol = pos_data.get("symbol")
        if not symbol or pos_data.get("size", 0) == 0:
            continue

        try:
            if not isinstance(pos_data, dict):
                logger.error(
                    "Invalid pos_data type in reconcile_protective_orders",
                    symbol=symbol,
                    pos_data_type=type(pos_data).__name__,
                )
                continue

            db_pos = await asyncio.to_thread(get_active_position, symbol)
            if not db_pos:
                continue

            if not isinstance(current_prices, dict):
                logger.error(
                    "Invalid current_prices type",
                    symbol=symbol,
                    current_prices_type=type(current_prices).__name__,
                )
                continue

            current_price = current_prices.get(symbol)
            if not current_price:
                logger.debug("Skipping TP backfill - no current price", symbol=symbol)
                continue

            if isinstance(current_price, dict):
                logger.error(
                    "Invalid current_price type (dict)",
                    symbol=symbol,
                    price_type=type(current_price).__name__,
                )
                continue
            if not isinstance(current_price, Decimal):
                current_price = Decimal(str(current_price))

            # In V2 mode, check registry for protection status (DB may be stale)
            is_protected = db_pos.is_protected
            if lt.use_state_machine_v2 and lt.position_registry:
                v2_pos = lt.position_registry.get_position(symbol)
                if v2_pos and v2_pos.stop_order_id:
                    is_protected = True

            if not is_protected:
                skipped_not_protected.append(symbol)
            if await should_skip_tp_backfill(lt, symbol, pos_data, db_pos, current_price, is_protected):
                continue

            from src.data.symbol_utils import position_symbol_matches_order

            open_orders = await lt.client.get_futures_open_orders()
            symbol_orders = [
                o for o in open_orders if position_symbol_matches_order(symbol, o.get("symbol") or "")
            ]

            needs_backfill = needs_tp_backfill(lt, db_pos, symbol_orders)

            if not needs_backfill:
                logger.debug(
                    "TP backfill not needed",
                    symbol=symbol,
                    has_tp_plan=bool(db_pos.tp1_price or db_pos.tp2_price),
                    has_tp_ids=bool(db_pos.tp_order_ids),
                    open_tp_count=len([o for o in symbol_orders if o.get("reduceOnly", False)]),
                )
                continue

            logger.info(
                "TP backfill needed",
                symbol=symbol,
                has_tp_plan=bool(db_pos.tp1_price or db_pos.tp2_price),
                has_tp_ids=bool(db_pos.tp_order_ids),
                open_tp_count=len([o for o in symbol_orders if o.get("reduceOnly", False)]),
            )

            tp_plan = await compute_tp_plan(lt, symbol, pos_data, db_pos, current_price)

            if not tp_plan:
                await async_record_event(
                    "TP_BACKFILL_SKIPPED",
                    symbol,
                    {
                        "reason": "failed_to_compute_plan",
                        "entry": str(pos_data.get("entry_price", 0)),
                        "sl": str(db_pos.initial_stop_price) if db_pos.initial_stop_price else None,
                    },
                )
                continue

            await place_tp_backfill(lt, symbol, pos_data, db_pos, tp_plan, symbol_orders, current_price)

        except (OperationalError, DataError, ValueError) as e:
            logger.exception("TP backfill failed", symbol=symbol, error=str(e), error_type=type(e).__name__)
            await async_record_event(
                "TP_BACKFILL_SKIPPED", symbol, {"reason": f"error: {str(e)}"}
            )

    if skipped_not_protected:
        symbols_dedupe = sorted(set(skipped_not_protected))
        logger.warning(
            "Positions needing protection (TP backfill skipped)",
            symbols=symbols_dedupe,
            count=len(symbols_dedupe),
            action="Run 'make place-missing-stops' (dry-run) then 'make place-missing-stops-live' to protect.",
        )


# ---------------------------------------------------------------------------
# Stop-loss order ID reconciliation
# ---------------------------------------------------------------------------

async def reconcile_stop_loss_order_ids(lt: "LiveTrading", raw_positions: List[Dict]) -> None:
    """
    Reconcile stop loss order IDs from exchange with database positions.
    Fixes the issue where stop loss orders exist on exchange but aren't
    tracked in the database, causing false 'UNPROTECTED' alerts.
    """
    from src.storage.repository import get_active_position, save_position

    try:
        open_orders = await lt.client.get_futures_open_orders()

        from src.data.symbol_utils import normalize_symbol_for_position_match

        orders_by_symbol: Dict[str, List[Dict]] = {}
        for order in open_orders:
            sym = order.get("symbol")
            key = normalize_symbol_for_position_match(sym) if sym else ""
            if key:
                if key not in orders_by_symbol:
                    orders_by_symbol[key] = []
                orders_by_symbol[key].append(order)

        for pos_data in raw_positions:
            symbol = pos_data.get("symbol")
            if not symbol or pos_data.get("size", 0) == 0:
                continue

            try:
                db_pos = await asyncio.to_thread(get_active_position, symbol)
                if not db_pos:
                    continue

                if (
                    db_pos.is_protected
                    and db_pos.stop_loss_order_id
                    and db_pos.initial_stop_price
                    and not str(db_pos.stop_loss_order_id).startswith("unknown_")
                ):
                    continue

                symbol_orders = orders_by_symbol.get(
                    normalize_symbol_for_position_match(symbol), []
                )
                stop_loss_order = None

                for order in symbol_orders:
                    info = order.get("info")
                    if not isinstance(info, dict):
                        info = {}
                    is_reduce_only = (
                        order.get("reduceOnly")
                        if order.get("reduceOnly") is not None
                        else (
                            order.get("reduce_only")
                            if order.get("reduce_only") is not None
                            else info.get("reduceOnly", info.get("reduce_only", False))
                        )
                    )
                    order_type = str(
                        order.get("type") or info.get("orderType") or info.get("type") or ""
                    ).lower()
                    has_stop_price = (
                        order.get("stopPrice") is not None
                        or order.get("triggerPrice") is not None
                        or info.get("stopPrice") is not None
                        or info.get("triggerPrice") is not None
                    )
                    is_stop_type = any(
                        stop_term in order_type
                        for stop_term in ["stop", "stop-loss", "stop_loss", "stp"]
                    )

                    if is_reduce_only and (has_stop_price or is_stop_type):
                        order_side = order.get("side", "").lower()
                        pos_side = _exchange_position_side(pos_data)
                        expected_order_side = "sell" if pos_side == "long" else "buy"

                        if order_side == expected_order_side:
                            stop_loss_order = order
                            break

                if stop_loss_order:
                    sl_order_id = stop_loss_order.get("id")
                    if sl_order_id:
                        logger.info(
                            "Reconciled stop loss order ID from exchange",
                            symbol=symbol,
                            stop_loss_order_id=sl_order_id,
                            previous_sl_id=db_pos.stop_loss_order_id,
                        )

                        db_pos.stop_loss_order_id = sl_order_id

                        if db_pos.initial_stop_price is None:
                            stop_price_raw = (
                                stop_loss_order.get("stopPrice")
                                or stop_loss_order.get("triggerPrice")
                                or (stop_loss_order.get("info") or {}).get("stopPrice")
                                or (stop_loss_order.get("info") or {}).get("triggerPrice")
                            )
                            if stop_price_raw is not None:
                                try:
                                    stop_price_dec = Decimal(str(stop_price_raw))
                                    if db_pos.side == Side.LONG and stop_price_dec < db_pos.entry_price:
                                        db_pos.initial_stop_price = stop_price_dec
                                    elif db_pos.side == Side.SHORT and stop_price_dec > db_pos.entry_price:
                                        db_pos.initial_stop_price = stop_price_dec
                                    else:
                                        logger.warning(
                                            "Skip reconciling initial_stop_price: direction mismatch",
                                            symbol=symbol,
                                            db_side=db_pos.side.value
                                            if hasattr(db_pos.side, "value")
                                            else str(db_pos.side),
                                            entry_price=str(db_pos.entry_price),
                                            exchange_stop_price=str(stop_price_dec),
                                        )
                                except (ValueError, TypeError, KeyError) as e:
                                    logger.warning(
                                        "Failed to parse stop price from exchange order",
                                        symbol=symbol,
                                        error=str(e),
                                        error_type=type(e).__name__,
                                    )

                        if db_pos.initial_stop_price and sl_order_id:
                            db_pos.is_protected = True
                            db_pos.protection_reason = None
                            logger.info(
                                "Position marked as protected after reconciliation",
                                symbol=symbol,
                                is_protected=True,
                            )

                        await asyncio.to_thread(save_position, db_pos)

            except (OperationalError, DataError, ValueError) as e:
                logger.warning(
                    "Failed to reconcile stop loss order ID",
                    symbol=symbol,
                    error=str(e),
                    error_type=type(e).__name__,
                )
                continue

    except (OperationalError, DataError) as e:
        logger.error("Stop loss order ID reconciliation failed", error=str(e), error_type=type(e).__name__)


# ---------------------------------------------------------------------------
# Place missing stops for unprotected positions
# ---------------------------------------------------------------------------

async def place_missing_stops_for_unprotected(
    lt: "LiveTrading", raw_positions: List[Dict], max_per_tick: int = 3
) -> None:
    """Place missing stop-loss orders for positions that have no SL on exchange."""
    from src.data.symbol_utils import position_symbol_matches_order

    def _order_is_stop(o: Dict, side: str) -> bool:
        t = (o.get("info") or {}).get("orderType") or o.get("type") or o.get("order_type") or ""
        t = str(t).lower()
        if "take_profit" in t or "take-profit" in t:
            return False
        if "stop" not in t and "stop_loss" not in t and t != "stop":
            return False
        if not o.get("reduceOnly", o.get("reduce_only", False)):
            return False
        order_side = (o.get("side") or "").lower()
        expect = "sell" if side == "long" else "buy"
        return order_side == expect

    if lt.config.system.dry_run:
        return
    try:
        open_orders = await lt.client.get_futures_open_orders()
    except (OperationalError, DataError) as e:
        logger.warning("Failed to fetch open orders for missing-stops check", error=str(e), error_type=type(e).__name__)
        return

    naked: list = []
    for pos_data in raw_positions:
        pos_sym = pos_data.get("symbol") or ""
        if not pos_sym or float(pos_data.get("size", 0)) == 0:
            continue
        side = _exchange_position_side(pos_data)
        has_stop = False
        for o in open_orders:
            if not position_symbol_matches_order(pos_sym, o.get("symbol") or ""):
                continue
            if _order_is_stop(o, side):
                has_stop = True
                break
        if not has_stop:
            naked.append(pos_data)

    if not naked:
        return

    stop_pct = Decimal("2.0")
    placed = 0
    for pos_data in naked:
        if placed >= max_per_tick:
            break
        symbol = pos_data.get("symbol") or ""
        size = Decimal(str(pos_data.get("size", 0)))
        if size <= 0:
            continue
        if size < Decimal("0.001"):
            logger.debug("Skip placing missing stop: size below venue min", symbol=symbol, size=str(size))
            continue
        entry = Decimal(str(pos_data.get("entryPrice", pos_data.get("entry_price", 0))))
        if entry <= 0:
            continue
        side = _exchange_position_side(pos_data)
        if side == "long":
            stop_price = entry * (Decimal("1") - stop_pct / Decimal("100"))
        else:
            stop_price = entry * (Decimal("1") + stop_pct / Decimal("100"))
        close_side = "sell" if side == "long" else "buy"
        unified = symbol
        if symbol.startswith("PF_") and "/" not in symbol:
            from src.data.symbol_utils import pf_to_unified

            unified = pf_to_unified(symbol) or symbol
        try:
            # Route through gateway (single choke point — P1.2)
            result = await lt.execution_gateway.place_emergency_order(
                symbol=unified,
                side=close_side,
                order_type="stop",
                size=size,
                stop_price=stop_price,
                reduce_only=True,
                reason="missing_stop",
            )
            if result.success:
                logger.info(
                    "Placed missing stop for unprotected position",
                    symbol=symbol,
                    stop_price=str(stop_price),
                    size=str(size),
                    exchange_order_id=result.exchange_order_id,
                )
                placed += 1
            else:
                logger.warning(
                    "Missing stop order rejected",
                    symbol=symbol,
                    error=result.error,
                )
        except (OperationalError, DataError) as e:
            logger.warning(
                "Failed to place missing stop for unprotected position",
                symbol=symbol,
                error=str(e),
                error_type=type(e).__name__,
            )


# ---------------------------------------------------------------------------
# TP backfill helpers
# ---------------------------------------------------------------------------

async def should_skip_tp_backfill(
    lt: "LiveTrading",
    symbol: str,
    pos_data: Dict,
    db_pos: Position,
    current_price: Decimal,
    is_protected: Optional[bool] = None,
) -> bool:
    """Safety checks: Don't backfill when it's unsafe."""
    last_backfill = lt.tp_backfill_cooldowns.get(symbol)
    if last_backfill:
        elapsed = (datetime.now(timezone.utc) - last_backfill).total_seconds()
        cooldown_seconds = lt.config.execution.tp_backfill_cooldown_minutes * 60
        if elapsed < cooldown_seconds:
            logger.debug(
                "TP backfill skipped: cooldown",
                symbol=symbol,
                elapsed=elapsed,
                cooldown=cooldown_seconds,
            )
            return True

    try:
        size_val = float(pos_data.get("size") or 0)
    except (TypeError, ValueError):
        size_val = 0
    if size_val <= 0:
        logger.debug("TP backfill skipped: zero size", symbol=symbol)
        return True

    protected = is_protected if is_protected is not None else db_pos.is_protected
    if not protected:
        logger.warning(
            "TP backfill skipped: position not protected",
            symbol=symbol,
            reason=db_pos.protection_reason,
            has_sl_price=bool(db_pos.initial_stop_price),
            has_sl_order=bool(db_pos.stop_loss_order_id),
        )
        return True

    if db_pos.opened_at:
        elapsed = (datetime.now(timezone.utc) - db_pos.opened_at).total_seconds()
        if elapsed < lt.config.execution.min_hold_seconds:
            logger.debug(
                "TP backfill skipped: too new",
                symbol=symbol,
                elapsed=elapsed,
                min_hold=lt.config.execution.min_hold_seconds,
            )
            return True

    return False


def needs_tp_backfill(lt: "LiveTrading", db_pos: Position, symbol_orders: List[Dict]) -> bool:
    """Determine if TP coverage is missing."""
    has_tp_plan = (db_pos.tp1_price is not None) or (db_pos.tp2_price is not None)
    has_tp_ids = bool(db_pos.tp_order_ids and len(db_pos.tp_order_ids) > 0)

    open_tp_orders = [
        o
        for o in symbol_orders
        if o.get("reduceOnly", False)
        and o.get("type", "").lower() in ("take_profit", "take-profit", "limit")
        and (
            (db_pos.side == Side.LONG and o.get("side", "").lower() == "sell")
            or (db_pos.side == Side.SHORT and o.get("side", "").lower() == "buy")
        )
    ]

    explicit_tp_orders = [
        o for o in open_tp_orders if o.get("type", "").lower() in ("take_profit", "take-profit")
    ]

    if explicit_tp_orders:
        open_tp_orders = explicit_tp_orders

    if not has_tp_plan and not has_tp_ids:
        return True

    if len(open_tp_orders) == 0:
        return True

    min_expected = lt.config.execution.min_tp_orders_expected
    if len(open_tp_orders) < min_expected:
        return True

    return False


async def compute_tp_plan(
    lt: "LiveTrading",
    symbol: str,
    pos_data: Dict,
    db_pos: Position,
    current_price: Decimal,
) -> Optional[List[Decimal]]:
    """Get or compute a TP plan."""
    tp_plan: list = []
    if db_pos.tp1_price:
        tp_plan.append(db_pos.tp1_price)
    if db_pos.tp2_price:
        tp_plan.append(db_pos.tp2_price)
    if db_pos.final_target_price:
        tp_plan.append(db_pos.final_target_price)

    if len(tp_plan) >= 2:
        return tp_plan

    if not isinstance(pos_data, dict):
        logger.error(
            "Invalid pos_data type in compute_tp_plan",
            symbol=symbol,
            pos_data_type=type(pos_data).__name__,
        )
        return None

    entry = Decimal(str(pos_data.get("entry_price", pos_data.get("entryPrice", 0))))
    sl = db_pos.initial_stop_price

    if not entry or not sl or entry == 0:
        return None

    risk = abs(entry - sl)
    if risk == 0:
        return None

    side_sign = Decimal("1") if db_pos.side == Side.LONG else Decimal("-1")

    # Determine TP count and R-multiples from config (runner-aware)
    multi_tp = getattr(lt.config, "multi_tp", None)
    runner_mode = (
        multi_tp is not None
        and getattr(multi_tp, "enabled", False)
        and not getattr(multi_tp, "runner_has_fixed_tp", False)
    )

    # Check if the position itself was opened in runner mode (stored state > config)
    registry = getattr(lt, "position_registry", None)
    if registry:
        v2_pos = registry.get_position(symbol)
        if v2_pos is not None and hasattr(v2_pos, "runner_mode"):
            runner_mode = bool(v2_pos.runner_mode)

    if runner_mode and multi_tp:
        tp1_r = Decimal(str(getattr(multi_tp, "tp1_r_multiple", 1.0)))
        tp2_r = Decimal(str(getattr(multi_tp, "tp2_r_multiple", 2.0)))
        tp1 = entry + side_sign * tp1_r * risk
        tp2 = entry + side_sign * tp2_r * risk
        tp_plan = [tp1, tp2]
    else:
        tp1_r = Decimal("1.0")
        tp2_r = Decimal("2.0")
        tp3_r = Decimal("3.0")
        if multi_tp:
            tp1_r = Decimal(str(getattr(multi_tp, "tp1_r_multiple", 1.0)))
            tp2_r = Decimal(str(getattr(multi_tp, "tp2_r_multiple", 2.0)))
            tp3_r = Decimal(str(getattr(multi_tp, "runner_tp_r_multiple", 3.0) or 3.0))
        tp1 = entry + side_sign * tp1_r * risk
        tp2 = entry + side_sign * tp2_r * risk
        tp3 = entry + side_sign * tp3_r * risk
        tp_plan = [tp1, tp2, tp3]

    from src.storage.repository import async_record_event

    await async_record_event(
        "TP_BACKFILL_PLANNED",
        symbol,
        {
            "side": db_pos.side.value,
            "entry": str(entry),
            "sl": str(sl),
            "risk": str(risk),
            "tp_plan": [str(tp) for tp in tp_plan],
            "runner_mode": runner_mode,
            "reason": "computed_from_config_r_multiples",
        },
    )

    min_distance = current_price * Decimal(str(lt.config.execution.min_tp_distance_pct))
    valid_tps: list = []

    for i, tp in enumerate(tp_plan):
        tp_label = f"TP{i + 1}"
        if db_pos.side == Side.LONG:
            if tp <= current_price + min_distance:
                logger.warning(
                    f"{tp_label} too close or already passed (LONG) - skipping",
                    symbol=symbol,
                    tp=str(tp),
                    current=str(current_price),
                    min_distance=str(min_distance),
                )
                continue
        else:
            if tp >= current_price - min_distance:
                logger.warning(
                    f"{tp_label} too close or already passed (SHORT) - skipping",
                    symbol=symbol,
                    tp=str(tp),
                    current=str(current_price),
                    min_distance=str(min_distance),
                )
                continue
        valid_tps.append(tp)

    if not valid_tps:
        logger.warning(
            "All TP levels too close or already passed - no TPs to place",
            symbol=symbol,
            side=db_pos.side.value,
            current_price=str(current_price),
            original_tps=[str(tp) for tp in tp_plan],
        )
        return None

    if lt.config.execution.max_tp_distance_pct:
        max_distance = current_price * Decimal(str(lt.config.execution.max_tp_distance_pct))
        if db_pos.side == Side.LONG:
            valid_tps = [min(tp, current_price + max_distance) for tp in valid_tps]
        else:
            valid_tps = [max(tp, current_price - max_distance) for tp in valid_tps]

    if len(valid_tps) < len(tp_plan):
        logger.info(
            "TP plan filtered - some levels already passed",
            symbol=symbol,
            original_count=len(tp_plan),
            valid_count=len(valid_tps),
            valid_tps=[str(tp) for tp in valid_tps],
        )

    return valid_tps


# ---------------------------------------------------------------------------
# Orphan reduce-only order cleanup
# ---------------------------------------------------------------------------

async def cleanup_orphan_reduce_only_orders(
    lt: "LiveTrading", raw_positions: List[Dict]
) -> None:
    """
    Cleanup orphan reduce-only orders (SL/TP) for positions that no longer exist.
    """
    open_syms: set = set()
    for p in raw_positions:
        pos_sym = p.get("symbol")
        if pos_sym and p.get("size", 0) != 0:
            open_syms.add(pos_sym)
            if pos_sym.startswith("PF_"):
                base = pos_sym[3:-3]
                if base == "XBT":
                    base = "BTC"
                normalized = f"{base}/USD:USD"
                open_syms.add(normalized)

    try:
        orders = await lt.client.get_futures_open_orders()
    except (OperationalError, DataError) as e:
        logger.error("Failed to fetch open orders for orphan cleanup", error=str(e), error_type=type(e).__name__)
        return

    cancelled = 0
    max_cancellations = 20

    for o in orders:
        if cancelled >= max_cancellations:
            break

        try:
            if not o.get("reduceOnly", False):
                continue

            sym = o.get("symbol")
            oid = o.get("id")

            if not sym or not oid:
                continue

            normalized_order_sym = sym
            if "/" in sym and ":" in sym:
                base = sym.split("/")[0]
                if base == "BTC":
                    base = "XBT"
                normalized_order_sym = f"PF_{base}USD"

            if sym in open_syms or normalized_order_sym in open_syms:
                continue

            if oid and not oid.startswith("unknown_"):
                try:
                    await lt.futures_adapter.cancel_order(oid, sym)
                    cancelled += 1
                    logger.info(
                        "Cancelled orphan reduce-only order",
                        symbol=sym,
                        order_id=oid,
                        order_type=o.get("type", "unknown"),
                    )
                except (OperationalError, DataError) as e:
                    error_str = str(e)
                    if "invalidArgument" in error_str or "order_id" in error_str.lower():
                        logger.debug(
                            "Skipped orphan order cancellation - invalid order ID",
                            symbol=sym,
                            order_id=oid,
                            error=error_str,
                        )
                    else:
                        logger.warning(
                            "Failed to cancel orphan reduce-only order",
                            symbol=sym,
                            order_id=oid,
                            error=str(e),
                            error_type=type(e).__name__,
                        )
            else:
                logger.debug(
                    "Skipped orphan order cancellation - placeholder order ID",
                    symbol=sym,
                    order_id=oid,
                )

        except (OperationalError, DataError) as e:
            logger.warning(
                "Error processing orphan order",
                symbol=o.get("symbol"),
                order_id=o.get("id"),
                error=str(e),
                error_type=type(e).__name__,
            )

    if cancelled > 0:
        logger.info(
            "Orphan order cleanup complete", cancelled=cancelled, total_orders=len(orders)
        )


# ---------------------------------------------------------------------------
# Place TP backfill orders
# ---------------------------------------------------------------------------

async def place_tp_backfill(
    lt: "LiveTrading",
    symbol: str,
    pos_data: Dict,
    db_pos: Position,
    tp_plan: List[Decimal],
    symbol_orders: List[Dict],
    current_price: Decimal,
) -> None:
    """Place / repair TP orders on exchange."""
    from src.storage.repository import save_position, async_record_event

    existing_tp_ids = db_pos.tp_order_ids or []

    existing_tp_orders = [
        o
        for o in symbol_orders
        if o.get("id") in existing_tp_ids
        or (
            o.get("reduceOnly", False)
            and o.get("type", "").lower() in ("take_profit", "take-profit", "limit")
        )
    ]

    needs_replace = False
    if existing_tp_orders:
        tolerance = Decimal(str(lt.config.execution.tp_price_tolerance))
        for existing_order in existing_tp_orders:
            existing_price = Decimal(str(existing_order.get("price", 0)))
            if existing_price == 0:
                continue

            closest_planned = min(tp_plan, key=lambda tp: abs(tp - existing_price))
            price_diff_pct = abs(existing_price - closest_planned) / closest_planned

            if price_diff_pct > tolerance:
                needs_replace = True
                break
    else:
        needs_replace = True

    if not needs_replace:
        await async_record_event(
            "TP_BACKFILL_SKIPPED",
            symbol,
            {"reason": "tp_orders_match_plan", "tp_count": len(existing_tp_orders)},
        )
        return

    # Cancel ALL matching TP orders on exchange (not just DB-tracked IDs) to prevent
    # orphaned orders when DB state diverges from exchange state.
    all_tp_ids_to_cancel = set(existing_tp_ids)
    for o in existing_tp_orders:
        oid = o.get("id")
        if oid:
            all_tp_ids_to_cancel.add(oid)

    for tp_id in all_tp_ids_to_cancel:
        try:
            await lt.futures_adapter.cancel_order(tp_id, symbol)
            logger.debug("Cancelled existing TP for backfill", order_id=tp_id, symbol=symbol)
        except (OperationalError, DataError) as e:
            logger.warning(
                "Failed to cancel existing TP", order_id=tp_id, symbol=symbol, error=str(e), error_type=type(e).__name__
            )

    try:
        position_size_notional = await lt.futures_adapter.position_size_notional(
            symbol=symbol, pos_data=pos_data, current_price=current_price
        )
        position_size_contracts = Decimal(str(pos_data.get("size", 0) or 0))
        multi_tp = getattr(lt.config, "multi_tp", None)
        instrument_spec_registry = getattr(lt, "instrument_spec_registry", None)

        # Single TP placement path for live/backfill: executor.update_protective_orders. With multi_tp + position_size_contracts + instrument_spec_registry we get contract sizing, step quantize, and venue min filter (no dust).
        new_sl_id, new_tp_ids = await lt.executor.update_protective_orders(
            symbol=symbol,
            side=db_pos.side,
            current_sl_id=db_pos.stop_loss_order_id,
            new_sl_price=db_pos.initial_stop_price,
            current_tp_ids=existing_tp_ids,
            new_tp_prices=tp_plan,
            position_size_notional=position_size_notional,
            position_size_contracts=position_size_contracts if position_size_contracts > 0 else None,
            current_price=current_price,
            multi_tp_config=multi_tp if multi_tp and getattr(multi_tp, "enabled", False) else None,
            instrument_spec_registry=instrument_spec_registry,
        )

        # Persist new SL order ID so protection monitor sees the live stop (avoids false NAKED → kill switch).
        if new_sl_id:
            db_pos.stop_loss_order_id = new_sl_id
            if lt.position_registry:
                reg_pos = lt.position_registry.get_position(symbol)
                if reg_pos:
                    reg_pos.stop_order_id = new_sl_id

        db_pos.tp_order_ids = new_tp_ids
        db_pos.tp1_price = tp_plan[0] if len(tp_plan) > 0 else None
        db_pos.tp2_price = tp_plan[1] if len(tp_plan) > 1 else None
        # Only set final_target_price if a 3rd TP exists (legacy mode);
        # in runner mode there are exactly 2 TPs and no final target.
        db_pos.final_target_price = tp_plan[2] if len(tp_plan) > 2 else None
        logger.info(
            "TP backfill plan applied",
            symbol=symbol,
            tp_count=len(tp_plan),
            runner_mode=len(tp_plan) == 2,
        )

        await asyncio.to_thread(save_position, db_pos)

        lt.tp_backfill_cooldowns[symbol] = datetime.now(timezone.utc)

        await async_record_event(
            "TP_BACKFILL_PLACED" if not existing_tp_orders else "TP_BACKFILL_REPLACED",
            symbol,
            {
                "side": db_pos.side.value,
                "size": str(pos_data.get("size", 0)),
                "entry": str(pos_data.get("entry_price", 0)),
                "sl": str(db_pos.initial_stop_price),
                "tp_plan": [str(tp) for tp in tp_plan],
                "tp_order_ids": new_tp_ids,
                "existing_tp_prices": [
                    str(Decimal(str(o.get("price", 0)))) for o in existing_tp_orders
                ]
                if existing_tp_orders
                else [],
                "reason": "backfill_repair" if existing_tp_orders else "backfill_new",
            },
        )

        logger.info(
            "TP backfill completed",
            symbol=symbol,
            action="replaced" if existing_tp_orders else "placed",
            tp_count=len(new_tp_ids),
        )

    except (OperationalError, DataError, ValueError) as e:
        logger.exception("Failed to place TP backfill", symbol=symbol, error=str(e), error_type=type(e).__name__)
        await async_record_event(
            "TP_BACKFILL_SKIPPED", symbol, {"reason": f"placement_failed: {str(e)}"}
        )
