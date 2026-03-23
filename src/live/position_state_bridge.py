from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Optional

from src.data.symbol_utils import normalize_symbol_for_position_match
from src.domain.models import Position
from src.exceptions import DataError, OperationalError
from src.monitoring.logger import get_logger

if TYPE_CHECKING:
    from src.execution.position_state_machine import ManagedPosition
    from src.live.live_trading import LiveTrading

logger = get_logger(__name__)


@dataclass
class TrackedPositionState:
    position: Position
    source: str
    registry_position: ManagedPosition | None = None


def _build_registry_position_snapshot(
    managed: "ManagedPosition",
    *,
    pos_data: dict | None = None,
    current_price: Decimal | None = None,
) -> Position:
    raw_entry_price = managed.avg_entry_price or managed.initial_entry_price
    raw_mark_price = current_price
    if raw_mark_price is None and pos_data is not None:
        raw_mark_price = Decimal(
            str(
                pos_data.get("markPrice")
                or pos_data.get("mark_price")
                or pos_data.get("entryPrice")
                or pos_data.get("entry_price")
                or raw_entry_price
            )
        )
    if raw_mark_price is None:
        raw_mark_price = raw_entry_price

    remaining_qty = managed.remaining_qty
    size_notional = remaining_qty * raw_mark_price if remaining_qty > 0 else Decimal("0")
    tp_order_ids = [oid for oid in (managed.tp1_order_id, managed.tp2_order_id) if oid]
    is_protected = bool(managed.initial_stop_price and managed.stop_order_id)
    protection_reason = None
    if not is_protected:
        protection_reason = "SL_ORDER_MISSING" if managed.initial_stop_price else "NO_SL_ORDER_OR_PRICE"

    return Position(
        symbol=managed.symbol,
        side=managed.side,
        size=remaining_qty,
        size_notional=size_notional,
        entry_price=raw_entry_price,
        current_mark_price=raw_mark_price,
        liquidation_price=raw_entry_price,
        unrealized_pnl=Decimal("0"),
        leverage=Decimal("0"),
        margin_used=Decimal("0"),
        stop_loss_order_id=managed.stop_order_id,
        tp_order_ids=tp_order_ids,
        initial_stop_price=managed.initial_stop_price,
        trade_type=managed.trade_type,
        tp1_price=managed.initial_tp1_price,
        tp2_price=managed.initial_tp2_price,
        final_target_price=managed.initial_final_target,
        partial_close_pct=managed.partial_close_pct,
        original_size=managed.initial_size,
        is_protected=is_protected,
        protection_reason=protection_reason,
        opened_at=managed.created_at or datetime.now(timezone.utc),
        setup_type=managed.setup_type,
        regime=managed.regime,
    )


async def load_tracked_position_state(
    lt: "LiveTrading",
    symbol: str,
    *,
    pos_data: dict | None = None,
    current_price: Decimal | None = None,
) -> Optional[TrackedPositionState]:
    from src.storage.repository import get_active_position

    db_pos: Position | None = None
    try:
        db_pos = await asyncio.to_thread(get_active_position, symbol)
    except (OperationalError, DataError, RuntimeError, ValueError, TypeError) as e:
        logger.warning(
            "Tracked position lookup failed in repository",
            symbol=symbol,
            error=str(e),
            error_type=type(e).__name__,
        )

    registry_position = None
    if lt.use_state_machine_v2 and getattr(lt, "position_registry", None):
        registry_position = lt.position_registry.get_position(symbol)

    if db_pos is not None:
        return TrackedPositionState(
            position=db_pos,
            source="postgres",
            registry_position=registry_position,
        )

    if registry_position is not None:
        return TrackedPositionState(
            position=_build_registry_position_snapshot(
                registry_position,
                pos_data=pos_data,
                current_price=current_price,
            ),
            source="registry",
            registry_position=registry_position,
        )

    return None


async def persist_tracked_position_state(
    lt: "LiveTrading",
    tracked: TrackedPositionState,
) -> None:
    from src.storage.repository import save_position

    if tracked.source == "postgres":
        await asyncio.to_thread(save_position, tracked.position)

    registry = getattr(lt, "position_registry", None)
    if registry is None:
        return

    target_norm = normalize_symbol_for_position_match(tracked.position.symbol)
    registry_positions = []
    for pos in registry.get_all_active():
        if normalize_symbol_for_position_match(pos.symbol) == target_norm:
            registry_positions.append(pos)

    if not registry_positions and tracked.registry_position is not None:
        registry_positions.append(tracked.registry_position)

    persistence = getattr(lt, "position_persistence", None)
    if persistence is None and getattr(lt, "execution_gateway", None) is not None:
        persistence = getattr(lt.execution_gateway, "persistence", None)

    for registry_position in registry_positions:
        registry_position.stop_order_id = tracked.position.stop_loss_order_id
        registry_position.initial_stop_price = tracked.position.initial_stop_price
        registry_position.initial_tp1_price = tracked.position.tp1_price
        registry_position.initial_tp2_price = tracked.position.tp2_price
        registry_position.initial_final_target = tracked.position.final_target_price
        registry_position.trade_type = tracked.position.trade_type
        registry_position.updated_at = datetime.now(timezone.utc)

        if persistence is not None:
            await asyncio.to_thread(persistence.save_position, registry_position)
