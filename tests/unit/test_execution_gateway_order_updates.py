from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.domain.models import OrderType, Side
from src.execution.execution_gateway import (
    ExecutionGateway,
    OrderPurpose,
    PendingOrder,
)
from src.execution.position_manager_v2 import ActionType, ManagementAction
from src.execution.position_state_machine import PositionState


def _build_gateway() -> ExecutionGateway:
    client = AsyncMock()
    registry = MagicMock()
    position_manager = MagicMock()
    persistence = MagicMock()
    return ExecutionGateway(
        exchange_client=client,
        registry=registry,
        position_manager=position_manager,
        persistence=persistence,
        use_safety=False,
    )


@pytest.mark.asyncio
async def test_process_order_update_uses_incremental_fill_qty_not_cumulative():
    gateway = _build_gateway()

    pending = PendingOrder(
        client_order_id="entry-client-1",
        position_id="pos-1",
        symbol="BTC/USD",
        purpose=OrderPurpose.ENTRY,
        side=Side.LONG,
        size=Decimal("1"),
        price=Decimal("50000"),
        order_type=OrderType.LIMIT,
        submitted_at=datetime.now(timezone.utc),
        exchange_order_id="exchange-entry-1",
        status="submitted",
    )
    gateway._pending_orders[pending.client_order_id] = pending
    gateway._order_id_map["exchange-entry-1"] = pending.client_order_id
    gateway.position_manager.handle_order_event.return_value = []
    gateway.registry.get_position.return_value = None

    # First poll: partial fill reports cumulative 0.4
    await gateway.process_order_update(
        {
            "id": "exchange-entry-1",
            "clientOrderId": "entry-client-1",
            "status": "open",
            "filled": "0.4",
            "remaining": "0.6",
            "average": "50010",
            "trades": [],
        }
    )
    first_event = gateway.position_manager.handle_order_event.call_args_list[0][0][1]
    assert first_event.fill_qty == Decimal("0.4")

    # Second poll: closed order reports cumulative 1.0.
    # Gateway must emit only the delta (0.6), not 1.0.
    await gateway.process_order_update(
        {
            "id": "exchange-entry-1",
            "clientOrderId": "entry-client-1",
            "status": "closed",
            "filled": "1.0",
            "remaining": "0.0",
            "average": "50020",
            "trades": [],
        }
    )
    second_event = gateway.position_manager.handle_order_event.call_args_list[1][0][1]
    assert second_event.fill_qty == Decimal("0.6")

    # Third poll of the same closed snapshot must be ignored (no new fill delta).
    await gateway.process_order_update(
        {
            "id": "exchange-entry-1",
            "clientOrderId": "entry-client-1",
            "status": "closed",
            "filled": "1.0",
            "remaining": "0.0",
            "average": "50020",
            "trades": [],
        }
    )
    assert gateway.position_manager.handle_order_event.call_count == 2


@pytest.mark.asyncio
async def test_execute_entry_passes_action_leverage_to_client():
    gateway = _build_gateway()
    gateway.client.create_order.return_value = {"id": "exchange-entry-2"}
    gateway.registry.get_position.return_value = None
    # _check_entry_liquidity calls await client.fetch_ticker(symbol); return real bid/ask so no coroutine comparison
    gateway.client.fetch_ticker = AsyncMock(
        return_value={"bid": 50000, "ask": 50010}
    )

    action = ManagementAction(
        type=ActionType.OPEN_POSITION,
        symbol="BTC/USD",
        reason="test-entry",
        side=Side.LONG,
        size=Decimal("1"),
        price=Decimal("50000"),
        leverage=Decimal("3"),
        order_type=OrderType.LIMIT,
        client_order_id="entry-client-2",
        position_id="pos-2",
        priority=10,
    )

    result = await gateway.execute_action(action, order_symbol="BTC/USD:USD")

    assert result.success is True
    kwargs = gateway.client.create_order.call_args.kwargs
    assert kwargs["leverage"] == Decimal("3")


@pytest.mark.asyncio
async def test_sync_with_exchange_runs_single_reconcile_and_reuses_issues():
    gateway = _build_gateway()
    gateway.client.get_all_futures_positions.return_value = [
        {"symbol": "PF_ENAUSD", "side": "short", "contracts": 111, "entryPrice": "0.1546"}
    ]
    gateway.client.get_futures_open_orders.return_value = []
    gateway.registry.reconcile_with_exchange.return_value = [
        ("ENA/USD", "STALE_ZERO_QTY: Registry 0 vs Exchange 111")
    ]
    gateway.position_manager.reconcile.return_value = []
    gateway.registry.get_all_active.return_value = []

    result = await gateway.sync_with_exchange()

    assert gateway.registry.reconcile_with_exchange.call_count == 1
    gateway.position_manager.reconcile.assert_called_once()
    assert gateway.position_manager.reconcile.call_args.kwargs["issues"] == [
        ("ENA/USD", "STALE_ZERO_QTY: Registry 0 vs Exchange 111")
    ]
    assert result["issues"] == [("ENA/USD", "STALE_ZERO_QTY: Registry 0 vs Exchange 111")]


@pytest.mark.asyncio
async def test_sync_with_exchange_persists_qty_synced_positions():
    gateway = _build_gateway()
    gateway.client.get_all_futures_positions.return_value = [
        {"symbol": "PF_ENAUSD", "side": "short", "contracts": 72, "entryPrice": "0.1546"}
    ]
    gateway.client.get_futures_open_orders.return_value = []
    gateway.registry.reconcile_with_exchange.return_value = [
        ("ENA/USD", "QTY_SYNCED: exit+39 local=111 exchange=72 price=0.1546")
    ]
    synced_pos = MagicMock()
    gateway.registry.get_position.return_value = synced_pos
    gateway.position_manager.reconcile.return_value = []
    gateway.registry.get_all_active.return_value = [synced_pos]

    result = await gateway.sync_with_exchange()

    gateway.registry.reconcile_with_exchange.assert_called_once()
    gateway.persistence.save_position.assert_called_with(synced_pos)
    assert result["issues"] == [
        ("ENA/USD", "QTY_SYNCED: exit+39 local=111 exchange=72 price=0.1546")
    ]


@pytest.mark.asyncio
async def test_sync_with_exchange_persists_closed_positions_after_exit_hysteresis():
    gateway = _build_gateway()
    gateway.client.get_all_futures_positions.return_value = []
    gateway.client.get_futures_open_orders.return_value = []
    gateway.registry.reconcile_with_exchange.return_value = [
        ("LTC/USD", "CLOSED_ON_EXCHANGE_MISSING_AFTER_EXIT: miss_count=2")
    ]
    closed_pos = MagicMock()
    closed_pos.symbol = "LTC/USD"
    closed_pos.state = PositionState.CLOSED
    gateway.registry._closed_positions = [closed_pos]
    gateway.position_manager.reconcile.return_value = []
    gateway.registry.get_all_active.return_value = []
    gateway._maybe_record_trade = AsyncMock()

    result = await gateway.sync_with_exchange()

    gateway.persistence.save_position.assert_called_with(closed_pos)
    gateway._maybe_record_trade.assert_awaited_once_with(closed_pos)
    assert result["issues"] == [
        ("LTC/USD", "CLOSED_ON_EXCHANGE_MISSING_AFTER_EXIT: miss_count=2")
    ]
