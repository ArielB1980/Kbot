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
from src.execution.position_state_machine import ExitReason, FillRecord, ManagedPosition, PositionState


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
async def test_process_order_update_partial_close_callback_only_when_exit_qty_progresses():
    gateway = _build_gateway()
    gateway._on_partial_close = MagicMock()

    pending = PendingOrder(
        client_order_id="tp1-pos-1",
        position_id="pos-1",
        symbol="ETH/USD",
        purpose=OrderPurpose.EXIT_TP,
        side=Side.SHORT,
        size=Decimal("0.5"),
        price=Decimal("3100"),
        order_type=OrderType.TAKE_PROFIT,
        submitted_at=datetime.now(timezone.utc),
        exchange_order_id="exchange-tp1-1",
        status="submitted",
    )
    gateway._pending_orders[pending.client_order_id] = pending
    gateway._order_id_map["exchange-tp1-1"] = pending.client_order_id
    gateway.position_manager.handle_order_event.return_value = []
    gateway.registry.get_position.return_value = None

    pre_position = MagicMock()
    pre_position.filled_exit_qty = Decimal("0.3")
    post_position = MagicMock()
    post_position.filled_exit_qty = Decimal("0.5")
    gateway.registry.get_position_any_state.side_effect = [pre_position, post_position]

    await gateway.process_order_update(
        {
            "id": "exchange-tp1-1",
            "clientOrderId": "tp1-pos-1",
            "status": "closed",
            "filled": "0.2",
            "remaining": "0",
            "average": "3100",
            "trades": [],
        }
    )

    gateway._on_partial_close.assert_called_once_with("ETH/USD")


@pytest.mark.asyncio
async def test_process_order_update_partial_close_callback_ignored_when_exit_qty_unchanged():
    gateway = _build_gateway()
    gateway._on_partial_close = MagicMock()

    pending = PendingOrder(
        client_order_id="tp2-pos-1",
        position_id="pos-1",
        symbol="ETH/USD",
        purpose=OrderPurpose.EXIT_TP,
        side=Side.SHORT,
        size=Decimal("0.5"),
        price=Decimal("3150"),
        order_type=OrderType.TAKE_PROFIT,
        submitted_at=datetime.now(timezone.utc),
        exchange_order_id="exchange-tp2-1",
        status="submitted",
    )
    gateway._pending_orders[pending.client_order_id] = pending
    gateway._order_id_map["exchange-tp2-1"] = pending.client_order_id
    gateway.position_manager.handle_order_event.return_value = []
    gateway.registry.get_position.return_value = None

    # Simulate terminal/ignored TP update where state machine does not increase exit qty.
    pre_position = MagicMock()
    pre_position.filled_exit_qty = Decimal("0.5")
    post_position = MagicMock()
    post_position.filled_exit_qty = Decimal("0.5")
    gateway.registry.get_position_any_state.side_effect = [pre_position, post_position]

    await gateway.process_order_update(
        {
            "id": "exchange-tp2-1",
            "clientOrderId": "tp2-pos-1",
            "status": "closed",
            "filled": "0.1",
            "remaining": "0",
            "average": "3150",
            "trades": [],
        }
    )

    gateway._on_partial_close.assert_not_called()


@pytest.mark.asyncio
async def test_execute_entry_passes_action_leverage_to_client():
    gateway = _build_gateway()
    gateway.client.create_order.return_value = {"id": "exchange-entry-2"}
    gateway.registry.get_position.return_value = None
    # _check_entry_liquidity calls await client.fetch_ticker(symbol); return real bid/ask so no coroutine comparison
    gateway.client.fetch_ticker = AsyncMock(
        return_value={"bid": 50000, "ask": 50010}
    )

    action = MagicMock()
    action.type = ActionType.OPEN_POSITION
    action.symbol = "BTC/USD"
    action.reason = "test-entry"
    action.side = Side.LONG
    action.size = Decimal("1")
    action.qty = Decimal("1")
    action.price = Decimal("50000")
    action.leverage = Decimal("3")
    action.order_type = OrderType.LIMIT
    action.client_order_id = "entry-client-2"
    action.position_id = "pos-2"
    action.priority = 10

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


@pytest.mark.asyncio
async def test_backfill_exit_fills_only_adds_missing_delta_per_order():
    gateway = _build_gateway()
    gateway.client.fetch_order.return_value = {
        "status": "filled",
        "filled": "8",
        "average": "110",
    }

    position = ManagedPosition(
        symbol="BTC/USD",
        side=Side.LONG,
        position_id="pos-1",
        initial_size=Decimal("10"),
        initial_entry_price=Decimal("100"),
        initial_stop_price=Decimal("95"),
        initial_tp1_price=Decimal("110"),
        initial_tp2_price=None,
        initial_final_target=None,
    )
    position.entry_fills.append(
        FillRecord(
            fill_id="entry-fill-1",
            order_id="entry-1",
            side=Side.LONG,
            qty=Decimal("10"),
            price=Decimal("100"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True,
        )
    )
    position.tp1_order_id = "tp1-1"
    position.exit_fills.append(
        FillRecord(
            fill_id="existing-exit-fill",
            order_id="tp1-1",
            side=Side.SHORT,
            qty=Decimal("4"),
            price=Decimal("110"),
            timestamp=datetime.now(timezone.utc),
            is_entry=False,
        )
    )
    position._mark_closed(ExitReason.TAKE_PROFIT_1)

    await gateway._backfill_exit_fills(position)

    assert len(position.exit_fills) == 2
    assert position.exit_fills[-1].order_id == "tp1-1"
    assert position.exit_fills[-1].qty == Decimal("4")


@pytest.mark.asyncio
async def test_backfill_exit_fills_skips_when_order_already_fully_represented():
    gateway = _build_gateway()
    gateway.client.fetch_order.return_value = {
        "status": "filled",
        "filled": "4",
        "average": "110",
    }

    position = ManagedPosition(
        symbol="BTC/USD",
        side=Side.LONG,
        position_id="pos-2",
        initial_size=Decimal("10"),
        initial_entry_price=Decimal("100"),
        initial_stop_price=Decimal("95"),
        initial_tp1_price=Decimal("110"),
        initial_tp2_price=None,
        initial_final_target=None,
    )
    position.entry_fills.append(
        FillRecord(
            fill_id="entry-fill-1",
            order_id="entry-1",
            side=Side.LONG,
            qty=Decimal("10"),
            price=Decimal("100"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True,
        )
    )
    position.tp1_order_id = "tp1-1"
    position.exit_fills.append(
        FillRecord(
            fill_id="existing-exit-fill",
            order_id="tp1-1",
            side=Side.SHORT,
            qty=Decimal("4"),
            price=Decimal("110"),
            timestamp=datetime.now(timezone.utc),
            is_entry=False,
        )
    )
    position._mark_closed(ExitReason.TAKE_PROFIT_1)

    await gateway._backfill_exit_fills(position)

    assert len(position.exit_fills) == 1


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


@pytest.mark.parametrize(
    "purpose",
    [OrderPurpose.EXIT_TP, OrderPurpose.EXIT_REVERSAL],
    ids=["tp", "reversal"],
)
async def test_poll_and_process_detects_tp_and_reversal_fills(purpose):
    """EXIT_TP and EXIT_REVERSAL orders must be polled (KBO-40 fix)."""
    gateway = _build_gateway()

    pending = PendingOrder(
        client_order_id="exit-tp-1",
        position_id="pos-1",
        symbol="BTC/USD",
        purpose=purpose,
        side=Side.SHORT,
        size=Decimal("0.5"),
        price=Decimal("55000"),
        order_type=OrderType.LIMIT,
        submitted_at=datetime.now(timezone.utc),
        exchange_order_id="exchange-tp-1",
        status="submitted",
    )
    gateway._pending_orders[pending.client_order_id] = pending
    gateway._order_id_map["exchange-tp-1"] = pending.client_order_id

    gateway.client.fetch_order = AsyncMock(
        return_value={
            "id": "exchange-tp-1",
            "clientOrderId": "exit-tp-1",
            "status": "closed",
            "filled": "0.5",
            "remaining": "0.0",
            "average": "55000",
            "trades": [],
        }
    )
    # Return a non-empty list so poll_and_process counts it as processed
    gateway.position_manager.handle_order_event.return_value = [MagicMock()]
    gateway.registry.get_position.return_value = None

    processed = await gateway.poll_and_process_order_updates()

    # Verify the TP/reversal order was polled and processed
    gateway.client.fetch_order.assert_called_once_with("exchange-tp-1", "BTC/USD")
    assert processed == 1
