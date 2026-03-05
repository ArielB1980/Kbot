from datetime import datetime, timezone
from decimal import Decimal

from src.domain.models import Side
from src.execution.position_manager_v2 import ActionType, PositionManagerV2
from src.execution.position_state_machine import FillRecord, ManagedPosition, PositionState, PositionRegistry


def _make_open_position(symbol: str) -> ManagedPosition:
    pos = ManagedPosition(
        symbol=symbol,
        side=Side.LONG,
        position_id="pos-test-stop-bind",
        initial_size=Decimal("1"),
        initial_entry_price=Decimal("50000"),
        initial_stop_price=Decimal("49000"),
        initial_tp1_price=Decimal("52000"),
        initial_tp2_price=None,
        initial_final_target=None,
    )
    pos.state = PositionState.OPEN
    pos.current_stop_price = Decimal("49000")
    pos.entry_fills.append(
        FillRecord(
            fill_id="fill-entry-1",
            order_id="entry-1",
            side=Side.LONG,
            qty=Decimal("1"),
            price=Decimal("50000"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True,
        )
    )
    pos.stop_order_id = None
    return pos


def test_reconcile_queues_stop_bind_for_pending_adopted_without_live_stop() -> None:
    registry = PositionRegistry()
    pos = _make_open_position("BTC/USD")
    registry.register_position(pos)

    manager = PositionManagerV2(registry=registry)
    actions = manager.reconcile(
        exchange_positions={"PF_BTCUSD": {"side": "long", "qty": "1", "entry_price": "50000"}},
        exchange_orders=[],
        issues=[("BTC/USD", "PENDING_ADOPTED: Registry adopted 1 from exchange (was PENDING)")],
    )

    stop_actions = [a for a in actions if a.type == ActionType.PLACE_STOP]
    assert len(stop_actions) == 1
    action = stop_actions[0]
    assert action.symbol == "BTC/USD"
    assert action.price == Decimal("49000")
    assert action.size == Decimal("1")


def test_reconcile_does_not_queue_stop_bind_when_live_stop_exists() -> None:
    registry = PositionRegistry()
    pos = _make_open_position("BTC/USD")
    registry.register_position(pos)

    manager = PositionManagerV2(registry=registry)
    actions = manager.reconcile(
        exchange_positions={"PF_BTCUSD": {"side": "long", "qty": "1", "entry_price": "50000"}},
        exchange_orders=[
            {
                "id": "stop-live-1",
                "symbol": "PF_BTCUSD",
                "type": "stop",
                "status": "open",
                "side": "sell",
                "reduceOnly": True,
                "stopPrice": 49000,
                "amount": 1,
            }
        ],
        issues=[("BTC/USD", "QTY_SYNCED: entry+1 local=0 exchange=1 price=50000")],
    )

    assert not any(a.type == ActionType.PLACE_STOP for a in actions)
