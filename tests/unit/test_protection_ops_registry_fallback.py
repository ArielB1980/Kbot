from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.execution.position_state_machine import (
    FillRecord,
    ManagedPosition,
    PositionRegistry,
    PositionState,
)
from src.domain.models import Side
from src.live.protection_ops import reconcile_protective_orders, reconcile_stop_loss_order_ids


class _FakeClient:
    async def get_futures_open_orders(self):
        return [
            {
                "id": "live-stop",
                "symbol": "DASH/USD:USD",
                "side": "buy",
                "reduceOnly": True,
                "type": "stop",
                "stopPrice": "35.356",
            }
        ]


class _FakePersistence:
    def __init__(self):
        self.saved = []

    def save_position(self, position):
        self.saved.append((position.symbol, position.stop_order_id))


def _build_managed(symbol: str, *, state: PositionState, stop_order_id: str) -> ManagedPosition:
    pos = ManagedPosition(
        symbol=symbol,
        side=Side.SHORT,
        position_id=f"{symbol}-id",
        initial_size=Decimal("3.2"),
        initial_entry_price=Decimal("33.0"),
        initial_stop_price=Decimal("35.356"),
        initial_tp1_price=Decimal("31.943"),
        initial_tp2_price=None,
        initial_final_target=None,
    )
    pos.entry_fills.append(
        FillRecord(
            fill_id=f"{symbol}-fill",
            order_id=f"{symbol}-entry",
            side=Side.SHORT,
            qty=Decimal("3.2"),
            price=Decimal("33.0"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True,
        )
    )
    pos.state = state
    pos.stop_order_id = stop_order_id
    return pos


@pytest.mark.asyncio
async def test_reconcile_stop_loss_order_ids_uses_registry_fallback_and_updates_duplicates(monkeypatch):
    registry = PositionRegistry()
    open_pos = _build_managed("DASH/USD", state=PositionState.OPEN, stop_order_id="old-open-stop")
    stale_pos = _build_managed(
        "PF_DASHUSD",
        state=PositionState.EXIT_PENDING,
        stop_order_id="old-stale-stop",
    )
    registry.register_position(open_pos)
    registry._positions[stale_pos.symbol] = stale_pos

    persistence = _FakePersistence()
    lt = SimpleNamespace(
        client=_FakeClient(),
        use_state_machine_v2=True,
        position_registry=registry,
        position_persistence=persistence,
        execution_gateway=None,
    )

    import src.storage.repository as repository

    monkeypatch.setattr(repository, "get_active_position", lambda symbol: None)
    monkeypatch.setattr(
        repository,
        "save_position",
        lambda position: (_ for _ in ()).throw(AssertionError("postgres save should not be used")),
    )

    await reconcile_stop_loss_order_ids(
        lt,
        [{"symbol": "PF_DASHUSD", "side": "short", "size": "3.2", "entryPrice": "33.0"}],
    )

    assert open_pos.stop_order_id == "live-stop"
    assert stale_pos.stop_order_id == "live-stop"
    assert ("DASH/USD", "live-stop") in persistence.saved
    assert ("PF_DASHUSD", "live-stop") in persistence.saved


@pytest.mark.asyncio
async def test_reconcile_protective_orders_skips_legacy_backfill_in_replay_v2():
    lt = SimpleNamespace(
        config=SimpleNamespace(
            execution=SimpleNamespace(tp_backfill_enabled=True),
        ),
        use_state_machine_v2=True,
        _replay_relaxed_signal_gates=True,
        client=SimpleNamespace(),
    )

    await reconcile_protective_orders(
        lt,
        [{"symbol": "XRP/USD", "size": "100"}],
        {"XRP/USD": Decimal("1.4")},
    )
