from datetime import datetime, timezone
from decimal import Decimal

from src.config.config import StrategyConfig
from src.domain.models import SetupType, Signal, SignalType, Side
from src.execution.position_manager_v2 import ActionType, PositionManagerV2
from src.execution.position_state_machine import FillRecord, ManagedPosition, PositionRegistry, PositionState


class _MemoryLowConviction:
    def update_conviction_for_symbol(self, symbol: str, **kwargs):
        return {"conviction": 20.0}

    def should_block_reentry(self, symbol: str, conviction=None) -> bool:
        return True


def _open_position(symbol: str = "BTC/USD") -> ManagedPosition:
    p = ManagedPosition(
        symbol=symbol,
        side=Side.LONG,
        position_id="pos-memory-1",
        initial_size=Decimal("1"),
        initial_entry_price=Decimal("100"),
        initial_stop_price=Decimal("90"),
        initial_tp1_price=Decimal("130"),
        initial_tp2_price=Decimal("140"),
        initial_final_target=Decimal("150"),
    )
    p.state = PositionState.OPEN
    p.current_stop_price = Decimal("90")
    p.entry_fills.append(
        FillRecord(
            fill_id="entry-fill-1",
            order_id="entry-order-1",
            side=Side.LONG,
            qty=Decimal("1"),
            price=Decimal("100"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True,
        )
    )
    return p


def _signal(symbol: str = "BTC/USD") -> Signal:
    now = datetime.now(timezone.utc)
    return Signal(
        timestamp=now,
        symbol=symbol,
        signal_type=SignalType.LONG,
        entry_price=Decimal("100"),
        stop_loss=Decimal("90"),
        take_profit=Decimal("120"),
        reasoning="test",
        setup_type=SetupType.OB,
        regime="tight_smc",
        higher_tf_bias="bullish",
        adx=Decimal("30"),
        atr=Decimal("1"),
        ema200_slope="up",
    )


def test_evaluate_position_early_exits_on_low_conviction() -> None:
    registry = PositionRegistry()
    registry.register_position(_open_position())
    cfg = StrategyConfig(
        memory_enabled=True,
        thesis_observe_only=False,
        thesis_management_enabled=True,
        thesis_early_exit_threshold=35.0,
    )
    pm = PositionManagerV2(registry=registry, strategy_config=cfg, institutional_memory=_MemoryLowConviction())

    actions = pm.evaluate_position("BTC/USD", current_price=Decimal("104"), current_atr=Decimal("1"))

    assert actions
    assert actions[0].type == ActionType.CLOSE_FULL
    assert "conviction" in actions[0].reason.lower()


def test_evaluate_entry_blocks_reentry_when_thesis_decayed() -> None:
    registry = PositionRegistry()
    cfg = StrategyConfig(
        memory_enabled=True,
        thesis_observe_only=False,
        thesis_management_enabled=True,
        thesis_reentry_block_threshold=25.0,
    )
    pm = PositionManagerV2(registry=registry, strategy_config=cfg, institutional_memory=_MemoryLowConviction())

    action, position = pm.evaluate_entry(
        signal=_signal(),
        entry_price=Decimal("100"),
        stop_price=Decimal("90"),
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        final_target=Decimal("130"),
        position_size=Decimal("1"),
    )

    assert position is None
    assert action.type == ActionType.REJECT_ENTRY
    assert "re-entry blocked" in action.reason.lower()
