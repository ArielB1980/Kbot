from datetime import datetime, timedelta, timezone
from decimal import Decimal

from src.config.config import StrategyConfig
from src.execution.position_manager_v2 import PositionManagerV2
from src.execution.position_state_machine import (
    FillRecord,
    ManagedPosition,
    PositionRegistry,
    PositionState,
)
from src.domain.models import SetupType, Signal, SignalType, Side
from src.memory.institutional_memory import InstitutionalMemoryManager
from src.memory.thesis import Thesis


def _strategy() -> StrategyConfig:
    return StrategyConfig(
        memory_enabled=True,
        thesis_alerts_enabled=True,
        thesis_observe_only=False,
        thesis_management_enabled=True,
        thesis_early_exit_threshold=35.0,
        thesis_reentry_block_threshold=25.0,
    )


def _thesis() -> Thesis:
    formed = datetime.now(timezone.utc) - timedelta(hours=14)
    return Thesis(
        thesis_id="thesis-btc",
        symbol="BTC/USD",
        formed_at=formed,
        weekly_zone_low=Decimal("100"),
        weekly_zone_high=Decimal("110"),
        daily_bias="bullish",
        current_conviction=80.0,
        last_updated=formed,
        last_price_respect_ts=formed,
        original_signal_id="sig-1",
        original_volume_avg=Decimal("1000"),
        status="active",
    )


def test_conviction_collapse_and_invalidated_alerts(monkeypatch):
    cfg = _strategy()
    mgr = InstitutionalMemoryManager(cfg)
    t = _thesis()

    sent = []
    monkeypatch.setattr("src.memory.institutional_memory.send_alert_sync", lambda *args, **kwargs: sent.append((args, kwargs)))
    monkeypatch.setattr(mgr, "_persist", lambda thesis: None)

    mgr.update_conviction(
        t,
        current_price=Decimal("95"),  # outside zone => strong decay
        current_volume_avg=Decimal("500"),
        emit_log=False,
    )

    event_types = [args[0] for args, _ in sent]
    assert "THESIS_CONVICTION_COLLAPSE" in event_types
    assert "THESIS_INVALIDATED" in event_types


class _MemoryLowConviction:
    def update_conviction_for_symbol(self, symbol: str, **kwargs):
        return {"conviction": 20.0}

    def should_block_reentry(self, symbol: str, conviction=None):
        return True


def _open_position(symbol: str = "BTC/USD") -> ManagedPosition:
    p = ManagedPosition(
        symbol=symbol,
        side=Side.LONG,
        position_id="pos-alert-1",
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


def test_position_manager_reentry_and_early_exit_alerts(monkeypatch):
    sent = []
    monkeypatch.setattr("src.execution.position_manager_v2.send_alert_sync", lambda *args, **kwargs: sent.append((args, kwargs)))

    cfg = _strategy()
    registry = PositionRegistry()
    pm = PositionManagerV2(registry=registry, strategy_config=cfg, institutional_memory=_MemoryLowConviction())

    # Re-entry blocked alert
    action, pos = pm.evaluate_entry(
        signal=_signal(),
        entry_price=Decimal("100"),
        stop_price=Decimal("90"),
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        final_target=Decimal("130"),
        position_size=Decimal("1"),
    )
    assert pos is None
    assert action.type.value == "reject_entry"

    # Early exit alert
    registry.register_position(_open_position())
    actions = pm.evaluate_position("BTC/USD", current_price=Decimal("103"), current_atr=Decimal("1"))
    assert actions

    event_types = [args[0] for args, _ in sent]
    assert "THESIS_REENTRY_BLOCKED" in event_types
    assert "THESIS_EARLY_EXIT_TRIGGERED" in event_types
