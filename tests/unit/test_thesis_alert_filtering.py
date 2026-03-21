from datetime import UTC, datetime, timedelta
from decimal import Decimal

from src.config.config import StrategyConfig
from src.memory.institutional_memory import InstitutionalMemoryManager
from src.memory.thesis import Thesis
from src.monitoring.alert_dispatcher import (
    THESIS_ALLOWED_EVENT_TYPES as _THESIS_ALLOWED_EVENT_TYPES,
)


def _thesis() -> Thesis:
    formed = datetime.now(UTC) - timedelta(hours=14)
    return Thesis(
        thesis_id="thesis-filter-1",
        symbol="BTC/USD",
        formed_at=formed,
        weekly_zone_low=Decimal("100"),
        weekly_zone_high=Decimal("110"),
        daily_bias="bullish",
        current_conviction=80.0,
        last_updated=formed,
        last_price_respect_ts=formed,
        original_signal_id="sig-filter-1",
        original_volume_avg=Decimal("1000"),
        status="active",
    )


def test_thesis_alerts_suppressed_without_open_position(monkeypatch):
    cfg = StrategyConfig(
        memory_enabled=True,
        thesis_alerts_enabled=True,
        thesis_alert_open_positions_only=True,
    )
    mgr = InstitutionalMemoryManager(cfg)
    t = _thesis()

    sent = []
    monkeypatch.setattr(
        "src.memory.institutional_memory.send_alert_sync",
        lambda *args, **kwargs: sent.append((args, kwargs)),
    )
    monkeypatch.setattr("src.memory.institutional_memory.get_active_position", lambda symbol: None)
    monkeypatch.setattr(mgr, "_persist", lambda thesis: None)

    mgr.update_conviction(
        t,
        current_price=Decimal("95"),  # outside zone -> invalidation path
        current_volume_avg=Decimal("500"),
        emit_log=False,
    )

    assert sent == []


def test_thesis_alerts_emit_with_open_position(monkeypatch):
    cfg = StrategyConfig(
        memory_enabled=True,
        thesis_alerts_enabled=True,
        thesis_alert_open_positions_only=True,
    )
    mgr = InstitutionalMemoryManager(cfg)
    t = _thesis()

    sent = []
    monkeypatch.setattr(
        "src.memory.institutional_memory.send_alert_sync",
        lambda *args, **kwargs: sent.append((args, kwargs)),
    )
    monkeypatch.setattr(
        "src.memory.institutional_memory.get_active_position", lambda symbol: object()
    )
    monkeypatch.setattr(mgr, "_persist", lambda thesis: None)

    mgr.update_conviction(
        t,
        current_price=Decimal("95"),
        current_volume_avg=Decimal("500"),
        emit_log=False,
    )

    event_types = [args[0] for args, _ in sent]
    assert "THESIS_CONVICTION_COLLAPSE" in event_types
    assert "THESIS_INVALIDATED" in event_types


def test_trade_open_close_events_are_allowed():
    assert "THESIS_TRADE_OPENED" in _THESIS_ALLOWED_EVENT_TYPES
    assert "THESIS_TRADE_CLOSED" in _THESIS_ALLOWED_EVENT_TYPES
