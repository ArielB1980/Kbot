from datetime import datetime, timedelta, timezone
from decimal import Decimal

from src.config.config import StrategyConfig
from src.memory.institutional_memory import InstitutionalMemoryManager
from src.memory.thesis import Thesis


def _manager() -> InstitutionalMemoryManager:
    cfg = StrategyConfig(memory_enabled=True)
    mgr = InstitutionalMemoryManager(cfg)
    mgr._persist = lambda thesis: None  # type: ignore[assignment]
    return mgr


def test_update_conviction_applies_time_zone_and_volume_decay() -> None:
    mgr = _manager()
    formed_at = datetime.now(timezone.utc) - timedelta(hours=12)
    thesis = Thesis(
        thesis_id="thesis-1",
        symbol="BTC/USD",
        formed_at=formed_at,
        weekly_zone_low=Decimal("100"),
        weekly_zone_high=Decimal("110"),
        daily_bias="bullish",
        current_conviction=100.0,
        last_updated=formed_at,
        last_price_respect_ts=formed_at,
        original_signal_id="sig-1",
        original_volume_avg=Decimal("1000"),
    )

    snap = mgr.update_conviction(
        thesis,
        current_price=Decimal("95"),  # outside zone
        current_volume_avg=Decimal("800"),  # lower than original
    )

    assert snap["time_decay"] == 45.0
    assert snap["zone_rejection"] == 35.0
    assert snap["volume_fade"] == 15.0
    assert snap["conviction"] == 5.0  # floor
    assert thesis.status == "invalidated"


def test_score_adjustment_positive_and_negative() -> None:
    mgr = _manager()
    pos = mgr.conviction_score_adjustment(90.0)
    neg = mgr.conviction_score_adjustment(20.0)

    assert pos > 0
    assert neg < 0
    assert pos <= mgr.config.thesis_score_max_bonus
    assert abs(neg) <= mgr.config.thesis_score_max_penalty
