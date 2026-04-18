"""Phase 2A Step 0: lookahead-safety guarantees for multi-TF level detection.

The multi-TF stacking work runs OB/FVG detection on 1D and 1W candles alongside
the existing 4H detection. If the HTF candle slice at signal time includes an
in-progress bar (daily bar whose close_time is still in the future, or a partial
ISO week), the detection silently uses future information and invalidates the
entire validation replay.

These tests enforce the slicing contract that Phase 2A relies on:
  - _candle_duration maps timeframe strings to timedeltas correctly.
  - _slice_completed_candles drops in-progress bars.
  - _to_weekly_candles_completed excludes partial ISO weeks at the cutoff.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.domain.models import Candle
from src.strategy.smc_engine import SMCEngine


def _mk_candle(
    tf: str,
    open_time: datetime,
    *,
    price: Decimal = Decimal("100"),
    symbol: str = "BTC/USD",
) -> Candle:
    return Candle(
        timestamp=open_time,
        symbol=symbol,
        timeframe=tf,
        open=price,
        high=price + Decimal("1"),
        low=price - Decimal("1"),
        close=price,
        volume=Decimal("1"),
    )


# --- _candle_duration -----------------------------------------------------


def test_candle_duration_known_timeframes():
    assert SMCEngine._candle_duration("1m") == timedelta(minutes=1)
    assert SMCEngine._candle_duration("15m") == timedelta(minutes=15)
    assert SMCEngine._candle_duration("1h") == timedelta(hours=1)
    assert SMCEngine._candle_duration("4h") == timedelta(hours=4)
    assert SMCEngine._candle_duration("1d") == timedelta(days=1)
    assert SMCEngine._candle_duration("1w") == timedelta(weeks=1)


def test_candle_duration_unknown_raises():
    with pytest.raises(ValueError):
        SMCEngine._candle_duration("3d")


# --- _slice_completed_candles ---------------------------------------------


def test_slice_completed_daily_excludes_in_progress_bar():
    # 10 daily bars starting 2026-01-01, one per day.
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    candles = [_mk_candle("1d", base + timedelta(days=i)) for i in range(10)]
    # Cutoff is mid-day on day 10 (the 10th bar opens at 2026-01-10 and closes
    # at 2026-01-11 — still in progress at 2026-01-10 12:00).
    cutoff = base + timedelta(days=9, hours=12)
    completed = SMCEngine._slice_completed_candles(candles, cutoff)
    assert len(completed) == 9
    # The last completed candle opens at day 8 (closes at day 9 00:00 <= cutoff).
    assert completed[-1].timestamp == base + timedelta(days=8)


def test_slice_completed_cutoff_at_exact_close_includes_bar():
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    candles = [_mk_candle("1d", base + timedelta(days=i)) for i in range(3)]
    # Cutoff exactly at the close of bar index 2 (2026-01-04 00:00).
    cutoff = base + timedelta(days=3)
    completed = SMCEngine._slice_completed_candles(candles, cutoff)
    # All three bars have closed at or before the cutoff.
    assert len(completed) == 3


def test_slice_completed_empty_input():
    cutoff = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert SMCEngine._slice_completed_candles([], cutoff) == []


def test_slice_completed_honors_per_candle_timeframe():
    # 4h and 1h candles mixed in one series — helper must respect each bar's TF.
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    mixed = [
        _mk_candle("4h", base),                       # closes base+4h
        _mk_candle("1h", base + timedelta(hours=5)),  # closes base+6h
    ]
    cutoff = base + timedelta(hours=5, minutes=30)
    completed = SMCEngine._slice_completed_candles(mixed, cutoff)
    # 4h bar closed at base+4h (completed). 1h bar closes at base+6h (in progress).
    assert len(completed) == 1
    assert completed[0].timeframe == "4h"


# --- _to_weekly_candles_completed -----------------------------------------


def test_weekly_completed_excludes_partial_current_week():
    # Build 14 days starting Monday 2026-01-05. That spans exactly 2 ISO weeks
    # (week 2 and week 3 of 2026). Cutoff mid-week-3 should drop week 3 entirely.
    base = datetime(2026, 1, 5, tzinfo=timezone.utc)  # Monday, ISO week 2
    candles = [_mk_candle("1d", base + timedelta(days=i)) for i in range(14)]
    # Cutoff mid-day Wednesday of ISO week 3 (only 2 days of week 3 completed
    # by end of that Tuesday; Wednesday is in progress).
    cutoff = base + timedelta(days=9, hours=12)  # 2026-01-14 12:00 UTC (Wed)
    weekly = SMCEngine._to_weekly_candles_completed(candles, cutoff)
    # Only ISO week 2 (fully completed at cutoff) should be present.
    assert len(weekly) == 1
    assert weekly[0].timeframe == "1w"
    assert weekly[0].timestamp.isocalendar().week == 2


def test_weekly_completed_at_week_boundary_includes_full_week():
    # Cutoff exactly at Monday 00:00 of the next ISO week — all 7 days of the
    # prior week are closed, so that week must be included.
    base = datetime(2026, 1, 5, tzinfo=timezone.utc)  # Monday, ISO week 2
    candles = [_mk_candle("1d", base + timedelta(days=i)) for i in range(7)]
    cutoff = base + timedelta(days=7)  # 2026-01-12 00:00 = Monday of week 3
    weekly = SMCEngine._to_weekly_candles_completed(candles, cutoff)
    assert len(weekly) == 1


def test_weekly_completed_empty_input():
    cutoff = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert SMCEngine._to_weekly_candles_completed([], cutoff) == []


def test_weekly_completed_drops_leading_partial_week():
    # Daily candles start mid-week (Wednesday), spanning to the following
    # Monday. The first ISO week has only 5 days and must be dropped even if
    # those 5 days are all completed by the cutoff.
    # 2026-01-07 is a Wednesday (ISO week 2).
    start = datetime(2026, 1, 7, tzinfo=timezone.utc)
    # 5 days of week 2 (Wed..Sun) + 7 days of week 3 = 12 candles.
    candles = [_mk_candle("1d", start + timedelta(days=i)) for i in range(12)]
    cutoff = start + timedelta(days=12)  # Monday of week 4; week 3 is complete.
    weekly = SMCEngine._to_weekly_candles_completed(candles, cutoff)
    # Week 2 has only 5 daily bars — dropped. Week 3 has all 7 — kept.
    assert len(weekly) == 1
    assert weekly[0].timestamp.isocalendar().week == 3


# --- integration guard: legacy _to_weekly_candles is unchanged -----------


def test_legacy_to_weekly_candles_still_includes_partial_weeks():
    """The partial-week-inclusive _to_weekly_candles is still used by
    HigherTFContext (weekly_confluence_bonus). Phase 2A must leave it alone;
    only _to_weekly_candles_completed applies the trim.
    """
    base = datetime(2026, 1, 5, tzinfo=timezone.utc)  # Monday, ISO week 2
    # 10 days: 7 full days of week 2 + 3 days of week 3 (partial).
    candles = [_mk_candle("1d", base + timedelta(days=i)) for i in range(10)]
    weekly = SMCEngine._to_weekly_candles(candles)
    # Legacy aggregator includes the partial week.
    assert len(weekly) == 2
