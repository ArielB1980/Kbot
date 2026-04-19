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

from src.config.config import StrategyConfig
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


# --- _compute_zone_relation ----------------------------------------------


def test_zone_relation_contained():
    # inner [100, 102] fully inside outer [99, 105]
    rel = SMCEngine._compute_zone_relation(
        Decimal("100"), Decimal("102"), Decimal("99"), Decimal("105"),
    )
    assert rel == "contained"


def test_zone_relation_contained_on_exact_boundary():
    # inner exactly equal to outer — contained (inclusive bounds)
    rel = SMCEngine._compute_zone_relation(
        Decimal("100"), Decimal("105"), Decimal("100"), Decimal("105"),
    )
    assert rel == "contained"


def test_zone_relation_overlapping_from_below():
    # inner [98, 101] overlaps outer [100, 105] from below
    rel = SMCEngine._compute_zone_relation(
        Decimal("98"), Decimal("101"), Decimal("100"), Decimal("105"),
    )
    assert rel == "overlapping"


def test_zone_relation_overlapping_from_above():
    # inner [103, 107] overlaps outer [100, 105] from above
    rel = SMCEngine._compute_zone_relation(
        Decimal("103"), Decimal("107"), Decimal("100"), Decimal("105"),
    )
    assert rel == "overlapping"


def test_zone_relation_none():
    # inner entirely above outer
    rel = SMCEngine._compute_zone_relation(
        Decimal("110"), Decimal("115"), Decimal("100"), Decimal("105"),
    )
    assert rel == "none"
    # inner entirely below outer
    rel = SMCEngine._compute_zone_relation(
        Decimal("90"), Decimal("95"), Decimal("100"), Decimal("105"),
    )
    assert rel == "none"


# --- _detect_multi_tf_levels wrapper gating ------------------------------


def _make_engine() -> SMCEngine:
    return SMCEngine(StrategyConfig())


def test_detect_multi_tf_levels_empty_context_returns_empty():
    engine = _make_engine()
    out = engine._detect_multi_tf_levels(
        symbol="BTC/USD",
        signal_timestamp=datetime(2026, 3, 1, tzinfo=timezone.utc),
        decision_tf="4h",
    )
    assert out == {}


def test_detect_multi_tf_levels_no_symbol_returns_empty():
    engine = _make_engine()
    engine._higher_tf_candle_context["BTC/USD"] = {
        "1d": [_mk_candle("1d", datetime(2026, 1, 1, tzinfo=timezone.utc))]
    }
    out = engine._detect_multi_tf_levels(
        symbol="",
        signal_timestamp=datetime(2026, 3, 1, tzinfo=timezone.utc),
        decision_tf="4h",
    )
    assert out == {}


def test_detect_multi_tf_levels_unknown_decision_tf_returns_empty():
    engine = _make_engine()
    engine._higher_tf_candle_context["BTC/USD"] = {
        "1d": [_mk_candle("1d", datetime(2026, 1, 1, tzinfo=timezone.utc))]
    }
    out = engine._detect_multi_tf_levels(
        symbol="BTC/USD",
        signal_timestamp=datetime(2026, 3, 1, tzinfo=timezone.utc),
        decision_tf="3d",  # unsupported
    )
    assert out == {}


def test_detect_multi_tf_levels_skips_tfs_not_strictly_higher():
    # decision_tf = 1d → 1d is not strictly higher than itself; only 1w qualifies.
    engine = _make_engine()
    base = datetime(2026, 1, 5, tzinfo=timezone.utc)  # Monday, ISO week 2
    daily = [_mk_candle("1d", base + timedelta(days=i)) for i in range(14)]
    engine._higher_tf_candle_context["BTC/USD"] = {"1d": daily}
    cutoff = base + timedelta(days=14)  # all 14 days complete
    out = engine._detect_multi_tf_levels(
        symbol="BTC/USD",
        signal_timestamp=cutoff,
        decision_tf="1d",
    )
    assert "1d" not in out
    # 14 days span weeks 2 + 3, both full → 1w entry exists.
    assert "1w" in out


def test_detect_multi_tf_levels_includes_1d_for_4h_decision():
    engine = _make_engine()
    base = datetime(2026, 1, 5, tzinfo=timezone.utc)
    daily = [_mk_candle("1d", base + timedelta(days=i)) for i in range(14)]
    engine._higher_tf_candle_context["BTC/USD"] = {"1d": daily}
    cutoff = base + timedelta(days=14)
    out = engine._detect_multi_tf_levels(
        symbol="BTC/USD",
        signal_timestamp=cutoff,
        decision_tf="4h",
    )
    assert "1d" in out
    # Structure of each TF entry: both OB and FVG keys present (values may be None).
    assert set(out["1d"].keys()) == {"order_block", "fvg"}
    assert "1w" in out
    assert set(out["1w"].keys()) == {"order_block", "fvg"}


def test_detect_multi_tf_levels_skips_1w_when_decision_is_1w():
    engine = _make_engine()
    base = datetime(2026, 1, 5, tzinfo=timezone.utc)
    daily = [_mk_candle("1d", base + timedelta(days=i)) for i in range(14)]
    engine._higher_tf_candle_context["BTC/USD"] = {"1d": daily}
    cutoff = base + timedelta(days=14)
    out = engine._detect_multi_tf_levels(
        symbol="BTC/USD",
        signal_timestamp=cutoff,
        decision_tf="1w",
    )
    # Neither 1d (< 1w) nor 1w (not strictly higher than itself) qualifies.
    assert out == {}


# --- _compute_tf_stack ---------------------------------------------------


def _mk_ob(
    tf_origin: str,
    body_low: Decimal,
    body_high: Decimal,
    *,
    ob_type: str = "bullish",
    freshness: str = "fully_untouched",
    body_freshness: str = "fully_untouched",
    age_candles: int = 5,
) -> dict:
    return {
        "type": ob_type,
        "body_low": body_low,
        "body_high": body_high,
        "freshness": freshness,
        "body_freshness": body_freshness,
        "age_candles": age_candles,
        "timeframe_origin": tf_origin,
    }


def _mk_fvg(
    tf_origin: str, freshness: str = "fully_untouched", age_candles: int = 3
) -> dict:
    return {"freshness": freshness, "age_candles": age_candles, "timeframe_origin": tf_origin}


def test_compute_tf_stack_no_4h_ob_returns_empty_defaults():
    stack = SMCEngine._compute_tf_stack(None, {"1d": {"order_block": _mk_ob("1d", Decimal("90"), Decimal("120"))}})
    assert stack["tf_stack_depth_contained"] == 0
    assert stack["tf_stack_depth_overlapping"] == 0
    assert stack["tf_stack_bias_conflict"] is False
    assert stack["tf_stack_relation"] == {"1d": "none", "1w": "none"}
    assert stack["htf_ob_bias"] == {"1d": None, "1w": None}


def test_compute_tf_stack_contained_bias_match_counts_depth_contained():
    four_h = _mk_ob("4h", Decimal("100"), Decimal("105"), ob_type="bullish")
    htf = {
        "1d": {
            "order_block": _mk_ob("1d", Decimal("95"), Decimal("110"), ob_type="bullish"),
            "fvg": None,
        }
    }
    stack = SMCEngine._compute_tf_stack(four_h, htf)
    assert stack["tf_stack_depth_contained"] == 1
    assert stack["tf_stack_depth_overlapping"] == 0
    assert stack["tf_stack_bias_conflict"] is False
    assert stack["tf_stack_relation"]["1d"] == "contained"
    assert stack["htf_ob_bias"]["1d"] == "bullish"


def test_compute_tf_stack_bias_conflict_contained_downgraded_to_none():
    # 4H bullish contained inside a bearish 1D OB — conflict downgrades to "none",
    # bias_conflict flag flips true, depth_contained stays 0.
    four_h = _mk_ob("4h", Decimal("100"), Decimal("105"), ob_type="bullish")
    htf = {
        "1d": {
            "order_block": _mk_ob("1d", Decimal("95"), Decimal("110"), ob_type="bearish"),
            "fvg": None,
        }
    }
    stack = SMCEngine._compute_tf_stack(four_h, htf)
    assert stack["tf_stack_depth_contained"] == 0
    assert stack["tf_stack_depth_overlapping"] == 0
    assert stack["tf_stack_relation"]["1d"] == "none"
    assert stack["tf_stack_bias_conflict"] is True


def test_compute_tf_stack_bias_conflict_overlapping_stays_in_overlap_bucket():
    # 4H bullish overlaps (not contained) bearish 1D OB — keep in overlapping bucket.
    four_h = _mk_ob("4h", Decimal("98"), Decimal("108"), ob_type="bullish")
    htf = {
        "1d": {
            "order_block": _mk_ob("1d", Decimal("100"), Decimal("115"), ob_type="bearish"),
            "fvg": None,
        }
    }
    stack = SMCEngine._compute_tf_stack(four_h, htf)
    assert stack["tf_stack_depth_contained"] == 0
    assert stack["tf_stack_depth_overlapping"] == 1
    assert stack["tf_stack_relation"]["1d"] == "overlapping"
    assert stack["tf_stack_bias_conflict"] is True


def test_compute_tf_stack_both_tfs_contained_depth_2():
    four_h = _mk_ob("4h", Decimal("100"), Decimal("105"), ob_type="bullish")
    htf = {
        "1d": {
            "order_block": _mk_ob("1d", Decimal("95"), Decimal("110"), ob_type="bullish"),
            "fvg": _mk_fvg("1d", freshness="wick_tested"),
        },
        "1w": {
            "order_block": _mk_ob("1w", Decimal("90"), Decimal("115"), ob_type="bullish", body_freshness="wick_tested", age_candles=12),
            "fvg": None,
        },
    }
    stack = SMCEngine._compute_tf_stack(four_h, htf)
    assert stack["tf_stack_depth_contained"] == 2
    assert stack["tf_stack_depth_overlapping"] == 0
    assert stack["tf_stack_bias_conflict"] is False
    assert stack["htf_ob_body_freshness"]["1w"] == "wick_tested"
    assert stack["htf_ob_age_candles"]["1w"] == 12
    assert stack["htf_fvg_freshness"]["1d"] == "wick_tested"


def test_compute_tf_stack_no_htf_ob_leaves_tf_entry_as_none():
    four_h = _mk_ob("4h", Decimal("100"), Decimal("105"), ob_type="bullish")
    htf = {"1d": {"order_block": None, "fvg": None}}
    stack = SMCEngine._compute_tf_stack(four_h, htf)
    assert stack["tf_stack_depth_contained"] == 0
    assert stack["tf_stack_relation"]["1d"] == "none"
    assert stack["htf_ob_bias"]["1d"] is None


def test_compute_tf_stack_missing_htf_tfs_default_none():
    four_h = _mk_ob("4h", Decimal("100"), Decimal("105"), ob_type="bullish")
    stack = SMCEngine._compute_tf_stack(four_h, {})  # no 1d or 1w entry
    assert stack["tf_stack_relation"] == {"1d": "none", "1w": "none"}
    assert stack["htf_ob_bias"] == {"1d": None, "1w": None}
    assert stack["htf_ob_age_candles"] == {"1d": None, "1w": None}


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
