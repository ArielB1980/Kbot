"""Tests for signal logging, daily reports, and performance attribution."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from src.research.daily_report import (
    HitRateStats,
    build_daily_summary,
    generate_daily_report,
    summary_to_json,
    summary_to_markdown,
)
from src.research.signal_attribution import (
    AttributionBreakdown,
    SignalAttributionTracker,
    TradeRecord,
)
from src.research.signal_logger import (
    SignalFileLogger,
    SignalLogEntry,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_entry(
    *,
    symbol: str = "BTC/USD",
    signal_type: str = "long",
    direction: str = "long",
    score: float = 75.0,
    regime: str = "tight_smc",
    setup_type: str = "ob",
    strategy_source: str = "smc_v1",
    timestamp: str = "2026-03-20T12:00:00+00:00",
) -> SignalLogEntry:
    return SignalLogEntry(
        timestamp=timestamp,
        symbol=symbol,
        signal_type=signal_type,
        direction=direction,
        entry_price=50000.0,
        stop_loss=49500.0,
        take_profit=51000.0,
        score=score,
        score_breakdown={"smc_quality": 20.0, "fib_confluence": 15.0},
        setup_type=setup_type,
        regime=regime,
        higher_tf_bias="bullish",
        adx=35.0,
        atr=500.0,
        ema200_slope="up",
        strategy_source=strategy_source,
        reasoning="Test signal",
    )


def _make_trade(
    *,
    trade_id: str = "t1",
    symbol: str = "BTC/USD",
    direction: str = "long",
    strategy_source: str = "smc_v1",
    regime: str = "tight_smc",
    setup_type: str = "ob",
    realized_pnl: float = 100.0,
    size_notional: float = 1000.0,
) -> TradeRecord:
    return TradeRecord(
        trade_id=trade_id,
        signal_timestamp="2026-03-20T12:00:00+00:00",
        symbol=symbol,
        direction=direction,
        strategy_source=strategy_source,
        regime=regime,
        setup_type=setup_type,
        signal_score=75.0,
        entry_price=50000.0,
        exit_price=50100.0,
        realized_pnl=realized_pnl,
        size_notional=size_notional,
        entry_time="2026-03-20T12:00:00+00:00",
        exit_time="2026-03-20T14:00:00+00:00",
        exit_reason="take_profit",
    )


# ---------------------------------------------------------------------------
# SignalFileLogger tests
# ---------------------------------------------------------------------------


class TestSignalFileLogger:
    def test_log_and_read(self, tmp_path: Path) -> None:
        logger = SignalFileLogger(tmp_path / "signals")
        entry = _make_entry()
        logger.log(entry)

        dt = datetime(2026, 3, 20, tzinfo=UTC)
        entries = logger.read_entries(dt)
        assert len(entries) == 1
        assert entries[0].symbol == "BTC/USD"
        assert entries[0].score == 75.0

    def test_multiple_entries(self, tmp_path: Path) -> None:
        logger = SignalFileLogger(tmp_path / "signals")
        logger.log(_make_entry(symbol="BTC/USD"))
        logger.log(_make_entry(symbol="ETH/USD"))
        logger.log(_make_entry(symbol="SOL/USD"))

        dt = datetime(2026, 3, 20, tzinfo=UTC)
        entries = logger.read_entries(dt)
        assert len(entries) == 3

    def test_read_empty_date(self, tmp_path: Path) -> None:
        logger = SignalFileLogger(tmp_path / "signals")
        dt = datetime(2026, 1, 1, tzinfo=UTC)
        entries = logger.read_entries(dt)
        assert entries == []

    def test_date_partitioning(self, tmp_path: Path) -> None:
        logger = SignalFileLogger(tmp_path / "signals")
        logger.log(_make_entry(timestamp="2026-03-20T12:00:00+00:00"))
        logger.log(_make_entry(timestamp="2026-03-21T12:00:00+00:00"))

        day1 = logger.read_entries(datetime(2026, 3, 20, tzinfo=UTC))
        day2 = logger.read_entries(datetime(2026, 3, 21, tzinfo=UTC))
        assert len(day1) == 1
        assert len(day2) == 1

    def test_creates_directory(self, tmp_path: Path) -> None:
        logger = SignalFileLogger(tmp_path / "deep" / "nested" / "signals")
        logger.log(_make_entry())
        assert (tmp_path / "deep" / "nested" / "signals").exists()


# ---------------------------------------------------------------------------
# Daily report tests
# ---------------------------------------------------------------------------


class TestDailyReport:
    def test_empty_entries(self) -> None:
        dt = datetime(2026, 3, 20, tzinfo=UTC)
        summary = build_daily_summary([], dt)
        assert summary.total_signals == 0
        assert summary.avg_score == 0.0

    def test_summary_counts(self) -> None:
        entries = [
            _make_entry(symbol="BTC/USD", direction="long", regime="tight_smc"),
            _make_entry(symbol="ETH/USD", direction="short", regime="wide_structure"),
            _make_entry(symbol="BTC/USD", direction="long", regime="tight_smc"),
        ]
        dt = datetime(2026, 3, 20, tzinfo=UTC)
        summary = build_daily_summary(entries, dt)

        assert summary.total_signals == 3
        assert summary.by_direction == {"long": 2, "short": 1}
        assert summary.by_regime == {"tight_smc": 2, "wide_structure": 1}
        assert summary.by_symbol == {"BTC/USD": 2, "ETH/USD": 1}

    def test_avg_score(self) -> None:
        entries = [
            _make_entry(score=60.0),
            _make_entry(score=80.0),
            _make_entry(score=100.0),
        ]
        dt = datetime(2026, 3, 20, tzinfo=UTC)
        summary = build_daily_summary(entries, dt)
        assert summary.avg_score == 80.0

    def test_score_distribution(self) -> None:
        entries = [
            _make_entry(score=90.0),
            _make_entry(score=70.0),
            _make_entry(score=30.0),
        ]
        dt = datetime(2026, 3, 20, tzinfo=UTC)
        summary = build_daily_summary(entries, dt)
        assert "A (80-100)" in summary.score_distribution
        assert "B (60-79)" in summary.score_distribution
        assert "D (20-39)" in summary.score_distribution

    def test_markdown_output(self) -> None:
        entries = [_make_entry()]
        dt = datetime(2026, 3, 20, tzinfo=UTC)
        summary = build_daily_summary(entries, dt)
        md = summary_to_markdown(summary)
        assert "# Daily Signal Report" in md
        assert "2026-03-20" in md
        assert "Total Signals" in md

    def test_json_output(self) -> None:
        entries = [_make_entry()]
        dt = datetime(2026, 3, 20, tzinfo=UTC)
        summary = build_daily_summary(entries, dt)
        j = summary_to_json(summary)
        import json

        data = json.loads(j)
        assert data["total_signals"] == 1
        assert data["date"] == "2026-03-20"

    def test_generate_daily_report_files(self, tmp_path: Path) -> None:
        signal_dir = tmp_path / "signals"
        logger = SignalFileLogger(signal_dir)
        logger.log(_make_entry())
        logger.log(_make_entry(symbol="ETH/USD"))

        dt = datetime(2026, 3, 20, tzinfo=UTC)
        report_dir = tmp_path / "reports"
        md_path, json_path = generate_daily_report(logger, dt, report_dir)

        assert md_path.exists()
        assert json_path.exists()
        assert "ETH/USD" in md_path.read_text()


# ---------------------------------------------------------------------------
# Attribution tests
# ---------------------------------------------------------------------------


class TestAttributionBreakdown:
    def test_add_trades(self) -> None:
        bd = AttributionBreakdown()
        bd.add_trade(100.0, 1000.0)
        bd.add_trade(-50.0, 1000.0)
        bd.add_trade(75.0, 500.0)

        assert bd.trade_count == 3
        assert bd.win_count == 2
        assert bd.loss_count == 1
        assert bd.total_pnl == 125.0
        assert bd.win_rate == pytest.approx(2 / 3)

    def test_empty_win_rate(self) -> None:
        bd = AttributionBreakdown()
        assert bd.win_rate == 0.0


class TestSignalAttributionTracker:
    def test_record_and_load(self, tmp_path: Path) -> None:
        tracker = SignalAttributionTracker(tmp_path / "attr")
        trade = _make_trade()
        tracker.record_trade(trade)

        trades = tracker.load_trades()
        assert len(trades) == 1
        assert trades[0].trade_id == "t1"
        assert trades[0].realized_pnl == 100.0

    def test_build_report(self, tmp_path: Path) -> None:
        tracker = SignalAttributionTracker(tmp_path / "attr")
        tracker.record_trade(_make_trade(trade_id="t1", realized_pnl=100.0))
        tracker.record_trade(
            _make_trade(
                trade_id="t2",
                realized_pnl=-50.0,
                strategy_source="mean_rev_v1",
                regime="wide_structure",
            )
        )
        tracker.record_trade(_make_trade(trade_id="t3", realized_pnl=200.0, symbol="ETH/USD"))

        trades = tracker.load_trades()
        start = datetime(2026, 3, 20, tzinfo=UTC)
        end = datetime(2026, 3, 21, tzinfo=UTC)
        report = tracker.build_report(trades, start, end)

        assert report.total.total_pnl == 250.0
        assert report.total.trade_count == 3
        assert len(report.by_strategy) == 2
        assert "smc_v1" in report.by_strategy
        assert "mean_rev_v1" in report.by_strategy
        assert len(report.by_symbol) == 2

    def test_report_markdown(self, tmp_path: Path) -> None:
        tracker = SignalAttributionTracker(tmp_path / "attr")
        tracker.record_trade(_make_trade())

        trades = tracker.load_trades()
        start = datetime(2026, 3, 20, tzinfo=UTC)
        end = datetime(2026, 3, 21, tzinfo=UTC)
        report = tracker.build_report(trades, start, end)
        md = tracker.report_to_markdown(report)

        assert "Performance Attribution Report" in md
        assert "BTC/USD" in md
        assert "smc_v1" in md

    def test_report_json(self, tmp_path: Path) -> None:
        tracker = SignalAttributionTracker(tmp_path / "attr")
        tracker.record_trade(_make_trade())

        trades = tracker.load_trades()
        start = datetime(2026, 3, 20, tzinfo=UTC)
        end = datetime(2026, 3, 21, tzinfo=UTC)
        report = tracker.build_report(trades, start, end)
        j = tracker.report_to_json(report)

        import json

        data = json.loads(j)
        assert data["total"]["total_pnl"] == 100.0
        assert data["total"]["win_rate"] == 1.0

    def test_time_filtered_load(self, tmp_path: Path) -> None:
        tracker = SignalAttributionTracker(tmp_path / "attr")
        tracker.record_trade(_make_trade(trade_id="t1"))

        since = datetime(2026, 3, 21, tzinfo=UTC)
        trades = tracker.load_trades(since=since)
        assert len(trades) == 0

    def test_empty_load(self, tmp_path: Path) -> None:
        tracker = SignalAttributionTracker(tmp_path / "attr")
        trades = tracker.load_trades()
        assert trades == []


# ---------------------------------------------------------------------------
# HitRateStats tests
# ---------------------------------------------------------------------------


class TestHitRateStats:
    def test_hit_rate_calculation(self) -> None:
        stats = HitRateStats(total_signals=10, hits=7, misses=3, pending=0)
        assert stats.hit_rate == 0.7

    def test_hit_rate_with_pending(self) -> None:
        stats = HitRateStats(total_signals=10, hits=5, misses=2, pending=3)
        assert stats.hit_rate == pytest.approx(5 / 7)

    def test_hit_rate_no_resolved(self) -> None:
        stats = HitRateStats(total_signals=5, hits=0, misses=0, pending=5)
        assert stats.hit_rate == 0.0
