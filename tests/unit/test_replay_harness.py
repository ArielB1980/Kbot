"""
Unit tests for the replay harness components.

Tests: SimClock, ReplayDataStore, ReplayKrakenClient (exchange sim),
FaultInjector, ReplayMetrics.
"""

import csv
import pytest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock

from src.backtest.replay_harness.sim_clock import SimClock
from src.backtest.replay_harness.data_store import ReplayDataStore, LiquidityParams, CandleBar
from src.backtest.replay_harness.exchange_sim import (
    ReplayKrakenClient, ExchangeSimConfig, SimOrder, OrderType, OrderStatus,
)
from src.backtest.replay_harness.fault_injector import FaultInjector, FaultSpec
from src.backtest.replay_harness.metrics import ReplayMetrics
from src.exceptions import OperationalError, RateLimitError, DataError


T0 = datetime(2025, 6, 1, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# SimClock
# ---------------------------------------------------------------------------

class TestSimClock:
    def test_initial_time(self):
        c = SimClock(start=T0)
        assert c.now() == T0

    def test_advance_seconds(self):
        c = SimClock(start=T0)
        c.advance(seconds=60)
        assert c.now() == T0 + timedelta(seconds=60)

    def test_advance_minutes(self):
        c = SimClock(start=T0)
        c.advance(minutes=5)
        assert c.now() == T0 + timedelta(minutes=5)

    def test_advance_to_specific_time(self):
        c = SimClock(start=T0)
        target = T0 + timedelta(hours=1)
        c.advance(to=target)
        assert c.now() == target

    def test_cannot_advance_backwards(self):
        c = SimClock(start=T0)
        c.advance(seconds=60)
        with pytest.raises(ValueError, match="Cannot advance backwards"):
            c.advance(to=T0)

    def test_time_returns_unix_timestamp(self):
        c = SimClock(start=T0)
        assert abs(c.time() - T0.timestamp()) < 0.001

    def test_elapsed(self):
        c = SimClock(start=T0)
        c.advance(seconds=300)
        assert c.elapsed == timedelta(seconds=300)

    @pytest.mark.asyncio
    async def test_sleep_is_noop(self):
        c = SimClock(start=T0)
        await c.sleep(60)
        # Time doesn't advance automatically
        assert c.now() == T0
        assert c.stats["total_sleeps"] == 1

    @pytest.mark.asyncio
    async def test_sleep_with_callback(self):
        advances = []
        def cb(clock, secs):
            advances.append(secs)
            clock.advance(seconds=secs)

        c = SimClock(start=T0, step_callback=cb)
        await c.sleep(30)
        assert c.now() == T0 + timedelta(seconds=30)
        assert advances == [30]

    def test_requires_timezone_aware(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            SimClock(start=datetime(2025, 1, 1))


# ---------------------------------------------------------------------------
# FaultInjector
# ---------------------------------------------------------------------------

class TestFaultInjector:
    def test_no_fault_outside_window(self):
        fi = FaultInjector([
            FaultSpec(
                start=T0 + timedelta(hours=1),
                end=T0 + timedelta(hours=2),
                fault_type="timeout",
            ),
        ])
        # Before window — no fault
        fi.maybe_inject("place_futures_order", T0)

    def test_fault_inside_window(self):
        fi = FaultInjector([
            FaultSpec(
                start=T0,
                end=T0 + timedelta(minutes=5),
                fault_type="timeout",
            ),
        ])
        with pytest.raises(OperationalError, match="INJECTED"):
            fi.maybe_inject("place_futures_order", T0 + timedelta(minutes=2))

    def test_rate_limit_fault(self):
        fi = FaultInjector([
            FaultSpec(
                start=T0,
                end=T0 + timedelta(minutes=1),
                fault_type="rate_limit",
            ),
        ])
        with pytest.raises(RateLimitError, match="INJECTED"):
            fi.maybe_inject("get_positions", T0)

    def test_method_filter(self):
        fi = FaultInjector([
            FaultSpec(
                start=T0,
                end=T0 + timedelta(minutes=5),
                fault_type="timeout",
                affected_methods=["place_futures_order"],
            ),
        ])
        # Non-matching method — no fault
        fi.maybe_inject("get_spot_ticker", T0)
        # Matching method — fault
        with pytest.raises(OperationalError):
            fi.maybe_inject("place_futures_order", T0)

    def test_stats_tracking(self):
        fi = FaultInjector([
            FaultSpec(start=T0, end=T0 + timedelta(hours=1), fault_type="timeout"),
        ])
        for _ in range(3):
            try:
                fi.maybe_inject("test", T0)
            except OperationalError:
                pass
        assert fi.stats["total_injections"] == 3


# ---------------------------------------------------------------------------
# ReplayDataStore
# ---------------------------------------------------------------------------

class TestReplayDataStore:
    @pytest.fixture
    def data_dir(self, tmp_path):
        """Create a temp data dir with synthetic candle CSV."""
        candle_dir = tmp_path / "candles"
        candle_dir.mkdir()
        # Write BTC candles
        path = candle_dir / "BTC_USD_USD_1m.csv"
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
            writer.writeheader()
            for i in range(60):
                ts = T0 + timedelta(minutes=i)
                price = 50000 + i * 10
                writer.writerow({
                    "timestamp": ts.isoformat(),
                    "open": price,
                    "high": price + 50,
                    "low": price - 30,
                    "close": price + 20,
                    "volume": 100000,
                })
        return tmp_path

    def test_load_and_query(self, data_dir):
        ds = ReplayDataStore(data_dir, symbols=["BTC/USD:USD"])
        ds.load()

        bars = ds.get_candles_up_to("BTC/USD:USD", "1m", T0 + timedelta(minutes=30))
        assert len(bars) == 31  # 0..30 inclusive

    def test_get_candle_at(self, data_dir):
        ds = ReplayDataStore(data_dir, symbols=["BTC/USD:USD"])
        ds.load()

        bar = ds.get_candle_at("BTC/USD:USD", "1m", T0 + timedelta(minutes=5))
        assert bar is not None
        assert bar.timestamp == T0 + timedelta(minutes=5)

    def test_derived_liquidity(self, data_dir):
        ds = ReplayDataStore(data_dir, symbols=["BTC/USD:USD"])
        ds.load()

        liq = ds.get_liquidity_at("BTC/USD:USD", T0 + timedelta(minutes=30))
        assert isinstance(liq, LiquidityParams)
        assert liq.spread_bps > 0

    def test_missing_symbol_returns_empty(self, data_dir):
        ds = ReplayDataStore(data_dir, symbols=["DOGE/USD:USD"])
        ds.load()
        bars = ds.get_candles_up_to("DOGE/USD:USD", "1m", T0)
        assert bars == []


# ---------------------------------------------------------------------------
# Exchange Sim
# ---------------------------------------------------------------------------

class TestExchangeSim:
    @pytest.fixture
    def exchange(self, tmp_path):
        """Create exchange with BTC candles."""
        candle_dir = tmp_path / "candles"
        candle_dir.mkdir()
        path = candle_dir / "BTC_USD_USD_1m.csv"
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
            writer.writeheader()
            for i in range(120):
                ts = T0 + timedelta(minutes=i)
                price = 50000 + i * 10
                writer.writerow({
                    "timestamp": ts.isoformat(),
                    "open": price,
                    "high": price + 100,
                    "low": price - 80,
                    "close": price + 20,
                    "volume": 500000,
                })

        clock = SimClock(start=T0)
        ds = ReplayDataStore(tmp_path, symbols=["BTC/USD:USD"])
        ds.load()

        ex = ReplayKrakenClient(
            clock=clock,
            data_store=ds,
            config=ExchangeSimConfig(initial_equity_usd=Decimal("10000")),
        )
        return ex, clock

    @pytest.mark.asyncio
    async def test_market_order_fills_immediately(self, exchange):
        ex, clock = exchange
        result = await ex.place_futures_order(
            symbol="BTC/USD:USD", side="buy", order_type="market",
            size=Decimal("0.1"),
        )
        assert result["status"] == "filled"

    @pytest.mark.asyncio
    async def test_fetch_order_includes_deterministic_trade_ids(self, exchange):
        ex, clock = exchange
        result = await ex.place_futures_order(
            symbol="BTC/USD:USD", side="buy", order_type="market",
            size=Decimal("0.1"),
            client_order_id="entry-1",
        )

        fetched = await ex.fetch_order(result["id"], "BTC/USD:USD")

        assert fetched is not None
        assert len(fetched["trades"]) == 1
        assert fetched["trades"][0]["id"] == f'{result["id"]}-fill-1'
        assert fetched["trades"][0]["order"] == result["id"]
        assert result["filled"] > 0

    @pytest.mark.asyncio
    async def test_position_created_after_fill(self, exchange):
        ex, clock = exchange
        await ex.place_futures_order(
            symbol="BTC/USD:USD", side="buy", order_type="market",
            size=Decimal("0.1"),
        )
        positions = await ex.get_all_futures_positions()
        assert len(positions) == 1
        assert positions[0]["side"] == "long"

    @pytest.mark.asyncio
    async def test_stop_order_triggers_and_fills(self, exchange):
        ex, clock = exchange
        # Open long position
        await ex.place_futures_order(
            symbol="BTC/USD:USD", side="buy", order_type="market",
            size=Decimal("0.1"),
        )
        # Place stop below current price
        await ex.place_futures_order(
            symbol="BTC/USD:USD", side="sell", order_type="stop",
            size=Decimal("0.1"), stop_price=Decimal("49000"),
            reduce_only=True,
        )
        # Current price is ~50000+, stop at 49000 shouldn't trigger yet
        open_orders = await ex.get_futures_open_orders()
        assert len(open_orders) == 1
        assert open_orders[0]["type"] == "stop"

    @pytest.mark.asyncio
    async def test_cancel_order(self, exchange):
        ex, clock = exchange
        result = await ex.place_futures_order(
            symbol="BTC/USD:USD", side="buy", order_type="limit",
            size=Decimal("0.1"), price=Decimal("40000"),
        )
        await ex.cancel_futures_order(result["id"])
        orders = await ex.get_futures_open_orders()
        assert len(orders) == 0

    @pytest.mark.asyncio
    async def test_account_info(self, exchange):
        ex, clock = exchange
        info = await ex.get_futures_account_info()
        assert info["equity"] == 10000.0
        assert info["availableMargin"] == 10000.0

    @pytest.mark.asyncio
    async def test_tickers_return_data(self, exchange):
        ex, clock = exchange
        tickers = await ex.get_futures_tickers_bulk()
        assert "BTC/USD:USD" in tickers
        assert tickers["BTC/USD:USD"] > 0

    @pytest.mark.asyncio
    async def test_ohlcv_returns_candles(self, exchange):
        ex, clock = exchange
        candles = await ex.get_spot_ohlcv("BTC/USD:USD", "1m")
        assert len(candles) > 0
        assert candles[0].symbol == "BTC/USD:USD"

    @pytest.mark.asyncio
    async def test_exchange_metrics(self, exchange):
        ex, clock = exchange
        m = ex.exchange_metrics
        assert m["equity"] == 10000.0
        assert m["orders_placed"] == 0

    @pytest.mark.asyncio
    async def test_close_position(self, exchange):
        ex, clock = exchange
        await ex.place_futures_order(
            symbol="BTC/USD:USD", side="buy", order_type="market",
            size=Decimal("0.1"),
        )
        assert len(await ex.get_all_futures_positions()) == 1
        await ex.close_position("BTC/USD:USD")
        assert len(await ex.get_all_futures_positions()) == 0


# ---------------------------------------------------------------------------
# Fix 1: Maker/taker based on mid-crossing
# ---------------------------------------------------------------------------

class TestMakerTakerMidCrossing:
    """Limit orders that cross the mid at placement → taker.
    Limit orders that rest below/above mid → maker when filled later."""

    @pytest.fixture
    def exchange(self, tmp_path):
        candle_dir = tmp_path / "candles"
        candle_dir.mkdir()
        path = candle_dir / "BTC_USD_USD_1m.csv"
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
            writer.writeheader()
            for i in range(60):
                ts = T0 + timedelta(minutes=i)
                # Stable price around 50000
                writer.writerow({
                    "timestamp": ts.isoformat(),
                    "open": 50000, "high": 50200, "low": 49800, "close": 50000,
                    "volume": 500000,
                })
        clock = SimClock(start=T0)
        ds = ReplayDataStore(tmp_path, symbols=["BTC/USD:USD"])
        ds.load()
        ex = ReplayKrakenClient(
            clock=clock, data_store=ds,
            config=ExchangeSimConfig(initial_equity_usd=Decimal("100000")),
        )
        return ex, clock

    @pytest.mark.asyncio
    async def test_buy_limit_below_mid_is_maker(self, exchange):
        """Buy limit at 49900 when mid is ~50000 → rests → maker."""
        ex, clock = exchange
        result = await ex.place_futures_order(
            symbol="BTC/USD:USD", side="buy", order_type="limit",
            size=Decimal("0.1"), price=Decimal("49900"),
        )
        # Advance clock so the fill processes
        clock.advance(seconds=60)
        fills = ex.step()
        assert len(fills) == 1
        assert fills[0].is_maker is True  # rested below mid

    @pytest.mark.asyncio
    async def test_buy_limit_above_mid_is_taker(self, exchange):
        """Buy limit at 50100 when mid is ~50000 → crosses spread → taker."""
        ex, clock = exchange
        result = await ex.place_futures_order(
            symbol="BTC/USD:USD", side="buy", order_type="limit",
            size=Decimal("0.1"), price=Decimal("50100"),
        )
        clock.advance(seconds=60)
        fills = ex.step()
        assert len(fills) == 1
        assert fills[0].is_maker is False  # crossed mid

    @pytest.mark.asyncio
    async def test_sell_limit_above_mid_is_maker(self, exchange):
        """Sell limit at 50100 when mid is ~50000 → rests → maker."""
        ex, clock = exchange
        # Need a position to sell
        await ex.place_futures_order(
            symbol="BTC/USD:USD", side="buy", order_type="market",
            size=Decimal("0.2"),
        )
        result = await ex.place_futures_order(
            symbol="BTC/USD:USD", side="sell", order_type="limit",
            size=Decimal("0.1"), price=Decimal("50100"),
        )
        clock.advance(seconds=60)
        fills = ex.step()
        assert len(fills) == 1
        assert fills[0].is_maker is True


# ---------------------------------------------------------------------------
# Fix 2: Entered_book delay is vol/depth dependent
# ---------------------------------------------------------------------------

class TestEnteredBookDelay:
    def test_low_vol_deep_book_fast_fill(self):
        """Low vol + deep book → near-instant fill (with jitter disabled for exact test)."""
        from src.backtest.replay_harness.exchange_sim import ReplayKrakenClient
        from src.backtest.replay_harness.data_store import LiquidityParams

        liq = LiquidityParams(spread_bps=3.0, depth_usd_at_1bp=100000, volatility_regime="low")
        clock = SimClock(start=T0)
        ds = ReplayDataStore(Path("/tmp"), symbols=[])
        ex = ReplayKrakenClient(clock=clock, data_store=ds,
                                config=ExchangeSimConfig(jitter_enabled=False))
        delay = ex._compute_entered_book_delay(liq)
        # base=1.0, low vol=0.2, deep=0.5 → max(0.2, 0.5) * 1.0 = 0.5
        assert delay == pytest.approx(0.5, abs=0.01)

    def test_extreme_vol_thin_book_slow_fill(self):
        from src.backtest.replay_harness.exchange_sim import ReplayKrakenClient

        liq = LiquidityParams(spread_bps=25.0, depth_usd_at_1bp=3000, volatility_regime="extreme")
        clock = SimClock(start=T0)
        ds = ReplayDataStore(Path("/tmp"), symbols=[])
        ex = ReplayKrakenClient(
            clock=clock, data_store=ds,
            config=ExchangeSimConfig(stop_entered_book_delay_base_seconds=2.0, jitter_enabled=False),
        )
        delay = ex._compute_entered_book_delay(liq)
        # base=2.0, extreme=8.0, thin=4.0 → max(8.0, 4.0) * 2.0 = 16.0
        assert delay == pytest.approx(16.0, abs=0.01)

    def test_jitter_varies_delay(self):
        """With jitter enabled, same inputs give different results across seeds."""
        from src.backtest.replay_harness.exchange_sim import ReplayKrakenClient

        liq = LiquidityParams(spread_bps=5.0, depth_usd_at_1bp=50000, volatility_regime="normal")
        delays = set()
        for seed in range(5):
            clock = SimClock(start=T0)
            ds = ReplayDataStore(Path("/tmp"), symbols=[])
            ex = ReplayKrakenClient(
                clock=clock, data_store=ds,
                config=ExchangeSimConfig(jitter_enabled=True, jitter_seed=seed),
            )
            delays.add(round(ex._compute_entered_book_delay(liq), 4))
        # Different seeds should produce at least 2 different delays
        assert len(delays) >= 2


# ---------------------------------------------------------------------------
# Fix 3: Order rejections
# ---------------------------------------------------------------------------

class TestOrderRejections:
    @pytest.fixture
    def exchange(self, tmp_path):
        candle_dir = tmp_path / "candles"
        candle_dir.mkdir()
        path = candle_dir / "BTC_USD_USD_1m.csv"
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
            writer.writeheader()
            for i in range(10):
                ts = T0 + timedelta(minutes=i)
                writer.writerow({
                    "timestamp": ts.isoformat(),
                    "open": 50000, "high": 50200, "low": 49800, "close": 50000,
                    "volume": 500000,
                })
        clock = SimClock(start=T0)
        ds = ReplayDataStore(tmp_path, symbols=["BTC/USD:USD"])
        ds.load()
        ex = ReplayKrakenClient(
            clock=clock, data_store=ds,
            config=ExchangeSimConfig(
                initial_equity_usd=Decimal("10000"),
                min_order_size_usd=10.0,
                reject_reduce_only_conflicts=True,
                reject_insufficient_margin=True,
            ),
        )
        return ex, clock

    @pytest.mark.asyncio
    async def test_reject_reduce_only_no_position(self, exchange):
        """reduceOnly with no open position → rejected."""
        ex, clock = exchange
        with pytest.raises(DataError, match="reduceOnly but no open position"):
            await ex.place_futures_order(
                symbol="BTC/USD:USD", side="sell", order_type="market",
                size=Decimal("0.1"), reduce_only=True,
            )
        assert ex._metrics["reduce_only_rejections"] == 1

    @pytest.mark.asyncio
    async def test_reject_reduce_only_same_direction(self, exchange):
        """reduceOnly buy on a long position → increases exposure → rejected."""
        ex, clock = exchange
        await ex.place_futures_order(
            symbol="BTC/USD:USD", side="buy", order_type="market",
            size=Decimal("0.01"),
        )
        with pytest.raises(DataError, match="would increase"):
            await ex.place_futures_order(
                symbol="BTC/USD:USD", side="buy", order_type="market",
                size=Decimal("0.01"), reduce_only=True,
            )

    @pytest.mark.asyncio
    async def test_reject_insufficient_margin(self, exchange):
        """Order requiring more margin than available → rejected."""
        ex, clock = exchange
        # 10 BTC * 50000 = $500k notional / 7 leverage = ~$71k margin
        # Equity is only $10k
        with pytest.raises(DataError, match="insufficient margin"):
            await ex.place_futures_order(
                symbol="BTC/USD:USD", side="buy", order_type="market",
                size=Decimal("10"),
            )

    @pytest.mark.asyncio
    async def test_min_size_rejection(self, exchange):
        """Order below min notional → rejected."""
        ex, clock = exchange
        # 0.0001 BTC * 50000 = $5, min is $10
        with pytest.raises(DataError, match="below min"):
            await ex.place_futures_order(
                symbol="BTC/USD:USD", side="buy", order_type="market",
                size=Decimal("0.0001"),
            )


# ---------------------------------------------------------------------------
# Fix 4: reduceOnly caps at flat, cannot reverse
# ---------------------------------------------------------------------------

class TestReduceOnlySemantics:
    @pytest.fixture
    def exchange(self, tmp_path):
        candle_dir = tmp_path / "candles"
        candle_dir.mkdir()
        path = candle_dir / "BTC_USD_USD_1m.csv"
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
            writer.writeheader()
            for i in range(10):
                ts = T0 + timedelta(minutes=i)
                writer.writerow({
                    "timestamp": ts.isoformat(),
                    "open": 50000, "high": 50200, "low": 49800, "close": 50000,
                    "volume": 500000,
                })
        clock = SimClock(start=T0)
        ds = ReplayDataStore(tmp_path, symbols=["BTC/USD:USD"])
        ds.load()
        ex = ReplayKrakenClient(
            clock=clock, data_store=ds,
            config=ExchangeSimConfig(
                initial_equity_usd=Decimal("100000"),
                reject_reduce_only_conflicts=False,  # disable pre-flight check to test fill logic
            ),
        )
        return ex, clock

    @pytest.mark.asyncio
    async def test_reduce_only_caps_at_flat(self, exchange):
        """reduceOnly sell 0.2 on a 0.1 long → close at flat, no reversal."""
        ex, clock = exchange
        # Open long 0.1
        await ex.place_futures_order(
            symbol="BTC/USD:USD", side="buy", order_type="market",
            size=Decimal("0.1"),
        )
        # reduceOnly sell 0.2 (more than position)
        await ex.place_futures_order(
            symbol="BTC/USD:USD", side="sell", order_type="market",
            size=Decimal("0.2"), reduce_only=True,
        )
        # Should be flat, NOT reversed into a short
        positions = await ex.get_all_futures_positions()
        assert len(positions) == 0

    @pytest.mark.asyncio
    async def test_reduce_only_market_fill_reports_capped_size(self, exchange):
        """Oversized reduceOnly market fills should report the executable size, not the requested size."""
        ex, clock = exchange
        await ex.place_futures_order(
            symbol="BTC/USD:USD", side="buy", order_type="market",
            size=Decimal("0.1"),
        )
        result = await ex.place_futures_order(
            symbol="BTC/USD:USD", side="sell", order_type="market",
            size=Decimal("0.2"), reduce_only=True,
        )

        fetched = await ex.fetch_order(result["id"], "BTC/USD:USD")

        assert fetched is not None
        assert fetched["status"] == "filled"
        assert Decimal(str(fetched["amount"])) == Decimal("0.1")
        assert Decimal(str(fetched["filled"])) == Decimal("0.1")
        assert Decimal(str(fetched["remaining"])) == Decimal("0")
        assert Decimal(str(fetched["trades"][0]["amount"])) == Decimal("0.1")

    @pytest.mark.asyncio
    async def test_reduce_only_limit_fill_reports_capped_size(self, exchange):
        """Oversized reduceOnly limit fills should also be clipped in the reported order payload."""
        ex, clock = exchange
        await ex.place_futures_order(
            symbol="BTC/USD:USD", side="buy", order_type="market",
            size=Decimal("0.1"),
        )
        result = await ex.place_futures_order(
            symbol="BTC/USD:USD", side="sell", order_type="limit",
            size=Decimal("0.2"), price=Decimal("49900"), reduce_only=True,
        )

        ex.step(clock.now())
        fetched = await ex.fetch_order(result["id"], "BTC/USD:USD")

        assert fetched is not None
        assert fetched["status"] == "filled"
        assert Decimal(str(fetched["amount"])) == Decimal("0.1")
        assert Decimal(str(fetched["filled"])) == Decimal("0.1")
        assert Decimal(str(fetched["remaining"])) == Decimal("0")
        assert Decimal(str(fetched["trades"][0]["amount"])) == Decimal("0.1")

    @pytest.mark.asyncio
    async def test_non_reduce_can_reverse(self, exchange):
        """Non-reduceOnly sell 0.2 on a 0.1 long → reverses into 0.1 short."""
        ex, clock = exchange
        await ex.place_futures_order(
            symbol="BTC/USD:USD", side="buy", order_type="market",
            size=Decimal("0.1"),
        )
        await ex.place_futures_order(
            symbol="BTC/USD:USD", side="sell", order_type="market",
            size=Decimal("0.2"),
        )
        positions = await ex.get_all_futures_positions()
        assert len(positions) == 1
        assert positions[0]["side"] == "short"
        assert positions[0]["contracts"] == pytest.approx(0.1, abs=0.001)


# ---------------------------------------------------------------------------
# Fix 6: fetch_open_orders hides entered_book
# ---------------------------------------------------------------------------

class TestEnteredBookVisibility:
    @pytest.fixture
    def exchange(self, tmp_path):
        candle_dir = tmp_path / "candles"
        candle_dir.mkdir()
        path = candle_dir / "BTC_USD_USD_1m.csv"
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
            writer.writeheader()
            for i in range(60):
                ts = T0 + timedelta(minutes=i)
                # Price drops from 50000 to 49000 over 60 mins
                price = 50000 - i * 20
                writer.writerow({
                    "timestamp": ts.isoformat(),
                    "open": price + 20, "high": price + 50, "low": price - 50,
                    "close": price, "volume": 500000,
                })
        clock = SimClock(start=T0)
        ds = ReplayDataStore(tmp_path, symbols=["BTC/USD:USD"])
        ds.load()
        # Enable the visibility quirk
        ex = ReplayKrakenClient(
            clock=clock, data_store=ds,
            config=ExchangeSimConfig(
                initial_equity_usd=Decimal("100000"),
                hide_entered_book_from_open_orders=True,
                stop_entered_book_delay_base_seconds=120.0,  # long delay so it stays entered_book
            ),
        )
        return ex, clock

    @pytest.mark.asyncio
    async def test_entered_book_hidden_from_open_orders(self, exchange):
        """entered_book stops are invisible to get_futures_open_orders but visible to fetch_order."""
        ex, clock = exchange
        # Open long position
        await ex.place_futures_order(
            symbol="BTC/USD:USD", side="buy", order_type="market",
            size=Decimal("0.1"),
        )
        # Place stop that will trigger when price drops
        stop_result = await ex.place_futures_order(
            symbol="BTC/USD:USD", side="sell", order_type="stop",
            size=Decimal("0.1"), stop_price=Decimal("49950"),
            reduce_only=True,
        )
        stop_id = stop_result["id"]

        # Advance past trigger point (price drops quickly)
        clock.advance(minutes=5)
        ex.step()

        # The stop should now be entered_book
        order = ex._orders[stop_id]
        assert order.status == OrderStatus.ENTERED_BOOK

        # Layer 1: get_futures_open_orders misses it
        open_orders = await ex.get_futures_open_orders()
        assert len(open_orders) == 0, "entered_book should be hidden from open_orders"

        # Layer 2: fetch_order by ID still shows it
        fetched = await ex.fetch_order(stop_id, "BTC/USD:USD")
        assert fetched is not None
        assert fetched["status"] == "entered_book"


# ---------------------------------------------------------------------------
# ReplayMetrics
# ---------------------------------------------------------------------------

class TestReplayMetrics:
    def test_record_trade_and_stats(self):
        m = ReplayMetrics()
        m.record_trade({"pnl": 100, "symbol": "BTC"})
        m.record_trade({"pnl": -50, "symbol": "ETH"})
        m.record_trade({"pnl": 75, "symbol": "BTC"})

        assert m.total_trades == 3
        assert m.winning_trades == 2
        assert m.losing_trades == 1
        assert m.win_rate == pytest.approx(2 / 3, abs=0.01)
        assert m.profit_factor == pytest.approx(175 / 50, abs=0.01)

    def test_equity_tracking(self):
        m = ReplayMetrics()
        m.peak_equity = Decimal("10000")
        m.record_equity(T0, Decimal("10500"), Decimal("0"), Decimal("0"), 0)
        assert m.peak_equity == Decimal("10500")

        m.record_equity(T0, Decimal("9500"), Decimal("0"), Decimal("0"), 0)
        assert m.max_drawdown_usd == Decimal("1000")
        assert m.max_drawdown_pct == pytest.approx(9.52, abs=0.1)

    def test_summary_format(self):
        m = ReplayMetrics()
        s = m.summary()
        assert "safety" in s
        assert "trading" in s
        assert "execution" in s
        assert "system" in s

    def test_save_and_load(self, tmp_path):
        m = ReplayMetrics()
        m.record_trade({"pnl": 100})
        m.total_ticks = 60
        path = tmp_path / "metrics.json"
        m.save(path)
        assert path.exists()
        import json
        data = json.loads(path.read_text())
        assert data["trading"]["total_trades"] == 1


# ---------------------------------------------------------------------------
# Funding curves
# ---------------------------------------------------------------------------

class TestFundingCurves:
    @pytest.fixture
    def exchange(self, tmp_path):
        candle_dir = tmp_path / "candles"
        candle_dir.mkdir()
        path = candle_dir / "BTC_USD_USD_1m.csv"
        # Write 10h of candles — low vol (narrow range) to keep base funding rate
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
            writer.writeheader()
            for i in range(600):
                ts = T0 + timedelta(minutes=i)
                writer.writerow({
                    "timestamp": ts.isoformat(),
                    "open": 50000, "high": 50050, "low": 49950, "close": 50000,
                    "volume": 500000,
                })

        from src.backtest.replay_harness.exchange_sim import FundingCurve
        clock = SimClock(start=T0)
        ds = ReplayDataStore(tmp_path, symbols=["BTC/USD:USD"])
        ds.load()
        ex = ReplayKrakenClient(
            clock=clock, data_store=ds,
            config=ExchangeSimConfig(
                initial_equity_usd=Decimal("100000"),
                jitter_enabled=False,
                funding_curves={
                    "BTC/USD:USD": FundingCurve(base_rate_8h_bps=2.0, vol_spike_multiplier=5.0),
                },
            ),
        )
        return ex, clock

    @pytest.mark.asyncio
    async def test_per_symbol_funding_applied(self, exchange):
        """Funding uses per-symbol curve rate, not the flat default rate."""
        ex, clock = exchange
        await ex.place_futures_order(
            symbol="BTC/USD:USD", side="buy", order_type="market",
            size=Decimal("1.0"),
        )
        # First step sets _last_funding_time baseline
        ex.step(clock.now())
        assert ex._total_funding == 0  # no funding yet

        # Advance past 8h funding window and step again
        clock.advance(minutes=540)
        ex.step(clock.now())
        assert ex._total_funding > 0
        assert ex._metrics["funding_events"] == 1
        # Verify the funding log records the per-symbol rate (2.0 bps)
        assert len(ex._funding_log) == 1
        assert ex._funding_log[0]["rate_bps"] == 2.0
        assert ex._funding_log[0]["symbol"] == "BTC/USD:USD"


# ---------------------------------------------------------------------------
# Latency model
# ---------------------------------------------------------------------------

class TestLatencyModel:
    @pytest.fixture
    def exchange(self, tmp_path):
        candle_dir = tmp_path / "candles"
        candle_dir.mkdir()
        path = candle_dir / "BTC_USD_USD_1m.csv"
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
            writer.writeheader()
            for i in range(10):
                ts = T0 + timedelta(minutes=i)
                writer.writerow({
                    "timestamp": ts.isoformat(),
                    "open": 50000, "high": 50200, "low": 49800, "close": 50000,
                    "volume": 500000,
                })
        clock = SimClock(start=T0)
        ds = ReplayDataStore(tmp_path, symbols=["BTC/USD:USD"])
        ds.load()
        ex = ReplayKrakenClient(
            clock=clock, data_store=ds,
            config=ExchangeSimConfig(
                initial_equity_usd=Decimal("100000"),
                latency_enabled=True,
                latency_base_ms=50.0,
                latency_max_ms=200.0,
            ),
        )
        return ex, clock

    @pytest.mark.asyncio
    async def test_latency_advances_clock(self, exchange):
        """API calls with latency enabled should advance the sim clock."""
        ex, clock = exchange
        t_before = clock.now()
        await ex.get_futures_tickers_bulk()
        t_after = clock.now()
        # Clock should have advanced by 50-200ms
        delta_ms = (t_after - t_before).total_seconds() * 1000
        assert 50 <= delta_ms <= 200

    @pytest.mark.asyncio
    async def test_latency_tracked_in_metrics(self, exchange):
        """Total injected latency should accumulate in metrics."""
        ex, clock = exchange
        for _ in range(5):
            await ex.get_futures_tickers_bulk()
        assert ex._metrics["latency_injected_ms_total"] > 250  # 5 * 50ms min


# ---------------------------------------------------------------------------
# Mid fallback counter
# ---------------------------------------------------------------------------

class TestMidFallbackCounter:
    @pytest.fixture
    def exchange(self, tmp_path):
        candle_dir = tmp_path / "candles"
        candle_dir.mkdir()
        path = candle_dir / "BTC_USD_USD_1m.csv"
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
            writer.writeheader()
            for i in range(60):
                ts = T0 + timedelta(minutes=i)
                writer.writerow({
                    "timestamp": ts.isoformat(),
                    "open": 50000, "high": 50200, "low": 49800, "close": 50000,
                    "volume": 500000,
                })
        clock = SimClock(start=T0)
        ds = ReplayDataStore(tmp_path, symbols=["BTC/USD:USD"])
        ds.load()
        ex = ReplayKrakenClient(
            clock=clock, data_store=ds,
            config=ExchangeSimConfig(initial_equity_usd=Decimal("100000"), jitter_enabled=False),
        )
        return ex, clock

    @pytest.mark.asyncio
    async def test_mid_fallback_not_counted_when_mid_known(self, exchange):
        """Orders placed with candle data should record mid, no fallback."""
        ex, clock = exchange
        await ex.place_futures_order(
            symbol="BTC/USD:USD", side="buy", order_type="limit",
            size=Decimal("0.1"), price=Decimal("49900"),
        )
        clock.advance(seconds=60)
        ex.step()
        # mid_at_placement should have been set, no fallback needed
        assert ex._metrics["mid_fallback_count"] == 0
