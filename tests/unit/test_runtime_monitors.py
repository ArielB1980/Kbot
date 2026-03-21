"""
Tests for the production runtime monitors:
  - Trade starvation monitor
  - Winner churn monitor

These are async background sentinels that fire Telegram alerts when they
detect regressions.  The tests use lightweight fakes of LiveTrading state
to exercise the detection logic without touching the real trading loop.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Minimal LiveTrading fake
# ---------------------------------------------------------------------------

class FakeLiveTradingBase:
    """Minimal stand-in for LiveTrading with only the fields monitors read."""

    def __init__(self) -> None:
        self.active = True
        self._signal_cooldown: Dict[str, datetime] = {}
        self.execution_gateway = None
        self.position_registry = None
        self.hardening_layer = None
        # Churn tracking (populated by auction_runner)
        self._auction_win_log: Dict[str, list] = {}
        self._auction_entry_log: Dict[str, datetime] = {}


class FakeCycleGuard:
    """CycleGuard that returns pre-configured cycle histories."""

    def __init__(self, cycles: List[dict]) -> None:
        self._cycles = cycles

    def get_recent_cycles(self, limit: int = 200) -> List[dict]:
        return self._cycles[:limit]


class FakeHardeningLayer:
    def __init__(self, cycle_guard: FakeCycleGuard) -> None:
        self.cycle_guard = cycle_guard


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cycle(
    started_at: datetime,
    signals: int = 0,
    orders: int = 0,
) -> dict:
    return {
        "started_at": started_at.isoformat(),
        "signals_generated": signals,
        "orders_placed": orders,
    }


async def _run_monitor_once(coro_fn, lt, **kwargs):
    """
    Run a monitor coroutine for exactly one check cycle then stop it.

    We do this by setting lt.active = False after a short delay so the
    monitor completes one full iteration of its while loop.
    """
    # Override the warmup sleep and check interval to be very short
    kwargs.setdefault("check_interval_seconds", 0)

    async def _stopper():
        await asyncio.sleep(0.15)
        lt.active = False

    task = asyncio.create_task(coro_fn(lt, **kwargs))
    stopper = asyncio.create_task(_stopper())
    await asyncio.gather(task, stopper, return_exceptions=True)


# ===========================================================================
# Trade Starvation Monitor
# ===========================================================================

class TestTradeStarvationMonitor:
    """Tests for run_trade_starvation_monitor."""

    @pytest.mark.asyncio
    async def test_fires_alert_when_signals_but_no_orders(self):
        """Should alert when signals exceed threshold but zero orders placed."""
        from src.live.health_monitor import run_trade_starvation_monitor

        lt = FakeLiveTradingBase()
        now = datetime.now(timezone.utc)

        # 20 cycles with signals but no orders
        cycles = [
            _make_cycle(now - timedelta(hours=3, minutes=i), signals=2, orders=0)
            for i in range(20)
        ]
        lt.hardening_layer = FakeHardeningLayer(FakeCycleGuard(cycles))

        with patch("src.monitoring.alert_dispatcher.send_alert", new_callable=AsyncMock) as mock_alert:
            # Patch sleep to not actually wait
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = _quick_sleep_then_stop(lt, max_iterations=2)

                await run_trade_starvation_monitor(
                    lt,
                    check_interval_seconds=0,
                    starvation_window_hours=6.0,
                    min_signals_threshold=10,
                )

            mock_alert.assert_called_once()
            call_args = mock_alert.call_args
            assert call_args[0][0] == "TRADE_STARVATION"
            assert "signals generated" in call_args[0][1].lower() or "TRADE STARVATION" in call_args[0][1]
            assert call_args[1].get("urgent") is True or call_args[0][2] is True

    @pytest.mark.asyncio
    async def test_no_alert_when_orders_present(self):
        """Should NOT alert when there are orders being placed."""
        from src.live.health_monitor import run_trade_starvation_monitor

        lt = FakeLiveTradingBase()
        now = datetime.now(timezone.utc)

        # 20 cycles with both signals and orders
        cycles = [
            _make_cycle(now - timedelta(hours=2, minutes=i * 5), signals=3, orders=1)
            for i in range(20)
        ]
        lt.hardening_layer = FakeHardeningLayer(FakeCycleGuard(cycles))

        with patch("src.monitoring.alert_dispatcher.send_alert", new_callable=AsyncMock) as mock_alert:
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = _quick_sleep_then_stop(lt, max_iterations=2)

                await run_trade_starvation_monitor(
                    lt,
                    check_interval_seconds=0,
                    starvation_window_hours=6.0,
                    min_signals_threshold=10,
                )

            mock_alert.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_alert_when_db_has_closed_trades_in_window(self):
        """When cycle history shows 0 orders but DB has closed trades in window, do not alarm."""
        from src.live.health_monitor import run_trade_starvation_monitor

        lt = FakeLiveTradingBase()
        now = datetime.now(timezone.utc)

        # Cycles with signals but 0 orders (would trigger starvation without backstop)
        cycles = [
            _make_cycle(now - timedelta(hours=2, minutes=i), signals=2, orders=0)
            for i in range(20)
        ]
        lt.hardening_layer = FakeHardeningLayer(FakeCycleGuard(cycles))

        with patch("src.monitoring.alert_dispatcher.send_alert", new_callable=AsyncMock) as mock_alert:
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = _quick_sleep_then_stop(lt, max_iterations=2)
                # DB backstop: closed trades=1, open positions=0 (repeat for each loop iteration)
                with patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread:
                    mock_to_thread.side_effect = [1, 0] * 5  # closed=1, open_positions=0

                    await run_trade_starvation_monitor(
                        lt,
                        check_interval_seconds=0,
                        starvation_window_hours=6.0,
                        min_signals_threshold=10,
                    )

            mock_alert.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_alert_when_open_positions_in_window(self):
        """When DB has 0 closed trades but an open position was entered in window, do not alarm.

        This is the key gap-closer: positions that are still live don't appear
        in the trades table, but *do* appear in the positions table.
        """
        from src.live.health_monitor import run_trade_starvation_monitor

        lt = FakeLiveTradingBase()
        now = datetime.now(timezone.utc)

        cycles = [
            _make_cycle(now - timedelta(hours=2, minutes=i), signals=2, orders=0)
            for i in range(20)
        ]
        lt.hardening_layer = FakeHardeningLayer(FakeCycleGuard(cycles))

        with patch("src.monitoring.alert_dispatcher.send_alert", new_callable=AsyncMock) as mock_alert:
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = _quick_sleep_then_stop(lt, max_iterations=2)
                # DB backstop: closed trades=0, open positions=1 (repeat for each loop iteration)
                with patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread:
                    mock_to_thread.side_effect = [0, 1] * 5  # closed=0, open_positions=1

                    await run_trade_starvation_monitor(
                        lt,
                        check_interval_seconds=0,
                        starvation_window_hours=6.0,
                        min_signals_threshold=10,
                    )

            mock_alert.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_alert_below_signal_threshold(self):
        """Should NOT alert when signal count is below the threshold."""
        from src.live.health_monitor import run_trade_starvation_monitor

        lt = FakeLiveTradingBase()
        now = datetime.now(timezone.utc)

        # Only 3 cycles with signals (below threshold of 10)
        cycles = [
            _make_cycle(now - timedelta(hours=1, minutes=i * 10), signals=1, orders=0)
            for i in range(3)
        ]
        lt.hardening_layer = FakeHardeningLayer(FakeCycleGuard(cycles))

        with patch("src.monitoring.alert_dispatcher.send_alert", new_callable=AsyncMock) as mock_alert:
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = _quick_sleep_then_stop(lt, max_iterations=2)

                await run_trade_starvation_monitor(
                    lt,
                    check_interval_seconds=0,
                    starvation_window_hours=6.0,
                    min_signals_threshold=10,
                )

            mock_alert.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_alert_without_hardening_layer(self):
        """Should not crash when hardening_layer is missing."""
        from src.live.health_monitor import run_trade_starvation_monitor

        lt = FakeLiveTradingBase()
        lt.hardening_layer = None

        with patch("src.monitoring.alert_dispatcher.send_alert", new_callable=AsyncMock) as mock_alert:
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = _quick_sleep_then_stop(lt, max_iterations=2)

                await run_trade_starvation_monitor(
                    lt,
                    check_interval_seconds=0,
                    starvation_window_hours=6.0,
                    min_signals_threshold=10,
                )

            mock_alert.assert_not_called()

    @pytest.mark.asyncio
    async def test_deduplication_single_alert_per_episode(self):
        """Should only fire one alert per starvation episode, not every check."""
        from src.live.health_monitor import run_trade_starvation_monitor

        lt = FakeLiveTradingBase()
        now = datetime.now(timezone.utc)

        cycles = [
            _make_cycle(now - timedelta(hours=2, minutes=i * 5), signals=3, orders=0)
            for i in range(20)
        ]
        lt.hardening_layer = FakeHardeningLayer(FakeCycleGuard(cycles))

        with patch("src.monitoring.alert_dispatcher.send_alert", new_callable=AsyncMock) as mock_alert:
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                # Let it run 4 iterations
                mock_sleep.side_effect = _quick_sleep_then_stop(lt, max_iterations=5)

                await run_trade_starvation_monitor(
                    lt,
                    check_interval_seconds=0,
                    starvation_window_hours=6.0,
                    min_signals_threshold=10,
                )

            # Only 1 alert despite multiple checks
            assert mock_alert.call_count == 1

    @pytest.mark.asyncio
    async def test_resolution_clears_alert(self):
        """After starvation resolves (orders appear), state resets for future episodes."""
        from src.live.health_monitor import run_trade_starvation_monitor

        lt = FakeLiveTradingBase()
        now = datetime.now(timezone.utc)

        # Start with starvation
        starving_cycles = [
            _make_cycle(now - timedelta(hours=2, minutes=i * 5), signals=3, orders=0)
            for i in range(20)
        ]
        # Then resolve
        resolved_cycles = [
            _make_cycle(now - timedelta(hours=1, minutes=i * 5), signals=3, orders=2)
            for i in range(20)
        ]

        call_count = 0
        guard = FakeCycleGuard(starving_cycles)

        def switch_to_resolved(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 3:
                # Switch to resolved cycles on iteration 3
                guard._cycles = resolved_cycles
            if call_count >= 5:
                lt.active = False
            return asyncio.sleep(0)

        lt.hardening_layer = FakeHardeningLayer(guard)

        with patch("src.monitoring.alert_dispatcher.send_alert", new_callable=AsyncMock) as mock_alert:
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = switch_to_resolved

                await run_trade_starvation_monitor(
                    lt,
                    check_interval_seconds=0,
                    starvation_window_hours=6.0,
                    min_signals_threshold=10,
                )

            # Should have alerted during starvation (exactly once)
            assert mock_alert.call_count == 1


# ===========================================================================
# Winner Churn Monitor
# ===========================================================================

class TestWinnerChurnMonitor:
    """Tests for run_winner_churn_monitor."""

    @pytest.mark.asyncio
    async def test_fires_alert_when_symbol_churns(self):
        """Should alert when a symbol wins N+ times without entry."""
        from src.live.health_monitor import run_winner_churn_monitor

        lt = FakeLiveTradingBase()
        now = datetime.now(timezone.utc)

        # AXS wins 7 times in last 6 hours, no entry
        lt._auction_win_log["AXS/USD"] = [
            now - timedelta(hours=i) for i in range(7)
        ]

        with patch("src.monitoring.alert_dispatcher.send_alert", new_callable=AsyncMock) as mock_alert:
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = _quick_sleep_then_stop(lt, max_iterations=2)

                await run_winner_churn_monitor(
                    lt,
                    check_interval_seconds=0,
                    max_wins_without_entry=5,
                    decay_hours=12.0,
                )

            mock_alert.assert_called_once()
            call_args = mock_alert.call_args
            assert call_args[0][0] == "WINNER_CHURN"
            assert "AXS/USD" in call_args[0][1]

    @pytest.mark.asyncio
    async def test_no_alert_when_entry_exists(self):
        """Should NOT alert if the symbol eventually got an entry."""
        from src.live.health_monitor import run_winner_churn_monitor

        lt = FakeLiveTradingBase()
        now = datetime.now(timezone.utc)

        # SOL wins 8 times but also entered once recently
        lt._auction_win_log["SOL/USD"] = [
            now - timedelta(hours=i) for i in range(8)
        ]
        lt._auction_entry_log["SOL/USD"] = now - timedelta(hours=1)

        with patch("src.monitoring.alert_dispatcher.send_alert", new_callable=AsyncMock) as mock_alert:
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = _quick_sleep_then_stop(lt, max_iterations=2)

                await run_winner_churn_monitor(
                    lt,
                    check_interval_seconds=0,
                    max_wins_without_entry=5,
                    decay_hours=12.0,
                )

            mock_alert.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_alert_below_win_threshold(self):
        """Should NOT alert if win count is below threshold."""
        from src.live.health_monitor import run_winner_churn_monitor

        lt = FakeLiveTradingBase()
        now = datetime.now(timezone.utc)

        # Only 3 wins (below threshold of 5)
        lt._auction_win_log["DOGE/USD"] = [
            now - timedelta(hours=i) for i in range(3)
        ]

        with patch("src.monitoring.alert_dispatcher.send_alert", new_callable=AsyncMock) as mock_alert:
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = _quick_sleep_then_stop(lt, max_iterations=2)

                await run_winner_churn_monitor(
                    lt,
                    check_interval_seconds=0,
                    max_wins_without_entry=5,
                    decay_hours=12.0,
                )

            mock_alert.assert_not_called()

    @pytest.mark.asyncio
    async def test_old_wins_decay(self):
        """Wins older than decay_hours should not count."""
        from src.live.health_monitor import run_winner_churn_monitor

        lt = FakeLiveTradingBase()
        now = datetime.now(timezone.utc)

        # 10 wins but all older than the 6h decay window
        lt._auction_win_log["OLD/USD"] = [
            now - timedelta(hours=20 + i) for i in range(10)
        ]

        with patch("src.monitoring.alert_dispatcher.send_alert", new_callable=AsyncMock) as mock_alert:
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = _quick_sleep_then_stop(lt, max_iterations=2)

                await run_winner_churn_monitor(
                    lt,
                    check_interval_seconds=0,
                    max_wins_without_entry=5,
                    decay_hours=6.0,
                )

            mock_alert.assert_not_called()

    @pytest.mark.asyncio
    async def test_deduplication_per_symbol(self):
        """Should only alert once per symbol per episode."""
        from src.live.health_monitor import run_winner_churn_monitor

        lt = FakeLiveTradingBase()
        now = datetime.now(timezone.utc)

        lt._auction_win_log["AXS/USD"] = [
            now - timedelta(hours=i) for i in range(7)
        ]

        with patch("src.monitoring.alert_dispatcher.send_alert", new_callable=AsyncMock) as mock_alert:
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = _quick_sleep_then_stop(lt, max_iterations=5)

                await run_winner_churn_monitor(
                    lt,
                    check_interval_seconds=0,
                    max_wins_without_entry=5,
                    decay_hours=12.0,
                )

            assert mock_alert.call_count == 1

    @pytest.mark.asyncio
    async def test_multiple_symbols_churn_simultaneously(self):
        """Should alert for all churning symbols in a single message."""
        from src.live.health_monitor import run_winner_churn_monitor

        lt = FakeLiveTradingBase()
        now = datetime.now(timezone.utc)

        # Two symbols both churning
        lt._auction_win_log["AXS/USD"] = [
            now - timedelta(hours=i) for i in range(6)
        ]
        lt._auction_win_log["DOGE/USD"] = [
            now - timedelta(hours=i) for i in range(8)
        ]

        with patch("src.monitoring.alert_dispatcher.send_alert", new_callable=AsyncMock) as mock_alert:
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = _quick_sleep_then_stop(lt, max_iterations=2)

                await run_winner_churn_monitor(
                    lt,
                    check_interval_seconds=0,
                    max_wins_without_entry=5,
                    decay_hours=12.0,
                )

            mock_alert.assert_called_once()
            msg = mock_alert.call_args[0][1]
            assert "AXS/USD" in msg
            assert "DOGE/USD" in msg

    @pytest.mark.asyncio
    async def test_resolution_clears_alert_for_symbol(self):
        """Once a churning symbol gets an entry, it should stop alerting."""
        from src.live.health_monitor import run_winner_churn_monitor

        lt = FakeLiveTradingBase()
        now = datetime.now(timezone.utc)

        lt._auction_win_log["AXS/USD"] = [
            now - timedelta(hours=i) for i in range(7)
        ]

        call_count = 0

        def resolve_after_iteration(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 3:
                # Simulate entry happening
                lt._auction_entry_log["AXS/USD"] = datetime.now(timezone.utc)
            if call_count >= 5:
                lt.active = False
            return asyncio.sleep(0)

        with patch("src.monitoring.alert_dispatcher.send_alert", new_callable=AsyncMock) as mock_alert:
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                mock_sleep.side_effect = resolve_after_iteration

                await run_winner_churn_monitor(
                    lt,
                    check_interval_seconds=0,
                    max_wins_without_entry=5,
                    decay_hours=12.0,
                )

            # Alert fired once during churn, not again after resolution
            assert mock_alert.call_count == 1


# ===========================================================================
# Helpers for controlling monitor loops in tests
# ===========================================================================

def _quick_sleep_then_stop(lt, max_iterations: int = 2):
    """
    Return a side_effect for asyncio.sleep that:
      - Lets the first sleep (warm-up) pass instantly
      - Counts subsequent sleeps and stops the loop after max_iterations
    """
    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count > max_iterations:
            lt.active = False
        return asyncio.sleep(0)

    return side_effect
