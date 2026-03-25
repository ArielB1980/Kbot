"""
BacktestRunner — Orchestrates LiveTrading in replay mode.

Runs the real LiveTrading._tick() method step-by-step with:
- Simulated clock (SimClock)
- Simulated exchange (ReplayKrakenClient)
- Metrics collection (ReplayMetrics)
- Optional fault injection (FaultInjector)

Key trick: we don't run LiveTrading.run() (which has the main loop + sleeps).
Instead we:
1. Construct LiveTrading with the replay client
2. Call the startup sequence manually
3. Step through ticks one at a time, advancing the clock between each

This gives us deterministic control while exercising the real code paths.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable
from unittest.mock import patch, AsyncMock, MagicMock

from src.backtest.replay_harness.sim_clock import SimClock
from src.backtest.replay_harness.data_store import ReplayDataStore
from src.backtest.replay_harness.exchange_sim import ReplayKrakenClient, ExchangeSimConfig
from src.backtest.replay_harness.fault_injector import FaultInjector
from src.backtest.replay_harness.metrics import ReplayMetrics
from src.exceptions import InvariantError, OperationalError, DataError
from src.monitoring.logger import get_logger

logger = get_logger(__name__)


class BacktestRunner:
    """Step-by-step replay runner for the live trading stack.

    Usage:
        runner = BacktestRunner(
            data_dir=Path("data/replay/episode_1"),
            symbols=["BTC/USD:USD", "ETH/USD:USD"],
            start=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        )
        results = await runner.run()
        results.print_report()
    """

    def __init__(
        self,
        data_dir: Path,
        symbols: List[str],
        start: datetime,
        end: datetime,
        *,
        tick_interval_seconds: int = 60,
        exchange_config: Optional[ExchangeSimConfig] = None,
        fault_injector: Optional[FaultInjector] = None,
        config_overrides: Optional[Dict[str, Any]] = None,
        max_ticks: Optional[int] = None,
        timeframes: Optional[List[str]] = None,
        disable_cycle_guard_throttle: bool = False,
        disable_db_mock: bool = False,
    ):
        self._data_dir = Path(data_dir)
        self._symbols = symbols
        self._start = start
        self._end = end
        self._tick_interval = tick_interval_seconds
        self._exchange_config = exchange_config or ExchangeSimConfig()
        self._fault_injector = fault_injector
        self._config_overrides = config_overrides or {}
        self._max_ticks = max_ticks
        resolved_timeframes = list(timeframes or ["1m"])
        if "1m" not in resolved_timeframes:
            # Exchange simulation fill model requires 1m bars.
            resolved_timeframes = ["1m", *resolved_timeframes]
        self._timeframes = resolved_timeframes
        self._disable_cycle_guard_throttle = disable_cycle_guard_throttle
        self._disable_db_mock = disable_db_mock

        # Built during setup
        self._clock: Optional[SimClock] = None
        self._data_store: Optional[ReplayDataStore] = None
        self._exchange: Optional[ReplayKrakenClient] = None
        self._metrics: Optional[ReplayMetrics] = None
        self._live_trading: Optional[Any] = None  # LiveTrading instance
        self._db_patches: List[Any] = []  # Active unittest.mock patchers

    def _setup_db_mock(self) -> None:
        """Mock the database layer so replay doesn't need DATABASE_URL.

        We inject a fake ``_db_instance`` into ``src.storage.db`` **before**
        any repository function calls ``get_db()``.  The mock provides a
        no-op session context manager so all writes silently succeed and all
        reads return empty results.
        """
        # Build a mock session that behaves like a SQLAlchemy session.
        # Key requirement: every chainable method returns the same mock,
        # and terminal methods return sensible defaults (empty list, 0, None)
        # so downstream code that does `count > 0` or `for row in all()` works.
        mock_session = MagicMock()
        mock_session.query.return_value = mock_session  # chainable
        mock_session.filter.return_value = mock_session
        mock_session.filter_by.return_value = mock_session
        mock_session.order_by.return_value = mock_session
        mock_session.limit.return_value = mock_session
        mock_session.offset.return_value = mock_session
        mock_session.all.return_value = []
        mock_session.first.return_value = None
        mock_session.one_or_none.return_value = None
        mock_session.count.return_value = 0
        mock_session.scalar.return_value = 0
        mock_session.delete.return_value = 0  # row count for bulk delete

        @contextmanager
        def _fake_session():
            yield mock_session

        mock_db = MagicMock()
        mock_db.get_session = _fake_session

        # Patch the global singleton so get_db() returns our mock
        p = patch("src.storage.db._db_instance", mock_db)
        p.start()
        self._db_patches.append(p)
        logger.info("REPLAY_DB_MOCK_INSTALLED")

    def _teardown_db_mock(self) -> None:
        """Remove database mock patches."""
        for p in self._db_patches:
            p.stop()
        self._db_patches.clear()

    async def run(self) -> ReplayMetrics:
        """Execute the full replay backtest. Returns metrics."""
        self._setup()
        use_db_mock = (
            not self._disable_db_mock
            and os.getenv("REPLAY_DISABLE_DB_MOCK", "0") != "1"
        )
        if use_db_mock:
            self._setup_db_mock()
        else:
            logger.warning("REPLAY_DB_MOCK_DISABLED_USING_REAL_DB")
        try:
            return await self._run_with_db_mock()
        finally:
            # Always remove DB patching, even when replay raises early.
            if use_db_mock:
                self._teardown_db_mock()

    async def _run_with_db_mock(self) -> ReplayMetrics:
        """Run replay body with DB mock already installed."""
        await self._initialize()
        if (
            self._disable_cycle_guard_throttle
            and self._live_trading is not None
            and getattr(self._live_trading, "hardening", None) is not None
            and getattr(self._live_trading.hardening, "cycle_guard", None) is not None
        ):
            self._live_trading.hardening.cycle_guard.min_interval = timedelta(seconds=0)

        tick_count = 0
        current = self._start

        logger.info(
            "REPLAY_START",
            start=self._start.isoformat(),
            end=self._end.isoformat(),
            symbols=self._symbols,
            tick_interval=self._tick_interval,
        )

        while current <= self._end:
            if self._max_ticks and tick_count >= self._max_ticks:
                break

            self._clock.set(current)

            # Step exchange simulation (process pending orders, triggers, funding)
            fills = self._exchange.step(current)

            # Record fills in metrics
            for fill in fills:
                self._metrics.total_fills += 1
                if fill.is_maker:
                    self._metrics.maker_fills += 1
                else:
                    self._metrics.taker_fills += 1

            # Run one tick of the live trading engine
            try:
                await self._run_tick()
                # In replay we do not run background health loops; explicitly
                # poll exchange order status so fills/cancels reach state machine.
                if (
                    self._live_trading is not None
                    and getattr(self._live_trading, "execution_gateway", None) is not None
                ):
                    await self._live_trading.execution_gateway.poll_and_process_order_updates()
                self._metrics.total_ticks += 1
                if bool(getattr(self._live_trading, "trade_paused", False)):
                    self._metrics.trade_paused_ticks += 1
            except InvariantError as e:
                self._metrics.record_event("INVARIANT_VIOLATION", {"error": str(e)})
                self._metrics.invariant_k_violations += 1
                self._metrics.record_exception("InvariantError")
                logger.error("REPLAY_INVARIANT_VIOLATION", tick=tick_count, error=str(e))
            except OperationalError as e:
                self._metrics.record_exception("OperationalError")
                self._metrics.failed_ticks += 1
            except DataError as e:
                self._metrics.record_exception("DataError")
                self._metrics.failed_ticks += 1
            except AttributeError as e:
                # AttributeError = programming bug → must crash (not continue).
                # This matches production behavior where systemd restarts on crash.
                self._metrics.record_exception("AttributeError")
                self._metrics.failed_ticks += 1
                logger.error("REPLAY_BUG_CRASH", tick=tick_count, error=str(e))
                break  # Stop ticking — process would have crashed
            except Exception as e:
                self._metrics.record_exception(type(e).__name__)
                self._metrics.failed_ticks += 1
                logger.error("REPLAY_TICK_EXCEPTION", tick=tick_count, error=str(e), type=type(e).__name__)

            # Record equity snapshot
            ex = self._exchange.exchange_metrics
            self._metrics.record_equity(
                timestamp=current,
                equity=Decimal(str(ex["equity"])),
                margin_used=Decimal(str(ex["margin_used"])),
                unrealized_pnl=Decimal(str(ex.get("unrealized_pnl", 0))),
                open_positions=ex["open_positions"],
            )

            tick_count += 1
            current += timedelta(seconds=self._tick_interval)

        # Ensure replay windows finalize to flat so trade outcomes are realized.
        if os.getenv("REPLAY_FORCE_FLAT_AT_END", "1") == "1":
            try:
                open_positions = await self._exchange.get_all_futures_positions()
                for p in open_positions:
                    symbol = str(p.get("symbol") or "").strip()
                    if not symbol:
                        continue
                    await self._exchange.close_position(symbol)
            except Exception as e:
                logger.warning("REPLAY_FORCE_FLAT_FAILED", error=str(e))

        # Finalize
        exchange_summary = self._exchange.exchange_metrics
        self._metrics.total_fees = Decimal(str(exchange_summary["total_fees"]))
        self._metrics.total_funding = Decimal(str(exchange_summary["total_funding"]))
        self._metrics.gross_pnl = Decimal(str(exchange_summary["realized_pnl"]))
        self._metrics.total_trades = int(exchange_summary.get("trades_closed", 0))
        self._metrics.orders_blocked_by_rate_limiter = (
            self._live_trading.execution_gateway._order_rate_limiter.orders_blocked_total
            if self._live_trading and hasattr(self._live_trading, "execution_gateway")
            else 0
        )

        logger.info(
            "REPLAY_COMPLETE",
            ticks=tick_count,
            pnl=float(self._metrics.gross_pnl),
            fees=float(self._metrics.total_fees),
            trades=self._metrics.total_trades,
        )

        return self._metrics

    def _setup(self) -> None:
        """Initialize all components."""
        self._clock = SimClock(start=self._start)

        self._data_store = ReplayDataStore(
            data_dir=self._data_dir,
            symbols=self._symbols,
            timeframes=self._timeframes,
        )
        self._data_store.load()

        self._exchange = ReplayKrakenClient(
            clock=self._clock,
            data_store=self._data_store,
            config=self._exchange_config,
            fault_injector=self._fault_injector,
        )

        self._metrics = ReplayMetrics()
        self._metrics.peak_equity = self._exchange_config.initial_equity_usd

    async def _initialize(self) -> None:
        """Initialize LiveTrading with the replay client.

        We patch KrakenClient so that construction returns our ReplayKrakenClient,
        then re-wire every component that stored a reference to the client.
        """
        from src.config.config import load_config

        # Set env vars for config loading
        os.environ.setdefault("ENV", "local")
        os.environ.setdefault("DRY_RUN", "0")
        # Reduce noisy per-tick logs in replay research loops.
        os.environ["REPLAY_RESEARCH_MINIMAL_LOGS"] = "1"
        # Replay research: prefer filled entries to avoid stale pending churn.
        os.environ["REPLAY_FORCE_MARKET_ENTRY"] = "1"
        # Keep pending positions from being immediately archived in replay.
        os.environ["REPLAY_SKIP_STALE_PENDING_CLOSE"] = "1"
        # Replay can cluster synthetic orders; relax order-rate invariant limits.
        os.environ["REPLAY_RELAX_ORDER_RATE_LIMITER"] = "1"

        # Isolate instrument spec cache so replay never overwrites the live bot's cache
        import tempfile
        _replay_cache = tempfile.mktemp(suffix="_replay_specs.json", prefix="instrument_specs_")
        os.environ["INSTRUMENT_SPECS_CACHE_PATH"] = _replay_cache

        # Isolate peak equity state so replay doesn't read stale production peaks
        # that would trigger DEGRADED mode (90% phantom drawdown).
        _replay_peak = tempfile.mktemp(suffix="_peak_equity.json", prefix="replay_")
        os.environ["PEAK_EQUITY_STATE_PATH"] = _replay_peak

        config = load_config()

        # Apply overrides
        for key, value in self._config_overrides.items():
            parts = key.split(".")
            obj = config
            for p in parts[:-1]:
                obj = getattr(obj, p)
            setattr(obj, parts[-1], value)

        # Ensure test-safe settings
        config.system.dry_run = False  # We want the exchange sim to receive orders
        config.exchange.spot_markets = self._symbols
        config.exchange.futures_markets = self._symbols
        # Keep replay scope bounded to requested symbols only.
        config.exchange.use_market_discovery = False
        if hasattr(config, "coin_universe") and config.coin_universe:
            config.coin_universe.enabled = False
        if hasattr(config, "assets") and config.assets:
            config.assets.mode = "whitelist"
            config.assets.whitelist = list(self._symbols)
        if hasattr(config, "data") and config.data:
            config.data.min_healthy_coins = min(1, len(self._symbols))
            if hasattr(config.data, "data_sanity") and config.data.data_sanity:
                config.data.data_sanity.allow_spot_fallback = True
                config.data.data_sanity.min_decision_tf_candles = 1
                config.data.data_sanity.min_volume_24h_usd = 0
                config.data.data_sanity.max_spread_pct = 1.0
        if (
            os.getenv("REPLAY_RELAX_MIN_SCORES", "1") == "1"
            and hasattr(config, "strategy")
            and config.strategy
        ):
            config.strategy.min_score_tight_smc_aligned = min(
                float(getattr(config.strategy, "min_score_tight_smc_aligned", 75.0) or 75.0),
                55.0,
            )
            config.strategy.min_score_tight_smc_neutral = min(
                float(getattr(config.strategy, "min_score_tight_smc_neutral", 80.0) or 80.0),
                60.0,
            )
            config.strategy.min_score_wide_structure_aligned = min(
                float(getattr(config.strategy, "min_score_wide_structure_aligned", 70.0) or 70.0),
                55.0,
            )
            config.strategy.min_score_wide_structure_neutral = min(
                float(getattr(config.strategy, "min_score_wide_structure_neutral", 75.0) or 75.0),
                60.0,
            )

        # Build LiveTrading with our replay client injected at construction time.
        # The key is making KrakenClient(...) return self._exchange so every
        # downstream component (DataAcquisition, FuturesAdapter, KillSwitch,
        # CandleManager, ExecutionGateway) gets the real replay client — not a MagicMock.
        from src.live.live_trading import LiveTrading

        exchange_ref = self._exchange

        def _fake_kraken_client(*args, **kwargs):
            """Return the replay exchange instead of constructing a real KrakenClient."""
            return exchange_ref

        with patch("src.live.live_trading.KrakenClient", side_effect=_fake_kraken_client):
            lt = LiveTrading(config)

        # Belt-and-suspenders: ensure client reference is the replay exchange
        lt.client = self._exchange

        # Re-wire all sub-components that cached the client reference
        if hasattr(lt, "execution_gateway"):
            lt.execution_gateway.client = self._exchange
        if hasattr(lt, "kill_switch"):
            lt.kill_switch.client = self._exchange
        if hasattr(lt, "data_acq"):
            lt.data_acq.client = self._exchange
        if hasattr(lt, "futures_adapter"):
            lt.futures_adapter.client = self._exchange
        if hasattr(lt, "candle_manager"):
            lt.candle_manager.client = self._exchange
        # Replay should not stall on live candle-health entry gate.
        lt._replay_disable_candle_health_gate = True
        # Replay research should bypass strict live gating for signal discovery.
        lt._replay_relaxed_signal_gates = True

        # Advance startup state machine to READY so _tick() is allowed.
        # In real production, LiveTrading.run() goes through SYNCING →
        # RECONCILING → READY. In replay we skip that (no real exchange
        # sync needed) and jump straight to READY.
        from src.runtime.startup_phases import StartupPhase
        if hasattr(lt, "_startup_sm"):
            lt._startup_sm.advance_to(StartupPhase.SYNCING, reason="replay: skip to READY")
            lt._startup_sm.advance_to(StartupPhase.RECONCILING, reason="replay: skip to READY")
            lt._startup_sm.advance_to(StartupPhase.READY, reason="replay: all startup steps bypassed")
            logger.info("REPLAY_STARTUP_READY", phase=lt._startup_sm.phase.value)

        self._live_trading = lt

    async def _run_tick(self) -> None:
        """Run one tick of the live trading engine.

        We do NOT mock datetime (it breaks >= comparisons with MagicMock).
        Instead we let the real datetime module run. The SimClock controls
        simulated time, and _tick() mostly uses datetime.now(timezone.utc)
        for logging/staleness checks — slight discrepancy is acceptable
        since exchange data is driven by the SimClock-controlled exchange.
        """
        if self._live_trading is None:
            raise RuntimeError("LiveTrading not initialized")

        await self._live_trading._tick()
