import asyncio
import inspect
import os
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from src.config.config import Config
from src.data.candle_manager import CandleManager
from src.data.data_acquisition import DataAcquisition
from src.data.data_quality_tracker import DataQualityTracker
from src.data.data_sanity import SanityThresholds
from src.data.fiat_currencies import has_disallowed_base
from src.data.kraken_client import KrakenClient
from src.data.market_discovery import MarketDiscoveryService
from src.domain.models import Position, Signal
from src.exceptions import CircuitOpenError, DataError, InvariantError, OperationalError
from src.execution.execution_engine import ExecutionEngine
from src.execution.execution_gateway import ExecutionGateway
from src.execution.executor import Executor
from src.execution.futures_adapter import FuturesAdapter
from src.execution.position_manager_v2 import (
    PositionManagerV2,
)
from src.execution.position_persistence import PositionPersistence

# Production-Grade Position State Machine
from src.execution.position_state_machine import (
    get_position_registry,
)
from src.live.cooldown_resolver import (
    CooldownResolver,
)
from src.live.maintenance import periodic_data_maintenance
from src.live.policy_fingerprint import build_policy_hash
from src.monitoring.logger import get_logger
from src.reconciliation.reconciler import Reconciler
from src.risk.risk_manager import RiskManager
from src.runtime.startup_phases import StartupStateMachine

# Production Hardening Layer V2 (Issue #1-5 fixes + V2 hardening)
from src.safety.integration import (
    HardeningDecision,
    init_hardening_layer,
)
from src.storage.maintenance import DatabasePruner
from src.storage.repository import (
    record_event,
    record_metrics_snapshot,
)
from src.strategy.smc_engine import SMCEngine
from src.utils.kill_switch import KillSwitch, KillSwitchReason

logger = get_logger(__name__)


class LiveTrading:
    """
    Live trading runtime.

    CRITICAL: Real capital at risk. Enforces all safety gates.
    """

    def __init__(self, config: Config):
        """Initialize live trading."""
        self.config = config

        # ========== STARTUP STATE MACHINE (P2.3) ==========
        self._startup_sm = StartupStateMachine()

        # ========== POSITION STATE MACHINE V2 ==========
        # Feature flag for gradual rollout (prod live hard-requires via runtime guard)
        self.use_state_machine_v2 = os.getenv("USE_STATE_MACHINE_V2", "false").lower() == "true"

        # CRITICAL: Runtime assertion - detect test mocks in production
        import sys
        from unittest.mock import MagicMock, Mock

        # Check if we're in a test environment
        is_test = (
            "pytest" in sys.modules
            or "PYTEST_CURRENT_TEST" in os.environ
            or any("test" in path.lower() for path in sys.path if isinstance(path, str))
        )

        if not is_test:
            # Production mode - verify no mocks are being used
            # Informational: confirms runtime is not contaminated by test harness.
            logger.info(
                "PRODUCTION_MODE_VERIFICATION",
                pytest_in_modules="pytest" in sys.modules,
                pytest_env=os.getenv("PYTEST_CURRENT_TEST"),
                sys_path_test_dirs=[
                    p for p in sys.path if isinstance(p, str) and "test" in p.lower()
                ],
            )

        # Core Components
        cache_mins = getattr(config.exchange, "market_discovery_cache_minutes", 60)
        cache_mins = int(cache_mins) if isinstance(cache_mins, (int, float)) else 60
        self.client = KrakenClient(
            api_key=config.exchange.api_key,
            api_secret=config.exchange.api_secret,
            futures_api_key=config.exchange.futures_api_key,
            futures_api_secret=config.exchange.futures_api_secret,
            use_testnet=config.exchange.use_testnet,
            market_cache_minutes=cache_mins,
            dry_run=config.system.dry_run,
            breaker_failure_threshold=getattr(
                config.exchange, "circuit_breaker_failure_threshold", 5
            ),
            breaker_rate_limit_threshold=getattr(
                config.exchange, "circuit_breaker_rate_limit_threshold", 2
            ),
            breaker_cooldown_seconds=getattr(
                config.exchange, "circuit_breaker_cooldown_seconds", 60.0
            ),
        )

        # CRITICAL: Verify client is not a mock
        if not is_test and (isinstance(self.client, Mock) or isinstance(self.client, MagicMock)):
            logger.critical("CRITICAL: KrakenClient is a Mock/MagicMock in production!")
            raise RuntimeError(
                "CRITICAL: KrakenClient is a Mock/MagicMock. "
                "This should never happen in production. Check for test code leaking into runtime."
            )

        self.data_acq = DataAcquisition(
            self.client,
            spot_symbols=config.exchange.spot_markets,
            futures_symbols=config.exchange.futures_markets,
        )

        from src.memory.institutional_memory import InstitutionalMemoryManager

        self.institutional_memory_manager = (
            InstitutionalMemoryManager(config.strategy)
            if getattr(config.strategy, "memory_enabled", False)
            else None
        )
        self.smc_engine = SMCEngine(
            config.strategy,
            event_recorder=record_event,
            institutional_memory=self.institutional_memory_manager,
        )
        self.risk_manager = RiskManager(
            config.risk, liquidity_filters=config.liquidity_filters, event_recorder=record_event
        )
        from src.execution.instrument_specs import InstrumentSpecRegistry

        self.instrument_spec_registry = InstrumentSpecRegistry(
            get_instruments_fn=self.client.get_futures_instruments,
            cache_ttl_seconds=getattr(
                config.exchange, "instrument_spec_cache_ttl_seconds", 12 * 3600
            ),
            ccxt_exchange=self.client.futures_exchange
            if hasattr(self.client, "futures_exchange")
            else None,
        )
        self.futures_adapter = FuturesAdapter(
            self.client,
            position_size_is_notional=config.exchange.position_size_is_notional,
            instrument_spec_registry=self.instrument_spec_registry,
        )

        # Store latest futures tickers for mapping (updated each tick)
        self.latest_futures_tickers: dict[str, Decimal] | None = None

        # ShockGuard: Wick/Flash move protection
        self.shock_guard = None
        if config.risk.shock_guard_enabled:
            from src.risk.shock_guard import ShockGuard

            self.shock_guard = ShockGuard(
                shock_move_pct=config.risk.shock_move_pct,
                shock_range_pct=config.risk.shock_range_pct,
                basis_shock_pct=config.risk.basis_shock_pct,
                shock_cooldown_minutes=config.risk.shock_cooldown_minutes,
                emergency_buffer_pct=config.risk.emergency_buffer_pct,
                trim_buffer_pct=config.risk.trim_buffer_pct,
                shock_marketwide_count=config.risk.shock_marketwide_count,
                shock_marketwide_window_sec=config.risk.shock_marketwide_window_sec,
            )
            logger.info("ShockGuard enabled")
        self.executor = Executor(config.execution, self.futures_adapter)
        self.execution_engine = ExecutionEngine(config)
        self.kill_switch = KillSwitch(self.client)
        self.market_discovery = MarketDiscoveryService(self.client, config)
        self._last_discovery_error_log_time: datetime | None = None

        # Auction mode allocator (if enabled)
        self.auction_allocator = None
        self.auction_signals_this_tick = []  # Collect signals for auction mode

        # Churn tracking: populated by auction_runner, consumed by winner churn monitor
        # Dict[symbol, list[datetime]] — timestamps when symbol won the auction
        self._auction_win_log: dict[str, list] = {}
        # Dict[symbol, datetime] — timestamp of last successful entry per symbol
        self._auction_entry_log: dict[str, datetime] = {}
        # Dict[symbol, {"reason": str, "at": datetime}] — last known open blocking cause.
        self._auction_open_block_reason_by_symbol: dict[str, dict[str, Any]] = {}
        # Rebalancer cooldown tracking: symbol -> last cycle where a trim executed
        self._last_trim_cycle_by_symbol: dict[str, int] = {}
        # Strategic no-signal persistence tracking (auction strategic closes only)
        self._auction_no_signal_cycles: int = 0

        # Signal cooldown resolver (encapsulates in-position + post-close cooldown logic)
        self.cooldown_resolver = CooldownResolver(config.strategy)
        # Legacy aliases kept for backward-compat with replay harness attribute access
        self._signal_cooldown = self.cooldown_resolver._signal_cooldown
        self._signal_cooldown_hours: float = float(
            getattr(config.strategy, "signal_cooldown_hours", 4.0)
        )
        self._tick_counter: int = 0
        # Rate-limited log guards for high-frequency replay loops.
        self._last_rate_limited_log_at: dict[str, datetime] = {}
        self._suppressed_rate_limited_logs: dict[str, int] = {}

        # Auto halt recovery tracking (instance-level, not class-level)
        self._auto_recovery_attempts: list = []
        if config.risk.auction_mode_enabled:
            from src.portfolio.auction_allocator import (
                AuctionAllocator,
                PortfolioLimits,
            )

            limits = PortfolioLimits(
                max_positions=config.risk.auction_max_positions,
                max_margin_util=config.risk.auction_max_margin_util,
                max_per_cluster=config.risk.auction_max_per_cluster,
                max_per_symbol=config.risk.auction_max_per_symbol,
                direction_concentration_penalty=config.risk.auction_direction_concentration_penalty,
            )
            allocator_kwargs = {
                "limits": limits,
                "swap_threshold": config.risk.auction_swap_threshold,
                "min_hold_minutes": config.risk.auction_min_hold_minutes,
                "max_trades_per_cycle": config.risk.auction_max_trades_per_cycle,
                "max_new_opens_per_cycle": config.risk.auction_max_new_opens_per_cycle,
                "max_closes_per_cycle": config.risk.auction_max_closes_per_cycle,
                "entry_cost": config.risk.auction_entry_cost,
                "exit_cost": config.risk.auction_exit_cost,
                "rebalancer_enabled": config.risk.auction_rebalancer_enabled,
                "rebalancer_trigger_pct_equity": config.risk.auction_rebalancer_trigger_pct_equity,
                "rebalancer_clear_pct_equity": config.risk.auction_rebalancer_clear_pct_equity,
                "rebalancer_per_symbol_trim_cooldown_cycles": config.risk.auction_rebalancer_per_symbol_trim_cooldown_cycles,
                "rebalancer_max_reductions_per_cycle": config.risk.auction_rebalancer_max_reductions_per_cycle,
                "rebalancer_max_total_margin_reduced_per_cycle": config.risk.auction_rebalancer_max_total_margin_reduced_per_cycle,
                "no_signal_persistence_enabled": config.risk.auction_no_signal_persistence_enabled,
                "no_signal_close_persistence_cycles": config.risk.auction_no_signal_close_persistence_cycles,
                "no_signal_persistence_canary_symbols": config.risk.auction_no_signal_persistence_canary_symbols,
            }
            accepted_params = set(inspect.signature(AuctionAllocator.__init__).parameters.keys())
            filtered_kwargs = {k: v for k, v in allocator_kwargs.items() if k in accepted_params}
            dropped_kwargs = sorted(set(allocator_kwargs.keys()) - set(filtered_kwargs.keys()))
            if dropped_kwargs:
                logger.warning(
                    "AuctionAllocator does not support some config args; using compatible subset",
                    dropped_kwargs=dropped_kwargs,
                )
            self.auction_allocator = AuctionAllocator(**filtered_kwargs)
            policy_snapshot, policy_hash = build_policy_hash(config)
            self._policy_snapshot = policy_snapshot
            self._policy_hash = policy_hash
            logger.info("Auction mode enabled", max_positions=limits.max_positions)
            logger.info(
                "STARTUP_POLICY_FINGERPRINT",
                policy_hash=policy_hash,
                policy_snapshot=policy_snapshot,
            )

        self._last_partial_close_at: datetime | None = None
        if self.use_state_machine_v2:
            logger.critical("🚀 POSITION STATE MACHINE V2 ENABLED")

            # Initialize the Position Registry (singleton)
            self.position_registry = get_position_registry()

            # Initialize Persistence (SQLite)
            self.position_persistence = PositionPersistence("data/positions.db")

            # Initialize Position Manager V2 (pass multi_tp config for runner mode)
            self.position_manager_v2 = PositionManagerV2(
                registry=self.position_registry,
                multi_tp_config=getattr(self.config, "multi_tp", None),
                instrument_spec_registry=getattr(self, "instrument_spec_registry", None),
                strategy_config=self.config.strategy,
                institutional_memory=self.institutional_memory_manager,
            )

            # Initialize Execution Gateway - ALL orders flow through here
            self.execution_gateway = ExecutionGateway(
                exchange_client=self.client,
                registry=self.position_registry,
                position_manager=self.position_manager_v2,
                persistence=self.position_persistence,
                on_partial_close=lambda _: setattr(
                    self, "_last_partial_close_at", datetime.now(UTC)
                ),
                instrument_spec_registry=getattr(self, "instrument_spec_registry", None),
                on_trade_recorded=self._on_trade_recorded,
                startup_machine=self._startup_sm,
                maker_fee_bps=config.risk.maker_fee_bps,
                taker_fee_bps=config.risk.taker_fee_bps,
                funding_rate_daily_bps=config.risk.funding_rate_daily_bps,
            )

            logger.critical("State Machine V2 running - all orders via gateway")
            self._protection_monitor = None
            self._protection_task = None
            self._order_poll_task = None

        self.active = False

        # Candle data managed by dedicated service
        from src.data.ohlcv_fetcher import OHLCVFetcher

        _ohlcv_fetcher = OHLCVFetcher(self.client, config)
        self.candle_manager = CandleManager(
            self.client,
            spot_to_futures=self.futures_adapter.map_spot_to_futures,
            use_futures_fallback=getattr(config.exchange, "use_futures_ohlcv_fallback", True),
            ohlcv_fetcher=_ohlcv_fetcher,
        )

        self.last_trace_log: dict[str, datetime] = {}  # Dashboard update throttling
        self._current_cycle_id: str | None = None
        self.last_account_sync = datetime.min.replace(tzinfo=UTC)
        self.last_maintenance_run = datetime.min.replace(tzinfo=UTC)
        self.last_data_maintenance = datetime.min.replace(tzinfo=UTC)
        self.last_recon_time = datetime.min.replace(tzinfo=UTC)
        self.last_metrics_emit = datetime.min.replace(tzinfo=UTC)
        self.ticks_since_emit = 0
        self.signals_since_emit = 0
        self.last_fetch_latency_ms: int | None = None
        self.db_pruner = DatabasePruner()

        # TP Backfill cooldown tracking (symbol -> last_backfill_time)
        self.tp_backfill_cooldowns: dict[str, datetime] = {}

        # Coin processing tracking
        self.coin_processing_stats: dict[str, dict] = {}  # Track processing stats per coin
        self.last_status_summary = datetime.min.replace(tzinfo=UTC)

        # Market Expansion (Coin Universe)
        # V3: Use get_all_candidates() - config tiers are for universe selection only
        self.markets = config.exchange.spot_markets
        if config.assets.mode == "whitelist":
            self.markets = config.assets.whitelist
        elif config.coin_universe and config.coin_universe.enabled:
            # Get all candidate symbols (flattened from tiers or direct list)
            expanded = config.coin_universe.get_all_candidates()
            # Deduplicate and exclude disallowed bases (fiat + stablecoin).
            self.markets = [s for s in list(set(expanded)) if not has_disallowed_base(s)]
            logger.info(
                "Coin Universe Enabled (V3 - dynamic tier classification)",
                market_count=len(self.markets),
            )

        # Update Data Acquisition with full list
        self.data_acq = DataAcquisition(
            self.client, spot_symbols=self.markets, futures_symbols=config.exchange.futures_markets
        )

        # ===== PRODUCTION HARDENING LAYER =====
        # Integrates: InvariantMonitor, CycleGuard, PositionDeltaReconciler, DecisionAuditLogger
        # This provides hard safety limits, timing protection, and decision-complete logging
        try:
            self.hardening = init_hardening_layer(
                config=config,
                kill_switch=self.kill_switch,
            )
            logger.info(
                "ProductionHardeningLayer initialized",
                trading_allowed=self.hardening.is_trading_allowed(),
            )
        except (ValueError, TypeError, KeyError, ImportError, OSError) as e:
            logger.warning(
                "Failed to initialize ProductionHardeningLayer",
                error=str(e),
                error_type=type(e).__name__,
            )
            self.hardening = None

        # ===== DATA SANITY GATE + QUALITY TRACKER =====
        try:
            from src.config.config import DataSanityConfig

            ds = getattr(config.data, "data_sanity", None)
            if isinstance(ds, DataSanityConfig):
                self.sanity_thresholds = SanityThresholds(
                    max_spread_pct=Decimal(str(ds.max_spread_pct)),
                    min_volume_24h_usd=Decimal(str(ds.min_volume_24h_usd)),
                    min_decision_tf_candles=ds.min_decision_tf_candles,
                    decision_tf=ds.decision_tf,
                    allow_spot_fallback=ds.allow_spot_fallback,
                )
                self.data_quality_tracker = DataQualityTracker(
                    degraded_after_failures=ds.degraded_after_failures,
                    suspend_after_seconds=ds.suspend_after_hours * 3600,
                    release_after_successes=ds.release_after_successes,
                    probe_interval_seconds=ds.probe_interval_minutes * 60,
                    log_cooldown_seconds=ds.log_cooldown_seconds,
                    degraded_skip_ratio=ds.degraded_skip_ratio,
                )
                self.data_quality_tracker.restore(active_universe=self._market_symbols())
                logger.info(
                    "DataSanityGate initialized",
                    max_spread_pct=float(self.sanity_thresholds.max_spread_pct),
                    min_volume=float(self.sanity_thresholds.min_volume_24h_usd),
                    min_candles=self.sanity_thresholds.min_decision_tf_candles,
                    decision_tf=self.sanity_thresholds.decision_tf,
                )
            else:
                raise TypeError("data_sanity config not found or wrong type")
        except (ValueError, TypeError, KeyError) as e:
            self.sanity_thresholds = SanityThresholds()
            self.data_quality_tracker = DataQualityTracker()
            logger.debug("DataSanityGate init with defaults", error=str(e))

        logger.info(
            "Live Trading initialized",
            markets=config.exchange.futures_markets,
            state_machine_v2=self.use_state_machine_v2,
            hardening_enabled=self.hardening is not None,
        )

    def _market_symbols(self) -> list[str]:
        """Return filtered spot symbols -- delegates to coin_processor module."""
        from src.live.coin_processor import market_symbols

        return market_symbols(self)

    def _get_static_tier(self, symbol: str) -> str | None:
        """DEPRECATED tier lookup -- delegates to coin_processor module."""
        from src.live.coin_processor import get_static_tier

        return get_static_tier(self, symbol)

    async def _update_market_universe(self):
        """Discover and update trading universe -- delegates to coin_processor module."""
        from src.live.coin_processor import update_market_universe

        await update_market_universe(self)

    async def run(self):
        """
        Main trading loop.
        """
        import os

        from src.storage.repository import record_event

        # 0. Record Startup Event
        try:
            record_event(
                "SYSTEM_STARTUP",
                "system",
                {
                    "version": self.config.system.version,
                    "pid": os.getpid(),
                    "mode": "LiveTradingEngine",
                },
            )
        except (OperationalError, DataError, OSError) as e:
            logger.error(
                "Failed to record startup event", error=str(e), error_type=type(e).__name__
            )

        # Smoke Mode / Local Dev Limits
        max_loops = int(os.getenv("MAX_LOOPS", "-1"))
        run_seconds = int(os.getenv("RUN_SECONDS", "-1"))
        start_time = time.time()
        loop_count = 0
        is_smoke_mode = max_loops > 0 or run_seconds > 0

        logger.info(
            "Starting run loop",
            max_loops=max_loops if max_loops > 0 else "unlimited",
            run_seconds=run_seconds if run_seconds > 0 else "unlimited",
            dry_run=self.config.system.dry_run,
            smoke_mode=is_smoke_mode,
        )

        self.active = True
        self._reconcile_requested = False
        self.trade_paused = False
        # Important but not an error condition.
        logger.warning("🚀 STARTING LIVE TRADING")

        # ===== PRODUCTION HARDENING SELF-TEST (V2) =====
        # Must pass before trading can start
        if self.hardening:
            success, errors = self.hardening.self_test()
            if not success:
                logger.critical(
                    "HARDENING_SELF_TEST_FAILED",
                    errors=errors,
                    action="REFUSING_TO_START",
                )
                raise RuntimeError(f"Production hardening self-test failed: {errors}")
            logger.info("Hardening self-test passed", run_id=self.hardening._run_id)

        try:
            from src.live.startup import initialize_and_sync

            await initialize_and_sync(self)

            # Safety state banner — one-line "why are we paused?" visibility
            try:
                from src.safety.safety_state import get_safety_state_manager

                ss = get_safety_state_manager().load()
                logger.info(
                    "SAFETY_STATE_ON_STARTUP",
                    halt_active=ss.halt_active,
                    halt_reason=ss.halt_reason,
                    kill_switch_active=ss.kill_switch_active,
                    kill_switch_reason=ss.kill_switch_reason,
                    kill_switch_latched=ss.kill_switch_latched,
                    peak_equity=ss.peak_equity,
                    peak_equity_updated_at=ss.peak_equity_updated_at,
                    last_reset_at=ss.last_reset_at,
                    last_reset_mode=ss.last_reset_mode,
                )
            except Exception as e:
                logger.debug("Could not load unified safety state on startup", error=str(e))

            # 4.5. Run first tick to hydrate runtime state (now safely after READY)
            if not (self.config.system.dry_run and not self.client.has_valid_futures_credentials()):
                try:
                    self._current_cycle_id = f"startup_{int(datetime.now(UTC).timestamp())}"
                    await self._tick()
                    logger.info("Initial tick completed - runtime state hydrated")
                except (OperationalError, DataError) as e:
                    logger.error("Initial tick failed", error=str(e), error_type=type(e).__name__)
                finally:
                    self._current_cycle_id = None

            # 4.6. Validate position protection (startup safety gate)
            try:
                await self._polling_monitor.validate_position_protection()
            except (OperationalError, DataError) as e:
                logger.error(
                    "Position protection validation failed",
                    error=str(e),
                    error_type=type(e).__name__,
                )

            # 5. Main Loop
            while self.active:
                # Check Smoke Mode Limits
                if max_loops > 0 and loop_count >= max_loops:
                    logger.info(
                        "Smoke mode: Max loops reached",
                        max_loops=max_loops,
                        loops_completed=loop_count,
                    )
                    break

                if run_seconds > 0 and (time.time() - start_time) >= run_seconds:
                    elapsed = time.time() - start_time
                    logger.info(
                        "Smoke mode: Run time limit reached",
                        run_seconds=run_seconds,
                        elapsed_seconds=f"{elapsed:.1f}",
                    )
                    break

                loop_count += 1
                self._last_cycle_count = loop_count

                if self.kill_switch.is_active():
                    # Attempt auto-recovery for margin_critical (the most common false-positive halt)
                    recovered = False
                    if self.kill_switch.reason == KillSwitchReason.MARGIN_CRITICAL:
                        recovered = await self._polling_monitor.try_auto_recovery()

                    if not recovered:
                        logger.critical(
                            "Kill switch active - pausing loop",
                            reason=self.kill_switch.reason.value
                            if self.kill_switch.reason
                            else "unknown",
                        )
                        await asyncio.sleep(60)
                        continue

                # Periodic Market Discovery
                if self.config.exchange.use_market_discovery:
                    now = datetime.now(UTC)
                    elapsed_discovery = (now - self.last_discovery_time).total_seconds()
                    refresh_sec = self.config.exchange.discovery_refresh_hours * 3600

                    if elapsed_discovery >= refresh_sec:
                        await self._update_market_universe()
                        self.last_discovery_time = now

                loop_start = datetime.now(UTC)
                cycle_id = f"tick_{loop_count}_{int(loop_start.timestamp())}"
                self._current_cycle_id = cycle_id

                try:
                    record_event(
                        "CYCLE_TICK_BEGIN",
                        "system",
                        {"cycle_id": cycle_id, "loop_count": loop_count},
                    )
                except Exception as e:
                    logger.debug("Failed to record CYCLE_TICK_BEGIN event", error=str(e))

                try:
                    await self._tick()
                except CircuitOpenError as e:
                    logger.warning(
                        "Tick skipped: API circuit breaker open",
                        breaker_info=str(e)[:200],
                    )
                except InvariantError as e:
                    self._record_tick_crash(cycle_id, e)
                    logger.critical(
                        "INVARIANT VIOLATION in tick — triggering kill switch", error=str(e)
                    )
                    if self.kill_switch:
                        await self.kill_switch.activate(KillSwitchReason.INVARIANT_VIOLATION)
                    break
                except OperationalError as e:
                    logger.warning("Operational error in tick (transient)", error=str(e))
                except DataError as e:
                    logger.warning("Data error in tick", error=str(e))
                except Exception as e:
                    self._record_tick_crash(cycle_id, e)
                    raise
                else:
                    try:
                        record_event(
                            "CYCLE_TICK_END",
                            "system",
                            {"cycle_id": cycle_id, "loop_count": loop_count},
                        )
                    except Exception as e:
                        logger.debug("Failed to record CYCLE_TICK_END event", error=str(e))
                finally:
                    self._current_cycle_id = None

                self.ticks_since_emit += 1
                # P0.4: Write heartbeat file after each successful tick
                self._write_heartbeat()
                now = datetime.now(UTC)
                if (now - self.last_metrics_emit).total_seconds() >= 60.0:
                    try:
                        record_metrics_snapshot(
                            {
                                "last_tick_at": now.isoformat(),
                                "ticks_last_min": self.ticks_since_emit,
                                "signals_last_min": self.signals_since_emit,
                                "markets_count": len(self._market_symbols()),
                                "api_fetch_latency_ms": getattr(
                                    self, "last_fetch_latency_ms", None
                                ),
                                "coins_futures_fallback_used": self.candle_manager.get_futures_fallback_count(),
                                "orders_per_minute": self.execution_gateway._order_rate_limiter.orders_last_minute,
                                "orders_per_10s": self.execution_gateway._order_rate_limiter.orders_last_10s,
                                "orders_blocked_total": self.execution_gateway._order_rate_limiter.orders_blocked_total,
                            }
                        )
                        self.last_metrics_emit = now
                        self.ticks_since_emit = 0
                        self.signals_since_emit = 0
                        # P3.2: Alert on high order rate
                        opm = self.execution_gateway._order_rate_limiter.orders_last_minute
                        if opm >= 30:  # 50% of limit = warning threshold
                            logger.warning(
                                "HIGH_ORDER_RATE",
                                orders_per_minute=opm,
                                orders_per_10s=self.execution_gateway._order_rate_limiter.orders_last_10s,
                                limit_per_minute=60,
                            )
                    except (OperationalError, DataError) as ex:
                        logger.warning("Failed to emit metrics snapshot", error=str(ex))
                    except Exception as ex:
                        logger.error(
                            "Unexpected error in metrics snapshot",
                            error=str(ex),
                            error_type=type(ex).__name__,
                        )

                # Periodic reconciliation (positions: system vs exchange)
                _recon_cfg = getattr(self.config, "reconciliation", None)
                if _recon_cfg and getattr(_recon_cfg, "reconcile_enabled", True):
                    recon_interval = getattr(_recon_cfg, "periodic_interval_seconds", 120)
                    run_after_orders = getattr(self, "_reconcile_requested", False)
                    if (
                        run_after_orders
                        or (now - self.last_recon_time).total_seconds() >= recon_interval
                    ):
                        try:
                            if self.use_state_machine_v2 and self.execution_gateway:
                                # V2: reconcile against registry/exchange (no DB-only authority)
                                res = await self.execution_gateway.sync_with_exchange()
                                logger.info("V2 sync_with_exchange complete", **res)
                            else:
                                recon = self._build_reconciler()
                                await recon.reconcile_all()
                            self.last_recon_time = now
                            if run_after_orders:
                                self._reconcile_requested = False
                        except (OperationalError, DataError) as ex:
                            logger.warning("Reconciliation failed (transient)", error=str(ex))
                        except Exception as ex:
                            logger.error(
                                "Unexpected reconciliation error",
                                error=str(ex),
                                error_type=type(ex).__name__,
                            )

                # ===== CYCLE SUMMARY (single log line per tick with key metrics) =====
                now = datetime.now(UTC)
                cycle_elapsed = (now - loop_start).total_seconds()
                try:
                    positions_count = 0
                    if self.use_state_machine_v2 and self.execution_gateway:
                        positions_count = len(self.execution_gateway.registry.get_all_active())
                    elif self.position_manager_v2:
                        positions_count = len(self.position_manager_v2.get_all_positions())

                    kill_active = self.kill_switch.is_active() if self.kill_switch else False
                    system_state = "NORMAL"
                    if kill_active:
                        system_state = "KILL_SWITCH"
                    elif self.hardening and hasattr(self.hardening, "invariant_monitor"):
                        inv_state = self.hardening.invariant_monitor.state.value
                        if inv_state != "active":
                            system_state = inv_state.upper()

                    # Circuit breaker status (P2.1 observability)
                    breaker_state = "n/a"
                    breaker_failures = 0
                    try:
                        bi = self.client.api_breaker.get_state_info()
                        breaker_state = bi["state"]
                        breaker_failures = bi["failure_count"] + bi["rate_limit_count"]
                    except (OperationalError, DataError, KeyError, AttributeError) as e:
                        logger.debug("Breaker state fetch failed (non-fatal)", error=str(e))

                    logger.info(
                        "CYCLE_SUMMARY",
                        cycle=loop_count,
                        duration_ms=int(cycle_elapsed * 1000),
                        positions=positions_count,
                        universe=len(self._market_symbols()),
                        system_state=system_state,
                        cooldowns_active=len(self._signal_cooldown),
                        breaker=breaker_state,
                        breaker_failures=breaker_failures,
                    )
                except (OperationalError, DataError) as summary_err:
                    logger.warning(
                        "CYCLE_SUMMARY_FAILED",
                        error=str(summary_err),
                        error_type=type(summary_err).__name__,
                    )
                except Exception as summary_err:
                    # Bug in summary logic — log but don't crash the loop for it
                    logger.error(
                        "CYCLE_SUMMARY_BUG",
                        error=str(summary_err),
                        error_type=type(summary_err).__name__,
                    )

                # Dynamic sleep to align with 1m intervals
                elapsed = cycle_elapsed
                sleep_time = max(5.0, 60.0 - elapsed)
                await asyncio.sleep(sleep_time)

            # Smoke mode summary
            if is_smoke_mode:
                total_runtime = time.time() - start_time
                logger.info(
                    "✅ SMOKE TEST COMPLETED SUCCESSFULLY",
                    loops_completed=loop_count,
                    runtime_seconds=f"{total_runtime:.1f}",
                    markets_tracked=len(self.markets),
                    dry_run=self.config.system.dry_run,
                )

        except asyncio.CancelledError:
            logger.info("Live trading loop cancelled")
        except Exception as e:
            # Mark startup as failed if we haven't reached READY yet
            if not self._startup_sm.is_ready and not self._startup_sm.is_failed:
                self._startup_sm.fail(reason=f"Exception during startup: {e}")
            # Log the exception and re-raise to ensure non-zero exit code
            logger.critical("Live trading failed with exception", error=str(e), exc_info=True)
            raise
        finally:
            self.active = False
            if getattr(self, "_protection_monitor", None):
                self._protection_monitor.stop()
            # Stop LivePollingMonitor (cancels all managed tasks: protection,
            # order polling, daily summary, starvation, churn, trade recording)
            if getattr(self, "_polling_monitor", None):
                await self._polling_monitor.stop()

            # Stop HealthCheckCoordinator
            if getattr(self, "_health_coordinator", None):
                self._health_coordinator.stop()
            if (
                getattr(self, "_health_coordinator_task", None)
                and not self._health_coordinator_task.done()
            ):
                self._health_coordinator_task.cancel()
                try:
                    await self._health_coordinator_task
                except asyncio.CancelledError:
                    pass
            if getattr(self, "_spot_dca_task", None) and not self._spot_dca_task.done():
                self._spot_dca_task.cancel()
                try:
                    await self._spot_dca_task
                except asyncio.CancelledError:
                    pass
            if getattr(self, "_spec_refresh_task", None) and not self._spec_refresh_task.done():
                self._spec_refresh_task.cancel()
                try:
                    await self._spec_refresh_task
                except asyncio.CancelledError:
                    pass
            if getattr(self, "_ws_candle_feed", None):
                await self._ws_candle_feed.stop()
            if getattr(self, "_ws_candle_task", None) and not self._ws_candle_task.done():
                self._ws_candle_task.cancel()
                try:
                    await self._ws_candle_task
                except asyncio.CancelledError:
                    pass
            if getattr(self, "_telegram_cmd_task", None) and not self._telegram_cmd_task.done():
                if getattr(self, "_telegram_handler", None):
                    self._telegram_handler.stop()
                self._telegram_cmd_task.cancel()
                try:
                    await self._telegram_cmd_task
                except asyncio.CancelledError:
                    pass
            await self.data_acq.stop()
            await self.client.close()
            # Persist data quality state so SUSPENDED/DEGRADED symbols survive restart
            self.data_quality_tracker.force_persist()
            logger.info("Live trading shutdown complete")

    async def _run_spot_dca(self) -> None:
        """Daily spot DCA purchase -- delegates to spot_dca module."""
        from src.live.spot_dca import run_spot_dca

        await run_spot_dca(self)

    async def _run_spec_refresh(self, interval_seconds: int = 4 * 3600) -> None:
        """Periodically refresh instrument specs from the Kraken API.

        Prevents the in-memory cache from going stale and falling back to a
        potentially corrupted disk cache (e.g. overwritten by a cron job).
        """
        await asyncio.sleep(60)  # initial delay to let startup finish
        while True:
            try:
                await asyncio.sleep(interval_seconds)
                registry = getattr(self, "instrument_spec_registry", None)
                if registry:
                    old_count = len(registry._by_raw)
                    registry._loaded_at = 0  # force refresh
                    await registry.refresh()
                    new_count = len(registry._by_raw)
                    logger.info(
                        "Periodic instrument spec refresh completed",
                        old_count=old_count,
                        new_count=new_count,
                    )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(
                    "Periodic spec refresh failed (will retry)",
                    error=str(e),
                    error_type=type(e).__name__,
                )

    async def _run_ws_candle_feed(self) -> None:
        """Stream 15m OHLC candles from Kraken WebSocket v2 into CandleManager."""
        from src.data.ws_candle_feed import KrakenCandleFeed

        symbols = self._market_symbols()
        if not symbols:
            logger.warning("No symbols for WS candle feed -- skipping")
            return
        self._ws_candle_feed = KrakenCandleFeed(
            candle_manager=self.candle_manager,
            symbols=symbols,
            interval=15,
            max_retries=self.config.data.ws_reconnect_max_retries,
            backoff_base=self.config.data.ws_reconnect_backoff_seconds,
        )
        await self._ws_candle_feed.run()

    def _convert_to_position(self, data: dict) -> Position:
        """Convert raw exchange position dict to Position domain object -- delegates to exchange_sync module."""
        from src.live.exchange_sync import convert_to_position

        return convert_to_position(self, data)

    async def _sync_positions(self, raw_positions: list[dict] | None = None) -> list[dict]:
        """Sync active positions from exchange -- delegates to exchange_sync module."""
        from src.live.exchange_sync import sync_positions

        return await sync_positions(self, raw_positions)

    def _build_reconciler(self) -> "Reconciler":
        """Build Reconciler -- delegates to exchange_sync module."""
        from src.live.exchange_sync import build_reconciler

        return build_reconciler(self)

    def _record_tick_crash(self, cycle_id: str, exc: BaseException) -> None:
        """Best-effort: record CYCLE_TICK_CRASH to DB and crash log file."""
        try:
            from src.storage.repository import record_event as _rec

            _rec(
                "CYCLE_TICK_CRASH",
                "system",
                {
                    "cycle_id": cycle_id,
                    "exception_type": type(exc).__name__,
                    "exception_msg": str(exc)[:500],
                },
            )
        except Exception as db_err:
            logger.warning("Failed to record tick crash to DB", error=str(db_err))
        try:
            from src.runtime.crash_capture import write_crash_log

            write_crash_log(exc, context="tick", cycle_id=cycle_id)
        except Exception as log_err:
            logger.warning("Failed to write crash log file", error=str(log_err))

    def _rate_limited_log(self, key: str, interval_seconds: int) -> tuple[bool, int]:
        """Return (emit_now, suppressed_since_last_emit)."""
        now = datetime.now(UTC)
        last = self._last_rate_limited_log_at.get(key)
        if last is None or (now - last).total_seconds() >= interval_seconds:
            suppressed = int(self._suppressed_rate_limited_logs.get(key, 0))
            self._last_rate_limited_log_at[key] = now
            self._suppressed_rate_limited_logs[key] = 0
            return True, suppressed
        self._suppressed_rate_limited_logs[key] = (
            int(self._suppressed_rate_limited_logs.get(key, 0)) + 1
        )
        return False, 0

    async def _tick(self):
        """
        Single iteration of live trading logic.
        Optimized for batch processing (Phase 10).
        """
        # Gate: no tick before READY (P0.1 invariant)
        if not self._startup_sm.is_ready:
            raise InvariantError(
                f"_tick() called before READY (phase={self._startup_sm.phase.value}). "
                "This is a startup ordering bug — no trading actions before READY."
            )
        self._tick_counter = int(getattr(self, "_tick_counter", 0) or 0) + 1

        # 0. Kill Switch Check
        from src.live.tick_safety import handle_kill_switch

        if await handle_kill_switch(self):
            return

        # 0.1 Order Timeout Monitoring (CRITICAL: Check first)
        try:
            cancelled_count = await self.executor.check_order_timeouts()
            if cancelled_count > 0:
                logger.warning("Cancelled expired orders", count=cancelled_count)
        except (OperationalError, DataError) as e:
            logger.error(
                "Failed to check order timeouts", error=str(e), error_type=type(e).__name__
            )

        # 1. Check Data Health
        if not self.data_acq.is_healthy():
            if getattr(self, "_replay_disable_candle_health_gate", False):
                emit, suppressed = self._rate_limited_log("replay_data_acq_unhealthy", 60)
                if emit:
                    logger.warning(
                        "Replay fail-open: data acquisition unhealthy, continuing in degraded mode",
                        suppressed_since_last=suppressed,
                    )
            else:
                logger.error("Data acquisition unhealthy")
                return

        # 2. Sync Active Positions (Global Sync)
        # Phase 2 Fix: Pass positions to _sync_positions to avoid duplicate API call
        try:
            # This updates global state in Repository and internal trackers
            if self.config.system.dry_run and not self.client.has_valid_futures_credentials():
                all_raw_positions = []
            else:
                all_raw_positions = await self.client.get_all_futures_positions()
            # Pass positions to sync to avoid duplicate API call
            await self._sync_positions(all_raw_positions)
        except (OperationalError, DataError) as e:
            logger.error("Failed to sync positions", error=str(e), error_type=type(e).__name__)
            return

        # 2.1 PRODUCTION HARDENING V2: Pre-tick Invariant Check
        # This checks all hard limits (drawdown, positions, margin) and halts if violated
        # Uses HardeningDecision enum for explicit state handling
        if self.hardening:
            try:
                # Get account info for invariant checks
                account_info = await self.client.get_futures_account_info()
                current_equity = Decimal(str(account_info.get("equity", 0)))
                available_margin = Decimal(str(account_info.get("availableMargin", 0)))
                margin_used = Decimal(str(account_info.get("marginUsed", 0)))
                margin_util = margin_used / current_equity if current_equity > 0 else Decimal("0")

                # Convert raw positions to Position objects for check
                position_objs = [
                    self._convert_to_position(p) for p in all_raw_positions if p.get("size", 0) != 0
                ]

                # Equity refetch callback for implausibility guard (P0.4)
                async def _refetch_equity() -> Decimal:
                    info = await self.client.get_futures_account_info()
                    return Decimal(str(info.get("equity", 0)))

                # Run pre-tick safety checks (returns HardeningDecision)
                decision = await self.hardening.pre_tick_check(
                    current_equity=current_equity,
                    open_positions=position_objs,
                    margin_utilization=margin_util,
                    available_margin=available_margin,
                    refetch_equity_fn=_refetch_equity,
                )

                if decision == HardeningDecision.HALT:
                    logger.critical(
                        "TRADING_HALTED_BY_INVARIANT_MONITOR",
                        message="System halted - manual intervention required via clear_halt()",
                    )
                    # Ensure cleanup runs via finally block
                    return

                if decision == HardeningDecision.SKIP_TICK:
                    logger.debug("TICK_SKIPPED_BY_CYCLE_GUARD")
                    return

                # Log if new entries are blocked but position management allowed
                if not self.hardening.is_trading_allowed():
                    logger.warning(
                        "NEW_ENTRIES_BLOCKED",
                        system_state=self.hardening.invariant_monitor.state.value,
                        management_allowed=self.hardening.is_management_allowed(),
                    )
            except (OperationalError, DataError, ValueError) as e:
                logger.critical(
                    "Production hardening pre-tick check failed — SKIPPING TICK",
                    error=str(e),
                    error_type=type(e).__name__,
                )
                return

        # 2.5. Cleanup orphan reduce-only orders (SL/TP orders for closed positions)
        try:
            await self._cleanup_orphan_reduce_only_orders(all_raw_positions)
        except (OperationalError, DataError) as e:
            logger.error(
                "Failed to cleanup orphan orders", error=str(e), error_type=type(e).__name__
            )
            # Don't return - continue with trading loop

        # 3. Batch Data Fetching
        from src.live.tick_safety import fetch_batch_data

        batch = await fetch_batch_data(self, all_raw_positions)
        if batch is None:
            return
        market_symbols = batch.market_symbols
        map_spot_tickers = batch.map_spot_tickers
        map_futures_tickers = batch.map_futures_tickers
        map_futures_tickers_full = batch.map_futures_tickers_full
        map_positions = batch.map_positions
        recent_close_by_symbol = batch.recent_close_by_symbol

        # 4. Parallel Analysis Loop
        from src.live.coin_processor import TickContext, process_coin

        sem = asyncio.Semaphore(50)
        tick_ctx = TickContext(
            map_spot_tickers=map_spot_tickers,
            map_futures_tickers=map_futures_tickers,
            map_futures_tickers_full=map_futures_tickers_full,
            map_positions=map_positions,
            recent_close_by_symbol=recent_close_by_symbol,
            analysis_funnel={
                "universe_total": len(market_symbols),
                "eligible_symbols": 0,
                "symbols_analyzed": 0,
                "symbols_skipped_by_reason": {},
                "setups_found": 0,
                "signals_scored": 0,
                "signals_above_threshold": 0,
                "signals_generated": 0,
                "suppress_in_position": 0,
                "suppress_post_close_win": 0,
                "suppress_post_close_loss": 0,
                "suppress_global_open_throttle": 0,
                "suppress_other": 0,
            },
        )

        async def _process_coin_wrapper(spot_symbol: str) -> None:
            async with sem:
                await process_coin(self, tick_ctx, spot_symbol)

        # Execute parallel processing (process_coin extracted to coin_processor.py)
        if self.auction_allocator:
            self.auction_signals_this_tick = []

        if getattr(self, "_replay_relaxed_signal_gates", False):
            analyzable = list(market_symbols)
        else:
            analyzable = [s for s in market_symbols if self.data_quality_tracker.should_analyze(s)]
        tick_ctx.analysis_funnel.setdefault("symbols_skipped_by_reason", {})[
            "data_quality_gate"
        ] = max(0, len(market_symbols) - len(analyzable))
        await asyncio.gather(
            *[_process_coin_wrapper(s) for s in analyzable], return_exceptions=True
        )
        self._analysis_funnel_metrics = tick_ctx.analysis_funnel

        # Run auction mode allocation (if enabled) - after all signals processed
        if self.auction_allocator:
            signals_count = len(self.auction_signals_this_tick)
            emit_auction_tick_log = True
            if getattr(self, "_replay_disable_candle_health_gate", False):
                cycle_num = int(
                    getattr(self, "_last_cycle_count", 0) or getattr(self, "_tick_counter", 0) or 0
                )
                emit_auction_tick_log = (
                    signals_count > 0 or cycle_num <= 3 or (cycle_num % 100 == 0)
                )
            if emit_auction_tick_log:
                logger.info("AUCTION_START", signals_collected=signals_count)
                logger.info(
                    "Auction: About to run allocation",
                    signals_collected=signals_count,
                    auction_allocator_exists=bool(self.auction_allocator),
                )
            await self._run_auction_allocation(all_raw_positions)
            if emit_auction_tick_log:
                logger.info("AUCTION_END", signals_collected=signals_count)
        else:
            logger.debug("Auction: Skipped (auction_allocator is None)")

        # Phase 2: Batch save all collected candles (grouped by symbol/timeframe)
        # Phase 2: Batch save all collected candles (delegated to Manager)
        await self.candle_manager.flush_pending()

        # Persist data quality state (rate-limited internally to every 5 min)
        self.data_quality_tracker.persist()

        # Log periodic status summary (every 5 minutes)
        now = datetime.now(UTC)
        if (now - self.last_status_summary).total_seconds() > 300:  # 5 minutes
            try:
                total_coins = len(market_symbols)
                coins_with_candles = sum(
                    1
                    for s in market_symbols
                    if len(self.candle_manager.get_candles(s, "15m")) >= 50
                )
                coins_processed_recently = sum(
                    1
                    for s in market_symbols
                    if self.coin_processing_stats.get(s, {}).get(
                        "last_processed", datetime.min.replace(tzinfo=UTC)
                    )
                    > (now - timedelta(minutes=10))
                )
                coins_with_traces = len([s for s in market_symbols if s in self.last_trace_log])

                summary = {
                    "total_coins": total_coins,
                    "coins_with_sufficient_candles": coins_with_candles,
                    "coins_processed_recently": coins_processed_recently,
                    "coins_with_traces": coins_with_traces,
                    "coins_waiting_for_candles": total_coins - coins_with_candles,
                }
                if getattr(self, "_last_ticker_with", None) is not None:
                    summary["symbols_with_ticker"] = self._last_ticker_with
                    summary["symbols_without_ticker"] = getattr(self, "_last_ticker_without", 0)
                if getattr(self, "_last_futures_count", None) is not None:
                    summary["symbols_with_futures"] = self._last_futures_count
                fc = self.candle_manager.pop_futures_fallback_count()
                if fc > 0:
                    summary["coins_futures_fallback_used"] = fc
                # Include data quality summary
                dq = self.data_quality_tracker.get_status_summary()
                summary["data_quality_healthy"] = dq["healthy"]
                summary["data_quality_degraded"] = dq["degraded"]
                summary["data_quality_suspended"] = dq["suspended"]
                logger.info("Coin processing status summary", **summary)
                self.last_status_summary = now
            except (OperationalError, DataError, ValueError) as e:
                logger.error(
                    "Failed to log status summary", error=str(e), error_type=type(e).__name__
                )

        # 4.5 CRITICAL: Validate all positions have stop loss protection
        # Legacy validation removed - using new _validate_position_protection after initial tick

        # 5. Account Sync (Throttled) - Moved to step 2 to prevent duplicate calls
        # Reference: _sync_positions call in Step 2 handles global state update

        # 7. Operational Maintenance (Daily)
        now = datetime.now(UTC)
        if (now - self.last_maintenance_run).total_seconds() > 86400:  # 24 hours
            try:
                results = self.db_pruner.run_maintenance()
                logger.info("Daily database maintenance complete", results=results)
                self.last_maintenance_run = now
            except (OperationalError, DataError, OSError) as e:
                logger.error("Daily maintenance failed", error=str(e), error_type=type(e).__name__)

        # 8. Periodic data maintenance (hourly): stale/missing trace recovery
        if (now - self.last_data_maintenance).total_seconds() > 3600:
            try:
                await periodic_data_maintenance(self._market_symbols(), max_age_hours=6.0)
                self.last_data_maintenance = now
            except (OperationalError, DataError) as e:
                logger.error(
                    "Periodic data maintenance failed", error=str(e), error_type=type(e).__name__
                )

        # 9. PRODUCTION HARDENING V2: Post-tick Cleanup
        # CRITICAL: This must always run, even on exceptions
        # The post_tick_cleanup() method internally uses try/finally to ensure lock release
        if self.hardening:
            self.hardening.post_tick_cleanup()

    # _background_hydration_task removed (Replaced by CandleManager.initialize)

    # ===== AUTO HALT RECOVERY =====
    _AUTO_RECOVERY_MAX_PER_DAY = 2
    _AUTO_RECOVERY_COOLDOWN_SECONDS = 300  # 5 minutes since halt
    _AUTO_RECOVERY_MARGIN_SAFE_PCT = 85  # Must be below this to recover

    async def _sync_account_state(self):
        """Fetch and persist real-time account state -- delegates to exchange_sync module."""
        from src.live.exchange_sync import sync_account_state

        await sync_account_state(self)

    # -----------------------------------------------------------------------
    # Signal handling (delegated to src.live.signal_handler)
    # -----------------------------------------------------------------------

    async def _handle_signal(
        self,
        signal: Signal,
        spot_price: Decimal,
        mark_price: Decimal,
        notional_override: "Decimal | None" = None,
    ) -> dict:
        """Signal processing -- delegates to signal_handler module.

        Args:
            notional_override: When set (auction execution path), used as
                base notional in risk sizing and enables utilisation boost.
        """
        from src.live.signal_handler import handle_signal

        return await handle_signal(
            self, signal, spot_price, mark_price, notional_override=notional_override
        )

    async def _handle_signal_v2(
        self,
        signal: Signal,
        spot_price: Decimal,
        mark_price: Decimal,
    ) -> dict:
        """V2 signal processing -- delegates to signal_handler module."""
        from src.live.signal_handler import handle_signal_v2

        return await handle_signal_v2(self, signal, spot_price, mark_price)

    async def _update_candles(self, symbol: str):
        """Update local candle caches from acquisition with throttling."""
        await self.candle_manager.update_candles(symbol)

    async def _run_auction_allocation(self, raw_positions: list[dict]):
        """Auction allocation -- delegates to auction_runner module."""
        from src.live.auction_runner import run_auction_allocation

        await run_auction_allocation(self, raw_positions)

    async def _save_trade_history(self, position: Position, exit_price: Decimal, exit_reason: str):
        """Save closed position to trade history -- delegates to exchange_sync module."""
        from src.live.exchange_sync import save_trade_history

        await save_trade_history(self, position, exit_price, exit_reason)

    def _write_heartbeat(self) -> None:
        """Write heartbeat file -- delegates to health_monitor module."""
        from src.live.health_monitor import write_heartbeat

        write_heartbeat(self)

    async def _on_trade_recorded(self, position, trade) -> None:
        """Callback fired by ExecutionGateway after trade recorded -- delegates to trade_callbacks module."""
        from src.live.trade_callbacks import on_trade_recorded

        await on_trade_recorded(self, position, trade)

    # -----------------------------------------------------------------------
    # Protection operations (delegated to src.live.protection_ops)
    # -----------------------------------------------------------------------

    async def _reconcile_protective_orders(
        self, raw_positions: list[dict], current_prices: dict[str, Decimal]
    ):
        """TP Backfill / Reconciliation -- delegates to protection_ops module."""
        from src.live.protection_ops import reconcile_protective_orders

        await reconcile_protective_orders(self, raw_positions, current_prices)

    async def _reconcile_stop_loss_order_ids(self, raw_positions: list[dict]):
        """SL order ID reconciliation -- delegates to protection_ops module."""
        from src.live.protection_ops import reconcile_stop_loss_order_ids

        await reconcile_stop_loss_order_ids(self, raw_positions)

    async def _place_missing_stops_for_unprotected(
        self, raw_positions: list[dict], max_per_tick: int = 3
    ) -> None:
        """Place missing stops -- delegates to protection_ops module."""
        from src.live.protection_ops import place_missing_stops_for_unprotected

        await place_missing_stops_for_unprotected(self, raw_positions, max_per_tick)

    async def _should_skip_tp_backfill(
        self,
        symbol: str,
        pos_data: dict,
        db_pos: Position,
        current_price: Decimal,
        is_protected: bool | None = None,
    ) -> bool:
        """Safety checks -- delegates to protection_ops module."""
        from src.live.protection_ops import should_skip_tp_backfill

        return await should_skip_tp_backfill(
            self, symbol, pos_data, db_pos, current_price, is_protected
        )

    def _needs_tp_backfill(self, db_pos: Position, symbol_orders: list[dict]) -> bool:
        """TP coverage check -- delegates to protection_ops module."""
        from src.live.protection_ops import needs_tp_backfill

        return needs_tp_backfill(self, db_pos, symbol_orders)

    async def _compute_tp_plan(
        self, symbol: str, pos_data: dict, db_pos: Position, current_price: Decimal
    ) -> list[Decimal] | None:
        """TP plan computation -- delegates to protection_ops module."""
        from src.live.protection_ops import compute_tp_plan

        return await compute_tp_plan(self, symbol, pos_data, db_pos, current_price)

    async def _cleanup_orphan_reduce_only_orders(self, raw_positions: list[dict]):
        """Orphan order cleanup -- delegates to protection_ops module."""
        from src.live.protection_ops import cleanup_orphan_reduce_only_orders

        await cleanup_orphan_reduce_only_orders(self, raw_positions)

    async def _place_tp_backfill(
        self,
        symbol: str,
        pos_data: dict,
        db_pos: Position,
        tp_plan: list[Decimal],
        symbol_orders: list[dict],
        current_price: Decimal,
    ):
        """TP order placement -- delegates to protection_ops module."""
        from src.live.protection_ops import place_tp_backfill

        await place_tp_backfill(
            self, symbol, pos_data, db_pos, tp_plan, symbol_orders, current_price
        )
