import asyncio
import inspect
import os
import re
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import List, Dict, Optional, Any

from src.config.config import Config
from src.data.market_discovery import MarketDiscoveryService
from src.monitoring.logger import get_logger
from src.data.fiat_currencies import has_disallowed_base
from src.data.kraken_client import KrakenClient
from src.data.data_acquisition import DataAcquisition
from src.data.candle_manager import CandleManager
from src.strategy.smc_engine import SMCEngine
from src.risk.risk_manager import RiskManager
from src.execution.executor import Executor
from src.execution.futures_adapter import FuturesAdapter
from src.execution.execution_engine import ExecutionEngine
# Production-Grade Position State Machine
from src.execution.position_state_machine import (
    ManagedPosition,
    PositionState,
    PositionRegistry,
    ExitReason,
    OrderEvent,
    OrderEventType,
    get_position_registry,
    reset_position_registry
)
from src.execution.position_manager_v2 import (
    PositionManagerV2,
    ManagementAction as ManagementActionV2,
    ActionType as ActionTypeV2
)
from src.execution.execution_gateway import ExecutionGateway
from src.execution.position_persistence import PositionPersistence
from src.execution.production_safety import (
    SafetyConfig,
    ProtectionEnforcer,
    PositionProtectionMonitor,
)

from src.utils.kill_switch import KillSwitch, KillSwitchReason
from src.exceptions import CircuitOpenError, OperationalError, DataError, InvariantError
from src.runtime.startup_phases import StartupStateMachine, StartupPhase
from src.domain.models import Candle, Signal, SignalType, Position, Side
from src.storage.repository import record_event, record_metrics_snapshot, get_trades_since
from src.storage.maintenance import DatabasePruner
from src.live.startup_validator import ensure_all_coins_have_traces
from src.live.maintenance import periodic_data_maintenance
from src.reconciliation.reconciler import Reconciler

# Production Hardening Layer V2 (Issue #1-5 fixes + V2 hardening)
from src.safety.integration import (
    ProductionHardeningLayer,
    init_hardening_layer,
    HardeningDecision,
)

from src.data.symbol_utils import exchange_position_side as _exchange_position_side
from src.data.data_sanity import SanityThresholds, check_ticker_sanity, check_candle_sanity
from src.data.data_quality_tracker import DataQualityTracker
from src.live.policy_fingerprint import build_policy_hash

logger = get_logger(__name__)


def _normalize_symbol_key(symbol: str) -> str:
    key = (symbol or "").strip().upper()
    key = key.split(":")[0]
    if key.startswith("PF_"):
        base = key.replace("PF_", "").replace("USD", "")
        if base:
            return f"{base}/USD"
    return key


def _attach_thesis_trace_fields(trace_details: Dict[str, Any], thesis_snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Enrich DECISION_TRACE payload with thesis conviction telemetry."""
    if not thesis_snapshot:
        return trace_details
    trace_details["thesis_conviction"] = thesis_snapshot.get("conviction")
    trace_details["thesis_status"] = thesis_snapshot.get("status")
    trace_details["thesis_decay"] = {
        "time_decay": thesis_snapshot.get("time_decay"),
        "zone_rejection": thesis_snapshot.get("zone_rejection"),
        "volume_fade": thesis_snapshot.get("volume_fade"),
    }
    return trace_details


def _resolve_signal_cooldown_params(strategy_config, symbol: str) -> Dict[str, Any]:
    """
    Resolve signal cooldown hours (global policy).
    """
    base_hours = float(getattr(strategy_config, "signal_cooldown_hours", 4.0))
    return {
        "cooldown_hours": base_hours,
        "canary_applied": False,
    }


def _resolve_post_close_cooldown_kind_and_minutes(
    exit_reason: str,
    strategy_config,
) -> tuple[str, int]:
    """
    Classify post-close cooldown bucket from exit reason.
    """
    reason = (exit_reason or "").strip().lower()
    strategic_markers = ("time_based", "strategic_close", "auction_strategic_close")
    if any(marker in reason for marker in strategic_markers):
        minutes = int(
            getattr(strategy_config, "signal_post_close_cooldown_strategic_minutes", 120)
        )
        return "POST_CLOSE_STRATEGIC", max(0, minutes)
    loss_markers = ("stop", "invalidation", "loss", "liquidation")
    if any(marker in reason for marker in loss_markers):
        minutes = int(getattr(strategy_config, "signal_post_close_cooldown_loss_minutes", 120))
        return "POST_CLOSE_LOSS", max(0, minutes)
    minutes = int(getattr(strategy_config, "signal_post_close_cooldown_win_minutes", 30))
    return "POST_CLOSE_WIN", max(0, minutes)


def _resolve_canary_diagnostic_symbols(strategy_config) -> set[str]:
    """
    Build normalized canary symbol set from strategy canary allowlists.
    """
    symbols: list[str] = []
    symbols.extend(getattr(strategy_config, "signal_cooldown_canary_symbols", []) or [])
    symbols.extend(getattr(strategy_config, "fvg_min_size_pct_canary_symbols", []) or [])
    return {_normalize_symbol_key(s) for s in symbols if s}


def _build_4h_warmup_skip_diagnostic(
    strategy_config,
    symbol: str,
    futures_symbol: Optional[str],
    stage_b_reason: str,
    candles_4h: List[Candle],
    required_candles: int,
    decision_tf: str,
) -> Optional[Dict[str, Any]]:
    """
    Build canary-only warmup diagnostic payload for stage-B insufficient-candle skips.
    """
    canary_symbols = _resolve_canary_diagnostic_symbols(strategy_config)
    if not canary_symbols:
        return None
    normalized_symbol = _normalize_symbol_key(symbol)
    if normalized_symbol not in canary_symbols:
        return None

    reason = str(stage_b_reason or "")
    if not (reason.startswith(f"candles_{decision_tf}=") and "<" in reason):
        return None

    latest = candles_4h[-1].timestamp if candles_4h else None
    return {
        "symbol": symbol,
        "normalized_symbol": normalized_symbol,
        "resolved_futures_symbol": futures_symbol,
        "candles_4h_count": len(candles_4h),
        "required_candles_4h": int(required_candles),
        "last_4h_ts": latest.isoformat() if isinstance(latest, datetime) else None,
        "skip_reason": "insufficient_4h_history",
        "stage_b_reason": reason,
        "is_canary": True,
    }


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
        from unittest.mock import Mock, MagicMock
        
        # Check if we're in a test environment
        is_test = (
            "pytest" in sys.modules or
            "PYTEST_CURRENT_TEST" in os.environ or
            any("test" in path.lower() for path in sys.path if isinstance(path, str))
        )
        
        if not is_test:
            # Production mode - verify no mocks are being used
            # Informational: confirms runtime is not contaminated by test harness.
            logger.info(
                "PRODUCTION_MODE_VERIFICATION",
                pytest_in_modules="pytest" in sys.modules,
                pytest_env=os.getenv("PYTEST_CURRENT_TEST"),
                sys_path_test_dirs=[p for p in sys.path if isinstance(p, str) and "test" in p.lower()],
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
            breaker_failure_threshold=getattr(config.exchange, "circuit_breaker_failure_threshold", 5),
            breaker_rate_limit_threshold=getattr(config.exchange, "circuit_breaker_rate_limit_threshold", 2),
            breaker_cooldown_seconds=getattr(config.exchange, "circuit_breaker_cooldown_seconds", 60.0),
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
            futures_symbols=config.exchange.futures_markets
        )
        
        from src.storage.repository import record_event
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
        self.risk_manager = RiskManager(config.risk, liquidity_filters=config.liquidity_filters, event_recorder=record_event)
        from src.execution.instrument_specs import InstrumentSpecRegistry
        self.instrument_spec_registry = InstrumentSpecRegistry(
            get_instruments_fn=self.client.get_futures_instruments,
            cache_ttl_seconds=getattr(config.exchange, "instrument_spec_cache_ttl_seconds", 12 * 3600),
            ccxt_exchange=self.client.futures_exchange if hasattr(self.client, 'futures_exchange') else None,
        )
        self.futures_adapter = FuturesAdapter(
            self.client,
            position_size_is_notional=config.exchange.position_size_is_notional,
            instrument_spec_registry=self.instrument_spec_registry,
        )
        
        # Store latest futures tickers for mapping (updated each tick)
        self.latest_futures_tickers: Optional[Dict[str, Decimal]] = None
        
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
        self._last_discovery_error_log_time: Optional[datetime] = None

        # Auction mode allocator (if enabled)
        self.auction_allocator = None
        self.auction_signals_this_tick = []  # Collect signals for auction mode

        # Churn tracking: populated by auction_runner, consumed by winner churn monitor
        # Dict[symbol, list[datetime]] — timestamps when symbol won the auction
        self._auction_win_log: Dict[str, list] = {}
        # Dict[symbol, datetime] — timestamp of last successful entry per symbol
        self._auction_entry_log: Dict[str, datetime] = {}
        # Dict[symbol, {"reason": str, "at": datetime}] — last known open blocking cause.
        self._auction_open_block_reason_by_symbol: Dict[str, Dict[str, Any]] = {}
        # Rebalancer cooldown tracking: symbol -> last cycle where a trim executed
        self._last_trim_cycle_by_symbol: Dict[str, int] = {}
        # Strategic no-signal persistence tracking (auction strategic closes only)
        self._auction_no_signal_cycles: int = 0

        # Signal cooldown: prevent the same signal from firing repeatedly for the same symbol.
        # Key: symbol, Value: (signal_type, structure_timestamp, cooldown_until)
        # A signal is considered "same" if it has the same symbol + signal_type + structure_timestamp.
        self._signal_cooldown: Dict[str, datetime] = {}  # symbol -> cooldown expiry
        self._signal_cooldown_hours: float = float(getattr(config.strategy, "signal_cooldown_hours", 4.0))
        
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
        
        self._last_partial_close_at: Optional[datetime] = None
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
                on_partial_close=lambda _: setattr(self, "_last_partial_close_at", datetime.now(timezone.utc)),
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
        
        self.last_trace_log: Dict[str, datetime] = {} # Dashboard update throttling
        self.last_account_sync = datetime.min.replace(tzinfo=timezone.utc)
        self.last_maintenance_run = datetime.min.replace(tzinfo=timezone.utc)
        self.last_data_maintenance = datetime.min.replace(tzinfo=timezone.utc)
        self.last_recon_time = datetime.min.replace(tzinfo=timezone.utc)
        self.last_metrics_emit = datetime.min.replace(tzinfo=timezone.utc)
        self.ticks_since_emit = 0
        self.signals_since_emit = 0
        self.last_fetch_latency_ms: Optional[int] = None
        self.db_pruner = DatabasePruner()
        
        # TP Backfill cooldown tracking (symbol -> last_backfill_time)
        self.tp_backfill_cooldowns: Dict[str, datetime] = {}
        
        # Coin processing tracking
        self.coin_processing_stats: Dict[str, Dict] = {}  # Track processing stats per coin
        self.last_status_summary = datetime.min.replace(tzinfo=timezone.utc)
        
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
             logger.info("Coin Universe Enabled (V3 - dynamic tier classification)", 
                        market_count=len(self.markets))
             
        # Update Data Acquisition with full list
        self.data_acq = DataAcquisition(
            self.client,
            spot_symbols=self.markets,
            futures_symbols=config.exchange.futures_markets
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
            logger.warning("Failed to initialize ProductionHardeningLayer", error=str(e), error_type=type(e).__name__)
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
        
        logger.info("Live Trading initialized", 
                   markets=config.exchange.futures_markets,
                   state_machine_v2=self.use_state_machine_v2,
                   hardening_enabled=self.hardening is not None)

    def _market_symbols(self) -> List[str]:
        """Return filtered spot symbols -- delegates to coin_processor module."""
        from src.live.coin_processor import market_symbols
        return market_symbols(self)

    def _get_static_tier(self, symbol: str) -> Optional[str]:
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
        import time
        from src.storage.repository import record_event
        
        # 0. Record Startup Event
        try:
            record_event("SYSTEM_STARTUP", "system", {
                "version": self.config.system.version,
                "pid": os.getpid(),
                "mode": "LiveTradingEngine"
            })
        except (OperationalError, DataError, OSError) as e:
            logger.error("Failed to record startup event", error=str(e), error_type=type(e).__name__)
        
        # Smoke Mode / Local Dev Limits
        max_loops = int(os.getenv("MAX_LOOPS", "-1"))
        run_seconds = int(os.getenv("RUN_SECONDS", "-1"))
        start_time = time.time()
        loop_count = 0
        is_smoke_mode = max_loops > 0 or run_seconds > 0
        
        logger.info("Starting run loop", 
                   max_loops=max_loops if max_loops > 0 else "unlimited",
                   run_seconds=run_seconds if run_seconds > 0 else "unlimited",
                   dry_run=self.config.system.dry_run,
                   smoke_mode=is_smoke_mode)

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
            # 1. Initialize Client (INITIALIZING phase)
            logger.info("Initializing Kraken client...")
            await self.client.initialize()
            
            # 1.5 Initial Market Discovery
            if self.config.exchange.use_market_discovery:
                logger.info("Performing initial market discovery...")
                await self._update_market_universe()
                self.last_discovery_time = datetime.now(timezone.utc)
            else:
                self.last_discovery_time = datetime.min.replace(tzinfo=timezone.utc)

            # Startup banner: config flags and universe size (same log sink)
            _recon_cfg = getattr(self.config, "reconciliation", None)
            _auction = getattr(self.config.risk, "auction_mode_enabled", False)
            _recon_en = getattr(_recon_cfg, "reconcile_enabled", True) if _recon_cfg else True
            _shock = getattr(self.config.risk, "shock_guard_enabled", False)
            logger.info(
                "STARTUP_BANNER",
                auction_enabled=_auction,
                reconcile_enabled=_recon_en,
                shock_guard_enabled=bool(self.shock_guard or _shock),
                universe_size=len(self._market_symbols()),
            )

            # 1.6 Startup: ensure all monitored coins have DECISION_TRACE (dashboard coverage)
            try:
                await ensure_all_coins_have_traces(self._market_symbols())
            except (OperationalError, DataError, OSError) as e:
                logger.error("Startup trace validation failed", error=str(e), error_type=type(e).__name__)

            # ===== PHASE: INITIALIZING → SYNCING =====
            self._startup_sm.advance_to(StartupPhase.SYNCING, reason="client initialized, market discovered")

            # 2. Sync State (skip in dry run if no keys)
            if self.config.system.dry_run and not self.client.has_valid_futures_credentials():
                 logger.warning("Dry Run Mode: No Futures credentials found. Skipping account sync.")
            else:
                # Sync Account
                try:
                    await self._sync_account_state()
                    await self._sync_positions()
                    await self.executor.sync_open_orders()
                except (OperationalError, DataError) as e:
                    logger.error("Initial sync failed", error=str(e), error_type=type(e).__name__)
                    if not self.config.system.dry_run:
                        raise
            
            # 2.5 Position State Machine V2 Startup Recovery
            if self.use_state_machine_v2 and self.execution_gateway:
                try:
                    logger.info("Starting Position State Machine V2 recovery...")
                    await self.execution_gateway.startup()
                    logger.info("Position State Machine V2 recovery complete",
                               active_positions=len(self.position_registry.get_all_active()) if self.position_registry else 0)
                except (OperationalError, DataError) as e:
                    logger.error("Position State Machine V2 startup failed", error=str(e), error_type=type(e).__name__)

            # ===== PHASE: SYNCING → RECONCILING =====
            self._startup_sm.advance_to(StartupPhase.RECONCILING, reason="account/positions synced")

            # 2.6 Startup takeover & protect (V2 authoritative pass)
            # In V2 mode, do not use the legacy Reconciler (DB-only) as the source of truth.
            if (
                self.use_state_machine_v2
                and self.execution_gateway
                and not (self.config.system.dry_run and not self.client.has_valid_futures_credentials())
            ):
                _recon_cfg = getattr(self.config, "reconciliation", None)
                if _recon_cfg and getattr(_recon_cfg, "reconcile_enabled", True):
                    try:
                        from src.execution.production_takeover import ProductionTakeover, TakeoverConfig
                        takeover = ProductionTakeover(
                            self.execution_gateway,
                            TakeoverConfig(
                                takeover_stop_pct=Decimal(str(os.getenv("TAKEOVER_STOP_PCT", "0.02"))),
                                stop_replace_atomically=True,
                                dry_run=bool(self.config.system.dry_run),
                            ),
                        )
                        logger.critical("Running startup takeover (V2)...")
                        stats = await takeover.execute_takeover()
                        logger.critical("Startup takeover complete", **stats)
                        self.last_recon_time = datetime.now(timezone.utc)
                    except (OperationalError, DataError) as ex:
                        logger.critical("Startup takeover failed", error=str(ex), exc_info=True)
                        if not self.config.system.dry_run:
                            raise

            # 2.6a Retry trade recording for positions closed before last restart
            if self.use_state_machine_v2 and self.execution_gateway:
                try:
                    retried = await self.execution_gateway.retry_unrecorded_trades()
                    if retried > 0:
                        logger.info("Startup trade recording retry recorded trades", count=retried)
                except (OperationalError, DataError) as e:
                    logger.error("Startup trade recording retry failed", error=str(e), error_type=type(e).__name__)

            # 2.6 PositionProtectionMonitor (Invariant K) - periodic check when V2 live
            if (
                self.use_state_machine_v2
                and self.execution_gateway
                and self.position_registry
            ):
                try:
                    cfg = SafetyConfig()
                    enforcer = ProtectionEnforcer(self.client, cfg)
                    self._protection_monitor = PositionProtectionMonitor(
                        self.client, self.position_registry, enforcer,
                        persistence=self.position_persistence,
                    )
                    self._protection_task = asyncio.create_task(
                        self._run_protection_checks(interval_seconds=30)
                    )
                    logger.info("PositionProtectionMonitor started (interval=30s)")
                except (ValueError, TypeError, RuntimeError) as e:
                    logger.error("Failed to start PositionProtectionMonitor", error=str(e), error_type=type(e).__name__)

            # 2.6b Order-status polling: detect entry fills, trigger PLACE_STOP (SL/TP)
            if self.use_state_machine_v2 and self.execution_gateway:
                try:
                    self._order_poll_task = asyncio.create_task(
                        self._run_order_polling(interval_seconds=12)
                    )
                    logger.info("Order-status polling started (interval=12s)")
                except (ValueError, TypeError, RuntimeError) as e:
                    logger.error("Failed to start order poller", error=str(e), error_type=type(e).__name__)

            # 2.6c Daily P&L summary (runs once per day at midnight UTC)
            try:
                self._daily_summary_task = asyncio.create_task(
                    self._run_daily_summary()
                )
                logger.info("Daily summary task started")
            except (ValueError, TypeError, RuntimeError) as e:
                logger.error("Failed to start daily summary task", error=str(e), error_type=type(e).__name__)

            # 2.6c.2 Spot DCA (daily scheduled spot purchases)
            try:
                self._spot_dca_task = asyncio.create_task(
                    self._run_spot_dca()
                )
                logger.info("Spot DCA task started")
            except (ValueError, TypeError, RuntimeError) as e:
                logger.error("Failed to start spot DCA task", error=str(e), error_type=type(e).__name__)

            # 2.6c.3 WebSocket candle feed — deferred to AFTER DB hydration (step 3)
            # to prevent WS candles from poisoning the merge logic.

            # 2.6c.4 Periodic instrument spec refresh (prevents stale cache from blocking new entries)
            try:
                self._spec_refresh_task = asyncio.create_task(
                    self._run_spec_refresh(interval_seconds=4 * 3600)
                )
                logger.info("Instrument spec refresh task started (interval=4h)")
            except (ValueError, TypeError, RuntimeError) as e:
                logger.error("Failed to start spec refresh task", error=str(e), error_type=type(e).__name__)

            # 2.6d Runtime regression monitors (trade starvation + winner churn)
            try:
                self._starvation_monitor_task = asyncio.create_task(
                    self._run_trade_starvation_monitor(interval_seconds=300)
                )
                logger.info("Trade starvation monitor started (interval=300s)")
            except (ValueError, TypeError, RuntimeError) as e:
                logger.error("Failed to start trade starvation monitor", error=str(e), error_type=type(e).__name__)

            try:
                self._churn_monitor_task = asyncio.create_task(
                    self._run_winner_churn_monitor(interval_seconds=300)
                )
                logger.info("Winner churn monitor started (interval=300s)")
            except (ValueError, TypeError, RuntimeError) as e:
                logger.error("Failed to start winner churn monitor", error=str(e), error_type=type(e).__name__)

            try:
                self._trade_recording_monitor_task = asyncio.create_task(
                    self._run_trade_recording_monitor(interval_seconds=300)
                )
                logger.info("Trade recording invariant monitor started (interval=300s)")
            except (ValueError, TypeError, RuntimeError) as e:
                logger.error("Failed to start trade recording monitor", error=str(e), error_type=type(e).__name__)

            # 2.6e Telegram command handler (/status, /positions, /help)
            try:
                from src.monitoring.telegram_bot import TelegramCommandHandler
                self._telegram_handler = TelegramCommandHandler(
                    data_provider=self._get_system_status
                )
                self._telegram_cmd_task = asyncio.create_task(
                    self._telegram_handler.run()
                )
                logger.info("Telegram command handler started")
            except (ValueError, TypeError, RuntimeError) as e:
                logger.error("Failed to start Telegram command handler", error=str(e), error_type=type(e).__name__)

            # 3. Fast Startup - Load candles
            logger.info("Loading candles from database...")
            try:
                # 3. Fast Startup - Load candles via Manager
                await self.candle_manager.initialize(self._market_symbols())
            except (OperationalError, DataError) as e:
                logger.error("Failed to hydrate candles", error=str(e), error_type=type(e).__name__)

            # 3.5 Start WebSocket candle feed AFTER DB hydration
            try:
                self._ws_candle_task = asyncio.create_task(
                    self._run_ws_candle_feed()
                )
                logger.info("WebSocket candle feed task started (post-hydration)")
            except (ValueError, TypeError, RuntimeError) as e:
                logger.error("Failed to start WS candle feed task", error=str(e), error_type=type(e).__name__)

            # 4. Start Data Acquisition
            await self.data_acq.start()
            
            # ===== PHASE: RECONCILING → READY =====
            # CRITICAL: READY must be set BEFORE first tick.
            # No trading actions (including self-heal / ShockGuard) may run before READY.
            self._startup_sm.advance_to(StartupPhase.READY, reason="all startup steps complete")
            logger.info(
                "STARTUP_COMPLETE",
                startup_epoch=self._startup_sm.startup_epoch.isoformat() if self._startup_sm.startup_epoch else None,
                status=self._startup_sm.get_status(),
            )
            
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
                    await self._tick()
                    logger.info("Initial tick completed - runtime state hydrated")
                except (OperationalError, DataError) as e:
                    logger.error("Initial tick failed", error=str(e), error_type=type(e).__name__)
            
            # 4.6. Validate position protection (startup safety gate)
            try:
                await self._validate_position_protection()
            except (OperationalError, DataError) as e:
                logger.error("Position protection validation failed", error=str(e), error_type=type(e).__name__)

            # 5. Main Loop
            while self.active:
            # Check Smoke Mode Limits
                if max_loops > 0 and loop_count >= max_loops:
                    logger.info("Smoke mode: Max loops reached", max_loops=max_loops, loops_completed=loop_count)
                    break
                    
                if run_seconds > 0 and (time.time() - start_time) >= run_seconds:
                    elapsed = time.time() - start_time
                    logger.info("Smoke mode: Run time limit reached", run_seconds=run_seconds, elapsed_seconds=f"{elapsed:.1f}")
                    break
                
                loop_count += 1
                self._last_cycle_count = loop_count

                if self.kill_switch.is_active():
                    # Attempt auto-recovery for margin_critical (the most common false-positive halt)
                    recovered = False
                    if self.kill_switch.reason == KillSwitchReason.MARGIN_CRITICAL:
                        recovered = await self._try_auto_recovery()
                    
                    if not recovered:
                        logger.critical("Kill switch active - pausing loop",
                                       reason=self.kill_switch.reason.value if self.kill_switch.reason else "unknown")
                        await asyncio.sleep(60)
                        continue
                
                # Periodic Market Discovery
                if self.config.exchange.use_market_discovery:
                    now = datetime.now(timezone.utc)
                    elapsed_discovery = (now - self.last_discovery_time).total_seconds()
                    refresh_sec = self.config.exchange.discovery_refresh_hours * 3600
                    
                    if elapsed_discovery >= refresh_sec:
                        await self._update_market_universe()
                        self.last_discovery_time = now
                
                loop_start = datetime.now(timezone.utc)
                cycle_id = f"tick_{loop_count}_{int(loop_start.timestamp())}"

                try:
                    record_event("CYCLE_TICK_BEGIN", "system", {"cycle_id": cycle_id, "loop_count": loop_count})
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
                    logger.critical("INVARIANT VIOLATION in tick — triggering kill switch", error=str(e))
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
                        record_event("CYCLE_TICK_END", "system", {"cycle_id": cycle_id, "loop_count": loop_count})
                    except Exception as e:
                        logger.debug("Failed to record CYCLE_TICK_END event", error=str(e))
                
                self.ticks_since_emit += 1
                # P0.4: Write heartbeat file after each successful tick
                self._write_heartbeat()
                now = datetime.now(timezone.utc)
                if (now - self.last_metrics_emit).total_seconds() >= 60.0:
                    try:
                        record_metrics_snapshot({
                            "last_tick_at": now.isoformat(),
                            "ticks_last_min": self.ticks_since_emit,
                            "signals_last_min": self.signals_since_emit,
                            "markets_count": len(self._market_symbols()),
                            "api_fetch_latency_ms": getattr(self, "last_fetch_latency_ms", None),
                            "coins_futures_fallback_used": self.candle_manager.get_futures_fallback_count(),
                            "orders_per_minute": self.execution_gateway._order_rate_limiter.orders_last_minute,
                            "orders_per_10s": self.execution_gateway._order_rate_limiter.orders_last_10s,
                            "orders_blocked_total": self.execution_gateway._order_rate_limiter.orders_blocked_total,
                        })
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
                        logger.error("Unexpected error in metrics snapshot", error=str(ex), error_type=type(ex).__name__)

                # Periodic reconciliation (positions: system vs exchange)
                _recon_cfg = getattr(self.config, "reconciliation", None)
                if _recon_cfg and getattr(_recon_cfg, "reconcile_enabled", True):
                    recon_interval = getattr(_recon_cfg, "periodic_interval_seconds", 120)
                    run_after_orders = getattr(self, "_reconcile_requested", False)
                    if run_after_orders or (now - self.last_recon_time).total_seconds() >= recon_interval:
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
                            logger.error("Unexpected reconciliation error", error=str(ex), error_type=type(ex).__name__)

                # ===== CYCLE SUMMARY (single log line per tick with key metrics) =====
                now = datetime.now(timezone.utc)
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
                    elif self.hardening and hasattr(self.hardening, 'invariant_monitor'):
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
                    logger.warning("CYCLE_SUMMARY_FAILED", error=str(summary_err), error_type=type(summary_err).__name__)
                except Exception as summary_err:
                    # Bug in summary logic — log but don't crash the loop for it
                    logger.error("CYCLE_SUMMARY_BUG", error=str(summary_err), error_type=type(summary_err).__name__)

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
                    dry_run=self.config.system.dry_run
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
            if getattr(self, "_protection_task", None) and not self._protection_task.done():
                self._protection_task.cancel()
                try:
                    await self._protection_task
                except asyncio.CancelledError:
                    pass
            if getattr(self, "_order_poll_task", None) and not self._order_poll_task.done():
                self._order_poll_task.cancel()
                try:
                    await self._order_poll_task
                except asyncio.CancelledError:
                    pass
            if getattr(self, "_daily_summary_task", None) and not self._daily_summary_task.done():
                self._daily_summary_task.cancel()
                try:
                    await self._daily_summary_task
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
            if getattr(self, "_starvation_monitor_task", None) and not self._starvation_monitor_task.done():
                self._starvation_monitor_task.cancel()
                try:
                    await self._starvation_monitor_task
                except asyncio.CancelledError:
                    pass
            if getattr(self, "_churn_monitor_task", None) and not self._churn_monitor_task.done():
                self._churn_monitor_task.cancel()
                try:
                    await self._churn_monitor_task
                except asyncio.CancelledError:
                    pass
            if getattr(self, "_trade_recording_monitor_task", None) and not self._trade_recording_monitor_task.done():
                self._trade_recording_monitor_task.cancel()
                try:
                    await self._trade_recording_monitor_task
                except asyncio.CancelledError:
                    pass
            await self.data_acq.stop()
            await self.client.close()
            # Persist data quality state so SUSPENDED/DEGRADED symbols survive restart
            self.data_quality_tracker.force_persist()
            logger.info("Live trading shutdown complete")

    async def _run_order_polling(self, interval_seconds: int = 12) -> None:
        """Poll pending entry order status -- delegates to health_monitor module."""
        from src.live.health_monitor import run_order_polling
        await run_order_polling(self, interval_seconds)

    async def _run_protection_checks(self, interval_seconds: int = 30) -> None:
        """V2 protection monitor loop -- delegates to health_monitor module."""
        from src.live.health_monitor import run_protection_checks
        await run_protection_checks(self, interval_seconds)

    async def _get_system_status(self) -> dict:
        """System status for Telegram -- delegates to health_monitor module."""
        from src.live.health_monitor import get_system_status
        return await get_system_status(self)

    async def _run_daily_summary(self) -> None:
        """Daily P&L summary at midnight UTC -- delegates to health_monitor module."""
        from src.live.health_monitor import run_daily_summary
        await run_daily_summary(self)

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
                logger.warning("Periodic spec refresh failed (will retry)", error=str(e), error_type=type(e).__name__)

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

    async def _run_trade_starvation_monitor(self, interval_seconds: int = 300) -> None:
        """Trade starvation sentinel -- delegates to health_monitor module."""
        from src.live.health_monitor import run_trade_starvation_monitor
        await run_trade_starvation_monitor(self, interval_seconds)

    async def _run_winner_churn_monitor(self, interval_seconds: int = 300) -> None:
        """Winner churn sentinel -- delegates to health_monitor module."""
        from src.live.health_monitor import run_winner_churn_monitor
        await run_winner_churn_monitor(self, interval_seconds)

    async def _run_trade_recording_monitor(self, interval_seconds: int = 300) -> None:
        """Trade recording invariant monitor -- delegates to health_monitor module."""
        from src.live.health_monitor import run_trade_recording_monitor
        await run_trade_recording_monitor(self, interval_seconds)

    def _convert_to_position(self, data: Dict) -> Position:
        """Convert raw exchange position dict to Position domain object -- delegates to exchange_sync module."""
        from src.live.exchange_sync import convert_to_position
        return convert_to_position(self, data)

    async def _sync_positions(self, raw_positions: Optional[List[Dict]] = None) -> List[Dict]:
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
            _rec("CYCLE_TICK_CRASH", "system", {
                "cycle_id": cycle_id,
                "exception_type": type(exc).__name__,
                "exception_msg": str(exc)[:500],
            })
        except Exception as db_err:
            logger.warning("Failed to record tick crash to DB", error=str(db_err))
        try:
            from src.runtime.crash_capture import write_crash_log
            write_crash_log(exc, context="tick", cycle_id=cycle_id)
        except Exception as log_err:
            logger.warning("Failed to write crash log file", error=str(log_err))

    async def _validate_position_protection(self):
        """Startup position protection validation -- delegates to health_monitor module."""
        from src.live.health_monitor import validate_position_protection
        await validate_position_protection(self)

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

        # 0. Kill Switch Check (HIGHEST PRIORITY)
        # P0.3: SAFE_HOLD semantics — kill switch active does NOT auto-flatten.
        # Only emergency=True activation (via KillSwitch.activate(emergency=True))
        # triggers position closure. That path runs inside activate() itself.
        #
        # On tick with kill switch active, we enter SAFE_HOLD:
        #   1. Cancel non-SL orders (preserve stop losses)
        #   2. Verify stops exist for open positions
        #   3. Do NOT market-close positions
        #   4. Require explicit operator action to flatten
        #
        # Exception: Recent (<2 min) EMERGENCY_RUNTIME reasons may auto-flatten.
        ks = self.kill_switch
        
        if ks.is_active():
            # Determine if this is a recent emergency that should auto-flatten
            should_auto_flatten = False
            if ks.reason and ks.reason.allows_auto_flatten_on_startup and ks.activated_at:
                age_seconds = (datetime.now(timezone.utc) - ks.activated_at).total_seconds()
                if age_seconds < 120:  # < 2 minutes = recent emergency
                    should_auto_flatten = True
                    logger.critical(
                        "Kill switch SAFE_HOLD: recent emergency — allowing auto-flatten",
                        reason=ks.reason.value,
                        age_seconds=f"{age_seconds:.0f}",
                    )

            if should_auto_flatten:
                # EMERGENCY path: cancel all + close positions (original behavior)
                logger.critical("Kill switch EMERGENCY: cancelling orders and closing positions")
                try:
                    cancelled = await self.client.cancel_all_orders()
                    logger.info(f"Kill switch: Cancelled {len(cancelled)} orders")
                except InvariantError:
                    raise
                except OperationalError as e:
                    logger.error("Kill switch: cancel_all transient failure", kill_step="cancel_all", error=str(e), error_type=type(e).__name__)
                except Exception as e:
                    logger.exception("Kill switch: unexpected error in cancel_all", kill_step="cancel_all", error=str(e), error_type=type(e).__name__)
                    raise

                try:
                    positions = await self.client.get_all_futures_positions()
                    for pos in positions:
                        if pos.get('size', 0) != 0:
                            symbol = pos.get('symbol')
                            try:
                                await self.client.close_position(symbol)
                                logger.warning(f"Kill switch: Emergency closed position for {symbol}")
                            except InvariantError:
                                raise
                            except OperationalError as e:
                                logger.error("Kill switch: close_position transient failure", kill_step="close_position", symbol=symbol, error=str(e), error_type=type(e).__name__)
                            except Exception as e:
                                logger.exception("Kill switch: unexpected error closing position", kill_step="close_position", symbol=symbol, error=str(e), error_type=type(e).__name__)
                                raise
                except InvariantError:
                    raise
                except OperationalError as e:
                    logger.error("Kill switch: close_all transient failure", kill_step="close_all", error=str(e), error_type=type(e).__name__)
                except Exception as e:
                    logger.exception("Kill switch: unexpected error in close_all", kill_step="close_all", error=str(e), error_type=type(e).__name__)
                    raise
            else:
                # SAFE_HOLD path: cancel non-SL orders, verify stops, do NOT flatten
                logger.critical(
                    "Kill switch SAFE_HOLD: preserving positions + stops, refusing new entries",
                    reason=ks.reason.value if ks.reason else "unknown",
                    activated_at=ks.activated_at.isoformat() if ks.activated_at else "unknown",
                )
                try:
                    cancelled, preserved_sls = await ks._cancel_non_sl_orders()
                    logger.info(
                        "Kill switch SAFE_HOLD: order cleanup done",
                        cancelled_non_sl=cancelled,
                        preserved_stop_losses=preserved_sls,
                    )
                except InvariantError:
                    raise
                except OperationalError as e:
                    logger.error("Kill switch SAFE_HOLD: cancel transient failure", error=str(e), error_type=type(e).__name__)
                except Exception as e:
                    logger.exception("Kill switch SAFE_HOLD: unexpected error in cancel", error=str(e), error_type=type(e).__name__)
                    raise

            # Stop processing (no new entries while kill switch is active)
            return
        
        # 0.1 Order Timeout Monitoring (CRITICAL: Check first)
        try:
            cancelled_count = await self.executor.check_order_timeouts()
            if cancelled_count > 0:
                logger.warning("Cancelled expired orders", count=cancelled_count)
        except (OperationalError, DataError) as e:
            logger.error("Failed to check order timeouts", error=str(e), error_type=type(e).__name__)
        
        # 1. Check Data Health
        if not self.data_acq.is_healthy():
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
                position_objs = [self._convert_to_position(p) for p in all_raw_positions if p.get('size', 0) != 0]
                
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
            logger.error("Failed to cleanup orphan orders", error=str(e), error_type=type(e).__name__)
            # Don't return - continue with trading loop

        # 3. Batch Data Fetching (Optimization)
        try:
            import time
            _t0 = time.perf_counter()
            market_symbols = self._market_symbols()
            # Health gate: pause new entries when candle health below threshold
            total_coins = len(market_symbols)
            coins_with_sufficient_candles = sum(
                1 for s in market_symbols if len(self.candle_manager.get_candles(s, "15m")) >= 50
            )
            min_healthy = getattr(self.config.data, "min_healthy_coins", 30)
            min_ratio = getattr(self.config.data, "min_health_ratio", 0.25)
            if total_coins > 0:
                ratio = coins_with_sufficient_candles / total_coins
                # When universe is smaller than min_healthy, require all coins to have data (ratio 1.0)
                effective_min = min(min_healthy, total_coins)
                if coins_with_sufficient_candles < effective_min or ratio < min_ratio:
                    self.trade_paused = True
                    logger.critical(
                        "TRADING PAUSED: candle health insufficient",
                        coins_with_sufficient_candles=coins_with_sufficient_candles,
                        total=total_coins,
                        min_healthy_coins=min_healthy,
                        effective_min=effective_min,
                        min_health_ratio=min_ratio,
                    )
                else:
                    self.trade_paused = False
            else:
                self.trade_paused = False
            map_spot_tickers = await self.client.get_spot_tickers_bulk(market_symbols)
            map_futures_tickers = await self.client.get_futures_tickers_bulk()
            # Full FuturesTicker objects for data sanity gate (bid/ask/volume).
            # None => bulk fetch failed => Stage A skipped (fail-open).
            try:
                map_futures_tickers_full = await self.client.get_futures_tickers_bulk_full()
            except (OperationalError, DataError) as _e:
                logger.warning("get_futures_tickers_bulk_full failed; sanity gate will skip Stage A", error=str(_e), error_type=type(_e).__name__)
                map_futures_tickers_full = None
            _t1 = time.perf_counter()
            self.last_fetch_latency_ms = round((_t1 - _t0) * 1000)
            map_positions = {p["symbol"]: p for p in all_raw_positions}
            recent_close_by_symbol: Dict[str, Dict[str, Any]] = {}
            if bool(getattr(self.config.strategy, "signal_post_close_cooldown_enabled", True)):
                close_lookback_hours = int(
                    getattr(self.config.strategy, "signal_post_close_lookback_hours", 24)
                )
                try:
                    recent_trades = await asyncio.to_thread(
                        get_trades_since,
                        datetime.now(timezone.utc) - timedelta(hours=close_lookback_hours),
                    )
                    for trade in recent_trades:
                        symbol_key = _normalize_symbol_key(getattr(trade, "symbol", ""))
                        if not symbol_key:
                            continue
                        last_close_at = getattr(trade, "exited_at", None)
                        if not isinstance(last_close_at, datetime):
                            continue
                        existing = recent_close_by_symbol.get(symbol_key)
                        if existing and existing["last_close_at"] >= last_close_at:
                            continue
                        last_close_reason = str(getattr(trade, "exit_reason", "") or "")
                        cooldown_kind, cooldown_minutes = _resolve_post_close_cooldown_kind_and_minutes(
                            last_close_reason,
                            self.config.strategy,
                        )
                        recent_close_by_symbol[symbol_key] = {
                            "last_close_at": last_close_at,
                            "last_close_reason": last_close_reason,
                            "cooldown_kind": cooldown_kind,
                            "cooldown_minutes": cooldown_minutes,
                        }
                except (OperationalError, DataError, ValueError) as e:
                    logger.warning(
                        "Post-close cooldown source unavailable; proceeding without post-close gating",
                        error=str(e),
                        error_type=type(e).__name__,
                    )
            
            # Store latest futures tickers for use in _handle_signal and other call sites
            self.latest_futures_tickers = map_futures_tickers
            # Also store on executor for use in execute_signal
            self.executor.latest_futures_tickers = map_futures_tickers
            # Update adapter cache for use when futures_tickers not explicitly passed
            self.futures_adapter.update_cached_futures_tickers(map_futures_tickers)
            
            # Ensure instrument specs are loaded (used to decide tradability and size/leverage rules).
            # This is TTL-cached; refresh() is a cheap no-op when not stale.
            if getattr(self, "instrument_spec_registry", None):
                try:
                    await self.instrument_spec_registry.refresh()
                except (OperationalError, DataError) as e:
                    logger.warning("InstrumentSpecRegistry refresh failed (non-fatal)", error=str(e), error_type=type(e).__name__)
            
            # ShockGuard: Evaluate shock conditions and update state
            if self.shock_guard:
                # CRITICAL: Deduplicate futures tickers to one canonical symbol per asset
                # map_futures_tickers contains aliases (PI_*, PF_*, BASE/USD:USD, BASE/USD)
                # We need to pick one canonical format per asset to avoid false triggers
                def extract_base(symbol: str) -> Optional[str]:
                    """Extract base currency from symbol."""
                    for prefix in ["PI_", "PF_", "FI_"]:
                        if symbol.startswith(prefix):
                            symbol = symbol[len(prefix):]
                    # IMPORTANT: match longer suffixes first ("/USD:USD" must not be reduced by "USD").
                    for suffix in ["/USD:USD", "/USD", "USD"]:
                        if symbol.endswith(suffix):
                            symbol = symbol[:-len(suffix)]
                    return symbol.rstrip(":/") if symbol else None
                
                # Build canonical mark prices (prefer CCXT unified BASE/USD:USD, else PF_*)
                canonical_mark_prices = {}
                base_to_symbol = {}  # Track which symbol we chose per base
                for symbol, mark_price in map_futures_tickers.items():
                    base = extract_base(symbol)
                    if not base:
                        continue
                    # Prefer CCXT unified format, else PF_ format
                    if base not in base_to_symbol:
                        base_to_symbol[base] = symbol
                        canonical_mark_prices[symbol] = mark_price
                    elif "/USD:USD" in symbol and "/USD:USD" not in base_to_symbol[base]:
                        # Upgrade to CCXT unified if available
                        canonical_mark_prices.pop(base_to_symbol[base], None)
                        base_to_symbol[base] = symbol
                        canonical_mark_prices[symbol] = mark_price
                    elif symbol.startswith("PF_") and not base_to_symbol[base].startswith("PF_") and "/USD:USD" not in base_to_symbol[base]:
                        # Use PF_ as fallback
                        canonical_mark_prices.pop(base_to_symbol[base], None)
                        base_to_symbol[base] = symbol
                        canonical_mark_prices[symbol] = mark_price
                
                # Spot prices already use canonical format (spot symbols)
                spot_prices_dict = {}
                for symbol, ticker in map_spot_tickers.items():
                    if isinstance(ticker, dict) and "last" in ticker:
                        spot_prices_dict[symbol] = Decimal(str(ticker["last"]))
                
                # Evaluate shock conditions with canonical symbols only
                shock_detected = self.shock_guard.evaluate(
                    mark_prices=canonical_mark_prices,
                    spot_prices=spot_prices_dict if spot_prices_dict else None,
                )
                
                # Run exposure reduction if shock active
                if self.shock_guard.shock_mode_active:
                    # Get positions as Position objects
                    positions_list = []
                    liquidation_prices_dict = {}
                    # Build mark prices keyed by position symbols (exchange symbols like PF_*)
                    # Positions use exchange symbols, so we need mark prices for those symbols
                    mark_prices_for_positions = {}
                    for pos_data in all_raw_positions:
                        pos = self._convert_to_position(pos_data)
                        positions_list.append(pos)
                        liquidation_prices_dict[pos.symbol] = pos.liquidation_price
                        
                        # Get mark price for this position symbol (try multiple formats)
                        pos_symbol = pos.symbol
                        mark_price = None
                        # Try direct lookup first
                        if pos_symbol in map_futures_tickers:
                            mark_price = map_futures_tickers[pos_symbol]
                        else:
                            # Try to find any alias for this symbol
                            for ticker_symbol, ticker_price in map_futures_tickers.items():
                                # Extract base from both and compare
                                pos_base = extract_base(pos_symbol)
                                ticker_base = extract_base(ticker_symbol)
                                if pos_base and ticker_base and pos_base == ticker_base:
                                    mark_price = ticker_price
                                    break
                        
                        # Fallback to position data if available
                        if not mark_price:
                            mark_price = Decimal(str(pos_data.get("markPrice", pos_data.get("mark_price", pos_data.get("entryPrice", 0)))))
                        
                        mark_prices_for_positions[pos_symbol] = mark_price
                    
                    # Get exposure reduction actions
                    actions = self.shock_guard.get_exposure_reduction_actions(
                        positions=positions_list,
                        mark_prices=mark_prices_for_positions,
                        liquidation_prices=liquidation_prices_dict,
                    )
                    
                    # Execute actions
                    for action_item in actions:
                        try:
                            symbol = action_item.symbol
                            if action_item.action.value == "CLOSE":
                                logger.warning(
                                    "ShockGuard: Closing position (emergency)",
                                    symbol=symbol,
                                    buffer_pct=float(action_item.buffer_pct),
                                    reason=action_item.reason,
                                )
                                await self.client.close_position(symbol)
                            elif action_item.action.value == "TRIM":
                                # Get current position size
                                pos_data = map_positions.get(symbol)
                                if pos_data:
                                    # Get position size - check if system uses notional or contracts
                                    current_size_raw = Decimal(str(pos_data.get("size", 0)))
                                    
                                    # Determine if size is in contracts or notional
                                    # If position_size_is_notional is True, size is USD notional
                                    # If False, size is in contracts
                                    position_size_is_notional = getattr(
                                        self.config.exchange, "position_size_is_notional", False
                                    )
                                    
                                    if position_size_is_notional:
                                        # Size is in USD notional - trim by 50%
                                        trim_notional = current_size_raw * Decimal("0.5")
                                        # Convert notional to contracts using proper adapter method
                                        # Use mark_prices_for_positions which is keyed by position symbols
                                        mark_price = mark_prices_for_positions.get(symbol)
                                        if not mark_price or mark_price <= 0:
                                            # Fallback to position data
                                            mark_price = Decimal(str(pos_data.get("markPrice", pos_data.get("mark_price", pos_data.get("entryPrice", 1)))))
                                        if mark_price > 0:
                                            # Use adapter's notional_to_contracts method for proper conversion
                                            trim_size_contracts = self.futures_adapter.notional_to_contracts(
                                                trim_notional, mark_price
                                            )
                                        else:
                                            logger.error("ShockGuard: Cannot trim - invalid mark price", symbol=symbol)
                                            continue
                                    else:
                                        # Size is in contracts - trim by 50%
                                        trim_size_contracts = current_size_raw * Decimal("0.5")
                                    
                                    # Determine side for reduce-only order
                                    side_raw = pos_data.get("side", "long").lower()
                                    close_side = "sell" if side_raw == "long" else "buy"
                                    
                                    logger.warning(
                                        "ShockGuard: Trimming position",
                                        symbol=symbol,
                                        buffer_pct=float(action_item.buffer_pct),
                                        current_size=str(current_size_raw),
                                        trim_size_contracts=str(trim_size_contracts),
                                        position_size_is_notional=position_size_is_notional,
                                        reason=action_item.reason,
                                    )
                                    
                                    # Route through gateway (P1.2 — single choke point)
                                    futures_symbol = symbol
                                    trim_result = await self.execution_gateway.place_emergency_order(
                                        symbol=futures_symbol,
                                        side=close_side,
                                        order_type="market",
                                        size=trim_size_contracts,
                                        reduce_only=True,
                                        reason="shockguard_trim",
                                    )
                                    if not trim_result.success:
                                        logger.error(
                                            "ShockGuard: trim order rejected by gateway",
                                            symbol=symbol,
                                            error=trim_result.error,
                                        )
                        except (OperationalError, DataError) as e:
                            logger.error(
                                "ShockGuard: Failed to execute exposure reduction",
                                symbol=action_item.symbol,
                                action=action_item.action.value,
                                error=str(e),
                                error_type=type(e).__name__,
                            )
            
            # 2.4. Fetch open orders once, index by *normalized* symbol (for position hydration)
            # This is critical because positions are PF_* while CCXT orders often use unified symbols (e.g. X/USD:USD).
            from src.data.symbol_utils import normalize_symbol_for_position_match
            orders_by_symbol: Dict[str, List[Dict]] = {}
            try:
                # CRITICAL: Verify client is not a mock before calling
                from unittest.mock import Mock, MagicMock
                import sys
                import os
                is_test = (
                    "pytest" in sys.modules or
                    "PYTEST_CURRENT_TEST" in os.environ or
                    any("test" in path.lower() for path in sys.path if isinstance(path, str))
                )
                if not is_test and (isinstance(self.client, Mock) or isinstance(self.client, MagicMock)):
                    logger.critical("CRITICAL: self.client is a Mock/MagicMock in _tick!")
                    raise RuntimeError("CRITICAL: self.client is a Mock/MagicMock in production")
                
                open_orders = await self.client.get_futures_open_orders()
                for order in open_orders:
                    sym = order.get('symbol')
                    key = normalize_symbol_for_position_match(sym) if sym else ""
                    if key:
                        if key not in orders_by_symbol:
                            orders_by_symbol[key] = []
                        orders_by_symbol[key].append(order)
            except RuntimeError:
                raise  # Re-raise critical errors
            except (OperationalError, DataError) as e:
                logger.warning("Failed to fetch open orders for hydration", error=str(e), error_type=type(e).__name__)
            
            # 2.5. TP Backfill / Reconciliation (after position sync and price data fetch)
            try:
                # Build current prices map for backfill logic (use futures ticker data)
                current_prices_map = {}
                for pos_data in all_raw_positions:
                    symbol = pos_data.get('symbol')
                    if symbol:
                        # map_futures_tickers is Dict[str, Decimal] - mark price directly
                        mark_price = map_futures_tickers.get(symbol)
                        if mark_price:
                            current_prices_map[symbol] = mark_price
                        else:
                            current_prices_map[symbol] = Decimal(str(pos_data.get('markPrice', pos_data.get('mark_price', pos_data.get('entryPrice', 0)))))
                
                # Reconcile stop loss order IDs from exchange FIRST
                # This updates is_protected flag based on actual exchange orders
                await self._reconcile_stop_loss_order_ids(all_raw_positions)
                
                # THEN do TP backfill (which checks is_protected)
                await self._reconcile_protective_orders(all_raw_positions, current_prices_map)

                # Auto-place missing stops for unprotected positions (rate-limited per tick)
                await self._place_missing_stops_for_unprotected(all_raw_positions, max_per_tick=3)
            except (OperationalError, DataError, ValueError) as e:
                logger.exception("TP backfill reconciliation failed", error=str(e), error_type=type(e).__name__)
                # Don't return - continue with trading loop
            symbols_with_spot = len([s for s in market_symbols if s in map_spot_tickers])
            # Use futures_tickers for accurate coverage counting
            symbols_with_futures = len([
                s for s in market_symbols
                if self.futures_adapter.map_spot_to_futures(s, futures_tickers=map_futures_tickers) in map_futures_tickers
            ])
            symbols_with_neither = len([
                s for s in market_symbols
                if s not in map_spot_tickers
                and self.futures_adapter.map_spot_to_futures(s, futures_tickers=map_futures_tickers) not in map_futures_tickers
            ])
            self._last_ticker_with = symbols_with_spot
            self._last_ticker_without = len(market_symbols) - symbols_with_spot
            self._last_futures_count = symbols_with_futures
            if symbols_with_neither > 0 or symbols_with_futures < len(market_symbols):
                self._last_ticker_skip_log = getattr(self, "_last_ticker_skip_log", datetime.min.replace(tzinfo=timezone.utc))
                if (datetime.now(timezone.utc) - self._last_ticker_skip_log).total_seconds() >= 300:
                    logger.warning(
                        "Ticker coverage: spot/futures",
                        total=len(market_symbols),
                        with_spot=symbols_with_spot,
                        with_futures=symbols_with_futures,
                        with_neither=symbols_with_neither,
                        hint="Trading requires futures ticker; ensure bulk API keys match discovery (CCXT vs PF_*).",
                    )
                    self._last_ticker_skip_log = datetime.now(timezone.utc)
        except (OperationalError, DataError) as e:
            logger.error("Failed batch data fetch", error=str(e), error_type=type(e).__name__)
            return

        # 4. Parallel Analysis Loop
        # Semaphore to control concurrency for candle fetching.
        # Most time is I/O-bound (waiting on Kraken API), so higher concurrency is safe.
        sem = asyncio.Semaphore(50)
        analysis_funnel: Dict[str, Any] = {
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
        }

        def _af_inc(key: str, amount: int = 1) -> None:
            analysis_funnel[key] = int(analysis_funnel.get(key, 0) or 0) + amount

        def _af_skip(reason: str) -> None:
            skips = analysis_funnel.setdefault("symbols_skipped_by_reason", {})
            skips[reason] = int(skips.get(reason, 0) or 0) + 1
        
        async def process_coin(spot_symbol: str):
            async with sem:
                try:
                    _af_inc("symbols_analyzed")
                    # Use futures tickers for improved mapping
                    futures_symbol = self.futures_adapter.map_spot_to_futures(spot_symbol, futures_tickers=map_futures_tickers)
                    has_spot = spot_symbol in map_spot_tickers
                    has_futures = futures_symbol in map_futures_tickers
                    
                    # Debug: Log when futures symbol not found
                    if not has_futures and spot_symbol in map_spot_tickers:
                        # Check if similar futures symbols exist
                        similar = [s for s in map_futures_tickers.keys() if spot_symbol.split('/')[0].upper() in s.upper()][:3]
                        logger.debug(
                            "Futures symbol not found for signal",
                            spot_symbol=spot_symbol,
                            mapped_futures=futures_symbol,
                            similar_futures=similar,
                            total_futures_available=len(map_futures_tickers)
                        )
                    
                    if not has_spot and not has_futures:
                        _af_skip("no_ticker_spot_and_futures")
                        return  # Skip if no ticker (spot or futures)

                    if has_spot:
                        spot_ticker = map_spot_tickers[spot_symbol]
                        spot_price = Decimal(str(spot_ticker["last"]))
                    else:
                        spot_price = None
                    # map_futures_tickers is Dict[str, Decimal] - mark price directly
                    mark_price = map_futures_tickers.get(futures_symbol) if has_futures else None
                    if not mark_price:
                        mark_price = spot_price
                    if not mark_price:
                        return
                    if spot_price is None:
                        spot_price = mark_price  # Use futures mark when no spot ticker (futures-only)
                    
                    # Tradability gate:
                    # - Must have a futures ticker
                    # - Must have an instrument spec (otherwise execution will fail with NO_SPEC)
                    has_spec = True
                    if has_futures and getattr(self, "instrument_spec_registry", None):
                        try:
                            has_spec = self.instrument_spec_registry.get_spec(futures_symbol) is not None
                        except (ValueError, TypeError, KeyError, AttributeError):
                            has_spec = False
                    
                    skip_reason: Optional[str] = None
                    if not has_futures:
                        skip_reason = "no_futures_ticker"
                    elif not has_spec:
                        skip_reason = "no_instrument_spec"
                        # Throttle to avoid log spam if a spot market exists but the futures instrument is not tradeable/known.
                        now = datetime.now(timezone.utc)
                        last_map = getattr(self, "_last_no_spec_log", {})
                        last = last_map.get(futures_symbol, datetime.min.replace(tzinfo=timezone.utc))
                        if (now - last).total_seconds() >= 3600:
                            logger.warning(
                                "Signal skipped (no instrument spec)",
                                spot_symbol=spot_symbol,
                                futures_symbol=futures_symbol,
                                reason="NO_SPEC",
                                hint="Futures ticker exists but instrument specs missing; likely delisted/non-tradeable. Skipping to avoid AUCTION_OPEN_REJECTED spam.",
                            )
                            last_map[futures_symbol] = now
                            self._last_no_spec_log = last_map
                    
                    is_tradable = skip_reason is None
                    if is_tradable:
                        _af_inc("eligible_symbols")
                    else:
                        _af_skip(skip_reason or "not_tradable")

                    # --- STAGE A: Futures ticker sanity (pre-I/O) ---
                    # Check spread + volume from already-fetched futures ticker.
                    # If data is garbage, skip candle fetch + signal gen entirely.
                    # Fail-open: if the bulk full ticker fetch failed (None), skip Stage A.
                    if map_futures_tickers_full is not None:
                        futures_ticker_full = map_futures_tickers_full.get(futures_symbol)
                        stage_a = check_ticker_sanity(
                            symbol=spot_symbol,
                            futures_ticker=futures_ticker_full,
                            spot_ticker=map_spot_tickers.get(spot_symbol),
                            thresholds=self.sanity_thresholds,
                        )
                        if not stage_a.passed:
                            _af_skip(f"stage_a_{stage_a.reason or 'failed'}")
                            self.data_quality_tracker.record_result(
                                spot_symbol, passed=False, reason=stage_a.reason,
                            )
                            return

                    # Update Candles (spot first; futures fallback when spot unavailable)
                    await self._update_candles(spot_symbol)
                    
                    # Position Management (V2 State Machine)
                    position_data = map_positions.get(futures_symbol)
                    if position_data:
                        symbol = position_data['symbol']
                        try:
                            v2_actions = self.position_manager_v2.evaluate_position(
                                symbol=symbol,
                                current_price=mark_price,
                                current_atr=None,
                            )
                            if v2_actions:
                                await self.execution_gateway.execute_actions(v2_actions)
                        except InvariantError:
                            raise  # Safety violation — must propagate to kill switch
                        except (OperationalError, DataError) as e:
                            logger.error(
                                "V2 position evaluation failed",
                                symbol=symbol,
                                error=str(e),
                                error_type=type(e).__name__,
                            )
                        except Exception as e:
                            logger.exception(
                                "V2 position evaluation: unexpected error",
                                symbol=symbol,
                                error=str(e),
                                error_type=type(e).__name__,
                            )
                            raise
                            
                    # ShockGuard: Skip signal generation if entries paused
                    if self.shock_guard and self.shock_guard.should_pause_entries():
                        logger.debug(
                            "ShockGuard: Skipping signal generation (entries paused)",
                            symbol=spot_symbol,
                        )
                        return
                    
                    # Signal Generation (SMC)
                    # Use 15m candles (primary timeframe)
                    # NOTE: _update_candles ensures self.candles_15m is populated
                    candles = self.candle_manager.get_candles(spot_symbol, "15m")
                    candle_count = len(candles)
                    
                    # Update processing stats
                    if spot_symbol not in self.coin_processing_stats:
                        self.coin_processing_stats[spot_symbol] = {
                            "processed_count": 0,
                            "last_processed": datetime.min.replace(tzinfo=timezone.utc),
                            "candle_count": 0
                        }
                    
                    prev_count = self.coin_processing_stats[spot_symbol]["candle_count"]
                        
                    self.coin_processing_stats[spot_symbol]["processed_count"] += 1
                    self.coin_processing_stats[spot_symbol]["last_processed"] = datetime.now(timezone.utc)
                    self.coin_processing_stats[spot_symbol]["candle_count"] = candle_count
                    
                    if prev_count > 50 and candle_count == 0:
                        logger.critical("Data Depth Drop Detected!", symbol=spot_symbol, prev=prev_count, now=0)

                    # --- STAGE B: Candle integrity (post-I/O) ---
                    # Check 4H decision-timeframe candle count + freshness.
                    # Replaces the old `candle_count < 50` guard with a more
                    # principled check that validates the decision TF specifically.
                    stage_b = check_candle_sanity(
                        symbol=spot_symbol,
                        candle_manager=self.candle_manager,
                        thresholds=self.sanity_thresholds,
                    )
                    if not stage_b.passed:
                        candles_4h = self.candle_manager.get_candles(
                            spot_symbol,
                            self.sanity_thresholds.decision_tf,
                        )
                        warmup_diag = _build_4h_warmup_skip_diagnostic(
                            strategy_config=self.config.strategy,
                            symbol=spot_symbol,
                            futures_symbol=futures_symbol,
                            stage_b_reason=stage_b.reason,
                            candles_4h=candles_4h,
                            required_candles=self.sanity_thresholds.min_decision_tf_candles,
                            decision_tf=self.sanity_thresholds.decision_tf,
                        )
                        if warmup_diag:
                            logger.info("4h_warmup_skip", **warmup_diag)
                        _af_skip(f"stage_b_{stage_b.reason or 'failed'}")
                        self.data_quality_tracker.record_result(
                            spot_symbol, passed=False, reason=stage_b.reason,
                        )
                        return

                    # Both stages passed -- record success
                    self.data_quality_tracker.record_result(spot_symbol, passed=True)

                    # 4H DECISION AUTHORITY HIERARCHY:
                    # 1D: Regime filter (EMA200 bias)
                    # 4H: Decision authority (OB/FVG/BOS, ATR for stops)
                    # 1H: Refinement (ADX, swing points)
                    # 15m: Refinement (entry timing)
                    signal = self.smc_engine.generate_signal(
                        symbol=spot_symbol,
                        regime_candles_1d=self.candle_manager.get_candles(spot_symbol, "1d"),
                        decision_candles_4h=self.candle_manager.get_candles(spot_symbol, "4h"),
                        refine_candles_1h=self.candle_manager.get_candles(spot_symbol, "1h"),
                        refine_candles_15m=candles,
                    )
                    _af_inc("signals_scored")
                    if signal.signal_type != SignalType.NO_SIGNAL:
                        _af_inc("setups_found")
                        _af_inc("signals_above_threshold")
                    
                    # Pass context to signal for execution (mark price for futures)
                    # Signal is spot-based, execution is futures-based.
                    order_outcome = None
                    if signal.signal_type != SignalType.NO_SIGNAL:
                        # Signal cooldown: prevent the same symbol from re-signalling
                        # every cycle (which caused the SPX compounding bug).
                        # Split semantics:
                        # - IN_POSITION cooldown (short): while symbol has active position.
                        # - POST_CLOSE cooldown: based on latest close reason and age.
                        cooldown_params = _resolve_signal_cooldown_params(
                            self.config.strategy,
                            spot_symbol,
                        )
                        in_position_cooldown_hours = float(cooldown_params["cooldown_hours"])
                        cooldown_canary_applied = bool(cooldown_params["canary_applied"])
                        position_size = Decimal("0")
                        if position_data:
                            try:
                                position_size = abs(Decimal(str(position_data.get("size", 0) or 0)))
                            except (ArithmeticError, ValueError, TypeError):
                                position_size = Decimal("0")
                        has_position = bool(position_data) and position_size > 0
                        last_open_at = None
                        if position_data:
                            last_open_at = (
                                position_data.get("opened_at")
                                or position_data.get("openedAt")
                                or position_data.get("open_time")
                                or position_data.get("timestamp")
                            )
                        symbol_key = _normalize_symbol_key(spot_symbol)
                        close_ctx = recent_close_by_symbol.get(symbol_key)
                        now_cd = datetime.now(timezone.utc)
                        cooldown_kind = None
                        cooldown_until = None
                        last_close_at = None
                        last_close_reason = None
                        if has_position:
                            cooldown_kind = "IN_POSITION"
                            cooldown_until = self._signal_cooldown.get(spot_symbol)
                            if cooldown_until is None:
                                cooldown_until = now_cd + timedelta(hours=in_position_cooldown_hours)
                                self._signal_cooldown[spot_symbol] = cooldown_until
                        else:
                            # Prevent stale in-position cooldown from suppressing flat symbols.
                            self._signal_cooldown.pop(spot_symbol, None)
                            if close_ctx:
                                cooldown_kind = str(close_ctx["cooldown_kind"])
                                last_close_at = close_ctx["last_close_at"]
                                last_close_reason = close_ctx["last_close_reason"]
                                cooldown_until = last_close_at + timedelta(
                                    minutes=int(close_ctx["cooldown_minutes"])
                                )
                        if cooldown_until and now_cd < cooldown_until:
                            kind = (cooldown_kind or "GLOBAL_THROTTLE").strip().upper()
                            if kind == "IN_POSITION":
                                _af_inc("suppress_in_position")
                            elif kind == "POST_CLOSE_WIN":
                                _af_inc("suppress_post_close_win")
                            elif kind == "POST_CLOSE_STRATEGIC":
                                _af_inc("suppress_post_close_strategic")
                            elif kind == "POST_CLOSE_LOSS":
                                _af_inc("suppress_post_close_loss")
                            elif kind == "GLOBAL_THROTTLE":
                                _af_inc("suppress_global_open_throttle")
                            else:
                                _af_inc("suppress_other")
                            logger.info(
                                "SIGNAL_SUPPRESSED_COOLDOWN",
                                symbol=spot_symbol,
                                has_position=has_position,
                                cooldown_kind=cooldown_kind or "GLOBAL_THROTTLE",
                                cooldown_hours=in_position_cooldown_hours if cooldown_kind == "IN_POSITION" else None,
                                cooldown_minutes=int(close_ctx["cooldown_minutes"]) if close_ctx else None,
                                last_open_at=str(last_open_at) if last_open_at else None,
                                last_close_at=last_close_at.isoformat() if isinstance(last_close_at, datetime) else None,
                                last_close_reason=last_close_reason,
                                cooldown_until=cooldown_until.isoformat() if isinstance(cooldown_until, datetime) else str(cooldown_until),
                                cooldown_remaining_seconds=max(
                                    0,
                                    int((cooldown_until - now_cd).total_seconds()),
                                ),
                                canary_applied=cooldown_canary_applied,
                                reason="pre_auction_cooldown_active",
                            )
                        else:
                            # Pre-entry spread check (fail-open: if anything fails, allow the trade).
                            # Uses already-fetched spot ticker bid/ask — zero new API calls.
                            spread_ok = True
                            try:
                                st = map_spot_tickers.get(spot_symbol)
                                if st:
                                    bid = Decimal(str(st.get("bid", 0) or 0))
                                    ask = Decimal(str(st.get("ask", 0) or 0))
                                    if bid > 0 and ask > 0:
                                        live_spread = (ask - bid) / bid
                                        max_entry_spread = Decimal("0.010")  # 1.0% — reject extreme spreads only
                                        if live_spread > max_entry_spread:
                                            spread_ok = False
                                            logger.warning(
                                                "SIGNAL_REJECTED_SPREAD: live spread too wide for entry",
                                                symbol=spot_symbol,
                                                spread=f"{live_spread:.3%}",
                                                threshold=f"{max_entry_spread:.3%}",
                                                signal=signal.signal_type.value,
                                            )
                            except (ValueError, TypeError, ArithmeticError, KeyError) as e:
                                spread_ok = False
                                logger.warning(
                                    "Spread check failed — BLOCKING entry (fail-closed)",
                                    symbol=spot_symbol,
                                    error=str(e),
                                    error_type=type(e).__name__,
                                )

                            if not spread_ok:
                                _af_skip("spread_guard")
                                return  # Skip this coin — spread too wide right now

                            # Record cooldown for this symbol
                            if has_position:
                                self._signal_cooldown[spot_symbol] = now_cd + timedelta(
                                    hours=in_position_cooldown_hours
                                )
                        
                            # Collect signal for auction mode (if enabled)
                            if self.auction_allocator and is_tradable:
                                self.auction_signals_this_tick.append((signal, spot_price, mark_price))
                                _af_inc("signals_generated")
                        
                            if not is_tradable:
                                logger.warning(
                                    "Signal skipped (not tradable)",
                                    symbol=spot_symbol,
                                    signal=signal.signal_type.value,
                                    futures_symbol=futures_symbol,
                                    skip_reason=skip_reason,
                                )
                            else:
                                # In auction mode, skip individual signal handling - auction will decide
                                if not self.auction_allocator:
                                    order_outcome = await self._handle_signal(signal, spot_price, mark_price)
                    
                    # Trace Logging (Throttled)
                    now = datetime.now(timezone.utc)
                    last_trace = self.last_trace_log.get(spot_symbol, datetime.min.replace(tzinfo=timezone.utc))
                    
                    if (now - last_trace).total_seconds() > 300: # 5 minutes
                        try:
                            from src.storage.repository import async_record_event
                            
                            trace_details = {
                                "signal": signal.signal_type.value,
                                "regime": signal.regime,
                                "bias": signal.higher_tf_bias,
                                "adx": float(signal.adx) if signal.adx else 0.0,
                                "atr": float(signal.atr) if signal.atr else 0.0,
                                "ema200_slope": signal.ema200_slope,
                                "spot_price": float(spot_price),
                                "setup_quality": sum(
                                    float(v)
                                    for v in (signal.score_breakdown or {}).values()
                                    if isinstance(v, (int, float, Decimal))
                                ),
                                "score_breakdown": signal.score_breakdown or {},
                                "status": "active",
                                "candle_count": candle_count,
                                "reason": signal.reasoning,  # CAPTURE REASON
                                "structure": signal.structure_info,
                                "meta": signal.meta_info
                            }
                            if self.institutional_memory_manager and self.institutional_memory_manager.is_enabled_for_symbol(spot_symbol):
                                thesis_snapshot = self.institutional_memory_manager.update_conviction_for_symbol(
                                    spot_symbol,
                                    current_price=spot_price,
                                    current_volume_avg=None,
                                    emit_log=True,
                                )
                                _attach_thesis_trace_fields(trace_details, thesis_snapshot)
                            if signal.signal_type != SignalType.NO_SIGNAL:
                                trace_details["skipped"] = not is_tradable
                                if not is_tradable:
                                    trace_details["skip_reason"] = skip_reason or "unknown"
                                elif order_outcome is not None:
                                    trace_details["order_placed"] = order_outcome.get("order_placed", False)
                                    if not order_outcome.get("order_placed") and order_outcome.get("reason"):
                                        trace_details["order_fail_reason"] = order_outcome["reason"]

                            if signal.signal_type == SignalType.NO_SIGNAL and signal.reasoning:
                                # Downgrade to debug: this fires for most coins every cycle.
                                # Reasoning is still captured in DECISION_TRACE event below.
                                logger.debug(
                                    "SMC Analysis: NO_SIGNAL",
                                    symbol=spot_symbol,
                                    reasoning=signal.reasoning.replace("\n", " | "),
                                )
                            
                            await async_record_event(
                                event_type="DECISION_TRACE",
                                symbol=spot_symbol,
                                details=trace_details,
                                timestamp=now
                            )
                            self.last_trace_log[spot_symbol] = now
                        except (OperationalError, DataError, OSError) as e:
                            logger.error("Failed to record decision trace", symbol=spot_symbol, error=str(e), error_type=type(e).__name__)

                    if self.hardening:
                        self.hardening.record_coin_processed()

                except (OperationalError, DataError) as e:
                    logger.warning(f"Error processing {spot_symbol}", error=str(e), error_type=type(e).__name__)
                except Exception as e:
                    # Unknown exception in per-coin processing — escape to tick-level handler
                    logger.error(f"Unexpected error processing {spot_symbol}", error=str(e), error_type=type(e).__name__)
                    raise

        # Execute parallel processing
        # Clear signal collection for this tick
        if self.auction_allocator:
            self.auction_signals_this_tick = []
        
        # CRITICAL: Only process the filtered universe.
        # `self.markets` may include symbols that must be hard-blocked (e.g. fiat pairs),
        # and `process_coin()` can still trade them via futures tickers even without spot tickers.
        # Data quality filter: exclude SUSPENDED/DEGRADED-skipped symbols at scheduling level.
        analyzable = [s for s in market_symbols if self.data_quality_tracker.should_analyze(s)]
        analysis_funnel["symbols_skipped_by_reason"]["data_quality_gate"] = max(
            0, len(market_symbols) - len(analyzable)
        )
        await asyncio.gather(*[process_coin(s) for s in analyzable], return_exceptions=True)
        self._analysis_funnel_metrics = analysis_funnel
        
        # Run auction mode allocation (if enabled) - after all signals processed
        if self.auction_allocator:
            signals_count = len(self.auction_signals_this_tick)
            logger.info("AUCTION_START", signals_collected=signals_count)
            logger.info(
                "Auction: About to run allocation",
                signals_collected=signals_count,
                auction_allocator_exists=bool(self.auction_allocator),
            )
            await self._run_auction_allocation(all_raw_positions)
            logger.info("AUCTION_END", signals_collected=signals_count)
        else:
            logger.debug("Auction: Skipped (auction_allocator is None)")
        
        # Phase 2: Batch save all collected candles (grouped by symbol/timeframe)
        # Phase 2: Batch save all collected candles (delegated to Manager)
        await self.candle_manager.flush_pending()
        
        # Persist data quality state (rate-limited internally to every 5 min)
        self.data_quality_tracker.persist()
        
        # Log periodic status summary (every 5 minutes)
        now = datetime.now(timezone.utc)
        if (now - self.last_status_summary).total_seconds() > 300:  # 5 minutes
            try:
                total_coins = len(market_symbols)
                coins_with_candles = sum(1 for s in market_symbols if len(self.candle_manager.get_candles(s, "15m")) >= 50)
                coins_processed_recently = sum(
                    1 for s in market_symbols
                    if self.coin_processing_stats.get(s, {}).get("last_processed", datetime.min.replace(tzinfo=timezone.utc)) > (now - timedelta(minutes=10))
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
                logger.error("Failed to log status summary", error=str(e), error_type=type(e).__name__)
        
        # 4.5 CRITICAL: Validate all positions have stop loss protection
        # Legacy validation removed - using new _validate_position_protection after initial tick
        
        # 5. Account Sync (Throttled) - Moved to step 2 to prevent duplicate calls
        # Reference: _sync_positions call in Step 2 handles global state update
            
        # 7. Operational Maintenance (Daily)
        now = datetime.now(timezone.utc)
        if (now - self.last_maintenance_run).total_seconds() > 86400: # 24 hours
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
                logger.error("Periodic data maintenance failed", error=str(e), error_type=type(e).__name__)

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

    async def _try_auto_recovery(self) -> bool:
        """Auto-recovery from kill switch -- delegates to health_monitor module."""
        from src.live.health_monitor import try_auto_recovery
        return await try_auto_recovery(self)

    async def _sync_account_state(self):
        """Fetch and persist real-time account state -- delegates to exchange_sync module."""
        from src.live.exchange_sync import sync_account_state
        await sync_account_state(self)
    
    # -----------------------------------------------------------------------
    # Signal handling (delegated to src.live.signal_handler)
    # -----------------------------------------------------------------------

    async def _handle_signal(
        self, signal: Signal, spot_price: Decimal, mark_price: Decimal,
        notional_override: "Optional[Decimal]" = None,
    ) -> dict:
        """Signal processing -- delegates to signal_handler module.

        Args:
            notional_override: When set (auction execution path), used as
                base notional in risk sizing and enables utilisation boost.
        """
        from src.live.signal_handler import handle_signal
        return await handle_signal(self, signal, spot_price, mark_price, notional_override=notional_override)

    async def _handle_signal_v2(
        self, signal: Signal, spot_price: Decimal, mark_price: Decimal,
    ) -> dict:
        """V2 signal processing -- delegates to signal_handler module."""
        from src.live.signal_handler import handle_signal_v2
        return await handle_signal_v2(self, signal, spot_price, mark_price)

    async def _update_candles(self, symbol: str):
        """Update local candle caches from acquisition with throttling."""
        await self.candle_manager.update_candles(symbol)

    async def _run_auction_allocation(self, raw_positions: List[Dict]):
        """Auction allocation -- delegates to auction_runner module."""
        from src.live.auction_runner import run_auction_allocation
        await run_auction_allocation(self, raw_positions)
    
    async def _save_trade_history(self, position: Position, exit_price: Decimal, exit_reason: str):
        """Save closed position to trade history -- delegates to exchange_sync module."""
        from src.live.exchange_sync import save_trade_history
        await save_trade_history(self, position, exit_price, exit_reason)

    def _write_heartbeat(self) -> None:
        """Write heartbeat file with timestamp, phase, and health banner.

        External watchdog (systemd timer / sidecar) checks file staleness.
        If timestamp > 60s old → process is hung → restart.

        Health banner includes breaker state, self-heal metrics, rate limiter,
        and trade recording failures — gives immediate "are we drifting?" visibility.
        """
        try:
            import json
            import subprocess
            heartbeat_dir = Path("runtime")
            heartbeat_dir.mkdir(parents=True, exist_ok=True)
            heartbeat_path = heartbeat_dir / "heartbeat.json"

            # Core identity
            data = {
                "timestamp": time.time(),
                "iso": datetime.now(timezone.utc).isoformat(),
                "startup_phase": self._startup_sm.phase.value if self._startup_sm else "unknown",
                "cycle": getattr(self, "_last_cycle_count", 0),
                "kill_switch_active": self.kill_switch.is_active() if self.kill_switch else False,
            }

            # Git commit hash (cached after first call)
            if not hasattr(self, "_git_sha"):
                try:
                    self._git_sha = subprocess.check_output(
                        ["git", "rev-parse", "--short", "HEAD"],
                        stderr=subprocess.DEVNULL, timeout=2,
                    ).decode().strip()
                except Exception:
                    self._git_sha = "unknown"
            data["git_sha"] = self._git_sha

            # Circuit breaker state
            if hasattr(self.client, "api_breaker"):
                breaker = self.client.api_breaker
                state = getattr(breaker, "_state", None)
                data["breaker_state"] = state.value if hasattr(state, "value") else str(state)
                data["breaker_failure_count"] = getattr(breaker, "_failure_count", 0)

            # Stop self-heal metrics
            heal = getattr(self, "_stop_heal_metrics", {})
            if heal:
                data["stop_self_heal_attempts"] = heal.get("stop_self_heal_attempts_total", 0)
                data["stop_self_heal_success"] = heal.get("stop_self_heal_success_total", 0)
                data["stop_self_heal_failures"] = heal.get("stop_self_heal_failures_total", 0)
                data["layer3_saves"] = heal.get("layer3_saves_total", 0)

            # Execution gateway metrics
            if hasattr(self, "execution_gateway"):
                gw = self.execution_gateway
                data["trade_record_failures"] = gw.metrics.get("trade_record_failures_total", 0)
                data["orders_blocked_by_rate_limit"] = gw._order_rate_limiter.orders_blocked_total
                data["orders_per_minute"] = gw._order_rate_limiter.orders_last_minute
                data["gateway_errors"] = gw.metrics.get("errors", 0)

            # Atomic write: write to temp then rename
            tmp_path = heartbeat_path.with_suffix(".tmp")
            tmp_path.write_text(json.dumps(data))
            tmp_path.rename(heartbeat_path)
        except OSError as e:
            logger.debug("Failed to write heartbeat", error=str(e))

    async def _on_trade_recorded(self, position, trade) -> None:
        """
        Callback fired by ExecutionGateway after a trade is recorded.
        
        Updates risk manager daily PnL tracking and checks daily loss limits.
        This replaces the old save_trade_history() risk manager update path
        that was orphaned when V2 moved to trade_recorder.
        """
        try:
            from src.execution.equity import calculate_effective_equity
            
            net_pnl = trade.net_pnl
            setup_type = getattr(position, "setup_type", None)
            if self.institutional_memory_manager:
                self.institutional_memory_manager.on_trade_recorded(
                    symbol=position.symbol,
                    trade_id=trade.trade_id,
                    net_pnl=trade.net_pnl,
                    exited_at=trade.exited_at,
                )
            
            # Get current equity for risk manager
            balance = await self.client.get_futures_balance()
            base = getattr(self.config.exchange, "base_currency", "USD")
            equity_now, _, _ = await calculate_effective_equity(
                balance, base_currency=base, kraken_client=self.client
            )
            self.risk_manager.record_trade_result(net_pnl, equity_now, setup_type)
            
            # Check if daily loss limit approached
            daily_loss_pct = (
                abs(self.risk_manager.daily_pnl) / self.risk_manager.daily_start_equity
                if self.risk_manager.daily_start_equity > 0
                and self.risk_manager.daily_pnl < 0
                else Decimal("0")
            )
            if daily_loss_pct > Decimal(str(self.config.risk.daily_loss_limit_pct * 0.7)):
                from src.monitoring.alerting import send_alert
                
                limit_pct = self.config.risk.daily_loss_limit_pct * 100
                await send_alert(
                    "DAILY_LOSS_WARNING",
                    f"Daily loss at {daily_loss_pct:.1%} of equity\n"
                    f"Limit: {limit_pct:.0f}%\n"
                    f"Daily P&L: ${self.risk_manager.daily_pnl:.2f}",
                    urgent=daily_loss_pct > Decimal(str(self.config.risk.daily_loss_limit_pct)),
                )
        except (OperationalError, DataError, ImportError) as e:
            from src.monitoring.logger import get_logger
            logger = get_logger(__name__)
            logger.warning(
                "on_trade_recorded callback: failed to update risk manager (non-fatal)",
                error=str(e),
                error_type=type(e).__name__,
            )


    # -----------------------------------------------------------------------
    # Protection operations (delegated to src.live.protection_ops)
    # -----------------------------------------------------------------------

    async def _reconcile_protective_orders(self, raw_positions: List[Dict], current_prices: Dict[str, Decimal]):
        """TP Backfill / Reconciliation -- delegates to protection_ops module."""
        from src.live.protection_ops import reconcile_protective_orders
        await reconcile_protective_orders(self, raw_positions, current_prices)

    async def _reconcile_stop_loss_order_ids(self, raw_positions: List[Dict]):
        """SL order ID reconciliation -- delegates to protection_ops module."""
        from src.live.protection_ops import reconcile_stop_loss_order_ids
        await reconcile_stop_loss_order_ids(self, raw_positions)

    async def _place_missing_stops_for_unprotected(self, raw_positions: List[Dict], max_per_tick: int = 3) -> None:
        """Place missing stops -- delegates to protection_ops module."""
        from src.live.protection_ops import place_missing_stops_for_unprotected
        await place_missing_stops_for_unprotected(self, raw_positions, max_per_tick)

    async def _should_skip_tp_backfill(
        self, symbol: str, pos_data: Dict, db_pos: Position, current_price: Decimal,
        is_protected: Optional[bool] = None
    ) -> bool:
        """Safety checks -- delegates to protection_ops module."""
        from src.live.protection_ops import should_skip_tp_backfill
        return await should_skip_tp_backfill(self, symbol, pos_data, db_pos, current_price, is_protected)

    def _needs_tp_backfill(self, db_pos: Position, symbol_orders: List[Dict]) -> bool:
        """TP coverage check -- delegates to protection_ops module."""
        from src.live.protection_ops import needs_tp_backfill
        return needs_tp_backfill(self, db_pos, symbol_orders)

    async def _compute_tp_plan(
        self, symbol: str, pos_data: Dict, db_pos: Position, current_price: Decimal
    ) -> Optional[List[Decimal]]:
        """TP plan computation -- delegates to protection_ops module."""
        from src.live.protection_ops import compute_tp_plan
        return await compute_tp_plan(self, symbol, pos_data, db_pos, current_price)

    async def _cleanup_orphan_reduce_only_orders(self, raw_positions: List[Dict]):
        """Orphan order cleanup -- delegates to protection_ops module."""
        from src.live.protection_ops import cleanup_orphan_reduce_only_orders
        await cleanup_orphan_reduce_only_orders(self, raw_positions)

    async def _place_tp_backfill(
        self, symbol: str, pos_data: Dict, db_pos: Position, tp_plan: List[Decimal],
        symbol_orders: List[Dict], current_price: Decimal
    ):
        """TP order placement -- delegates to protection_ops module."""
        from src.live.protection_ops import place_tp_backfill
        await place_tp_backfill(self, symbol, pos_data, db_pos, tp_plan, symbol_orders, current_price)
