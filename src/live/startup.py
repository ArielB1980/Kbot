"""
Startup / initialization phase extracted from LiveTrading.run().

Contains the full initialization sequence from client init through READY phase:
- Client initialization and market discovery
- Account/position sync
- Position State Machine V2 recovery
- Startup takeover (reconciliation)
- Background monitor setup (polling, health, protection, order polling)
- Candle hydration and data acquisition start
- Advance to READY phase

All functions receive the LiveTrading instance as their first argument (``lt``)
to access shared state, following the same delegate pattern used by the other
extracted modules.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from src.exceptions import DataError, OperationalError
from src.execution.production_safety import (
    PositionProtectionMonitor,
    ProtectionEnforcer,
    SafetyConfig,
)
from src.live.startup_validator import ensure_all_coins_have_traces
from src.monitoring.logger import get_logger
from src.runtime.startup_phases import StartupPhase

if TYPE_CHECKING:
    from src.live.live_trading import LiveTrading

logger = get_logger(__name__)


async def initialize_and_sync(lt: LiveTrading) -> None:
    """Execute the full startup/initialization sequence for LiveTrading.

    This covers everything from client initialization through advancing to the
    READY startup phase. The caller (``LiveTrading.run``) retains the outer
    try/except/finally and the main while-loop.

    Args:
        lt: The LiveTrading instance whose state is being initialized.
    """
    # 1. Initialize Client (INITIALIZING phase)
    logger.info("Initializing Kraken client...")
    await lt.client.initialize()

    # 1.5 Initial Market Discovery
    if lt.config.exchange.use_market_discovery:
        logger.info("Performing initial market discovery...")
        await lt._update_market_universe()
        lt.last_discovery_time = datetime.now(UTC)
    else:
        lt.last_discovery_time = datetime.min.replace(tzinfo=UTC)

    # Startup banner: config flags and universe size (same log sink)
    _recon_cfg = getattr(lt.config, "reconciliation", None)
    _auction = getattr(lt.config.risk, "auction_mode_enabled", False)
    _recon_en = getattr(_recon_cfg, "reconcile_enabled", True) if _recon_cfg else True
    _shock = getattr(lt.config.risk, "shock_guard_enabled", False)
    logger.info(
        "STARTUP_BANNER",
        auction_enabled=_auction,
        reconcile_enabled=_recon_en,
        shock_guard_enabled=bool(lt.shock_guard or _shock),
        universe_size=len(lt._market_symbols()),
    )

    # 1.6 Startup: ensure all monitored coins have DECISION_TRACE (dashboard coverage)
    try:
        await ensure_all_coins_have_traces(lt._market_symbols())
    except (OperationalError, DataError, OSError) as e:
        logger.error("Startup trace validation failed", error=str(e), error_type=type(e).__name__)

    # ===== PHASE: INITIALIZING → SYNCING =====
    lt._startup_sm.advance_to(StartupPhase.SYNCING, reason="client initialized, market discovered")

    # 2. Sync State (skip in dry run if no keys)
    if lt.config.system.dry_run and not lt.client.has_valid_futures_credentials():
        logger.warning("Dry Run Mode: No Futures credentials found. Skipping account sync.")
    else:
        # Sync Account
        try:
            await lt._sync_account_state()
            await lt._sync_positions()
            await lt.executor.sync_open_orders()
        except (OperationalError, DataError) as e:
            logger.error("Initial sync failed", error=str(e), error_type=type(e).__name__)
            if not lt.config.system.dry_run:
                raise

    # 2.5 Position State Machine V2 Startup Recovery
    if lt.use_state_machine_v2 and lt.execution_gateway:
        try:
            logger.info("Starting Position State Machine V2 recovery...")
            await lt.execution_gateway.startup()
            logger.info(
                "Position State Machine V2 recovery complete",
                active_positions=len(lt.position_registry.get_all_active())
                if lt.position_registry
                else 0,
            )
        except (OperationalError, DataError) as e:
            logger.error(
                "Position State Machine V2 startup failed",
                error=str(e),
                error_type=type(e).__name__,
            )

    # ===== PHASE: SYNCING → RECONCILING =====
    lt._startup_sm.advance_to(StartupPhase.RECONCILING, reason="account/positions synced")

    # 2.6 Startup takeover & protect (V2 authoritative pass)
    # In V2 mode, do not use the legacy Reconciler (DB-only) as the source of truth.
    if (
        lt.use_state_machine_v2
        and lt.execution_gateway
        and not (lt.config.system.dry_run and not lt.client.has_valid_futures_credentials())
    ):
        _recon_cfg = getattr(lt.config, "reconciliation", None)
        if _recon_cfg and getattr(_recon_cfg, "reconcile_enabled", True):
            try:
                from src.execution.production_takeover import (
                    ProductionTakeover,
                    TakeoverConfig,
                )

                takeover = ProductionTakeover(
                    lt.execution_gateway,
                    TakeoverConfig(
                        takeover_stop_pct=Decimal(str(os.getenv("TAKEOVER_STOP_PCT", "0.02"))),
                        stop_replace_atomically=True,
                        dry_run=bool(lt.config.system.dry_run),
                    ),
                )
                logger.critical("Running startup takeover (V2)...")
                stats = await takeover.execute_takeover()
                logger.critical("Startup takeover complete", **stats)
                lt.last_recon_time = datetime.now(UTC)
            except (OperationalError, DataError) as ex:
                logger.critical("Startup takeover failed", error=str(ex), exc_info=True)
                if not lt.config.system.dry_run:
                    raise

    # 2.6a Retry trade recording for positions closed before last restart
    if lt.use_state_machine_v2 and lt.execution_gateway:
        try:
            retried = await lt.execution_gateway.retry_unrecorded_trades()
            if retried > 0:
                logger.info("Startup trade recording retry recorded trades", count=retried)
        except (OperationalError, DataError) as e:
            logger.error(
                "Startup trade recording retry failed",
                error=str(e),
                error_type=type(e).__name__,
            )

    # 2.6 LivePollingMonitor — unified facade for background monitors
    from src.monitoring.live_polling_monitor import LivePollingMonitor

    lt._polling_monitor = LivePollingMonitor(lt)

    # 2.6.0 HealthCheckCoordinator — wire into HTTP layer for /api/monitoring/status
    try:
        from src.health import set_coordinator
        from src.monitoring.health_checks_impl import (
            BalanceCheck,
            ExchangeConnectivityCheck,
            ProcessHealthCheck,
        )
        from src.monitoring.health_coordinator import HealthCheckCoordinator

        coordinator = HealthCheckCoordinator(default_interval=60.0)
        coordinator.register(
            ExchangeConnectivityCheck(client_factory=lambda: lt.client),
            interval_seconds=60,
        )
        coordinator.register(
            BalanceCheck(client_factory=lambda: lt.client),
            interval_seconds=120,
        )
        coordinator.register(
            ProcessHealthCheck(client_factory=lambda: lt.client),
            interval_seconds=60,
        )
        set_coordinator(coordinator)
        lt._health_coordinator = coordinator
        lt._health_coordinator_task = asyncio.create_task(coordinator.start())
        logger.info("HealthCheckCoordinator started and registered with HTTP layer")
    except (ValueError, TypeError, RuntimeError, ImportError) as e:
        logger.warning(
            "Failed to start HealthCheckCoordinator",
            error=str(e),
            error_type=type(e).__name__,
        )

    # 2.6a PositionProtectionMonitor (Invariant K) - periodic check when V2 live
    if lt.use_state_machine_v2 and lt.execution_gateway and lt.position_registry:
        try:
            cfg = SafetyConfig()
            enforcer = ProtectionEnforcer(lt.client, cfg)
            lt._protection_monitor = PositionProtectionMonitor(
                lt.client,
                lt.position_registry,
                enforcer,
                persistence=lt.position_persistence,
            )
            lt._protection_task = lt._polling_monitor.spawn(
                lt._polling_monitor.run_protection_checks(interval_seconds=30)
            )
            logger.info("PositionProtectionMonitor started (interval=30s)")
        except (ValueError, TypeError, RuntimeError) as e:
            logger.error(
                "Failed to start PositionProtectionMonitor",
                error=str(e),
                error_type=type(e).__name__,
            )

    # 2.6b Order-status polling: detect entry fills, trigger PLACE_STOP (SL/TP)
    if lt.use_state_machine_v2 and lt.execution_gateway:
        try:
            lt._order_poll_task = lt._polling_monitor.spawn(
                lt._polling_monitor.run_order_polling(interval_seconds=12)
            )
            logger.info("Order-status polling started (interval=12s)")
        except (ValueError, TypeError, RuntimeError) as e:
            logger.error("Failed to start order poller", error=str(e), error_type=type(e).__name__)

    # 2.6c Daily P&L summary (runs once per day at midnight UTC)
    try:
        lt._daily_summary_task = lt._polling_monitor.spawn(lt._polling_monitor.run_daily_summary())
        logger.info("Daily summary task started")
    except (ValueError, TypeError, RuntimeError) as e:
        logger.error(
            "Failed to start daily summary task", error=str(e), error_type=type(e).__name__
        )

    # 2.6c.2 Spot DCA (daily scheduled spot purchases)
    try:
        lt._spot_dca_task = asyncio.create_task(lt._run_spot_dca())
        logger.info("Spot DCA task started")
    except (ValueError, TypeError, RuntimeError) as e:
        logger.error("Failed to start spot DCA task", error=str(e), error_type=type(e).__name__)

    # 2.6c.3 WebSocket candle feed — deferred to AFTER DB hydration (step 3)
    # to prevent WS candles from poisoning the merge logic.

    # 2.6c.4 Periodic instrument spec refresh (prevents stale cache from blocking new entries)
    try:
        lt._spec_refresh_task = asyncio.create_task(lt._run_spec_refresh(interval_seconds=4 * 3600))
        logger.info("Instrument spec refresh task started (interval=4h)")
    except (ValueError, TypeError, RuntimeError) as e:
        logger.error("Failed to start spec refresh task", error=str(e), error_type=type(e).__name__)

    # 2.6d Runtime regression monitors (trade starvation + winner churn)
    try:
        lt._starvation_monitor_task = lt._polling_monitor.spawn(
            lt._polling_monitor.run_trade_starvation_monitor(interval_seconds=300)
        )
        logger.info("Trade starvation monitor started (interval=300s)")
    except (ValueError, TypeError, RuntimeError) as e:
        logger.error(
            "Failed to start trade starvation monitor",
            error=str(e),
            error_type=type(e).__name__,
        )

    try:
        lt._churn_monitor_task = lt._polling_monitor.spawn(
            lt._polling_monitor.run_winner_churn_monitor(interval_seconds=300)
        )
        logger.info("Winner churn monitor started (interval=300s)")
    except (ValueError, TypeError, RuntimeError) as e:
        logger.error(
            "Failed to start winner churn monitor",
            error=str(e),
            error_type=type(e).__name__,
        )

    try:
        lt._trade_recording_monitor_task = lt._polling_monitor.spawn(
            lt._polling_monitor.run_trade_recording_monitor(interval_seconds=300)
        )
        logger.info("Trade recording invariant monitor started (interval=300s)")
    except (ValueError, TypeError, RuntimeError) as e:
        logger.error(
            "Failed to start trade recording monitor",
            error=str(e),
            error_type=type(e).__name__,
        )

    # 2.6e Telegram command handler (/status, /positions, /help)
    try:
        from src.monitoring.telegram_bot import TelegramCommandHandler

        lt._telegram_handler = TelegramCommandHandler(
            data_provider=lt._polling_monitor.get_system_status
        )
        lt._telegram_cmd_task = asyncio.create_task(lt._telegram_handler.run())
        logger.info("Telegram command handler started")
    except (ValueError, TypeError, RuntimeError) as e:
        logger.error(
            "Failed to start Telegram command handler",
            error=str(e),
            error_type=type(e).__name__,
        )

    # 3. Fast Startup - Load candles
    logger.info("Loading candles from database...")
    try:
        # 3. Fast Startup - Load candles via Manager
        await lt.candle_manager.initialize(lt._market_symbols())
    except (OperationalError, DataError) as e:
        logger.error("Failed to hydrate candles", error=str(e), error_type=type(e).__name__)

    # 3.5 Start WebSocket candle feed AFTER DB hydration
    try:
        lt._ws_candle_task = asyncio.create_task(lt._run_ws_candle_feed())
        logger.info("WebSocket candle feed task started (post-hydration)")
    except (ValueError, TypeError, RuntimeError) as e:
        logger.error(
            "Failed to start WS candle feed task", error=str(e), error_type=type(e).__name__
        )

    # 4. Start Data Acquisition
    await lt.data_acq.start()

    # ===== PHASE: RECONCILING → READY =====
    # CRITICAL: READY must be set BEFORE first tick.
    # No trading actions (including self-heal / ShockGuard) may run before READY.
    lt._startup_sm.advance_to(StartupPhase.READY, reason="all startup steps complete")
    logger.info(
        "STARTUP_COMPLETE",
        startup_epoch=lt._startup_sm.startup_epoch.isoformat()
        if lt._startup_sm.startup_epoch
        else None,
        status=lt._startup_sm.get_status(),
    )
