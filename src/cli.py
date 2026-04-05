"""
CLI entrypoint for the Kraken Futures SMC Trading System.

Provides commands for backtest, paper, live, status, and kill-switch.
"""
import json
import os
import typer
from typing import Optional
from pathlib import Path
from datetime import datetime
from decimal import Decimal

from src.exceptions import OperationalError, DataError
from src.monitoring.logger import get_logger
from src.cli_output import print_critical_error

app = typer.Typer(
    name="kraken-futures-smc",
    help="Kraken Futures SMC Trading System",
    add_completion=False,
)

logger = get_logger(__name__)


def _configure_replay_state_isolation(mode: str, state_file: Path) -> dict[str, str]:
    """Isolate replay safety state from live state files unless explicitly overridden."""
    if mode != "replay":
        return {}

    base_dir = state_file.parent
    base_dir.mkdir(parents=True, exist_ok=True)
    applied: dict[str, str] = {}

    if not os.environ.get("KILL_SWITCH_STATE_PATH"):
        path = str((base_dir / "kill_switch_state.replay.json").resolve())
        os.environ["KILL_SWITCH_STATE_PATH"] = path
        applied["KILL_SWITCH_STATE_PATH"] = path

    if not os.environ.get("SAFETY_STATE_PATH"):
        path = str((base_dir / "safety_state.replay.json").resolve())
        os.environ["SAFETY_STATE_PATH"] = path
        applied["SAFETY_STATE_PATH"] = path

    # Replay research must never inherit production/live startup semantics
    # from the host environment. The replay runner will later set DRY_RUN=0
    # inside its own isolated runtime so the exchange sim can accept orders.
    for key, value in (
        ("ENV", "dev"),
        ("ENVIRONMENT", "dev"),
        ("DRY_RUN", "1"),
        ("SYSTEM_DRY_RUN", "1"),
    ):
        if os.environ.get(key) != value:
            os.environ[key] = value
            applied[key] = value

    return applied


def _load_config(config_path: Path):
    """
    Lazy import: keep config loading out of module import time.
    """
    from src.config.config import load_config

    return load_config(str(config_path))


def _setup_logging_from_config(config, *, log_file: Optional[Path] = None) -> None:
    """
    Lazy import: keep logging setup out of module import time.
    """
    from src.monitoring.logger import setup_logging

    setup_logging(
        config.monitoring.log_level,
        config.monitoring.log_format,
        log_file=str(log_file) if log_file else None,
    )


@app.command()
def backtest(
    start: str = typer.Option(..., "--start", help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., "--end", help="End date (YYYY-MM-DD)"),
    symbol: str = typer.Option("BTC/USD", "--symbol", help="Symbol to backtest"),
    config_path: Path = typer.Option("src/config/config.yaml", "--config", help="Path to config file"),
):
    """
    Run backtest on historical spot data with futures cost simulation.
    
    Example:
        python src/cli.py backtest --start 2024-01-01 --end 2024-12-31 --symbol ETH/USD
    """
    # Load configuration
    config = _load_config(config_path)
    research_log_file = state_file.with_name(f"{state_file.stem}.app.log")
    _setup_logging_from_config(config, log_file=research_log_file)
    
    logger.info("Starting backtest", start=start, end=end, symbol=symbol)
    
    # Parse dates
    from datetime import timezone
    start_date = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_date = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    
    # Initialize components
    logger.info("Initializing backtest components...")
    
    # Imports here to avoid circular dependencies at top level if any
    import asyncio
    from src.data.kraken_client import KrakenClient
    from src.backtest.backtest_engine import BacktestEngine
    
    async def run_backtest():
        # Initialize client (testnet=False for backtest data usually, or True if strict)
        # Using real API for data execution
        client = KrakenClient(
            api_key=config.exchange.api_key if hasattr(config.exchange, "api_key") else "",
            api_secret=config.exchange.api_secret if hasattr(config.exchange, "api_secret") else "",
            use_testnet=False # Data comes from mainnet usually
        )
        
        try:
            # Create engine with symbol
            engine = BacktestEngine(config, symbol=symbol)
            engine.set_client(client)
            
            # Run simulation
            metrics = await engine.run(start_date, end_date)
            
            # Calculate final metrics
            end_equity = metrics.equity_curve[-1] if metrics.equity_curve else Decimal(str(config.backtest.starting_equity))
            total_return_pct = (metrics.total_pnl / Decimal(str(config.backtest.starting_equity))) * 100
            
            # Output results
            typer.echo("\n" + "="*60)
            typer.echo(f"BACKTEST RESULTS: {symbol}")
            typer.echo("="*60)
            typer.echo(f"Period:        {start_date.date()} to {end_date.date()}")
            typer.echo(f"Start Equity:  ${config.backtest.starting_equity:,.2f}")
            typer.echo(f"End Equity:    ${end_equity:,.2f}")
            typer.echo(f"PnL:           ${metrics.total_pnl:,.2f} ({total_return_pct:.2f}%)")
            typer.echo(f"Fees:          ${metrics.total_fees:,.2f}")
            typer.echo(f"Net PnL:       ${metrics.total_pnl - metrics.total_fees:,.2f}")
            typer.echo(f"Max Drawdown:  {metrics.max_drawdown:.2%}")
            typer.echo(f"Trades:        {metrics.total_trades} ({metrics.winning_trades}W-{metrics.losing_trades}L)")
            typer.echo(f"Win Rate:      {metrics.win_rate:.1f}%")
            if getattr(metrics, 'profit_factor', 0) > 0:
                typer.echo(f"Profit Factor: {metrics.profit_factor:.2f}")
            
            if getattr(metrics, 'runner_exits', 0) > 0:
                typer.echo("-"*60)
                typer.echo("RUNNER METRICS")
                typer.echo(f"TP1 fills:     {metrics.tp1_fills}")
                typer.echo(f"TP2 fills:     {metrics.tp2_fills}")
                typer.echo(f"Runner exits:  {metrics.runner_exits}")
                typer.echo(f"Runner avg R:  {metrics.runner_avg_r:.2f}")
                typer.echo(f"Beyond 3R:     {metrics.runner_exits_beyond_3r}")
                typer.echo(f"Best runner:   {metrics.runner_max_r:.2f}R")
            typer.echo("="*60 + "\n")
            
        finally:
            await client.close()

    # Run async loop
    asyncio.run(run_backtest())
    
    logger.info("Backtest completed")


@app.command()
def live(
    config_path: Path = typer.Option("src/config/config.yaml", "--config", help="Path to config file"),
    force: bool = typer.Option(False, "--force", help="Force live trading (bypass safety gates)"),
    with_health: bool = typer.Option(False, "--with-health", help="Start minimal HTTP health server on PORT/8080 (for App Platform worker readiness)"),
    log_file: Optional[Path] = typer.Option(None, "--log-file", help="Path to log file"),
):
    """
    Run live trading on Kraken Futures (REAL CAPITAL AT RISK).
    
    ⚠️  WARNING: This mode trades real money. Use with extreme caution.
    
    Example:
        python src/cli.py live
    """
    # Load configuration with error handling
    try:
        config = _load_config(config_path)
    except (OperationalError, DataError, OSError, ValueError, TypeError, KeyError) as e:
        print_critical_error("Failed to load configuration", e)
        raise typer.Exit(1)
    
    # Setup logging (may fail if config is invalid)
    try:
        _setup_logging_from_config(config, log_file=log_file)
    except (ValueError, TypeError, KeyError, ImportError, OSError) as e:
        print_critical_error("Failed to setup logging", e, include_type=False)
        raise typer.Exit(1)
    
    # Validate environment
    if config.environment != "prod" and not force and not config.system.dry_run:
        typer.secho(
            f"❌ Environment is '{config.environment}', not 'prod'. Set environment='prod' in config for live trading.",
            fg=typer.colors.RED,
            bold=True,
        )
        raise typer.Abort()

    # Production live guardrails (env-driven, fail fast)
    try:
        from src.runtime.guards import assert_prod_live_prereqs, is_prod_live_env

        assert_prod_live_prereqs()
    except (OperationalError, DataError, OSError) as e:
        logger.critical(
            "PROD_LIVE_GUARD_FAILED",
            error=str(e),
            error_type=type(e).__name__,
            exc_info=True,
        )
        raise typer.Exit(1)

    # Defense-in-depth: replacement must not be enabled in prod live unless explicitly overridden.
    if is_prod_live_env() and bool(getattr(getattr(config, "risk", None), "replacement_enabled", False)):
        if os.getenv("ALLOW_REPLACEMENT_IN_PROD", "").strip().upper() != "YES":
            logger.critical(
                "PROD_LIVE_REPLACEMENT_FORBIDDEN",
                message="replacement_enabled=true is not permitted in prod live unless ALLOW_REPLACEMENT_IN_PROD=YES is set",
            )
            raise typer.Exit(1)
    
    # Safety gates
    if config.live.require_paper_success and not force:
        typer.secho(
            "⚠️  Live trading requires successful paper trading:",
            fg=typer.colors.YELLOW,
            bold=True,
        )
        typer.echo(f"  - Minimum {config.live.min_paper_days} days of paper trading")
        typer.echo(f"  - Minimum {config.live.min_paper_trades} trades")
        typer.echo(f"  - Maximum {config.live.max_paper_drawdown_pct * 100}% drawdown")
        typer.echo("\nPaper trading validation not yet implemented.")
        typer.echo("Use --force to bypass (NOT RECOMMENDED)")
        raise typer.Abort()
    
    # Final confirmation
    typer.secho(
        "\n⚠️  LIVE TRADING MODE ⚠️",
        fg=typer.colors.RED,
        bold=True,
    )
    typer.secho(
        "You are about to trade REAL MONEY on Kraken Futures.",
        fg=typer.colors.RED,
    )
    typer.secho(
        "Leveraged futures trading carries substantial risk of loss.",
        fg=typer.colors.RED,
    )
    
    if not force and not typer.confirm("\nDo you want to proceed?"):
        raise typer.Abort()
    
    logger.warning("Live trading started - REAL CAPITAL AT RISK")

    # Optional: minimal health server for App Platform worker (readiness on :8080)
    if with_health:
        import threading
        import uvicorn
        import time
        from src.health import worker_health_app
        from src.utils.secret_manager import is_cloud_platform
        port = int(os.environ.get("PORT", "8080"))
        health_host = os.environ.get("WORKER_HEALTH_HOST") or os.environ.get("HEALTH_HOST")
        if not health_host:
            # Default safe behavior:
            # - In App Platform / managed environments, bind publicly for readiness.
            # - On a droplet/VM, bind localhost to avoid exposing debug/metrics endpoints to the internet.
            try:
                health_host = "0.0.0.0" if is_cloud_platform() else "127.0.0.1"
            except (ImportError, OSError):
                health_host = "127.0.0.1"

        # Reduce noisy warnings like "Invalid HTTP request received." (common from port scans).
        # Health server is auxiliary; keep errors, drop warnings by default.
        health_log_level = os.environ.get("WORKER_HEALTH_LOG_LEVEL") or os.environ.get("HEALTH_LOG_LEVEL") or "error"
        
        def _run_health():
            try:
                uvicorn.run(
                    worker_health_app,
                    host=health_host,
                    port=port,
                    log_level=health_log_level,
                    access_log=False,
                )
            except (OperationalError, DataError, OSError) as e:
                logger.error("Health server error: %s", e, error_type=type(e).__name__, exc_info=True)
        
        t = threading.Thread(target=_run_health, daemon=False)  # Non-daemon so it keeps running
        t.start()
        
        # Give the health server a moment to start before proceeding
        time.sleep(1)
        logger.info("Worker health server started", host=health_host, port=port, log_level=health_log_level)

    # Prod-live distributed lock (one trading process per account)
    # Acquire before importing the live runtime so we fail fast on duplicate workers.
    prod_lock = None
    try:
        from src.runtime.guards import (
            acquire_prod_live_lock,
            account_fingerprint,
            confirm_live_env,
            is_prod_live_env,
            is_dry_run_env,
            use_state_machine_v2_env,
        )
        from src.runtime.startup_identity import sanitize_for_logging, stable_sha256_hex
        from src.config.config import CONFIG_SCHEMA_VERSION

        exchange_name = getattr(getattr(config, "exchange", None), "name", None) or "kraken"
        prod_lock = acquire_prod_live_lock(exchange_name=str(exchange_name), market_type="futures")

        # Startup identity banner (pre-runtime): runtime + env fingerprint + config hash
        try:
            cfg_obj = sanitize_for_logging(config.model_dump())
            config_hash = stable_sha256_hex(cfg_obj)[:12]
        except (ValueError, TypeError, KeyError):
            config_hash = "unknown"

        git_sha = os.getenv("GIT_SHA") or os.getenv("GITHUB_SHA") or "unknown"
        strategy_id = os.getenv("STRATEGY_ID") or git_sha
        prod_safe_mode = (os.getenv("PROD_LIVE_SAFE_MODE") or "").strip().upper() == "YES"

        db_ident = prod_lock.db_identity() if prod_lock is not None else {}
        db_schema = prod_lock.schema_fingerprint() if prod_lock is not None else None

        logger.info(
            "STARTUP_IDENTITY",
            runtime="LiveTrading",
            pid=os.getpid(),
            env=os.getenv("ENVIRONMENT", "unknown"),
            is_prod_live=is_prod_live_env(),
            dry_run=is_dry_run_env(),
            use_state_machine_v2=use_state_machine_v2_env(),
            prod_live_safe_mode=prod_safe_mode,
            git_sha=git_sha,
            strategy_id=strategy_id,
            config_version=getattr(getattr(config, "system", None), "version", "unknown"),
            config_schema_version=CONFIG_SCHEMA_VERSION,
            config_hash=config_hash,
            exchange=str(exchange_name),
            market_type="futures",
            account_fingerprint=account_fingerprint(),
            lock_key_short=getattr(prod_lock, "lock_key_short", None),
            db_host=db_ident.get("db_host"),
            db_port=db_ident.get("db_port"),
            db_name=db_ident.get("db_name"),
            db_user=db_ident.get("db_user"),
            db_schema_hash=db_schema or "unknown",
            replacement_enabled=bool(getattr(getattr(config, "risk", None), "replacement_enabled", False)),
        )

        # Prod invariant report (best-effort, fail-closed is enforced by runtime guards)
        db_reachable = False
        try:
            if prod_lock is not None:
                prod_lock.ping()
                db_reachable = True
        except (OperationalError, DataError, OSError):
            db_reachable = False

        try:
            from src.utils.kill_switch import KillSwitch
            ks = KillSwitch()
            ks_status = ks.get_status()
        except (OperationalError, DataError, OSError):
            ks_status = {"active": None, "latched": None, "reason": None}

        logger.critical(
            "PROD_INVARIANT_REPORT",
            lock_acquired=bool(prod_lock is not None),
            lock_key_short=getattr(prod_lock, "lock_key_short", None),
            confirm_live=confirm_live_env(),
            v2_enabled=use_state_machine_v2_env(),
            dotenv_policy="disabled_in_prod",
            kill_switch_active=ks_status.get("active"),
            kill_switch_latched=ks_status.get("latched"),
            db_reachable=db_reachable,
            exchange_reachable="unknown_pre_runtime",
            time_sync="unknown_best_effort",
        )
    except (OperationalError, DataError, OSError) as e:
        logger.critical("Failed to acquire prod-live lock", error=str(e), error_type=type(e).__name__, exc_info=True)
        raise typer.Exit(1)

    # Initialize live trading engine
    import asyncio
    import traceback
    from src.live.live_trading import LiveTrading

    async def run_live():
        try:
            logger.info("Initializing LiveTrading engine...")
            engine = LiveTrading(config)
            logger.info("LiveTrading engine initialized successfully")
            logger.info("Starting main trading loop...")
            await engine.run()
        except (OperationalError, DataError, OSError) as e:
            logger.critical(
                "Live trading engine failed",
                error=str(e),
                error_type=type(e).__name__,
                exc_info=True
            )
            # Print full traceback for debugging
            logger.critical("Full traceback:\n%s", traceback.format_exc())
            raise

    try:
        asyncio.run(run_live())
    except KeyboardInterrupt:
        logger.info("Live trading stopped by user")
    except (OperationalError, DataError, OSError) as e:
        logger.critical(
            "Live trading failed with unhandled error",
            error=str(e),
            error_type=type(e).__name__,
            exc_info=True
        )
        # Print full traceback to stderr for visibility in logs
        import sys
        print("=" * 80, file=sys.stderr)
        print("CRITICAL ERROR - Live Trading Failed", file=sys.stderr)
        print("=" * 80, file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        print("=" * 80, file=sys.stderr)
        raise typer.Exit(1)
    finally:
        try:
            if prod_lock is not None:
                prod_lock.release()
        except (OperationalError, DataError, OSError):
            # Best effort: don't mask the original exception during shutdown.
            pass


@app.command()
def test(
    config_path: Path = typer.Option("src/config/config.yaml", "--config", help="Path to config file"),
):
    """
    Run system tests to verify API connection, data acquisition, and signal processing.
    
    Example:
        python run.py test
    """
    import asyncio
    from src.test_system import run_all_tests
    
    # Load config for logging setup
    config = _load_config(config_path)
    _setup_logging_from_config(config)
    
    success = asyncio.run(run_all_tests())
    if not success:
        raise typer.Exit(1)


@app.command(name="kill-switch")
def kill_switch_cmd(
    action: str = typer.Argument("status", help="Action: activate, deactivate, or status"),
    reason: str = typer.Option("Manual activation", help="Reason for activation")
):
    """
    Emergency kill switch control.
    
    Actions:
    - activate: Stop all trading and close positions
    - deactivate: Resume normal trading
    - status: Check kill switch state
    
    Examples:
        python src/cli.py kill-switch activate --reason "Market volatility"
        python src/cli.py kill-switch deactivate
        python src/cli.py kill-switch status
    """
    from rich.console import Console
    from src.utils.kill_switch import get_kill_switch, KillSwitchReason

    console = Console()
    ks = get_kill_switch()

    if action == "activate":
        ks.activate_sync(reason=KillSwitchReason.MANUAL)
        console.print("[bold red]🚨 KILL SWITCH ACTIVATED[/bold red]")
        console.print(f"Reason: {reason}")
        console.print("\nAll trading halted. Orders will be cancelled and positions closed on next tick.")
        console.print("Use 'acknowledge' to allow restart after resolving the issue.")

    elif action == "acknowledge":
        if ks.acknowledge():
            console.print("[bold green]✅ KILL SWITCH ACKNOWLEDGED[/bold green]")
            console.print("Trading can resume.")
        else:
            console.print("[yellow]Kill switch is not latched - nothing to acknowledge[/yellow]")

    elif action == "deactivate":
        console.print("[yellow]Note: Use 'acknowledge' instead of 'deactivate' for latched kill switch[/yellow]")
        if ks.acknowledge():
            console.print("[bold green]✅ KILL SWITCH DEACTIVATED[/bold green]")
            console.print("Trading can resume.")
        else:
            console.print("[yellow]Kill switch is not active[/yellow]")
        
    elif action == "status":
        status = ks.get_status()
        if status["active"]:
            console.print("[bold red]🚨 KILL SWITCH: ACTIVE[/bold red]")
            console.print(f"Latched: {status['latched']}")
            console.print(f"Activated at: {status['activated_at']}")
            console.print(f"Reason: {status['reason']}")
            console.print(f"Duration: {status['duration_seconds']:.0f}s")
            if status['latched']:
                console.print("\n[yellow]Run 'kill-switch acknowledge' to allow restart[/yellow]")
        else:
            console.print("[bold green]✅ KILL SWITCH: INACTIVE[/bold green]")
            console.print("Trading is operational.")
    else:
        console.print(f"[bold red]Unknown action: {action}[/bold red]")
        console.print("Valid actions: activate, deactivate, status")
        raise typer.Exit(1)



@app.command()
def status(
    config_path: Path = typer.Option("src/config/config.yaml", "--config", help="Path to config file"),
):
    """
    Display current system status.
    
    Shows:
    - Current positions
    - P&L
    - Risk metrics
    - Kill switch status
    
    Example:
        python src/cli.py status
    """
    # Load configuration
    config = _load_config(config_path)
    _setup_logging_from_config(config)
    
    typer.echo("System Status")
    typer.echo("=" * 50)
    typer.echo(f"Environment: {config.environment}")
    
    # 1. Active Position
    from src.storage.repository import get_active_position, get_all_trades
    from src.domain.models import Side
    
    pos = get_active_position()
    if pos:
        pnl_color = typer.colors.GREEN if pos.unrealized_pnl >= 0 else typer.colors.RED
        typer.secho(f"\n🟢 Active Position: {pos.symbol} ({pos.side.value.upper()})", bold=True)
        typer.echo(f"  Entry:      ${pos.entry_price:,.2f}")
        typer.echo(f"  Current:    ${pos.current_mark_price:,.2f}")
        typer.echo(f"  Size:       ${pos.size_notional:,.2f} ({pos.leverage}x)")
        typer.echo(f"  Liq Price:  ${pos.liquidation_price:,.2f}")
        typer.secho(f"  Unrealized: ${pos.unrealized_pnl:,.2f}", fg=pnl_color)
    else:
        typer.echo("\n⚪️ No Active Position (Scanning...)")
        
    # 2. Recent Trades
    trades = get_all_trades()
    if trades:
        typer.echo(f"\nRecent Trades ({len(trades)} total)")
        typer.echo("-" * 50)
        for t in trades[:5]:
            pnl_color = typer.colors.GREEN if t.net_pnl >= 0 else typer.colors.RED
            icon = "WIN" if t.net_pnl > 0 else "LOSS"
            typer.secho(f"  {t.exited_at.strftime('%Y-%m-%d %H:%M')} | {t.side.value.upper()} | ${t.net_pnl:,.2f} ({icon})", fg=pnl_color)
    else:
        typer.echo("\nNo trades recorded yet.")
        
    typer.echo("\n" + "=" * 50)


@app.command()
def report(
    hours: int = typer.Option(24, "--hours", help="Hours to look back"),
    format: str = typer.Option("text", "--format", help="Output format (text/table)"),
):
    """
    Generate activity report (coins scanned, signals, regimes).
    
    Example:
        python src/cli.py report --hours 24
    """
    from src.reporting.activity import generate_activity_report
    generate_activity_report(hours=hours, format_type=format)


@app.command()
def research(
    iterations: int = typer.Option(12, "--iterations", min=1, help="Number of candidate iterations"),
    days: int = typer.Option(90, "--days", min=1, help="Backtest lookback window in days"),
    symbols: str = typer.Option(
        "BTC/USD,ETH/USD,SOL/USD,XRP/USD,ADA/USD,LINK/USD",
        "--symbols",
        help="Comma-separated symbol list for evaluation",
    ),
    mode: str = typer.Option(
        "backtest",
        "--mode",
        help="Evaluation backend: backtest, replay, or mock",
    ),
    objective_mode: str = typer.Option(
        "risk_adjusted",
        "--objective-mode",
        help="Scoring objective: risk_adjusted or net_pnl_only",
    ),
    symbol_by_symbol: bool = typer.Option(
        False,
        "--symbol-by-symbol/--no-symbol-by-symbol",
        help="Optimize each symbol independently",
    ),
    symbols_from_live_universe: bool = typer.Option(
        False,
        "--symbols-from-live-universe/--no-symbols-from-live-universe",
        help="Use current live universe from config instead of --symbols",
    ),
    symbols_from_config_universe: bool = typer.Option(
        False,
        "--symbols-from-config-universe/--no-symbols-from-config-universe",
        help="Use full coin_universe.candidate_symbols from config (overrides --symbols and live universe)",
    ),
    until_convergence: bool = typer.Option(
        False,
        "--until-convergence/--no-until-convergence",
        help="Run each symbol until no improvement for N iterations",
    ),
    max_stagnant_iterations: int = typer.Option(
        20,
        "--max-stagnant-iterations",
        min=1,
        help="Convergence stop threshold (no improvement streak)",
    ),
    max_iterations_per_symbol: int = typer.Option(
        300,
        "--max-iterations-per-symbol",
        min=1,
        help="Safety cap per symbol when convergence mode is enabled",
    ),
    digest_every: int = typer.Option(5, "--digest-every", min=1, help="Telegram digest interval"),
    window_offsets: str = typer.Option(
        "0,30,60",
        "--window-offsets",
        help="Comma-separated window offsets in days for split scoring",
    ),
    holdout_ratio: float = typer.Option(
        0.30,
        "--holdout-ratio",
        min=0.10,
        max=0.80,
        help="Holdout fraction inside each window for robust split scoring",
    ),
    out_dir: Path = typer.Option("data/research", "--out-dir", help="Output directory"),
    state_file: Path = typer.Option("data/research/state.json", "--state-file", help="Research state file"),
    telegram: bool = typer.Option(True, "--telegram/--no-telegram", help="Enable Telegram notifications and control commands"),
    auto_replay_gate: bool = typer.Option(
        False,
        "--auto-replay-gate/--no-auto-replay-gate",
        help="Run replay harness automatically for best candidate before promotion readiness",
    ),
    replay_seeds: str = typer.Option(
        "42",
        "--replay-seeds",
        help="Comma-separated replay jitter seeds for gate (all must pass)",
    ),
    replay_data_dir: str = typer.Option(
        "data/replay",
        "--replay-data-dir",
        help="Replay harness data directory",
    ),
    replay_timeout_seconds: int = typer.Option(
        1200,
        "--replay-timeout-seconds",
        min=30,
        help="Timeout per replay seed execution",
    ),
    auto_queue_promotion: bool = typer.Option(
        False,
        "--auto-queue-promotion/--no-auto-queue-promotion",
        help="Automatically queue promotion when replay gate passes",
    ),
    auto_backfill_data: bool = typer.Option(
        True,
        "--auto-backfill-data/--no-auto-backfill-data",
        help="For replay mode, fetch missing historical candles before evaluation",
    ),
    replay_timeframes: str = typer.Option(
        "1m,15m,1h,4h,1d",
        "--replay-timeframes",
        help="Comma-separated replay data timeframes",
    ),
    config_path: Path = typer.Option("src/config/config.yaml", "--config", help="Path to config file"),
):
    """
    Run sandbox autoresearch loop for strategy parameter optimization.
    """
    applied_state_overrides = _configure_replay_state_isolation(mode, state_file)

    config = _load_config(config_path)
    _setup_logging_from_config(config)
    if applied_state_overrides:
        logger.info(
            "Replay state isolation enabled",
            mode=mode,
            **applied_state_overrides,
        )
    symbol_tuple = tuple(x.strip() for x in symbols.split(",") if x.strip())
    if symbols_from_config_universe:
        from src.data.fiat_currencies import has_disallowed_base

        if config.coin_universe and getattr(config.coin_universe, "get_all_candidates", None):
            symbol_tuple = tuple(
                s for s in config.coin_universe.get_all_candidates() if not has_disallowed_base(s)
            )
    elif symbols_from_live_universe:
        from src.data.fiat_currencies import has_disallowed_base

        if config.assets.mode == "whitelist":
            symbol_tuple = tuple(config.assets.whitelist)
        elif config.coin_universe and config.coin_universe.enabled:
            symbol_tuple = tuple(
                s for s in config.coin_universe.get_all_candidates() if not has_disallowed_base(s)
            )
    window_offsets_tuple = tuple(int(x.strip()) for x in window_offsets.split(",") if x.strip())
    replay_seeds_tuple = tuple(int(x.strip()) for x in replay_seeds.split(",") if x.strip())
    replay_timeframes_tuple = tuple(x.strip() for x in replay_timeframes.split(",") if x.strip())
    if not symbol_tuple:
        typer.secho("❌ At least one symbol is required.", fg=typer.colors.RED, bold=True)
        raise typer.Exit(1)
    if not window_offsets_tuple:
        typer.secho("❌ At least one --window-offsets value is required.", fg=typer.colors.RED, bold=True)
        raise typer.Exit(1)
    if not replay_seeds_tuple:
        typer.secho("❌ At least one --replay-seeds value is required.", fg=typer.colors.RED, bold=True)
        raise typer.Exit(1)
    if mode not in {"backtest", "replay", "mock"}:
        typer.secho("❌ --mode must be one of: backtest, replay, mock", fg=typer.colors.RED, bold=True)
        raise typer.Exit(1)
    if objective_mode not in {"risk_adjusted", "net_pnl_only"}:
        typer.secho("❌ --objective-mode must be one of: risk_adjusted, net_pnl_only", fg=typer.colors.RED, bold=True)
        raise typer.Exit(1)
    if not replay_timeframes_tuple:
        typer.secho("❌ At least one --replay-timeframes value is required.", fg=typer.colors.RED, bold=True)
        raise typer.Exit(1)

    from src.research.harness import HarnessConfig, run_sandbox_autoresearch
    from src.research.state_store import ResearchStateStore
    from src.research.telegram_router import ResearchTelegramRouter
    from src.monitoring.telegram_bot import TelegramCommandHandler

    async def _run() -> None:
        store = ResearchStateStore(state_file)
        cfg = HarnessConfig(
            iterations=iterations,
            digest_every=digest_every,
            out_dir=str(out_dir),
            lookback_days=days,
            symbols=symbol_tuple,
            evaluation_mode=mode,
            enable_telegram=telegram,
            evaluation_window_offsets_days=window_offsets_tuple,
            holdout_ratio=holdout_ratio,
            auto_replay_gate=auto_replay_gate,
            replay_gate_seeds=replay_seeds_tuple,
            replay_data_dir=replay_data_dir,
            replay_gate_timeout_seconds=replay_timeout_seconds,
            replay_eval_timeout_seconds=replay_timeout_seconds,
            auto_queue_promotion_on_replay_pass=auto_queue_promotion,
            objective_mode=objective_mode,
            symbol_by_symbol=symbol_by_symbol,
            until_convergence=until_convergence,
            max_stagnant_iterations=max_stagnant_iterations,
            max_iterations_per_symbol=max_iterations_per_symbol,
            auto_backfill_data=auto_backfill_data,
            replay_timeframes=replay_timeframes_tuple,
        )

        telegram_task = None
        telegram_handler = None
        if telegram:
            router = ResearchTelegramRouter(store)

            async def _empty_status_provider() -> dict:
                return {}

            telegram_handler = TelegramCommandHandler(
                data_provider=_empty_status_provider,
                command_router=router.handle_command,
            )
            telegram_task = asyncio.create_task(telegram_handler.run())

        try:
            leaderboard_path, summary_path = await run_sandbox_autoresearch(
                base_config=config,
                harness_config=cfg,
                state_store=store,
            )
            typer.echo(f"Leaderboard: {leaderboard_path}")
            typer.echo(f"Summary: {summary_path}")
            typer.echo(f"State: {state_file}")
        finally:
            if telegram_handler is not None:
                telegram_handler.stop()
            if telegram_task is not None:
                telegram_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await telegram_task

    import asyncio
    import contextlib

    asyncio.run(_run())


@app.command("counterfactual-twin")
def counterfactual_twin(
    hours: int = typer.Option(24, "--hours", min=1, help="Hours of decision tape to analyze"),
    symbols: str = typer.Option("", "--symbols", help="Optional comma-separated symbol filter"),
    params_file: Optional[Path] = typer.Option(
        None,
        "--params-file",
        help="JSON file with candidate dot-path params (e.g. strategy.min_score_...)",
    ),
    out_file: Path = typer.Option(
        Path("data/research/counterfactual_twin.json"),
        "--out-file",
        help="Output JSON report path",
    ),
    config_path: Path = typer.Option("src/config/config.yaml", "--config", help="Path to config file"),
):
    """Run Counterfactual Live Twin uplift analysis on captured decision tape."""
    config = _load_config(config_path)
    _setup_logging_from_config(config)
    symbol_tuple = tuple(x.strip() for x in symbols.split(",") if x.strip())
    candidate_params: dict[str, float] = {}
    if params_file:
        if not params_file.exists():
            typer.secho(f"❌ params file not found: {params_file}", fg=typer.colors.RED, bold=True)
            raise typer.Exit(1)
        raw = json.loads(params_file.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            typer.secho("❌ params file must contain a JSON object", fg=typer.colors.RED, bold=True)
            raise typer.Exit(1)
        candidate_params = {str(k): float(v) for k, v in raw.items()}

    from src.research.counterfactual_twin import evaluate_counterfactual_uplift, load_decision_tape

    tape = load_decision_tape(since_hours=hours, symbols=symbol_tuple if symbol_tuple else None)
    report = evaluate_counterfactual_uplift(
        base_config=config,
        candidate_params=candidate_params,
        tape=tape,
    )
    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "hours": hours,
        "symbols": list(symbol_tuple),
        "candidate_params": candidate_params,
        "report": report,
    }
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    typer.echo(f"Counterfactual twin report: {out_file}")
    typer.echo(
        f"Utility uplift={report['utility_uplift']:.4f} "
        f"(baseline_open={report['baseline_open_count']} candidate_open={report['candidate_open_count']})"
    )


@app.command("counterfactual-twin-batch")
def counterfactual_twin_batch(
    hours: int = typer.Option(24, "--hours", min=1, help="Hours of decision tape to analyze"),
    symbols: str = typer.Option("", "--symbols", help="Optional comma-separated symbol filter"),
    candidates_dir: Path = typer.Option(
        Path("data/research/counterfactual_twin/candidates"),
        "--candidates-dir",
        help="Directory containing candidate param JSON files",
    ),
    top_n: int = typer.Option(10, "--top-n", min=1, help="How many top candidates to print"),
    out_file: Path = typer.Option(
        Path("data/research/counterfactual_twin/batch_ranking.json"),
        "--out-file",
        help="Output JSON ranking path",
    ),
    config_path: Path = typer.Option("src/config/config.yaml", "--config", help="Path to config file"),
):
    """Rank many candidate JSON files against the same decision tape."""
    config = _load_config(config_path)
    _setup_logging_from_config(config)
    symbol_tuple = tuple(x.strip() for x in symbols.split(",") if x.strip())
    if not candidates_dir.exists():
        typer.secho(f"❌ candidates directory not found: {candidates_dir}", fg=typer.colors.RED, bold=True)
        raise typer.Exit(1)
    candidate_files = sorted(candidates_dir.glob("*.json"))
    if not candidate_files:
        typer.secho(f"❌ no candidate JSON files found in: {candidates_dir}", fg=typer.colors.RED, bold=True)
        raise typer.Exit(1)

    from src.research.counterfactual_twin import evaluate_candidate_batch, load_decision_tape

    tape = load_decision_tape(since_hours=hours, symbols=symbol_tuple if symbol_tuple else None)
    candidates: dict[str, dict[str, float]] = {}
    skipped: list[dict[str, str]] = []
    for fpath in candidate_files:
        try:
            raw = json.loads(fpath.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("JSON root must be object")
            candidates[fpath.stem] = {str(k): float(v) for k, v in raw.items()}
        except Exception as exc:  # noqa: BLE001
            skipped.append({"file": str(fpath), "reason": str(exc)})

    ranking = evaluate_candidate_batch(base_config=config, tape=tape, candidates=candidates)
    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "hours": hours,
        "symbols": list(symbol_tuple),
        "tape_samples": len(tape),
        "candidate_count": len(candidates),
        "skipped": skipped,
        "ranking": ranking,
    }
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    typer.echo(f"Counterfactual twin batch ranking: {out_file}")
    if not ranking:
        typer.echo("No valid candidates to rank.")
        return
    typer.echo("Top candidates:")
    for idx, row in enumerate(ranking[:top_n], start=1):
        typer.echo(
            f"{idx:>2}. {row['candidate_id']}: uplift={row['utility_uplift']:.4f} "
            f"delta_open={row['delta_open_count']} eligible={row['eligible_opportunities']}"
        )



@app.command("falsification-random-entry")
def falsification_random_entry(
    symbols: str = typer.Option(..., "--symbols", help="Comma-separated symbols"),
    days: int = typer.Option(120, "--days", help="Lookback days"),
    data_dir: Path = typer.Option(Path("data/replay"), "--data-dir", help="Replay data directory"),
    out_file: Path = typer.Option(
        Path("data/research/falsification_random_entry.json"), "--out-file",
    ),
    trials: int = typer.Option(5, "--trials", help="Number of random trials"),
    signal_prob: Optional[float] = typer.Option(
        None,
        "--signal-prob",
        help="Optional Bernoulli signal probability override. Defaults to using the strategy signal schedule.",
    ),
    strategy_signal_file: Optional[Path] = typer.Option(
        None,
        "--strategy-signal-file",
        help="Optional falsification-signal-accuracy artifact to match signal timing/frequency.",
    ),
    timeframes: str = typer.Option("15m,1h,4h,1d", "--timeframes"),
    config_path: Path = typer.Option("src/config/config.yaml", "--config"),
):
    """Run random-entry baseline falsification test."""
    import asyncio
    config = _load_config(config_path)
    _setup_logging_from_config(config)
    sym_list = [s.strip() for s in symbols.split(",") if s.strip()]
    tf_list = [t.strip() for t in timeframes.split(",") if t.strip()]

    from src.research.falsification_random_entry import run_falsification

    result = asyncio.get_event_loop().run_until_complete(
        run_falsification(
            data_dir=data_dir,
            symbols=sym_list,
            days=days,
            num_trials=trials,
            signal_probability=signal_prob,
            timeframes=tf_list,
            strategy_signal_file=strategy_signal_file or out_file.with_name("falsification_signal_accuracy.json"),
        )
    )
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    typer.echo(f"Random entry baseline: {out_file}")
    avg = result.get("random_mean") or {}
    typer.echo(
        f"  Random mean: return={avg.get('net_return_pct', 0):.2f}% "
        f"wr={avg.get('win_rate_pct', 0):.1f}% "
        f"trades={avg.get('trade_count', 0):.0f}"
    )


@app.command("falsification-signal-accuracy")
def falsification_signal_accuracy(
    symbols: str = typer.Option(..., "--symbols", help="Comma-separated symbols"),
    days: int = typer.Option(120, "--days", help="Lookback days"),
    data_dir: Path = typer.Option(Path("data/replay"), "--data-dir", help="Replay data directory"),
    out_file: Path = typer.Option(
        Path("data/research/falsification_signal_accuracy.json"), "--out-file",
    ),
    timeframes: str = typer.Option("15m,1h,4h,1d", "--timeframes"),
    config_path: Path = typer.Option("src/config/config.yaml", "--config"),
):
    """Run signal directional accuracy falsification test."""
    import asyncio
    config = _load_config(config_path)
    _setup_logging_from_config(config)
    sym_list = [s.strip() for s in symbols.split(",") if s.strip()]
    tf_list = [t.strip() for t in timeframes.split(",") if t.strip()]

    from src.research.falsification_signal_accuracy import run_falsification

    result = asyncio.get_event_loop().run_until_complete(
        run_falsification(
            data_dir=data_dir,
            symbols=sym_list,
            days=days,
            timeframes=tf_list,
            strategy_config=config.strategy,
        )
    )
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    typer.echo(f"Signal accuracy report: {out_file}")
    edge = result.get("edge_assessment", {})
    typer.echo(
        f"  Signals: {result.get('total_signals', 0)} | "
        f"Best horizon: {edge.get('best_horizon')} "
        f"hit_rate={edge.get('best_hit_rate', 0):.1%} "
        f"p={edge.get('best_p_value', 1):.4f} | "
        f"Has edge: {edge.get('has_directional_edge')}"
    )


@app.callback()
def main(
    version: bool = typer.Option(False, "--version", help="Show version and exit"),
):
    """
    Kraken Futures SMC Trading System
    
    A professional algorithmic trading system for Kraken Futures perpetual contracts.
    """
    if version:
        typer.echo("Kraken Futures SMC Trading System v1.0.0")
        raise typer.Exit()


if __name__ == "__main__":
    app()
