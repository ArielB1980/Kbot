"""
Health monitoring operations extracted from LiveTrading.

Functions in this module handle:
- Order polling (pending entry orders)
- Protection checks (naked position detection with escalation)
- System status reporting (for Telegram commands)
- Daily P&L summary (midnight UTC)
- Startup position protection validation
- Auto-recovery from margin-critical kill switch

All functions receive the LiveTrading instance as their first argument (``lt``)
to access shared state, following the same delegate pattern used by
protection_ops, signal_handler, auction_runner, and exchange_sync.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Dict

from src.exceptions import OperationalError, DataError
from src.execution.equity import calculate_effective_equity
from src.monitoring.logger import get_logger
from src.utils.kill_switch import KillSwitchReason

if TYPE_CHECKING:
    from src.live.live_trading import LiveTrading

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Order polling
# ---------------------------------------------------------------------------

async def run_order_polling(lt: "LiveTrading", interval_seconds: int = 12) -> None:
    """Poll pending entry order status, process fills, trigger PLACE_STOP (SL/TP)."""
    while lt.active:
        await asyncio.sleep(interval_seconds)
        if not lt.active:
            break
        if not lt.execution_gateway:
            continue
        try:
            n = await lt.execution_gateway.poll_and_process_order_updates()
            if n > 0:
                logger.info("Order poll processed updates", count=n)
        except asyncio.CancelledError:
            raise
        except (OperationalError, DataError) as e:
            logger.warning("Order poll failed", error=str(e))


# ---------------------------------------------------------------------------
# Protection checks
# ---------------------------------------------------------------------------

async def run_protection_checks(lt: "LiveTrading", interval_seconds: int = 30) -> None:
    """
    V2 protection monitor loop with escalation policy.

    If a naked position is detected in prod live, attempt self-healing
    (place missing stop) before escalating to kill switch.

    Escalation ladder:
      1-4 consecutive detections → WARN, let main loop / self-heal fix it
      5   consecutive detections → attempt to place missing stops ourselves
      6+  still naked after heal → activate kill switch

    Startup grace: The first check is delayed by 3x the normal interval (90s
    by default) to give the main tick loop time to place missing stops after a
    restart or kill switch recovery.
    """
    startup_grace_seconds = interval_seconds * 3
    logger.info(
        "Protection monitor: startup grace period",
        grace_seconds=startup_grace_seconds,
        enforce_after="first check",
    )
    await asyncio.sleep(startup_grace_seconds)

    consecutive_naked_count: Dict[str, int] = {}
    # Escalation thresholds:
    # - At HEAL_THRESHOLD: attempt to place missing stops (self-heal)
    # - At KILL_THRESHOLD: if still naked after heal attempt, activate kill switch
    # At 30s intervals: heal at 150s, kill at 180s — gives the system 2.5 minutes
    # to self-recover before any destructive action.
    HEAL_THRESHOLD = 5
    KILL_THRESHOLD = 6
    _heal_attempted = False

    # ── Self-heal counters (readable via lt for dashboards / alerts) ──
    from src.monitoring.alerting import send_alert
    
    # Stored on the LiveTrading object so they survive across function calls
    # and are accessible from other monitors.
    if not hasattr(lt, "_stop_heal_metrics"):
        lt._stop_heal_metrics = {
            "stop_self_heal_attempts_total": 0,
            "stop_self_heal_success_total": 0,
            "stop_self_heal_failures_total": 0,
        }

    while lt.active:
        if not lt.active:
            break
        if not getattr(lt, "_protection_monitor", None):
            await asyncio.sleep(interval_seconds)
            continue
        try:
            results = await lt._protection_monitor.check_all_positions()
            naked = [s for s, ok in results.items() if not ok]
            if naked:
                for s in naked:
                    consecutive_naked_count[s] = consecutive_naked_count.get(s, 0) + 1

                max_count = max(consecutive_naked_count.get(s, 0) for s in naked)

                # ── TIER 3: Kill switch (heal failed) ──
                persistent_naked = [
                    s
                    for s in naked
                    if consecutive_naked_count.get(s, 0) >= KILL_THRESHOLD
                ]

                if persistent_naked:
                    logger.critical(
                        "NAKED_POSITIONS_DETECTED (persistent, self-heal failed)",
                        naked_symbols=persistent_naked,
                        details=results,
                        consecutive_counts={
                            s: consecutive_naked_count[s] for s in persistent_naked
                        },
                    )
                    is_prod_live = (
                        os.getenv("ENVIRONMENT", "").strip().lower() == "prod"
                    ) and (not lt.config.system.dry_run)
                    if is_prod_live:
                        await lt.kill_switch.activate(
                            KillSwitchReason.RECONCILIATION_FAILURE, emergency=True
                        )
                        return
                    continue

                # ── TIER 2: Self-heal attempt (place missing stops) ──
                heal_candidates = [
                    s
                    for s in naked
                    if consecutive_naked_count.get(s, 0) >= HEAL_THRESHOLD
                ]

                if heal_candidates and not _heal_attempted:
                    _heal_attempted = True
                    metrics = lt._stop_heal_metrics
                    metrics["stop_self_heal_attempts_total"] += 1
                    logger.warning(
                        "NAKED_POSITIONS: attempting self-heal (placing missing stops)",
                        naked_symbols=heal_candidates,
                        consecutive_counts={
                            s: consecutive_naked_count[s] for s in heal_candidates
                        },
                        stop_self_heal_attempts_total=metrics["stop_self_heal_attempts_total"],
                    )
                    try:
                        raw_positions = await lt.client.get_all_futures_positions()
                        from src.live.protection_ops import place_missing_stops_for_unprotected
                        await place_missing_stops_for_unprotected(
                            lt, raw_positions, max_per_tick=10
                        )
                        # Brief pause for exchange to process the new orders
                        await asyncio.sleep(5)
                        # Re-check immediately
                        results2 = await lt._protection_monitor.check_all_positions()
                        still_naked = [s for s, ok in results2.items() if not ok]
                        if not still_naked:
                            metrics["stop_self_heal_success_total"] += 1
                            logger.info(
                                "Self-heal SUCCESS: naked positions now protected",
                                healed_symbols=heal_candidates,
                                stop_self_heal_success_total=metrics["stop_self_heal_success_total"],
                                stop_self_heal_attempts_total=metrics["stop_self_heal_attempts_total"],
                            )
                            try:
                                await send_alert(
                                    "SELF_HEAL_SUCCESS",
                                    f"Stop self-heal succeeded\n"
                                    f"Healed: {', '.join(heal_candidates)}\n"
                                    f"Attempts: {metrics['stop_self_heal_attempts_total']}",
                                    urgent=False,
                                )
                            except (OperationalError, ImportError, OSError):
                                pass
                            consecutive_naked_count.clear()
                            _heal_attempted = False
                            await asyncio.sleep(interval_seconds)
                            continue
                        else:
                            metrics["stop_self_heal_failures_total"] += 1
                            logger.warning(
                                "Self-heal PARTIAL: some positions still naked after stop placement",
                                still_naked=still_naked,
                                healed=[s for s in heal_candidates if s not in still_naked],
                                stop_self_heal_failures_total=metrics["stop_self_heal_failures_total"],
                                stop_self_heal_attempts_total=metrics["stop_self_heal_attempts_total"],
                            )
                            try:
                                await send_alert(
                                    "SELF_HEAL_PARTIAL",
                                    f"Stop self-heal partial — still naked:\n"
                                    f"{', '.join(still_naked)}\n"
                                    f"Healed: {', '.join(s for s in heal_candidates if s not in still_naked) or 'none'}\n"
                                    f"Failures: {metrics['stop_self_heal_failures_total']}",
                                    urgent=True,
                                )
                            except (OperationalError, ImportError, OSError):
                                pass
                            # Let it escalate to KILL_THRESHOLD on next iteration
                    except (OperationalError, DataError) as e:
                        metrics["stop_self_heal_failures_total"] += 1
                        logger.error(
                            "Self-heal FAILED: could not place missing stops",
                            error=str(e),
                            error_type=type(e).__name__,
                            stop_self_heal_failures_total=metrics["stop_self_heal_failures_total"],
                            stop_self_heal_attempts_total=metrics["stop_self_heal_attempts_total"],
                        )
                        try:
                            await send_alert(
                                "SELF_HEAL_FAILED",
                                f"Stop self-heal FAILED\n"
                                f"Symbols: {', '.join(heal_candidates)}\n"
                                f"Error: {str(e)[:100]}\n"
                                f"Failures: {metrics['stop_self_heal_failures_total']}",
                                urgent=True,
                            )
                        except (OperationalError, ImportError, OSError):
                            pass
                    continue

                # ── TIER 1: Warning (give time to self-heal) ──
                logger.warning(
                    "NAKED_POSITIONS_DETECTED (monitoring, self-heal pending)",
                    naked_symbols=naked,
                    details=results,
                    consecutive_counts={
                        s: consecutive_naked_count.get(s, 0) for s in naked
                    },
                    heal_at=HEAL_THRESHOLD,
                    kill_at=KILL_THRESHOLD,
                )
            else:
                if consecutive_naked_count:
                    logger.info(
                        "Naked position counters cleared (all positions protected)",
                        previous_counts=dict(consecutive_naked_count),
                    )
                consecutive_naked_count.clear()
                _heal_attempted = False

            # ── Periodic metrics snapshot (every check, in structured log) ──
            # Allows grep / alerting on systemic instability.
            metrics = lt._stop_heal_metrics
            layer3_saves = (
                lt._protection_monitor.layer3_saves_total
                if getattr(lt, "_protection_monitor", None)
                and hasattr(lt._protection_monitor, "layer3_saves_total")
                else 0
            )
            if (
                metrics["stop_self_heal_attempts_total"] > 0
                or layer3_saves > 0
            ):
                logger.info(
                    "STOP_HEAL_METRICS",
                    stop_self_heal_attempts_total=metrics["stop_self_heal_attempts_total"],
                    stop_self_heal_success_total=metrics["stop_self_heal_success_total"],
                    stop_self_heal_failures_total=metrics["stop_self_heal_failures_total"],
                    layer3_saves_total=layer3_saves,
                )
        except asyncio.CancelledError:
            raise
        except (OperationalError, DataError) as e:
            logger.warning(
                "Protection check loop failed",
                error=str(e),
                error_type=type(e).__name__,
            )

        await asyncio.sleep(interval_seconds)


# ---------------------------------------------------------------------------
# Trade starvation monitor
# ---------------------------------------------------------------------------

async def run_trade_starvation_monitor(
    lt: "LiveTrading",
    check_interval_seconds: int = 300,
    *,
    starvation_window_hours: float = 6.0,
    min_signals_threshold: int = 10,
) -> None:
    """
    Alert if the system generates signals but never executes trades.

    This catches silent regressions where the signal pipeline works
    but the execution pipeline is blocked (sizing bug, auction deadlock,
    exchange rejection loop, missing futures mapping, etc.).

    Logic:
        Every ``check_interval_seconds`` (default 5 min), look at the
        rolling window of ``starvation_window_hours``.  If
        ``signals_generated >= min_signals_threshold`` AND
        ``orders_placed == 0`` over that window, fire an alert.

    The monitor tracks per-cycle stats from the CycleGuard (via the
    safety integration layer) and from the auction runner logs.
    """
    from src.monitoring.alerting import send_alert

    # Rolling window accumulators: (cycle_end_ts, signals, orders_placed)
    _history: list[tuple[datetime, int, int]] = []
    _alerted = False  # De-duplicate: only alert once per starvation episode

    # Let the system warm up before checking
    await asyncio.sleep(max(check_interval_seconds, 120))

    while lt.active:
        try:
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(hours=starvation_window_hours)

            # Collect current cycle stats from the CycleGuard
            hardening = getattr(lt, "hardening_layer", None)
            if hardening and hasattr(hardening, "cycle_guard") and hardening.cycle_guard:
                recent = hardening.cycle_guard.get_recent_cycles(limit=200)
                # Rebuild history from CycleGuard data
                _history.clear()
                for cycle in recent:
                    ts = cycle.get("started_at") or cycle.get("ended_at")
                    if ts and isinstance(ts, str):
                        try:
                            ts = datetime.fromisoformat(ts)
                        except (ValueError, TypeError):
                            continue
                    if ts is None:
                        continue
                    sig = cycle.get("signals_generated", 0)
                    orders = cycle.get("orders_placed", 0)
                    _history.append((ts, sig, orders))

            # Prune old entries
            _history[:] = [(ts, s, o) for ts, s, o in _history if ts >= cutoff]

            window_signals = sum(s for _, s, _ in _history)
            window_orders = sum(o for _, _, o in _history)

            # Source-of-truth backstop: cycle history is bounded (e.g. 100 cycles).
            # If cycle window says 0 orders but DB shows activity in the same time
            # window, do not alarm.  Two checks cover both halves:
            #   1. count_trades_opened_since  → closed trades entered in window
            #   2. count_open_positions_opened_since → still-open positions entered in window
            # Either > 0 means "not starved".
            db_evidence = 0
            try:
                from src.storage.repository import (
                    count_trades_opened_since,
                    count_open_positions_opened_since,
                )
                closed_in_window = await asyncio.to_thread(count_trades_opened_since, cutoff)
                open_in_window = await asyncio.to_thread(count_open_positions_opened_since, cutoff)
                db_evidence = closed_in_window + open_in_window
            except (OperationalError, DataError, OSError):
                pass  # If DB unavailable, rely on cycle history only

            if window_signals >= min_signals_threshold and window_orders == 0:
                if db_evidence > 0:
                    logger.debug(
                        "Trade starvation check: cycle history shows 0 orders but DB has activity in window, not alarming",
                        db_closed_trades=closed_in_window if db_evidence else 0,
                        db_open_positions=open_in_window if db_evidence else 0,
                        window_hours=starvation_window_hours,
                    )
                elif not _alerted:
                    msg = (
                        f"TRADE STARVATION: {window_signals} signals generated "
                        f"but 0 orders placed in the last {starvation_window_hours:.0f}h.\n"
                        f"Possible causes: sizing rejection, auction deadlock, "
                        f"exchange mapping failure, or risk pipeline bug."
                    )
                    logger.critical(
                        "TRADE_STARVATION_DETECTED",
                        signals=window_signals,
                        orders=window_orders,
                        window_hours=starvation_window_hours,
                    )
                    await send_alert("TRADE_STARVATION", msg, urgent=True)
                    _alerted = True
            else:
                if _alerted and window_orders > 0:
                    logger.info(
                        "Trade starvation resolved",
                        signals=window_signals,
                        orders=window_orders,
                    )
                    _alerted = False

            # Periodic health log (debug level)
            logger.debug(
                "Trade starvation check",
                window_signals=window_signals,
                window_orders=window_orders,
                db_evidence=db_evidence,
                window_hours=starvation_window_hours,
                alerted=_alerted,
            )

        except asyncio.CancelledError:
            raise
        except (OperationalError, DataError) as e:
            logger.warning(
                "Trade starvation monitor failed",
                error=str(e),
                error_type=type(e).__name__,
            )

        await asyncio.sleep(check_interval_seconds)


# ---------------------------------------------------------------------------
# Winner churn monitor
# ---------------------------------------------------------------------------

async def run_winner_churn_monitor(
    lt: "LiveTrading",
    check_interval_seconds: int = 300,
    *,
    max_wins_without_entry: int = 5,
    decay_hours: float = 12.0,
) -> None:
    """
    Alert if the same symbol wins the auction repeatedly without ever
    getting an entry executed.

    This catches the AXS-style deadlock regression: a symbol scores
    highest every cycle, wins the auction, but is always rejected at
    execution (basis guard, min-notional, exchange error, etc.).  The
    auction keeps picking it, starving other contenders.

    Logic:
        Track ``(symbol -> [win_timestamps])`` from auction plan logs.
        Track ``(symbol -> last_entry_ts)`` from successful opens.
        If a symbol has >= ``max_wins_without_entry`` wins in
        ``decay_hours`` without a single successful entry, fire an alert.
    """
    from src.monitoring.alerting import send_alert

    # symbols already alerted (to de-duplicate)
    _alerted_symbols: set[str] = set()

    # Warm-up
    await asyncio.sleep(max(check_interval_seconds, 120))

    while lt.active:
        try:
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(hours=decay_hours)

            # ---- Read auction tracking data from LiveTrading ----
            # lt._auction_win_log: Dict[symbol, list[datetime]] populated by auction_runner
            # lt._auction_entry_log: Dict[symbol, datetime] populated by auction_runner
            win_log = getattr(lt, "_auction_win_log", {})
            entry_log = getattr(lt, "_auction_entry_log", {})
            open_block_log = getattr(lt, "_auction_open_block_reason_by_symbol", {})

            # ---- Prune old win entries ----
            for sym in list(win_log):
                win_log[sym] = [t for t in win_log[sym] if t >= cutoff]
                if not win_log[sym]:
                    del win_log[sym]

            # ---- Check for churn ----
            churn_symbols = []
            for sym, wins in win_log.items():
                if len(wins) < max_wins_without_entry:
                    continue
                # Has this symbol had a successful entry in the window?
                last_entry = entry_log.get(sym)
                if last_entry and last_entry >= cutoff:
                    continue  # Entry happened, not churning
                churn_symbols.append((sym, len(wins)))

            if churn_symbols:
                new_churn = [
                    (sym, count)
                    for sym, count in churn_symbols
                    if sym not in _alerted_symbols
                ]
                if new_churn:
                    symbols_str = ", ".join(
                        f"{sym} ({count} wins)" for sym, count in new_churn
                    )
                    blocker_parts = []
                    blocker_summary = {}
                    for sym, _count in new_churn:
                        block_ctx = open_block_log.get(sym)
                        reason = None
                        if isinstance(block_ctx, dict):
                            reason = block_ctx.get("reason")
                        if reason:
                            blocker_parts.append(f"{sym}: {reason}")
                            blocker_summary[sym] = reason
                    blocker_hint = (
                        f"\nObserved blockers: {'; '.join(blocker_parts)}"
                        if blocker_parts
                        else ""
                    )
                    msg = (
                        f"WINNER CHURN: {symbols_str} won the auction "
                        f">={max_wins_without_entry} times in {decay_hours:.0f}h "
                        f"without a single entry.\n"
                        f"Possible causes: persistent risk rejection, basis guard, "
                        f"exchange min-notional, missing futures mapping."
                        f"{blocker_hint}"
                    )
                    logger.warning(
                        "WINNER_CHURN_DETECTED",
                        churn_symbols=[(s, c) for s, c in new_churn],
                        observed_blockers=blocker_summary or None,
                        window_hours=decay_hours,
                    )
                    await send_alert("WINNER_CHURN", msg, urgent=False)
                    for sym, _ in new_churn:
                        _alerted_symbols.add(sym)

            # Clear alerts for symbols that resolved (got an entry)
            resolved = _alerted_symbols - {sym for sym, _ in churn_symbols}
            if resolved:
                logger.info("Winner churn resolved", symbols=list(resolved))
            _alerted_symbols -= resolved

            logger.debug(
                "Winner churn check",
                tracked_symbols=len(win_log),
                churn_count=len(churn_symbols),
                alerted=len(_alerted_symbols),
            )

        except asyncio.CancelledError:
            raise
        except (OperationalError, DataError) as e:
            logger.warning(
                "Winner churn monitor failed",
                error=str(e),
                error_type=type(e).__name__,
            )

        await asyncio.sleep(check_interval_seconds)


# ---------------------------------------------------------------------------
# Trade recording invariant monitor
# ---------------------------------------------------------------------------

async def run_trade_recording_monitor(
    lt: "LiveTrading",
    check_interval_seconds: int = 300,
) -> None:
    """
    Advisory monitor: alert if positions are closing but no trades are
    being recorded to the database.

    Invariant: if position_fills exist in the last 24 h (from the SQLite
    persistence layer) AND the Postgres trades table has 0 new rows in the
    same window, something is broken.

    This does NOT trigger the kill switch — it logs an ERROR and (optionally)
    sends a Telegram alert.
    """
    _alerted = False

    # Warm-up: let the system start and record at least one cycle
    await asyncio.sleep(max(check_interval_seconds, 180))

    while lt.active:
        try:
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(hours=24)

            # Source 1: closed positions in recent history (SQLite registry)
            # Only count positions where trade recording has NOT completed.
            # Positions with trade_recorded=True have been processed (either
            # a Trade was persisted, or the recorder determined nothing to
            # record, e.g. reconciliation closures with 0 fills).
            recent_closes = 0
            if lt.position_registry:
                for pos in lt.position_registry.get_closed_history(limit=200):
                    exit_t = pos.exit_time
                    if exit_t and exit_t >= cutoff and not pos.trade_recorded:
                        recent_closes += 1

            # Source 2: trades recorded in Postgres
            trades_in_window = 0
            try:
                from src.storage.repository import get_trades_since
                trades = await asyncio.to_thread(get_trades_since, cutoff)
                trades_in_window = len(trades)
            except (OperationalError, DataError, OSError):
                pass  # DB may not be available

            # Check gateway failure counter
            record_failures = 0
            if lt.execution_gateway:
                record_failures = lt.execution_gateway.metrics.get("trade_record_failures_total", 0)

            # Invariant check 1: positions closing but no trades recorded
            if recent_closes > 0 and trades_in_window == 0 and not _alerted:
                _alerted = True
                logger.error(
                    "TRADE_RECORDING_INVARIANT_VIOLATION: positions closed but no trades recorded in 24h",
                    recent_closes=recent_closes,
                    trades_recorded=trades_in_window,
                    record_failures=record_failures,
                    window_hours=24,
                )
                try:
                    from src.monitoring.alerting import send_alert
                    await send_alert(
                        "TRADE_RECORDING_STALL",
                        f"{recent_closes} positions closed in 24h but 0 trades recorded.\n"
                        f"Record failures: {record_failures}\n"
                        "Check trade_recorder logs for TRADE_RECORD_FAILURE.",
                        urgent=False,
                    )
                except (OperationalError, ImportError, OSError):
                    pass
            elif trades_in_window > 0 and _alerted:
                _alerted = False
                logger.info(
                    "Trade recording invariant restored",
                    trades_recorded=trades_in_window,
                )

            # Invariant check 2: any recording failures in this process lifetime
            if record_failures > 0:
                logger.warning(
                    "Trade record failures detected",
                    trade_record_failures_total=record_failures,
                    trades_recorded=trades_in_window,
                )

            logger.debug(
                "Trade recording check",
                recent_closes=recent_closes,
                trades_recorded=trades_in_window,
                record_failures=record_failures,
                alerted=_alerted,
            )

        except asyncio.CancelledError:
            raise
        except (OperationalError, DataError) as e:
            logger.warning(
                "Trade recording monitor failed",
                error=str(e),
                error_type=type(e).__name__,
            )

        await asyncio.sleep(check_interval_seconds)


# ---------------------------------------------------------------------------
# System status (Telegram data provider)
# ---------------------------------------------------------------------------

async def get_system_status(lt: "LiveTrading") -> dict:
    """
    Data provider for Telegram command handler.
    Returns current system state for /status and /positions commands.
    """
    result: dict = {
        "equity": Decimal("0"),
        "margin_used": Decimal("0"),
        "margin_pct": 0.0,
        "positions": [],
        "system_state": "UNKNOWN",
        "kill_switch_active": False,
        "cycle_count": getattr(lt, "_last_cycle_count", 0),
        "cooldowns_active": len(lt._signal_cooldown),
        "universe_size": len(lt._market_symbols()),
    }

    try:
        balance = await lt.client.get_futures_balance()
        base = getattr(lt.config.exchange, "base_currency", "USD")
        equity, available_margin, margin_used = await calculate_effective_equity(
            balance, base_currency=base, kraken_client=lt.client
        )
        result["equity"] = equity
        result["margin_used"] = margin_used
        result["margin_pct"] = (
            float((margin_used / equity) * 100) if equity > 0 else 0
        )
    except (OperationalError, DataError) as e:
        logger.warning("Status: failed to get equity", error=str(e))

    try:
        positions = await lt.client.get_all_futures_positions()
        active_positions = [p for p in positions if p.get("size", 0) != 0]
        
        # Enrich positions with mark prices and compute unrealized PnL
        # Kraken's openpositions endpoint does NOT return unrealizedPnl or markPrice
        if active_positions:
            try:
                mark_prices = await lt.client.get_futures_tickers_bulk()
                for p in active_positions:
                    sym = p.get("symbol", "")
                    mark = mark_prices.get(sym)
                    if mark is not None:
                        p["mark_price"] = mark
                        entry = p.get("entry_price", Decimal("0"))
                        size = p.get("size", Decimal("0"))
                        side = p.get("side", "long")
                        if side == "long":
                            p["unrealized_pnl"] = (mark - entry) * size
                        else:
                            p["unrealized_pnl"] = (entry - mark) * size
            except (ValueError, TypeError, ArithmeticError, KeyError) as e:
                logger.debug("Status: failed to enrich mark prices", error=str(e))
        
        result["positions"] = active_positions
    except (OperationalError, DataError) as e:
        logger.warning("Status: failed to get positions", error=str(e))

    # System state
    kill_active = lt.kill_switch.is_active() if lt.kill_switch else False
    result["kill_switch_active"] = kill_active
    if kill_active:
        result["system_state"] = "KILL_SWITCH"
    elif lt.hardening and hasattr(lt.hardening, "invariant_monitor"):
        inv_state = lt.hardening.invariant_monitor.state.value
        result["system_state"] = (
            inv_state.upper() if inv_state != "active" else "NORMAL"
        )
    else:
        result["system_state"] = "NORMAL"

    # Connection pool health
    try:
        from src.storage.db import get_pool_status
        result["db_pool"] = get_pool_status()
    except (OperationalError, DataError, OSError, ImportError):
        pass

    return result


# ---------------------------------------------------------------------------
# Daily P&L summary
# ---------------------------------------------------------------------------

async def run_daily_summary(lt: "LiveTrading") -> None:
    """
    Send a daily P&L summary via Telegram at midnight UTC.

    Calculates: equity, daily P&L, open positions, trades today, win rate.
    Runs in a background loop, sleeping until the next midnight.
    """
    from src.monitoring.alerting import send_alert

    while lt.active:
        try:
            now = datetime.now(timezone.utc)
            tomorrow = (now + timedelta(days=1)).replace(
                hour=0, minute=0, second=5, microsecond=0
            )
            sleep_seconds = (tomorrow - now).total_seconds()
            await asyncio.sleep(sleep_seconds)

            if not lt.active:
                break

            try:
                account_info = await lt.client.get_futures_account_info()
                equity = Decimal(str(account_info.get("equity", 0)))
                margin_used = Decimal(str(account_info.get("marginUsed", 0)))
                margin_pct = (
                    float((margin_used / equity) * 100) if equity > 0 else 0
                )

                positions = await lt.client.get_all_futures_positions()
                open_positions = [
                    p for p in positions if p.get("size", 0) != 0
                ]

                # Enrich open positions with mark prices and computed unrealized PnL
                if open_positions:
                    try:
                        mark_prices = await lt.client.get_futures_tickers_bulk()
                        for p in open_positions:
                            sym = p.get("symbol", "")
                            mark = mark_prices.get(sym)
                            if mark is not None:
                                p["mark_price"] = mark
                                entry = p.get("entry_price", Decimal("0"))
                                size = p.get("size", Decimal("0"))
                                sd = p.get("side", "long")
                                if sd == "long":
                                    p["unrealized_pnl"] = (mark - entry) * size
                                else:
                                    p["unrealized_pnl"] = (entry - mark) * size
                    except (ValueError, TypeError, ArithmeticError, KeyError):
                        pass  # Graceful fallback to 0

                today_trades = []
                try:
                    from src.storage.repository import get_trades_since

                    since = now - timedelta(hours=24)
                    all_trades = await asyncio.to_thread(get_trades_since, since)
                    today_trades = all_trades if all_trades else []
                except Exception as trades_err:
                    logger.warning(
                        "Failed to load trades for daily summary",
                        error=str(trades_err),
                        error_type=type(trades_err).__name__,
                    )

                wins = sum(
                    1 for t in today_trades if getattr(t, "net_pnl", 0) > 0
                )
                losses = sum(
                    1 for t in today_trades if getattr(t, "net_pnl", 0) <= 0
                )
                total_pnl = sum(
                    getattr(t, "net_pnl", Decimal("0")) for t in today_trades
                )
                win_rate = (
                    f"{(wins / (wins + losses) * 100):.0f}%"
                    if (wins + losses) > 0
                    else "N/A"
                )

                pnl_sign = "+" if total_pnl >= 0 else ""
                pnl_emoji = "\U0001f4c8" if total_pnl >= 0 else "\U0001f4c9"

                pos_lines = []
                for p in open_positions[:10]:
                    sym = p.get("symbol", "?")
                    side = p.get("side", "?")
                    upnl = Decimal(
                        str(
                            p.get(
                                "unrealizedPnl", p.get("unrealized_pnl", 0)
                            )
                        )
                    )
                    upnl_sign = "+" if upnl >= 0 else ""
                    pos_lines.append(
                        f"  \u2022 {sym} ({side}) {upnl_sign}${upnl:.2f}"
                    )

                positions_str = (
                    "\n".join(pos_lines) if pos_lines else "  None"
                )

                summary = (
                    f"{pnl_emoji} Daily Summary ({now.strftime('%Y-%m-%d')})\n"
                    f"\n"
                    f"Equity: ${equity:.2f}\n"
                    f"Margin used: {margin_pct:.1f}%\n"
                    f"\n"
                    f"Trades today: {len(today_trades)}\n"
                    f"Win/Loss: {wins}W / {losses}L ({win_rate})\n"
                    f"Day P&L: {pnl_sign}${total_pnl:.2f}\n"
                    f"\n"
                    f"Open positions ({len(open_positions)}):\n"
                    f"{positions_str}"
                )

                await send_alert("DAILY_SUMMARY", summary, urgent=True)
                logger.info(
                    "Daily summary sent",
                    equity=str(equity),
                    trades=len(today_trades),
                )

                lt.risk_manager.reset_daily_metrics(equity)

                # Daily position DB cleanup: remove terminal positions older than 30 days
                try:
                    if hasattr(lt, "position_persistence") and lt.position_persistence:
                        cleaned = lt.position_persistence.cleanup_old_positions(days=30)
                        if cleaned > 0:
                            logger.info("Daily position DB cleanup", removed=cleaned)
                except Exception as cleanup_err:
                    logger.warning("Position DB cleanup failed", error=str(cleanup_err))

            except (OperationalError, DataError) as e:
                logger.warning(
                    "Failed to gather daily summary data", error=str(e)
                )

        except asyncio.CancelledError:
            raise
        except (OperationalError, DataError) as e:
            logger.warning("Daily summary loop error", error=str(e))
            await asyncio.sleep(3600)


# ---------------------------------------------------------------------------
# Startup position protection validation
# ---------------------------------------------------------------------------

async def validate_position_protection(lt: "LiveTrading") -> None:
    """
    Validate all positions have protection (startup safety gate).

    Checks V2 position registry for unprotected positions.
    Emits alerts and optionally pauses trading.
    """
    from src.storage.repository import async_record_event

    unprotected = []
    tracked_symbols: set[str] = set()

    # V2: Check registry state (authoritative)
    for p in lt.position_registry.get_all_active():
        tracked_symbols.add(p.symbol)

        if p.remaining_qty <= 0:
            continue

        has_stop_price = p.current_stop_price is not None
        has_stop_order = p.stop_order_id is not None
        is_protected = bool(has_stop_price and has_stop_order)
        if not is_protected:
            unprotected.append(
                {
                    "symbol": p.symbol,
                    "source": "registry_v2",
                    "reason": "MISSING_STOP",
                    "has_sl_price": has_stop_price,
                    "has_sl_order": has_stop_order,
                    "is_protected": is_protected,
                    "remaining_qty": str(p.remaining_qty),
                }
            )

    if unprotected:
        logger.error(
            "UNPROTECTED positions detected",
            count=len(unprotected),
            positions=unprotected,
        )
        for up in unprotected:
            await async_record_event(
                "UNPROTECTED_POSITION", up["symbol"], up
            )
    else:
        logger.info(
            "All positions are protected",
            total_positions=len(tracked_symbols),
        )


# ---------------------------------------------------------------------------
# Auto-recovery from kill switch
# ---------------------------------------------------------------------------

async def try_auto_recovery(lt: "LiveTrading") -> bool:
    """
    Attempt automatic recovery from kill switch (margin_critical only).

    Rules (ALL must be true):
    1. Kill switch reason is MARGIN_CRITICAL
    2. At least 5 minutes since the halt was activated
    3. Current margin utilization is below 85% (well below 92% trigger)
    4. Fewer than 2 auto-recoveries in the last 24 hours

    Returns True if recovery was successful, False otherwise.
    """
    if not lt.kill_switch or not lt.kill_switch.is_active():
        return False

    if lt.kill_switch.reason != KillSwitchReason.MARGIN_CRITICAL:
        return False

    now = datetime.now(timezone.utc)

    # Rule 2: Cooldown since halt activation
    if lt.kill_switch.activated_at:
        elapsed = (now - lt.kill_switch.activated_at).total_seconds()
        if elapsed < lt._AUTO_RECOVERY_COOLDOWN_SECONDS:
            logger.debug(
                "Auto-recovery: waiting for cooldown",
                elapsed_seconds=int(elapsed),
                required_seconds=lt._AUTO_RECOVERY_COOLDOWN_SECONDS,
            )
            return False

    # Rule 4: Max recoveries per day
    cutoff = now - timedelta(hours=24)
    recent_attempts = [t for t in lt._auto_recovery_attempts if t > cutoff]
    lt._auto_recovery_attempts = recent_attempts  # Prune old entries
    if len(recent_attempts) >= lt._AUTO_RECOVERY_MAX_PER_DAY:
        logger.warning(
            "Auto-recovery: daily limit reached (system needs manual intervention)",
            attempts_today=len(recent_attempts),
            max_per_day=lt._AUTO_RECOVERY_MAX_PER_DAY,
        )
        return False

    # Rule 3: Check current margin utilization
    try:
        account_info = await lt.client.get_futures_account_info()
        equity = Decimal(str(account_info.get("equity", 0)))
        margin_used = Decimal(str(account_info.get("marginUsed", 0)))

        if equity <= 0:
            return False

        margin_util_pct = float((margin_used / equity) * 100)

        if margin_util_pct >= lt._AUTO_RECOVERY_MARGIN_SAFE_PCT:
            logger.info(
                "Auto-recovery: margin still too high",
                margin_util_pct=f"{margin_util_pct:.1f}",
                required_below=lt._AUTO_RECOVERY_MARGIN_SAFE_PCT,
            )
            return False

        # All conditions met -- recover!
        lt._auto_recovery_attempts.append(now)

        logger.critical(
            "AUTO_RECOVERY: Clearing kill switch (margin recovered)",
            margin_util_pct=f"{margin_util_pct:.1f}",
            recovery_attempt=len(lt._auto_recovery_attempts),
            max_per_day=lt._AUTO_RECOVERY_MAX_PER_DAY,
            halt_duration_seconds=(
                int((now - lt.kill_switch.activated_at).total_seconds())
                if lt.kill_switch.activated_at
                else 0
            ),
        )

        lt.kill_switch.acknowledge()

        if lt.hardening and lt.hardening.is_halted():
            lt.hardening.clear_halt(operator="auto_recovery")

        try:
            from src.monitoring.alerting import send_alert_sync

            send_alert_sync(
                "AUTO_RECOVERY",
                f"System auto-recovered from MARGIN_CRITICAL\n"
                f"Margin utilization: {margin_util_pct:.1f}%\n"
                f"Recovery #{len(lt._auto_recovery_attempts)} of {lt._AUTO_RECOVERY_MAX_PER_DAY}/day",
                urgent=True,
            )
        except (OperationalError, ImportError, OSError):
            pass

        return True

    except (OperationalError, DataError) as e:
        logger.warning(
            "Auto-recovery: failed to check margin", error=str(e)
        )
        return False
