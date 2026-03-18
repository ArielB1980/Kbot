"""
Auction-based portfolio allocation execution.

Extracted from live_trading.py to reduce god-object size.
"""
from __future__ import annotations

import asyncio
from collections import Counter
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List

from src.exceptions import OperationalError, DataError
from src.execution.equity import calculate_effective_equity
from src.live.policy_fingerprint import build_policy_hash
from src.monitoring.logger import get_logger
from src.storage.repository import get_active_position, get_trades_since

if TYPE_CHECKING:
    from src.live.live_trading import LiveTrading

logger = get_logger(__name__)


def _should_emit_replay_tick_log(lt: "LiveTrading", *, force: bool = False, every_cycles: int = 100) -> bool:
    """Reduce per-tick auction log volume in replay research mode."""
    if not bool(getattr(lt, "_replay_disable_candle_health_gate", False)):
        return True
    if force:
        return True
    cycle_num = int(
        getattr(lt, "_last_cycle_count", 0)
        or getattr(lt, "_tick_counter", 0)
        or 0
    )
    # Emit early lifecycle logs and periodic checkpoints only.
    return cycle_num <= 3 or (cycle_num % max(1, every_cycles) == 0)


def _is_qty_synced_dust(issue_text: str) -> bool:
    """
    Treat tiny QTY_SYNCED residuals as non-blocking dust.

    Example issue text:
    "QTY_SYNCED: exit+0.001 local=0.002 exchange=0.001 price=1954.97"
    """
    if not isinstance(issue_text, str) or not issue_text.startswith("QTY_SYNCED:"):
        return False
    try:
        import re

        local_match = re.search(r"local=([0-9]*\.?[0-9]+)", issue_text)
        exchange_match = re.search(r"exchange=([0-9]*\.?[0-9]+)", issue_text)
        if not local_match or not exchange_match:
            return False
        local_qty = Decimal(local_match.group(1))
        exchange_qty = Decimal(exchange_match.group(1))
        delta = abs(local_qty - exchange_qty)
        # Conservative dust gate: only tiny residuals should bypass open blocking.
        return delta <= Decimal("0.01") and max(local_qty, exchange_qty) <= Decimal("0.05")
    except (ArithmeticError, ValueError, TypeError):
        return False


def _split_reconcile_issues(issues: List) -> tuple[List, List]:
    """
    Split reconcile issues into (blocking, non_blocking).

    Rationale:
    - ORPHANED issues are often transient right after stop/TP-driven closes.
      They indicate registry/exchange convergence in progress and are
      persisted/handled by gateway reconciliation.
    - QTY_SYNCED dust residuals are non-blocking to avoid freezing opens on
      tiny convergence lag.
    - Other issue classes (PHANTOM, meaningful QTY drift, etc.) remain blocking.
    """
    blocking = []
    non_blocking = []
    for issue in issues or []:
        # Issue format is typically (symbol, "TYPE: details"), but keep robust.
        if (
            isinstance(issue, (list, tuple))
            and len(issue) >= 2
            and isinstance(issue[1], str)
            and issue[1].startswith("ORPHANED:")
        ):
            non_blocking.append(issue)
        elif (
            isinstance(issue, (list, tuple))
            and len(issue) >= 2
            and isinstance(issue[1], str)
            and _is_qty_synced_dust(issue[1])
        ):
            non_blocking.append(issue)
        else:
            blocking.append(issue)
    return blocking, non_blocking


def _filter_strategic_closes_for_gate(
    closes: List[str],
    trading_allowed: bool,
) -> List[str]:
    """
    Suppress allocator-driven rotation closes when hardening gate is closed.

    In DEGRADED/HALTED/EMERGENCY, we still allow management reductions
    (rebalancer reduceOnly trims), but we avoid one-way churn where swap closes
    execute and matching opens are later blocked by the pre-open gate.
    """
    if trading_allowed:
        return closes
    return []


def _normalize_symbol_key(symbol: str) -> str:
    key = (symbol or "").strip().upper()
    key = key.split(":")[0]
    if key.startswith("PF_"):
        base = key.replace("PF_", "").replace("USD", "")
        if base:
            return f"{base}/USD"
    return key


def _resolve_symbol_cooldown_params(strategy_config, symbol: str) -> Dict[str, float]:
    """
    Resolve cooldown parameters for a symbol, with optional canary overrides.
    """
    params = {
        "lookback_hours": int(getattr(strategy_config, "symbol_loss_lookback_hours", 24)),
        "loss_threshold": int(getattr(strategy_config, "symbol_loss_threshold", 3)),
        "cooldown_hours": int(getattr(strategy_config, "symbol_loss_cooldown_hours", 12)),
        "min_pnl_pct": float(getattr(strategy_config, "symbol_loss_min_pnl_pct", -0.5)),
        "canary_applied": False,
    }
    if not bool(getattr(strategy_config, "symbol_loss_cooldown_canary_enabled", False)):
        return params
    canary_symbols = {
        _normalize_symbol_key(s) for s in (getattr(strategy_config, "symbol_loss_cooldown_canary_symbols", []) or [])
    }
    if canary_symbols and _normalize_symbol_key(symbol) not in canary_symbols:
        return params

    for field_name, key_name in (
        ("symbol_loss_cooldown_canary_lookback_hours", "lookback_hours"),
        ("symbol_loss_cooldown_canary_threshold", "loss_threshold"),
        ("symbol_loss_cooldown_canary_hours", "cooldown_hours"),
        ("symbol_loss_cooldown_canary_min_pnl_pct", "min_pnl_pct"),
    ):
        override_value = getattr(strategy_config, field_name, None)
        if override_value is not None:
            params[key_name] = override_value
    params["canary_applied"] = True
    return params


def _symbol_in_canary(symbol: str, canary_symbols: List[str]) -> bool:
    if not canary_symbols:
        return True
    return _normalize_symbol_key(symbol) in {
        _normalize_symbol_key(s) for s in canary_symbols
    }


def _score_std(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return variance ** 0.5


def _build_strategic_close_action(position):
    """
    Build allocator-driven close action with explicit non-null exit reason.
    """
    from src.execution.position_manager_v2 import ManagementAction, ActionType
    from src.execution.position_state_machine import ExitReason

    return ManagementAction(
        type=ActionType.CLOSE_FULL,
        symbol=position.symbol,
        reason="AUCTION_STRATEGIC_CLOSE",
        side=position.side,
        size=position.remaining_qty,
        position_id=position.position_id,
        exit_reason=ExitReason.TIME_BASED,
    )


async def _compute_quick_reversal_metrics(lt: "LiveTrading") -> Dict[str, object]:
    risk_cfg = lt.config.risk
    window_hours = int(getattr(risk_cfg, "auction_chop_reversal_window_hours", 12) or 12)
    hold_minutes_threshold = int(getattr(risk_cfg, "auction_chop_quick_reversal_hold_minutes", 60) or 60)
    opposite_reentry_minutes = int(getattr(risk_cfg, "auction_chop_opposite_reentry_minutes", 120) or 120)
    now_utc = datetime.now(timezone.utc)
    trades = await asyncio.to_thread(get_trades_since, now_utc - timedelta(hours=window_hours))

    by_symbol: Dict[str, List] = {}
    for trade in trades:
        key = _normalize_symbol_key(getattr(trade, "symbol", ""))
        if key:
            by_symbol.setdefault(key, []).append(trade)

    quick_reversal_reasons = {
        "STOP",
        "STOP_LOSS",
        "TRAILING_STOP",
        "PREMISE_INVALIDATION",
        "NO_SIGNAL_CLOSE",
    }
    quick_reversal = 0
    opposite_reentry_fast = 0
    quick_profit_close = 0
    quick_loss_close = 0
    per_symbol_quick_reversal: Dict[str, int] = {}

    for symbol_key, symbol_trades in by_symbol.items():
        ordered = sorted(symbol_trades, key=lambda t: t.entered_at)
        for idx, trade in enumerate(ordered):
            hold_minutes = float((trade.holding_period_hours or Decimal("0")) * Decimal("60"))
            if hold_minutes >= hold_minutes_threshold:
                continue

            if trade.net_pnl > 0:
                quick_profit_close += 1
            elif trade.net_pnl < 0:
                quick_loss_close += 1

            exit_reason = str(getattr(trade, "exit_reason", "") or "").upper()
            is_quick_reversal = exit_reason in quick_reversal_reasons
            if not is_quick_reversal:
                continue

            quick_reversal += 1
            per_symbol_quick_reversal[symbol_key] = per_symbol_quick_reversal.get(symbol_key, 0) + 1
            if idx + 1 >= len(ordered):
                continue
            next_trade = ordered[idx + 1]
            gap_minutes = (next_trade.entered_at - trade.exited_at).total_seconds() / 60.0
            if gap_minutes < 0 or gap_minutes > opposite_reentry_minutes:
                continue
            if next_trade.side != trade.side:
                opposite_reentry_fast += 1

    return {
        "window_hours": window_hours,
        "quick_reversal": quick_reversal,
        "opposite_reentry_fast": opposite_reentry_fast,
        "quick_profit_close": quick_profit_close,
        "quick_loss_close": quick_loss_close,
        "per_symbol_quick_reversal": per_symbol_quick_reversal,
    }


async def _compute_symbol_churn_cooldowns(lt: "LiveTrading") -> Dict[str, datetime]:
    """
    Build per-symbol churn cooldown expiries from DB trades.

    Churn event definition:
    - A trade closes quickly (holding <= hold_max_minutes), and
    - next trade in same symbol reopens within reopen_max_minutes.
    """
    risk_cfg = lt.config.risk
    if not bool(getattr(risk_cfg, "auction_churn_guard_enabled", False)):
        return {}

    window_hours = int(getattr(risk_cfg, "auction_churn_window_hours", 6) or 6)
    hold_max_minutes = int(getattr(risk_cfg, "auction_churn_hold_max_minutes", 60) or 60)
    reopen_max_minutes = int(getattr(risk_cfg, "auction_churn_reopen_max_minutes", 120) or 120)
    max_events = int(getattr(risk_cfg, "auction_churn_max_events", 2) or 2)
    tier_cooldowns = [
        int(getattr(risk_cfg, "auction_churn_cooldown_tier1_minutes", 30) or 30),
        int(getattr(risk_cfg, "auction_churn_cooldown_tier2_minutes", 120) or 120),
        int(getattr(risk_cfg, "auction_churn_cooldown_tier3_minutes", 360) or 360),
    ]

    now_utc = datetime.now(timezone.utc)
    window_start = now_utc - timedelta(hours=window_hours)
    trades = await asyncio.to_thread(get_trades_since, window_start)

    by_symbol: Dict[str, List] = {}
    for trade in trades:
        key = _normalize_symbol_key(getattr(trade, "symbol", ""))
        if not key:
            continue
        by_symbol.setdefault(key, []).append(trade)

    cooldowns: Dict[str, datetime] = {}
    for key, symbol_trades in by_symbol.items():
        ordered = sorted(symbol_trades, key=lambda t: t.entered_at)
        event_times: List[datetime] = []
        for idx in range(len(ordered) - 1):
            current = ordered[idx]
            nxt = ordered[idx + 1]
            hold_minutes = float((current.holding_period_hours or Decimal("0")) * Decimal("60"))
            reopen_gap_minutes = (nxt.entered_at - current.exited_at).total_seconds() / 60.0
            if hold_minutes <= hold_max_minutes and 0 <= reopen_gap_minutes <= reopen_max_minutes:
                event_times.append(nxt.entered_at)

        if len(event_times) < max_events:
            continue

        over = len(event_times) - max_events + 1
        if over <= 1:
            cooldown_minutes = tier_cooldowns[0]
        elif over == 2:
            cooldown_minutes = tier_cooldowns[1]
        else:
            cooldown_minutes = tier_cooldowns[2]

        expiry = event_times[-1] + timedelta(minutes=cooldown_minutes)
        if now_utc < expiry:
            cooldowns[key] = expiry

    if cooldowns:
        logger.info(
            "Auction churn cooldowns active",
            count=len(cooldowns),
            symbols=sorted(cooldowns.keys())[:15],
            window_hours=window_hours,
        )
    return cooldowns


async def run_auction_allocation(lt: "LiveTrading", raw_positions: List[Dict]) -> None:
    """
    Run auction-based portfolio allocation if auction mode is enabled.

    Collects all open positions and candidate signals, runs the auction,
    and executes the allocation plan.
    """
    logger.debug(
        "Auction: _run_auction_allocation called",
        signals_count=len(lt.auction_signals_this_tick),
    )
    funnel_rejections: Counter = Counter()
    try:
        from src.portfolio.auction_allocator import (
            position_to_open_metadata,
            create_candidate_signal,
        )
        from src.domain.models import Signal, SignalType

        # Get account state
        balance = await lt.client.get_futures_balance()
        base = getattr(lt.config.exchange, "base_currency", "USD")
        equity, available_margin, _ = await calculate_effective_equity(
            balance, base_currency=base, kraken_client=lt.client
        )
        if (
            getattr(lt, "_replay_relaxed_signal_gates", False)
            and Decimal(str(equity or 0)) <= 0
        ):
            replay_equity = Decimal(
                str(getattr(getattr(lt.config, "backtest", None), "starting_equity", Decimal("10000")))
            )
            equity = replay_equity
            if Decimal(str(available_margin or 0)) <= 0:
                available_margin = replay_equity

        # Build spot-to-futures mapping for symbol matching
        spot_to_futures_map: Dict[str, str] = {}
        for pos_data in raw_positions:
            futures_sym = pos_data.get("symbol")
            if futures_sym:
                for spot_sym in lt.auction_signals_this_tick:
                    mapped_futures = lt.futures_adapter.map_spot_to_futures(
                        spot_sym[0].symbol,
                        futures_tickers=lt.latest_futures_tickers,
                    )
                    if mapped_futures == futures_sym:
                        spot_to_futures_map[spot_sym[0].symbol] = futures_sym
                        break

        open_positions_meta = []
        for pos_data in raw_positions:
            if pos_data.get("size", 0) == 0:
                continue
            try:
                pos = lt._convert_to_position(pos_data)
                futures_symbol = pos.symbol

                # Merge protection status from database
                try:
                    db_pos = await asyncio.to_thread(get_active_position, futures_symbol)
                    if db_pos:
                        pos.is_protected = db_pos.is_protected
                        pos.protection_reason = db_pos.protection_reason
                        # Preserve true position age from DB so allocator lock checks
                        # don't treat long-lived positions as newly opened.
                        pos.opened_at = db_pos.opened_at
                        pos.stop_loss_order_id = db_pos.stop_loss_order_id
                        pos.initial_stop_price = db_pos.initial_stop_price
                        if hasattr(db_pos, "tp_order_ids"):
                            pos.tp_order_ids = db_pos.tp_order_ids
                except (OperationalError, DataError) as e:
                    logger.warning(
                        "Failed to fetch DB position for protection merge",
                        symbol=futures_symbol,
                        error=str(e),
                        error_type=type(e).__name__,
                    )

                is_protective_live = pos.stop_loss_order_id is not None or (
                    hasattr(pos, "tp_order_ids") and pos.tp_order_ids
                )
                meta = position_to_open_metadata(
                    position=pos,
                    account_equity=equity,
                    is_protective_orders_live=is_protective_live,
                )
                spot_symbol = None
                for spot_sym, fut_sym in spot_to_futures_map.items():
                    if fut_sym == futures_symbol:
                        spot_symbol = spot_sym
                        break
                if not spot_symbol:
                    base_sym = (
                        futures_symbol.replace("PF_", "")
                        .replace("USD", "")
                        .replace("PI_", "")
                        .replace("FI_", "")
                    )
                    if base_sym:
                        spot_symbol = f"{base_sym}/USD"
                meta.spot_symbol = spot_symbol
                meta.conviction = None
                if (
                    spot_symbol
                    and getattr(lt, "institutional_memory_manager", None)
                    and lt.institutional_memory_manager.is_enabled_for_symbol(spot_symbol)
                ):
                    thesis_snapshot = lt.institutional_memory_manager.get_latest_thesis(
                        spot_symbol
                    )
                    if thesis_snapshot is not None:
                        conviction_val = getattr(
                            thesis_snapshot, "current_conviction", None
                        )
                        if conviction_val is not None:
                            try:
                                meta.conviction = float(conviction_val)
                            except (TypeError, ValueError):
                                meta.conviction = None
                open_positions_meta.append(meta)
            except (ValueError, TypeError, KeyError) as e:
                logger.error(
                    "Failed to convert position for auction",
                    symbol=pos_data.get("symbol"),
                    error=str(e),
                    error_type=type(e).__name__,
                )

        # Pre-filter: exclude signals for symbols that already have open positions
        from src.data.symbol_utils import normalize_symbol_for_position_match

        open_position_symbols: set = set()
        for meta in open_positions_meta:
            spot = getattr(meta, "spot_symbol", None)
            if spot:
                open_position_symbols.add(normalize_symbol_for_position_match(spot))

        pre_filter_count = len(lt.auction_signals_this_tick)
        lt.auction_signals_this_tick = [
            (sig, sp, mp) for sig, sp, mp in lt.auction_signals_this_tick
            if normalize_symbol_for_position_match(sig.symbol) not in open_position_symbols
        ]
        filtered_out = pre_filter_count - len(lt.auction_signals_this_tick)
        if filtered_out > 0:
            logger.debug(
                "Auction: pre-filtered signals for existing positions",
                filtered_out=filtered_out,
                open_symbols=sorted(open_position_symbols),
            )

        signals_count = len(lt.auction_signals_this_tick)
        if _should_emit_replay_tick_log(lt):
            logger.info("Auction: Collecting candidate signals", signals_count=signals_count, pre_filtered=filtered_out)
        signals_after_cooldown = 0
        risk_approved_count = 0
        risk_rejected_count = 0
        canary_overrides_applied = 0
        now_utc = datetime.now(timezone.utc)
        churn_cooldowns = await _compute_symbol_churn_cooldowns(lt)
        quick_reversal_metrics = await _compute_quick_reversal_metrics(lt)
        signal_scores = [float(sig.score) for sig, _, _ in lt.auction_signals_this_tick]
        cycle_score_std = _score_std(signal_scores)
        per_symbol_quick_reversal = dict(
            quick_reversal_metrics.get("per_symbol_quick_reversal", {}) or {}
        )
        chop_per_symbol: Dict[str, bool] = {}
        chop_signals = 0

        # Refresh instrument spec registry
        try:
            await lt.instrument_spec_registry.refresh()
        except (OperationalError, DataError) as e:
            logger.warning("Instrument spec refresh failed before auction", error=str(e), error_type=type(e).__name__)

        auction_budget_margin = equity * Decimal(str(lt.config.risk.auction_max_margin_util))

        candidate_signals = []
        signal_to_candidate: dict = {}
        requested_leverage = int(getattr(lt.config.risk, "target_leverage", 7) or 7)

        from src.risk.symbol_cooldown import check_symbol_cooldown

        for signal, spot_price, mark_price in lt.auction_signals_this_tick:
            try:
                normalized_signal_symbol = _normalize_symbol_key(signal.symbol)
                adx_value = float(getattr(signal, "adx", 0) or 0)
                adx_threshold = float(getattr(lt.config.risk, "auction_chop_adx_threshold", 18.0) or 18.0)
                score_std_threshold = float(
                    getattr(lt.config.risk, "auction_chop_score_std_threshold", 6.0) or 6.0
                )
                symbol_quick_reversals = int(per_symbol_quick_reversal.get(normalized_signal_symbol, 0) or 0)
                symbol_is_chop = (
                    adx_value < adx_threshold
                    and cycle_score_std < score_std_threshold
                    and symbol_quick_reversals > 0
                )
                chop_per_symbol[normalized_signal_symbol] = bool(symbol_is_chop)
                if symbol_is_chop:
                    chop_signals += 1
                churn_cooldown_until = churn_cooldowns.get(normalized_signal_symbol)
                if churn_cooldown_until and now_utc < churn_cooldown_until:
                    funnel_rejections["REJECT_CHURN_COOLDOWN"] += 1
                    remaining_minutes = int((churn_cooldown_until - now_utc).total_seconds() / 60)
                    logger.warning(
                        "AUCTION_OPEN_REJECTED",
                        symbol=signal.symbol,
                        reason="REJECT_CHURN_COOLDOWN",
                        details=f"remaining={remaining_minutes}m",
                    )
                    continue

                if getattr(lt.config.strategy, "symbol_loss_cooldown_enabled", True):
                    cooldown_params = _resolve_symbol_cooldown_params(lt.config.strategy, signal.symbol)
                    if cooldown_params["canary_applied"]:
                        canary_overrides_applied += 1
                    is_on_cooldown, cooldown_reason = check_symbol_cooldown(
                        symbol=signal.symbol,
                        lookback_hours=int(cooldown_params["lookback_hours"]),
                        loss_threshold=int(cooldown_params["loss_threshold"]),
                        cooldown_hours=int(cooldown_params["cooldown_hours"]),
                        min_pnl_pct=float(cooldown_params["min_pnl_pct"]),
                    )
                    if is_on_cooldown:
                        funnel_rejections["SYMBOL_COOLDOWN"] += 1
                        logger.warning(
                            "AUCTION_OPEN_REJECTED",
                            symbol=signal.symbol,
                            reason="SYMBOL_COOLDOWN",
                            details=cooldown_reason,
                        )
                        continue
                signals_after_cooldown += 1

                futures_symbol = lt.futures_adapter.map_spot_to_futures(
                    signal.symbol, futures_tickers=lt.latest_futures_tickers
                )
                spec = lt.instrument_spec_registry.get_spec(futures_symbol)
                if not spec:
                    if getattr(lt, "_replay_relaxed_signal_gates", False):
                        logger.info(
                            "Ablation: bypassing NO_SPEC in replay",
                            symbol=signal.symbol,
                            futures_symbol=futures_symbol,
                        )
                    else:
                        funnel_rejections["NO_SPEC"] += 1
                        logger.warning(
                            "AUCTION_OPEN_REJECTED",
                            symbol=signal.symbol,
                            reason="NO_SPEC",
                            requested_leverage=requested_leverage,
                            spec_summary=None,
                        )
                        continue

                symbol_tier = (
                    lt.market_discovery.get_symbol_tier(signal.symbol)
                    if lt.market_discovery
                    else "C"
                )
                if symbol_tier != "A":
                    static_tier = lt._get_static_tier(signal.symbol)
                    if static_tier == "A":
                        logger.warning(
                            "Tier downgrade detected",
                            symbol=signal.symbol,
                            static_tier=static_tier,
                            dynamic_tier=symbol_tier,
                            reason="Dynamic classification is authoritative",
                        )

                decision = lt.risk_manager.validate_trade(
                    signal,
                    equity,
                    spot_price,
                    mark_price,
                    available_margin=auction_budget_margin,
                    symbol_tier=symbol_tier,
                )
                if not decision.approved:
                    risk_rejected_count += 1
                    for reason in (decision.rejection_reasons or []):
                        funnel_rejections[f"RISK_{reason}"] += 1
                    logger.info(
                        "Auction candidate rejected by risk manager",
                        symbol=signal.symbol,
                        score=signal.score,
                        rejection_reasons=decision.rejection_reasons,
                        position_notional=str(decision.position_notional),
                    )
                elif decision.position_notional > 0 and decision.margin_required > 0:
                    risk_approved_count += 1
                    stop_distance = (
                        abs(signal.entry_price - signal.stop_loss) / signal.entry_price
                        if signal.stop_loss
                        else Decimal("0")
                    )
                    risk_R = (
                        decision.position_notional * stop_distance
                        if stop_distance > 0
                        else Decimal("0")
                    )
                    candidate = create_candidate_signal(
                        signal=signal,
                        required_margin=decision.margin_required,
                        risk_R=risk_R,
                        position_notional=decision.position_notional,
                    )
                    candidate_signals.append(candidate)
                    signal_to_candidate[signal.symbol] = candidate
                    logger.info(
                        "Auction candidate created",
                        symbol=signal.symbol,
                        score=signal.score,
                        notional=str(decision.position_notional),
                        margin=str(decision.margin_required),
                        regime="CHOP" if symbol_is_chop else "TREND",
                        score_std=round(cycle_score_std, 3),
                        symbol_quick_reversals=symbol_quick_reversals,
                    )
                else:
                    funnel_rejections["APPROVED_BUT_NON_TRADABLE_SIZE"] += 1
                    logger.warning(
                        "Signal not added to auction candidates",
                        symbol=signal.symbol,
                        score=signal.score,
                        position_notional=str(decision.position_notional),
                        margin_required=str(decision.margin_required),
                        approved=decision.approved,
                        rejection_reasons=decision.rejection_reasons,
                    )
            except (OperationalError, DataError, ValueError, TypeError, KeyError) as e:
                funnel_rejections[f"CANDIDATE_BUILD_{type(e).__name__}"] += 1
                logger.error(
                    "Failed to create candidate signal for auction",
                    symbol=signal.symbol,
                    error=str(e),
                    error_type=type(e).__name__,
                )

        if candidate_signals:
            lt._auction_no_signal_cycles = 0
        else:
            lt._auction_no_signal_cycles = int(getattr(lt, "_auction_no_signal_cycles", 0) or 0) + 1
        if _should_emit_replay_tick_log(lt):
            logger.info(
                "Auction no-signal cycle state updated",
                no_signal_cycles=lt._auction_no_signal_cycles,
                candidate_count=len(candidate_signals),
            )

        unique_scanned_symbols = {
            _normalize_symbol_key(sig.symbol) for sig, _, _ in lt.auction_signals_this_tick
        }
        choppy_symbols = {sym for sym, is_chop in chop_per_symbol.items() if is_chop}
        chop_symbol_ratio = (
            (len(choppy_symbols) / len(unique_scanned_symbols)) if unique_scanned_symbols else 0.0
        )
        global_chop = chop_symbol_ratio >= float(
            getattr(lt.config.risk, "auction_chop_global_symbol_pct", 0.50) or 0.50
        )

        trading_allowed_now = bool(
            not lt.hardening or lt.hardening.is_trading_allowed()
        )
        base_swap_threshold = float(getattr(lt.config.risk, "auction_swap_threshold", 10.0) or 10.0)
        base_min_hold_minutes = int(getattr(lt.config.risk, "auction_min_hold_minutes", 15) or 15)
        high_conviction_min_hold_minutes = int(
            getattr(lt.config.risk, "auction_min_hold_high_conviction_minutes", base_min_hold_minutes)
            or base_min_hold_minutes
        )
        high_conviction_threshold = float(
            getattr(lt.config.risk, "auction_min_hold_high_conviction_threshold", 50.0) or 50.0
        )
        base_max_new_opens = int(getattr(lt.config.risk, "auction_max_new_opens_per_cycle", 1) or 1)
        base_no_signal_cycles = int(
            getattr(lt.config.risk, "auction_no_signal_close_persistence_cycles", 3) or 3
        )

        chop_guard_enabled = bool(getattr(lt.config.risk, "auction_chop_guard_enabled", False))
        chop_telemetry_only = bool(getattr(lt.config.risk, "auction_chop_telemetry_only", True))
        chop_canary_symbols = list(getattr(lt.config.risk, "auction_chop_canary_symbols", []) or [])
        canary_set = {_normalize_symbol_key(s) for s in chop_canary_symbols}
        chop_active_symbols = (
            {s for s in choppy_symbols if not canary_set or s in canary_set}
            if global_chop
            else set()
        )
        chop_policy_active = chop_guard_enabled and bool(chop_active_symbols)
        canary_scoped_mode = bool(canary_set)
        chop_swap_threshold = base_swap_threshold + float(
            getattr(lt.config.risk, "auction_chop_swap_threshold_delta", 2.0) or 2.0
        )
        chop_min_hold_minutes = int(
            round(
                base_min_hold_minutes
                * float(getattr(lt.config.risk, "auction_chop_min_hold_multiplier", 2.0) or 2.0)
            )
        )
        chop_max_new_opens = max(
            base_max_new_opens + int(getattr(lt.config.risk, "auction_chop_max_new_opens_delta", -1) or -1),
            1,
        )
        chop_no_signal_cycles = max(
            base_no_signal_cycles + int(getattr(lt.config.risk, "auction_chop_no_signal_persistence_delta", 1) or 1),
            1,
        )
        would_block_replace = 0
        if chop_policy_active:
            active_candidate_count = sum(
                1
                for c in candidate_signals
                if _normalize_symbol_key(getattr(c, "symbol", "")) in chop_active_symbols
            )
            would_block_replace = max(active_candidate_count - chop_max_new_opens, 0)

        portfolio_state = {
            "available_margin": auction_budget_margin,
            "account_equity": equity,
            "last_partial_close_at": getattr(lt, "_last_partial_close_at", None),
            "partial_close_cooldown_seconds": getattr(
                lt.config.risk, "auction_partial_close_cooldown_seconds", 0
            ),
            "current_cycle": int(getattr(lt, "_last_cycle_count", 0) or 0),
            "last_trim_cycle_by_symbol": dict(
                getattr(lt, "_last_trim_cycle_by_symbol", {}) or {}
            ),
            # In degraded/halted/emergency, allow reduceOnly concentration trims
            # even for lock-flagged positions to unblock safety recovery.
            "allow_locked_rebalancer_trims": not trading_allowed_now,
            "auction_no_signal_cycles": int(getattr(lt, "_auction_no_signal_cycles", 0) or 0),
            "auction_no_signal_persistence_enabled": bool(
                getattr(lt.config.risk, "auction_no_signal_persistence_enabled", False)
            ),
            "auction_no_signal_persistence_canary_symbols": list(
                getattr(lt.config.risk, "auction_no_signal_persistence_canary_symbols", []) or []
            ),
            "auction_chop_active_symbols": sorted(chop_active_symbols),
            "auction_chop_swap_threshold": chop_swap_threshold,
            "auction_chop_min_hold_minutes": chop_min_hold_minutes,
            "auction_swap_threshold": chop_swap_threshold if (chop_policy_active and not chop_telemetry_only) else base_swap_threshold,
            "auction_min_hold_minutes": base_min_hold_minutes,
            "auction_min_hold_high_conviction_minutes": high_conviction_min_hold_minutes,
            "auction_min_hold_high_conviction_threshold": high_conviction_threshold,
            "auction_max_new_opens_per_cycle": (
                chop_max_new_opens
                if (chop_policy_active and not chop_telemetry_only and not canary_scoped_mode)
                else base_max_new_opens
            ),
            "auction_no_signal_close_persistence_cycles": (
                chop_no_signal_cycles if (chop_policy_active and not chop_telemetry_only) else base_no_signal_cycles
            ),
        }

        plan = lt.auction_allocator.allocate(
            open_positions=open_positions_meta,
            candidate_signals=candidate_signals,
            portfolio_state=portfolio_state,
        )

        has_activity = bool(plan.closes or plan.opens or plan.reductions)
        if _should_emit_replay_tick_log(lt, force=has_activity):
            logger.info(
                "Auction plan generated",
                closes_count=len(plan.closes),
                closes_symbols=plan.closes,
                opens_count=len(plan.opens),
                opens_symbols=[s.symbol for s in plan.opens],
                reductions_count=len(plan.reductions),
                reductions=[(sym, str(qty)) for sym, qty in plan.reductions],
                reasons=plan.reasons,
            )
        _, policy_hash = build_policy_hash(lt.config)
        if _should_emit_replay_tick_log(lt, force=has_activity):
            logger.info(
                "AUCTION_CHOP_SUMMARY",
                policy_hash=policy_hash,
                global_chop=global_chop,
                chop_guard_enabled=chop_guard_enabled,
                chop_telemetry_only=chop_telemetry_only,
                chop_canary_mode=canary_scoped_mode,
                chop_canary_symbols=sorted(list(canary_set))[:10] if canary_set else None,
                chop_signals=chop_signals,
                unique_symbols=len(unique_scanned_symbols),
                choppy_symbols=len(choppy_symbols),
                active_chop_symbols=len(chop_active_symbols),
                chop_symbol_ratio=round(chop_symbol_ratio, 3),
                cycle_score_std=round(cycle_score_std, 3),
                quick_reversal=int(quick_reversal_metrics.get("quick_reversal", 0) or 0),
                opposite_reentry_fast=int(quick_reversal_metrics.get("opposite_reentry_fast", 0) or 0),
                quick_profit_close=int(quick_reversal_metrics.get("quick_profit_close", 0) or 0),
                quick_loss_close=int(quick_reversal_metrics.get("quick_loss_close", 0) or 0),
                would_block_replace=would_block_replace,
                would_block_close_no_signal=int(
                    (plan.reasons or {}).get("hysteresis_close_suppressed_no_signal", 0) or 0
                ),
            )

        # Record auction wins for churn tracking
        now_utc = datetime.now(timezone.utc)
        for sig in plan.opens:
            lt._auction_win_log.setdefault(sig.symbol, []).append(now_utc)

        # Execute concentration reductions first (reduceOnly partial closes only).
        reductions_executed = 0
        reductions_failed = 0
        rebalancer_shadow = bool(
            getattr(lt.config.risk, "auction_rebalancer_shadow_mode", True)
        )
        if plan.reductions:
            if rebalancer_shadow:
                logger.info(
                    "Auction rebalancer shadow mode: reductions not executed",
                    reductions=[(sym, str(qty)) for sym, qty in plan.reductions],
                )
            elif not (lt.use_state_machine_v2 and lt.execution_gateway and lt.position_registry):
                logger.warning(
                    "Auction rebalancer skipped: state machine gateway unavailable",
                    reductions=len(plan.reductions),
                )
            else:
                from src.execution.position_manager_v2 import ManagementAction, ActionType

                for symbol, trim_qty in plan.reductions:
                    if trim_qty <= 0:
                        reductions_failed += 1
                        logger.warning(
                            "Auction rebalancer skipped non-positive trim",
                            symbol=symbol,
                            trim_qty=str(trim_qty),
                        )
                        continue
                    position = lt.position_registry.get_position(symbol)
                    if not position:
                        reductions_failed += 1
                        logger.warning(
                            "Auction rebalancer skipped missing registry position",
                            symbol=symbol,
                        )
                        continue
                    if trim_qty >= position.remaining_qty:
                        trim_qty = position.remaining_qty * Decimal("0.95")
                    if trim_qty <= 0:
                        reductions_failed += 1
                        logger.warning(
                            "Auction rebalancer skipped zero trim after clamp",
                            symbol=symbol,
                            remaining_qty=str(position.remaining_qty),
                        )
                        continue

                    action = ManagementAction(
                        type=ActionType.CLOSE_PARTIAL,
                        symbol=symbol,
                        reason="AUTONOMOUS_REBALANCER_TRIM",
                        side=position.side,
                        size=trim_qty,
                    )
                    result = await lt.execution_gateway.execute_action(action)
                    if result.success:
                        reductions_executed += 1
                        lt._last_trim_cycle_by_symbol[symbol] = int(
                            getattr(lt, "_last_cycle_count", 0) or 0
                        )
                        logger.info(
                            "Auction rebalancer trim executed",
                            symbol=symbol,
                            trim_qty=str(trim_qty),
                            client_order_id=action.client_order_id,
                            exchange_order_id=result.exchange_order_id,
                        )
                    else:
                        reductions_failed += 1
                        logger.error(
                            "Auction rebalancer trim failed",
                            symbol=symbol,
                            trim_qty=str(trim_qty),
                            error=result.error,
                        )

        closes_to_execute = _filter_strategic_closes_for_gate(
            plan.closes,
            trading_allowed_now,
        )
        anti_flip_would_block = 0
        anti_flip_enforced_blocks = 0
        anti_flip_lock_enabled = bool(getattr(lt.config.risk, "auction_anti_flip_lock_enabled", False))
        anti_flip_telemetry_only = bool(
            getattr(lt.config.risk, "auction_anti_flip_lock_telemetry_only", True)
        )
        anti_flip_lock_minutes = int(getattr(lt.config.risk, "auction_anti_flip_lock_minutes", 45) or 45)
        anti_flip_canary_symbols = list(
            getattr(lt.config.risk, "auction_anti_flip_canary_symbols", []) or []
        )
        open_age_minutes_by_symbol: Dict[str, float] = {}
        for op_meta in open_positions_meta:
            age_minutes = max(float(op_meta.age_seconds) / 60.0, 0.0)
            open_age_minutes_by_symbol[_normalize_symbol_key(op_meta.position.symbol)] = age_minutes
            if op_meta.spot_symbol:
                open_age_minutes_by_symbol[_normalize_symbol_key(op_meta.spot_symbol)] = age_minutes
        if anti_flip_lock_enabled and closes_to_execute:
            filtered_closes: List[str] = []
            for close_symbol in closes_to_execute:
                normalized_close = _normalize_symbol_key(close_symbol)
                age_minutes = open_age_minutes_by_symbol.get(normalized_close)
                in_lock_window = age_minutes is not None and age_minutes < anti_flip_lock_minutes
                canary_match = _symbol_in_canary(close_symbol, anti_flip_canary_symbols)
                if in_lock_window and canary_match:
                    anti_flip_would_block += 1
                    if anti_flip_telemetry_only:
                        filtered_closes.append(close_symbol)
                    else:
                        anti_flip_enforced_blocks += 1
                        funnel_rejections["REJECT_ANTI_FLIP_LOCK"] += 1
                        logger.warning(
                            "AUCTION_CLOSE_REJECTED",
                            symbol=close_symbol,
                            reason="REJECT_ANTI_FLIP_LOCK",
                            details=f"age_minutes={age_minutes:.1f}, lock_minutes={anti_flip_lock_minutes}",
                        )
                else:
                    filtered_closes.append(close_symbol)
            closes_to_execute = filtered_closes
        if plan.closes and not closes_to_execute:
            logger.warning(
                "Auction closes suppressed by hardening gate",
                planned_closes=len(plan.closes),
                system_state="gate_closed",
            )

        # Execute closes next.
        # Guardrail: if this is a swap cycle (planned opens present), stop closing
        # as soon as hardening gate closes to avoid close-without-open cascades.
        closes_executed_count = 0
        for symbol in closes_to_execute:
            if plan.opens and lt.hardening and not lt.hardening.is_trading_allowed():
                logger.warning(
                    "Auction close loop stopped: hardening gate closed mid-swap",
                    closes_executed=closes_executed_count,
                    closes_remaining=max(len(closes_to_execute) - closes_executed_count, 0),
                    planned_opens=len(plan.opens),
                )
                break
            try:
                # Route strategic closes through the gateway/state machine when available.
                # This preserves fill tracking and prevents ORPHANED close artifacts.
                if lt.use_state_machine_v2 and lt.execution_gateway and lt.position_registry:
                    normalized_symbol = _normalize_symbol_key(symbol)
                    position = lt.position_registry.get_position(normalized_symbol)
                    if not position:
                        logger.warning(
                            "Auction close fallback: position missing in registry",
                            requested_symbol=symbol,
                            normalized_symbol=normalized_symbol,
                        )
                        await lt.client.close_position(symbol)
                        closes_executed_count += 1
                        logger.info("Auction: Closed position (direct fallback)", symbol=symbol)
                        continue

                    action = _build_strategic_close_action(position)
                    result = await lt.execution_gateway.execute_action(action)
                    if not result.success:
                        raise RuntimeError(result.error or "gateway close failed")

                    closes_executed_count += 1
                    logger.info(
                        "Auction: Closed position via gateway",
                        symbol=position.symbol,
                        requested_symbol=symbol,
                        exchange_order_id=result.exchange_order_id,
                    )
                else:
                    await lt.client.close_position(symbol)
                    closes_executed_count += 1
                    logger.info("Auction: Closed position", symbol=symbol)
            except (OperationalError, DataError) as e:
                logger.error("Auction: Failed to close position", symbol=symbol, error=str(e), error_type=type(e).__name__)
            except RuntimeError as e:
                logger.error("Auction: Failed to close position", symbol=symbol, error=str(e), error_type=type(e).__name__)

        # Refresh protective orders after trims, then reconcile + refresh margin before opens.
        fresh_snapshot_ok = False
        reconcile_blocking_issues = False
        refreshed_available_margin = available_margin
        if reductions_executed > 0:
            try:
                refreshed_positions = await lt.client.get_all_futures_positions()
                current_prices_map = {}
                for pos_data in refreshed_positions:
                    symbol = pos_data.get("symbol")
                    if not symbol:
                        continue
                    mark_price = None
                    if lt.latest_futures_tickers:
                        mark_price = lt.latest_futures_tickers.get(symbol)
                    if mark_price is None:
                        mark_price = Decimal(
                            str(
                                pos_data.get("markPrice")
                                or pos_data.get("mark_price")
                                or pos_data.get("entryPrice")
                                or 0
                            )
                        )
                    current_prices_map[symbol] = mark_price
                await lt._reconcile_stop_loss_order_ids(refreshed_positions)
                await lt._reconcile_protective_orders(refreshed_positions, current_prices_map)
                logger.info(
                    "Auction rebalancer protective refresh complete",
                    refreshed_positions=len(refreshed_positions),
                )
            except (OperationalError, DataError, ValueError, TypeError) as e:
                logger.warning(
                    "Auction rebalancer protective refresh failed",
                    error=str(e),
                    error_type=type(e).__name__,
                )

        if lt.use_state_machine_v2 and lt.execution_gateway:
            try:
                sync_result = await lt.execution_gateway.sync_with_exchange()
                all_issues = sync_result.get("issues", []) or []
                blocking_issues, non_blocking_issues = _split_reconcile_issues(all_issues)
                reconcile_blocking_issues = bool(blocking_issues)
                logger.info(
                    "Auction rebalancer pre-open reconcile",
                    actions_taken=sync_result.get("actions_taken", 0),
                    issues=all_issues,
                    blocking_issues=blocking_issues,
                    non_blocking_issues=non_blocking_issues,
                )
            except (OperationalError, DataError) as e:
                reconcile_blocking_issues = True
                logger.warning(
                    "Auction rebalancer pre-open reconcile failed",
                    error=str(e),
                    error_type=type(e).__name__,
                )

        try:
            balance_after_actions = await lt.client.get_futures_balance()
            equity_after, refreshed_available_margin, _ = await calculate_effective_equity(
                balance_after_actions, base_currency=base, kraken_client=lt.client
            )
            if (
                getattr(lt, "_replay_relaxed_signal_gates", False)
                and Decimal(str(equity_after or 0)) <= 0
            ):
                replay_equity = Decimal(
                    str(getattr(getattr(lt.config, "backtest", None), "starting_equity", Decimal("10000")))
                )
                equity_after = replay_equity
                if Decimal(str(refreshed_available_margin or 0)) <= 0:
                    refreshed_available_margin = replay_equity
            fresh_snapshot_ok = True
            logger.info(
                "Auction: Margin refreshed after management actions",
                equity=str(equity_after),
                refreshed_available_margin=str(refreshed_available_margin),
                previous_available_margin=str(available_margin),
                reductions_executed=reductions_executed,
                closes_executed=closes_executed_count,
            )
        except (OperationalError, DataError) as e:
            logger.warning(
                "Auction: Failed to refresh margin after management actions",
                error=str(e),
                error_type=type(e).__name__,
            )

        # Execute opens (deduplicated)
        seen_opens: set = set()
        opens_executed = 0
        opens_failed = 0
        rejection_counts: Dict[str, int] = {}

        # Gate opens: degraded/halted state, stale snapshot, or unresolved reconcile issues
        open_gate_reason = None
        if lt.hardening and not lt.hardening.is_trading_allowed():
            open_gate_reason = "TRADING_GATE_CLOSED"
        elif not fresh_snapshot_ok:
            open_gate_reason = "FRESH_SNAPSHOT_UNAVAILABLE"
        elif reconcile_blocking_issues:
            open_gate_reason = "RECONCILE_BLOCKING_ISSUES"
        if open_gate_reason and getattr(lt, "_replay_relaxed_signal_gates", False):
            logger.warning(
                "Replay relaxed gate: bypassing pre-open gate",
                blocked_reason=open_gate_reason,
                planned_opens=len(plan.opens),
            )
            open_gate_reason = None
        if open_gate_reason:
            logger.warning(
                "Auction opens suppressed by pre-open gate",
                reason=open_gate_reason,
                planned_opens=len(plan.opens),
            )
            if hasattr(lt, "_auction_open_block_reason_by_symbol"):
                now_utc_gate = datetime.now(timezone.utc)
                for planned_signal in plan.opens:
                    lt._auction_open_block_reason_by_symbol[planned_signal.symbol] = {
                        "reason": open_gate_reason,
                        "at": now_utc_gate,
                    }

        for signal in plan.opens:
            if open_gate_reason:
                opens_failed += 1
                rejection_counts[open_gate_reason] = rejection_counts.get(open_gate_reason, 0) + 1
                funnel_rejections[f"OPEN_{open_gate_reason}"] += 1
                continue
            if signal.symbol in seen_opens:
                logger.warning(
                    "Auction: Skipping duplicate open for same symbol",
                    symbol=signal.symbol,
                )
                continue
            seen_opens.add(signal.symbol)
            try:
                # Hard entry blocklist
                spot_key = (signal.symbol or "").strip().upper().split(":")[0]
                base_cur = spot_key.split("/")[0].strip() if "/" in spot_key else spot_key
                blocked_spot = set(
                    s.strip().upper().split(":")[0]
                    for s in getattr(lt.config.execution, "entry_blocklist_spot_symbols", []) or []
                )
                blocked_base = set(
                    b.strip().upper()
                    for b in getattr(lt.config.execution, "entry_blocklist_bases", []) or []
                )
                if (spot_key and spot_key in blocked_spot) or (base_cur and base_cur in blocked_base):
                    opens_failed += 1
                    reason = "ENTRY_BLOCKED"
                    rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
                    logger.warning(
                        "Auction: Open blocked by entry blocklist",
                        symbol=signal.symbol,
                        reason=(
                            "blocked_spot_symbol"
                            if spot_key in blocked_spot
                            else "blocked_base"
                        ),
                    )
                    continue

                spot_price_val = None
                mark_price_val = None
                candidate = signal_to_candidate.get(signal.symbol)

                for sig, sp, mp in lt.auction_signals_this_tick:
                    if sig.symbol == signal.symbol:
                        spot_price_val = sp
                        mark_price_val = mp
                        break

                if spot_price_val and mark_price_val and candidate:
                    logger.info(
                        "Auction: Executing open with overrides",
                        symbol=signal.symbol,
                        notional_override=str(candidate.position_notional),
                        refreshed_margin=str(refreshed_available_margin),
                    )
                    result = await lt._handle_signal(
                        signal, spot_price_val, mark_price_val,
                        notional_override=candidate.position_notional,
                    )
                    if result.get("order_placed", False):
                        opens_executed += 1
                        lt._auction_entry_log[signal.symbol] = datetime.now(timezone.utc)
                        if hasattr(lt, "_auction_open_block_reason_by_symbol"):
                            lt._auction_open_block_reason_by_symbol.pop(signal.symbol, None)
                        logger.info(
                            "Auction: Opened position",
                            symbol=signal.symbol,
                            reason=result.get("reason", "unknown"),
                        )
                    else:
                        opens_failed += 1
                        rejection_reasons = result.get("rejection_reasons", [])
                        for r in rejection_reasons:
                            rejection_counts[r] = rejection_counts.get(r, 0) + 1
                            funnel_rejections[f"OPEN_{r}"] += 1
                        if hasattr(lt, "_auction_open_block_reason_by_symbol"):
                            lt._auction_open_block_reason_by_symbol[signal.symbol] = {
                                "reason": (rejection_reasons[0] if rejection_reasons else "OPEN_REJECTED"),
                                "at": datetime.now(timezone.utc),
                            }
                        logger.warning(
                            "Auction: Open rejected/failed",
                            symbol=signal.symbol,
                            reason=result.get("reason", "unknown"),
                            rejection_reasons=rejection_reasons,
                        )
                else:
                    opens_failed += 1
                    missing: list = []
                    if not spot_price_val or not mark_price_val:
                        missing.append("price_data")
                    if not candidate:
                        missing.append("candidate")
                    reason = "missing_data:" + ",".join(missing)
                    rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
                    funnel_rejections[f"OPEN_{reason}"] += 1
                    logger.warning(
                        "Auction: Missing data for signal",
                        symbol=signal.symbol,
                        missing=missing,
                    )
            except ValueError as e:
                opens_failed += 1
                err_str = str(e)
                if "SIZE_BELOW_MIN" in err_str:
                    reason = "SIZE_BELOW_MIN"
                elif "SIZE_STEP_ROUND_TO_ZERO" in err_str:
                    reason = "SIZE_STEP_ROUND_TO_ZERO"
                elif "Size validation failed" in err_str:
                    reason = err_str.split(":")[-1].strip() if ":" in err_str else "SIZE_VALIDATION_FAILED"
                else:
                    reason = "ValueError"
                rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
                funnel_rejections[f"OPEN_{reason}"] += 1
                logger.error(
                    "Auction: Failed to open position (size/validation)",
                    symbol=signal.symbol,
                    error=err_str,
                )
            except (OperationalError, DataError) as e:
                opens_failed += 1
                reason = type(e).__name__
                rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
                funnel_rejections[f"OPEN_{reason}"] += 1
                logger.error(
                    "Auction: Failed to open position",
                    symbol=signal.symbol,
                    error=str(e),
                    error_type=type(e).__name__,
                    exc_info=True,
                )

        activity_executed = bool(opens_executed or closes_executed_count or reductions_executed)
        if _should_emit_replay_tick_log(lt, force=has_activity or activity_executed):
            logger.info(
                "Auction allocation executed",
                closes=closes_executed_count,
                reductions_planned=len(plan.reductions),
                reductions_executed=reductions_executed,
                reductions_failed=reductions_failed,
                opens_planned=len(plan.opens),
                opens_executed=opens_executed,
                opens_failed=opens_failed,
                rejection_counts=rejection_counts if rejection_counts else None,
                reasons=plan.reasons,
            )
        cycle_num = int(getattr(lt, "_last_cycle_count", 0) or 0)
        analysis_funnel = getattr(lt, "_analysis_funnel_metrics", {}) or {}
        funnel_payload = {
            "cycle": cycle_num if cycle_num > 0 else None,
            "cycle_phase": "main_loop" if cycle_num > 0 else "startup_hydration",
            "symbols_scanned": len(lt._market_symbols()) if hasattr(lt, "_market_symbols") else None,
            "universe_total": analysis_funnel.get("universe_total"),
            "eligible_symbols": analysis_funnel.get("eligible_symbols"),
            "symbols_analyzed": analysis_funnel.get("symbols_analyzed"),
            "symbols_skipped_by_reason": analysis_funnel.get("symbols_skipped_by_reason"),
            "setups_found": analysis_funnel.get("setups_found"),
            "signals_scored": analysis_funnel.get("signals_scored"),
            "signals_above_threshold": analysis_funnel.get("signals_above_threshold"),
            "signals_generated": analysis_funnel.get("signals_generated"),
            "signals_raw": pre_filter_count,
            "signals_after_position_prefilter": signals_count,
            "signals_after_cooldown": signals_after_cooldown,
            "suppress_in_position": analysis_funnel.get("suppress_in_position", 0),
            "suppress_post_close_win": analysis_funnel.get("suppress_post_close_win", 0),
            "suppress_post_close_strategic": analysis_funnel.get("suppress_post_close_strategic", 0),
            "suppress_post_close_loss": analysis_funnel.get("suppress_post_close_loss", 0),
            "suppress_global_open_throttle": analysis_funnel.get("suppress_global_open_throttle", 0),
            "suppress_other": analysis_funnel.get("suppress_other", 0),
            "risk_approved": risk_approved_count,
            "risk_rejected": risk_rejected_count,
            "auction_candidates_created": len(candidate_signals),
            "auction_opens_planned": len(plan.opens),
            "auction_opens_executed": opens_executed,
            "auction_opens_failed": opens_failed,
            "cooldowns_active": len(getattr(lt, "_signal_cooldown", {}) or {}),
            "canary_cooldown_overrides_applied": canary_overrides_applied,
            "global_chop": global_chop,
            "chop_signals": chop_signals,
            "active_chop_symbols": len(chop_active_symbols),
            "chop_canary_mode": canary_scoped_mode,
            "chop_symbol_ratio": round(chop_symbol_ratio, 3),
            "quick_reversal": int(quick_reversal_metrics.get("quick_reversal", 0) or 0),
            "opposite_reentry_fast": int(quick_reversal_metrics.get("opposite_reentry_fast", 0) or 0),
            "quick_profit_close": int(quick_reversal_metrics.get("quick_profit_close", 0) or 0),
            "quick_loss_close": int(quick_reversal_metrics.get("quick_loss_close", 0) or 0),
            "would_block_replace": would_block_replace,
            "would_block_close_no_signal": int(
                (plan.reasons or {}).get("hysteresis_close_suppressed_no_signal", 0) or 0
            ),
            "would_block_flip": anti_flip_would_block,
            "blocked_flip_enforced": anti_flip_enforced_blocks,
            "rejection_buckets": dict(funnel_rejections) if funnel_rejections else None,
            "policy_hash": policy_hash,
        }
        if _should_emit_replay_tick_log(lt, force=has_activity or activity_executed):
            logger.info("ENTRY_FUNNEL_SUMMARY", **funnel_payload)
        if opens_executed > 0 or closes_executed_count > 0 or reductions_executed > 0:
            lt._reconcile_requested = True

    except (OperationalError, DataError, ValueError, TypeError, KeyError) as e:
        logger.error("Failed to run auction allocation", error=str(e), error_type=type(e).__name__)
