"""Extracted safety checks from LiveTrading._tick().

Pure extraction — no logic changes. Delegates back to LiveTrading instance
attributes via the ``lt`` parameter.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from src.exceptions import DataError, InvariantError, OperationalError
from src.monitoring.logger import get_logger
from src.utils.kill_switch import KillSwitchReason

if TYPE_CHECKING:
    from src.live.live_trading import LiveTrading

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Batch Data Fetching (was "# 3. Batch Data Fetching" in _tick)
# ---------------------------------------------------------------------------


@dataclass
class BatchFetchResult:
    """Result of batch data fetching in _tick."""

    market_symbols: list[str]
    map_spot_tickers: dict
    map_futures_tickers: dict
    map_futures_tickers_full: dict | None
    map_positions: dict
    recent_close_by_symbol: dict
    all_raw_positions: list = field(default_factory=list)


async def fetch_batch_data(
    lt: LiveTrading, all_raw_positions: list[dict]
) -> BatchFetchResult | None:
    """Run batch data fetching for a tick.

    Returns ``None`` if the fetch fails and the tick should abort.
    Otherwise returns a populated :class:`BatchFetchResult`.
    """
    from src.data.symbol_utils import normalize_symbol_for_position_match
    from src.live.cooldown_resolver import normalize_symbol_key as _normalize_symbol_key
    from src.live.cooldown_resolver import (
        resolve_post_close_cooldown_kind_and_minutes as _resolve_post_close_cooldown_kind_and_minutes,
    )
    from src.storage.repository import get_trades_since

    try:
        _t0 = time.perf_counter()
        market_symbols = lt._market_symbols()
        # Health gate: pause new entries when candle health below threshold
        total_coins = len(market_symbols)
        coins_with_sufficient_candles = sum(
            1 for s in market_symbols if len(lt.candle_manager.get_candles(s, "15m")) >= 50
        )
        min_healthy = getattr(lt.config.data, "min_healthy_coins", 30)
        min_ratio = getattr(lt.config.data, "min_health_ratio", 0.25)
        if getattr(lt, "_replay_disable_candle_health_gate", False):
            # Replay research can run on partial windows to discover signals.
            lt.trade_paused = False
        elif total_coins > 0:
            ratio = coins_with_sufficient_candles / total_coins
            # When universe is smaller than min_healthy, require all coins to have data (ratio 1.0)
            effective_min = min(min_healthy, total_coins)
            if coins_with_sufficient_candles < effective_min or ratio < min_ratio:
                lt.trade_paused = True
                logger.critical(
                    "TRADING PAUSED: candle health insufficient",
                    coins_with_sufficient_candles=coins_with_sufficient_candles,
                    total=total_coins,
                    min_healthy_coins=min_healthy,
                    effective_min=effective_min,
                    min_health_ratio=min_ratio,
                )
            else:
                lt.trade_paused = False
        else:
            lt.trade_paused = False
        map_spot_tickers = await lt.client.get_spot_tickers_bulk(market_symbols)
        map_futures_tickers = await lt.client.get_futures_tickers_bulk()
        # Full FuturesTicker objects for data sanity gate (bid/ask/volume).
        # None => bulk fetch failed => Stage A skipped (fail-open).
        try:
            map_futures_tickers_full = await lt.client.get_futures_tickers_bulk_full()
        except (OperationalError, DataError) as _e:
            logger.warning(
                "get_futures_tickers_bulk_full failed; sanity gate will skip Stage A",
                error=str(_e),
                error_type=type(_e).__name__,
            )
            map_futures_tickers_full = None
        _t1 = time.perf_counter()
        lt.last_fetch_latency_ms = round((_t1 - _t0) * 1000)
        map_positions = {p["symbol"]: p for p in all_raw_positions}
        recent_close_by_symbol: dict[str, dict[str, Any]] = {}
        if bool(getattr(lt.config.strategy, "signal_post_close_cooldown_enabled", True)):
            close_lookback_hours = int(
                getattr(lt.config.strategy, "signal_post_close_lookback_hours", 24)
            )
            try:
                recent_trades = await asyncio.to_thread(
                    get_trades_since,
                    datetime.now(UTC) - timedelta(hours=close_lookback_hours),
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
                        lt.config.strategy,
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
        lt.latest_futures_tickers = map_futures_tickers
        # Also store on executor for use in execute_signal
        lt.executor.latest_futures_tickers = map_futures_tickers
        # Update adapter cache for use when futures_tickers not explicitly passed
        lt.futures_adapter.update_cached_futures_tickers(map_futures_tickers)

        # Ensure instrument specs are loaded (used to decide tradability and size/leverage rules).
        # This is TTL-cached; refresh() is a cheap no-op when not stale.
        if getattr(lt, "instrument_spec_registry", None):
            try:
                await lt.instrument_spec_registry.refresh()
            except (OperationalError, DataError) as e:
                logger.warning(
                    "InstrumentSpecRegistry refresh failed (non-fatal)",
                    error=str(e),
                    error_type=type(e).__name__,
                )

        # ShockGuard evaluation (extracted to tick_safety)
        if lt.shock_guard:
            await evaluate_shock_guard(
                lt, map_spot_tickers, map_futures_tickers, map_positions, all_raw_positions
            )

        # 2.4. Fetch open orders once, index by *normalized* symbol (for position hydration)
        # This is critical because positions are PF_* while CCXT orders often use unified symbols (e.g. X/USD:USD).
        orders_by_symbol: dict[str, list[dict]] = {}
        try:
            # CRITICAL: Verify client is not a mock before calling
            import os
            import sys
            from unittest.mock import MagicMock, Mock

            is_test = (
                "pytest" in sys.modules
                or "PYTEST_CURRENT_TEST" in os.environ
                or any("test" in path.lower() for path in sys.path if isinstance(path, str))
            )
            if not is_test and (isinstance(lt.client, Mock) or isinstance(lt.client, MagicMock)):
                logger.critical("CRITICAL: self.client is a Mock/MagicMock in _tick!")
                raise RuntimeError("CRITICAL: self.client is a Mock/MagicMock in production")

            open_orders = await lt.client.get_futures_open_orders()
            for order in open_orders:
                sym = order.get("symbol")
                key = normalize_symbol_for_position_match(sym) if sym else ""
                if key:
                    if key not in orders_by_symbol:
                        orders_by_symbol[key] = []
                    orders_by_symbol[key].append(order)
        except RuntimeError:
            raise  # Re-raise critical errors
        except (OperationalError, DataError) as e:
            logger.warning(
                "Failed to fetch open orders for hydration",
                error=str(e),
                error_type=type(e).__name__,
            )

        # 2.5. TP Backfill / Reconciliation (after position sync and price data fetch)
        try:
            # Build current prices map for backfill logic (use futures ticker data)
            current_prices_map = {}
            for pos_data in all_raw_positions:
                symbol = pos_data.get("symbol")
                if symbol:
                    # map_futures_tickers is Dict[str, Decimal] - mark price directly
                    mark_price = map_futures_tickers.get(symbol)
                    if mark_price:
                        current_prices_map[symbol] = mark_price
                    else:
                        current_prices_map[symbol] = Decimal(
                            str(
                                pos_data.get(
                                    "markPrice",
                                    pos_data.get("mark_price", pos_data.get("entryPrice", 0)),
                                )
                            )
                        )

            # Reconcile stop loss order IDs from exchange FIRST
            # This updates is_protected flag based on actual exchange orders
            await lt._reconcile_stop_loss_order_ids(all_raw_positions)

            # THEN do TP backfill (which checks is_protected)
            await lt._reconcile_protective_orders(all_raw_positions, current_prices_map)

            # Auto-place missing stops for unprotected positions (rate-limited per tick)
            await lt._place_missing_stops_for_unprotected(all_raw_positions, max_per_tick=3)
        except (OperationalError, DataError, ValueError) as e:
            logger.exception(
                "TP backfill reconciliation failed", error=str(e), error_type=type(e).__name__
            )
            # Don't return - continue with trading loop
        symbols_with_spot = len([s for s in market_symbols if s in map_spot_tickers])
        # Use futures_tickers for accurate coverage counting
        symbols_with_futures = len(
            [
                s
                for s in market_symbols
                if lt.futures_adapter.map_spot_to_futures(s, futures_tickers=map_futures_tickers)
                in map_futures_tickers
            ]
        )
        symbols_with_neither = len(
            [
                s
                for s in market_symbols
                if s not in map_spot_tickers
                and lt.futures_adapter.map_spot_to_futures(s, futures_tickers=map_futures_tickers)
                not in map_futures_tickers
            ]
        )
        lt._last_ticker_with = symbols_with_spot
        lt._last_ticker_without = len(market_symbols) - symbols_with_spot
        lt._last_futures_count = symbols_with_futures
        if symbols_with_neither > 0 or symbols_with_futures < len(market_symbols):
            lt._last_ticker_skip_log = getattr(
                lt, "_last_ticker_skip_log", datetime.min.replace(tzinfo=UTC)
            )
            if (datetime.now(UTC) - lt._last_ticker_skip_log).total_seconds() >= 300:
                if getattr(lt, "_replay_relaxed_signal_gates", False):
                    logger.info(
                        "Replay ticker coverage: using spot fallback",
                        total=len(market_symbols),
                        with_spot=symbols_with_spot,
                        with_futures=symbols_with_futures,
                        with_neither=symbols_with_neither,
                    )
                else:
                    logger.warning(
                        "Ticker coverage: spot/futures",
                        total=len(market_symbols),
                        with_spot=symbols_with_spot,
                        with_futures=symbols_with_futures,
                        with_neither=symbols_with_neither,
                        hint="Trading requires futures ticker; ensure bulk API keys match discovery (CCXT vs PF_*).",
                    )
                lt._last_ticker_skip_log = datetime.now(UTC)
    except (OperationalError, DataError) as e:
        logger.error("Failed batch data fetch", error=str(e), error_type=type(e).__name__)
        return None

    return BatchFetchResult(
        market_symbols=market_symbols,
        map_spot_tickers=map_spot_tickers,
        map_futures_tickers=map_futures_tickers,
        map_futures_tickers_full=map_futures_tickers_full,
        map_positions=map_positions,
        recent_close_by_symbol=recent_close_by_symbol,
        all_raw_positions=all_raw_positions,
    )


# ---------------------------------------------------------------------------
# Kill‑switch handling (was "# 0. Kill Switch Check" in _tick)
# ---------------------------------------------------------------------------


async def handle_kill_switch(lt: LiveTrading) -> bool:
    """Run kill‑switch logic. Returns ``True`` if the tick should abort."""
    ks = lt.kill_switch

    if not ks.is_active():
        return False

    replay_fail_open = bool(getattr(lt, "_replay_disable_candle_health_gate", False)) and (
        ks.reason == KillSwitchReason.DATA_FAILURE
    )
    if replay_fail_open:
        emit, suppressed = lt._rate_limited_log("replay_data_failure_kill_switch", 60)
        if emit:
            logger.warning(
                "Replay fail-open: bypassing data_failure kill switch",
                reason=ks.reason.value if ks.reason else "unknown",
                suppressed_since_last=suppressed,
            )
    else:
        # Determine if this is a recent emergency that should auto-flatten
        should_auto_flatten = False
        if ks.reason and ks.reason.allows_auto_flatten_on_startup and ks.activated_at:
            age_seconds = (datetime.now(UTC) - ks.activated_at).total_seconds()
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
                cancelled = await lt.client.cancel_all_orders()
                logger.info(f"Kill switch: Cancelled {len(cancelled)} orders")
            except InvariantError:
                raise
            except OperationalError as e:
                logger.error(
                    "Kill switch: cancel_all transient failure",
                    kill_step="cancel_all",
                    error=str(e),
                    error_type=type(e).__name__,
                )
            except Exception as e:
                logger.exception(
                    "Kill switch: unexpected error in cancel_all",
                    kill_step="cancel_all",
                    error=str(e),
                    error_type=type(e).__name__,
                )
                raise

            try:
                positions = await lt.client.get_all_futures_positions()
                for pos in positions:
                    if pos.get("size", 0) != 0:
                        symbol = pos.get("symbol")
                        try:
                            await lt.client.close_position(symbol)
                            logger.warning(f"Kill switch: Emergency closed position for {symbol}")
                        except InvariantError:
                            raise
                        except OperationalError as e:
                            logger.error(
                                "Kill switch: close_position transient failure",
                                kill_step="close_position",
                                symbol=symbol,
                                error=str(e),
                                error_type=type(e).__name__,
                            )
                        except Exception as e:
                            logger.exception(
                                "Kill switch: unexpected error closing position",
                                kill_step="close_position",
                                symbol=symbol,
                                error=str(e),
                                error_type=type(e).__name__,
                            )
                            raise
            except InvariantError:
                raise
            except OperationalError as e:
                logger.error(
                    "Kill switch: close_all transient failure",
                    kill_step="close_all",
                    error=str(e),
                    error_type=type(e).__name__,
                )
            except Exception as e:
                logger.exception(
                    "Kill switch: unexpected error in close_all",
                    kill_step="close_all",
                    error=str(e),
                    error_type=type(e).__name__,
                )
                raise
        else:
            # SAFE_HOLD path: cancel non-SL orders, verify stops, do NOT flatten
            emit, suppressed = lt._rate_limited_log("kill_switch_safe_hold", 60)
            if emit:
                logger.critical(
                    "Kill switch SAFE_HOLD: preserving positions + stops, refusing new entries",
                    reason=ks.reason.value if ks.reason else "unknown",
                    activated_at=ks.activated_at.isoformat() if ks.activated_at else "unknown",
                    suppressed_since_last=suppressed,
                )
            try:
                cancelled, preserved_sls = await ks._cancel_non_sl_orders()
                emit_cleanup, suppressed_cleanup = lt._rate_limited_log(
                    "kill_switch_safe_hold_cleanup", 60
                )
                if emit_cleanup:
                    logger.info(
                        "Kill switch SAFE_HOLD: order cleanup done",
                        cancelled_non_sl=cancelled,
                        preserved_stop_losses=preserved_sls,
                        suppressed_since_last=suppressed_cleanup,
                    )
            except InvariantError:
                raise
            except OperationalError as e:
                logger.error(
                    "Kill switch SAFE_HOLD: cancel transient failure",
                    error=str(e),
                    error_type=type(e).__name__,
                )
            except Exception as e:
                logger.exception(
                    "Kill switch SAFE_HOLD: unexpected error in cancel",
                    error=str(e),
                    error_type=type(e).__name__,
                )
                raise

    # Stop processing (no new entries while kill switch is active)
    return True


# ---------------------------------------------------------------------------
# ShockGuard evaluation (was "# ShockGuard: Evaluate shock conditions" in _tick)
# ---------------------------------------------------------------------------


def _extract_base(symbol: str) -> str | None:
    """Extract base currency from symbol."""
    for prefix in ["PI_", "PF_", "FI_"]:
        if symbol.startswith(prefix):
            symbol = symbol[len(prefix) :]
    # IMPORTANT: match longer suffixes first ("/USD:USD" must not be reduced by "USD").
    for suffix in ["/USD:USD", "/USD", "USD"]:
        if symbol.endswith(suffix):
            symbol = symbol[: -len(suffix)]
    return symbol.rstrip(":/") if symbol else None


async def evaluate_shock_guard(
    lt: LiveTrading,
    map_spot_tickers: dict,
    map_futures_tickers: dict,
    map_positions: dict,
    all_raw_positions: list,
) -> None:
    """Evaluate ShockGuard conditions and execute exposure reduction if needed."""
    # CRITICAL: Deduplicate futures tickers to one canonical symbol per asset
    # map_futures_tickers contains aliases (PI_*, PF_*, BASE/USD:USD, BASE/USD)
    # We need to pick one canonical format per asset to avoid false triggers
    extract_base = _extract_base

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
        elif (
            symbol.startswith("PF_")
            and not base_to_symbol[base].startswith("PF_")
            and "/USD:USD" not in base_to_symbol[base]
        ):
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
    lt.shock_guard.evaluate(
        mark_prices=canonical_mark_prices,
        spot_prices=spot_prices_dict if spot_prices_dict else None,
    )

    # Run exposure reduction if shock active
    if lt.shock_guard.shock_mode_active:
        # Get positions as Position objects
        positions_list = []
        liquidation_prices_dict = {}
        # Build mark prices keyed by position symbols (exchange symbols like PF_*)
        # Positions use exchange symbols, so we need mark prices for those symbols
        mark_prices_for_positions = {}
        for pos_data in all_raw_positions:
            pos = lt._convert_to_position(pos_data)
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
                mark_price = Decimal(
                    str(
                        pos_data.get(
                            "markPrice",
                            pos_data.get("mark_price", pos_data.get("entryPrice", 0)),
                        )
                    )
                )

            mark_prices_for_positions[pos_symbol] = mark_price

        # Get exposure reduction actions
        actions = lt.shock_guard.get_exposure_reduction_actions(
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
                    await lt.client.close_position(symbol)
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
                            lt.config.exchange, "position_size_is_notional", False
                        )

                        if position_size_is_notional:
                            # Size is in USD notional - trim by 50%
                            trim_notional = current_size_raw * Decimal("0.5")
                            # Convert notional to contracts using proper adapter method
                            # Use mark_prices_for_positions which is keyed by position symbols
                            mark_price = mark_prices_for_positions.get(symbol)
                            if not mark_price or mark_price <= 0:
                                # Fallback to position data
                                mark_price = Decimal(
                                    str(
                                        pos_data.get(
                                            "markPrice",
                                            pos_data.get(
                                                "mark_price",
                                                pos_data.get("entryPrice", 1),
                                            ),
                                        )
                                    )
                                )
                            if mark_price > 0:
                                # Use adapter's notional_to_contracts method for proper conversion
                                trim_size_contracts = lt.futures_adapter.notional_to_contracts(
                                    trim_notional, mark_price
                                )
                            else:
                                logger.error(
                                    "ShockGuard: Cannot trim - invalid mark price",
                                    symbol=symbol,
                                )
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
                        trim_result = await lt.execution_gateway.place_emergency_order(
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
