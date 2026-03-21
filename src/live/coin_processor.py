"""
Coin/market universe operations and per-coin tick processing extracted from LiveTrading.

Functions in this module handle:
- Market symbol filtering (blocklist, fiat exclusion)
- Static tier lookup (deprecated legacy helper)
- Market universe discovery and update
- Per-coin signal generation, cooldown, and trace recording (process_coin)

All functions receive the LiveTrading instance as their first argument (``lt``)
to access shared state, following the same delegate pattern used by the other
extracted modules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from src.data.data_sanity import check_candle_sanity, check_ticker_sanity
from src.data.fiat_currencies import has_disallowed_base
from src.domain.models import SignalType
from src.exceptions import DataError, InvariantError, OperationalError
from src.live.cooldown_resolver import (
    attach_thesis_trace_fields as _attach_thesis_trace_fields,
)
from src.live.cooldown_resolver import (
    build_4h_warmup_skip_diagnostic as _build_4h_warmup_skip_diagnostic,
)
from src.live.cooldown_resolver import (
    normalize_symbol_key as _normalize_symbol_key,
)
from src.live.cooldown_resolver import (
    resolve_signal_cooldown_params as _resolve_signal_cooldown_params,
)
from src.monitoring.logger import get_logger
from src.storage.repository import (
    get_candles as db_get_candles,
)
from src.storage.repository import (
    get_latest_candle_timestamp,
)

if TYPE_CHECKING:
    from src.live.live_trading import LiveTrading

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# TickContext: bundles per-tick batch-fetched data for process_coin
# ---------------------------------------------------------------------------


@dataclass
class TickContext:
    """Data fetched once per tick and shared across all coin processing calls."""

    map_spot_tickers: dict[str, Any]
    map_futures_tickers: dict[str, Decimal]
    map_futures_tickers_full: dict[str, Any] | None
    map_positions: dict[str, dict]
    recent_close_by_symbol: dict[str, dict[str, Any]]
    analysis_funnel: dict[str, Any] = field(default_factory=dict)

    def af_inc(self, key: str, amount: int = 1) -> None:
        """Increment an analysis funnel counter."""
        self.analysis_funnel[key] = int(self.analysis_funnel.get(key, 0) or 0) + amount

    def af_skip(self, reason: str) -> None:
        """Record a skip reason in the analysis funnel."""
        skips = self.analysis_funnel.setdefault("symbols_skipped_by_reason", {})
        skips[reason] = int(skips.get(reason, 0) or 0) + 1


# ---------------------------------------------------------------------------
# Market symbol filtering
# ---------------------------------------------------------------------------


def market_symbols(lt: LiveTrading) -> list[str]:
    """Return list of spot symbols. Handles both list and dict. Excludes blocklist."""
    blocklist = set(
        s.strip().upper() for s in getattr(lt.config.exchange, "spot_ohlcv_blocklist", []) or []
    )
    # Also honor assets.blacklist
    blocklist |= set(s.strip().upper() for s in getattr(lt.config.assets, "blacklist", []) or [])
    # Also honor execution entry blocklist for universe filtering
    blocklist |= set(
        s.strip().upper().split(":")[0]
        for s in getattr(lt.config.execution, "entry_blocklist_spot_symbols", []) or []
    )
    blocked_bases = set(
        b.strip().upper() for b in getattr(lt.config.execution, "entry_blocklist_bases", []) or []
    )

    if isinstance(lt.markets, dict):
        raw = list(lt.markets.keys())
    else:
        raw = list(lt.markets)

    if not blocklist and not blocked_bases:
        return raw

    out: list[str] = []
    for s in raw:
        key = s.strip().upper().split(":")[0] if s else ""
        if not key:
            continue
        if key in blocklist:
            continue
        # Global exclusion: never include fiat/stablecoin-base instruments
        if has_disallowed_base(key):
            continue
        if blocked_bases:
            base = key.split("/")[0].strip() if "/" in key else key
            if base in blocked_bases:
                continue
        out.append(s)
    return out


# ---------------------------------------------------------------------------
# Static tier lookup (deprecated)
# ---------------------------------------------------------------------------


def get_static_tier(lt: LiveTrading, symbol: str) -> str | None:
    """DEPRECATED: Debug-only legacy tier lookup.

    Looks up the symbol in config coin_universe.liquidity_tiers (candidate
    groups, not tier assignments). For authoritative tier classification, use
    ``lt.market_discovery.get_symbol_tier(symbol)``.

    Returns "A", "B", "C", or None.
    """
    if not getattr(lt.config, "coin_universe", None) or not getattr(
        lt.config.coin_universe, "enabled", False
    ):
        return None
    tiers = getattr(lt.config.coin_universe, "liquidity_tiers", None) or {}
    for tier in ("A", "B", "C"):
        if symbol in tiers.get(tier, []):
            return tier
    return None


# ---------------------------------------------------------------------------
# Market universe discovery
# ---------------------------------------------------------------------------


async def update_market_universe(lt: LiveTrading) -> None:
    """Discover and update trading universe."""
    if not lt.config.exchange.use_market_discovery:
        return

    try:
        logger.info("Executing periodic market discovery...")
        mapping = await lt.market_discovery.discover_markets()

        if not mapping:
            cooldown_min = getattr(
                lt.config.exchange,
                "market_discovery_failure_log_cooldown_minutes",
                60,
            )
            now = datetime.now(UTC)
            should_log = (
                lt._last_discovery_error_log_time is None
                or (now - lt._last_discovery_error_log_time).total_seconds() >= cooldown_min * 60
            )
            if should_log:
                logger.critical(
                    "Market discovery empty; using existing universe; "
                    "check spot/futures market fetch (get_spot_markets/get_futures_markets)."
                )
                lt._last_discovery_error_log_time = now
            return

        # Shrink protection: if new universe is <50% of LAST DISCOVERED
        # universe, something is wrong (API issue, temporary outage).
        last_discovered_count = getattr(lt, "_last_discovered_count", 0)
        new_count = len(mapping)
        if last_discovered_count > 10 and new_count < last_discovered_count * 0.5:
            logger.critical(
                "UNIVERSE_SHRINK_REJECTED: new universe is <50% of last discovery -- likely API issue, keeping old universe",
                last_discovered=last_discovered_count,
                new_count=new_count,
                dropped_pct=f"{(1 - new_count / last_discovered_count) * 100:.0f}%",
            )
            try:
                from src.monitoring.alert_dispatcher import send_alert

                await send_alert(
                    "UNIVERSE_SHRINK",
                    f"Discovery returned {new_count} coins vs {last_discovered_count} last discovery -- rejected",
                    urgent=True,
                )
            except (OperationalError, ImportError, OSError):
                pass
            return

        # Track last successful discovery count
        lt._last_discovered_count = new_count

        # Log added/removed symbols vs current universe
        prev_symbols = set(market_symbols(lt))
        supported = set(mapping.keys())
        dropped = prev_symbols - supported
        added = supported - prev_symbols
        for sym in sorted(dropped):
            logger.warning("SYMBOL_REMOVED", symbol=sym)
        for sym in sorted(added):
            logger.info("SYMBOL_ADDED", symbol=sym)

        # Update internal state (Maintain Spot -> Futures mapping)
        lt.markets = mapping
        lt.futures_adapter.set_spot_to_futures_override(mapping)

        # Update Data Acquisition
        new_spot_symbols = list(mapping.keys())
        new_futures_symbols = list(mapping.values())
        lt.data_acq.update_symbols(new_spot_symbols, new_futures_symbols)

        logger.info("Market universe updated", count=len(lt.markets))

    except (OperationalError, DataError) as e:
        logger.error("Failed to update market universe", error=str(e), error_type=type(e).__name__)


# ---------------------------------------------------------------------------
# Per-coin tick processing (extracted from LiveTrading._tick)
# ---------------------------------------------------------------------------


async def process_coin(lt: LiveTrading, ctx: TickContext, spot_symbol: str) -> None:
    """Analyze a single coin: price resolution, signal generation, cooldown, trace recording.

    This is the inner loop body extracted from ``LiveTrading._tick()``.
    It accesses ``lt`` for shared components and ``ctx`` for per-tick batch data.
    """
    try:
        ctx.af_inc("symbols_analyzed")
        # Use futures tickers for improved mapping
        futures_symbol = lt.futures_adapter.map_spot_to_futures(
            spot_symbol, futures_tickers=ctx.map_futures_tickers
        )
        has_spot = spot_symbol in ctx.map_spot_tickers
        has_futures = futures_symbol in ctx.map_futures_tickers

        # Debug: Log when futures symbol not found
        if not has_futures and spot_symbol in ctx.map_spot_tickers:
            similar = [
                s for s in ctx.map_futures_tickers if spot_symbol.split("/")[0].upper() in s.upper()
            ][:3]
            logger.debug(
                "Futures symbol not found for signal",
                spot_symbol=spot_symbol,
                mapped_futures=futures_symbol,
                similar_futures=similar,
                total_futures_available=len(ctx.map_futures_tickers),
            )

        if not has_spot and not has_futures:
            ctx.af_skip("no_ticker_spot_and_futures")
            return

        # --- Price resolution ---
        spot_price = _resolve_spot_price(lt, ctx, spot_symbol, has_spot)
        mark_price = ctx.map_futures_tickers.get(futures_symbol) if has_futures else None
        if not mark_price:
            mark_price = spot_price
        if (not mark_price or Decimal(str(mark_price)) <= 0) and getattr(
            lt, "_replay_relaxed_signal_gates", False
        ):
            mark_price, spot_price = await _replay_price_fallback(
                lt, spot_symbol, mark_price, spot_price
            )
        if not mark_price:
            ctx.af_skip("no_mark_price")
            return
        if spot_price is None:
            spot_price = mark_price

        # --- Tradability gate ---
        skip_reason = _check_tradability(lt, spot_symbol, futures_symbol, has_spot, has_futures)
        is_tradable = skip_reason is None
        if is_tradable:
            ctx.af_inc("eligible_symbols")
        else:
            ctx.af_skip(skip_reason or "not_tradable")

        # --- STAGE A: Futures ticker sanity (pre-I/O) ---
        if ctx.map_futures_tickers_full is not None:
            futures_ticker_full = ctx.map_futures_tickers_full.get(futures_symbol)
            stage_a = check_ticker_sanity(
                symbol=spot_symbol,
                futures_ticker=futures_ticker_full,
                spot_ticker=ctx.map_spot_tickers.get(spot_symbol),
                thresholds=lt.sanity_thresholds,
            )
            if not stage_a.passed:
                if getattr(lt, "_replay_relaxed_signal_gates", False):
                    ctx.af_skip(f"stage_a_relaxed_bypass_{stage_a.reason or 'failed'}")
                else:
                    ctx.af_skip(f"stage_a_{stage_a.reason or 'failed'}")
                    lt.data_quality_tracker.record_result(
                        spot_symbol,
                        passed=False,
                        reason=stage_a.reason,
                    )
                    return

        # --- Update candles ---
        await lt._update_candles(spot_symbol)
        if getattr(lt, "_replay_relaxed_signal_gates", False):
            _replay_hydrate_candles(lt, spot_symbol)

        # --- Position management (V2) ---
        position_data = ctx.map_positions.get(futures_symbol)
        if position_data:
            await _evaluate_position_v2(lt, position_data, mark_price)

        # --- ShockGuard entry pause ---
        if lt.shock_guard and lt.shock_guard.should_pause_entries():
            logger.debug(
                "ShockGuard: Skipping signal generation (entries paused)",
                symbol=spot_symbol,
            )
            return

        # --- Candle stats + data sanity Stage B ---
        candles = lt.candle_manager.get_candles(spot_symbol, "15m")
        candle_count = len(candles)
        _update_processing_stats(lt, spot_symbol, candle_count)

        stage_b = check_candle_sanity(
            symbol=spot_symbol,
            candle_manager=lt.candle_manager,
            thresholds=lt.sanity_thresholds,
        )
        if not stage_b.passed:
            _handle_stage_b_failure(lt, ctx, spot_symbol, futures_symbol, stage_b)
            if not getattr(lt, "_replay_relaxed_signal_gates", False):
                return

        lt.data_quality_tracker.record_result(spot_symbol, passed=True)

        # --- Signal generation ---
        signal = lt.smc_engine.generate_signal(
            symbol=spot_symbol,
            regime_candles_1d=lt.candle_manager.get_candles(spot_symbol, "1d"),
            decision_candles_4h=lt.candle_manager.get_candles(spot_symbol, "4h"),
            refine_candles_1h=lt.candle_manager.get_candles(spot_symbol, "1h"),
            refine_candles_15m=candles,
        )
        ctx.af_inc("signals_scored")
        if signal.signal_type != SignalType.NO_SIGNAL:
            ctx.af_inc("setups_found")
            ctx.af_inc("signals_above_threshold")

        # --- Signal cooldown + spread gate + execution ---
        order_outcome = None
        if signal.signal_type != SignalType.NO_SIGNAL:
            order_outcome = await _handle_signal_with_cooldown(
                lt,
                ctx,
                spot_symbol,
                futures_symbol,
                signal,
                spot_price,
                mark_price,
                position_data,
                is_tradable,
                skip_reason,
            )

        # --- Trace recording ---
        await _record_traces(
            lt,
            ctx,
            spot_symbol,
            futures_symbol,
            signal,
            spot_price,
            candle_count,
            is_tradable,
            skip_reason,
            order_outcome,
        )

        if lt.hardening:
            lt.hardening.record_coin_processed()

    except (OperationalError, DataError) as e:
        logger.warning(f"Error processing {spot_symbol}", error=str(e), error_type=type(e).__name__)
    except Exception as e:
        logger.error(
            f"Unexpected error processing {spot_symbol}",
            error=str(e),
            error_type=type(e).__name__,
        )
        raise


# ---------------------------------------------------------------------------
# Sub-functions for process_coin (keep module-private)
# ---------------------------------------------------------------------------


def _resolve_spot_price(
    lt: LiveTrading, ctx: TickContext, spot_symbol: str, has_spot: bool
) -> Decimal | None:
    """Resolve spot price from ticker data."""
    if not has_spot:
        return None
    spot_ticker = ctx.map_spot_tickers[spot_symbol]
    spot_price = Decimal(str(spot_ticker.get("last", 0) or 0))
    if spot_price <= 0 and getattr(lt, "_replay_relaxed_signal_gates", False):
        bid = Decimal(str(spot_ticker.get("bid", 0) or 0))
        ask = Decimal(str(spot_ticker.get("ask", 0) or 0))
        if bid > 0 and ask > 0:
            spot_price = (bid + ask) / 2
    return spot_price


async def _replay_price_fallback(
    lt: LiveTrading,
    spot_symbol: str,
    mark_price: Decimal | None,
    spot_price: Decimal | None,
) -> tuple[Decimal | None, Decimal | None]:
    """Bootstrap price from candles/DB when ticker carries zeros (replay mode)."""
    await lt._update_candles(spot_symbol)
    replay_candles = lt.candle_manager.get_candles(spot_symbol, "15m")
    if replay_candles:
        last_close = Decimal(str(replay_candles[-1].close))
        if last_close > 0:
            mark_price = last_close
            if not spot_price or spot_price <= 0:
                spot_price = last_close
    if not mark_price or Decimal(str(mark_price)) <= 0:
        for tf in ("15m", "1h", "4h", "1d"):
            latest_ts = get_latest_candle_timestamp(spot_symbol, tf)
            if not latest_ts:
                continue
            candles = db_get_candles(spot_symbol, tf, start_time=latest_ts, end_time=latest_ts)
            if not candles:
                continue
            db_close = Decimal(str(candles[-1].close))
            if db_close > 0:
                mark_price = db_close
                if not spot_price or spot_price <= 0:
                    spot_price = db_close
                break
    return mark_price, spot_price


def _check_tradability(
    lt: LiveTrading,
    spot_symbol: str,
    futures_symbol: str,
    has_spot: bool,
    has_futures: bool,
) -> str | None:
    """Check whether a symbol is tradable. Returns skip reason or None."""
    has_spec = True
    if has_futures and getattr(lt, "instrument_spec_registry", None):
        try:
            has_spec = lt.instrument_spec_registry.get_spec(futures_symbol) is not None
        except (ValueError, TypeError, KeyError, AttributeError):
            has_spec = False

    if getattr(lt, "_replay_relaxed_signal_gates", False) and has_spot and not has_futures:
        return None
    if not has_futures:
        return "no_futures_ticker"
    if not has_spec:
        # Throttle log spam
        now = datetime.now(UTC)
        last_map = getattr(lt, "_last_no_spec_log", {})
        last = last_map.get(futures_symbol, datetime.min.replace(tzinfo=UTC))
        if (now - last).total_seconds() >= 3600:
            logger.warning(
                "Signal skipped (no instrument spec)",
                spot_symbol=spot_symbol,
                futures_symbol=futures_symbol,
                reason="NO_SPEC",
                hint="Futures ticker exists but instrument specs missing; likely delisted/non-tradeable. Skipping to avoid AUCTION_OPEN_REJECTED spam.",
            )
            last_map[futures_symbol] = now
            lt._last_no_spec_log = last_map
        return "no_instrument_spec"
    return None


def _replay_hydrate_candles(lt: LiveTrading, spot_symbol: str) -> None:
    """Hydrate in-memory candles from DB cache when exchange pulls are unavailable (replay)."""
    now_utc = datetime.now(UTC)
    lookback_days_by_tf = {"15m": 14, "1h": 60, "4h": 180, "1d": 365}
    for tf, lookback_days in lookback_days_by_tf.items():
        if lt.candle_manager.get_candles(spot_symbol, tf):
            continue
        candles = db_get_candles(
            spot_symbol,
            tf,
            start_time=now_utc - timedelta(days=lookback_days),
            end_time=now_utc,
        )
        if candles:
            lt.candle_manager.candles.setdefault(tf, {})[spot_symbol] = candles


async def _evaluate_position_v2(
    lt: LiveTrading,
    position_data: dict,
    mark_price: Decimal,
) -> None:
    """Run V2 position management evaluation."""
    symbol = position_data["symbol"]
    try:
        v2_actions = lt.position_manager_v2.evaluate_position(
            symbol=symbol,
            current_price=mark_price,
            current_atr=None,
        )
        if v2_actions:
            await lt.execution_gateway.execute_actions(v2_actions)
    except InvariantError:
        raise
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


def _update_processing_stats(lt: LiveTrading, spot_symbol: str, candle_count: int) -> None:
    """Update coin processing stats and detect data depth drops."""
    if spot_symbol not in lt.coin_processing_stats:
        lt.coin_processing_stats[spot_symbol] = {
            "processed_count": 0,
            "last_processed": datetime.min.replace(tzinfo=UTC),
            "candle_count": 0,
        }
    prev_count = lt.coin_processing_stats[spot_symbol]["candle_count"]
    lt.coin_processing_stats[spot_symbol]["processed_count"] += 1
    lt.coin_processing_stats[spot_symbol]["last_processed"] = datetime.now(UTC)
    lt.coin_processing_stats[spot_symbol]["candle_count"] = candle_count

    if prev_count > 50 and candle_count == 0:
        logger.critical("Data Depth Drop Detected!", symbol=spot_symbol, prev=prev_count, now=0)


def _handle_stage_b_failure(
    lt: LiveTrading,
    ctx: TickContext,
    spot_symbol: str,
    futures_symbol: str,
    stage_b: Any,
) -> None:
    """Handle stage B (candle sanity) failure: log diagnostic and record skip."""
    candles_4h = lt.candle_manager.get_candles(
        spot_symbol,
        lt.sanity_thresholds.decision_tf,
    )
    warmup_diag = _build_4h_warmup_skip_diagnostic(
        strategy_config=lt.config.strategy,
        symbol=spot_symbol,
        futures_symbol=futures_symbol,
        stage_b_reason=stage_b.reason,
        candles_4h=candles_4h,
        required_candles=lt.sanity_thresholds.min_decision_tf_candles,
        decision_tf=lt.sanity_thresholds.decision_tf,
    )
    if warmup_diag:
        logger.info("4h_warmup_skip", **warmup_diag)
    if getattr(lt, "_replay_relaxed_signal_gates", False):
        ctx.af_skip(f"stage_b_relaxed_bypass_{stage_b.reason or 'failed'}")
    else:
        ctx.af_skip(f"stage_b_{stage_b.reason or 'failed'}")
        lt.data_quality_tracker.record_result(
            spot_symbol,
            passed=False,
            reason=stage_b.reason,
        )


async def _handle_signal_with_cooldown(
    lt: LiveTrading,
    ctx: TickContext,
    spot_symbol: str,
    futures_symbol: str,
    signal: Any,
    spot_price: Decimal,
    mark_price: Decimal,
    position_data: dict | None,
    is_tradable: bool,
    skip_reason: str | None,
) -> dict | None:
    """Handle signal cooldown evaluation, spread check, and execution."""
    cooldown_params = _resolve_signal_cooldown_params(lt.config.strategy, spot_symbol)
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
    close_ctx = ctx.recent_close_by_symbol.get(symbol_key)
    now_cd = datetime.now(UTC)

    cooldown_kind = None
    cooldown_until = None
    last_close_at = None
    last_close_reason = None

    if has_position:
        cooldown_kind = "IN_POSITION"
        cooldown_until = lt._signal_cooldown.get(spot_symbol)
        if cooldown_until is None:
            cooldown_until = now_cd + timedelta(hours=in_position_cooldown_hours)
            lt._signal_cooldown[spot_symbol] = cooldown_until
    else:
        lt._signal_cooldown.pop(spot_symbol, None)
        if close_ctx:
            cooldown_kind = str(close_ctx["cooldown_kind"])
            last_close_at = close_ctx["last_close_at"]
            last_close_reason = close_ctx["last_close_reason"]
            cooldown_until = last_close_at + timedelta(minutes=int(close_ctx["cooldown_minutes"]))

    if getattr(lt, "_replay_relaxed_signal_gates", False):
        cooldown_until = None

    if cooldown_until and now_cd < cooldown_until:
        _log_cooldown_suppression(
            ctx,
            spot_symbol,
            has_position,
            cooldown_kind,
            in_position_cooldown_hours,
            close_ctx,
            last_open_at,
            last_close_at,
            last_close_reason,
            cooldown_until,
            now_cd,
            cooldown_canary_applied,
        )
        return None

    # Pre-entry spread check
    spread_ok = _check_spread(ctx, spot_symbol, signal)
    if not spread_ok:
        ctx.af_skip("spread_guard")
        return None

    # Record cooldown for this symbol
    if has_position:
        lt._signal_cooldown[spot_symbol] = now_cd + timedelta(hours=in_position_cooldown_hours)

    # Collect signal for auction mode
    if lt.auction_allocator and is_tradable:
        lt.auction_signals_this_tick.append((signal, spot_price, mark_price))
        ctx.af_inc("signals_generated")

    if not is_tradable:
        logger.warning(
            "Signal skipped (not tradable)",
            symbol=spot_symbol,
            signal=signal.signal_type.value,
            futures_symbol=futures_symbol,
            skip_reason=skip_reason,
        )
        return None

    # In auction mode, skip individual signal handling
    if lt.auction_allocator:
        return None

    return await lt._handle_signal(signal, spot_price, mark_price)


def _log_cooldown_suppression(
    ctx: TickContext,
    spot_symbol: str,
    has_position: bool,
    cooldown_kind: str | None,
    in_position_cooldown_hours: float,
    close_ctx: dict | None,
    last_open_at: Any,
    last_close_at: datetime | None,
    last_close_reason: str | None,
    cooldown_until: datetime,
    now_cd: datetime,
    cooldown_canary_applied: bool,
) -> None:
    """Log cooldown suppression and update funnel counters."""
    kind = (cooldown_kind or "GLOBAL_THROTTLE").strip().upper()
    counter_map = {
        "IN_POSITION": "suppress_in_position",
        "POST_CLOSE_WIN": "suppress_post_close_win",
        "POST_CLOSE_STRATEGIC": "suppress_post_close_strategic",
        "POST_CLOSE_LOSS": "suppress_post_close_loss",
        "GLOBAL_THROTTLE": "suppress_global_open_throttle",
    }
    ctx.af_inc(counter_map.get(kind, "suppress_other"))

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
        cooldown_until=cooldown_until.isoformat()
        if isinstance(cooldown_until, datetime)
        else str(cooldown_until),
        cooldown_remaining_seconds=max(0, int((cooldown_until - now_cd).total_seconds())),
        canary_applied=cooldown_canary_applied,
        reason="pre_auction_cooldown_active",
    )


def _check_spread(ctx: TickContext, spot_symbol: str, signal: Any) -> bool:
    """Pre-entry spread check. Returns True if spread is acceptable."""
    try:
        st = ctx.map_spot_tickers.get(spot_symbol)
        if st:
            bid = Decimal(str(st.get("bid", 0) or 0))
            ask = Decimal(str(st.get("ask", 0) or 0))
            if bid > 0 and ask > 0:
                live_spread = (ask - bid) / bid
                max_entry_spread = Decimal("0.010")
                if live_spread > max_entry_spread:
                    logger.warning(
                        "SIGNAL_REJECTED_SPREAD: live spread too wide for entry",
                        symbol=spot_symbol,
                        spread=f"{live_spread:.3%}",
                        threshold=f"{max_entry_spread:.3%}",
                        signal=signal.signal_type.value,
                    )
                    return False
    except (ValueError, TypeError, ArithmeticError, KeyError) as e:
        logger.warning(
            "Spread check failed — BLOCKING entry (fail-closed)",
            symbol=spot_symbol,
            error=str(e),
            error_type=type(e).__name__,
        )
        return False
    return True


async def _record_traces(
    lt: LiveTrading,
    ctx: TickContext,
    spot_symbol: str,
    futures_symbol: str,
    signal: Any,
    spot_price: Decimal,
    candle_count: int,
    is_tradable: bool,
    skip_reason: str | None,
    order_outcome: dict | None,
) -> None:
    """Record counterfactual decision and throttled DECISION_TRACE events."""
    now = datetime.now(UTC)

    decision_id = None
    if isinstance(getattr(signal, "meta_info", None), dict):
        decision_id = signal.meta_info.get("decision_id")
    if not decision_id:
        decision_id = f"{lt._current_cycle_id or 'cycle'}:{spot_symbol}:{int(now.timestamp())}"

    trace_details: dict[str, Any] = {
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
        "reason": signal.reasoning,
        "structure": signal.structure_info,
        "meta": signal.meta_info,
        "cycle_id": lt._current_cycle_id,
        "is_tradable": bool(is_tradable),
    }

    if lt.institutional_memory_manager and lt.institutional_memory_manager.is_enabled_for_symbol(
        spot_symbol
    ):
        thesis_snapshot = lt.institutional_memory_manager.update_conviction_for_symbol(
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

    try:
        from src.storage.repository import async_record_event

        await async_record_event(
            event_type="COUNTERFACTUAL_DECISION",
            symbol=spot_symbol,
            details=trace_details,
            decision_id=decision_id,
            timestamp=now,
        )
    except (OperationalError, DataError, OSError) as e:
        logger.debug(
            "Failed to record counterfactual decision event",
            symbol=spot_symbol,
            error=str(e),
            error_type=type(e).__name__,
        )

    # Throttled DECISION_TRACE
    last_trace = lt.last_trace_log.get(spot_symbol, datetime.min.replace(tzinfo=UTC))
    if (now - last_trace).total_seconds() > 300:
        try:
            from src.storage.repository import async_record_event

            if signal.signal_type == SignalType.NO_SIGNAL and signal.reasoning:
                logger.debug(
                    "SMC Analysis: NO_SIGNAL",
                    symbol=spot_symbol,
                    reasoning=signal.reasoning.replace("\n", " | "),
                )

            await async_record_event(
                event_type="DECISION_TRACE",
                symbol=spot_symbol,
                details=trace_details,
                decision_id=decision_id,
                timestamp=now,
            )
            lt.last_trace_log[spot_symbol] = now
        except (OperationalError, DataError, OSError) as e:
            logger.error(
                "Failed to record decision trace",
                symbol=spot_symbol,
                error=str(e),
                error_type=type(e).__name__,
            )
