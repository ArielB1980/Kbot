"""
ReplayKrakenClient — Simulated exchange that implements the KrakenClient interface.

Models:
- Market/limit/stop order lifecycle with Kraken semantics
- Fill model (spread, slippage, partial fills, maker/taker)
- Stop trigger → entered_book → fill sequence
- Account state (equity, margin, positions)
- Fees (maker/taker configurable)
- Per-symbol funding rate curves with vol-spike variability
- Deterministic seeded jitter on fills, delays, slippage
- Per-API-call latency model (seeded 50-200ms)

This is a drop-in replacement for KrakenClient in replay mode.
"""

from __future__ import annotations

import asyncio
import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.backtest.replay_harness.sim_clock import SimClock
from src.backtest.replay_harness.data_store import ReplayDataStore, LiquidityParams, CandleBar
from src.data.kraken_client import FuturesTicker
from src.domain.models import Candle
from src.exceptions import OperationalError, DataError
from src.monitoring.logger import get_logger
from src.utils.circuit_breaker import APICircuitBreaker

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Order & position models
# ---------------------------------------------------------------------------

class OrderStatus(str, Enum):
    OPEN = "open"
    ENTERED_BOOK = "entered_book"  # Stop triggered, waiting to fill
    FILLED = "filled"
    CANCELLED = "cancelled"
    PARTIALLY_FILLED = "partially_filled"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    TAKE_PROFIT = "take_profit"


@dataclass
class SimOrder:
    """Simulated order on the exchange."""
    id: str
    client_order_id: Optional[str]
    symbol: str
    side: str  # "buy" or "sell"
    order_type: OrderType
    size: Decimal
    filled_size: Decimal = Decimal("0")
    price: Optional[Decimal] = None        # limit price
    stop_price: Optional[Decimal] = None   # trigger price for stops
    reduce_only: bool = False
    leverage: Optional[Decimal] = None
    status: OrderStatus = OrderStatus.OPEN
    created_at: Optional[datetime] = None
    filled_at: Optional[datetime] = None
    triggered_at: Optional[datetime] = None  # when stop triggered
    avg_fill_price: Optional[Decimal] = None
    fills: list = field(default_factory=list)  # list of (price, size, fee, is_maker)
    mid_at_placement: Optional[Decimal] = None  # for maker/taker determination


@dataclass
class SimPosition:
    """Simulated exchange position."""
    symbol: str
    side: str  # "long" or "short"
    size: Decimal
    entry_price: Decimal
    unrealized_pnl: Decimal = Decimal("0")
    leverage: Decimal = Decimal("7")
    liquidation_price: Optional[Decimal] = None


@dataclass
class SimFill:
    """A single fill event."""
    order_id: str
    symbol: str
    side: str
    price: Decimal
    size: Decimal
    fee: Decimal
    is_maker: bool
    timestamp: datetime
    reduce_only: bool = False


# ---------------------------------------------------------------------------
# Exchange config
# ---------------------------------------------------------------------------

@dataclass
class FundingCurve:
    """Per-symbol funding rate curve.

    base_rate_8h_bps: normal funding rate per 8h in bps
    vol_spike_multiplier: multiplier applied when volatility_regime is "high"/"extreme"
    """
    base_rate_8h_bps: float = 1.0
    vol_spike_multiplier: float = 3.0  # 3x funding during high vol


@dataclass
class ExchangeSimConfig:
    """Configuration for the simulated exchange."""
    initial_equity_usd: Decimal = Decimal("10000")
    maker_fee_bps: float = 2.0
    taker_fee_bps: float = 5.0
    funding_rate_8h_bps: float = 1.0  # flat fallback funding per 8h
    default_leverage: Decimal = Decimal("7")
    # Fill model
    slippage_factor: float = 0.5   # multiplier for slippage calc
    partial_fill_probability: float = 0.0  # 0 = always full fill
    # Stop entered_book delay: base value, scaled by vol/depth at runtime
    stop_entered_book_delay_base_seconds: float = 1.0
    # Maker/taker: fallback probability ONLY when mid at placement is unknown
    maker_probability_fallback: float = 0.8
    # Order rejection realism
    min_order_size_usd: float = 5.0  # min notional for an order
    reject_reduce_only_conflicts: bool = True
    reject_insufficient_margin: bool = True
    # fetch_open_orders visibility: if True, entered_book orders are hidden
    # from get_futures_open_orders (like real Kraken Layer 1 quirk)
    hide_entered_book_from_open_orders: bool = False
    # -- Deterministic jitter (seeded) --
    jitter_seed: int = 42       # seed for all randomness; change to run variants
    jitter_enabled: bool = True  # add micro-jitter to fills, delays, slippage
    jitter_fill_bps: float = 2.0      # ±2 bps fill price jitter
    jitter_delay_pct: float = 0.20    # ±20% delay jitter
    jitter_slippage_pct: float = 0.15  # ±15% slippage jitter
    # -- Per-symbol funding curves --
    funding_curves: Optional[Dict[str, FundingCurve]] = None
    # -- API latency model --
    latency_enabled: bool = False  # inject simulated API latency
    latency_base_ms: float = 50.0
    latency_max_ms: float = 200.0


# ---------------------------------------------------------------------------
# Main exchange simulator
# ---------------------------------------------------------------------------

class ReplayKrakenClient:
    """Simulated Kraken futures exchange for replay backtesting.

    Implements the full KrakenClient public interface.
    All methods are async to match the real client.
    """

    def __init__(
        self,
        clock: SimClock,
        data_store: ReplayDataStore,
        config: Optional[ExchangeSimConfig] = None,
        fault_injector: Optional[Any] = None,
        # KrakenClient compat params (ignored)
        api_key: str = "",
        api_secret: str = "",
        futures_api_key: Optional[str] = None,
        futures_api_secret: Optional[str] = None,
        use_testnet: bool = False,
        *,
        market_cache_minutes: int = 60,
        dry_run: bool = False,
        breaker_failure_threshold: int = 5,
        breaker_rate_limit_threshold: int = 2,
        breaker_cooldown_seconds: float = 60.0,
    ):
        self._clock = clock
        self._data = data_store
        self._config = config or ExchangeSimConfig()
        self._fault = fault_injector
        self._dry_run = dry_run

        # Deterministic RNG for jitter (seeded for reproducibility)
        self._rng = random.Random(self._config.jitter_seed)

        # Account state
        self._equity = self._config.initial_equity_usd
        self._available_margin = self._config.initial_equity_usd
        self._margin_used = Decimal("0")
        self._realized_pnl = Decimal("0")
        self._total_fees = Decimal("0")
        self._total_funding = Decimal("0")

        # Order book
        self._orders: Dict[str, SimOrder] = {}  # order_id -> SimOrder
        self._positions: Dict[str, SimPosition] = {}  # symbol -> SimPosition
        self._fill_log: List[SimFill] = []

        # Funding tracking
        self._last_funding_time: Optional[datetime] = None
        self._funding_log: List[Dict[str, Any]] = []  # per-event funding records

        # API circuit breaker (for interface compat)
        self._api_breaker = APICircuitBreaker(
            failure_threshold=breaker_failure_threshold,
            rate_limit_threshold=breaker_rate_limit_threshold,
            cooldown_seconds=breaker_cooldown_seconds,
            name="replay_api",
        )

        # Metrics
        self._metrics: Dict[str, Any] = {
            "orders_placed": 0,
            "orders_filled": 0,
            "orders_cancelled": 0,
            "orders_rejected": 0,
            "stops_triggered": 0,
            "partial_fills": 0,
            "total_fills": 0,
            "reduce_only_rejections": 0,
            "insufficient_margin_rejections": 0,
            "min_size_rejections": 0,
            "mid_fallback_count": 0,  # Fix 1 micro-add: tracks data store gaps
            "funding_events": 0,
            "latency_injected_ms_total": 0.0,
            "trades_closed": 0,
        }
        self._closed_trades: List[Dict[str, Any]] = []
        self._symbol_resolution_cache: Dict[str, str] = {}

    # -- Initialization --

    def _resolve_market_symbol(self, symbol: str) -> str:
        """Map futures-style symbols to loaded replay market symbols."""
        raw = str(symbol or "").strip()
        if not raw:
            return raw
        if raw in self._symbol_resolution_cache:
            return self._symbol_resolution_cache[raw]

        available = set(self._data.get_all_symbols())
        candidates: list[str] = []

        def _add(val: str) -> None:
            if val and val not in candidates:
                candidates.append(val)

        core = raw.split(":", 1)[0]
        _add(raw)
        _add(core)
        _add(core.replace("XBT", "BTC"))
        if core.startswith("PF_"):
            core = core[3:]
            _add(core)
            _add(core.replace("XBT", "BTC"))
        if "/" not in core and core.endswith("USD") and len(core) > 3:
            s = f"{core[:-3]}/USD"
            _add(s)
            _add(s.replace("XBT", "BTC"))

        resolved = next((c for c in candidates if c in available), candidates[0] if candidates else raw)
        self._symbol_resolution_cache[raw] = resolved
        return resolved

    def _market_bar_at(self, symbol: str, at: datetime) -> Optional[CandleBar]:
        """Best-effort current bar lookup across loaded replay timeframes."""
        resolved = self._resolve_market_symbol(symbol)
        for tf in ("1m", "5m", "15m", "1h", "4h", "1d"):
            bar = self._data.get_candle_at(resolved, tf, at)
            if bar is not None:
                return bar
        return None

    async def initialize(self) -> None:
        """No-op for replay."""
        pass

    async def close(self) -> None:
        """No-op for replay."""
        pass

    async def close_http_session(self) -> None:
        pass

    @property
    def api_breaker(self) -> APICircuitBreaker:
        return self._api_breaker

    def has_valid_spot_credentials(self) -> bool:
        return True

    def has_valid_futures_credentials(self) -> bool:
        return True

    # -- Fault injection + latency hook --

    def _check_fault(self, method_name: str) -> None:
        """Check if a fault should be injected for this method call."""
        if self._fault:
            self._fault.maybe_inject(method_name, self._clock.now())

    async def _maybe_inject_latency(self) -> None:
        """Inject simulated API latency (seeded, deterministic).

        Does NOT block the clock — it yields to the event loop for 0 seconds
        but advances the sim clock by the latency amount. This surfaces
        race conditions where two coroutines interleave.
        """
        if not self._config.latency_enabled:
            return
        base = self._config.latency_base_ms
        max_ms = self._config.latency_max_ms
        latency_ms = base + self._rng.random() * (max_ms - base)
        self._metrics["latency_injected_ms_total"] += latency_ms
        # Advance sim clock by the latency
        self._clock.advance(seconds=latency_ms / 1000.0)
        # Yield to event loop to allow interleaving
        await asyncio.sleep(0)

    # -- Core simulation: advance exchange state --

    def step(self, now: Optional[datetime] = None) -> List[SimFill]:
        """Advance the exchange simulation by one step.

        Called by BacktestRunner after each clock advance.
        Processes: stop triggers, limit fills, funding.

        Returns list of fills that occurred.
        """
        now = now or self._clock.now()
        new_fills: List[SimFill] = []

        pending_count = sum(
            1 for o in self._orders.values()
            if o.status not in (OrderStatus.FILLED, OrderStatus.CANCELLED)
        )
        if pending_count > 0:
            logger.info(
                "EXCHANGE_SIM_STEP",
                now=str(now),
                pending_orders=pending_count,
                total_orders=len(self._orders),
            )

        for order_id, order in list(self._orders.items()):
            if order.status in (OrderStatus.FILLED, OrderStatus.CANCELLED):
                continue

            bar = self._market_bar_at(order.symbol, now)
            if bar is None:
                logger.info(
                    "EXCHANGE_SIM_NO_BAR",
                    order_id=order.id,
                    symbol=order.symbol,
                    order_type=order.order_type.value,
                    now=str(now),
                )
                continue

            liq = self._data.get_liquidity_at(order.symbol, now)

            # Process stops: trigger check
            if order.order_type in (OrderType.STOP, OrderType.TAKE_PROFIT) and order.status == OrderStatus.OPEN:
                if self._should_trigger_stop(order, bar):
                    order.status = OrderStatus.ENTERED_BOOK
                    order.triggered_at = now
                    self._metrics["stops_triggered"] += 1
                    logger.debug("Stop triggered", order_id=order.id, symbol=order.symbol,
                                stop_price=str(order.stop_price), bar_low=str(bar.low), bar_high=str(bar.high))

            # Process entered_book stops: fill after vol/depth-dependent delay
            if order.status == OrderStatus.ENTERED_BOOK:
                delay_secs = self._compute_entered_book_delay(liq)
                delay = timedelta(seconds=delay_secs)
                if order.triggered_at and now >= order.triggered_at + delay:
                    fills = self._fill_market_order(order, bar, liq, now)
                    new_fills.extend(fills)

            # Process market orders: immediate fill
            if order.order_type == OrderType.MARKET and order.status == OrderStatus.OPEN:
                fills = self._fill_market_order(order, bar, liq, now)
                new_fills.extend(fills)

            # Process limit orders: check if price crossed
            if order.order_type == OrderType.LIMIT and order.status == OrderStatus.OPEN:
                fills = self._try_fill_limit(order, bar, liq, now)
                if fills:
                    logger.info(
                        "LIMIT_ORDER_FILLED",
                        order_id=order.id,
                        symbol=order.symbol,
                        side=order.side,
                        price=str(order.price),
                        size=str(fills[0].size),
                        bar_low=str(bar.low),
                        bar_high=str(bar.high),
                        reduce_only=order.reduce_only,
                    )
                new_fills.extend(fills)

        # Apply funding
        self._apply_funding(now)

        # Update unrealized PnL
        self._update_unrealized_pnl(now)

        return new_fills

    # -- Stop trigger logic (Kraken semantics) --

    def _should_trigger_stop(self, order: SimOrder, bar: CandleBar) -> bool:
        if order.stop_price is None:
            return False

        if order.order_type == OrderType.STOP:
            # Stop-loss: triggers when price reaches stop level
            if order.side == "buy":
                # Buy stop: triggers when price >= stop_price (short position SL)
                return bar.high >= order.stop_price
            else:
                # Sell stop: triggers when price <= stop_price (long position SL)
                return bar.low <= order.stop_price

        if order.order_type == OrderType.TAKE_PROFIT:
            if order.side == "buy":
                # Buy TP: triggers when price <= tp_price (short cover)
                return bar.low <= order.stop_price
            else:
                # Sell TP: triggers when price >= tp_price (long profit)
                return bar.high >= order.stop_price

        return False

    # -- Entered_book delay: f(volatility, depth) --

    def _compute_entered_book_delay(self, liq: LiquidityParams) -> float:
        """Compute entered_book → fill delay based on liquidity regime.

        Calm + deep   → near-instant (base * 0.2)
        Normal        → base * 1.0
        High vol      → base * 3.0
        Extreme + thin→ base * 8.0

        Adds deterministic seeded jitter (±jitter_delay_pct) so safety
        holds across slight timing variations, not just exact boundaries.
        """
        base = self._config.stop_entered_book_delay_base_seconds

        # Volatility multiplier
        regime = liq.volatility_regime
        vol_mult = {"low": 0.2, "normal": 1.0, "high": 3.0, "extreme": 8.0}.get(regime, 1.0)

        # Depth multiplier: thinner book → longer delay
        depth = max(liq.depth_usd_at_1bp, 1.0)
        if depth > 80_000:
            depth_mult = 0.5
        elif depth > 30_000:
            depth_mult = 1.0
        elif depth > 10_000:
            depth_mult = 2.0
        else:
            depth_mult = 4.0

        delay = base * max(vol_mult, depth_mult)

        # Deterministic jitter
        if self._config.jitter_enabled:
            pct = self._config.jitter_delay_pct
            jitter = self._rng.uniform(-pct, pct)
            delay *= (1.0 + jitter)

        return max(0.0, delay)

    # -- Maker/taker determination for limit orders --

    def _determine_limit_maker_taker(self, order: SimOrder, bar: CandleBar) -> bool:
        """Determine if a limit fill is maker or taker.

        Rule (faithful to exchange semantics):
        - If limit price crossed the mid at placement time → taker (you crossed the spread)
        - If it fills later because price traded through → maker
        - Fallback to candle open if mid at placement is unknown (tracked as data gap).
        """
        mid_at_place = order.mid_at_placement
        if mid_at_place is None:
            # Fallback: use candle open as proxy for mid at placement
            mid_at_place = bar.open
            self._metrics["mid_fallback_count"] += 1

        if order.price is None:
            return False  # shouldn't happen for limits

        if order.side == "buy":
            # Buy limit at or above mid → crosses spread → taker
            if order.price >= mid_at_place:
                return False  # taker
            else:
                return True  # maker (resting, filled by price coming down)
        else:
            # Sell limit at or below mid → crosses spread → taker
            if order.price <= mid_at_place:
                return False  # taker
            else:
                return True  # maker (resting, filled by price coming up)

    # -- Fill models --

    def _fill_market_order(
        self, order: SimOrder, bar: CandleBar, liq: LiquidityParams, now: datetime
    ) -> List[SimFill]:
        """Fill a market order (or entered_book stop) with slippage model + jitter."""
        mid = (bar.high + bar.low) / 2
        spread_half = mid * liq.spread_fraction / 2

        # Slippage = f(notional / depth, volatility)
        notional = float(order.size * mid)
        depth = max(liq.depth_usd_at_1bp, 1.0)
        slippage_mult = self._config.slippage_factor * (notional / depth)

        # Deterministic jitter on slippage
        if self._config.jitter_enabled:
            pct = self._config.jitter_slippage_pct
            jitter = self._rng.uniform(-pct, pct)
            slippage_mult *= (1.0 + jitter)

        slippage = mid * Decimal(str(min(max(slippage_mult, 0), 0.01)))  # cap at 1%

        if order.side == "buy":
            fill_price = mid + spread_half + slippage
            # For stops, fill at stop_price if worse
            if order.stop_price and order.stop_price > fill_price:
                fill_price = order.stop_price
        else:
            fill_price = mid - spread_half - slippage
            if order.stop_price and order.stop_price < fill_price:
                fill_price = order.stop_price

        # Deterministic jitter on fill price (±fill_bps)
        if self._config.jitter_enabled:
            jitter_bps = self._config.jitter_fill_bps
            jitter_frac = self._rng.uniform(-jitter_bps, jitter_bps) / 10_000
            fill_price *= (1 + Decimal(str(jitter_frac)))

        # Ensure fill price is within candle range
        fill_price = max(bar.low, min(bar.high, fill_price))

        # Fee: market orders are always taker for stops/market
        fee_rate = Decimal(str(self._config.taker_fee_bps / 10_000))
        fill_size = self._get_effective_fill_size(order)
        if fill_size <= 0:
            order.status = OrderStatus.CANCELLED
            return []
        fee = fill_size * fill_price * fee_rate

        fill = SimFill(
            order_id=order.id,
            symbol=order.symbol,
            side=order.side,
            price=fill_price.quantize(Decimal("0.0001")),
            size=fill_size,
            fee=fee.quantize(Decimal("0.01")),
            is_maker=False,
            timestamp=now,
            reduce_only=order.reduce_only,
        )

        order.filled_size += fill_size
        if order.reduce_only and order.filled_size < order.size:
            # Reduce-only orders are exchange-capped at current exposure.
            order.size = order.filled_size
        order.avg_fill_price = fill_price
        order.status = OrderStatus.FILLED
        order.filled_at = now
        order.fills.append((fill.price, fill.size, fill.fee, fill.is_maker))

        self._fill_log.append(fill)
        self._total_fees += fill.fee
        self._metrics["orders_filled"] += 1
        self._metrics["total_fills"] += 1

        # Update position (reduce_only caps at flat, cannot reverse)
        self._apply_fill_to_position(fill, reduce_only=order.reduce_only)

        return [fill]

    def _try_fill_limit(
        self, order: SimOrder, bar: CandleBar, liq: LiquidityParams, now: datetime
    ) -> List[SimFill]:
        """Try to fill a limit order if price crossed."""
        if order.price is None:
            return []

        crossed = False
        if order.side == "buy":
            crossed = bar.low <= order.price
        else:
            crossed = bar.high >= order.price

        if not crossed:
            return []

        # Determine maker vs taker using mid-crossing logic
        is_maker = self._determine_limit_maker_taker(order, bar)

        fee_rate = Decimal(str(
            self._config.maker_fee_bps / 10_000 if is_maker
            else self._config.taker_fee_bps / 10_000
        ))

        fill_price = order.price  # Limit orders fill at limit price
        fill_size = self._get_effective_fill_size(order)
        if fill_size <= 0:
            order.status = OrderStatus.CANCELLED
            return []
        fee = fill_size * fill_price * fee_rate

        fill = SimFill(
            order_id=order.id,
            symbol=order.symbol,
            side=order.side,
            price=fill_price,
            size=fill_size,
            fee=fee.quantize(Decimal("0.01")),
            is_maker=is_maker,
            timestamp=now,
            reduce_only=order.reduce_only,
        )

        order.filled_size += fill_size
        if order.reduce_only and order.filled_size < order.size:
            # Reduce-only orders are exchange-capped at current exposure.
            order.size = order.filled_size
        order.avg_fill_price = fill_price
        order.status = OrderStatus.FILLED
        order.filled_at = now
        order.fills.append((fill.price, fill.size, fill.fee, fill.is_maker))

        self._fill_log.append(fill)
        self._total_fees += fill.fee
        self._metrics["orders_filled"] += 1
        self._metrics["total_fills"] += 1

        self._apply_fill_to_position(fill, reduce_only=order.reduce_only)
        return [fill]

    def _get_effective_fill_size(self, order: SimOrder) -> Decimal:
        """Return the exchange-executable fill size for an order at this instant."""
        requested = order.size - order.filled_size
        if requested <= 0:
            return Decimal("0")
        if not order.reduce_only:
            return requested

        pos = self._positions.get(order.symbol)
        if pos is None:
            return Decimal("0")

        is_same_direction = (
            (pos.side == "long" and order.side == "buy") or
            (pos.side == "short" and order.side == "sell")
        )
        if is_same_direction:
            return Decimal("0")

        return min(requested, pos.size)

    def _apply_fill_to_position(self, fill: SimFill, reduce_only: bool = False) -> None:
        """Update positions based on a fill.

        Faithfully models Kraken position semantics:
        - reduceOnly fills cap at flat (cannot reverse or open new position)
        - non-reduce fills that exceed position size generate two logical
          fills: close existing + open reversed (position flip)
        """
        pos = self._positions.get(fill.symbol)

        if pos is None:
            if reduce_only:
                # reduceOnly with no position: no-op (order would have been rejected,
                # but if we get here, cap at zero)
                return
            # New position
            side = "long" if fill.side == "buy" else "short"
            self._positions[fill.symbol] = SimPosition(
                symbol=fill.symbol,
                side=side,
                size=fill.size,
                entry_price=fill.price,
                leverage=self._config.default_leverage,
            )
        else:
            # Same direction: increase position
            is_same_direction = (
                (pos.side == "long" and fill.side == "buy") or
                (pos.side == "short" and fill.side == "sell")
            )
            if is_same_direction:
                if reduce_only:
                    # reduceOnly cannot increase exposure — no-op
                    return
                # Average entry
                total_notional = pos.entry_price * pos.size + fill.price * fill.size
                pos.size += fill.size
                if pos.size > 0:
                    pos.entry_price = total_notional / pos.size
            else:
                # Opposite direction: reduce / close / reverse
                effective_size = fill.size
                if reduce_only:
                    # Cap at flat — cannot reverse
                    effective_size = min(fill.size, pos.size)

                if effective_size >= pos.size:
                    # Close existing position (logical fill 1)
                    pnl = self._calculate_close_pnl(pos, fill.price, pos.size)
                    self._realized_pnl += pnl
                    self._metrics["trades_closed"] += 1
                    self._closed_trades.append({
                        "symbol": fill.symbol,
                        "side": pos.side,
                        "entry_price": float(pos.entry_price),
                        "exit_price": float(fill.price),
                        "size": float(pos.size),
                        "pnl": float(pnl),
                    })
                    remaining = effective_size - pos.size
                    del self._positions[fill.symbol]
                    if remaining > 0 and not reduce_only:
                        # Open reversed position (logical fill 2)
                        new_side = "long" if fill.side == "buy" else "short"
                        self._positions[fill.symbol] = SimPosition(
                            symbol=fill.symbol,
                            side=new_side,
                            size=remaining,
                            entry_price=fill.price,
                            leverage=self._config.default_leverage,
                        )
                else:
                    # Partial close
                    pnl = self._calculate_close_pnl(pos, fill.price, effective_size)
                    self._realized_pnl += pnl
                    self._closed_trades.append({
                        "symbol": fill.symbol,
                        "side": pos.side,
                        "entry_price": float(pos.entry_price),
                        "exit_price": float(fill.price),
                        "size": float(effective_size),
                        "pnl": float(pnl),
                    })
                    pos.size -= effective_size

        # Update margin
        self._recalculate_margin()

    def _calculate_close_pnl(self, pos: SimPosition, exit_price: Decimal, size: Decimal) -> Decimal:
        if pos.side == "long":
            return (exit_price - pos.entry_price) * size
        else:
            return (pos.entry_price - exit_price) * size

    def _recalculate_margin(self) -> None:
        total_margin = Decimal("0")
        for pos in self._positions.values():
            notional = pos.size * pos.entry_price
            margin = notional / pos.leverage
            total_margin += margin
        self._margin_used = total_margin
        self._equity = self._config.initial_equity_usd + self._realized_pnl - self._total_fees - self._total_funding
        # Add unrealized PnL to equity
        for pos in self._positions.values():
            self._equity += pos.unrealized_pnl
        self._available_margin = self._equity - self._margin_used

    def _apply_funding(self, now: datetime) -> None:
        """Apply funding every 8 hours with per-symbol rate curves.

        Each symbol can have its own FundingCurve with:
        - base_rate_8h_bps: normal rate
        - vol_spike_multiplier: applied when liquidity regime is "high"/"extreme"

        Falls back to config.funding_rate_8h_bps if no per-symbol curve.
        """
        if self._last_funding_time is None:
            self._last_funding_time = now
            return
        hours_elapsed = (now - self._last_funding_time).total_seconds() / 3600
        if hours_elapsed >= 8:
            curves = self._config.funding_curves or {}
            for pos in self._positions.values():
                curve = curves.get(pos.symbol)
                if curve:
                    rate_bps = curve.base_rate_8h_bps
                    # Check current vol regime for spike
                    liq = self._data.get_liquidity_at(pos.symbol, now)
                    if liq.volatility_regime in ("high", "extreme"):
                        rate_bps *= curve.vol_spike_multiplier
                else:
                    rate_bps = self._config.funding_rate_8h_bps

                funding_rate = Decimal(str(rate_bps / 10_000))
                notional = pos.size * pos.entry_price
                funding = notional * funding_rate
                self._total_funding += funding
                self._funding_log.append({
                    "timestamp": now.isoformat(),
                    "symbol": pos.symbol,
                    "rate_bps": rate_bps,
                    "notional": float(notional),
                    "funding_usd": float(funding),
                })
            self._metrics["funding_events"] += 1
            self._last_funding_time = now

    def _update_unrealized_pnl(self, now: datetime) -> None:
        for pos in self._positions.values():
            bar = self._market_bar_at(pos.symbol, now)
            if bar:
                mark = bar.close
                if pos.side == "long":
                    pos.unrealized_pnl = (mark - pos.entry_price) * pos.size
                else:
                    pos.unrealized_pnl = (pos.entry_price - mark) * pos.size

    # ===================================================================
    # KrakenClient interface implementation
    # ===================================================================

    # -- Market data --

    async def get_spot_markets(self) -> Dict[str, dict]:
        self._check_fault("get_spot_markets")
        return {s: {"symbol": s, "active": True} for s in self._data.get_all_symbols()}

    async def get_futures_markets(self) -> Dict[str, dict]:
        self._check_fault("get_futures_markets")
        return {s: {"symbol": s, "active": True} for s in self._data.get_all_symbols()}

    async def get_spot_ticker(self, symbol: str) -> Dict:
        self._check_fault("get_spot_ticker")
        return await self._make_ticker_dict(symbol)

    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        self._check_fault("get_ticker")
        return await self._make_ticker_dict(symbol)

    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        return await self._make_ticker_dict(symbol)

    async def get_spot_tickers_bulk(self, symbols: List[str]) -> Dict[str, Dict]:
        self._check_fault("get_spot_tickers_bulk")
        await self._maybe_inject_latency()
        result = {}
        for s in symbols:
            result[s] = await self._make_ticker_dict(s)
        return result

    async def get_spot_ohlcv(
        self, symbol: str, timeframe: str, since: Optional[int] = None, limit: Optional[int] = None,
    ) -> List[Candle]:
        self._check_fault("get_spot_ohlcv")
        return self._get_candles(symbol, timeframe, since, limit)

    async def get_futures_ohlcv(
        self, futures_symbol: str, timeframe: str, since: Optional[int] = None, limit: Optional[int] = None,
    ) -> List[Candle]:
        self._check_fault("get_futures_ohlcv")
        return self._get_candles(futures_symbol, timeframe, since, limit)

    async def get_futures_mark_price(self, symbol: str) -> Decimal:
        self._check_fault("get_futures_mark_price")
        bar = self._market_bar_at(symbol, self._clock.now())
        return bar.close if bar else Decimal("0")

    async def get_futures_tickers_bulk(self) -> Dict[str, Decimal]:
        self._check_fault("get_futures_tickers_bulk")
        await self._maybe_inject_latency()
        result = {}
        for s in self._data.get_all_symbols():
            bar = self._market_bar_at(s, self._clock.now())
            if bar:
                result[s] = bar.close
        return result

    async def get_futures_tickers_bulk_full(self) -> Dict[str, FuturesTicker]:
        self._check_fault("get_futures_tickers_bulk_full")
        await self._maybe_inject_latency()
        result = {}
        for s in self._data.get_all_symbols():
            bar = self._market_bar_at(s, self._clock.now())
            liq = self._data.get_liquidity_at(s, self._clock.now())
            if bar:
                mid = bar.close
                spread_half = mid * liq.spread_fraction / 2
                result[s] = FuturesTicker(
                    symbol=s,
                    mark_price=mid,
                    bid=mid - spread_half,
                    ask=mid + spread_half,
                    volume_24h=bar.volume * 1440,  # extrapolate
                    open_interest=Decimal("1000000"),
                    funding_rate=Decimal(str(self._config.funding_rate_8h_bps / 10_000)),
                )
        return result

    async def get_futures_instruments(self) -> List[Dict]:
        self._check_fault("get_futures_instruments")
        return [
            {"symbol": s, "contractSize": 1, "tickSize": 0.0001, "type": "perpetual"}
            for s in self._data.get_all_symbols()
        ]

    # -- Account --

    async def get_spot_balance(self) -> Dict[str, Any]:
        self._check_fault("get_spot_balance")
        return {"USD": {"free": float(self._available_margin), "total": float(self._equity)}}

    async def get_account_balance(self) -> Dict[str, Decimal]:
        self._check_fault("get_account_balance")
        return {"USD": self._equity}

    async def get_futures_balance(self) -> Dict[str, Any]:
        self._check_fault("get_futures_balance")
        return {
            "USD": {
                "free": float(self._available_margin),
                "used": float(self._margin_used),
                "total": float(self._equity),
            }
        }

    async def get_futures_account_info(self) -> Dict[str, Any]:
        self._check_fault("get_futures_account_info")
        await self._maybe_inject_latency()
        return {
            "equity": float(self._equity),
            "availableMargin": float(self._available_margin),
            "marginUsed": float(self._margin_used),
            "unrealizedPnl": float(sum(p.unrealized_pnl for p in self._positions.values())),
            "leverage": float(self._config.default_leverage),
        }

    # -- Positions --

    async def get_futures_position(self, symbol: str) -> Optional[Dict]:
        self._check_fault("get_futures_position")
        symbol = self._resolve_market_symbol(symbol)
        pos = self._positions.get(symbol)
        if not pos:
            return None
        return self._position_to_dict(pos)

    async def get_all_futures_positions(self) -> List[Dict]:
        self._check_fault("get_all_futures_positions")
        await self._maybe_inject_latency()
        return [self._position_to_dict(p) for p in self._positions.values()]

    def _position_to_dict(self, pos: SimPosition) -> Dict:
        return {
            "symbol": pos.symbol,
            "side": pos.side,
            "size": float(pos.size),
            "contracts": float(pos.size),
            "contractSize": 1,
            "entryPrice": float(pos.entry_price),
            "unrealizedPnl": float(pos.unrealized_pnl),
            "leverage": float(pos.leverage),
            "percentage": float(pos.unrealized_pnl / pos.entry_price * 100) if pos.entry_price else 0,
            "info": {"side": pos.side, "size": str(pos.size)},
        }

    # -- Order placement --

    async def place_futures_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        size: Decimal,
        price: Optional[Decimal] = None,
        stop_price: Optional[Decimal] = None,
        reduce_only: bool = False,
        leverage: Optional[Decimal] = None,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._check_fault("place_futures_order")
        await self._api_breaker.can_execute()
        await self._maybe_inject_latency()

        if self._dry_run:
            raise OperationalError("dry_run_active: order placement refused at transport boundary")

        symbol = self._resolve_market_symbol(symbol)

        # -- Order rejection checks (Fix 3: realism) --
        bar = self._market_bar_at(symbol, self._clock.now())
        liq = self._data.get_liquidity_at(symbol, self._clock.now())
        mid = bar.close if bar else Decimal("0")

        # Min size check
        notional = size * mid if mid > 0 else Decimal("0")
        if float(notional) < self._config.min_order_size_usd and mid > 0:
            self._metrics["orders_rejected"] += 1
            self._metrics["min_size_rejections"] += 1
            raise DataError(
                f"Order rejected: notional ${float(notional):.2f} below min ${self._config.min_order_size_usd}"
            )

        # reduceOnly conflict: cannot increase exposure or open new position
        if reduce_only and self._config.reject_reduce_only_conflicts:
            pos = self._positions.get(symbol)
            if pos is None:
                self._metrics["orders_rejected"] += 1
                self._metrics["reduce_only_rejections"] += 1
                raise DataError(
                    f"Order rejected: reduceOnly but no open position for {symbol}"
                )
            # Would this order increase exposure?
            is_same_direction = (
                (pos.side == "long" and side == "buy") or
                (pos.side == "short" and side == "sell")
            )
            if is_same_direction:
                self._metrics["orders_rejected"] += 1
                self._metrics["reduce_only_rejections"] += 1
                raise DataError(
                    f"Order rejected: reduceOnly {side} would increase {pos.side} position"
                )

        # Insufficient margin check
        if self._config.reject_insufficient_margin and not reduce_only:
            required_margin = notional / (leverage or self._config.default_leverage)
            if required_margin > self._available_margin:
                self._metrics["orders_rejected"] += 1
                self._metrics["insufficient_margin_rejections"] += 1
                raise DataError(
                    f"Order rejected: insufficient margin (need ${float(required_margin):.2f}, "
                    f"have ${float(self._available_margin):.2f})"
                )

        oid = f"sim-{uuid.uuid4().hex[:12]}"
        otype = OrderType(order_type) if order_type in [e.value for e in OrderType] else OrderType.MARKET

        order = SimOrder(
            id=oid,
            client_order_id=client_order_id,
            symbol=symbol,
            side=side,
            order_type=otype,
            size=size,
            price=price,
            stop_price=stop_price,
            reduce_only=reduce_only,
            leverage=leverage,
            created_at=self._clock.now(),
            mid_at_placement=mid if mid > 0 else None,  # Fix 1: for maker/taker determination
        )
        self._orders[oid] = order
        self._metrics["orders_placed"] += 1

        # Market orders fill immediately on next step()
        # For instant fills during order placement, step now
        if otype == OrderType.MARKET:
            if bar:
                self._fill_market_order(order, bar, liq, self._clock.now())

        await self._api_breaker.record_success()

        return {
            "id": oid,
            "clientOrderId": client_order_id,
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "amount": float(size),
            "price": float(price) if price else None,
            "stopPrice": float(stop_price) if stop_price else None,
            "status": order.status.value,
            "filled": float(order.filled_size),
            "remaining": float(order.size - order.filled_size),
            "average": float(order.avg_fill_price) if order.avg_fill_price else None,
            "reduceOnly": reduce_only,
            "info": {
                "order_id": oid,
                "status": order.status.value,
            },
        }

    async def create_order(
        self,
        symbol: str,
        type: str,
        side: str,
        amount: float,
        price: Optional[float] = None,
        params: Optional[Dict[str, Any]] = None,
        leverage: Optional[Decimal] = None,
    ) -> Dict[str, Any]:
        p = params or {}
        client_order_id = p.get("clientOrderId") or p.get("cliOrdId")
        reduce_only = bool(p.get("reduceOnly", False))
        stop_price = p.get("stopPrice")
        if stop_price is not None:
            stop_price = Decimal(str(stop_price))
        elif type in ("stop", "stop_loss") and price is not None:
            stop_price = Decimal(str(price))

        order_type = "stop" if type in ("stop", "stop_loss") else type
        size = Decimal(str(amount))
        price_dec = Decimal(str(price)) if price is not None else None

        return await self.place_futures_order(
            symbol=symbol, side=side, order_type=order_type, size=size,
            price=price_dec, stop_price=stop_price, reduce_only=reduce_only,
            leverage=leverage, client_order_id=client_order_id,
        )

    # -- Order queries --

    async def get_futures_open_orders(self) -> List[Dict[str, Any]]:
        """Return open orders.

        When hide_entered_book_from_open_orders is True (simulating Kraken's
        Layer 1 visibility quirk), entered_book orders are excluded.
        fetch_order(id) still shows them — exactly like real Kraken.
        This validates the Layer 1 miss / Layer 2 rescue multi-layer logic.
        """
        self._check_fault("get_futures_open_orders")
        await self._maybe_inject_latency()
        result = []
        hide_entered = self._config.hide_entered_book_from_open_orders
        for order in self._orders.values():
            if order.status == OrderStatus.ENTERED_BOOK and hide_entered:
                continue  # Layer 1 misses transitional state
            if order.status in (OrderStatus.OPEN, OrderStatus.ENTERED_BOOK, OrderStatus.PARTIALLY_FILLED):
                result.append(self._order_to_dict(order))
        return result

    async def fetch_order(self, order_id: str, symbol: str) -> Optional[Dict[str, Any]]:
        self._check_fault("fetch_order")
        order = self._orders.get(order_id)
        if not order:
            return None
        return self._order_to_dict(order)

    # -- Order cancellation --

    async def cancel_futures_order(self, order_id: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        self._check_fault("cancel_futures_order")
        order = self._orders.get(order_id)
        if not order:
            raise DataError(f"Order {order_id} not found")
        if order.status in (OrderStatus.FILLED, OrderStatus.CANCELLED):
            raise DataError(f"Order {order_id} already {order.status.value}")
        order.status = OrderStatus.CANCELLED
        self._metrics["orders_cancelled"] += 1
        return {"result": "success", "order_id": order_id}

    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        return await self.cancel_futures_order(order_id, symbol)

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        self._check_fault("cancel_all_orders")
        cancelled = []
        for order in self._orders.values():
            if order.status in (OrderStatus.OPEN, OrderStatus.ENTERED_BOOK, OrderStatus.PARTIALLY_FILLED):
                if symbol is None or order.symbol == symbol:
                    order.status = OrderStatus.CANCELLED
                    cancelled.append({"result": "success", "order_id": order.id})
                    self._metrics["orders_cancelled"] += 1
        return cancelled

    # -- Order editing --

    async def edit_futures_order(
        self, *, order_id: str, symbol: str,
        stop_price: Optional[Decimal] = None, price: Optional[Decimal] = None,
    ) -> Dict[str, Any]:
        self._check_fault("edit_futures_order")
        order = self._orders.get(order_id)
        if not order:
            raise DataError(f"Order {order_id} not found")
        if stop_price is not None:
            order.stop_price = stop_price
        if price is not None:
            order.price = price
        return self._order_to_dict(order)

    # -- Position closing --

    async def close_position(self, symbol: str) -> Dict[str, Any]:
        self._check_fault("close_position")
        symbol = self._resolve_market_symbol(symbol)
        pos = self._positions.get(symbol)
        if not pos:
            raise DataError(f"No position for {symbol}")
        # Place a market close order
        close_side = "sell" if pos.side == "long" else "buy"
        return await self.place_futures_order(
            symbol=symbol, side=close_side, order_type="market",
            size=pos.size, reduce_only=True,
        )

    # -- Helpers --

    async def _make_ticker_dict(self, symbol: str) -> Dict:
        symbol = self._resolve_market_symbol(symbol)
        bar = self._market_bar_at(symbol, self._clock.now())
        liq = self._data.get_liquidity_at(symbol, self._clock.now())
        if not bar:
            return {"symbol": symbol, "last": 0, "bid": 0, "ask": 0, "volume": 0}
        mid = bar.close
        spread_half = mid * liq.spread_fraction / 2
        return {
            "symbol": symbol,
            "last": float(mid),
            "bid": float(mid - spread_half),
            "ask": float(mid + spread_half),
            "high": float(bar.high),
            "low": float(bar.low),
            "open": float(bar.open),
            "close": float(bar.close),
            "volume": float(bar.volume),
            "percentage": 0.0,
            "info": {},
        }

    def _get_candles(
        self, symbol: str, timeframe: str, since: Optional[int], limit: Optional[int],
    ) -> List[Candle]:
        """Convert CandleBars to domain Candle objects."""
        symbol = self._resolve_market_symbol(symbol)
        now = self._clock.now()
        bars = self._data.get_candles_up_to(symbol, timeframe, now, limit=limit or 500)
        if since:
            cutoff = datetime.fromtimestamp(since / 1000, tz=timezone.utc)
            bars = [b for b in bars if b.timestamp >= cutoff]
        return [
            Candle(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=b.timestamp,
                open=b.open,
                high=b.high,
                low=b.low,
                close=b.close,
                volume=b.volume,
            )
            for b in bars
        ]

    def _order_to_dict(self, order: SimOrder) -> Dict[str, Any]:
        trades = []
        for index, (price, size, fee, is_maker) in enumerate(order.fills, start=1):
            trades.append(
                {
                    "id": f"{order.id}-fill-{index}",
                    "order": order.id,
                    "price": float(price),
                    "amount": float(size),
                    "fee": float(fee),
                    "maker": bool(is_maker),
                    "side": order.side,
                    "symbol": order.symbol,
                }
            )
        return {
            "id": order.id,
            "clientOrderId": order.client_order_id,
            "symbol": order.symbol,
            "side": order.side,
            "type": order.order_type.value,
            "amount": float(order.size),
            "price": float(order.price) if order.price else None,
            "stopPrice": float(order.stop_price) if order.stop_price else None,
            "status": order.status.value,
            "filled": float(order.filled_size),
            "remaining": float(order.size - order.filled_size),
            "average": float(order.avg_fill_price) if order.avg_fill_price else None,
            "reduceOnly": order.reduce_only,
            "datetime": order.created_at.isoformat() if order.created_at else None,
            "timestamp": int(order.created_at.timestamp() * 1000) if order.created_at else None,
            "trades": trades,
            "info": {
                "order_id": order.id,
                "status": order.status.value,
                "reduceOnly": order.reduce_only,
            },
        }

    # -- Metrics --

    @property
    def closed_trades(self) -> List[Dict[str, Any]]:
        """Return all closed trade records accumulated during the replay."""
        return self._closed_trades

    @property
    def exchange_metrics(self) -> Dict[str, Any]:
        return {
            **self._metrics,
            "equity": float(self._equity),
            "available_margin": float(self._available_margin),
            "margin_used": float(self._margin_used),
            "realized_pnl": float(self._realized_pnl),
            "total_fees": float(self._total_fees),
            "total_funding": float(self._total_funding),
            "open_positions": len(self._positions),
            "total_orders": len(self._orders),
            "total_fills": len(self._fill_log),
            "jitter_seed": self._config.jitter_seed,
            "closed_trades": list(self._closed_trades),
        }
