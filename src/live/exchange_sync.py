"""
Exchange synchronization operations extracted from LiveTrading.

Functions in this module handle:
- Position syncing between exchange and internal state
- Account state fetching and persistence
- Position data conversion (exchange dict -> domain object)
- Reconciler construction
- Trade history persistence with P&L calculation

All functions receive the LiveTrading instance as their first argument (``lt``)
to access shared state, following the same delegate pattern used by
protection_ops, signal_handler, and auction_runner.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional

from src.domain.models import Position, Side, Trade
from src.execution.equity import calculate_effective_equity
from src.monitoring.logger import get_logger
from src.reconciliation.reconciler import Reconciler
from src.storage.repository import (
    save_account_state,
    save_trade,
    sync_active_positions,
)

from src.exceptions import OperationalError, DataError

if TYPE_CHECKING:
    from src.live.live_trading import LiveTrading

logger = get_logger(__name__)


def _parse_opened_at(data: Dict) -> datetime:
    """
    Best-effort parse of exchange-provided open timestamp.

    Falls back to current UTC time when timestamp is unavailable/invalid.
    """
    raw = (
        data.get("openedAt")
        or data.get("opened_at")
        or data.get("openTime")
        or data.get("open_time")
        or data.get("timestamp")
    )
    if raw is None:
        return datetime.now(timezone.utc)

    # Numeric epoch (seconds or milliseconds)
    if isinstance(raw, (int, float)):
        ts = float(raw)
        if ts > 10_000_000_000:  # likely ms
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return datetime.now(timezone.utc)

    # Numeric string epoch
    if isinstance(raw, str):
        s = raw.strip()
        if s.isdigit():
            ts = float(s)
            if ts > 10_000_000_000:
                ts /= 1000.0
            try:
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except (OSError, OverflowError, ValueError):
                return datetime.now(timezone.utc)
        # ISO string
        try:
            parsed = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            return datetime.now(timezone.utc)

    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Position conversion
# ---------------------------------------------------------------------------

def convert_to_position(lt: "LiveTrading", data: Dict) -> Position:
    """Convert raw exchange position dict to Position domain object."""
    symbol = data.get("symbol")

    # Parse Side
    side_raw = data.get("side", "long").lower()
    side = Side.LONG if side_raw in ["long", "buy"] else Side.SHORT

    # Parse Numerics
    size = Decimal(str(data.get("size", 0)))
    entry_price = Decimal(str(data.get("entryPrice", data.get("entry_price", 0))))
    mark_price = Decimal(str(data.get("markPrice", data.get("mark_price", 0))))
    liq_price = Decimal(
        str(data.get("liquidationPrice", data.get("liquidation_price", 0)))
    )
    unrealized_pnl = Decimal(
        str(data.get("unrealizedPnl", data.get("unrealized_pnl", 0)))
    )
    leverage = Decimal(str(data.get("leverage", 1)))
    margin_used = Decimal(
        str(data.get("initialMargin", data.get("margin_used", 0)))
    )

    if mark_price == 0:
        mark_price = entry_price

    size_notional = size * mark_price

    return Position(
        symbol=symbol,
        side=side,
        size=size,
        size_notional=size_notional,
        entry_price=entry_price,
        current_mark_price=mark_price,
        liquidation_price=liq_price,
        unrealized_pnl=unrealized_pnl,
        leverage=leverage,
        margin_used=margin_used,
        opened_at=_parse_opened_at(data),
    )


# ---------------------------------------------------------------------------
# Position sync
# ---------------------------------------------------------------------------

async def sync_positions(
    lt: "LiveTrading", raw_positions: Optional[List[Dict]] = None
) -> List[Dict]:
    """
    Sync active positions from exchange and update RiskManager.

    Args:
        lt: LiveTrading instance.
        raw_positions: Optional pre-fetched positions list (to avoid duplicate API calls).

    Returns:
        List of active positions (dicts).
    """
    if raw_positions is None:
        try:
            raw_positions = await asyncio.wait_for(
                lt.client.get_all_futures_positions(), timeout=30.0
            )
        except asyncio.TimeoutError:
            logger.error("Timeout fetching futures positions during sync")
            raw_positions = []
        except (OperationalError, DataError) as e:
            logger.error("Failed to fetch futures positions", error=str(e), error_type=type(e).__name__)
            raw_positions = []

    # Convert to domain objects
    active_positions = []
    for p in raw_positions:
        try:
            pos_obj = convert_to_position(lt, p)
            active_positions.append(pos_obj)
        except (ValueError, TypeError, KeyError) as e:
            logger.error(
                "Failed to convert position object", data=str(p), error=str(e), error_type=type(e).__name__
            )

    # Update Risk Manager
    lt.risk_manager.update_position_list(active_positions)

    # Persist to DB for Dashboard
    try:
        await asyncio.to_thread(
            sync_active_positions, lt.risk_manager.current_positions
        )
    except (OperationalError, DataError, OSError) as e:
        logger.error("Failed to sync positions to DB", error=str(e), error_type=type(e).__name__)

    logger.info(
        f"Active Portfolio: {len(active_positions)} positions",
        symbols=[p.symbol for p in active_positions],
    )

    return raw_positions


# ---------------------------------------------------------------------------
# Reconciler construction
# ---------------------------------------------------------------------------

def build_reconciler(lt: "LiveTrading") -> Reconciler:
    """Build Reconciler with config, place_futures_order, and optional place_protection callback."""
    place_futures = lambda symbol, side, order_type, size, reduce_only: lt.client.place_futures_order(
        symbol=symbol,
        side=side,
        order_type=order_type,
        size=size,
        reduce_only=reduce_only,
    )
    place_protection = None  # Adopted positions get protection on next tick via _reconcile_protective_orders
    return Reconciler(
        lt.client,
        lt.config,
        place_futures_order_fn=place_futures,
        place_protection_callback=place_protection,
    )


# ---------------------------------------------------------------------------
# Account state sync
# ---------------------------------------------------------------------------

async def sync_account_state(lt: "LiveTrading") -> None:
    """Fetch and persist real-time account state."""
    try:
        balance = await lt.client.get_futures_balance()
        if not balance:
            return

        base = getattr(lt.config.exchange, "base_currency", "USD")
        equity, avail_margin, margin_used_val = await calculate_effective_equity(
            balance, base_currency=base, kraken_client=lt.client
        )

        save_account_state(
            equity=equity,
            balance=equity,  # For futures margin, equity IS the balance relevant for trading
            margin_used=margin_used_val,
            available_margin=avail_margin,
            unrealized_pnl=Decimal("0.0"),  # Included in portfolioValue usually
        )

        # Initialize daily loss tracking if not set
        if lt.risk_manager.daily_start_equity <= 0:
            lt.risk_manager.reset_daily_metrics(equity)
            logger.info(
                "Daily loss tracking initialized", starting_equity=str(equity)
            )

    except (OperationalError, DataError) as e:
        logger.error("Failed to sync account state", error=str(e), error_type=type(e).__name__)


# ---------------------------------------------------------------------------
# Trade history
# ---------------------------------------------------------------------------

async def save_trade_history(
    lt: "LiveTrading",
    position: Position,
    exit_price: Decimal,
    exit_reason: str,
) -> None:
    """
    Save closed position to trade history.

    Calculates P&L (gross, fees, funding, net), persists to DB, updates daily
    P&L tracking in risk manager, and sends Telegram alerts.
    """
    try:
        now = datetime.now(timezone.utc)
        holding_hours = (now - position.opened_at).total_seconds() / 3600

        # Calculate PnL
        if position.side == Side.LONG:
            gross_pnl = (exit_price - position.entry_price) * position.size
        else:
            gross_pnl = (position.entry_price - exit_price) * position.size

        # Estimate fees (simplified -- should use actual fees if available)
        # Maker: 0.02%, Taker: 0.05% (Kraken Futures)
        entry_fee = position.size_notional * Decimal("0.0002")  # Assume maker
        exit_fee = position.size_notional * Decimal("0.0002")
        fees = entry_fee + exit_fee

        # Estimate funding (simplified -- should use actual funding if available)
        # Average funding rate ~0.01% per 8 hours
        funding_periods = holding_hours / 8
        funding = position.size_notional * Decimal("0.0001") * Decimal(
            str(funding_periods)
        )

        net_pnl = gross_pnl - fees - funding

        if position.entry_price <= 0 or position.size_notional <= 0:
            logger.critical(
                "TRADE_SAVE_BLOCKED_INVALID_INPUT",
                symbol=position.symbol,
                entry_price=str(position.entry_price),
                size_notional=str(position.size_notional),
                exit_reason=exit_reason,
            )
            return

        if not exit_reason or exit_reason == "unknown":
            resolved_reason = "reconciliation"
            logger.warning(
                "Missing exit reason in legacy trade saver; applying fallback",
                symbol=position.symbol,
                resolved_exit_reason=resolved_reason,
            )
            exit_reason = resolved_reason

        trade = Trade(
            trade_id=str(uuid.uuid4()),
            symbol=position.symbol,
            side=position.side,
            entry_price=position.entry_price,
            exit_price=exit_price,
            size_notional=position.size_notional,
            leverage=position.leverage,
            gross_pnl=gross_pnl,
            fees=fees,
            funding=funding,
            net_pnl=net_pnl,
            entered_at=position.opened_at,
            exited_at=now,
            holding_period_hours=Decimal(str(holding_hours)),
            exit_reason=exit_reason,
        )

        await asyncio.to_thread(save_trade, trade)

        # Update daily P&L tracking in risk manager
        try:
            setup_type = getattr(position, "setup_type", None)
            balance = await lt.client.get_futures_balance()
            base = getattr(lt.config.exchange, "base_currency", "USD")
            equity_now, _, _ = await calculate_effective_equity(
                balance, base_currency=base, kraken_client=lt.client
            )
            lt.risk_manager.record_trade_result(net_pnl, equity_now, setup_type)

            # Alert if daily loss limit approached or exceeded
            daily_loss_pct = (
                abs(lt.risk_manager.daily_pnl) / lt.risk_manager.daily_start_equity
                if lt.risk_manager.daily_start_equity > 0
                and lt.risk_manager.daily_pnl < 0
                else Decimal("0")
            )
            if daily_loss_pct > Decimal(
                str(lt.config.risk.daily_loss_limit_pct * 0.7)
            ):
                from src.monitoring.alert_dispatcher import send_alert

                limit_pct = lt.config.risk.daily_loss_limit_pct * 100
                await send_alert(
                    "DAILY_LOSS_WARNING",
                    f"Daily loss at {daily_loss_pct:.1%} of equity\n"
                    f"Limit: {limit_pct:.0f}%\n"
                    f"Daily P&L: ${lt.risk_manager.daily_pnl:.2f}",
                    urgent=daily_loss_pct
                    > Decimal(str(lt.config.risk.daily_loss_limit_pct)),
                )
        except (OperationalError, DataError, ImportError) as e:
            logger.warning("Failed to update daily P&L tracking", error=str(e), error_type=type(e).__name__)

        logger.info(
            "Trade saved to history",
            symbol=position.symbol,
            side=position.side.value,
            entry_price=str(position.entry_price),
            exit_price=str(exit_price),
            net_pnl=str(net_pnl),
            exit_reason=exit_reason,
            holding_hours=f"{holding_hours:.2f}",
        )

        # Send close alert via Telegram
        try:
            from src.monitoring.alert_dispatcher import send_alert, fmt_price

            pnl_sign = "+" if net_pnl >= 0 else ""
            pnl_emoji = "\u2705" if net_pnl >= 0 else "\u274c"
            await send_alert(
                "POSITION_CLOSED",
                f"{pnl_emoji} Position closed: {position.symbol}\n"
                f"Side: {position.side.value.upper()}\n"
                f"Entry: ${fmt_price(position.entry_price)} \u2192 Exit: ${fmt_price(exit_price)}\n"
                f"P&L: {pnl_sign}${net_pnl:.2f}\n"
                f"Reason: {exit_reason}\n"
                f"Duration: {holding_hours:.1f}h",
            )
        except (OperationalError, ImportError, OSError) as e:
            logger.debug("Alert failure (non-blocking)", error=str(e))

    except (OperationalError, DataError, OSError) as e:
        logger.error(
            "Failed to save trade history",
            symbol=position.symbol,
            error=str(e),
            error_type=type(e).__name__,
        )
