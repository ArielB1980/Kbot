"""
Spot DCA (Dollar Cost Averaging) – daily scheduled spot purchases.

Runs as an asyncio background task within the live trading loop.
At the configured time each day (default: midnight UTC), fetches the
available USD balance on the spot account and places a market buy
for the configured asset (default: SOL).

Safety:
- Skips if balance below min_purchase_usd
- Caps at max_purchase_usd if configured
- Reserves reserve_usd in the account
- Respects DRY_RUN mode (no real order)
- Logs every decision and outcome
- Never crashes the main trading loop
"""
import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN

from src.monitoring.logger import get_logger
from src.exceptions import CircuitOpenError, OperationalError, RateLimitError

logger = get_logger(__name__)


async def run_spot_dca(lt: "LiveTrading") -> None:
    """
    Background task: daily spot DCA purchase.

    Sleeps until the next scheduled time, executes the purchase,
    then sleeps until the next day. Repeats while lt.active.
    """
    dca_cfg = getattr(lt.config, "spot_dca", None)
    if not dca_cfg or not dca_cfg.enabled:
        logger.info("Spot DCA disabled in config, background task exiting")
        return

    asset = dca_cfg.asset
    quote = dca_cfg.quote_currency
    symbol = f"{asset}/{quote}"

    logger.info(
        "Spot DCA task started",
        asset=asset,
        symbol=symbol,
        schedule=f"{dca_cfg.schedule_hour_utc:02d}:{dca_cfg.schedule_minute_utc:02d} UTC",
        use_full_balance=dca_cfg.use_full_balance,
        fixed_amount=dca_cfg.fixed_amount_usd,
        min_purchase=dca_cfg.min_purchase_usd,
        max_purchase=dca_cfg.max_purchase_usd,
        reserve=dca_cfg.reserve_usd,
    )

    while lt.active:
        try:
            seconds_to_next = _seconds_until_next_run(
                dca_cfg.schedule_hour_utc,
                dca_cfg.schedule_minute_utc,
            )
            logger.info(
                "Spot DCA sleeping until next run",
                next_run_in_hours=f"{seconds_to_next / 3600:.1f}",
            )
            await asyncio.sleep(seconds_to_next)

            if not lt.active:
                break

            completed, retryable = await _execute_dca_purchase(lt, dca_cfg, symbol)

            # If the midnight run failed for a transient API reason
            # (e.g., circuit breaker open), retry for a short window
            # instead of skipping the whole day.
            if lt.active and not completed and retryable:
                retry_deadline = datetime.now(timezone.utc) + timedelta(minutes=15)
                retry_attempt = 0
                while lt.active and datetime.now(timezone.utc) < retry_deadline:
                    retry_attempt += 1
                    wait_seconds = 65  # breaker cooldown is 60s
                    logger.warning(
                        "Spot DCA transient failure, retrying",
                        attempt=retry_attempt,
                        wait_seconds=wait_seconds,
                    )
                    await asyncio.sleep(wait_seconds)
                    completed, retryable = await _execute_dca_purchase(lt, dca_cfg, symbol)
                    if completed or not retryable:
                        break

                if not completed and retryable:
                    logger.error(
                        "Spot DCA retry window exhausted",
                        window_minutes=15,
                    )

        except asyncio.CancelledError:
            logger.info("Spot DCA task cancelled")
            break
        except Exception as e:
            logger.error(
                "Spot DCA unexpected error (will retry next cycle)",
                error=str(e),
                error_type=type(e).__name__,
            )
            await asyncio.sleep(60)


def _is_retryable_dca_error(exc: Exception) -> bool:
    """Return True for transient failures worth retrying soon."""
    if isinstance(exc, (CircuitOpenError, RateLimitError)):
        return True
    if isinstance(exc, TimeoutError):
        return True
    # Some operational wrappers carry only message text.
    msg = str(exc).lower()
    if "circuit breaker" in msg or "timeout" in msg or "temporar" in msg:
        return True
    # Keep OperationalError last (broader type).
    return isinstance(exc, OperationalError)


async def _execute_dca_purchase(lt, dca_cfg, symbol: str) -> tuple[bool, bool]:
    """Execute one DCA purchase cycle.

    Returns:
        (completed, retryable_error)
        - completed=True means no retry needed (success or intentional skip)
        - completed=False and retryable_error=True means retry is recommended
    """
    try:
        from src.monitoring.alert_dispatcher import send_alert
    except ImportError:
        send_alert = None

    asset = dca_cfg.asset
    quote = dca_cfg.quote_currency

    logger.info("Spot DCA purchase cycle starting", symbol=symbol)

    # 1. Fetch spot balance
    try:
        balance = await lt.client.get_spot_balance()
    except Exception as e:
        logger.error("Spot DCA: failed to fetch spot balance", error=str(e))
        if send_alert:
            await send_alert("SPOT_DCA", f"Balance fetch failed – {e}")
        return False, _is_retryable_dca_error(e)

    # Extract available quote currency (USD)
    free_quote = Decimal(str(balance.get("free", {}).get(quote, 0)))

    logger.info(
        "Spot DCA: balance fetched",
        free_quote=str(free_quote),
        quote=quote,
    )

    # 2. Determine purchase amount
    available = free_quote - Decimal(str(dca_cfg.reserve_usd))
    if available <= 0:
        logger.info(
            "Spot DCA: no available balance after reserve",
            free=str(free_quote),
            reserve=str(dca_cfg.reserve_usd),
        )
        return True, False

    if dca_cfg.fixed_amount_usd is not None:
        spend_usd = min(Decimal(str(dca_cfg.fixed_amount_usd)), available)
    elif dca_cfg.use_full_balance:
        spend_usd = available
    else:
        logger.warning("Spot DCA: no amount strategy configured, skipping")
        return True, False

    if dca_cfg.max_purchase_usd is not None:
        spend_usd = min(spend_usd, Decimal(str(dca_cfg.max_purchase_usd)))

    if spend_usd < Decimal(str(dca_cfg.min_purchase_usd)):
        logger.info(
            "Spot DCA: amount below minimum, skipping",
            spend_usd=str(spend_usd),
            min_required=str(dca_cfg.min_purchase_usd),
        )
        return True, False

    # 3. Get spot price to compute quantity
    try:
        ticker = await lt.client.get_spot_ticker(symbol)
    except Exception as e:
        logger.error("Spot DCA: failed to fetch ticker", error=str(e), symbol=symbol)
        if send_alert:
            await send_alert("SPOT_DCA", f"Ticker fetch failed for {symbol} – {e}")
        return False, _is_retryable_dca_error(e)

    ask_price = Decimal(str(ticker.get("ask", 0)))
    if ask_price <= 0:
        last_price = Decimal(str(ticker.get("last", 0)))
        if last_price <= 0:
            logger.error("Spot DCA: invalid ticker price", ticker=ticker)
            return True, False
        ask_price = last_price

    quantity = (spend_usd / ask_price).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    if quantity <= 0:
        logger.info("Spot DCA: computed quantity too small", spend_usd=str(spend_usd), price=str(ask_price))
        return True, False

    logger.info(
        "Spot DCA: placing market buy",
        symbol=symbol,
        spend_usd=str(spend_usd),
        price=str(ask_price),
        quantity=str(quantity),
    )

    # 4. Place market buy order
    try:
        result = await lt.client.place_spot_order(
            symbol=symbol,
            side="buy",
            order_type="market",
            amount=quantity,
        )
        order_id = result.get("id", "unknown")
        status = result.get("status", "unknown")
        filled = result.get("filled", result.get("amount", quantity))
        avg_price = result.get("average", result.get("price", ask_price))

        logger.info(
            "Spot DCA: order placed successfully",
            order_id=order_id,
            status=status,
            filled=str(filled),
            avg_price=str(avg_price),
            spend_usd=str(spend_usd),
            symbol=symbol,
        )

        if send_alert:
            await send_alert(
                "SPOT_DCA",
                f"Bought {filled} {asset} @ ${avg_price} (${spend_usd:.2f} spent)",
            )
        return True, False

    except Exception as e:
        retryable = _is_retryable_dca_error(e)
        logger.error(
            "Spot DCA: order placement FAILED",
            error=str(e),
            error_type=type(e).__name__,
            symbol=symbol,
            quantity=str(quantity),
            retryable=retryable,
        )
        if send_alert:
            await send_alert("SPOT_DCA", f"Order FAILED for {symbol} – {e}", urgent=True)
        return False, retryable


def _seconds_until_next_run(hour_utc: int, minute_utc: int) -> float:
    """Compute seconds until the next occurrence of HH:MM UTC."""
    now = datetime.now(timezone.utc)
    target_today = now.replace(
        hour=hour_utc, minute=minute_utc, second=5, microsecond=0,
    )
    if now >= target_today:
        target = target_today + timedelta(days=1)
    else:
        target = target_today
    return (target - now).total_seconds()
