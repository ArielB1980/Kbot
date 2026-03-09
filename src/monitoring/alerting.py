"""
Lightweight alerting module for critical trading events.

Sends notifications via webhook (Telegram or Discord).
Configure via environment variables:
  ALERT_WEBHOOK_URL  - Telegram bot URL or Discord webhook URL
  ALERT_CHAT_ID      - Telegram chat ID (required for Telegram, ignored for Discord)

If no webhook is configured, alerts are logged but not sent.
"""
import os
import asyncio
from datetime import datetime, timezone
from typing import Optional
import aiohttp

from src.exceptions import OperationalError
from src.monitoring.logger import get_logger

logger = get_logger(__name__)

# Rate limit: max 1 alert per event type per 5 minutes
_last_alert_times: dict[str, datetime] = {}
_RATE_LIMIT_SECONDS = 300
_THESIS_ALLOWED_EVENT_TYPES = {
    "THESIS_CONVICTION_COLLAPSE",
    "THESIS_INVALIDATED",
    "THESIS_EARLY_EXIT_TRIGGERED",
    "THESIS_REENTRY_BLOCKED",
    "THESIS_TRADE_OPENED",
    "THESIS_TRADE_CLOSED",
}


def fmt_price(value) -> str:
    """Format a price/stop for notification display.

    Rules:
      >= $1:    2 decimal places  (e.g. 4728.69)
      >= $0.01: 4 decimal places  (e.g. 0.0312)
      < $0.01:  6 decimal places  (e.g. 0.000042)
      None/invalid: '-'
    """
    if value is None:
        return "-"
    try:
        num = float(value)
    except (TypeError, ValueError):
        return str(value)
    if num == 0:
        return "0.00"
    abs_val = abs(num)
    if abs_val >= 1:
        return f"{num:,.2f}"
    if abs_val >= 0.01:
        return f"{num:,.4f}"
    return f"{num:,.6f}"


def fmt_size(value) -> str:
    """Format a position size for notification display.

    Uses enough decimals to be meaningful but caps at 4.
    """
    if value is None:
        return "-"
    try:
        num = float(value)
    except (TypeError, ValueError):
        return str(value)
    abs_val = abs(num)
    if abs_val >= 100:
        return f"{num:,.2f}"
    if abs_val >= 1:
        return f"{num:,.4f}"
    if abs_val >= 0.01:
        return f"{num:,.4f}"
    return f"{num:,.6f}"


def _is_telegram(url: str) -> bool:
    return "api.telegram.org" in url


def _is_discord(url: str) -> bool:
    return "discord.com/api/webhooks" in url or "discordapp.com/api/webhooks" in url


async def send_alert(
    event_type: str,
    message: str,
    urgent: bool = False,
    *,
    rate_limit_key: Optional[str] = None,
    rate_limit_seconds: int = _RATE_LIMIT_SECONDS,
) -> None:
    """
    Send an alert notification.
    
    Args:
        event_type: Type of event (e.g., "KILL_SWITCH", "HALT", "NEW_POSITION")
        message: Human-readable message
        urgent: If True, bypass rate limiting
    """
    webhook_url = os.environ.get("ALERT_WEBHOOK_URL", "").strip()
    chat_id = os.environ.get("ALERT_CHAT_ID", "").strip()
    
    if not webhook_url:
        # No webhook configured — log only
        logger.info("Alert (no webhook configured)", event_type=event_type, message=message)
        return
    
    # Suppress all non-thesis Telegram alerts (requested ops mode).
    # Existing alert callsites are preserved but muted here.
    if event_type not in _THESIS_ALLOWED_EVENT_TYPES:
        logger.info("Alert suppressed (non-thesis event)", event_type=event_type)
        return

    # Rate limiting (unless urgent)
    now = datetime.now(timezone.utc)
    key = rate_limit_key or event_type
    if not urgent:
        last = _last_alert_times.get(key)
        if last and (now - last).total_seconds() < max(1, int(rate_limit_seconds)):
            return  # Rate limited, skip
    
    _last_alert_times[key] = now
    
    # Format message
    timestamp = now.strftime("%H:%M:%S UTC")
    prefix = "🚨" if urgent else "📊"
    formatted = f"{prefix} [{event_type}] {timestamp}\n{message}"
    
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            if _is_telegram(webhook_url):
                payload = {
                    "chat_id": chat_id,
                    "text": formatted,
                    "parse_mode": "HTML",
                }
                async with session.post(webhook_url, json=payload) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.warning("Telegram alert failed", status=resp.status, body=body[:200])
            elif _is_discord(webhook_url):
                payload = {"content": formatted}
                async with session.post(webhook_url, json=payload) as resp:
                    if resp.status not in (200, 204):
                        body = await resp.text()
                        logger.warning("Discord alert failed", status=resp.status, body=body[:200])
            else:
                # Generic webhook — POST JSON
                payload = {
                    "event_type": event_type,
                    "message": message,
                    "timestamp": now.isoformat(),
                    "urgent": urgent,
                }
                async with session.post(webhook_url, json=payload) as resp:
                    if resp.status >= 400:
                        logger.warning("Webhook alert failed", status=resp.status)
    except (OperationalError, OSError, ConnectionError) as e:
        # Alert failures must never crash the trading system
        logger.warning("Alert send failed (non-fatal)", event_type=event_type, error=str(e), error_type=type(e).__name__)


def send_alert_sync(
    event_type: str,
    message: str,
    urgent: bool = False,
    *,
    rate_limit_key: Optional[str] = None,
    rate_limit_seconds: int = _RATE_LIMIT_SECONDS,
) -> None:
    """Synchronous wrapper for send_alert (for use outside async context)."""
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(
            send_alert(
                event_type,
                message,
                urgent,
                rate_limit_key=rate_limit_key,
                rate_limit_seconds=rate_limit_seconds,
            )
        )
    except RuntimeError:
        # No running loop — run directly
        asyncio.run(
            send_alert(
                event_type,
                message,
                urgent,
                rate_limit_key=rate_limit_key,
                rate_limit_seconds=rate_limit_seconds,
            )
        )
