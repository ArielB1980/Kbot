"""Unified alert dispatcher for critical trading events.

Routes notifications to multiple backends (Telegram, Discord, Slack, log)
with centralized rate limiting, formatting, and severity routing.

Configure via environment variables:
  ALERT_WEBHOOK_URL  - Telegram bot URL or Discord webhook URL
  ALERT_CHAT_ID      - Telegram chat ID (required for Telegram)
  SLACK_WEBHOOK_URL  - Slack incoming webhook URL

If no webhooks are configured, alerts are logged only.
"""

import asyncio
import json
import os
import urllib.error
import urllib.request
from datetime import UTC, datetime
from enum import Enum

import aiohttp

from src.exceptions import OperationalError
from src.monitoring.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Alert severity
# ---------------------------------------------------------------------------


class AlertLevel(Enum):
    """Alert severity levels for routing and display."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


# Mapping from event type to default severity (for routing).
# Events not listed here default to INFO.
_EVENT_SEVERITY: dict[str, AlertLevel] = {
    "KILL_SWITCH": AlertLevel.CRITICAL,
    "SYSTEM_HALTED": AlertLevel.CRITICAL,
    "CORRUPTED_POSITION": AlertLevel.CRITICAL,
    "TRADE_RECORD_EXHAUSTED": AlertLevel.CRITICAL,
    "SELF_HEAL_FAILED": AlertLevel.CRITICAL,
    "STALE_PEAK_EQUITY": AlertLevel.WARNING,
    "DAILY_LOSS_WARNING": AlertLevel.WARNING,
    "TRADE_STARVATION": AlertLevel.WARNING,
    "WINNER_CHURN": AlertLevel.WARNING,
    "TRADE_RECORDING_STALL": AlertLevel.WARNING,
    "PHANTOM_IMPORT_FAILED": AlertLevel.WARNING,
    "UNIVERSE_SHRINK": AlertLevel.WARNING,
    "SELF_HEAL_PARTIAL": AlertLevel.WARNING,
    "THESIS_CONVICTION_COLLAPSE": AlertLevel.WARNING,
    "THESIS_INVALIDATED": AlertLevel.WARNING,
    "THESIS_EARLY_EXIT_TRIGGERED": AlertLevel.INFO,
    "THESIS_REENTRY_BLOCKED": AlertLevel.INFO,
    "THESIS_TRADE_OPENED": AlertLevel.INFO,
    "THESIS_TRADE_CLOSED": AlertLevel.INFO,
    "POSITION_CLOSED": AlertLevel.INFO,
    "POSITION_CLOSED_UNPRICED": AlertLevel.WARNING,
    "SELF_HEAL_SUCCESS": AlertLevel.INFO,
    "DAILY_SUMMARY": AlertLevel.INFO,
    "AUTO_RECOVERY": AlertLevel.INFO,
    "SPOT_DCA": AlertLevel.WARNING,
    "TRADE_RECORD_FAILURE": AlertLevel.WARNING,
}

# Thesis event whitelist — non-thesis events are suppressed at Telegram/Discord
# to reduce operational noise.
THESIS_ALLOWED_EVENT_TYPES: frozenset[str] = frozenset(
    {
        "THESIS_CONVICTION_COLLAPSE",
        "THESIS_INVALIDATED",
        "THESIS_EARLY_EXIT_TRIGGERED",
        "THESIS_REENTRY_BLOCKED",
        "THESIS_TRADE_OPENED",
        "THESIS_TRADE_CLOSED",
    }
)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def fmt_price(value: object) -> str:
    """Format a price for notification display.

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


def fmt_size(value: object) -> str:
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


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

_last_alert_times: dict[str, datetime] = {}
_DEFAULT_RATE_LIMIT_SECONDS = 300


# ---------------------------------------------------------------------------
# Backend detection helpers
# ---------------------------------------------------------------------------


def _is_telegram(url: str) -> bool:
    return "api.telegram.org" in url


def _is_discord(url: str) -> bool:
    return "discord.com/api/webhooks" in url or "discordapp.com/api/webhooks" in url


# ---------------------------------------------------------------------------
# Backend senders
# ---------------------------------------------------------------------------


async def _send_telegram(
    session: aiohttp.ClientSession,
    url: str,
    chat_id: str,
    formatted: str,
) -> None:
    payload = {"chat_id": chat_id, "text": formatted, "parse_mode": "HTML"}
    async with session.post(url, json=payload) as resp:
        if resp.status != 200:
            body = await resp.text()
            logger.warning("Telegram alert failed", status=resp.status, body=body[:200])


async def _send_discord_async(
    session: aiohttp.ClientSession,
    url: str,
    formatted: str,
) -> None:
    payload = {"content": formatted}
    async with session.post(url, json=payload) as resp:
        if resp.status not in (200, 204):
            body = await resp.text()
            logger.warning("Discord alert failed", status=resp.status, body=body[:200])


async def _send_generic_webhook(
    session: aiohttp.ClientSession,
    url: str,
    event_type: str,
    message: str,
    now: datetime,
    urgent: bool,
) -> None:
    payload = {
        "event_type": event_type,
        "message": message,
        "timestamp": now.isoformat(),
        "urgent": urgent,
    }
    async with session.post(url, json=payload) as resp:
        if resp.status >= 400:
            logger.warning("Webhook alert failed", status=resp.status)


def _send_slack_sync(
    url: str,
    level: str,
    title: str,
    message: str,
    metadata: dict | None = None,
) -> None:
    """POST to Slack incoming webhook (synchronous, used for Slack backend)."""
    try:
        payload: dict = {
            "text": f"[{level.upper()}] {title}",
            "blocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*{title}*\n{message}"},
                },
            ],
        }
        if metadata:
            payload["blocks"].append(
                {
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": json.dumps(metadata)[:2000]}],
                }
            )
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url, data=data, method="POST", headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as _:  # noqa: S310
            pass
    except (OperationalError, OSError, ConnectionError) as e:
        logger.warning("Slack webhook failed", error=str(e), error_type=type(e).__name__)


# ---------------------------------------------------------------------------
# Core dispatch
# ---------------------------------------------------------------------------


async def send_alert(
    event_type: str,
    message: str,
    urgent: bool = False,
    *,
    rate_limit_key: str | None = None,
    rate_limit_seconds: int = _DEFAULT_RATE_LIMIT_SECONDS,
) -> None:
    """Send an alert notification to all configured backends.

    Args:
        event_type: Type of event (e.g. "KILL_SWITCH", "THESIS_TRADE_OPENED").
        message: Human-readable message body.
        urgent: If True, bypass rate limiting.
        rate_limit_key: Custom key for rate-limit bucketing.
        rate_limit_seconds: Per-key cooldown (default 300s).
    """
    webhook_url = os.environ.get("ALERT_WEBHOOK_URL", "").strip()
    chat_id = os.environ.get("ALERT_CHAT_ID", "").strip()
    slack_url = os.environ.get("SLACK_WEBHOOK_URL", "").strip()

    has_primary = bool(webhook_url)
    has_slack = bool(slack_url)

    if not has_primary and not has_slack:
        logger.info("Alert (no webhook configured)", event_type=event_type, message=message)
        return

    # Suppress non-thesis events from real-time notification backends.
    if event_type not in THESIS_ALLOWED_EVENT_TYPES:
        logger.info("Alert suppressed (non-thesis event)", event_type=event_type)
        return

    # Rate limiting (unless urgent)
    now = datetime.now(UTC)
    key = rate_limit_key or event_type
    if not urgent:
        last = _last_alert_times.get(key)
        if last and (now - last).total_seconds() < max(1, int(rate_limit_seconds)):
            return
    _last_alert_times[key] = now

    # Format message
    timestamp = now.strftime("%H:%M:%S UTC")
    prefix = "\U0001f6a8" if urgent else "\U0001f4ca"
    formatted = f"{prefix} [{event_type}] {timestamp}\n{message}"

    severity = _EVENT_SEVERITY.get(event_type, AlertLevel.INFO)

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            # Primary backend (Telegram / Discord / generic webhook)
            if has_primary:
                if _is_telegram(webhook_url):
                    await _send_telegram(session, webhook_url, chat_id, formatted)
                elif _is_discord(webhook_url):
                    await _send_discord_async(session, webhook_url, formatted)
                else:
                    await _send_generic_webhook(
                        session, webhook_url, event_type, message, now, urgent
                    )

            # Slack backend (if configured, for CRITICAL/WARNING events)
            if has_slack and severity in (AlertLevel.CRITICAL, AlertLevel.WARNING):
                _send_slack_sync(
                    slack_url,
                    severity.value,
                    event_type,
                    message,
                )
    except (OperationalError, OSError, ConnectionError) as e:
        # Alert failures must never crash the trading system
        logger.warning(
            "Alert send failed (non-fatal)",
            event_type=event_type,
            error=str(e),
            error_type=type(e).__name__,
        )


def send_alert_sync(
    event_type: str,
    message: str,
    urgent: bool = False,
    *,
    rate_limit_key: str | None = None,
    rate_limit_seconds: int = _DEFAULT_RATE_LIMIT_SECONDS,
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
        asyncio.run(
            send_alert(
                event_type,
                message,
                urgent,
                rate_limit_key=rate_limit_key,
                rate_limit_seconds=rate_limit_seconds,
            )
        )


def get_event_severity(event_type: str) -> AlertLevel:
    """Look up the default severity for an event type."""
    return _EVENT_SEVERITY.get(event_type, AlertLevel.INFO)
