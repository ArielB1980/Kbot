"""Tests for the unified alert dispatcher."""

from unittest.mock import AsyncMock, patch

import pytest

from src.monitoring.alert_dispatcher import (
    THESIS_ALLOWED_EVENT_TYPES,
    AlertLevel,
    _last_alert_times,
    fmt_price,
    fmt_size,
    get_event_severity,
    send_alert,
    send_alert_sync,
)

# ---------------------------------------------------------------------------
# fmt_price
# ---------------------------------------------------------------------------


class TestFmtPrice:
    def test_none(self):
        assert fmt_price(None) == "-"

    def test_zero(self):
        assert fmt_price(0) == "0.00"

    def test_large(self):
        assert fmt_price(4728.69) == "4,728.69"

    def test_medium(self):
        assert fmt_price(0.0312) == "0.0312"

    def test_small(self):
        assert fmt_price(0.000042) == "0.000042"

    def test_invalid(self):
        assert fmt_price("abc") == "abc"

    def test_negative(self):
        assert fmt_price(-5.5) == "-5.50"


# ---------------------------------------------------------------------------
# fmt_size
# ---------------------------------------------------------------------------


class TestFmtSize:
    def test_none(self):
        assert fmt_size(None) == "-"

    def test_large(self):
        assert fmt_size(150.5) == "150.50"

    def test_medium(self):
        assert fmt_size(1.5) == "1.5000"

    def test_small(self):
        assert fmt_size(0.05) == "0.0500"

    def test_tiny(self):
        assert fmt_size(0.001) == "0.001000"


# ---------------------------------------------------------------------------
# Event severity
# ---------------------------------------------------------------------------


class TestEventSeverity:
    def test_critical_events(self):
        assert get_event_severity("KILL_SWITCH") == AlertLevel.CRITICAL
        assert get_event_severity("SYSTEM_HALTED") == AlertLevel.CRITICAL

    def test_warning_events(self):
        assert get_event_severity("DAILY_LOSS_WARNING") == AlertLevel.WARNING

    def test_info_events(self):
        assert get_event_severity("THESIS_TRADE_OPENED") == AlertLevel.INFO

    def test_unknown_defaults_to_info(self):
        assert get_event_severity("UNKNOWN_EVENT") == AlertLevel.INFO


# ---------------------------------------------------------------------------
# Thesis allowed event types
# ---------------------------------------------------------------------------


def test_thesis_allowed_types():
    assert "THESIS_TRADE_OPENED" in THESIS_ALLOWED_EVENT_TYPES
    assert "THESIS_TRADE_CLOSED" in THESIS_ALLOWED_EVENT_TYPES
    assert "THESIS_CONVICTION_COLLAPSE" in THESIS_ALLOWED_EVENT_TYPES
    assert "KILL_SWITCH" not in THESIS_ALLOWED_EVENT_TYPES


# ---------------------------------------------------------------------------
# send_alert
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_rate_limits():
    """Reset rate limit state between tests."""
    _last_alert_times.clear()
    yield
    _last_alert_times.clear()


@pytest.mark.asyncio
async def test_send_alert_no_webhook_logs_only(monkeypatch):
    """When no webhook is configured, alert should be logged only."""
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("ALERT_CHAT_ID", raising=False)

    with patch("src.monitoring.alert_dispatcher.logger") as mock_logger:
        await send_alert("THESIS_TRADE_OPENED", "test message")
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        assert "no webhook configured" in call_args[0][0]


@pytest.mark.asyncio
async def test_send_alert_suppresses_non_thesis_events(monkeypatch):
    """Non-thesis events should be suppressed."""
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://api.telegram.org/bot123/sendMessage")
    monkeypatch.setenv("ALERT_CHAT_ID", "12345")

    with patch("src.monitoring.alert_dispatcher.logger") as mock_logger:
        await send_alert("KILL_SWITCH", "test")
        mock_logger.info.assert_called_once()
        assert "suppressed" in mock_logger.info.call_args[0][0]


@pytest.mark.asyncio
async def test_send_alert_rate_limiting(monkeypatch):
    """Second alert within rate limit window should be dropped."""
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://api.telegram.org/bot123/sendMessage")
    monkeypatch.setenv("ALERT_CHAT_ID", "12345")

    send_calls: list[str] = []
    mock_telegram = AsyncMock(side_effect=lambda *a, **kw: send_calls.append("sent"))
    with patch("src.monitoring.alert_dispatcher._send_telegram", mock_telegram):
        await send_alert("THESIS_TRADE_OPENED", "first")
        assert len(send_calls) == 1

        # Second call within rate limit is dropped
        await send_alert("THESIS_TRADE_OPENED", "second")
        assert len(send_calls) == 1  # still 1


@pytest.mark.asyncio
async def test_send_alert_urgent_bypasses_rate_limit(monkeypatch):
    """Urgent alerts should bypass rate limiting."""
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://api.telegram.org/bot123/sendMessage")
    monkeypatch.setenv("ALERT_CHAT_ID", "12345")

    send_calls: list[str] = []
    mock_telegram = AsyncMock(side_effect=lambda *a, **kw: send_calls.append("sent"))
    with patch("src.monitoring.alert_dispatcher._send_telegram", mock_telegram):
        await send_alert("THESIS_TRADE_OPENED", "first", urgent=True)
        await send_alert("THESIS_TRADE_OPENED", "second", urgent=True)
        assert len(send_calls) == 2


def test_send_alert_sync_works(monkeypatch):
    """send_alert_sync should call send_alert without errors."""
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)

    # Should not raise
    send_alert_sync("THESIS_TRADE_OPENED", "sync test")
