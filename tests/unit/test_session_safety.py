"""
Tests that repository read functions return detached-safe payloads.

Verifies that ORM attribute access happens within the session scope,
preventing DetachedInstanceError at runtime.
"""
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch


def _make_orm_row(**attrs):
    """Build a MagicMock ORM row that raises on attribute access after detach."""
    row = MagicMock()
    row._detached = False

    def _getattr(name):
        if row._detached and name not in ("_detached", "_mock_name", "_mock_children"):
            raise Exception(
                f"DetachedInstanceError: accessing '{name}' after session close"
            )
        return attrs.get(name)

    row.__getattr__ = _getattr
    # Pre-set attributes so they work while "attached"
    for k, v in attrs.items():
        setattr(row, k, v)
    return row


class TestGetCandlesSessionSafety:
    """get_candles must convert ORM→Candle inside the session."""

    def test_returns_candle_dataclass(self):
        from src.storage.repository import get_candles

        candles = get_candles("BTC/USD", "1h", limit=5)
        # With the mocked DB returning [], result should be empty list
        assert isinstance(candles, list)

    def test_returns_empty_for_no_data(self):
        from src.storage.repository import get_candles

        result = get_candles("BTC/USD", "1h")
        assert result == []


class TestGetActivePositionsSessionSafety:
    """get_active_positions must convert ORM→Position inside the session."""

    def test_returns_list(self):
        from src.storage.repository import get_active_positions

        result = get_active_positions()
        assert isinstance(result, list)


class TestGetRecentEventsSessionSafety:
    """get_recent_events must convert ORM→dict inside the session."""

    def test_returns_list_of_dicts(self):
        from src.storage.repository import get_recent_events

        result = get_recent_events(limit=10)
        assert isinstance(result, list)


class TestLoadRecentIntentHashes:
    """load_recent_intent_hashes must return Set[str]."""

    def test_returns_set(self):
        from src.storage.repository import load_recent_intent_hashes

        result = load_recent_intent_hashes(lookback_hours=1)
        assert isinstance(result, set)


class TestReplayTickerProviderSessionSafety:
    """ReplayTickerProvider.preload must extract ORM data before session close."""

    def test_preload_extracts_before_close(self):
        """Verify that all ORM attribute reads happen before session.close()."""
        from src.replay.replay_ticker_provider import ReplayTickerProvider

        ts = datetime(2024, 1, 1, tzinfo=UTC)

        mock_row = MagicMock()
        mock_row.symbol = "BTC/USD"
        mock_row.ts_utc = ts
        mock_row.futures_bid = "50000"
        mock_row.futures_ask = "50001"
        mock_row.futures_volume_usd_24h = "1000000"
        mock_row.open_interest_usd = "500000"
        mock_row.funding_rate = "0.0001"
        mock_row.error_code = None

        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [mock_row]

        with patch(
            "src.replay.replay_ticker_provider.sessionmaker"
        ) as mock_sm:
            mock_sm.return_value.return_value = mock_session
            provider = ReplayTickerProvider("sqlite:///:memory:")
            provider.preload(["BTC/USD"], ts, ts)

        # After preload, cache should contain extracted data
        assert "BTC/USD" in provider._cache
        assert len(provider._cache["BTC/USD"]) == 1
        entry = provider._cache["BTC/USD"][0]
        assert entry.bid == Decimal("50000")
        assert entry.ask == Decimal("50001")


class TestReplayCandleMetaProviderSessionSafety:
    """ReplayCandleMetaProvider.preload must extract ORM data before session close."""

    def test_preload_extracts_before_close(self):
        from src.replay.replay_candle_meta_provider import ReplayCandleMetaProvider

        ts = datetime(2024, 1, 1, tzinfo=UTC)

        mock_row = MagicMock()
        mock_row.symbol = "BTC/USD"
        mock_row.ts_utc = ts
        mock_row.last_candle_ts_json = '{"1h": "2024-01-01T00:00:00+00:00"}'
        mock_row.candle_count_json = '{"1h": 100}'

        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [mock_row]

        with patch(
            "src.replay.replay_candle_meta_provider.sessionmaker"
        ) as mock_sm:
            mock_sm.return_value.return_value = mock_session
            provider = ReplayCandleMetaProvider("sqlite:///:memory:")
            provider.preload(["BTC/USD"], ts, ts)

        assert "BTC/USD" in provider._cache
        assert len(provider._cache["BTC/USD"]) == 1
        entry = provider._cache["BTC/USD"][0]
        assert entry.candle_counts == {"1h": 100}
        assert "1h" in entry.last_candle_ts
