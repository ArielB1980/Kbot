"""
Unit tests for market discovery: Registry uses KrakenClient methods (not spot_exchange),
discovery non-empty, spot-fails-futures-succeeds, and LiveTrading keeps existing when empty.
"""
from __future__ import annotations

import pytest
from decimal import Decimal
from datetime import datetime, timezone
from unittest.mock import MagicMock, AsyncMock, patch

from src.data.market_registry import MarketRegistry, MarketPair
from src.data.market_discovery import MarketDiscoveryService


from decimal import Decimal
from src.data.kraken_client import FuturesTicker


class FakeKrakenClient:
    """Client that exposes get_spot_markets/get_futures_markets only. No spot_exchange/futures_exchange."""

    def __init__(self, spot_markets=None, futures_markets=None, spot_raises=False):
        self._spot = spot_markets or {"BTC/USD": {"id": "btcusd", "base": "BTC", "quote": "USD", "active": True}}
        self._futures = futures_markets or {
            "BTC/USD": {"symbol": "PF_XBTUSD", "base": "XBT", "quote": "USD", "active": True},
            "ETH/USD": {"symbol": "PF_ETHUSD", "base": "ETH", "quote": "USD", "active": True},
        }
        self._spot_raises = spot_raises

    async def get_spot_markets(self):
        if self._spot_raises:
            raise RuntimeError("spot fetch failed")
        return self._spot

    async def get_futures_markets(self):
        return self._futures

    async def get_spot_ticker(self, symbol: str):
        return {"quoteVolume": 10_000_000, "bid": 1, "ask": 1.001, "last": 1}

    async def get_spot_tickers_bulk(self, symbols):
        """Bulk spot tickers for filtering."""
        return {s: {"quoteVolume": 10_000_000, "bid": 50000, "ask": 50010, "last": 50005} for s in symbols}

    async def get_futures_tickers_bulk_full(self):
        """Bulk futures tickers for filtering (new method)."""
        tickers = {}
        for spot_symbol, info in self._futures.items():
            futures_symbol = info.get("symbol", f"PF_{spot_symbol.replace('/', '')}")
            tickers[futures_symbol] = FuturesTicker(
                symbol=futures_symbol,
                mark_price=Decimal("50000"),
                bid=Decimal("49995"),
                ask=Decimal("50005"),
                volume_24h=Decimal("100000000"),  # $100M - passes filters
                open_interest=Decimal("50000000"),  # $50M - passes filters
                funding_rate=Decimal("0.0001"),
            )
            # Also add spot symbol key for lookup
            tickers[spot_symbol] = tickers[futures_symbol]
        return tickers

    def __getattr__(self, name):
        if name in ("spot_exchange", "futures_exchange"):
            raise AttributeError(f"'{type(self).__name__}' has no attribute '{name}'")
        raise AttributeError(name)


@pytest.fixture
def mock_config():
    c = MagicMock()
    c.exchange = MagicMock()
    c.exchange.allow_futures_only_universe = False
    c.exchange.allow_futures_only_pairs = False
    c.liquidity_filters = MagicMock()
    # Spot filters
    c.liquidity_filters.min_spot_volume_usd_24h = Decimal("1")
    c.liquidity_filters.max_spread_pct = Decimal("0.01")
    c.liquidity_filters.min_price_usd = Decimal("0.001")
    # Futures filters (new)
    c.liquidity_filters.min_futures_open_interest = Decimal("1")  # Very low for testing
    c.liquidity_filters.max_futures_spread_pct = Decimal("0.01")
    c.liquidity_filters.min_futures_volume_usd_24h = Decimal("1")
    c.liquidity_filters.max_funding_rate_abs = Decimal("0.01")
    c.liquidity_filters.filter_mode = "futures_primary"
    return c


@pytest.mark.asyncio
async def test_registry_uses_client_methods_not_attributes(mock_config):
    """MarketRegistry must call get_spot_markets/get_futures_markets; must not touch spot_exchange."""
    client = FakeKrakenClient(
        spot_markets={"BTC/USD": {"id": "x", "base": "BTC", "quote": "USD", "active": True}},
        futures_markets={"BTC/USD": {"symbol": "PF_XBTUSD", "base": "XBT", "quote": "USD", "active": True}},
    )
    registry = MarketRegistry(client, mock_config)
    pairs = await registry.discover_markets()
    # If registry had used client.spot_exchange, FakeKrakenClient would raise AttributeError
    assert isinstance(pairs, dict)
    assert len(pairs) >= 1


@pytest.mark.asyncio
async def test_discovery_non_empty(mock_config):
    """When client returns spot and futures, registry returns > 0 tradable pairs."""
    client = FakeKrakenClient(
        spot_markets={"BTC/USD": {"id": "a", "base": "BTC", "quote": "USD", "active": True}},
        futures_markets={"BTC/USD": {"symbol": "PF_XBTUSD", "base": "XBT", "quote": "USD", "active": True}},
    )
    registry = MarketRegistry(client, mock_config)
    pairs = await registry.discover_markets()
    assert len(pairs) > 0
    for spot_symbol, pair in pairs.items():
        assert isinstance(pair, MarketPair)
        assert pair.spot_symbol == spot_symbol
        assert pair.futures_symbol


@pytest.mark.asyncio
async def test_spot_fails_futures_succeeds_allow_futures_only(mock_config):
    """When spot raises, futures succeeds, and allow_futures_only_universe=True, registry returns futures-only."""
    mock_config.exchange.allow_futures_only_universe = True
    client = FakeKrakenClient(spot_raises=True)
    registry = MarketRegistry(client, mock_config)
    pairs = await registry.discover_markets()
    # _apply_filters still runs over pairs; get_spot_ticker is called per symbol.
    # With futures-only mapping we get pairs from _build_futures_only_mappings.
    assert len(pairs) >= 1


@pytest.mark.asyncio
async def test_spot_fails_futures_succeeds_disallow_futures_only(mock_config):
    """When spot raises, futures succeeds, allow_futures_only_universe=False -> mappings from spot×futures are empty."""
    mock_config.exchange.allow_futures_only_universe = False
    client = FakeKrakenClient(spot_raises=True)
    registry = MarketRegistry(client, mock_config)
    pairs = await registry.discover_markets()
    # spot_markets={}, futures_markets=non-empty, allow_futures_only=False => _build_mappings({}, fut) => {}
    assert len(pairs) == 0


@pytest.mark.asyncio
async def test_allow_futures_only_pairs_includes_unmapped_futures(mock_config):
    """When enabled, per-symbol futures-only contracts are included even if spot mapping is missing."""
    mock_config.exchange.allow_futures_only_pairs = True
    client = FakeKrakenClient(
        spot_markets={"BTC/USD": {"id": "a", "base": "BTC", "quote": "USD", "active": True}},
        futures_markets={
            "BTC/USD": {"symbol": "PF_XBTUSD", "base": "XBT", "quote": "USD", "active": True},
            "NEWTOKEN/USD": {"symbol": "PF_NEWTOKENUSD", "base": "NEWTOKEN", "quote": "USD", "active": True},
        },
    )
    registry = MarketRegistry(client, mock_config)
    pairs = await registry.discover_markets()

    assert "BTC/USD" in pairs
    assert "NEWTOKEN/USD" in pairs
    assert pairs["NEWTOKEN/USD"].source == "futures_only"


@pytest.mark.asyncio
async def test_discovery_gap_report_marks_unmapped_futures_when_disabled(mock_config):
    """Gap report should explain unmapped futures contracts when futures-only pair mode is disabled."""
    mock_config.exchange.allow_futures_only_pairs = False
    client = FakeKrakenClient(
        spot_markets={"BTC/USD": {"id": "a", "base": "BTC", "quote": "USD", "active": True}},
        futures_markets={
            "BTC/USD": {"symbol": "PF_XBTUSD", "base": "XBT", "quote": "USD", "active": True},
            "NEWTOKEN/USD": {"symbol": "PF_NEWTOKENUSD", "base": "NEWTOKEN", "quote": "USD", "active": True},
        },
    )
    registry = MarketRegistry(client, mock_config)
    await registry.discover_markets()

    report = registry.get_last_discovery_report()
    entries = {e["spot_symbol"]: e for e in report.get("entries", [])}

    assert entries["BTC/USD"]["status"] == "eligible"
    assert entries["NEWTOKEN/USD"]["status"] == "unmapped_no_spot"
    assert "allow_futures_only_pairs" in entries["NEWTOKEN/USD"]["reason"]


@pytest.mark.asyncio
async def test_live_trading_keeps_existing_universe_and_logs_critical():
    """When discovery returns empty, LiveTrading keeps existing symbols and logs CRITICAL (once per cooldown)."""
    from src.live.live_trading import LiveTrading

    config = MagicMock()
    config.exchange = MagicMock()
    config.exchange.use_market_discovery = True
    config.exchange.market_discovery_failure_log_cooldown_minutes = 60
    config.exchange.market_discovery_cache_minutes = 60
    config.exchange.api_key = "k"
    config.exchange.api_secret = "s"
    config.exchange.futures_api_key = "fk"
    config.exchange.futures_api_secret = "fs"
    config.exchange.use_testnet = False
    config.exchange.spot_markets = ["BTC/USD", "ETH/USD"]
    config.exchange.futures_markets = ["BTCUSD-PERP", "ETHUSD-PERP"]
    config.exchange.position_size_is_notional = False
    config.system = MagicMock()
    config.system.dry_run = True
    config.risk = MagicMock()
    config.risk.auction_mode_enabled = False
    config.risk.shock_guard_enabled = False
    config.strategy = MagicMock()
    config.strategy.bias_timeframes = ["4h"]
    config.strategy.execution_timeframes = ["15m"]
    config.execution = MagicMock()
    config.execution.tp_backfill_enabled = False
    config.execution.order_timeout_seconds = 120
    config.assets = MagicMock()
    config.assets.mode = "auto"
    config.assets.whitelist = []
    config.assets.blacklist = []
    config.coin_universe = MagicMock()
    config.coin_universe.enabled = False
    config.data = MagicMock()
    config.data.min_healthy_coins = 30
    config.data.min_health_ratio = 0.25
    config.data.max_concurrent_ohlcv = 8
    config.reconciliation = MagicMock()
    config.reconciliation.reconcile_enabled = False

    with patch("src.live.live_trading.KrakenClient"):
        with patch("src.live.live_trading.DataAcquisition"):
            with patch("src.live.live_trading.CandleManager", MagicMock()):
                lt = LiveTrading(config)
    lt.markets = {"BTC/USD": "PF_XBTUSD", "ETH/USD": "PF_ETHUSD"}
    lt._last_discovery_error_log_time = None

    with patch.object(lt.market_discovery, "discover_markets", new_callable=AsyncMock, return_value={}):
        with patch("src.live.coin_processor.logger") as mock_log:
            await lt._update_market_universe()

    assert lt.markets == {"BTC/USD": "PF_XBTUSD", "ETH/USD": "PF_ETHUSD"}
    critical_calls = [c for c in mock_log.method_calls if c[0] == "critical"]
    assert len(critical_calls) >= 1
    assert "Market discovery empty" in str(critical_calls[0])


@pytest.mark.asyncio
async def test_live_trading_discovery_respects_whitelist_mode():
    """Discovery updates must not re-add symbols excluded by assets.whitelist."""
    from src.live.live_trading import LiveTrading

    config = MagicMock()
    config.exchange = MagicMock()
    config.exchange.use_market_discovery = True
    config.exchange.market_discovery_failure_log_cooldown_minutes = 60
    config.exchange.market_discovery_cache_minutes = 60
    config.exchange.api_key = "k"
    config.exchange.api_secret = "s"
    config.exchange.futures_api_key = "fk"
    config.exchange.futures_api_secret = "fs"
    config.exchange.use_testnet = False
    config.exchange.spot_markets = ["BTC/USD", "ETH/USD"]
    config.exchange.futures_markets = ["BTCUSD-PERP", "ETHUSD-PERP"]
    config.exchange.position_size_is_notional = False
    config.system = MagicMock()
    config.system.dry_run = True
    config.risk = MagicMock()
    config.risk.auction_mode_enabled = False
    config.risk.shock_guard_enabled = False
    config.strategy = MagicMock()
    config.strategy.bias_timeframes = ["4h"]
    config.strategy.execution_timeframes = ["15m"]
    config.execution = MagicMock()
    config.execution.tp_backfill_enabled = False
    config.execution.order_timeout_seconds = 120
    config.execution.entry_blocklist_spot_symbols = []
    config.execution.entry_blocklist_bases = []
    config.assets = MagicMock()
    config.assets.mode = "whitelist"
    config.assets.whitelist = ["BTC/USD", "XRP/USD"]
    config.assets.blacklist = []
    config.coin_universe = MagicMock()
    config.coin_universe.enabled = False
    config.data = MagicMock()
    config.data.min_healthy_coins = 30
    config.data.min_health_ratio = 0.25
    config.data.max_concurrent_ohlcv = 8
    config.reconciliation = MagicMock()
    config.reconciliation.reconcile_enabled = False

    with patch("src.live.live_trading.KrakenClient"):
        with patch("src.live.live_trading.DataAcquisition"):
            with patch("src.live.live_trading.CandleManager", MagicMock()):
                lt = LiveTrading(config)

    with patch.object(
        lt.market_discovery,
        "discover_markets",
        new_callable=AsyncMock,
        return_value={
            "BTC/USD": "PF_XBTUSD",
            "ETH/USD": "PF_ETHUSD",
            "SOL/USD": "PF_SOLUSD",
            "XRP/USD": "PF_XRPUSD",
        },
    ):
        await lt._update_market_universe()

    assert set(lt.markets.keys()) == {"BTC/USD", "XRP/USD"}
    lt.data_acq.update_symbols.assert_called_once()
    args, _kwargs = lt.data_acq.update_symbols.call_args
    assert set(args[0]) == {"BTC/USD", "XRP/USD"}


def test_market_discovery_service_pinned_majors_default_to_tier_a():
    """Pinned majors should resolve Tier A even if discovery cache is empty."""
    config = MagicMock()
    config.exchange = MagicMock()
    config.exchange.allow_futures_only_universe = False
    config.exchange.allow_futures_only_pairs = False
    service = MarketDiscoveryService(FakeKrakenClient(), config)

    assert service.get_symbol_tier("BTC/USD") == "A"
    assert service.get_symbol_tier("ETH/USD") == "A"
    assert service.get_symbol_tier("SOL/USD") == "A"
    assert service.get_symbol_tier("BNB/USD") == "A"
