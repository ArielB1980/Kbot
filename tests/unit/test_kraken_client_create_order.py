from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from src.data.kraken_client import KrakenClient


def _client() -> KrakenClient:
    return KrakenClient(
        api_key="spot_key",
        api_secret="spot_secret",
        futures_api_key="fut_key",
        futures_api_secret="fut_secret",
    )


@pytest.mark.asyncio
async def test_create_order_does_not_force_default_leverage():
    client = _client()
    client.place_futures_order = AsyncMock(return_value={"id": "order-1"})

    await client.create_order(
        symbol="BTC/USD:USD",
        type="limit",
        side="buy",
        amount=1.0,
        price=50000.0,
        params={},
    )

    kwargs = client.place_futures_order.call_args.kwargs
    assert kwargs["leverage"] is None


@pytest.mark.asyncio
async def test_create_order_uses_explicit_leverage_when_provided():
    client = _client()
    client.place_futures_order = AsyncMock(return_value={"id": "order-2"})

    await client.create_order(
        symbol="BTC/USD:USD",
        type="limit",
        side="buy",
        amount=1.0,
        price=50000.0,
        params={},
        leverage=Decimal("4"),
    )

    kwargs = client.place_futures_order.call_args.kwargs
    assert kwargs["leverage"] == Decimal("4")


@pytest.mark.asyncio
async def test_place_futures_order_resolves_plain_usd_symbol_to_unified():
    client = _client()
    fx = AsyncMock()
    fx.markets = {
        "PF_XMRUSD": {"id": "PF_XMRUSD", "symbol": "XMR/USD:USD"},
    }
    fx.create_order = AsyncMock(return_value={"id": "order-xmr"})
    fx.price_to_precision = lambda symbol, price: str(price)
    fx.set_leverage = AsyncMock(return_value=None)
    client.futures_exchange = fx

    await client.place_futures_order(
        symbol="XMR/USD",
        side="sell",
        order_type="market",
        size=Decimal("1"),
        reduce_only=True,
    )

    kwargs = fx.create_order.call_args.kwargs
    assert kwargs["symbol"] == "XMR/USD:USD"
