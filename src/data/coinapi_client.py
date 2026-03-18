"""CoinAPI historical OHLCV client."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import asyncio
from typing import Optional

import aiohttp

from src.domain.models import Candle
from src.monitoring.logger import get_logger

logger = get_logger(__name__)

COINAPI_BASE_URL = "https://rest.coinapi.io/v1"


@dataclass(frozen=True)
class CoinAPISymbolResolution:
    """Resolved CoinAPI market identifier for a spot symbol."""

    symbol_id: str
    source_hint: str


class CoinAPIClient:
    """Minimal CoinAPI client for historical 4H candles."""

    def __init__(self, api_key: str, base_url: str = COINAPI_BASE_URL):
        if not api_key:
            raise ValueError("CoinAPI key is required")
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None

    async def close(self) -> None:
        """Close HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _session_or_create(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    @staticmethod
    def _period_id(timeframe: str) -> str:
        mapping = {
            "1m": "1MIN",
            "5m": "5MIN",
            "15m": "15MIN",
            "30m": "30MIN",
            "1h": "1HRS",
            "4h": "4HRS",
            "1d": "1DAY",
        }
        try:
            return mapping[timeframe]
        except KeyError as exc:
            raise ValueError(f"Unsupported timeframe for CoinAPI: {timeframe}") from exc

    @staticmethod
    def _candidates_for_symbol(symbol: str) -> list[CoinAPISymbolResolution]:
        base = symbol.split("/")[0].upper()
        return [
            CoinAPISymbolResolution(symbol_id=f"KRAKEN_SPOT_{base}_USD", source_hint="kraken_usd"),
            CoinAPISymbolResolution(symbol_id=f"KRAKEN_SPOT_{base}_USDT", source_hint="kraken_usdt"),
            CoinAPISymbolResolution(symbol_id=f"BINANCE_SPOT_{base}_USDT", source_hint="binance_usdt"),
            CoinAPISymbolResolution(symbol_id=f"COINBASE_SPOT_{base}_USD", source_hint="coinbase_usd"),
        ]

    async def fetch_spot_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[Candle]:
        """
        Fetch historical candles and normalize to Candle model.

        Notes:
        - Uses a candidate market-id search because exchange-specific listings vary.
        - Stores candles under requested `symbol` so downstream logic remains unchanged.
        """
        period_id = self._period_id(timeframe)
        session = await self._session_or_create()
        headers = {"X-CoinAPI-Key": self._api_key}

        last_error: Exception | None = None
        for candidate in self._candidates_for_symbol(symbol):
            candles: list[Candle] = []
            cursor = start_time.astimezone(timezone.utc)
            try:
                while cursor < end_time:
                    # CoinAPI supports up to large limits, but smaller pages reduce blast radius on retries.
                    page = await self._fetch_page(
                        session=session,
                        headers=headers,
                        symbol_id=candidate.symbol_id,
                        period_id=period_id,
                        time_start=cursor,
                        time_end=end_time,
                        limit=1000,
                    )
                    if not page:
                        break

                    page_candles = self._to_candles(page, symbol=symbol, timeframe=timeframe)
                    if not page_candles:
                        break
                    candles.extend(page_candles)

                    next_cursor = page_candles[-1].timestamp + self._timeframe_delta(timeframe)
                    if next_cursor <= cursor:
                        break
                    cursor = next_cursor
                    # Gentle pacing for provider limits.
                    await asyncio.sleep(0.05)

                if candles:
                    logger.info(
                        "CoinAPI candles fetched",
                        symbol=symbol,
                        timeframe=timeframe,
                        source_symbol_id=candidate.symbol_id,
                        source_hint=candidate.source_hint,
                        count=len(candles),
                    )
                    return candles
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                logger.debug(
                    "CoinAPI candidate failed",
                    symbol=symbol,
                    timeframe=timeframe,
                    source_symbol_id=candidate.symbol_id,
                    error=str(exc),
                )
                continue

        if last_error:
            raise last_error
        raise RuntimeError(f"No CoinAPI data found for symbol={symbol} timeframe={timeframe}")

    async def _fetch_page(
        self,
        *,
        session: aiohttp.ClientSession,
        headers: dict[str, str],
        symbol_id: str,
        period_id: str,
        time_start: datetime,
        time_end: datetime,
        limit: int,
    ) -> list[dict]:
        url = f"{self._base_url}/ohlcv/{symbol_id}/history"
        fmt_start = (
            time_start.astimezone(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )
        fmt_end = (
            time_end.astimezone(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )
        params = {
            "period_id": period_id,
            "time_start": fmt_start,
            "time_end": fmt_end,
            "limit": str(limit),
        }
        async with session.get(url, headers=headers, params=params) as resp:
            if resp.status == 429:
                retry_after = float(resp.headers.get("Retry-After", "1") or "1")
                await asyncio.sleep(max(1.0, retry_after))
                raise RuntimeError(f"CoinAPI rate limited for {symbol_id}")
            if resp.status == 401:
                raise RuntimeError("CoinAPI authentication failed (check COINAPI_API_KEY)")
            if resp.status == 403:
                text = await resp.text()
                raise RuntimeError(
                    "CoinAPI access forbidden or quota exhausted; "
                    f"symbol_id={symbol_id} body={text[:200]}"
                )
            if resp.status == 404:
                return []
            if resp.status >= 400:
                text = await resp.text()
                raise RuntimeError(f"CoinAPI request failed status={resp.status} body={text[:200]}")
            payload = await resp.json()
            if not isinstance(payload, list):
                return []
            return payload

    @staticmethod
    def _to_candles(rows: list[dict], *, symbol: str, timeframe: str) -> list[Candle]:
        candles: list[Candle] = []
        for row in rows:
            ts = row.get("time_period_start") or row.get("time_open")
            if not ts:
                continue
            timestamp = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(timezone.utc)
            candles.append(
                Candle(
                    timestamp=timestamp,
                    symbol=symbol,
                    timeframe=timeframe,
                    open=Decimal(str(row.get("price_open", 0))),
                    high=Decimal(str(row.get("price_high", 0))),
                    low=Decimal(str(row.get("price_low", 0))),
                    close=Decimal(str(row.get("price_close", 0))),
                    volume=Decimal(str(row.get("volume_traded", 0))),
                )
            )
        candles.sort(key=lambda c: c.timestamp)
        return candles

    @staticmethod
    def _timeframe_delta(timeframe: str) -> timedelta:
        mapping = {
            "1m": timedelta(minutes=1),
            "5m": timedelta(minutes=5),
            "15m": timedelta(minutes=15),
            "30m": timedelta(minutes=30),
            "1h": timedelta(hours=1),
            "4h": timedelta(hours=4),
            "1d": timedelta(days=1),
        }
        return mapping.get(timeframe, timedelta(hours=1))
