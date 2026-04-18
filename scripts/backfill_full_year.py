#!/usr/bin/env python3
"""Backfill full year of candle data by aggregating Kraken trades.

Kraken's OHLC endpoint only returns the last ~720 candles regardless of the
`since` parameter. This script fetches raw trades via the Trades endpoint
(which supports full history) and aggregates them into OHLCV candles.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.domain.models import Candle
from src.storage.repository import count_candles, save_candles_bulk

KRAKEN_TRADES_URL = "https://api.kraken.com/0/public/Trades"

PAIR_MAP = {
    "BTC/USD": "XXBTZUSD",
    "ETH/USD": "XETHZUSD",
    "SOL/USD": "SOLUSD",
    "XRP/USD": "XXRPZUSD",
    "ADA/USD": "ADAUSD",
    "LINK/USD": "LINKUSD",
    "DOGE/USD": "XDGUSD",
}

TF_MINUTES = {
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
}


def _bucket_timestamp(trade_ts: float, tf_minutes: int) -> datetime:
    """Round a trade timestamp down to the candle bucket start."""
    dt = datetime.fromtimestamp(trade_ts, tz=timezone.utc)
    total_minutes = dt.hour * 60 + dt.minute
    bucket_minutes = (total_minutes // tf_minutes) * tf_minutes
    return dt.replace(
        hour=bucket_minutes // 60,
        minute=bucket_minutes % 60,
        second=0,
        microsecond=0,
    )


def _trades_to_candles(
    trades: list[list],
    symbol: str,
    timeframe: str,
    tf_minutes: int,
) -> list[Candle]:
    """Aggregate raw Kraken trades into OHLCV candles."""
    buckets: dict[datetime, dict] = defaultdict(
        lambda: {"open": None, "high": None, "low": None, "close": None, "volume": Decimal("0")}
    )

    for trade in trades:
        price = Decimal(str(trade[0]))
        volume = Decimal(str(trade[1]))
        ts = float(trade[2])
        bucket = _bucket_timestamp(ts, tf_minutes)
        b = buckets[bucket]
        if b["open"] is None:
            b["open"] = price
        b["high"] = max(b["high"], price) if b["high"] is not None else price
        b["low"] = min(b["low"], price) if b["low"] is not None else price
        b["close"] = price
        b["volume"] += volume

    candles = []
    for ts, b in sorted(buckets.items()):
        if b["open"] is None:
            continue
        candles.append(
            Candle(
                timestamp=ts,
                symbol=symbol,
                timeframe=timeframe,
                open=b["open"],
                high=b["high"],
                low=b["low"],
                close=b["close"],
                volume=b["volume"],
            )
        )
    return candles


async def fetch_trades_page(
    session: aiohttp.ClientSession,
    pair: str,
    since_ns: int,
) -> tuple[list[list], int | None]:
    """Fetch one page of trades from Kraken REST API.

    Args:
        pair: Kraken pair name
        since_ns: Nanosecond timestamp — Kraken returns trades *after* this

    Returns:
        (trades, last_ns) where last_ns is the continuation pointer
    """
    params = {"pair": pair, "since": str(since_ns), "count": 1000}
    async with session.get(KRAKEN_TRADES_URL, params=params) as resp:
        data = await resp.json()

    if data.get("error"):
        raise RuntimeError(f"Kraken API error: {data['error']}")

    result = data.get("result", {})
    last = result.pop("last", None)

    trades = []
    for key, value in result.items():
        if isinstance(value, list):
            trades = value
            break

    return trades, int(last) if last else None


async def backfill_symbol(
    symbol: str,
    timeframes: list[str],
    start_date: datetime,
    end_date: datetime,
    delay: float = 1.0,
) -> dict[str, int]:
    """Backfill all timeframes for a symbol by fetching trades and aggregating.

    Saves candles incrementally every FLUSH_PAGES pages to avoid holding all
    trades in memory and to persist progress on interruption.
    """
    pair = PAIR_MAP.get(symbol)
    if not pair:
        print(f"  SKIP {symbol}: no Kraken pair mapping", flush=True)
        return {}

    since_ns = int(start_date.timestamp() * 1e9)
    end_ts = end_date.timestamp()
    pages = 0
    total_trades = 0
    total_candles: dict[str, int] = {tf: 0 for tf in timeframes}
    batch_trades: list[list] = []
    flush_every = 200  # Save every 200 pages (~200k trades, ~2-3 days of SOL)

    print(f"  {symbol}: fetching trades {start_date.date()} → {end_date.date()}...", flush=True)

    async with aiohttp.ClientSession() as session:
        while True:
            trades, last_ns = await fetch_trades_page(session, pair, since_ns)
            pages += 1

            if not trades:
                break

            filtered = [t for t in trades if float(t[2]) <= end_ts]
            batch_trades.extend(filtered)
            total_trades += len(filtered)

            last_trade_ts = float(trades[-1][2])
            if last_trade_ts >= end_ts:
                break

            if last_ns and last_ns > since_ns:
                since_ns = last_ns
            else:
                break

            # Flush batch periodically
            if pages % flush_every == 0:
                for tf in timeframes:
                    tf_min = TF_MINUTES.get(tf)
                    if not tf_min:
                        continue
                    candles = _trades_to_candles(batch_trades, symbol, tf, tf_min)
                    if candles:
                        save_candles_bulk(candles)
                    total_candles[tf] += len(candles)
                batch_trades = []

            if pages % 100 == 0:
                current_date = datetime.fromtimestamp(last_trade_ts, tz=timezone.utc)
                print(
                    f"  {symbol}: page {pages}, {total_trades} trades, "
                    f"at {current_date.date()}, "
                    f"candles so far: { {tf: total_candles[tf] for tf in timeframes} }",
                    flush=True,
                )

            await asyncio.sleep(delay)

    # Final flush
    if batch_trades:
        for tf in timeframes:
            tf_min = TF_MINUTES.get(tf)
            if not tf_min:
                continue
            candles = _trades_to_candles(batch_trades, symbol, tf, tf_min)
            if candles:
                save_candles_bulk(candles)
            total_candles[tf] += len(candles)

    print(
        f"  {symbol}: done — {total_trades} trades across {pages} pages",
        flush=True,
    )
    for tf in timeframes:
        print(f"  {symbol} {tf}: {total_candles[tf]} candles built", flush=True)

    return total_candles


async def main(
    days: int,
    symbols: list[str],
    timeframes: list[str],
    delay: float,
) -> None:
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days)

    print(
        f"[start] {start_date.date()} → {end_date.date()} "
        f"({days}d, {len(symbols)} symbols, {len(timeframes)} timeframes)",
        flush=True,
    )

    for symbol in symbols:
        before = {tf: count_candles(symbol, tf) for tf in timeframes}
        await backfill_symbol(symbol, timeframes, start_date, end_date, delay)
        for tf in timeframes:
            after = count_candles(symbol, tf)
            gained = max(0, after - before[tf])
            print(f"  {symbol} {tf}: before={before[tf]} after={after} gained={gained}", flush=True)

    print(f"[done] ts={datetime.now(timezone.utc).isoformat()}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Full-year backfill via Kraken Trades aggregation")
    parser.add_argument("--days", type=int, default=500)
    parser.add_argument("--symbols", type=str, default="SOL/USD,BTC/USD,ETH/USD")
    parser.add_argument("--timeframes", type=str, default="15m,1h,4h")
    parser.add_argument("--delay", type=float, default=1.5, help="Seconds between API calls (rate limit)")
    args = parser.parse_args()

    asyncio.run(
        main(
            days=args.days,
            symbols=[s.strip() for s in args.symbols.split(",")],
            timeframes=[t.strip() for t in args.timeframes.split(",")],
            delay=args.delay,
        )
    )
