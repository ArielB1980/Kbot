#!/usr/bin/env python3
"""Backfill SOL intraday candles on server and print coverage."""

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.config.config import load_config
from src.data.data_acquisition import DataAcquisition
from src.data.kraken_client import KrakenClient
from src.storage.repository import count_candles, get_candles


async def main() -> None:
    cfg = load_config("src/config/config.yaml")
    symbol = "SOL/USD"
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=190)

    client = KrakenClient(
        api_key=os.getenv("KRAKEN_API_KEY", ""),
        api_secret=os.getenv("KRAKEN_API_SECRET", ""),
    )
    await client.initialize()
    try:
        acq = DataAcquisition(
            kraken_client=client,
            spot_symbols=[symbol],
            futures_symbols=cfg.exchange.futures_markets,
        )

        print("BACKFILL_START", symbol, start_time.isoformat(), end_time.isoformat())
        for tf in ("1h", "15m"):
            before = count_candles(symbol, tf)
            candles = await acq.fetch_spot_historical(
                symbol=symbol,
                timeframe=tf,
                start_time=start_time,
                end_time=end_time,
            )
            after = count_candles(symbol, tf)
            print(
                "BACKFILL_TF",
                tf,
                "fetched",
                len(candles),
                "count_before",
                before,
                "count_after",
                after,
            )
            await asyncio.sleep(2.0)

        for tf in ("1h", "4h", "15m", "1d"):
            series = get_candles(symbol, tf, datetime(2024, 1, 1), datetime(2026, 12, 31))
            if series:
                print(
                    "COVERAGE",
                    tf,
                    len(series),
                    series[0].timestamp.isoformat(),
                    series[-1].timestamp.isoformat(),
                )
            else:
                print("COVERAGE", tf, 0)
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
