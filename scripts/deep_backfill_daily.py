#!/usr/bin/env python3
"""Deep backfill candles to improve research context quality."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config.config import load_config
from src.data.data_acquisition import DataAcquisition
from src.data.coinapi_client import CoinAPIClient
from src.data.kraken_client import KrakenClient
from src.storage.repository import count_candles


async def run(
    days: int,
    delay_seconds: float,
    batch_size: int,
    batch_pause_seconds: float,
    symbols_csv: str,
    timeframe: str,
    source: str,
) -> None:
    cfg = load_config()
    if symbols_csv.strip():
        symbols = [s.strip() for s in symbols_csv.split(",") if s.strip()]
    else:
        symbols = list(getattr(cfg.exchange, "spot_markets", []) or [])
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=max(1, int(days)))

    print(
        f"[start] ts={datetime.now(timezone.utc).isoformat()} symbols={len(symbols)} timeframe={timeframe} days={days}",
        flush=True,
    )

    client = KrakenClient(
        api_key=cfg.exchange.api_key or "",
        api_secret=cfg.exchange.api_secret or "",
        futures_api_key=cfg.exchange.futures_api_key or "",
        futures_api_secret=cfg.exchange.futures_api_secret or "",
        use_testnet=False,
    )
    await client.initialize()
    source_normalized = str(source or "kraken").strip().lower()
    coinapi_client = None
    if source_normalized == "coinapi":
        coinapi_key = (os.getenv("COINAPI_API_KEY") or "").strip()
        if not coinapi_key:
            raise RuntimeError(
                "COINAPI_API_KEY is required when --source coinapi is used."
            )
        coinapi_client = CoinAPIClient(api_key=coinapi_key)

    acq = DataAcquisition(client, symbols, [], coinapi_client=coinapi_client)

    ok = 0
    failed = 0
    for idx, symbol in enumerate(symbols, start=1):
        before = int(count_candles(symbol, timeframe))
        try:
            candles = await acq.fetch_spot_historical(
                symbol=symbol,
                timeframe=timeframe,
                start_time=start_time,
                end_time=end_time,
                source=source_normalized,
            )
            after = int(count_candles(symbol, timeframe))
            gained = max(0, after - before)
            print(
                f"[{idx}/{len(symbols)}] {symbol} ok fetched={len(candles)} before={before} after={after} gained={gained}",
                flush=True,
            )
            ok += 1
        except Exception as exc:  # noqa: BLE001
            print(f"[{idx}/{len(symbols)}] {symbol} FAIL {type(exc).__name__}: {exc}", flush=True)
            failed += 1
        await asyncio.sleep(max(0.0, delay_seconds))
        if batch_size > 0 and idx % batch_size == 0:
            print(f"[batch-pause] idx={idx} ok={ok} failed={failed}", flush=True)
            await asyncio.sleep(max(0.0, batch_pause_seconds))

    if coinapi_client is not None:
        await coinapi_client.close()
    await client.close()
    print(f"[done] ts={datetime.now(timezone.utc).isoformat()} ok={ok} failed={failed}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Deep backfill candles for selected symbols.")
    parser.add_argument("--days", type=int, default=5000)
    parser.add_argument("--delay-seconds", type=float, default=0.25)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--batch-pause-seconds", type=float, default=2.0)
    parser.add_argument("--symbols", type=str, default="", help="Comma-separated symbols override")
    parser.add_argument("--timeframe", type=str, default="1d", choices=["15m", "1h", "4h", "1d"])
    parser.add_argument("--source", type=str, default="kraken", choices=["kraken", "coinapi"])
    args = parser.parse_args()
    asyncio.run(
        run(
            days=args.days,
            delay_seconds=args.delay_seconds,
            batch_size=args.batch_size,
            batch_pause_seconds=args.batch_pause_seconds,
            symbols_csv=args.symbols,
            timeframe=args.timeframe,
            source=args.source,
        )
    )


if __name__ == "__main__":
    main()

