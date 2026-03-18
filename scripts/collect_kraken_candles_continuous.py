#!/usr/bin/env python3
"""Continuously cache Kraken spot candles into the backtest database."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config.config import load_config
from src.data.data_acquisition import DataAcquisition
from src.data.kraken_client import KrakenClient
from src.monitoring.logger import get_logger
from src.storage.repository import count_candles, get_latest_candle_timestamp

logger = get_logger(__name__)


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


def _resolve_symbols(cfg, symbols_csv: str, max_symbols: int) -> list[str]:
    if symbols_csv.strip():
        return [s.strip() for s in symbols_csv.split(",") if s.strip()]

    universe = []
    try:
        universe = list((cfg.coin_universe.get_all_candidates() or []))
    except Exception:  # noqa: BLE001
        universe = []
    if not universe:
        universe = list(getattr(cfg.exchange, "spot_markets", []) or [])
    if max_symbols > 0:
        universe = universe[:max_symbols]
    return universe


async def collect_once(
    acq: DataAcquisition,
    *,
    symbols: list[str],
    timeframes: list[str],
    bootstrap_days: int,
    dry_run: bool,
) -> tuple[int, int, int]:
    now = datetime.now(timezone.utc)
    fetched_pairs = 0
    updated_pairs = 0
    failed_pairs = 0

    for symbol in symbols:
        for timeframe in timeframes:
            before = int(count_candles(symbol, timeframe))
            latest = get_latest_candle_timestamp(symbol, timeframe)
            if latest is None:
                start_time = now - timedelta(days=max(1, int(bootstrap_days)))
            else:
                start_time = latest + _timeframe_delta(timeframe)
            if start_time >= now:
                continue

            fetched_pairs += 1
            if dry_run:
                logger.info(
                    "Collector dry-run fetch plan",
                    symbol=symbol,
                    timeframe=timeframe,
                    start=start_time.isoformat(),
                    end=now.isoformat(),
                )
                continue

            try:
                candles = await acq.fetch_spot_historical(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_time=start_time,
                    end_time=now,
                    source="kraken",
                )
                after = int(count_candles(symbol, timeframe))
                gained = max(0, after - before)
                if gained > 0:
                    updated_pairs += 1
                logger.info(
                    "Collector fetch complete",
                    symbol=symbol,
                    timeframe=timeframe,
                    fetched=len(candles),
                    before=before,
                    after=after,
                    gained=gained,
                )
            except Exception as exc:  # noqa: BLE001
                failed_pairs += 1
                logger.error(
                    "Collector fetch failed",
                    symbol=symbol,
                    timeframe=timeframe,
                    error=str(exc),
                )

    return fetched_pairs, updated_pairs, failed_pairs


async def run(
    *,
    symbols_csv: str,
    timeframes_csv: str,
    max_symbols: int,
    interval_seconds: int,
    bootstrap_days: int,
    run_once: bool,
    dry_run: bool,
) -> None:
    cfg = load_config()
    symbols = _resolve_symbols(cfg, symbols_csv, max_symbols)
    timeframes = [t.strip() for t in timeframes_csv.split(",") if t.strip()]
    if not symbols:
        raise RuntimeError("No symbols resolved for collector")
    if not timeframes:
        raise RuntimeError("No timeframes configured for collector")

    logger.info(
        "Kraken collector starting",
        symbols=len(symbols),
        timeframes=timeframes,
        interval_seconds=interval_seconds,
        bootstrap_days=bootstrap_days,
        run_once=run_once,
        dry_run=dry_run,
    )

    client = KrakenClient(
        api_key=cfg.exchange.api_key or "",
        api_secret=cfg.exchange.api_secret or "",
        futures_api_key=cfg.exchange.futures_api_key or "",
        futures_api_secret=cfg.exchange.futures_api_secret or "",
        use_testnet=False,
    )
    await client.initialize()
    acq = DataAcquisition(client, symbols, [])

    cycle = 0
    try:
        while True:
            cycle += 1
            started_at = datetime.now(timezone.utc)
            fetched_pairs, updated_pairs, failed_pairs = await collect_once(
                acq,
                symbols=symbols,
                timeframes=timeframes,
                bootstrap_days=bootstrap_days,
                dry_run=dry_run,
            )
            logger.info(
                "Kraken collector cycle complete",
                cycle=cycle,
                fetched_pairs=fetched_pairs,
                updated_pairs=updated_pairs,
                failed_pairs=failed_pairs,
                started_at=started_at.isoformat(),
            )
            if run_once:
                break
            await asyncio.sleep(max(10, int(interval_seconds)))
    finally:
        await client.close()
        logger.info("Kraken collector stopped", cycles=cycle)


def main() -> None:
    parser = argparse.ArgumentParser(description="Continuously cache Kraken spot candles into DB.")
    parser.add_argument("--symbols", type=str, default="", help="Comma-separated spot symbols")
    parser.add_argument(
        "--timeframes",
        type=str,
        default="15m,1h,4h,1d",
        help="Comma-separated timeframes",
    )
    parser.add_argument("--max-symbols", type=int, default=20, help="Cap symbols when auto-resolving")
    parser.add_argument("--interval-seconds", type=int, default=300, help="Polling interval")
    parser.add_argument(
        "--bootstrap-days",
        type=int,
        default=30,
        help="History window when symbol/timeframe has no cached candles",
    )
    parser.add_argument("--once", action="store_true", help="Run one collection cycle then exit")
    parser.add_argument("--dry-run", action="store_true", help="Log plans without writing")
    args = parser.parse_args()

    asyncio.run(
        run(
            symbols_csv=args.symbols,
            timeframes_csv=args.timeframes,
            max_symbols=args.max_symbols,
            interval_seconds=args.interval_seconds,
            bootstrap_days=args.bootstrap_days,
            run_once=args.once,
            dry_run=args.dry_run,
        )
    )


if __name__ == "__main__":
    main()
