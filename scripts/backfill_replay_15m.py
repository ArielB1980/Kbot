#!/usr/bin/env python3
"""Backfill 15m candle data and export all timeframes to replay CSVs.

Kraken's OHLC API only returns recent ~720 candles for sub-daily timeframes,
so this script uses CoinAPI or Binance (free) for historical data, then
re-exports all timeframes from the DB to replay CSV files.

Steps:
  1. Fetch historical candles from data source into DB
  2. Export all timeframes (15m, 1h, 4h, 1d) from DB to replay CSV files

Usage:
  # Using free Binance source (recommended):
  python scripts/backfill_replay_15m.py \
    --source binance \
    --symbols "BTC/USD,ETH/USD,SOL/USD" \
    --timeframes "15m,1h,4h,1d" \
    --days 400 \
    --data-dir data/replay

  # Using CoinAPI (requires COINAPI_API_KEY):
  python scripts/backfill_replay_15m.py \
    --symbols "BTC/USD,ETH/USD,SOL/USD,XRP/USD,ADA/USD,LINK/USD" \
    --days 400 \
    --data-dir data/replay
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config.config import load_config
from src.data.coinapi_client import CoinAPIClient
from src.data.data_acquisition import DataAcquisition
from src.data.kraken_client import KrakenClient
from src.domain.models import Candle
from src.storage.repository import count_candles, get_candles, save_candles_bulk

# Binance uses USDT pairs; map our USD symbols to their USDT equivalents.
_BINANCE_SYMBOL_MAP: dict[str, str] = {
    "BTC/USD": "BTC/USDT",
    "ETH/USD": "ETH/USDT",
    "SOL/USD": "SOL/USDT",
    "XRP/USD": "XRP/USDT",
    "ADA/USD": "ADA/USDT",
    "LINK/USD": "LINK/USDT",
    "DOGE/USD": "DOGE/USDT",
    "DOT/USD": "DOT/USDT",
    "MATIC/USD": "MATIC/USDT",
    "AVAX/USD": "AVAX/USDT",
}

# ccxt timeframe strings match ours directly (15m, 1h, 4h, 1d)
_BINANCE_MAX_CANDLES = 1000  # Binance limit per request
_BINANCE_RATE_LIMIT_SLEEP = 0.5  # seconds between requests


def _fetch_binance_candles(
    symbol: str,
    timeframe: str,
    start_time: datetime,
    end_time: datetime,
) -> list[Candle]:
    """Fetch OHLCV candles from Binance via ccxt (synchronous).

    Paginates automatically using the ``since`` parameter, fetching up to
    ``_BINANCE_MAX_CANDLES`` bars per request until the full range is covered.

    Args:
        symbol: Our internal symbol, e.g. ``"BTC/USD"``.
        timeframe: Timeframe string, e.g. ``"15m"``, ``"1h"``, ``"4h"``, ``"1d"``.
        start_time: Inclusive start of the desired range (UTC-aware).
        end_time: Exclusive end of the desired range (UTC-aware).

    Returns:
        List of :class:`Candle` objects sorted by timestamp ascending.

    Raises:
        ValueError: If ``symbol`` has no Binance mapping.
        RuntimeError: If ccxt is unavailable.
    """
    try:
        import ccxt  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError(
            "ccxt is not installed. Run: uv add ccxt"
        ) from exc

    binance_symbol = _BINANCE_SYMBOL_MAP.get(symbol)
    if binance_symbol is None:
        raise ValueError(
            f"No Binance mapping for symbol {symbol!r}. "
            f"Known symbols: {list(_BINANCE_SYMBOL_MAP)}"
        )

    exchange = ccxt.binance({"enableRateLimit": False})

    since_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)
    all_candles: list[Candle] = []

    while since_ms < end_ms:
        raw = exchange.fetch_ohlcv(
            binance_symbol,
            timeframe=timeframe,
            since=since_ms,
            limit=_BINANCE_MAX_CANDLES,
        )
        if not raw:
            break

        batch: list[Candle] = []
        for row in raw:
            ts_ms, o, h, low_raw, c, v = row
            if ts_ms >= end_ms:
                break
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
            try:
                candle = Candle(
                    timestamp=ts,
                    symbol=symbol,
                    timeframe=timeframe,
                    open=Decimal(str(o)),
                    high=Decimal(str(h)),
                    low=Decimal(str(low_raw)),
                    close=Decimal(str(c)),
                    volume=Decimal(str(v)),
                )
                batch.append(candle)
            except (ValueError, ArithmeticError) as exc:
                print(f"    [warn] skipping invalid candle at {ts}: {exc}")

        all_candles.extend(batch)

        # Advance the cursor past the last candle we received
        last_ts_ms: int = raw[-1][0]
        if last_ts_ms <= since_ms:
            # Guard against infinite loop if exchange returns the same data
            break
        since_ms = last_ts_ms + 1

        if len(raw) < _BINANCE_MAX_CANDLES:
            # Received fewer than requested — we have reached the end
            break

        time.sleep(_BINANCE_RATE_LIMIT_SLEEP)

    return all_candles


def _write_replay_csv(data_dir: Path, symbol: str, timeframe: str) -> int:
    """Export candles from DB to replay CSV. Returns row count."""
    safe_symbol = symbol.replace("/", "_").replace(":", "_")
    candles_dir = data_dir / "candles"
    candles_dir.mkdir(parents=True, exist_ok=True)
    out = candles_dir / f"{safe_symbol}_{timeframe}.csv"

    candles = get_candles(symbol, timeframe)
    if not candles:
        print(f"  [skip] {symbol} {timeframe}: no candles in DB")
        return 0

    # Sort by timestamp and deduplicate
    candles.sort(key=lambda c: c.timestamp)
    seen: set[str] = set()
    unique: list = []
    for c in candles:
        key = c.timestamp.isoformat()
        if key not in seen:
            seen.add(key)
            unique.append(c)

    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"]
        )
        writer.writeheader()
        for c in unique:
            writer.writerow(
                {
                    "timestamp": c.timestamp.isoformat(),
                    "open": str(c.open),
                    "high": str(c.high),
                    "low": str(c.low),
                    "close": str(c.close),
                    "volume": str(c.volume),
                }
            )

    print(f"  [csv] {symbol} {timeframe}: {len(unique)} rows -> {out}")
    return len(unique)


async def backfill_and_export(
    symbols: list[str],
    days: int,
    data_dir: str,
    timeframes_to_fetch: list[str],
    timeframes_to_export: list[str],
    source: str,
) -> None:
    """Fetch historical data and export all timeframes to CSV."""
    cfg = load_config()
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(days=days)

    print("=== Backfill candles ===")
    print(f"Source: {source}")
    print(f"Symbols: {symbols}")
    print(f"Range: {start_time.date()} -> {end_time.date()} ({days} days)")
    print(f"Fetch timeframes: {timeframes_to_fetch}")
    print(f"Export timeframes: {timeframes_to_export}")
    print()

    if source == "binance":
        # Binance path: fetch from Binance and write directly to replay CSV.
        # No DB needed — candles go straight to disk.
        print("=== Fetching via Binance (free public API) ===")
        out_dir = Path(data_dir)
        candles_dir = out_dir / "candles"
        candles_dir.mkdir(parents=True, exist_ok=True)

        for symbol in symbols:
            for tf in timeframes_to_fetch:
                safe_symbol = symbol.replace("/", "_").replace(":", "_")
                csv_path = candles_dir / f"{safe_symbol}_{tf}.csv"
                print(
                    f"[fetch] {symbol} {tf}...", flush=True
                )
                try:
                    candles = _fetch_binance_candles(
                        symbol=symbol,
                        timeframe=tf,
                        start_time=start_time,
                        end_time=end_time,
                    )
                    # Deduplicate and sort
                    candles.sort(key=lambda c: c.timestamp)
                    seen: set[str] = set()
                    unique: list[Candle] = []
                    for c in candles:
                        key = c.timestamp.isoformat()
                        if key not in seen:
                            seen.add(key)
                            unique.append(c)
                    # Write CSV
                    with csv_path.open("w", newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(
                            f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"]
                        )
                        writer.writeheader()
                        for c in unique:
                            writer.writerow({
                                "timestamp": c.timestamp.isoformat(),
                                "open": str(c.open),
                                "high": str(c.high),
                                "low": str(c.low),
                                "close": str(c.close),
                                "volume": str(c.volume),
                            })
                    print(
                        f"  fetched={len(candles)} unique={len(unique)} -> {csv_path}",
                        flush=True,
                    )
                except Exception as exc:
                    print(f"  FAIL {type(exc).__name__}: {exc}", flush=True)

        print(f"\nDone (Binance). CSVs written to {candles_dir}")
        return
    else:
        # Initialize clients
        client = KrakenClient(
            api_key=cfg.exchange.api_key or "",
            api_secret=cfg.exchange.api_secret or "",
            futures_api_key=cfg.exchange.futures_api_key or "",
            futures_api_secret=cfg.exchange.futures_api_secret or "",
            use_testnet=False,
        )
        await client.initialize()

        coinapi_client = None
        if source == "coinapi":
            coinapi_key = (os.getenv("COINAPI_API_KEY") or "").strip()
            if not coinapi_key:
                raise RuntimeError(
                    "COINAPI_API_KEY env var required for --source coinapi"
                )
            coinapi_client = CoinAPIClient(api_key=coinapi_key)

        acq = DataAcquisition(client, symbols, [], coinapi_client=coinapi_client)

        # Step 1: Fetch into DB
        for symbol in symbols:
            for tf in timeframes_to_fetch:
                before = int(count_candles(symbol, tf))
                print(f"[fetch] {symbol} {tf} (have {before} in DB)...", flush=True)
                try:
                    candles = await acq.fetch_spot_historical(
                        symbol=symbol,
                        timeframe=tf,
                        start_time=start_time,
                        end_time=end_time,
                        source=source,
                    )
                    after = int(count_candles(symbol, tf))
                    gained = max(0, after - before)
                    print(
                        f"  fetched={len(candles)} before={before} after={after} gained={gained}",
                        flush=True,
                    )
                except Exception as exc:
                    print(f"  FAIL {type(exc).__name__}: {exc}", flush=True)

        if coinapi_client:
            await coinapi_client.close()
        await client.close()
    print()

    # Step 2: Export all timeframes from DB to CSV
    print(f"=== Export to replay CSVs ({data_dir}) ===")
    out_dir = Path(data_dir)
    total_rows = 0
    for symbol in symbols:
        print(f"[export] {symbol}")
        for tf in timeframes_to_export:
            total_rows += _write_replay_csv(out_dir, symbol, tf)

    print(f"\nDone. Total CSV rows written: {total_rows:,}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill data and export replay CSVs")
    parser.add_argument(
        "--symbols",
        type=str,
        default="BTC/USD,ETH/USD,SOL/USD,XRP/USD,ADA/USD,LINK/USD",
    )
    parser.add_argument("--days", type=int, default=400)
    parser.add_argument("--data-dir", type=str, default="data/replay")
    parser.add_argument(
        "--source",
        type=str,
        default="coinapi",
        choices=["kraken", "coinapi", "binance"],
        help=(
            "Data source (default: coinapi). "
            "Use 'binance' for free historical data via Binance public API."
        ),
    )
    parser.add_argument(
        "--timeframes",
        type=str,
        default=None,
        help=(
            "Comma-separated timeframes to fetch AND export (e.g. 15m,1h,4h,1d). "
            "When set, overrides --fetch-timeframes and --export-timeframes."
        ),
    )
    parser.add_argument(
        "--fetch-timeframes",
        type=str,
        default="15m",
        help="Comma-separated timeframes to fetch (default: 15m only)",
    )
    parser.add_argument(
        "--export-timeframes",
        type=str,
        default="15m,1h,4h,1d",
        help="Comma-separated timeframes to export to CSV",
    )
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    if args.timeframes is not None:
        # --timeframes is a convenience shorthand: fetch AND export these timeframes.
        unified = [t.strip() for t in args.timeframes.split(",") if t.strip()]
        fetch_tfs = unified
        export_tfs = unified
    else:
        fetch_tfs = [t.strip() for t in args.fetch_timeframes.split(",") if t.strip()]
        export_tfs = [t.strip() for t in args.export_timeframes.split(",") if t.strip()]

    asyncio.run(
        backfill_and_export(
            symbols=symbols,
            days=args.days,
            data_dir=args.data_dir,
            timeframes_to_fetch=fetch_tfs,
            timeframes_to_export=export_tfs,
            source=args.source,
        )
    )


if __name__ == "__main__":
    main()
