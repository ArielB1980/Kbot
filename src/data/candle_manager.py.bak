import asyncio
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Any
from collections import defaultdict

from src.monitoring.logger import get_logger
from src.domain.models import Candle
from src.data.kraken_client import KrakenClient
from src.storage.repository import load_candles_map, save_candles_bulk, get_latest_candle_timestamp
from src.exceptions import OperationalError, DataError

logger = get_logger(__name__)


def _candles_with_symbol(candles: List[Candle], symbol: str) -> List[Candle]:
    """Return new Candles with symbol overwritten (for futures fallback stored under spot)."""
    out: List[Candle] = []
    for c in candles:
        out.append(Candle(
            timestamp=c.timestamp,
            symbol=symbol,
            timeframe=c.timeframe,
            open=c.open,
            high=c.high,
            low=c.low,
            close=c.close,
            volume=c.volume,
        ))
    return out


class CandleManager:
    """
    Manages fetching, caching, and persistence of candle data.
    When spot OHLCV is unavailable, can use futures OHLCV for signal analysis (use_futures_fallback).
    """
    def __init__(
        self,
        client: KrakenClient,
        spot_to_futures: Optional[Callable[[str], str]] = None,
        use_futures_fallback: bool = False,
        ohlcv_fetcher: Optional[Any] = None,
    ):
        self.client = client
        self.spot_to_futures = spot_to_futures
        self.use_futures_fallback = use_futures_fallback
        self.ohlcv_fetcher = ohlcv_fetcher
        self._futures_fallback_symbols: set = set()  # Symbols that used futures OHLCV (cleared each summary)
        # Storage: timeframe -> symbol -> list[Candle]
        self.candles: Dict[str, Dict[str, List[Candle]]] = {
            "15m": {},
            "1h": {},
            "4h": {},
            "1d": {}
        }
        # Last update tracking: symbol -> timeframe -> datetime
        self.last_candle_update: Dict[str, Dict[str, datetime]] = {}
        # WS feed freshness tracking: "symbol:timeframe" -> datetime
        self._ws_last_update: Dict[str, datetime] = {}
        # Persistence queue
        self.pending_candles: List[Candle] = []

    async def initialize(self, markets: List[str]):
        """Bulk load history from database."""
        logger.info("Hydrating candle cache from database...", symbol_count=len(markets))

        pre_existing_15m = sum(1 for s in markets if self.candles["15m"].get(s))
        if pre_existing_15m:
            logger.warning(
                "Cache already populated before DB hydration",
                symbols_with_data=pre_existing_15m,
                sample={s: len(self.candles["15m"].get(s, [])) for s in sorted(markets)[:3]},
            )

        res_15m = await asyncio.to_thread(load_candles_map, markets, "15m", days=14)
        for s, cands in res_15m.items():
            if s not in self.candles["15m"]: self.candles["15m"][s] = cands
            else: self._merge_candles(s, "15m", cands)

        res_1h = await asyncio.to_thread(load_candles_map, markets, "1h", days=60)
        for s, cands in res_1h.items():
            if s not in self.candles["1h"]: self.candles["1h"][s] = cands
            else: self._merge_candles(s, "1h", cands)
        
        res_4h = await asyncio.to_thread(load_candles_map, markets, "4h", days=180)
        for s, cands in res_4h.items():
            if s not in self.candles["4h"]: self.candles["4h"][s] = cands
            else: self._merge_candles(s, "4h", cands)
        
        res_1d = await asyncio.to_thread(load_candles_map, markets, "1d", days=365)
        for s, cands in res_1d.items():
            if s not in self.candles["1d"]: self.candles["1d"][s] = cands
            else: self._merge_candles(s, "1d", cands)
        
        # Initialize update trackers
        now = datetime.now(timezone.utc)
        for symbol in markets:
            self.last_candle_update[symbol] = {
                "15m": self.candles["15m"].get(symbol, [])[-1].timestamp if self.candles["15m"].get(symbol) else datetime.min.replace(tzinfo=timezone.utc),
                "1h": self.candles["1h"].get(symbol, [])[-1].timestamp if self.candles["1h"].get(symbol) else datetime.min.replace(tzinfo=timezone.utc),
                "4h": self.candles["4h"].get(symbol, [])[-1].timestamp if self.candles["4h"].get(symbol) else datetime.min.replace(tzinfo=timezone.utc),
                "1d": self.candles["1d"].get(symbol, [])[-1].timestamp if self.candles["1d"].get(symbol) else datetime.min.replace(tzinfo=timezone.utc),
            }

        # Hydration summary: helps explain "only N coins with sufficient candles"
        sufficient_15m = sum(1 for s in markets if len(self.candles["15m"].get(s, [])) >= 50)
        zero_15m = sum(1 for s in markets if len(self.candles["15m"].get(s, [])) == 0)
        logger.info(
            "Hydration complete",
            total=len(markets),
            with_sufficient_15m=sufficient_15m,
            with_zero_15m=zero_15m,
            hint="Run backfill against this DB if most have zero; ensure universe matches live discovery.",
        )

    def _merge_candles(self, symbol: str, timeframe: str, new_candles: List[Candle]):
        """Merge candles into cache, keeping the larger historical window.

        When the incoming set is significantly larger than the existing cache
        (e.g. DB hydration vs a handful of WS-streamed bars), use the incoming
        set as the base and append any newer existing candles on top.
        """
        buffer = self.candles[timeframe]
        if symbol not in buffer:
            buffer[symbol] = new_candles
            return
            
        existing = buffer[symbol]
        if not existing:
            buffer[symbol] = new_candles
            return
        
        if not new_candles:
            return

        if len(new_candles) > len(existing) * 2:
            last_new_ts = new_candles[-1].timestamp
            tail = [c for c in existing if c.timestamp > last_new_ts]
            merged = list(new_candles)
            if tail:
                merged.extend(tail)
            buffer[symbol] = merged[-2000:]
            return
            
        last_ts = existing[-1].timestamp
        to_append = [c for c in new_candles if c.timestamp > last_ts]
        if to_append:
            existing.extend(to_append)
        
        if len(existing) > 2000:
            buffer[symbol] = existing[-2000:]

    def get_candles(self, symbol: str, timeframe: str) -> List[Candle]:
        """Get cached candles."""
        return self.candles.get(timeframe, {}).get(symbol, [])

    def has_fresh_ws_data(self, symbol: str, timeframe: str, max_age_seconds: float = 1800) -> bool:
        """Check if the WS feed has delivered fresh data for this symbol/timeframe.
        
        Used to skip redundant REST fetches when the WS feed is active and current.
        """
        last_ws = self._ws_last_update.get(f"{symbol}:{timeframe}")
        if last_ws is None:
            return False
        age = (datetime.now(timezone.utc) - last_ws).total_seconds()
        return age < max_age_seconds

    def receive_ws_candle(self, symbol: str, timeframe: str, candle: Candle) -> None:
        """Ingest a single candle from the WebSocket feed.

        If the candle's timestamp matches the latest cached bar, the bar is
        *updated* in place (WS sends progressive updates for the current bar).
        If it's newer, it's appended.  Older candles are ignored.

        Does **not** enqueue into ``pending_candles`` -- persistence is handled
        by the REST-based ``flush_pending`` path to avoid duplicate DB writes.
        """
        if timeframe not in self.candles:
            return

        buf = self.candles[timeframe]
        existing = buf.get(symbol)

        if not existing:
            buf[symbol] = [candle]
            return

        last = existing[-1]
        if candle.timestamp == last.timestamp:
            existing[-1] = candle  # update current bar in place
            self._ws_last_update[f"{symbol}:{timeframe}"] = datetime.now(timezone.utc)
        elif candle.timestamp > last.timestamp:
            existing.append(candle)
            if len(existing) > 2000:
                buf[symbol] = existing[-2000:]
            self._ws_last_update[f"{symbol}:{timeframe}"] = datetime.now(timezone.utc)
        # else: older than latest -- ignore

    def get_futures_fallback_count(self) -> int:
        """Return number of symbols that used futures OHLCV since last pop (no clear)."""
        return len(self._futures_fallback_symbols)

    def pop_futures_fallback_count(self) -> int:
        """Return number of symbols that used futures OHLCV since last call, then clear."""
        n = len(self._futures_fallback_symbols)
        self._futures_fallback_symbols.clear()
        return n

    async def update_candles(self, symbol: str):
        """Update candles for a symbol (Incremental). Uses spot OHLCV; if unavailable and use_futures_fallback, uses futures OHLCV."""
        now = datetime.now(timezone.utc)
        if symbol not in self.last_candle_update:
            self.last_candle_update[symbol] = {}

        async def fetch_tf(tf: str, interval_min: int):
            last_update = self.last_candle_update[symbol].get(tf, datetime.min.replace(tzinfo=timezone.utc))
            elapsed = (now - last_update).total_seconds()

            # WS-aware skip: if WS feed has fresh data for this symbol/tf,
            # skip REST fetch (saves API calls and latency).
            # WS only covers 15m; 1h/4h/1d still use REST exclusively.
            if tf == "15m" and self.has_fresh_ws_data(symbol, tf, max_age_seconds=1800):
                # WS is feeding us, but still do REST occasionally (every 30 min)
                # to persist candles to DB and catch any WS gaps.
                if elapsed < 1800:
                    return
            
            # Smart candle-boundary caching: only refetch when a new bar has likely closed.
            if elapsed < 30:
                return  # Hard floor: never refetch within 30 seconds
            
            if elapsed < (interval_min * 60):
                # Within the normal throttle. But check if a new bar boundary crossed.
                # If yes, we should refetch to get the newly closed candle.
                tf_minutes = {"15m": 15, "1h": 60, "4h": 240, "1d": 1440}.get(tf, interval_min)
                last_boundary_min = (now.minute // tf_minutes) * tf_minutes if tf_minutes <= 60 else 0
                
                # For sub-hourly timeframes, check if minute boundary crossed
                if tf_minutes <= 60:
                    # Current bar start
                    current_bar_start = now.replace(
                        minute=last_boundary_min, second=0, microsecond=0
                    )
                    # Did we fetch BEFORE this bar started? If so, a new bar closed.
                    if last_update >= current_bar_start:
                        return  # Already fetched this bar period
                # For 4h and 1d, the simple time throttle is fine
                else:
                    return
            buffer = self.candles[tf]
            last_ts = None
            existing = buffer.get(symbol, [])
            if existing:
                last_ts = existing[-1].timestamp
            else:
                last_ts = await asyncio.to_thread(get_latest_candle_timestamp, symbol, tf)
            since_ms = int(last_ts.replace(tzinfo=timezone.utc).timestamp() * 1000) if last_ts else None

            candles: List[Candle] = []
            used_futures = False
            data_source = "spot"

            try:
                if self.ohlcv_fetcher:
                    candles = await self.ohlcv_fetcher.fetch_spot_ohlcv(symbol, tf, since_ms, 300)
                    if not isinstance(candles, list):
                        candles = []
                else:
                    candles = await self.client.get_spot_ohlcv(symbol, tf, since=since_ms, limit=300)
            except (OperationalError, DataError) as e:
                logger.debug("Spot OHLCV fetch failed", symbol=symbol, tf=tf, error=str(e))
                candles = []

            if not candles and self.use_futures_fallback and self.spot_to_futures:
                try:
                    fsym = self.spot_to_futures(symbol)
                    raw = await self.client.get_futures_ohlcv(fsym, tf, since=since_ms, limit=300)
                    if raw:
                        candles = _candles_with_symbol(raw, symbol)
                        used_futures = True
                        data_source = "futures_fallback"
                except (OperationalError, DataError) as e:
                    logger.debug("Futures OHLCV fallback failed", symbol=symbol, tf=tf, error=str(e))

            if not candles:
                if not used_futures:
                    logger.warning(f"No candles for {symbol} {tf} (spot failed, futures fallback skipped or empty)")
                return

            if used_futures:
                self._futures_fallback_symbols.add(symbol)
                logger.debug(f"Using futures OHLCV for {symbol} {tf}", count=len(candles))
            
            # Rate-limited log for candle data source (log once per symbol per 5 minutes)
            # Only log for 15m timeframe to avoid spam
            if tf == "15m":
                # Use a simple rate limit: log if last log was > 5 minutes ago
                # Store last log time per symbol in a simple dict (cleared periodically)
                if not hasattr(self, '_last_source_log'):
                    self._last_source_log = {}
                
                last_log = self._last_source_log.get(symbol, datetime.min.replace(tzinfo=timezone.utc))
                if (now - last_log).total_seconds() > 300:  # 5 minutes
                    logger.info(
                        "Candle data source",
                        symbol=symbol,
                        timeframe=tf,
                        source=data_source,
                        count=len(candles),
                    )
                    self._last_source_log[symbol] = now

            self.last_candle_update[symbol][tf] = now
            if not existing:
                buffer[symbol] = candles
            else:
                last_buffer_ts = existing[-1].timestamp
                new_candles = [c for c in candles if c.timestamp > last_buffer_ts]
                if new_candles:
                    buffer[symbol].extend(new_candles)
                if len(buffer[symbol]) > 2000:
                    buffer[symbol] = buffer[symbol][-2000:]
            if candles:
                self.pending_candles.extend(candles)

        await asyncio.gather(
            fetch_tf("15m", 1),
            fetch_tf("1h", 5),
            fetch_tf("4h", 15),
            fetch_tf("1d", 60),
        )

    async def flush_pending(self):
        """Flush pending candles to DB."""
        if not self.pending_candles:
            return

        # Snapshot the batch to avoid clearing new data if this batch fails
        batch = list(self.pending_candles)
        
        try:
            # Group by symbol/tf for efficient upsert
            grouped = defaultdict(list)
            for candle in batch:
                key = (candle.symbol, candle.timeframe)
                grouped[key].append(candle)
            
            save_tasks = []
            for _, candle_group in grouped.items():
                save_tasks.append(asyncio.to_thread(save_candles_bulk, candle_group))
            
            if save_tasks:
                results = await asyncio.gather(*save_tasks, return_exceptions=True)
                
                # Check for individual task failures
                failures = [r for r in results if isinstance(r, Exception)]
                if failures:
                    logger.error("Some candle batches failed to save", count=len(failures), error=str(failures[0]))
                    # If any batch fails, we don't clear the queue to prevent data loss.
                    # Duplicates are handled by DB-level Upsert.
                    return 

            # Only clear the items we successfully attempted to save
            # This is safer than clear() if new items were added during the await
            processed_ids = {(c.symbol, c.timeframe, c.timestamp) for c in batch}
            self.pending_candles = [c for c in self.pending_candles if (c.symbol, c.timeframe, c.timestamp) not in processed_ids]
            
            if batch:
                logger.debug("Batched save complete", candles_saved=len(batch))
        except (OperationalError, DataError, OSError) as e:
            logger.error("Failed to batch save candles", error=str(e), error_type=type(e).__name__)
            # Keep pending_candles for next attempt
