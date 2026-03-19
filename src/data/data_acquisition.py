"""
Data acquisition orchestrator for spot and futures market data.

Manages:
- Spot data feeds (for strategy analysis)
- Futures data feeds (for execution monitoring)
- Data validation (no gaps, no duplicates)
- Graceful failure handling
- Storage integration
"""
import asyncio
from typing import Dict, List, Optional, Set
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from src.data.kraken_client import KrakenClient, KrakenWebSocket
from src.data.coinapi_client import CoinAPIClient
from src.data.orderbook import OrderBook
from src.domain.models import Candle
from src.storage.repository import save_candles_bulk
from src.monitoring.logger import get_logger

logger = get_logger(__name__)


class DataAcquisition:
    """
    Orchestrates spot and futures data feeds.
    
    Design pattern:
    - Spot data: for strategy signal generation
    - Futures data: for execution, position, margin monitoring only
    """
    
    def __init__(
        self,
        kraken_client: KrakenClient,
        spot_symbols: List[str],
        futures_symbols: List[str],
        max_gap_seconds: int = 60,
        coinapi_client: Optional[CoinAPIClient] = None,
    ):
        """
        Initialize data acquisition.
        
        Args:
            kraken_client: Kraken REST/WebSocket client
            spot_symbols: Spot symbols for strategy analysis (e.g., ["BTC/USD", "ETH/USD"])
            futures_symbols: Futures symbols for execution (e.g., ["BTCUSD-PERP", "ETHUSD-PERP"])
            max_gap_seconds: Maximum acceptable gap in data (alerts if exceeded)
        """
        self.kraken_client = kraken_client
        self.spot_symbols = spot_symbols
        self.futures_symbols = futures_symbols
        self.max_gap_seconds = max_gap_seconds
        self.coinapi_client = coinapi_client
        
        # Order books for futures mark price tracking
        self.orderbooks: Dict[str, OrderBook] = {
            symbol: OrderBook(symbol) for symbol in futures_symbols
        }
        
        # Last candle timestamps for gap detection
        self.last_candle_times: Dict[tuple, datetime] = {}  # (symbol, timeframe) -> timestamp
        
        # Running state
        self.running = False
        self.data_feed_healthy = True
        
        logger.info(
            "DataAcquisition initialized",
            spot_symbols=spot_symbols,
            futures_symbols=futures_symbols,
        )
        
    def update_symbols(self, spot_symbols: List[str], futures_symbols: List[str]):
        """
        Update the list of tracked symbols dynamically.
        
        Args:
            spot_symbols: New list of spot symbols
            futures_symbols: New list of futures symbols
        """
        self.spot_symbols = spot_symbols
        self.futures_symbols = futures_symbols
        
        # Initialize new orderbooks (if missing)
        for symbol in futures_symbols:
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = OrderBook(symbol)
                
        logger.info(
            "DataAcquisition symbols updated",
            spot_count=len(spot_symbols),
            futures_count=len(futures_symbols)
        )
    
    async def start(self):
        """Start data acquisition."""
        self.running = True
        logger.info("Data acquisition started")
        
        # Start background tasks (non-blocking)
        asyncio.create_task(self._monitor_data_gaps())
    
    async def stop(self):
        """Stop data acquisition."""
        self.running = False
        logger.info("Data acquisition stopped")
    
    async def fetch_spot_historical(
        self,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
        source: str = "kraken",
    ) -> List[Candle]:
        """
        Fetch historical spot OHLCV data.
        
        Args:
            symbol: Spot symbol (e.g., "BTC/USD")
            timeframe: Timeframe (e.g., "15m", "1h", "4h", "1d")
            start_time: Start time (inclusive)
            end_time: End time (inclusive)
        
        Returns:
            List of Candle objects
        """
        logger.info(
            "Fetching historical spot data",
            symbol=symbol,
            timeframe=timeframe,
            start=start_time.isoformat(),
            end=end_time.isoformat(),
            source=source,
        )

        normalized_source = str(source or "kraken").strip().lower()
        if normalized_source == "coinapi":
            if not self.coinapi_client:
                raise ValueError("CoinAPI source selected but coinapi_client is not configured")
            candles = await self.coinapi_client.fetch_spot_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                start_time=start_time,
                end_time=end_time,
            )
            if candles:
                self._validate_candles(candles, timeframe)
                save_candles_bulk(candles)
            logger.info(
                "Historical spot data fetched",
                symbol=symbol,
                timeframe=timeframe,
                count=len(candles),
                source=normalized_source,
            )
            return candles
        
        all_candles = []
        current_time = start_time
        
        # Fetch in chunks (Kraken limits to ~720 candles per request)
        while current_time <end_time:
            since_ms = int(current_time.timestamp() * 1000)
            
            try:
                candles = await self.kraken_client.get_spot_ohlcv(
                    symbol, timeframe, since=since_ms, limit=720
                )
                
                if not candles:
                    break
                
                # Validate no gaps
                self._validate_candles(candles, timeframe)
                
                # Store via bulk upsert (idempotent for backfill / retries)
                save_candles_bulk(candles)
                all_candles.extend(candles)
                
                # Update current time for next chunk
                current_time = candles[-1].timestamp + timedelta(minutes=self._timeframe_to_minutes(timeframe))
                
            except Exception as e:
                logger.error(
                    "Failed to fetch historical data",
                    symbol=symbol,
                    timeframe=timeframe,
                    error=str(e),
                )
                raise
        
        logger.info(
            "Historical spot data fetched",
            symbol=symbol,
            timeframe=timeframe,
            count=len(all_candles),
        )
        
        return all_candles
    
    def _validate_candles(self, candles: List[Candle], timeframe: str):
        """
        Validate candles for gaps and duplicates.
        
        Args:
            candles: List of candles to validate
            timeframe: Expected timeframe
        
        Raises:
            ValueError: If validation fails
        """
        if len(candles) < 2:
            return
        
        expected_delta = timedelta(minutes=self._timeframe_to_minutes(timeframe))
        
        for i in range(1, len(candles)):
            actual_delta = candles[i].timestamp - candles[i-1].timestamp
            
            # Check for duplicates
            if actual_delta == timedelta(0):
                raise ValueError(
                    f"Duplicate candle timestamps: {candles[i].timestamp}"
                )
            
            # Check for gaps
            if actual_delta != expected_delta:
                logger.warning(
                    "Gap detected in candle data",
                    symbol=candles[i].symbol,
                    timeframe=timeframe,
                    expected_delta=str(expected_delta),
                    actual_delta=str(actual_delta),
                    timestamp=candles[i].timestamp.isoformat(),
                )
    
    async def _monitor_data_gaps(self):
        """Monitor for data gaps and alert if exceeded."""
        while self.running:
            await asyncio.sleep(self.max_gap_seconds)
            
            now = datetime.now(timezone.utc)
            
            for (symbol, timeframe), last_time in self.last_candle_times.items():
                gap = now - last_time
                
                if gap.total_seconds() > self.max_gap_seconds:
                    logger.error(
                        "Data gap exceeded threshold",
                        symbol=symbol,
                        timeframe=timeframe,
                        gap_seconds=gap.total_seconds(),
                        max_gap_seconds=self.max_gap_seconds,
                    )
                    
                    # Set data feed unhealthy
                    self.data_feed_healthy = False
    
    def get_mark_price(self, futures_symbol: str) -> Optional[Decimal]:
        """
        Get current mark price for futures symbol.
        
        Args:
            futures_symbol: Futures symbol (e.g., "BTCUSD-PERP")
        
        Returns:
            Mark price or None if not available
        """
        orderbook = self.orderbooks.get(futures_symbol)
        return orderbook.get_mark_price() if orderbook else None
    
    def is_healthy(self) -> bool:
        """Check if data feed is healthy."""
        return self.data_feed_healthy
    
    # ===== Multi-Asset Support (NEW) =====
    
    async def fetch_candles_for_all(self, symbols: List[str], timeframes: List[str] = None):
        """
        Fetch candles for multiple symbols in parallel.
        
        Args:
            symbols: List of spot symbols
            timeframes: List of timeframes (default: from config)
        
        Returns:
            Dict[symbol][timeframe] -> List[Candle]
        """
        if timeframes is None:
            timeframes = ["15m", "1h", "4h", "1d"]
        
        tasks = []
        for symbol in symbols:
            for tf in timeframes:
                tasks.append(self.fetch_spot_historical(
                    symbol, tf,
                    datetime.now(timezone.utc) - timedelta(days=365),
                    datetime.now(timezone.utc)
                ))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Organize results
        candle_buffers = {}
        idx = 0
        for symbol in symbols:
            candle_buffers[symbol] = {}
            for tf in timeframes:
                result = results[idx]
                if isinstance(result, Exception):
                    logger.error(f"Failed to fetch {symbol} {tf}", error=str(result))
                    candle_buffers[symbol][tf] = []
                else:
                    candle_buffers[symbol][tf] = result
                idx += 1
        
        return candle_buffers
    
    @staticmethod
    def _timeframe_to_minutes(timeframe: str) -> int:
        """Convert timeframe string to minutes."""
        mapping = {
            "1m": 1,
            "5m": 5,
            "15m": 15,
            "30m": 30,
            "1h": 60,
            "4h": 240,
            "1d": 1440,
        }
        return mapping.get(timeframe, 60)  # Default to 1h
