"""
Kraken REST API and WebSocket client for spot and futures markets.

Handles:
- REST API calls (spot and futures)
- Authentication (API key + HMAC signature)
- Rate limiting (token bucket)
- WebSocket connections (spot and futures)
- Reconnection with exponential backoff
"""
import ccxt
import ccxt.async_support as ccxt_async
import hashlib
import hmac
import base64
import os
import time
import asyncio
import json
import websockets
import aiohttp
import certifi
import ssl
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime, timezone
from decimal import Decimal
from collections import deque
from dataclasses import dataclass
from src.monitoring.logger import get_logger
from src.domain.models import Candle
from src.data.fiat_currencies import has_disallowed_base, is_disallowed_trading_base
from src.constants import (
    PUBLIC_API_CAPACITY,
    PUBLIC_API_REFILL_RATE,
    PRIVATE_API_CAPACITY,
    PRIVATE_API_REFILL_RATE,
    KRAKEN_FUTURES_BASE_URL,
)
from src.exceptions import (
    APIError,
    AuthenticationError,
    CircuitOpenError,
    DataError,
    InvariantError,
    OperationalError,
    RateLimitError,
)
from src.utils.circuit_breaker import APICircuitBreaker
from src.utils.retry import retry_on_transient_errors

logger = get_logger(__name__)


def _extract_venue_error(exc: Exception) -> tuple:
    """Extract venue error code and message from Kraken/ccxt exception. Returns (code, message)."""
    code, msg = "UNKNOWN", str(exc)
    try:
        import json
        if hasattr(exc, "response") and exc.response is not None:
            r = getattr(exc, "response", {}) or {}
            errs = r.get("errors") or r.get("error") or []
            if isinstance(errs, list) and errs and isinstance(errs[0], dict):
                code = str(errs[0].get("code", code))
                msg = str(errs[0].get("message", msg))
                return (code, msg)
        s = str(exc)
        if "{" in s and "errors" in s.lower():
            start, end = s.find("{"), s.rfind("}") + 1
            if start >= 0 and end > start:
                data = json.loads(s[start:end])
                errs = data.get("errors") or []
                if errs and isinstance(errs[0], dict):
                    code = str(errs[0].get("code", code))
                    msg = str(errs[0].get("message", msg))
    except Exception:
        pass
    return (code, msg)


@dataclass
class RateLimiter:
    """Token bucket rate limiter."""
    capacity: int  # Maximum tokens
    refill_rate: float  # Tokens per second
    tokens: float
    last_refill: float
    
    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = float(capacity)
        self.last_refill = time.time()
    
    def consume(self, tokens: int = 1) -> bool:
        """
        Try to consume tokens.
        
        Returns:
            True if tokens consumed, False if insufficient tokens
        """
        self._refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False
    
    def _refill(self):
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now
    
    async def wait_for_token(self):
        """Wait until a token is available."""
        while not self.consume(1):
            await asyncio.sleep(0.1)


@dataclass
class FuturesTicker:
    """Full futures ticker data for market discovery filtering."""
    symbol: str  # Raw symbol (e.g., "PI_ETHUSD")
    mark_price: Decimal
    bid: Decimal
    ask: Decimal
    volume_24h: Decimal  # 24h volume in quote currency
    open_interest: Decimal  # Open interest in contracts/notional
    funding_rate: Optional[Decimal]  # Current funding rate (perpetuals only)
    
    @property
    def spread_pct(self) -> Decimal:
        """Calculate bid-ask spread as percentage."""
        if self.bid > 0:
            return (self.ask - self.bid) / self.bid
        return Decimal("1")  # Fallback: 100% spread if no bid


class KrakenClient:
    """
    Kraken REST API client for spot and futures markets.
    """
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        futures_api_key: Optional[str] = None,
        futures_api_secret: Optional[str] = None,
        use_testnet: bool = False,
        *,
        market_cache_minutes: int = 60,
        dry_run: bool = False,
        breaker_failure_threshold: int = 5,
        breaker_rate_limit_threshold: int = 2,
        breaker_cooldown_seconds: float = 60.0,
    ):
        """
        Initialize Kraken client.

        Args:
            api_key: Kraken spot API key
            api_secret: Kraken spot API secret
            futures_api_key: Kraken Futures API key (optional)
            futures_api_secret: Kraken Futures API secret (optional)
            use_testnet: Use testnet
            market_cache_minutes: TTL for get_spot_markets/get_futures_markets cache (default 60)
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.futures_api_key = futures_api_key
        self.futures_api_secret = futures_api_secret
        self.use_testnet = use_testnet
        self._market_cache_minutes = max(1, market_cache_minutes)
        self._dry_run = dry_run
        if dry_run:
            logger.warning("KrakenClient initialized in DRY_RUN mode — order placement will be refused")

        # Helper to sanitize base64 secrets
        def sanitize_secret(secret: str) -> str:
            if not secret: return secret
            s = secret.strip()
            # Standardize padding
            return s + '=' * (-len(s) % 4)

        if self.api_secret:
            self.api_secret = sanitize_secret(self.api_secret)
        if self.futures_api_secret:
            self.futures_api_secret = sanitize_secret(self.futures_api_secret)

        if self.futures_api_secret:
            self.futures_api_secret = sanitize_secret(self.futures_api_secret)

        self.exchange = None
        self.futures_exchange = None

        # Markets cache for get_spot_markets / get_futures_markets (avoids spamming load_markets)
        self._markets_cache: Dict[str, tuple] = {}  # "spot" -> (ts, data), "futures" -> (ts, data)
        self._markets_lock = asyncio.Lock()

        # Rate limiters (configurable per endpoint group)
        self.public_limiter = RateLimiter(capacity=PUBLIC_API_CAPACITY, refill_rate=PUBLIC_API_REFILL_RATE)
        self.private_limiter = RateLimiter(capacity=PRIVATE_API_CAPACITY, refill_rate=PRIVATE_API_REFILL_RATE)

        # Reusable SSL context
        self._ssl_context = None
        
        # Persistent HTTP session for connection reuse (created lazily)
        self._http_session: Optional[aiohttp.ClientSession] = None

        # API-level circuit breaker (P2.1) — guards all outbound calls
        self._api_breaker = APICircuitBreaker(
            failure_threshold=breaker_failure_threshold,
            rate_limit_threshold=breaker_rate_limit_threshold,
            cooldown_seconds=breaker_cooldown_seconds,
            name="kraken_api",
        )

        logger.info("Kraken client configuration loaded")
    
    async def _get_http_session(self) -> aiohttp.ClientSession:
        """Get or create a persistent aiohttp.ClientSession for connection reuse."""
        if self._http_session is None or self._http_session.closed:
            connector = aiohttp.TCPConnector(
                ssl=self._get_ssl_context(),
                limit=20,  # max concurrent connections
                ttl_dns_cache=300,  # DNS cache TTL
            )
            timeout = aiohttp.ClientTimeout(total=30)
            self._http_session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
            )
        return self._http_session

    async def close_http_session(self) -> None:
        """Close the persistent HTTP session. Call on shutdown."""
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None

    # -- Circuit-breaker guarded call (P2.1) --------------------------------

    async def _guarded_call(self, coro, *, label: str = "api_call"):
        """Execute an async coroutine through the API circuit breaker.

        1. Check breaker — raises CircuitOpenError if OPEN
        2. Await the coroutine
        3. Record success / failure and classify the exception

        All public API methods should delegate through this.
        """
        await self._api_breaker.can_execute()  # raises CircuitOpenError if open
        try:
            result = await coro
            await self._api_breaker.record_success()
            return result
        except Exception as exc:
            classified = self._classify_exception(exc)
            is_rate_limit = isinstance(classified, RateLimitError)
            # Only record breaker-triggering failures (OperationalError family)
            if isinstance(classified, OperationalError) and not isinstance(classified, CircuitOpenError):
                await self._api_breaker.record_failure(exc, is_rate_limit=is_rate_limit)
            # Re-raise the classified exception
            raise classified from exc

    @staticmethod
    def _classify_exception(exc: Exception) -> Exception:
        """Map raw ccxt / aiohttp / stdlib exceptions to our hierarchy.

        Rules:
          - 5xx, timeout, connection reset, DNS -> OperationalError
          - 429 rate-limit -> RateLimitError (subclass of OperationalError)
          - Bad symbol, invalid order, min size -> DataError
          - Auth failure -> InvariantError (halt)
          - Already one of our types -> pass through
        """
        # Already classified
        if isinstance(exc, (OperationalError, DataError, InvariantError)):
            return exc

        exc_str = str(exc).lower()

        # --- Auth (escalate to halt) ---
        if isinstance(exc, getattr(ccxt, "AuthenticationError", type(None))):
            return InvariantError(f"Exchange auth failure: {exc}")

        # --- Rate limit (fast-trigger breaker) ---
        if isinstance(exc, getattr(ccxt, "RateLimitExceeded", type(None))):
            return RateLimitError(f"Rate limit: {exc}")
        if "429" in exc_str or "rate limit" in exc_str or "too many requests" in exc_str:
            return RateLimitError(f"Rate limit: {exc}")

        # --- Network / transient (breaker-triggering) ---
        # P1.1: Narrow OSError to networky subclasses only.
        # FileNotFoundError, PermissionError, etc. are deployment bugs, not transient.
        _NETWORK_OSERRORS = (
            ConnectionError,       # includes ConnectionResetError, etc.
            BrokenPipeError,
            ConnectionResetError,
            ConnectionRefusedError,
            ConnectionAbortedError,
        )
        if isinstance(exc, (asyncio.TimeoutError, aiohttp.ClientError)):
            return OperationalError(f"Network error: {exc}")
        if isinstance(exc, _NETWORK_OSERRORS):
            return OperationalError(f"Network error: {exc}")
        if isinstance(exc, OSError) and getattr(exc, "errno", None) in (
            # errno values for network-related OS errors
            110,   # ETIMEDOUT
            111,   # ECONNREFUSED
            113,   # EHOSTUNREACH
            101,   # ENETUNREACH
            104,   # ECONNRESET
        ):
            return OperationalError(f"Network OS error (errno={exc.errno}): {exc}")
        # Non-network OSError (FileNotFoundError, PermissionError) — let it crash
        if isinstance(exc, getattr(ccxt, "NetworkError", type(None))):
            return OperationalError(f"CCXT network error: {exc}")
        if isinstance(exc, getattr(ccxt, "ExchangeNotAvailable", type(None))):
            return OperationalError(f"Exchange unavailable: {exc}")
        if isinstance(exc, getattr(ccxt, "RequestTimeout", type(None))):
            return OperationalError(f"Request timeout: {exc}")

        # P1.1: 5xx detection — only from CCXT structured errors, NOT string matching.
        # Removed: `for code in ("500", "502", ...): if code in exc_str:` — too many false positives.
        # CCXT wraps 5xx responses as ExchangeNotAvailable (handled above) or ExchangeError.
        # For non-CCXT errors with HTTP status in the message, we check structured pattern only.
        import re
        if re.search(r"\bHTTP\s+5\d{2}\b", str(exc), re.IGNORECASE):
            return OperationalError(f"HTTP 5xx: {exc}")

        # --- Data / business errors (skip symbol, don't trip breaker) ---
        if isinstance(exc, getattr(ccxt, "BadSymbol", type(None))):
            return DataError(f"Bad symbol: {exc}")
        if isinstance(exc, getattr(ccxt, "InvalidOrder", type(None))):
            return DataError(f"Invalid order: {exc}")
        if isinstance(exc, getattr(ccxt, "InsufficientFunds", type(None))):
            return DataError(f"Insufficient funds: {exc}")
        if isinstance(exc, getattr(ccxt, "BadRequest", type(None))):
            return DataError(f"Bad request: {exc}")
        for keyword in ("does not have market", "invalid symbol", "min size",
                        "minimum", "insufficient", "not found"):
            if keyword in exc_str:
                return DataError(f"Business error: {exc}")

        # --- ccxt.ExchangeError fallback ---
        # P1.1: Only known-transient ccxt errors count as OperationalError.
        # Unknown ExchangeError → DataError (won't trip breaker).
        if isinstance(exc, getattr(ccxt, "DDoSProtection", type(None))):
            return OperationalError(f"DDoS protection: {exc}")
        if isinstance(exc, getattr(ccxt, "ExchangeError", type(None))):
            # Unknown exchange error — treat as DataError (not breaker-triggering)
            return DataError(f"Exchange error (unclassified): {exc}")

        # Unknown — don't wrap. Let it crash (systemd restarts).
        return exc

    @property
    def api_breaker(self) -> APICircuitBreaker:
        """Expose breaker for metrics / health endpoints."""
        return self._api_breaker

    def has_valid_spot_credentials(self) -> bool:
        """Check if spot API keys are present."""
        return bool(self.api_key and self.api_secret and not self.api_key.startswith("${"))

    def has_valid_futures_credentials(self) -> bool:
        """Check if futures API keys are present."""
        return bool(self.futures_api_key and self.futures_api_secret and not self.futures_api_key.startswith("${"))

    async def initialize(self):
        """
        Lazy initialization of CCXT exchanges.
        MUST be called inside the running event loop of the target process.
        """
        # Initialize CCXT exchange (Spot - ASYNC)
        if not self.exchange:
            self.exchange = ccxt_async.kraken({
                'apiKey': self.api_key,
                'secret': self.api_secret,
                'enableRateLimit': True,
                'timeout': 30000,
            })

        # Initialize CCXT Futures Exchange (Futures - Async)
        if self.futures_api_key and self.futures_api_secret and not self.futures_exchange:
            self.futures_exchange = ccxt_async.krakenfutures({
                'apiKey': self.futures_api_key,
                'secret': self.futures_api_secret,
                'enableRateLimit': True,
                'timeout': 30000,
                'options': {'defaultType': 'future'},
            })
            if self.futures_exchange and self.use_testnet:
                 self.futures_exchange.set_sandbox_mode(True)
        
        logger.info("KrakenClient initialized (Lazy)")
        
        # Rate limiters (configurable per endpoint group)
        self.public_limiter = RateLimiter(capacity=PUBLIC_API_CAPACITY, refill_rate=PUBLIC_API_REFILL_RATE)
        self.private_limiter = RateLimiter(capacity=PRIVATE_API_CAPACITY, refill_rate=PRIVATE_API_REFILL_RATE)
        
        # Reusable SSL context
        self._ssl_context = None
        
        logger.info("Kraken client initialized")

    async def get_spot_markets(self) -> Dict[str, dict]:
        """
        Fetch Kraken spot markets (USD-quoted, active). Cached for market_cache_minutes.
        Callers must use this instead of accessing spot_exchange.
        """
        if not self.exchange:
            await self.initialize()
        async with self._markets_lock:
            now = time.time()
            if "spot" in self._markets_cache:
                ts, data = self._markets_cache["spot"]
                if (now - ts) < self._market_cache_minutes * 60:
                    return data
            try:
                await self.exchange.load_markets()
                usd_markets = {}
                for m in self.exchange.markets.values():
                    if m.get("quote") == "USD" and m.get("active", True):
                        sym = m.get("symbol", "")
                        # Exclude fiat + stablecoin bases (e.g., GBP/USD, USDT/USD).
                        base = m.get("base")
                        if is_disallowed_trading_base(base):
                            continue
                        if sym:
                            usd_markets[sym] = {
                                "id": m.get("id"),
                                "base": m.get("base"),
                                "quote": m.get("quote"),
                                "active": m.get("active", True),
                            }
                self._markets_cache["spot"] = (now, usd_markets)
                return usd_markets
            except Exception as e:
                logger.error("Failed to fetch spot markets", error=str(e))
                raise

    async def get_futures_markets(self) -> Dict[str, dict]:
        """
        Fetch Kraken futures perpetuals (swap, active). Cached for market_cache_minutes.
        Returns Dict[base_quote, info] e.g. "BTC/USD" -> {symbol, base, quote, active}.
        """
        if not self.futures_exchange:
            await self.initialize()
        if not self.futures_exchange:
            return {}
        async with self._markets_lock:
            now = time.time()
            if "futures" in self._markets_cache:
                ts, data = self._markets_cache["futures"]
                if (now - ts) < self._market_cache_minutes * 60:
                    return data
            try:
                await self.futures_exchange.load_markets()
                perps = {}
                for m in self.futures_exchange.markets.values():
                    if m.get("type") == "swap" and m.get("active", True):
                        ccxt_symbol = m.get("symbol", "") or ""
                        market_id = m.get("id") or ccxt_symbol

                        # Skip inverse perpetuals (PI_): they have no USD
                        # order book and would overwrite the linear PF_ entry
                        # when both map to the same base_quote key.
                        if str(market_id).upper().startswith("PI_"):
                            continue

                        base = m.get("base")
                        quote = m.get("quote")
                        if is_disallowed_trading_base(base):
                            continue

                        if base and quote:
                            base_quote = f"{base}/{quote}"
                        elif ":" in ccxt_symbol:
                            base_quote = ccxt_symbol.split(":")[0]
                        else:
                            base_quote = ccxt_symbol
                        if base_quote:
                            perps[base_quote] = {
                                "id": market_id,
                                "symbol": market_id,
                                "ccxt_symbol": ccxt_symbol,
                                "base": base,
                                "quote": quote,
                                "active": m.get("active", True),
                            }
                self._markets_cache["futures"] = (now, perps)
                return perps
            except Exception as e:
                logger.error("Failed to fetch futures markets", error=str(e))
                raise

    async def get_spot_balance(self) -> Dict[str, Any]:
        """
        Get spot account balance using CCXT.
        
        Returns:
            Dict containing balance info
        """
        await self.private_limiter.wait_for_token()
        await self._api_breaker.can_execute()

        try:
            # Now fully async
            balance = await self.exchange.fetch_balance()
            await self._api_breaker.record_success()
            logger.debug("Fetched spot balance")
            return balance
        except Exception as e:
            classified = self._classify_exception(e)
            if isinstance(classified, OperationalError) and not isinstance(classified, CircuitOpenError):
                await self._api_breaker.record_failure(e, is_rate_limit=isinstance(classified, RateLimitError))
            logger.error("Failed to fetch spot balance", error=str(e))
            raise classified from e

    async def get_spot_ticker(self, symbol: str) -> Dict:
        """Get current spot ticker information."""
        await self.public_limiter.wait_for_token()
        await self._api_breaker.can_execute()
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            await self._api_breaker.record_success()
            return ticker
        except Exception as e:
            classified = self._classify_exception(e)
            if isinstance(classified, OperationalError) and not isinstance(classified, CircuitOpenError):
                await self._api_breaker.record_failure(e, is_rate_limit=isinstance(classified, RateLimitError))
            # Don't log errors for invalid symbols - just skip them silently
            error_msg = str(e).lower()
            if "does not have market" in error_msg or "invalid symbol" in error_msg:
                logger.debug(f"Symbol {symbol} not available on exchange, skipping", error=str(e))
            else:
                logger.error(f"Failed to fetch spot ticker for {symbol}", error=str(e))
            raise classified from e

    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Compatibility alias used by shared helpers (e.g. equity valuation).

        Prefer `get_spot_ticker` elsewhere for clarity.
        """
        return await self.get_spot_ticker(symbol)

    async def get_spot_tickers_bulk(self, symbols: List[str]) -> Dict[str, Dict]:
        """
        Get spot tickers for multiple symbols in one call.
        Returns dict: {symbol: ticker_data}
        Handles invalid symbols gracefully by skipping them.
        """
        await self.public_limiter.wait_for_token()
        results = {}
        chunk_size = 50 
        
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i + chunk_size]
            try:
                # Wrap bulk fetch in timeout
                tickers = await asyncio.wait_for(
                    self.exchange.fetch_tickers(chunk),
                    timeout=3.0
                )
                results.update(tickers)
            except asyncio.TimeoutError:
                 logger.debug(f"Bulk fetch timed out for chunk {i//chunk_size}, falling back to individual")
                 pass 
            except Exception as e:
                # If bulk fetch fails, it might be due to ONE bad symbol in the chunk
                logger.debug(f"Bulk fetch failed for chunk {i//chunk_size}, falling back to individual", error=str(e))
                pass

            # Fallback logic: check which symbols are missing from results and fetch individually
            # This handles both timeouts and partial failures (invalid symbols)
            missing = [s for s in chunk if s not in results]
            if missing:
                for symbol in missing:
                    try:
                        # Wrap individual fetch in short timeout
                        ticker = await asyncio.wait_for(
                            self.get_spot_ticker(symbol),
                            timeout=0.5
                        )
                        results[symbol] = ticker
                    except Exception as e:
                        # Log as debug to avoid spamming warnings for unsupported coins
                        logger.debug(f"Individual fetch failed for {symbol}", error=str(e))
                        pass
                    await asyncio.sleep(0) # Yield
        
        return results

    async def get_spot_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Candle]:
        """
        Fetch OHLCV data from spot market.
        
        Args:
            symbol: Symbol (e.g., "BTC/USD")
            timeframe: Timeframe (e.g., "15m", "1h", "4h", "1d")
            since: Unix timestamp (milliseconds) to fetch from
            limit: Maximum number of candles
        
        Returns:
            List of Candle objects
        """
        await self.public_limiter.wait_for_token()
        await self._api_breaker.can_execute()

        try:
            # Wrap fetch in timeout to prevent hangs
            ohlcv = await asyncio.wait_for(
                self.exchange.fetch_ohlcv(
                    symbol, timeframe, since=since, limit=limit
                ),
                timeout=10.0
            )
            
            candles = []
            for row in ohlcv:
                timestamp_ms, open_price, high, low, close, volume = row
                timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                
                candle = Candle(
                    timestamp=timestamp,
                    symbol=symbol,
                    timeframe=timeframe,
                    open=Decimal(str(open_price)),
                    high=Decimal(str(high)),
                    low=Decimal(str(low)),
                    close=Decimal(str(close)),
                    volume=Decimal(str(volume)),
                )
                candles.append(candle)
            
            await self._api_breaker.record_success()
            logger.debug(
                "Fetched spot OHLCV",
                symbol=symbol,
                timeframe=timeframe,
                count=len(candles),
            )
            return candles

        except Exception as e:
            classified = self._classify_exception(e)
            if isinstance(classified, OperationalError) and not isinstance(classified, CircuitOpenError):
                await self._api_breaker.record_failure(e, is_rate_limit=isinstance(classified, RateLimitError))
            err_detail = str(e)
            err_type = type(e).__name__
            resp = getattr(e, "response", None)
            if resp is not None and hasattr(resp, "text"):
                err_detail = f"{err_detail}; response={resp.text[:200]!r}"
            logger.error(
                "Failed to fetch spot OHLCV",
                symbol=symbol,
                error=err_detail,
                error_type=err_type,
            )
            raise classified from e
    
    async def get_futures_ohlcv(
        self,
        futures_symbol: str,
        timeframe: str,
        since: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Candle]:
        """
        Fetch OHLCV from Kraken Futures. Used when spot OHLCV is unavailable.

        Args:
            futures_symbol: Kraken futures symbol (e.g. PF_XBTUSD)
            timeframe: 15m, 1h, 4h, 1d
            since: Unix ms (optional)
            limit: Max candles (default 300)

        Returns:
            List of Candle (symbol set to futures_symbol; caller may overwrite for storage).
        """
        if not self.futures_exchange:
            logger.warning("Futures exchange not configured; cannot fetch futures OHLCV")
            return []
        limit = limit or 300
        await self.public_limiter.wait_for_token()
        await self._api_breaker.can_execute()
        try:
            if not self.futures_exchange.markets:
                await self.futures_exchange.load_markets()
            unified = futures_symbol
            for m in self.futures_exchange.markets.values():
                if m.get("id") and str(m["id"]).upper() == str(futures_symbol).upper():
                    unified = m["symbol"]
                    break
                if m.get("symbol") == futures_symbol:
                    break
            ohlcv = await asyncio.wait_for(
                self.futures_exchange.fetch_ohlcv(unified, timeframe, since=since, limit=limit),
                timeout=10.0,
            )
            candles = []
            for row in ohlcv:
                ts_ms, o, h, l, c, v = row
                ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                candles.append(Candle(
                    timestamp=ts,
                    symbol=futures_symbol,
                    timeframe=timeframe,
                    open=Decimal(str(o)),
                    high=Decimal(str(h)),
                    low=Decimal(str(l)),
                    close=Decimal(str(c)),
                    volume=Decimal(str(v)),
                ))
            await self._api_breaker.record_success()
            logger.debug("Fetched futures OHLCV", symbol=futures_symbol, timeframe=timeframe, count=len(candles))
            return candles
        except Exception as e:
            classified = self._classify_exception(e)
            if isinstance(classified, OperationalError) and not isinstance(classified, CircuitOpenError):
                await self._api_breaker.record_failure(e, is_rate_limit=isinstance(classified, RateLimitError))
            logger.debug("Futures OHLCV fetch failed", symbol=futures_symbol, timeframe=timeframe, error=str(e))
            return []

    async def get_futures_position(self, symbol: str) -> Optional[Dict]:
        """
        Get current futures position from Kraken Futures API.
        
        Args:
            symbol: Futures symbol (e.g., "BTCUSD-PERP")
        
        Returns:
            Position dict with keys: size, entry_price, liquidation_price, unrealized_pnl
        """
        all_positions = await self.get_all_futures_positions()
        for pos in all_positions:
            if pos['symbol'] == symbol:
                return pos
        return None

    @retry_on_transient_errors(max_retries=3, base_delay=1.0)
    async def get_all_futures_positions(self) -> List[Dict]:
        """
        Get all open futures positions from Kraken Futures API.
        
        Returns:
            List of position dicts
        """
        await self.private_limiter.wait_for_token()
        await self._api_breaker.can_execute()

        if not self.futures_api_key or not self.futures_api_secret:
            raise ValueError("Futures API credentials not configured")
        
        try:
            url = "https://futures.kraken.com/derivatives/api/v3/openpositions"
            headers = await self._get_futures_auth_headers(url, "GET")
            
            session = await self._get_http_session()
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise OperationalError(f"Futures API error ({response.status}): {error_text}")
                
                data = await response.json()
                logger.debug("Raw Positions Response", keys=list(data.keys()), count=len(data.get('openPositions', [])))
                
                positions = []
                for pos in data.get('openPositions', []):
                    positions.append({
                        'symbol': pos.get('symbol'),
                        'size': abs(Decimal(str(pos.get('size', 0)))),  # Always positive
                        'entry_price': Decimal(str(pos.get('price', 0))),
                        'liquidation_price': Decimal(str(pos.get('liquidationPrice', 0))),
                        'unrealized_pnl': Decimal(str(pos.get('unrealizedPnl', 0))),
                        'side': pos.get('side', 'long'),  # Use API's side field directly
                    })
                
                await self._api_breaker.record_success()
                return positions
            
        except Exception as e:
            classified = self._classify_exception(e)
            if isinstance(classified, OperationalError) and not isinstance(classified, CircuitOpenError):
                await self._api_breaker.record_failure(e, is_rate_limit=isinstance(classified, RateLimitError))
            logger.error("Failed to fetch all futures positions", error=str(e))
            raise classified from e
    
    async def get_futures_instruments(self) -> List[Dict]:
        """
        Fetch all futures instruments and their specifications.
        Required to get contractSize for conversion.
        """
        await self.public_limiter.wait_for_token()
        try:
            url = "https://futures.kraken.com/derivatives/api/v3/instruments"
            session = await self._get_http_session()
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Futures API error: {await response.text()}")
                data = await response.json()
                return data.get('instruments', [])
        except Exception as e:
            logger.error("Failed to fetch futures instruments", error=str(e))
            raise

    async def get_futures_mark_price(self, symbol: str) -> Decimal:
        """
        Get current mark price from Kraken Futures official feed.
        
        CRITICAL: Mark price MUST be sourced from Kraken Futures mark/index feed,
        not computed from bid/ask. This is the official price used for liquidations.
        
        Args:
            symbol: Futures symbol (e.g., "BTCUSD-PERP", "PI_XBTUSD" for perpetual)
        
        Returns:
            Mark price as Decimal
        """
        await self.public_limiter.wait_for_token()
        
        try:
            url = "https://futures.kraken.com/derivatives/api/v3/tickers"
            
            session = await self._get_http_session()
            async with session.get(url) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error("Futures API error", status=response.status, error=error_text)
                    raise Exception(f"Futures API error: {error_text}")
                
                data = await response.json()
                
                # Kraken Futures uses PF_ prefix for perpetuals
                # and XBT instead of BTC
                search_symbols = [symbol]
                if symbol.endswith('-PERP'):
                    base = symbol.replace('-PERP', '').replace('/', '')
                    # Kraken uses XBT for Bitcoin
                    base = base.replace('BTC', 'XBT')
                    search_symbols.append(f"PF_{base}")
                    search_symbols.append(f"PI_{base}")  # Legacy format
                
                # Find ticker for this symbol
                for ticker in data.get('tickers', []):
                    ticker_symbol = ticker.get('symbol')
                    if ticker_symbol in search_symbols:
                        mark_price = ticker.get('markPrice')
                        if mark_price is None:
                            raise ValueError(f"Mark price not available for {symbol}")
                        
                        logger.debug(
                            "Fetched futures mark price",
                            symbol=symbol,
                            ticker_symbol=ticker_symbol,
                            mark_price=mark_price,
                        )
                        return Decimal(str(mark_price))
                
                raise ValueError(f"Symbol {symbol} not found in tickers. Searched: {search_symbols}")
            
        except Exception as e:
            logger.error("Failed to fetch futures mark price", symbol=symbol, error=str(e))
            raise

    async def get_futures_tickers_bulk(self) -> Dict[str, Decimal]:
        """
        Get ALL futures mark prices in one call.
        Returns dict keyed by multiple formats for each ticker:
        - Original raw symbol (e.g., "PI_THETAUSD")
        - PF_{BASE}USD format (e.g., "PF_THETAUSD")
        - CCXT unified format (e.g., "THETA/USD:USD")
        - BASE/USD format (e.g., "THETA/USD")
        
        This ensures lookup works regardless of format used.
        """
        await self.public_limiter.wait_for_token()
        await self._api_breaker.can_execute()
        try:
            url = "https://futures.kraken.com/derivatives/api/v3/tickers"
            session = await self._get_http_session()
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Futures API error: {await response.text()}")
                data = await response.json()

            results: Dict[str, Decimal] = {}
            
            def derive_base(symbol: str) -> Optional[str]:
                """Derive base currency from symbol (e.g., PI_THETAUSD -> THETA)."""
                # Strip prefixes: PI_, PF_, FI_
                base = symbol.upper()
                for prefix in ["PI_", "PF_", "FI_"]:
                    if base.startswith(prefix):
                        base = base[len(prefix):]
                        break
                # Strip trailing USD
                if base.endswith("USD"):
                    base = base[:-3]
                # Handle XBT -> BTC
                if base == "XBT":
                    base = "BTC"
                return base if base else None
            
            for ticker in data.get("tickers", []):
                raw_symbol = ticker.get("symbol")
                mark = ticker.get("markPrice")
                if not raw_symbol or not mark:
                    continue
                
                mark_decimal = Decimal(str(mark))
                
                # Store original raw key
                results[raw_symbol] = mark_decimal
                
                # Derive base and create normalized keys
                base = derive_base(raw_symbol)
                if base:
                    # Add PF_{BASE}USD format
                    pf_key = f"PF_{base}USD"
                    if pf_key not in results:
                        results[pf_key] = mark_decimal
                    
                    # Add {BASE}/USD:USD (CCXT unified)
                    ccxt_unified = f"{base}/USD:USD"
                    if ccxt_unified not in results:
                        results[ccxt_unified] = mark_decimal
                    
                    # Add {BASE}/USD (helper format)
                    base_usd = f"{base}/USD"
                    if base_usd not in results:
                        results[base_usd] = mark_decimal

            # Also add CCXT unified keys from exchange markets (for any we might have missed)
            if self.futures_exchange:
                try:
                    if not self.futures_exchange.markets:
                        await self.futures_exchange.load_markets()
                    for raw, val in list(results.items()):
                        raw_upper = str(raw).upper()
                        for m in self.futures_exchange.markets.values():
                            mid = m.get("id")
                            if not mid:
                                continue
                            if str(mid).upper() == raw_upper:
                                unified = m.get("symbol")
                                if unified and unified not in results:
                                    results[unified] = val
                                break
                except Exception as e:
                    logger.debug("Could not add CCXT keys to bulk futures tickers", error=str(e))

            await self._api_breaker.record_success()
            return results
        except Exception as e:
            classified = self._classify_exception(e)
            if isinstance(classified, OperationalError) and not isinstance(classified, CircuitOpenError):
                await self._api_breaker.record_failure(e, is_rate_limit=isinstance(classified, RateLimitError))
            logger.error("Failed to fetch bulk futures tickers", error=str(e))
            raise classified from e

    async def get_futures_tickers_bulk_full(self) -> Dict[str, FuturesTicker]:
        """
        Get ALL futures tickers with full data for market discovery filtering.
        
        Returns dict keyed by multiple formats for each ticker:
        - Original raw symbol (e.g., "PI_ETHUSD")
        - PF_{BASE}USD format (e.g., "PF_ETHUSD")
        - CCXT unified format (e.g., "ETH/USD:USD")
        - BASE/USD format (e.g., "ETH/USD")
        
        Each value is a FuturesTicker with: mark_price, bid, ask, volume_24h, open_interest, funding_rate
        """
        await self.public_limiter.wait_for_token()
        await self._api_breaker.can_execute()
        try:
            url = "https://futures.kraken.com/derivatives/api/v3/tickers"
            session = await self._get_http_session()
            async with session.get(url) as response:
                if response.status != 200:
                    raise OperationalError(f"Futures API error ({response.status}): {await response.text()}")
                data = await response.json()

            results: Dict[str, FuturesTicker] = {}
            
            def derive_base(symbol: str) -> Optional[str]:
                """Derive base currency from symbol (e.g., PI_THETAUSD -> THETA)."""
                base = symbol.upper()
                for prefix in ["PI_", "PF_", "FI_"]:
                    if base.startswith(prefix):
                        base = base[len(prefix):]
                        break
                if base.endswith("USD"):
                    base = base[:-3]
                if base == "XBT":
                    base = "BTC"
                return base if base else None
            
            for ticker in data.get("tickers", []):
                raw_symbol = ticker.get("symbol")
                if not raw_symbol:
                    continue
                
                # Parse all fields with safe defaults
                mark_price = Decimal(str(ticker.get("markPrice", 0) or 0))
                bid = Decimal(str(ticker.get("bid", 0) or 0))
                ask = Decimal(str(ticker.get("ask", 0) or 0))
                volume_24h = Decimal(str(ticker.get("volumeQuote", 0) or ticker.get("volume", 0) or 0))
                open_interest = Decimal(str(ticker.get("openInterest", 0) or 0))
                
                # Funding rate may be None for non-perpetuals
                funding_raw = ticker.get("fundingRate") or ticker.get("funding_rate")
                funding_rate = Decimal(str(funding_raw)) if funding_raw is not None else None
                
                ft = FuturesTicker(
                    symbol=raw_symbol,
                    mark_price=mark_price,
                    bid=bid,
                    ask=ask,
                    volume_24h=volume_24h,
                    open_interest=open_interest,
                    funding_rate=funding_rate,
                )
                
                # Store with raw key
                results[raw_symbol] = ft
                
                # Derive base and create normalized keys.
                # Only PF_ (perpetual futures) symbols create alias keys;
                # PI_ (price index) and FI_ (fixed-income) have no order
                # book and would poison aliases with bid=0/ask=0.
                raw_upper = raw_symbol.upper()
                is_tradeable_perp = raw_upper.startswith("PF_")
                
                base = derive_base(raw_symbol)
                if base and is_tradeable_perp:
                    # PF_ symbols always overwrite aliases to ensure
                    # the tradeable perpetual's data wins.
                    pf_key = f"PF_{base}USD"
                    results[pf_key] = ft
                    
                    ccxt_unified = f"{base}/USD:USD"
                    results[ccxt_unified] = ft
                    
                    base_usd = f"{base}/USD"
                    results[base_usd] = ft

            # Also add CCXT unified keys from exchange markets
            if self.futures_exchange:
                try:
                    if not self.futures_exchange.markets:
                        await self.futures_exchange.load_markets()
                    for raw, ft in list(results.items()):
                        raw_upper = str(raw).upper()
                        for m in self.futures_exchange.markets.values():
                            mid = m.get("id")
                            if not mid:
                                continue
                            if str(mid).upper() == raw_upper:
                                unified = m.get("symbol")
                                if unified and unified not in results:
                                    results[unified] = ft
                                break
                except Exception as e:
                    logger.debug("Could not add CCXT keys to bulk full futures tickers", error=str(e))

            await self._api_breaker.record_success()
            logger.info("Fetched full futures tickers", count=len(results))
            return results
        except Exception as e:
            classified = self._classify_exception(e)
            if isinstance(classified, OperationalError) and not isinstance(classified, CircuitOpenError):
                await self._api_breaker.record_failure(e, is_rate_limit=isinstance(classified, RateLimitError))
            logger.error("Failed to fetch bulk full futures tickers", error=str(e))
            raise classified from e
    
    async def get_account_balance(self) -> Dict[str, Decimal]:
        """
        Get account balance (spot).
        
        Returns:
            Dict of currency -> balance
        """
        await self.private_limiter.wait_for_token()
        await self._api_breaker.can_execute()
        
        try:
            balance = await self.exchange.fetch_balance()
            await self._api_breaker.record_success()
            return {
                currency: Decimal(str(amount))
                for currency, amount in balance['total'].items()
                if amount > 0
            }
            
        except Exception as e:
            classified = self._classify_exception(e)
            if isinstance(classified, OperationalError) and not isinstance(classified, CircuitOpenError):
                await self._api_breaker.record_failure(e, is_rate_limit=isinstance(classified, RateLimitError))
            logger.error("Failed to fetch account balance", error=str(e))
            raise classified from e
    
    async def get_futures_fills(
        self,
        symbol: Optional[str] = None,
        since: Optional[int] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Fetch recent futures trade fills (my trades) from the exchange.
        
        Args:
            symbol: Filter by symbol (e.g. "BTC/USD:USD"). None = all.
            since: Timestamp in ms to fetch from. None = recent.
            limit: Max number of trades to return.
            
        Returns:
            List of trade dicts with id, order, symbol, side, amount, price, timestamp, fee, etc.
        """
        if not self.futures_exchange:
            raise OperationalError("Futures exchange not initialized")
        
        await self.private_limiter.wait_for_token()
        await self._api_breaker.can_execute()
        
        try:
            trades = await self.futures_exchange.fetch_my_trades(
                symbol=symbol,
                since=since,
                limit=limit,
            )
            await self._api_breaker.record_success()
            logger.debug("Fetched futures fills", count=len(trades), symbol=symbol)
            return trades
        except Exception as e:
            classified = self._classify_exception(e)
            if isinstance(classified, OperationalError) and not isinstance(classified, CircuitOpenError):
                await self._api_breaker.record_failure(e, is_rate_limit=isinstance(classified, RateLimitError))
            logger.error("Failed to fetch futures fills", error=str(e), symbol=symbol)
            raise classified from e

    async def place_spot_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        amount: Decimal,
        price: Optional[Decimal] = None,
    ) -> Dict[str, Any]:
        """
        Place an order on the Kraken spot exchange using the spot CCXT instance.
        
        Args:
            symbol: Spot symbol (e.g. "SOL/USD")
            side: "buy" or "sell"
            order_type: "market" or "limit"
            amount: Quantity of base asset to buy/sell
            price: Limit price (required for limit orders, ignored for market)
            
        Returns:
            CCXT order response dict
        """
        if not self.exchange:
            raise OperationalError("Spot exchange not initialized")
        
        if not self.has_valid_spot_credentials():
            raise OperationalError("Spot API credentials not configured")
        
        dry_run = os.environ.get("DRY_RUN", "0") == "1"
        if dry_run:
            logger.info(
                "DRY_RUN: Spot order skipped",
                symbol=symbol, side=side, order_type=order_type,
                amount=str(amount), price=str(price) if price else "market",
            )
            return {"id": "dry-run", "status": "dry_run", "symbol": symbol, "side": side, "amount": str(amount)}
        
        await self.private_limiter.wait_for_token()
        await self._api_breaker.can_execute()
        
        try:
            params = {}
            result = await self.exchange.create_order(
                symbol=symbol,
                type=order_type,
                side=side,
                amount=float(amount),
                price=float(price) if price else None,
                params=params,
            )
            await self._api_breaker.record_success()
            logger.info(
                "Spot order placed",
                symbol=symbol, side=side, order_type=order_type,
                amount=str(amount), order_id=result.get("id"),
            )
            return result
        except Exception as e:
            classified = self._classify_exception(e)
            if isinstance(classified, OperationalError) and not isinstance(classified, CircuitOpenError):
                await self._api_breaker.record_failure(e, is_rate_limit=isinstance(classified, RateLimitError))
            logger.error("Failed to place spot order", error=str(e), symbol=symbol, side=side)
            raise classified from e

    async def place_futures_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        size: Decimal,
        price: Optional[Decimal] = None,
        stop_price: Optional[Decimal] = None,
        reduce_only: bool = False,
        leverage: Optional[Decimal] = None,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Place order on Kraken Futures using CCXT.
        
        ⚠️ PRODUCTION RISK: SL/TP semantics are exchange-specific and not fully validated.
        See docs/PRODUCTION_BLOCKERS.md for details. Integration tests required before live trading.
        
        Current SL/TP mapping (UNVALIDATED):
        - STOP_LOSS ("stp"): type="stop", params={"stopPrice": stop_price}
        - TAKE_PROFIT ("take_profit"): type="take_profit", params={"stopPrice": stop_price}
        - Behavior (stop-market vs stop-limit) depends on CCXT/Kraken implementation
        
        Args:
            symbol: Futures symbol (e.g., "BTC/USD:USD" or "PF_XBTUSD")
            side: "buy" or "sell"
            order_type: "limit", "market", "stop", "take_profit"
            size: Order size in contracts
            price: Limit price (required for limit orders)
            stop_price: Stop price (for stop/take_profit orders)
            reduce_only: Whether order is reduce-only
            leverage: Optional leverage to set
            client_order_id: Optional client order ID
        
        Returns:
            Dict with order details from exchange
        """
        if not self.futures_exchange:
            raise ValueError("Futures credentials not configured")

        # P0.1: Defense-in-depth DRY_RUN gate at transport boundary.
        # No simulated success — raise OperationalError so callers know immediately.
        if self._dry_run:
            logger.warning(
                "DRY_RUN_ACTIVE — refusing to send real order",
                symbol=symbol,
                side=side,
                order_type=order_type,
                size=str(size),
                price=str(price) if price else None,
                stop_price=str(stop_price) if stop_price else None,
                reduce_only=reduce_only,
            )
            raise OperationalError("dry_run_active: order placement refused at transport boundary")

        # Defense-in-depth: never place NEW (non-reduce-only) orders on excluded-base instruments
        # (fiat currencies + stablecoins).
        # Reduce-only exits/close operations are allowed so the system can unwind legacy exposure safely.
        if not reduce_only and has_disallowed_base(symbol):
            raise DataError(f"Blocked non-reduce-only order on excluded base instrument: {symbol}")

        # Reject zero or negative size to avoid venue "amount must be greater than minimum" errors
        size_f = float(size)
        if size_f <= 0:
            raise DataError(
                f"Order size must be positive (got {size}). "
                "Check instrument min_size and size rounding."
            )
        await self._api_breaker.can_execute()

        try:
            from src.data.symbol_utils import futures_candidate_symbols, pf_to_unified

            # Create params dict for extra options
            params = {}
            if reduce_only:
                params['reduceOnly'] = True
            if client_order_id:
                params['cliOrdId'] = client_order_id
                
            # Map order type 'lmt' -> 'limit' for CCXT if passed as raw kraken string
            type_map = {'lmt': 'limit', 'mkt': 'market', 'stp': 'stop'}
            ccxt_type = type_map.get(order_type, order_type)
            
            # CRITICAL: Resolve unified symbol format ONCE for all API calls
            # Kraken Futures uses symbols like PF_XBTUSD but CCXT needs unified format
            unified_symbol = symbol
            if not self.futures_exchange.markets:
                await self.futures_exchange.load_markets()

            symbol_upper = str(symbol or "").upper()
            id_to_unified = {}
            sym_to_unified = {}
            for m in self.futures_exchange.markets.values():
                mid = str(m.get("id") or "").upper()
                msym = str(m.get("symbol") or "")
                msym_upper = msym.upper()
                if mid and msym:
                    id_to_unified[mid] = msym
                if msym_upper and msym:
                    sym_to_unified[msym_upper] = msym

            candidates: List[str] = [symbol]
            if symbol_upper.startswith("PF_"):
                candidates.append(pf_to_unified(symbol_upper))
            if "/" in symbol_upper:
                candidates.extend(futures_candidate_symbols(symbol_upper))
                if symbol_upper.endswith("/USD"):
                    candidates.append(f"{symbol_upper}:USD")

            seen = set()
            ordered_candidates = []
            for cand in candidates:
                key = str(cand or "").strip().upper()
                if not key or key in seen:
                    continue
                seen.add(key)
                ordered_candidates.append(key)

            for cand_upper in ordered_candidates:
                if cand_upper in sym_to_unified:
                    unified_symbol = sym_to_unified[cand_upper]
                    break
                if cand_upper in id_to_unified:
                    unified_symbol = id_to_unified[cand_upper]
                    break
            
            if stop_price:
                # Round stop price to precision using CCXT
                rounded_stop = self.futures_exchange.price_to_precision(unified_symbol, float(stop_price))
                params['stopPrice'] = float(rounded_stop)
            
            logger.info(
                "Placing futures order",
                symbol=symbol,
                unified_symbol=unified_symbol,
                side=side,
                type=ccxt_type,
                size=str(size),
                leverage=str(leverage) if leverage else "default",
            )
            
            # Set leverage only when explicitly requested and not reduce-only.
            # When leverage is None (e.g. unknown spec), skip set_leverage and use venue default.
            leverage_set_success = False
            if leverage is not None and leverage and not reduce_only:
                try:
                    await self.futures_exchange.set_leverage(float(leverage), unified_symbol)
                    logger.info("Leverage set successfully", leverage=float(leverage), symbol=unified_symbol)
                    leverage_set_success = True
                except Exception as lev_err:
                    error_str = str(lev_err).lower()
                    if "already" in error_str or "same" in error_str or "no change" in error_str:
                        logger.info("Leverage already set to target value", leverage=float(leverage), symbol=unified_symbol)
                        leverage_set_success = True
                    else:
                        venue_code, venue_msg = _extract_venue_error(lev_err)
                        logger.error(
                            "ORDER_REJECTED_BY_VENUE",
                            symbol=unified_symbol,
                            venue_error_code=venue_code,
                            venue_error_message=venue_msg,
                            payload_summary={"side": side, "type": ccxt_type, "amount": float(size), "leverage": float(leverage)},
                        )
                        raise Exception(f"Leverage setting failed for {unified_symbol}: {lev_err}. Order rejected for safety.")
            else:
                leverage_set_success = True
            
            if leverage is not None and leverage and not reduce_only:
                params['leverage'] = float(leverage)
            
            try:
                order = await self.futures_exchange.create_order(
                    symbol=unified_symbol,
                    type=ccxt_type,
                    side=side,
                    amount=float(size),
                    price=float(price) if price else None,
                    params=params
                )
            except Exception as order_err:
                venue_code, venue_msg = _extract_venue_error(order_err)
                logger.error(
                    "ORDER_REJECTED_BY_VENUE",
                    symbol=unified_symbol,
                    venue_error_code=venue_code,
                    venue_error_message=venue_msg,
                    payload_summary={"side": side, "type": ccxt_type, "amount": float(size)},
                )
                raise
            
            logger.info(
                "Futures order placed successfully",
                order_id=order['id'],
                symbol=unified_symbol,
                leverage=float(leverage) if leverage else "default",
                leverage_confirmed=leverage_set_success
            )
            
            await self._api_breaker.record_success()
            return order
            
        except Exception as e:
            classified = self._classify_exception(e)
            if isinstance(classified, OperationalError) and not isinstance(classified, CircuitOpenError):
                await self._api_breaker.record_failure(e, is_rate_limit=isinstance(classified, RateLimitError))
            logger.error("Futures order placement failed", error=str(e))
            raise classified from e

    async def create_order(
        self,
        symbol: str,
        type: str,
        side: str,
        amount: float,
        price: Optional[float] = None,
        params: Optional[Dict[str, Any]] = None,
        leverage: Optional[Decimal] = None,
    ) -> Dict[str, Any]:
        """
        CCXT-style create_order for ExecutionGateway compatibility.
        Delegates to place_futures_order.
        """
        p = params or {}
        client_order_id = p.get("clientOrderId") or p.get("cliOrdId")
        reduce_only = bool(p.get("reduceOnly", False))
        stop_price = p.get("stopPrice")
        if stop_price is not None:
            stop_price = Decimal(str(stop_price))
        elif type in ("stop", "stop_loss") and price is not None:
            stop_price = Decimal(str(price))
        leverage_dec: Optional[Decimal] = None
        if not reduce_only and leverage is not None:
            leverage_dec = Decimal(str(leverage))
        order_type = "stop" if type in ("stop", "stop_loss") else type
        size = Decimal(str(amount))
        price_dec = Decimal(str(price)) if price is not None else None
        return await self.place_futures_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            size=size,
            price=price_dec,
            stop_price=stop_price,
            reduce_only=reduce_only,
            leverage=leverage_dec,
            client_order_id=client_order_id,
        )

    @retry_on_transient_errors(max_retries=3, base_delay=1.0)
    async def get_futures_balance(self) -> Dict[str, Any]:
        """
        Get futures account balance using CCXT.
        
        Returns:
            Dict containing balance info (free, used, total for each currency)
        """
        if not self.futures_exchange:
            raise ValueError("Futures credentials not configured")
        await self._api_breaker.can_execute()
            
        try:
            balance = await self.futures_exchange.fetch_balance()
            await self._api_breaker.record_success()
            logger.debug("Fetched futures balance")
            return balance
        except Exception as e:
            classified = self._classify_exception(e)
            if isinstance(classified, OperationalError) and not isinstance(classified, CircuitOpenError):
                await self._api_breaker.record_failure(e, is_rate_limit=isinstance(classified, RateLimitError))
            logger.error("Failed to fetch futures balance", error=str(e))
            raise classified from e

    @retry_on_transient_errors(max_retries=3, base_delay=1.0)
    async def get_futures_account_info(self) -> Dict[str, Any]:
        """
        Get a normalized futures account snapshot for risk/hardening checks.

        Returns keys used by the production hardening pre-tick check:
        - equity
        - availableMargin
        - marginUsed
        """
        balance = await self.get_futures_balance()
        # Local import to keep module load lightweight and avoid circular imports.
        from src.execution.equity import calculate_effective_equity

        equity, available_margin, margin_used = await calculate_effective_equity(
            balance,
            base_currency="USD",
            kraken_client=self,
        )
        return {
            "equity": equity,
            "availableMargin": available_margin,
            "marginUsed": margin_used,
            "balance": balance,
        }

    async def get_futures_open_orders(self) -> List[Dict[str, Any]]:
        """
        Get all open futures orders using CCXT.
        
        Returns:
            List of open order dicts
        """
        # CRITICAL: Runtime assertion - detect mocks in production
        import sys
        import os
        from unittest.mock import Mock, MagicMock
        
        is_test = (
            "pytest" in sys.modules or
            "PYTEST_CURRENT_TEST" in os.environ or
            any("test" in path.lower() for path in sys.path if isinstance(path, str))
        )
        
        if not is_test:
            # Verify futures_exchange is not a mock
            if isinstance(self.futures_exchange, Mock) or isinstance(self.futures_exchange, MagicMock):
                logger.critical("CRITICAL: futures_exchange is a Mock/MagicMock in production!")
                raise RuntimeError(
                    "CRITICAL: futures_exchange is a Mock/MagicMock. "
                    "This should never happen in production. Check for test code leaking into runtime."
                )
            
            # Verify fetch_open_orders is callable and not a mock
            if hasattr(self.futures_exchange, 'fetch_open_orders'):
                fetch_fn = getattr(self.futures_exchange, 'fetch_open_orders')
                if isinstance(fetch_fn, Mock) or isinstance(fetch_fn, MagicMock):
                    logger.critical("CRITICAL: fetch_open_orders is a Mock/MagicMock in production!")
                    raise RuntimeError(
                        "CRITICAL: fetch_open_orders is a Mock/MagicMock. "
                        "This should never happen in production. Check for test code leaking into runtime."
                    )
        
        if not self.futures_exchange:
            raise ValueError("Futures credentials not configured")
        await self._api_breaker.can_execute()
            
        try:
            orders = await self.futures_exchange.fetch_open_orders()
            await self._api_breaker.record_success()
            logger.debug("Fetched open futures orders", count=len(orders))
            return orders
        except Exception as e:
            classified = self._classify_exception(e)
            if isinstance(classified, OperationalError) and not isinstance(classified, CircuitOpenError):
                await self._api_breaker.record_failure(e, is_rate_limit=isinstance(classified, RateLimitError))
            logger.error("Failed to fetch futures open orders", error=str(e))
            raise classified from e

    async def fetch_order(self, order_id: str, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single order by id (open or closed).
        Returns dict with id, status, filled, remaining, average, trades, clientOrderId,
        plus side/type/amount/reduceOnly/stopPrice for semantic validation.
        Returns None if not found / error.
        """
        if not self.futures_exchange:
            return None
        try:
            raw = await self.futures_exchange.fetch_order(order_id, symbol)
            if not raw:
                return None
            info = raw.get("info") or {}
            return {
                "id": raw.get("id"),
                "status": (raw.get("status") or "").lower(),
                "filled": raw.get("filled", 0),
                "remaining": raw.get("remaining", 0),
                "average": raw.get("average") or raw.get("price") or 0,
                "trades": raw.get("trades") or [],
                "clientOrderId": raw.get("clientOrderId") or info.get("cliOrdId") or info.get("clientOrderId"),
                # Semantic validation fields (for stop-order health checks)
                "side": (raw.get("side") or "").lower(),
                "type": (raw.get("type") or info.get("orderType") or "").lower(),
                "amount": raw.get("amount"),
                "reduceOnly": bool(raw.get("reduceOnly") or info.get("reduceOnly") or False),
                "stopPrice": raw.get("stopPrice") or info.get("triggerPrice"),
            }
        except Exception as e:
            logger.debug("fetch_order failed", order_id=order_id, symbol=symbol, error=str(e))
            return None

    async def cancel_futures_order(self, order_id: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        """
        Cancel a futures order using CCXT.
        
        Args:
            order_id: Order ID to cancel
            symbol: Symbol (optional but recommended for CCXT)
        
        Returns:
            Cancellation response
        """
        if not self.futures_exchange:
            raise ValueError("Futures credentials not configured")

        if self._dry_run:
            logger.warning(
                "DRY_RUN_ACTIVE — refusing to cancel order",
                order_id=order_id,
                symbol=symbol,
            )
            raise OperationalError("dry_run_active: order cancellation refused at transport boundary")

        await self._api_breaker.can_execute()

        try:
            await self.futures_exchange.cancel_order(order_id, symbol)
            await self._api_breaker.record_success()
            logger.info("Futures order cancelled", order_id=order_id)
            return {"result": "success", "order_id": order_id}
        except Exception as e:
            classified = self._classify_exception(e)
            if isinstance(classified, OperationalError) and not isinstance(classified, CircuitOpenError):
                await self._api_breaker.record_failure(e, is_rate_limit=isinstance(classified, RateLimitError))
            logger.error("Failed to cancel futures order", order_id=order_id, error=str(e))
            raise classified from e

    async def edit_futures_order(
        self,
        *,
        order_id: str,
        symbol: str,
        stop_price: Optional[Decimal] = None,
        price: Optional[Decimal] = None,
    ) -> Dict[str, Any]:
        """
        Edit an existing futures order (primarily stop-loss updates).

        Strategy:
        - Prefer CCXT `edit_order` if supported by the exchange.
        - Fallback to cancel + recreate (returns a NEW order id).
        """
        if not self.futures_exchange:
            raise ValueError("Futures credentials not configured")

        if self._dry_run:
            logger.warning(
                "DRY_RUN_ACTIVE — refusing to edit order",
                order_id=order_id,
                symbol=symbol,
            )
            raise OperationalError("dry_run_active: order edit refused at transport boundary")

        if not symbol:
            raise ValueError("symbol is required to edit futures order")

        # Resolve unified symbol for CCXT calls
        unified_symbol = symbol
        if not self.futures_exchange.markets:
            await self.futures_exchange.load_markets()
        for m in self.futures_exchange.markets.values():
            mid = m.get("id")
            msym = m.get("symbol")
            if mid and (mid == symbol or str(mid).upper() == str(symbol).upper()):
                unified_symbol = msym
                break
            if msym and msym == symbol:
                unified_symbol = symbol
                break

        params: Dict[str, Any] = {}
        if stop_price is not None:
            rounded_stop = self.futures_exchange.price_to_precision(unified_symbol, float(stop_price))
            params["stopPrice"] = float(rounded_stop)

        # Try CCXT native edit first
        if hasattr(self.futures_exchange, "edit_order"):
            try:
                edited = await self.futures_exchange.edit_order(
                    id=order_id,
                    symbol=unified_symbol,
                    type=None,
                    side=None,
                    amount=None,
                    price=float(price) if price is not None else None,
                    params=params,
                )
                new_id = edited.get("id") if isinstance(edited, dict) else None
                logger.info("Futures order edited", order_id=order_id, new_order_id=new_id, symbol=unified_symbol)
                return {"result": "edited", "order_id": new_id or order_id, "order": edited}
            except Exception as e:
                logger.warning(
                    "edit_order failed; falling back to cancel+replace",
                    order_id=order_id,
                    symbol=unified_symbol,
                    error=str(e),
                )

        # Fallback: cancel + recreate using fetched order details
        existing = await self.futures_exchange.fetch_order(order_id, unified_symbol)
        if not existing:
            raise ValueError(f"Order not found for edit: {order_id} {unified_symbol}")

        side = existing.get("side")
        otype = existing.get("type") or "stop"
        amount = existing.get("amount") or existing.get("remaining") or existing.get("filled")
        if not side or not amount or float(amount) <= 0:
            raise ValueError(f"Cannot replace order without side/amount (order_id={order_id}, symbol={unified_symbol})")

        info = existing.get("info") or {}
        reduce_only = bool(existing.get("reduceOnly") or info.get("reduceOnly"))
        if reduce_only:
            params["reduceOnly"] = True

        cli = existing.get("clientOrderId") or info.get("cliOrdId") or info.get("clientOrderId")
        if cli:
            params["cliOrdId"] = cli

        await self.futures_exchange.cancel_order(order_id, unified_symbol)
        created = await self.futures_exchange.create_order(
            symbol=unified_symbol,
            type=otype,
            side=side,
            amount=float(amount),
            price=float(price) if price is not None else None,
            params=params,
        )
        new_id = created.get("id") if isinstance(created, dict) else None
        logger.info("Futures order replaced", old_order_id=order_id, new_order_id=new_id, symbol=unified_symbol)
        return {"result": "replaced", "old_order_id": order_id, "order_id": new_id, "order": created}

    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        """CCXT-style cancel_order for ExecutionGateway. Delegates to cancel_futures_order."""
        return await self.cancel_futures_order(order_id, symbol)

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Cancel all open futures orders.
        
        Args:
            symbol: Optional symbol to filter cancellations
            
        Returns:
            List of cancellation responses
        """
        if not self.futures_exchange:
            raise ValueError("Futures credentials not configured")
            
        try:
            if symbol:
                # CCXT cancelAllOrders often supports symbol
                await self.futures_exchange.cancel_all_orders(symbol)
                logger.info("All futures orders cancelled", symbol=symbol)
                return [{"result": "success", "symbol": symbol}]
            else:
                # Iterate all open orders if global cancel not supported or to be safe
                open_orders = await self.get_futures_open_orders()
                results = []
                for order in open_orders:
                    try:
                        await self.cancel_futures_order(order['id'], order['symbol'])
                        results.append({"id": order['id'], "status": "cancelled"})
                    except Exception as e:
                        results.append({"id": order['id'], "status": "failed", "error": str(e)})
                return results
                
        except Exception as e:
            logger.error("Failed to cancel all orders", error=str(e))
            raise

    async def close_position(self, symbol: str) -> Dict[str, Any]:
        """
        Close an entire position at market price.
        
        Args:
            symbol: Futures symbol
            
        Returns:
            Order result for the closing trade
        """
        position = await self.get_futures_position(symbol)
        if not position or position['size'] == 0:
            logger.info("No position to close", symbol=symbol)
            return {"status": "no_position"}
            
        # Determine opposite side
        size = position['size'] # Decimal
        current_side = position['side']  # 'long' or 'short'
        close_side = 'sell' if current_side == 'long' else 'buy'
        
        logger.warning(
            "Closing position (Market)",
            symbol=symbol,
            size=str(size),
            side=close_side
        )
        
        # reduce_only=True: close exit, no dust (required for all protective exits)
        return await self.place_futures_order(
            symbol=symbol,
            side=close_side,
            order_type='market',
            size=size,
            reduce_only=True,
        )

    async def close(self):
        """Cleanup resources."""
        await self.close_http_session()
        if self.futures_exchange:
            await self.futures_exchange.close()
        if self.exchange:
            await self.exchange.close()

    def _get_ssl_context(self) -> ssl.SSLContext:
        """
        Get or create reusable SSL context with certifi certificates.
        
        Returns:
            SSL context configured with certifi certificates
        """
        if self._ssl_context is None:
            self._ssl_context = ssl.create_default_context(cafile=certifi.where())
        return self._ssl_context
    
    def _generate_futures_signature(self, path: str, postdata: str, nonce: str) -> str:
        """
        Generate HMAC-SHA512 signature for Kraken Futures API.
        
        Args:
            path: API endpoint path (without /derivatives prefix)
            postdata: POST data string
            nonce: Timestamp nonce
            
        Returns:
            Base64-encoded signature
            
        Raises:
            AuthenticationError: If signature generation fails
        """
        try:
            # Step 1: Concatenate postdata + nonce + path
            message = postdata + nonce + path
            
            # Step 2: SHA-256 hash of the message
            sha256_hash = hashlib.sha256(message.encode('utf-8')).digest()
            
            # Step 3: Base64-decode the API secret
            secret = self.futures_api_secret.strip()
            padding = len(secret) % 4
            if padding != 0:
                secret += '=' * (4 - padding)
                
            secret_decoded = base64.b64decode(secret)
            
            # Step 4: HMAC-SHA-512 using the decoded secret and SHA-256 hash
            signature = hmac.new(
                secret_decoded,
                sha256_hash,
                hashlib.sha512
            ).digest()
            
            # Step 5: Base64-encode the signature
            return base64.b64encode(signature).decode('utf-8')
            
        except Exception as e:
            raise AuthenticationError(f"Failed to generate signature: {e}")
    
    async def _get_futures_auth_headers(self, url: str, method: str, postdata: str = "") -> Dict[str, str]:
        """
        Generate authentication headers for Kraken Futures API.
        
        Args:
            url: Full API endpoint URL
            method: HTTP method (GET, POST)
            postdata: POST data (for POST requests)
        
        Returns:
            Dict of headers including APIKey and Authent
            
        Raises:
            AuthenticationError: If credentials are missing or invalid
        """
        if not self.futures_api_key or not self.futures_api_secret:
            raise AuthenticationError("Futures API credentials not configured")
        
        # Extract path from URL
        path = url.split('.com', 1)[1]
        
        # CRITICAL: Signature uses path WITHOUT /derivatives prefix
        if path.startswith('/derivatives'):
            path = path[len('/derivatives'):]
        
        # Generate nonce (timestamp in milliseconds)
        nonce = str(int(time.time() * 1000))
        
        # Generate signature using extracted method
        authent = self._generate_futures_signature(path, postdata, nonce)
        
        return {
            'APIKey': self.futures_api_key,
            'Authent': authent,
            'Nonce': nonce,
        }


class KrakenWebSocket:
    """
    Kraken WebSocket client for real-time data feeds.
    """
    
    def __init__(
        self,
        endpoint: str,
        on_message: Callable[[Dict], None],
        max_retries: int = 10,
        backoff_seconds: int = 5,
    ):
        """
        Initialize WebSocket client.
        
        Args:
            endpoint: WebSocket endpoint URL
            on_message: Callback for received messages
            max_retries: Maximum reconnection attempts
            backoff_seconds: Base backoff for exponential backoff
        """
        self.endpoint = endpoint
        self.on_message = on_message
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.running = False
        self.retry_count = 0
        
        logger.info("WebSocket client initialized", endpoint=endpoint)
    
    async def connect(self):
        """Connect to WebSocket and start listening."""
        self.running = True
        
        while self.running and self.retry_count < self.max_retries:
            try:
                logger.info(
                    "Connecting to WebSocket",
                    endpoint=self.endpoint,
                    retry=self.retry_count,
                )
                
                async with websockets.connect(self.endpoint) as ws:
                    self.ws = ws
                    self.retry_count = 0  # Reset on successful connection
                    
                    logger.info("WebSocket connected", endpoint=self.endpoint)
                    
                    async for message in ws:
                        try:
                            data = json.loads(message)
                            self.on_message(data)
                        except Exception as e:
                            logger.error(
                                "Error processing WebSocket message",
                                error=str(e),
                                message=message[:200],  # Truncate for logging
                            )
                
            except Exception as e:
                self.retry_count += 1
                backoff = self.backoff_seconds * (2 ** (self.retry_count - 1))
                
                logger.warning(
                    "WebSocket connection failed",
                    endpoint=self.endpoint,
                    retry=self.retry_count,
                    max_retries=self.max_retries,
                    backoff=backoff,
                    error=str(e),
                )
                
                if self.retry_count < self.max_retries:
                    await asyncio.sleep(backoff)
                else:
                    logger.error(
                        "WebSocket max retries exceeded",
                        endpoint=self.endpoint,
                    )
                    break
    
    async def subscribe(self, channels: List[str]):
        """
        Subscribe to WebSocket channels.
        
        Args:
            channels: List of channel names to subscribe to
        """
        if not self.ws:
            raise RuntimeError("WebSocket not connected")
        
        subscription = {
            "event": "subscribe",
            "subscription": {"name": channels},
        }
        
        await self.ws.send(json.dumps(subscription))
        logger.info("Subscribed to channels", channels=channels)
    
    async def disconnect(self):
        """Disconnect WebSocket."""
        self.running = False
        if self.ws:
            await self.ws.close()
            logger.info("WebSocket disconnected", endpoint=self.endpoint)
