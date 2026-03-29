"""
SMC (Smart Money Concepts) signal generation engine.

Design lock enforced: Operates on spot market data ONLY.
No futures prices, funding data, or order book data may be accessed.
"""
from typing import Any, List, Optional, Dict, Tuple, Literal
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
import os
import pandas as pd
from src.domain.models import Candle, Signal, SignalType, SetupType
from src.strategy.indicators import Indicators
from src.strategy.fibonacci_engine import FibonacciEngine
from src.strategy.signal_scorer import SignalScorer
from src.strategy.market_structure_tracker import MarketStructureTracker
from src.strategy.ev_engine import EVEngine
from src.config.config import StrategyConfig, resolve_strategy_for_symbol
from src.monitoring.logger import get_logger
from src.domain.protocols import EventRecorder, _noop_event_recorder
from src.exceptions import OperationalError, DataError
from src.storage.repository import count_recent_stopouts
from src.memory.institutional_memory import InstitutionalMemoryManager
import uuid

logger = get_logger(__name__)


@dataclass
class HigherTFContext:
    weekly_fib_zone_low: Optional[Decimal] = None
    weekly_fib_zone_high: Optional[Decimal] = None
    daily_bias: Literal["bullish", "bearish", "neutral"] = "neutral"
    allowed_entry: bool = True
    weekly_confluence_bonus: float = 0.0


_stopout_cache: dict = {}  # key: (symbol, lookback_hours) -> (count, expires_at)
_STOPOUT_CACHE_TTL = 300  # 5 minutes


def get_recent_stopouts(symbol: str, lookback_hours: int = 24) -> int:
    """
    Query database for recent stop-outs on a symbol.
    Uses SQLAlchemy connection pool (not raw psycopg2) and a 5-minute TTL cache.
    
    Args:
        symbol: Trading symbol (e.g., 'WIF/USD' or 'PF_WIFUSD')
        lookback_hours: Hours to look back for stop-outs
        
    Returns:
        Number of stop-outs in the lookback period
    """
    import time as _time

    cache_key = (symbol, lookback_hours)
    cached = _stopout_cache.get(cache_key)
    if cached and _time.monotonic() < cached[1]:
        return cached[0]

    try:
        if not os.getenv("DATABASE_URL"):
            return 0

        count = count_recent_stopouts(symbol, lookback_hours)
        _stopout_cache[cache_key] = (count, _time.monotonic() + _STOPOUT_CACHE_TTL)
        
        if count > 0:
            logger.info("Recent stop-outs detected", symbol=symbol, count=count, lookback_hours=lookback_hours)
        
        return count
        
    except (OperationalError, DataError, OSError) as e:
        logger.error(
            "Failed to query stop-outs",
            symbol=symbol,
            error=str(e),
            error_type=type(e).__name__,
            exc_info=True,
        )
        return 0


class SMCEngine:
    """
    SMC signal generation engine using spot market data.
    
    CRITICAL DESIGN LOCKS:
    - Analyzes SPOT data only (BTC/USD, ETH/USD)
    - No futures prices, funding, or order book access
    - Deterministic: same input → same signal
    - All parameters configurable (no hardcoded values)
    """
    
    def __init__(
        self,
        config: StrategyConfig,
        *,
        event_recorder: EventRecorder = _noop_event_recorder,
        institutional_memory: Optional[InstitutionalMemoryManager] = None,
    ):
        """
        Initialize SMC engine.
        
        Args:
            config: Strategy configuration
            event_recorder: Callable for recording system events (injected; defaults to no-op)
        """
        self._base_config = config
        self.config = config
        self.indicators = Indicators()
        self._record_event = event_recorder

        # Per-symbol caching for multi-asset support (optimized with tuple keys)
        self.indicator_cache: Dict[Tuple[str, datetime], Dict] = {}
        self.cache_max_size = 1000  # Prevent unbounded growth
        self.cache_max_age = timedelta(hours=2)  # Configurable
        self._signal_fingerprint_last_seen: Dict[str, datetime] = {}
        self._signal_fingerprint_cache_max_size = 5000
        self._higher_tf_candle_context: Dict[str, Dict[str, List[Candle]]] = {}
        self._memory_manager = institutional_memory
        if self._memory_manager is None and bool(getattr(config, "memory_enabled", False)):
            self._memory_manager = InstitutionalMemoryManager(config)

        # Fibonacci engine for confluence scoring
        self.fibonacci_engine = FibonacciEngine(lookback_bars=100)

        # Signal quality scoring
        self.signal_scorer = SignalScorer(config)
        
        # Market Structure Tracker (confirmation + reconfirmation)
        self.ms_tracker = MarketStructureTracker(
            confirmation_candles=getattr(config, 'ms_confirmation_candles', 3),
            reconfirmation_candles=getattr(config, 'ms_reconfirmation_candles', 2),
            entry_zone_tolerance_pct=getattr(config, 'entry_zone_tolerance_pct', 0.015),
            entry_zone_tolerance_adaptive=getattr(config, 'entry_zone_tolerance_adaptive', True),
            entry_zone_tolerance_atr_mult=getattr(config, 'entry_zone_tolerance_atr_mult', 0.3)
        )
        
        # Store score penalty for tolerance entries
        self.entry_zone_tolerance_score_penalty = getattr(config, 'entry_zone_tolerance_score_penalty', -5)
        self._fvg_min_size_pct_default = self._safe_decimal(getattr(config, "fvg_min_size_pct", 0.001), Decimal("0.001"))
        self.ev_engine = EVEngine(config)
        
        logger.info("SMC Engine initialized", config=config.model_dump())

    @staticmethod
    def _normalize_symbol_key(raw_symbol: Optional[str]) -> str:
        if not raw_symbol:
            return ""
        return str(raw_symbol).strip().upper().split(":")[0]

    def _conviction_stop_sizing_enabled_for_symbol(self, symbol: str) -> bool:
        if not bool(getattr(self.config, "conviction_stop_sizing_enabled", False)):
            return False
        canary = set(
            self._normalize_symbol_key(s)
            for s in (getattr(self.config, "conviction_stop_sizing_canary_symbols", []) or [])
            if s
        )
        if not canary:
            return True
        return self._normalize_symbol_key(symbol) in canary

    def _resolve_conviction_min_for_entry(self, symbol: str) -> float:
        replay_override = os.getenv("REPLAY_OVERRIDE_CONVICTION_MIN_FOR_ENTRY")
        if replay_override is not None and replay_override.strip():
            try:
                return float(replay_override)
            except ValueError:
                logger.warning(
                    "Invalid REPLAY_OVERRIDE_CONVICTION_MIN_FOR_ENTRY",
                    value=replay_override,
                )
        base = float(getattr(self.config, "conviction_min_for_entry", 35.0))
        canary_threshold = getattr(self.config, "conviction_min_for_entry_canary", None)
        if canary_threshold is None:
            return base
        canary_symbols = set(
            self._normalize_symbol_key(s)
            for s in (getattr(self.config, "conviction_canary_symbols", []) or [])
            if s
        )
        if not canary_symbols:
            return float(canary_threshold)
        return float(canary_threshold) if self._normalize_symbol_key(symbol) in canary_symbols else base

    def _resolve_fvg_min_size_pct(self, symbol: Optional[str]) -> Decimal:
        # Base default with optional per-symbol override via resolved strategy config.
        return self._safe_decimal(
            getattr(self.config, "fvg_min_size_pct", self._fvg_min_size_pct_default),
            self._fvg_min_size_pct_default,
        )

    @staticmethod
    def _safe_decimal(value: Any, default: Decimal) -> Decimal:
        """Safely coerce config values to Decimal, falling back on mocks/invalid values."""
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return default

    def _ev_shadow_enabled_for_symbol(self, symbol: str) -> bool:
        if not bool(getattr(self.config, "ev_layer_enabled", False)):
            return False
        if not bool(getattr(self.config, "ev_shadow_mode", True)):
            return False
        canary = set(
            self._normalize_symbol_key(s)
            for s in (getattr(self.config, "ev_canary_symbols", []) or [])
            if s
        )
        if not canary:
            return True
        return self._normalize_symbol_key(symbol) in canary
    
    def _get_cache_key(self, symbol: str, candles: List[Candle]) -> Tuple[str, datetime]:
        """Generate cache key from symbol and last candle timestamp."""
        if not candles:
            return (symbol, datetime.min.replace(tzinfo=timezone.utc))
        return (symbol, candles[-1].timestamp)
    
    def _clean_cache(self):
        """Remove stale cache entries."""
        if len(self.indicator_cache) < self.cache_max_size:
            return
            
        now = datetime.now(timezone.utc)
        cutoff = now - self.cache_max_age
        
        # Remove old entries
        self.indicator_cache = {
            k: v for k, v in self.indicator_cache.items()
            if k[1] > cutoff
        }
        
        # If still too large, remove oldest entries
        if len(self.indicator_cache) > self.cache_max_size:
            sorted_keys = sorted(self.indicator_cache.keys(), key=lambda x: x[1], reverse=True)
            self.indicator_cache = {
                k: self.indicator_cache[k] 
                for k in sorted_keys[:self.cache_max_size]
            }

    @staticmethod
    def _normalize_dt(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    def _build_structure_fingerprint(
        self,
        symbol: str,
        decision_tf_label: str,
        structure_type: str,
        structure_timestamp: Optional[Any],
        signal: Signal,
    ) -> str:
        ts = self._normalize_dt(structure_timestamp)
        ts_key = ts.isoformat() if ts else "none"
        return "|".join(
            [
                self._normalize_symbol_key(symbol),
                decision_tf_label,
                signal.signal_type.value,
                structure_type,
                ts_key,
                f"{signal.entry_price:.10f}",
                f"{signal.stop_loss:.10f}",
                f"{signal.take_profit:.10f}" if signal.take_profit is not None else "none",
            ]
        )

    def _prune_signal_fingerprint_cache(self, now: datetime, debounce_window: timedelta) -> None:
        if len(self._signal_fingerprint_last_seen) < self._signal_fingerprint_cache_max_size:
            return
        cutoff = now - (debounce_window * 4)
        self._signal_fingerprint_last_seen = {
            k: v for k, v in self._signal_fingerprint_last_seen.items() if v > cutoff
        }

    def _is_duplicate_structure_signal(self, fingerprint: str, now: datetime, debounce_window: timedelta) -> bool:
        previous = self._signal_fingerprint_last_seen.get(fingerprint)
        if previous and now - previous < debounce_window:
            return True
        self._signal_fingerprint_last_seen[fingerprint] = now
        self._prune_signal_fingerprint_cache(now, debounce_window)
        return False

    @staticmethod
    def _to_weekly_candles(daily_candles: List[Candle]) -> List[Candle]:
        """Aggregate 1D candles into synthetic 1W candles (UTC calendar week)."""
        if not daily_candles:
            return []

        grouped: Dict[Tuple[int, int], List[Candle]] = {}
        for candle in daily_candles:
            iso = candle.timestamp.isocalendar()
            grouped.setdefault((iso.year, iso.week), []).append(candle)

        weekly: List[Candle] = []
        for _, week_rows in sorted(grouped.items(), key=lambda item: item[0]):
            week_rows = sorted(week_rows, key=lambda c: c.timestamp)
            first = week_rows[0]
            last = week_rows[-1]
            weekly.append(
                Candle(
                    timestamp=first.timestamp,
                    symbol=first.symbol,
                    timeframe="1w",
                    open=first.open,
                    high=max(c.high for c in week_rows),
                    low=min(c.low for c in week_rows),
                    close=last.close,
                    volume=sum((c.volume for c in week_rows), Decimal("0")),
                )
            )
        return weekly

    def _detect_daily_bos_bias(self, daily_candles: List[Candle]) -> Literal["bullish", "bearish", "neutral"]:
        if len(daily_candles) < max(10, self.config.bos_confirmation_candles + 5):
            return "neutral"
        bullish_bos = self._detect_break_of_structure(daily_candles, "bullish")
        bearish_bos = self._detect_break_of_structure(daily_candles, "bearish")
        if bullish_bos and not bearish_bos:
            return "bullish"
        if bearish_bos and not bullish_bos:
            return "bearish"
        return "neutral"

    def _detect_higher_tf_context(self, symbol: str) -> HigherTFContext:
        """
        Build 1W->1D context used to guide 4H signal generation.

        v1 is intentionally soft/canary-safe: outside weekly zone applies a score
        penalty later instead of hard-rejecting entries.
        """
        symbol_ctx = self._higher_tf_candle_context.get(symbol, {})
        daily_candles = symbol_ctx.get("1d", [])
        weekly_candles = symbol_ctx.get("1w") or self._to_weekly_candles(daily_candles)
        decision_candles = symbol_ctx.get("4h", [])
        current_price = (decision_candles[-1].close if decision_candles else (daily_candles[-1].close if daily_candles else None))
        if not current_price:
            return HigherTFContext(
                daily_bias="neutral",
            )

        daily_bias = self._detect_daily_bos_bias(daily_candles)
        weekly_fibs = self.fibonacci_engine.calculate_levels(weekly_candles, "1w")
        if not weekly_fibs:
            return HigherTFContext(
                daily_bias=daily_bias,
            )

        zone_low = min(weekly_fibs.fib_0_618, weekly_fibs.fib_0_786)
        zone_high = max(weekly_fibs.fib_0_618, weekly_fibs.fib_0_786)
        inside_zone = zone_low <= current_price <= zone_high
        daily_aligned = daily_bias != "neutral"
        allowed_entry = inside_zone or not bool(getattr(self.config, "higher_tf_enabled", False))

        weekly_weight = float(getattr(self.config, "weekly_fib_confluence_weight", 0.25))
        daily_weight = float(getattr(self.config, "daily_bias_weight", 0.15))
        weekly_confluence_bonus = weekly_weight + (daily_weight if daily_aligned else 0.0)

        return HigherTFContext(
            weekly_fib_zone_low=zone_low,
            weekly_fib_zone_high=zone_high,
            daily_bias=daily_bias,
            allowed_entry=allowed_entry,
            weekly_confluence_bonus=weekly_confluence_bonus if inside_zone else 0.0,
        )
    
    def generate_signal(
        self,
        symbol: str,
        regime_candles_1d: List[Candle],      # Regime layer: EMA200 bias
        decision_candles_4h: List[Candle],    # Decision layer: OB/FVG/BOS detection
        refine_candles_1h: List[Candle],      # Refinement: ADX filter, swing points
        refine_candles_15m: List[Candle],     # Refinement: entry timing
    ) -> Signal:
        """
        Generate trading signal from spot market data.
        
        TIMEFRAME HIERARCHY (Configurable via decision_timeframes):
        - 1D: Regime filter only (EMA200 bias, risk on/off)
        - Decision TF (4H or 1H): DECISION AUTHORITY - all SMC patterns (OB, FVG, BOS)
        - Refinement TFs: Entry timing, swing points
        
        The decision timeframe is determined by config.decision_timeframes[0]:
        - "4h": Use decision_candles_4h for structure detection (production default)
        - "1h": Use refine_candles_1h for structure detection (legacy/comparison mode)
        """
        self.config = resolve_strategy_for_symbol(self._base_config, symbol)

        # Context Variables for Trace
        decision_id = str(uuid.uuid4())
        reasoning_parts = []
        
        # ============================================================
        # DETERMINE DECISION TIMEFRAME FROM CONFIG
        # ============================================================
        # This allows switching between 4H and 1H decision modes for comparison
        decision_tf = self.config.decision_timeframes[0] if hasattr(self.config, 'decision_timeframes') and self.config.decision_timeframes else "4h"
        
        # Select effective decision candles based on config
        if decision_tf == "1h":
            effective_decision_candles = refine_candles_1h
            decision_tf_label = "1H"
        else:
            effective_decision_candles = decision_candles_4h
            decision_tf_label = "4H"
        
        # DEFENSIVE CHECK: Data Integrity
        # Ensure we have minimum required data to function
        if not refine_candles_15m:
            return Signal(
                timestamp=datetime.now(timezone.utc),
                symbol=symbol,
                signal_type=SignalType.NO_SIGNAL,
                entry_price=Decimal("0"),
                stop_loss=Decimal("0"),
                take_profit=None,
                reasoning="ERROR: Missing 15m Data",
                setup_type=SetupType.TREND,
                regime="no_data",
                higher_tf_bias="neutral",
                adx=Decimal("0"),
                atr=Decimal("0"),
                ema200_slope="flat"
            )
            
        if not effective_decision_candles:
             return Signal(
                timestamp=datetime.now(timezone.utc),
                symbol=symbol,
                signal_type=SignalType.NO_SIGNAL,
                entry_price=Decimal("0"),
                stop_loss=Decimal("0"),
                take_profit=None,
                reasoning=f"ERROR: Missing {decision_tf_label} Data (decision timeframe required)",
                setup_type=SetupType.TREND,
                regime="no_data",
                higher_tf_bias="neutral",
                adx=Decimal("0"),
                atr=Decimal("0"),
                ema200_slope="flat"
            )
        
        if not refine_candles_1h:
             return Signal(
                timestamp=datetime.now(timezone.utc),
                symbol=symbol,
                signal_type=SignalType.NO_SIGNAL,
                entry_price=Decimal("0"),
                stop_loss=Decimal("0"),
                take_profit=None,
                reasoning="ERROR: Missing 1h Data",
                setup_type=SetupType.TREND,
                regime="no_data",
                higher_tf_bias="neutral",
                adx=Decimal("0"),
                atr=Decimal("0"),
                ema200_slope="flat"
            )

        # Preserve existing generate_signal interface while enabling HTF context.
        self._higher_tf_candle_context[symbol] = {
            "1d": regime_candles_1d,
            "1w": self._to_weekly_candles(regime_candles_1d),
            "4h": decision_candles_4h,
        }

        bias = "neutral"
        structure_signal = None
        structure_decision = None  # Decision structure (from effective decision TF)
        adx_value = 0.0
        atr_value = Decimal("0")  # Initialize as Decimal to maintain type consistency
        atr_ratio = None
        tp_candidates = []
        used_tolerance = False  # Track if entry zone tolerance was used
        higher_tf_context: Optional[HigherTFContext] = None
        higher_tf_penalty = 0.0
        thesis_snapshot: Optional[Dict[str, Any]] = None
        thesis_score_adj = 0.0
        
        # Logic Flow
        signal = None

        # Step 0: Calculate Indicators (ADX on 1H for fast response, ATR/Fib on decision TF)
        # Cache key based on symbol and last decision candle timestamp
        cache_key = self._get_cache_key(symbol, effective_decision_candles)
        
        # Check cache for indicators
        cached_indicators = self.indicator_cache.get(cache_key)
        
        if cached_indicators:
            # Use cached values
            adx_value = cached_indicators['adx']
            atr_value = cached_indicators['atr']
            fib_levels = cached_indicators['fib_levels']
        else:
            # ADX on 1H for faster response to trend changes (refinement layer)
            adx_df = self.indicators.calculate_adx(refine_candles_1h, self.config.adx_period)
            if not adx_df.empty:
                adx_column = f'ADX_{self.config.adx_period}'
                adx_value = float(adx_df[adx_column].iloc[-1])
            else:
                adx_value = 0.0
            
            # ATR on decision TF for stop sizing
            atr_df = self.indicators.calculate_atr(effective_decision_candles, self.config.atr_period)
            if not atr_df.empty:
                atr_value = Decimal(str(atr_df.iloc[-1]))
            else:
                atr_value = Decimal("0")
            
            # Fib Levels on decision TF for consistency
            fib_levels = self.fibonacci_engine.calculate_levels(effective_decision_candles, decision_tf)
            
            # Store in cache
            self.indicator_cache[cache_key] = {
                'adx': adx_value,
                'atr': atr_value,
                'fib_levels': fib_levels
            }
            
            # Periodic cleanup
            if len(self.indicator_cache) % 100 == 0:
                self._clean_cache()
        

        # Step 1: Higher-timeframe bias (1D EMA200)
        if signal is None:
            bias = self._determine_bias(effective_decision_candles, regime_candles_1d, reasoning_parts)
            # Neutral bias DOES NOT block immediately - waits for Score Gate
            # unless bias determination failed completely (e.g. insufficient data)
            if reasoning_parts and "Insufficient candles" in reasoning_parts[-1]:
                signal = self._no_signal(
                    symbol, 
                    reasoning_parts, 
                    effective_decision_candles[-1] if effective_decision_candles else None,
                    adx=adx_value,
                    atr=atr_value
                )

        # ============================================================
        # CRITICAL GUARD: DECISION TF MUST HAVE VALID STRUCTURE
        # ============================================================
        # The decision timeframe (4H by default, or 1H in legacy mode) must have
        # valid structure. Lower timeframes alone can NEVER generate a trade.
        
        structures = {}
        regime_early = None
        used_fallback_tf = False  # Tier 2 (KBO-13): tracks if 1H fallback was used

        if signal is None:
            # Detect structure on decision timeframe
            structure_decision = self._detect_structure(
                effective_decision_candles,  # Using configured decision TF
                effective_decision_candles,
                bias,
                reasoning_parts,
                symbol=symbol,
            )
            
            # HARD GATE: No decision TF structure = NO TRADE
            has_decision_structure = (
                structure_decision and 
                (structure_decision.get('order_block') or structure_decision.get('fvg') or structure_decision.get('bos'))
            )
            
            decision_structure_ablation = (
                os.getenv("REPLAY_ABLATE_DISABLE_DECISION_STRUCTURE", "0") == "1"
            )
            if not has_decision_structure and decision_structure_ablation:
                logger.info(
                    "Ablation: bypassing decision structure gate in replay",
                    symbol=symbol,
                    decision_tf=decision_tf_label,
                    reason=f"{decision_tf_label}_STRUCTURE_REQUIRED",
                )
                reasoning_parts.append(
                    f"🧪 Ablation: bypass {decision_tf_label} structure-required gate"
                )
                synthetic_ts = (
                    effective_decision_candles[-1].timestamp if effective_decision_candles else None
                )
                structure_decision = {
                    "bos": {
                        "direction": bias,
                        "timestamp": synthetic_ts,
                    }
                }
                has_decision_structure = True

            # Tier 2 (KBO-13): 1H structure fallback when no 4H structure exists
            if (
                not has_decision_structure
                and bool(getattr(self.config, "structure_fallback_enabled", False))
                and decision_tf_label == "4H"
                and refine_candles_1h
            ):
                structure_fallback = self._detect_structure(
                    refine_candles_1h,
                    refine_candles_1h,
                    bias,
                    reasoning_parts,
                    symbol=symbol,
                )
                has_fallback = (
                    structure_fallback
                    and (
                        structure_fallback.get("order_block")
                        or structure_fallback.get("fvg")
                        or structure_fallback.get("bos")
                    )
                )
                if has_fallback:
                    structure_decision = structure_fallback
                    has_decision_structure = True
                    used_fallback_tf = True
                    reasoning_parts.append(
                        "⚠️ 1H Fallback Structure Used (score premium applies)"
                    )
                    logger.info(
                        "1H fallback structure found",
                        symbol=symbol,
                        fallback_structure_type=next(
                            (k for k in ("order_block", "fvg", "bos") if structure_fallback.get(k)),
                            "unknown",
                        ),
                    )

            if not has_decision_structure:
                reasoning_parts.append(f"❌ No valid {decision_tf_label} decision structure")
                logger.info(
                    "Signal rejected: No decision structure",
                    symbol=symbol,
                    decision_tf=decision_tf_label,
                    reason=f"{decision_tf_label}_STRUCTURE_REQUIRED"
                )
                signal = self._no_signal(
                    symbol,
                    reasoning_parts,
                    effective_decision_candles[-1] if effective_decision_candles else None,
                    adx=adx_value,
                    atr=atr_value,
                    regime=f"no_{decision_tf}_structure"
                )
            else:
                # Decision TF structure is valid - proceed
                structure_signal = structure_decision
                structures = structure_decision
                regime_early = self._classify_regime_from_structure(structure_decision)
                reasoning_parts.append(f"✅ {decision_tf_label} Decision Structure Found")
                reasoning_parts.append(f"📊 Market Regime: {regime_early}")
                if getattr(self.config, "higher_tf_enabled", False):
                    higher_tf_context = self._detect_higher_tf_context(symbol)
                    higher_tf_mode = str(getattr(self.config, "higher_tf_mode", "soft") or "soft").lower()
                    logger.info(
                        "higher_tf_context",
                        symbol=symbol,
                        weekly_zone_low=str(higher_tf_context.weekly_fib_zone_low) if higher_tf_context.weekly_fib_zone_low is not None else None,
                        weekly_zone_high=str(higher_tf_context.weekly_fib_zone_high) if higher_tf_context.weekly_fib_zone_high is not None else None,
                        daily_bias=higher_tf_context.daily_bias,
                        allowed_entry=higher_tf_context.allowed_entry,
                        mode=higher_tf_mode,
                    )
                    if (
                        self._memory_manager
                        and self._memory_manager.is_enabled_for_symbol(symbol)
                        and higher_tf_context.weekly_fib_zone_low is not None
                        and higher_tf_context.weekly_fib_zone_high is not None
                        and effective_decision_candles
                    ):
                        latest_price = effective_decision_candles[-1].close
                        recent = effective_decision_candles[-20:] if len(effective_decision_candles) >= 20 else effective_decision_candles
                        avg_volume = (
                            sum((c.volume for c in recent), Decimal("0")) / Decimal(str(len(recent)))
                            if recent else None
                        )
                        self._memory_manager.create_or_refresh_thesis(
                            symbol=symbol,
                            weekly_zone_low=higher_tf_context.weekly_fib_zone_low,
                            weekly_zone_high=higher_tf_context.weekly_fib_zone_high,
                            daily_bias=higher_tf_context.daily_bias,
                            signal_id=decision_id,
                            current_volume_avg=avg_volume,
                        )
                        thesis_snapshot = self._memory_manager.update_conviction_for_symbol(
                            symbol,
                            current_price=latest_price,
                            current_volume_avg=avg_volume,
                        )
                    weekly_zone_ablation = (
                        os.getenv("REPLAY_ABLATE_DISABLE_WEEKLY_ZONE", "0") == "1"
                    )
                    if not higher_tf_context.allowed_entry:
                        if weekly_zone_ablation:
                            logger.info(
                                "Ablation: bypassing weekly-zone gate in replay",
                                symbol=symbol,
                                reason="outside_weekly_zone",
                                mode=higher_tf_mode,
                            )
                            reasoning_parts.append("🧪 Ablation: bypass higher-TF weekly-zone gate")
                            higher_tf_penalty = 0.0
                        elif higher_tf_mode == "hard":
                            logger.info(
                                "higher_tf_hard_rejected",
                                symbol=symbol,
                                reason="outside_weekly_zone",
                            )
                            reasoning_parts.append("❌ Higher-TF hard reject: outside weekly zone")
                            signal = self._no_signal(
                                symbol,
                                reasoning_parts,
                                effective_decision_candles[-1] if effective_decision_candles else None,
                                adx=adx_value,
                                atr=atr_value,
                                regime=regime_early,
                            )
                            return signal
                        else:
                            higher_tf_penalty = float(getattr(self.config, "higher_tf_penalty_outside_zone", -18.0))
                            logger.info(
                                "higher_tf_penalty_applied",
                                symbol=symbol,
                                reason="outside_weekly_zone",
                                penalty=higher_tf_penalty,
                            )
                            reasoning_parts.append(f"⚠ Higher-TF outside weekly zone: penalty {higher_tf_penalty:.1f}")

        import traceback
        
        # Step 2: Market Structure Change Detection & Confirmation (on decision TF)
        # Require structure change confirmation + reconfirmation before entry
        if signal is None:
            # Update market structure tracking using decision timeframe
            ms_state, ms_change = self.ms_tracker.update_structure(symbol, effective_decision_candles)
            
            # V4: Adaptive Confirmation Logic (based on decision TF ATR)
            required_candles = None
            if self.config.adaptive_enabled:
                # Dynamic Logic based on Volatility State
                try:
                    atr_series = self.indicators.calculate_atr(effective_decision_candles, self.config.atr_period)
                    if not atr_series.empty:
                        current_atr = atr_series.iloc[-1]
                        # Use 20-period moving average of ATR as baseline
                        avg_atr = atr_series.rolling(20).mean().iloc[-1]
                        
                        if avg_atr > 0:
                            # Calculate ratio (float for local logic, Decimal for Signal if needed)
                            ratio_val = float(current_atr / avg_atr)
                            atr_ratio = Decimal(str(ratio_val))
                            
                            # Keep ATR as Decimal - don't overwrite with float
                            # atr_value is already set as Decimal in Step 0, preserve it

                            if ratio_val > self.config.atr_confirmation_threshold_high:
                                # High volatility: use extended confirmation (2 candles on 4H = 8 hours)
                                required_candles = getattr(self.config, 'ms_confirmation_candles_high_vol', 2)
                                reasoning_parts.append(f"🌊 High Volatility (ATR Ratio {ratio_val:.2f}) -> Extended 4H Confirmation ({required_candles} candles = {required_candles * 4}h)")
                            elif ratio_val < self.config.atr_confirmation_threshold_low:
                                required_candles = self.config.min_confirmation_candles

                except (ValueError, TypeError, ArithmeticError, KeyError) as e:
                    # Fallback to default if ATR calc fails
                    logger.warning("Adaptive confirmation failed, using default", error=str(e), error_type=type(e).__name__)

            
            ablate_ms_confirmation = os.getenv("REPLAY_ABLATE_DISABLE_MS_CONFIRMATION", "0") == "1"
            ablate_wait_structure_break = os.getenv("REPLAY_ABLATE_DISABLE_WAIT_STRUCTURE_BREAK", "0") == "1"
            ablate_reconfirmation = os.getenv("REPLAY_ABLATE_DISABLE_RECONFIRMATION", "0") == "1"

            if ms_change:
                # Structure change detected - check confirmation (on 4H)
                confirmed = self.ms_tracker.check_confirmation(
                    symbol, 
                    effective_decision_candles,  # Use decision TF for confirmation
                    ms_change,
                    required_candles=required_candles # Dynamic
                )
                if not confirmed and ablate_ms_confirmation:
                    logger.info(
                        "Ablation: bypassing market-structure confirmation gate in replay",
                        symbol=symbol,
                    )
                    reasoning_parts.append("🧪 Ablation: bypass market-structure confirmation gate")
                    confirmed = True
                
                if confirmed:
                    # Structure already detected on 4H in Step 1.5
                    # Use structure_4h (already set above) - no need to re-detect on 1H
                    # structure_signal is already set to structure_4h
                    
                    # V4: RSI Divergence Check (Gate before Reconfirmation) - on 1H for faster response
                    if self.config.rsi_divergence_enabled:
                         rsi_values = self.indicators.calculate_rsi(refine_candles_1h, self.config.rsi_period)
                         divergence = self.indicators.detect_rsi_divergence(refine_candles_1h, rsi_values, self.config.rsi_divergence_lookback)
                         
                         if divergence != "none":
                             # If Bias is Bullish but Bearish Divergence -> Weakness
                             if bias == "bullish" and divergence == "bearish":
                                 reasoning_parts.append(f"⚠️ Bearish RSI Divergence detected against Bullish bias")
                                 # We could reject or reduce size. For now, strict:
                                 # signal = self._no_signal(symbol, reasoning_parts, refine_candles_1h[-1]) 
                                 # Let's just log it for scoring to penalize
                             
                             elif bias == "bearish" and divergence == "bullish":
                                 reasoning_parts.append(f"⚠️ Bullish RSI Divergence detected against Bearish bias")


                    
                    entry_zone = None
                    if structure_signal:
                        # Extract entry zone from 4H structure (order block or FVG)
                        if structure_signal.get('order_block'):
                            ob = structure_signal['order_block']
                            entry_zone = {'low': ob.get('low'), 'high': ob.get('high')}
                        elif structure_signal.get('fvg'):
                            fvg = structure_signal['fvg']
                            entry_zone = {'bottom': fvg.get('bottom'), 'top': fvg.get('top')}
                    
                    # Get ATR value for adaptive tolerance calculation (4H ATR)
                    # Use cached atr_value if available, otherwise calculate with config period
                    if atr_value is None or atr_value == Decimal("0"):
                        if effective_decision_candles and len(effective_decision_candles) >= self.config.atr_period:
                            atr_series = self.indicators.calculate_atr(effective_decision_candles, self.config.atr_period)
                            if len(atr_series) > 0:
                                atr_value = Decimal(str(atr_series.iloc[-1]))
                    
                    # Check if we should skip reconfirmation (for trending markets)
                    skip_reconfirmation = getattr(self.config, 'skip_reconfirmation_in_trends', True)
                    
                    if ablate_reconfirmation:
                        reconfirmed = True
                        used_tolerance = False
                        reasoning_parts.append("🧪 Ablation: bypass reconfirmation gate")
                    elif skip_reconfirmation:
                        # In trending markets, enter immediately after confirmation
                        reconfirmed = True
                        used_tolerance = False
                        reasoning_parts.append(f"✅ 4H Structure confirmed - entering on confirmation (skip reconfirmation)")
                    else:
                        # Original logic: wait for retrace to entry zone
                        # check_reconfirmation uses 15m for entry timing, 4H for structure context
                        reconfirmed, used_tolerance = self.ms_tracker.check_reconfirmation(
                            symbol, refine_candles_15m, effective_decision_candles, ms_change, entry_zone, atr_value
                        )
                        
                        # Track if tolerance was used for score adjustment later
                        if used_tolerance:
                            reasoning_parts.append(f"📍 Entry used tolerance (near zone, not exact)")
                    
                    if not reconfirmed:
                        reasoning_parts.append(
                            f"⏳ 4H Structure change confirmed, waiting for reconfirmation (retrace to entry zone)"
                        )
                        signal = self._no_signal(
                            symbol, 
                            reasoning_parts, 
                            effective_decision_candles[-1] if effective_decision_candles else None,
                            adx=adx_value,
                            atr=atr_value,
                            regime=regime_early
                        )
                else:
                    reasoning_parts.append(
                        f"⏳ 4H Structure change detected ({ms_change.new_state.value}), waiting for confirmation"
                    )
                    signal = self._no_signal(
                        symbol, 
                        reasoning_parts, 
                        effective_decision_candles[-1] if effective_decision_candles else None,
                        adx=adx_value,
                        atr=atr_value,
                        regime=regime_early
                    )
            else:
                # No structure change - check if we're waiting for one
                if not self.ms_tracker.is_entry_ready(symbol):
                    # Check if we should require structure change
                    require_ms_change = getattr(self.config, 'require_ms_change_confirmation', True)
                    if require_ms_change and not ablate_wait_structure_break:
                        reasoning_parts.append(
                            f"⏳ No 4H market structure change detected - waiting for structure break"
                        )
                        signal = self._no_signal(
                            symbol, 
                            reasoning_parts, 
                            effective_decision_candles[-1] if effective_decision_candles else None,
                            adx=adx_value,
                            atr=atr_value,
                            regime=regime_early
                        )
                    elif require_ms_change and ablate_wait_structure_break:
                        logger.info(
                            "Ablation: bypassing wait-for-structure-break gate in replay",
                            symbol=symbol,
                        )
                        reasoning_parts.append("🧪 Ablation: bypass wait-for-structure-break gate")
        
        # Step 2.5: 4H structure already validated in Step 1.5
        # Just handle the case where no structure was found (should be caught by guard above)
        
        if signal is None and structure_signal is None:
             # If we failed to find 4H structure earlier, reject now
             signal = self._no_signal(
                 symbol, 
                 reasoning_parts, 
                 effective_decision_candles[-1] if effective_decision_candles else None,
                 adx=adx_value,
                 atr=atr_value,
                 regime=regime_early
             )

        if signal is None:
            # If entry ready, verify signal direction matches structure change
                if self.ms_tracker.is_entry_ready(symbol):
                    entry_signal = self.ms_tracker.get_entry_signal(symbol)
                    if entry_signal:
                        expected_direction, _ = entry_signal
                        # Verify the structure signal aligns with MS change direction
                        # This will be checked later when we determine signal_type

        # Step 3: Filters (ADX on 1H for faster response)
        if signal is None:
            # Filters applied below using pre-calculated values
            # (Indicator calculation moved to Step 0)

            
            # ADX REGIME FILTER: Skip ranging markets (1H ADX for faster response)
            adx_threshold = float(getattr(self.config, 'adx_threshold', 25.0))
            adx_override = os.getenv("REPLAY_OVERRIDE_ADX_THRESHOLD")
            if adx_override is not None and adx_override.strip():
                try:
                    adx_threshold = float(adx_override)
                except ValueError:
                    logger.warning("Invalid REPLAY_OVERRIDE_ADX_THRESHOLD", value=adx_override)
            if adx_value < adx_threshold:
                reasoning_parts.append(
                    f"❌ Ranging market: ADX {adx_value:.1f} < {adx_threshold} threshold (skip)"
                )
                signal = self._no_signal(
                    symbol, 
                    reasoning_parts, 
                    effective_decision_candles[-1] if effective_decision_candles else None,
                    adx=adx_value,
                    atr=atr_value,
                    regime=regime_early
                )
            
            # Apply other filters (1H for faster response)
            if signal is None and not self._apply_filters(refine_candles_1h, reasoning_parts):
                 signal = self._no_signal(
                     symbol, 
                     reasoning_parts, 
                     effective_decision_candles[-1] if effective_decision_candles else None,
                     adx=adx_value,
                     atr=atr_value,
                     regime=regime_early
                 )

            # Step 4: Calculate Levels (If passed all checks)
            # Uses 4H structure for entry zones, 1H for swing point precision
            if signal is None:
                signal_type, entry_price, stop_loss, take_profit, tp_candidates, classification_info = self._calculate_levels(
                    structure_signal,
                    effective_decision_candles,  # Decision TF for structure/ATR
                    refine_candles_1h,    # 1H for swing point precision
                    bias,
                    reasoning_parts,
                    atr_value=atr_value,  # Pass cached 4H ATR
                    symbol=symbol,  # For stop widening after recent stop-outs
                )
                
                # Step 5: Fib Validation (Gate for tight_smc) - use cached 4H fib_levels
                fib_valid = True
                
                setup_type = classification_info['setup_type']
                regime = classification_info['regime']
                
                if regime == "tight_smc":
                    # HARD REQUIREMENT: Must be in OTE or near key level
                    if fib_levels:
                        # Check OTE
                        in_ote = self.fibonacci_engine.is_in_ote_zone(entry_price, fib_levels)
                        # Check specific levels
                        fib_proximity_bps = float(getattr(self.config, "fib_proximity_bps", 20.0))
                        fib_bps_override = os.getenv("REPLAY_OVERRIDE_FIB_PROXIMITY_BPS")
                        if fib_bps_override is not None and fib_bps_override.strip():
                            try:
                                fib_proximity_bps = float(fib_bps_override)
                            except ValueError:
                                logger.warning(
                                    "Invalid REPLAY_OVERRIDE_FIB_PROXIMITY_BPS",
                                    value=fib_bps_override,
                                )
                        is_near, _ = self.fibonacci_engine.check_confluence(
                            entry_price, 
                            fib_levels, 
                            tolerance_pct=fib_proximity_bps / 10000
                        )
                        if not (in_ote or is_near):
                            reasoning_parts.append(f"❌ Rejected: tight_smc entry not in OTE/Key Fib (Gate)")
                            fib_valid = False
                        else:
                            reasoning_parts.append(f"✓ Fib requirement passed for tight_smc")
                    else:
                        # If no fib levels found, default to rejection for tight_smc safety
                        reasoning_parts.append(f"❌ Rejected: No Fib structure found for tight_smc")
                        fib_valid = False
                
                if not fib_valid:
                     signal = self._no_signal(
                         symbol, 
                         reasoning_parts, 
                         effective_decision_candles[-1],
                         adx=adx_value,
                         atr=atr_value,
                         regime=regime
                     )
                
                # Step 5.5: Verify signal direction matches MS change (if entry ready)
                if signal is None and signal_type != SignalType.NO_SIGNAL:
                    if self.ms_tracker.is_entry_ready(symbol):
                        entry_signal = self.ms_tracker.get_entry_signal(symbol)
                        if entry_signal:
                            expected_direction, _ = entry_signal
                            # Verify signal direction matches MS change
                            if expected_direction == "LONG" and signal_type != SignalType.LONG:
                                reasoning_parts.append(
                                    f"❌ Signal direction mismatch: MS change expects LONG but got {signal_type.value}"
                                )
                                signal = self._no_signal(
                                    symbol, 
                                    reasoning_parts, 
                                    effective_decision_candles[-1] if effective_decision_candles else None,
                                    adx=adx_value,
                                    atr=atr_value,
                                    regime=regime
                                )
                            elif expected_direction == "SHORT" and signal_type != SignalType.SHORT:
                                reasoning_parts.append(
                                    f"❌ Signal direction mismatch: MS change expects SHORT but got {signal_type.value}"
                                )
                                signal = self._no_signal(
                                    symbol, 
                                    reasoning_parts, 
                                    effective_decision_candles[-1] if effective_decision_candles else None,
                                    adx=adx_value,
                                    atr=atr_value,
                                    regime=regime
                                )
                            else:
                                reasoning_parts.append(
                                    f"✓ Signal direction matches MS change ({expected_direction})"
                                )
                
                # Step 6: Scoring & Final Validation
                if signal is None and signal_type != SignalType.NO_SIGNAL:
                    # Metadata - use 4H candle as reference (decision timeframe)
                    current_candle = effective_decision_candles[-1]
                    timestamp = current_candle.timestamp
                    ema_values = self.indicators.calculate_ema(regime_candles_1d, self.config.ema_period)
                    ema200_slope = self.indicators.get_ema_slope(ema_values) if not ema_values.empty else "flat"
                    
                    # Create TEMP signal for scoring
                    # Ensure ADX and ATR are Decimal types
                    temp_signal = Signal(
                        timestamp=timestamp,
                        symbol=symbol,
                        signal_type=signal_type,
                        entry_price=entry_price,
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                        reasoning="",
                        setup_type=setup_type,
                        regime=regime,
                        higher_tf_bias=bias,
                        adx=Decimal(str(adx_value)),
                        atr=atr_value if isinstance(atr_value, Decimal) else Decimal(str(atr_value)),
                        atr_ratio=atr_ratio,
                        ema200_slope=ema200_slope,
                        tp_candidates=tp_candidates
                    )
                    
                    # Estimate cost (rough bps)
                    cost_bps = Decimal("15.0") # Baseline assumption
                    
                    # Score
                    score_obj = self.signal_scorer.score_signal(
                        temp_signal,
                        structure_signal,
                        fib_levels,
                        adx_value,
                        cost_bps,
                        bias
                    )
                    if getattr(self.config, "higher_tf_enabled", False) and higher_tf_context:
                        score_obj.total_score += higher_tf_context.weekly_confluence_bonus
                        reasoning_parts.append(
                            f"📊 Higher-TF bonus applied: +{higher_tf_context.weekly_confluence_bonus:.2f}"
                        )
                    if getattr(self.config, "higher_tf_enabled", False) and higher_tf_penalty != 0.0:
                        score_obj.total_score += higher_tf_penalty
                        reasoning_parts.append(
                            f"📊 Higher-TF penalty applied: {higher_tf_penalty:.2f}"
                        )
                    if (
                        self._memory_manager
                        and bool(getattr(self.config, "thesis_score_enabled", False))
                        and not bool(getattr(self.config, "thesis_observe_only", True))
                        and thesis_snapshot is not None
                    ):
                        conviction_val = float(thesis_snapshot.get("conviction", 0.0))
                        thesis_score_adj = self._memory_manager.conviction_score_adjustment(conviction_val)
                        score_obj.total_score += thesis_score_adj
                        reasoning_parts.append(
                            f"📊 Thesis conviction adjustment: {thesis_score_adj:+.2f} (conv={conviction_val:.1f})"
                        )
                    
                    # Apply tolerance penalty if entry used zone tolerance
                    if used_tolerance:
                        penalty = self.entry_zone_tolerance_score_penalty
                        score_obj.total_score += penalty  # penalty is negative
                        reasoning_parts.append(f"📊 Tolerance penalty applied: {penalty} points")

                    # Phase C1: EV/Bayesian shadow trace (no behavior changes).
                    if self._ev_shadow_enabled_for_symbol(symbol):
                        ev_inputs = {
                            "symbol": symbol,
                            "conviction": float(thesis_snapshot.get("conviction", 20.0)) if thesis_snapshot else 20.0,
                            "regime": regime,
                            "risk_r": 1.0,  # placeholder for C1
                        }
                        ev_result = self.ev_engine.compute(ev_inputs)
                        self.ev_engine.log_ev_trace(ev_result, ev_inputs)
                    
                    # Tier 2 (KBO-13): Apply score premium for 1H fallback signals
                    if used_fallback_tf:
                        fallback_premium = float(
                            getattr(self.config, "structure_fallback_score_premium", 15.0)
                        )
                        score_obj.total_score -= fallback_premium
                        reasoning_parts.append(
                            f"📊 1H fallback score premium: -{fallback_premium:.0f} points"
                        )

                    # GATE: Check score
                    passed, threshold = self.signal_scorer.check_score_gate(score_obj.total_score, setup_type, bias)
                    
                    if not passed:
                        score_breakdown = {
                            "smc": float(score_obj.smc_quality),
                            "fib": float(score_obj.fib_confluence),
                            "htf": float(score_obj.htf_alignment),
                            "adx": float(score_obj.adx_strength),
                            "cost": float(score_obj.cost_efficiency),
                            "higher_tf_bonus": float(higher_tf_context.weekly_confluence_bonus) if higher_tf_context else 0.0,
                            "higher_tf_penalty": float(higher_tf_penalty),
                            "thesis_conviction": float(thesis_snapshot.get("conviction", 0.0)) if thesis_snapshot else 0.0,
                            "thesis_score_adj": float(thesis_score_adj),
                            "total": float(score_obj.total_score),
                            "threshold": float(threshold),
                        }
                        reasoning_parts.append(f"❌ Score {score_obj.total_score:.1f} < Threshold {threshold} (Grade: {score_obj.get_grade()})")
                        signal = self._no_signal(
                            symbol, 
                            reasoning_parts, 
                            current_candle, 
                            score_breakdown=score_breakdown,
                            adx=adx_value,
                            atr=atr_value,
                            regime=regime
                        )
                        
                        # LOG REJECTION (Mandatory)
                        logger.info(
                            "Signal Rejected (Score)",
                            symbol=symbol,
                            reason=f"Score {score_obj.total_score} < {threshold}",
                            setup=setup_type.value,
                            bias=bias,
                            adx=adx_value,
                            fib_confluence=fib_valid,
                            score_breakdown=score_breakdown,
                        )
                    else:
                        conviction_entry_gate_enabled = bool(getattr(self.config, "thesis_management_enabled", False)) and not bool(
                            getattr(self.config, "thesis_observe_only", True)
                        )
                        conviction_min_for_entry = self._resolve_conviction_min_for_entry(symbol)
                        conviction_val = float(thesis_snapshot.get("conviction", 100.0)) if thesis_snapshot else 100.0
                        if conviction_entry_gate_enabled and conviction_val < conviction_min_for_entry:
                            score_breakdown = {
                                "smc": float(score_obj.smc_quality),
                                "fib": float(score_obj.fib_confluence),
                                "htf": float(score_obj.htf_alignment),
                                "adx": float(score_obj.adx_strength),
                                "cost": float(score_obj.cost_efficiency),
                                "higher_tf_bonus": float(higher_tf_context.weekly_confluence_bonus) if higher_tf_context else 0.0,
                                "higher_tf_penalty": float(higher_tf_penalty),
                                "thesis_conviction": conviction_val,
                                "thesis_score_adj": float(thesis_score_adj),
                                "conviction_min_for_entry": conviction_min_for_entry,
                                "total": float(score_obj.total_score),
                                "threshold": float(threshold),
                            }
                            logger.info(
                                "ENTRY_BLOCKED_LOW_CONVICTION",
                                symbol=symbol,
                                conviction=conviction_val,
                                conviction_min_for_entry=conviction_min_for_entry,
                                setup=setup_type.value,
                                score=float(score_obj.total_score),
                            )
                            signal = self._no_signal(
                                symbol,
                                reasoning_parts
                                + [
                                    f"❌ Entry blocked by thesis conviction gate ({conviction_val:.1f} < {conviction_min_for_entry:.1f})"
                                ],
                                current_candle,
                                score_breakdown=score_breakdown,
                                adx=adx_value,
                                atr=atr_value,
                                regime=regime,
                            )
                            return signal

                        if conviction_entry_gate_enabled and thesis_snapshot is not None and self._conviction_stop_sizing_enabled_for_symbol(symbol):
                            high_threshold = float(getattr(self.config, "conviction_stop_high_threshold", 60.0))
                            high_multiplier = Decimal(str(getattr(self.config, "conviction_stop_high_multiplier", 1.5)))
                            if conviction_val >= high_threshold and high_multiplier > Decimal("1"):
                                original_stop = stop_loss
                                risk_distance = abs(entry_price - original_stop)
                                widened_distance = risk_distance * high_multiplier
                                if signal_type == SignalType.LONG:
                                    widened_stop = entry_price - widened_distance
                                else:
                                    widened_stop = entry_price + widened_distance
                                if widened_stop > Decimal("0"):
                                    stop_loss = widened_stop
                                    reasoning_parts.append(
                                        f"📈 Conviction stop widening applied: x{float(high_multiplier):.2f} "
                                        f"(conv={conviction_val:.1f}, stop {original_stop} -> {stop_loss})"
                                    )
                                    logger.info(
                                        "CONVICTION_STOP_WIDENED",
                                        symbol=symbol,
                                        conviction=conviction_val,
                                        threshold=high_threshold,
                                        multiplier=float(high_multiplier),
                                        signal_type=signal_type.value,
                                        entry=str(entry_price),
                                        old_stop=str(original_stop),
                                        new_stop=str(stop_loss),
                                    )

                        reasoning_parts.append(f"✓ Score Passed: {score_obj.total_score:.1f} >= {threshold}")
                        
                        # Create FINAL signal
                        # Ensure ADX and ATR are Decimal types
                        signal = Signal(
                            timestamp=timestamp,
                            symbol=symbol,
                            signal_type=signal_type,
                            entry_price=entry_price,
                            stop_loss=stop_loss,
                            take_profit=take_profit,
                            reasoning="\n".join(reasoning_parts),
                            setup_type=setup_type,
                            regime=regime,
                            higher_tf_bias=bias,
                            adx=Decimal(str(adx_value)),
                            atr=atr_value if isinstance(atr_value, Decimal) else Decimal(str(atr_value)),
                            atr_ratio=atr_ratio,
                            ema200_slope=ema200_slope,
                            tp_candidates=tp_candidates,
                            score=score_obj.total_score,
                            score_breakdown={
                                "smc": score_obj.smc_quality,
                                "fib": score_obj.fib_confluence,
                                "htf": score_obj.htf_alignment,
                                "adx": score_obj.adx_strength,
                                "cost": score_obj.cost_efficiency,
                                "ema_slope": score_obj.ema_slope,
                                "higher_tf_bonus": higher_tf_context.weekly_confluence_bonus if higher_tf_context else 0.0,
                                "higher_tf_penalty": higher_tf_penalty,
                                "thesis_conviction": thesis_snapshot.get("conviction") if thesis_snapshot else 0.0,
                                "thesis_score_adj": thesis_score_adj,
                            },
                            structure_info=structure_signal or {},
                            meta_info={
                                "fib_levels": {k: float(v) for k, v in vars(fib_levels).items() if isinstance(v, Decimal)} if fib_levels else {},
                                "filters": {
                                    "adx": float(adx_value),
                                    "atr": float(atr_value) if isinstance(atr_value, Decimal) else atr_value
                                },
                                "thesis": thesis_snapshot or {},
                                "decision_id": decision_id,
                                "structure_source": "1h_fallback" if used_fallback_tf else "4h_decision",
                            }
                        )
                elif signal is None:
                     # Calculate levels returned None (should be handled by signal_type check but safe fallback)
                     signal = self._no_signal(
                         symbol, 
                         reasoning_parts, 
                         effective_decision_candles[-1],
                         adx=adx_value,
                         atr=atr_value
                     )

        # If signal is still None after all steps
        if signal is None:
            current_candle = effective_decision_candles[-1] if effective_decision_candles else None
            timestamp = current_candle.timestamp if current_candle else datetime.now(timezone.utc)
            signal = self._no_signal(
                symbol, 
                reasoning_parts, 
                current_candle,
                adx=adx_value,
                atr=atr_value
            )


        # EVENT RECORDING DESIGN:
        # - DECISION_TRACE: Recorded by LiveTrading for all signals (including NO_SIGNAL) for dashboard coverage
        # - SIGNAL_GENERATED: Recorded here only for actual trading signals (not NO_SIGNAL) for explicit signal tracking
        # These serve different purposes: DECISION_TRACE is for dashboard/audit trail, SIGNAL_GENERATED is for signal lifecycle tracking
        # If you want a single source of truth, remove SIGNAL_GENERATED here and let LiveTrading handle all event recording

        if signal.signal_type != SignalType.NO_SIGNAL:
            # Structured logging showing 4H decision authority hierarchy
            structure_type = "OB" if structure_signal and structure_signal.get('order_block') else (
                "FVG" if structure_signal and structure_signal.get('fvg') else "BOS"
            )
            structure_ts = None
            if structure_signal:
                if structure_signal.get('order_block'):
                    structure_ts = structure_signal['order_block'].get('timestamp')
                elif structure_signal.get('fvg'):
                    structure_ts = structure_signal['fvg'].get('timestamp')

            dedupe_minutes = int(getattr(self.config, "signal_structure_dedupe_minutes", 0) or 0)
            dedupe_enabled = bool(getattr(self.config, "signal_structure_dedupe_enabled", False)) and dedupe_minutes > 0
            dedupe_override = os.getenv("REPLAY_OVERRIDE_STRUCTURE_DEDUPE_MINUTES")
            if dedupe_override is not None and dedupe_override.strip():
                try:
                    dedupe_minutes = max(0, int(float(dedupe_override)))
                    dedupe_enabled = dedupe_minutes > 0
                except ValueError:
                    logger.warning(
                        "Invalid REPLAY_OVERRIDE_STRUCTURE_DEDUPE_MINUTES",
                        value=dedupe_override,
                    )
            if dedupe_enabled:
                now_utc = datetime.now(timezone.utc)
                debounce_window = timedelta(minutes=dedupe_minutes)
                fingerprint = self._build_structure_fingerprint(
                    symbol=symbol,
                    decision_tf_label=decision_tf_label,
                    structure_type=structure_type,
                    structure_timestamp=structure_ts,
                    signal=signal,
                )
                if self._is_duplicate_structure_signal(fingerprint, now_utc, debounce_window):
                    logger.info(
                        "SIGNAL_SUPPRESSED_DUPLICATE_STRUCTURE",
                        symbol=symbol,
                        decision_tf=decision_tf_label,
                        structure_type=structure_type,
                        structure_timestamp=str(structure_ts) if structure_ts else None,
                        debounce_minutes=dedupe_minutes,
                        fingerprint=fingerprint,
                    )
                    signal = self._no_signal(
                        symbol,
                        reasoning_parts + [f"⏸ Duplicate structure suppressed for {dedupe_minutes}m debounce window"],
                        effective_decision_candles[-1] if effective_decision_candles else None,
                        adx=adx_value,
                        atr=atr_value if isinstance(atr_value, Decimal) else Decimal(str(atr_value)),
                        regime=signal.regime,
                    )
                    return signal

            logger.info(
                "Signal generated with 4H decision authority",
                symbol=symbol,
                decision_tf="4H",
                structure_type=structure_type,
                structure_timestamp=str(structure_ts) if structure_ts else None,
                entry_tf="15m",
                stop_tf="4H_ATR",
                atr_value=str(atr_value),
                signal_type=signal.signal_type.value,
                entry=str(signal.entry_price),
                stop=str(signal.stop_loss),
            )
            self._maybe_emit_dca_suggestion(signal)
            # Record explicit signal event (design choice: signal source records its own generation)
            self._record_event(
                "SIGNAL_GENERATED", 
                symbol, 
                {
                    "type": signal.signal_type.value,
                    "entry": float(signal.entry_price),
                    "stop": float(signal.stop_loss),
                    "tp": float(signal.take_profit) if signal.take_profit else None,
                    "tp_candidates": [float(tp) for tp in signal.tp_candidates],
                    "decision_tf": "4H",
                    "structure_type": structure_type,
                    "stop_tf": "4H_ATR",
                    "thesis_conviction": thesis_snapshot.get("conviction") if thesis_snapshot else None,
                    "thesis_status": thesis_snapshot.get("status") if thesis_snapshot else None,
                },
                decision_id=decision_id
            )
        
        return signal

    def _maybe_emit_dca_suggestion(self, signal: Signal) -> None:
        """
        Telemetry-only spot DCA suggestion channel via Telegram alerting.
        """
        if os.getenv("DCA_SUGGESTIONS_TELEGRAM_ENABLED", "").strip().lower() not in ("1", "true", "yes", "on"):
            return
        if signal.signal_type == SignalType.NO_SIGNAL:
            return
        min_score = float(os.getenv("DCA_SUGGESTIONS_MIN_SCORE", "70"))
        score = float(signal.score or 0.0)
        if score < min_score:
            return
        try:
            from src.monitoring.telegram_bot import send_telegram_message_sync

            entry = float(signal.entry_price) if signal.entry_price else None
            stop = float(signal.stop_loss) if signal.stop_loss else None
            tp1 = float(signal.take_profit) if signal.take_profit else None
            send_telegram_message_sync(
                (
                    f"Spot DCA suggestion (telemetry-only)\n"
                    f"symbol={signal.symbol} side={signal.signal_type.value} setup={signal.setup_type.value} regime={signal.regime}\n"
                    f"score={score:.1f} entry={entry} stop={stop} tp1={tp1}\n"
                    f"breakdown={signal.score_breakdown or {}}"
                )
            )
        except (OperationalError, DataError, OSError, ImportError, RuntimeError):
            logger.debug("DCA suggestion telemetry send failed (non-fatal)", symbol=signal.symbol)
    
    def _classify_setup(
        self,
        structures: dict,
        signal_type: SignalType,
    ) -> tuple[str, str]:
        """
        Classify setup type for regime determination.
        
        Returns:
            (setup_type, regime) tuple
            
        Priority (highest first):
        1. If Order Block present → ("ob", "tight_smc")
        2. If Fair Value Gap present → ("fvg", "tight_smc")
        3. If Break of Structure confirmed → ("bos", "wide_structure")
        4. Else (HTF trend only) → ("trend", "wide_structure")
        """
        from src.domain.models import SetupType
        
        if structures.get("order_block"):  # Fixed: was "orderblock"
            return (SetupType.OB, "tight_smc")
        
        elif structures.get("fvg"):
            return (SetupType.FVG, "tight_smc")
        
        elif structures.get("bos"):  # Fixed: was "bos_confirmed"
            return (SetupType.BOS, "wide_structure")
        
        else:
            # HTF trend following only
            return (SetupType.TREND, "wide_structure")
    
    def _classify_regime_from_structure(self, structure: dict) -> str:
        """
        Classify regime from detected structure (early classification).
        
        This is called immediately after structure detection to ensure
        rejected signals still show the correct regime on the dashboard.
        
        Returns:
            regime string: "tight_smc" or "wide_structure"
        
        Priority (highest first):
        1. If Order Block present → "tight_smc"
        2. If Fair Value Gap present → "tight_smc"
        3. If Break of Structure confirmed → "wide_structure"
        4. Else (HTF trend only) → "wide_structure"
        """
        if structure.get("order_block"):
            return "tight_smc"
        elif structure.get("fvg"):
            return "tight_smc"
        elif structure.get("bos"):
            return "wide_structure"
        else:
            # HTF trend following only
            return "wide_structure"
    
    def _determine_bias(
        self,
        candles_4h: List[Candle],
        candles_1d: List[Candle],
        reasoning: List[str],
    ) -> str:
        """
        Determine higher-timeframe bias (bullish/bearish/neutral).
        
        Rules:
        - Price > EMA200 -> Bullish
        - Price < EMA200 -> Bearish
        - abs(Price - EMA200) < configurable bps -> Neutral
        - EMA Slope: CONTRIBUTES TO SCORE ONLY (Does not block bias)
        """
        if not candles_4h or not candles_1d:
            reasoning.append("❌ Insufficient candles for bias determination")
            return "neutral"
        
        # EMA 200 on 1D
        ema_1d = self.indicators.calculate_ema(candles_1d, self.config.ema_period)
        
        if ema_1d.empty or len(ema_1d) < 1:
            reasoning.append("❌ EMA 200 not available on 1D")
            return "neutral"
        
        current_price = candles_1d[-1].close
        ema_value = Decimal(str(ema_1d.iloc[-1]))
        
        # Guard against division by zero
        if ema_value == Decimal("0"):
            reasoning.append("❌ EMA value is zero - cannot determine bias")
            return "neutral"
        
        # Check Neutral Zone
        dist_bps = abs(current_price - ema_value) / ema_value * Decimal("10000")
        neutral_zone = Decimal(str(self.config.ema_neutral_zone_bps))
        
        if dist_bps < neutral_zone:
            reasoning.append(f"○ Bias Neutral: Price vs EMA dist {dist_bps:.1f} bps < {neutral_zone}")
            return "neutral"
        
        # Direction
        if current_price > ema_value:
             reasoning.append(f"✓ Bias Bullish: Price ${current_price} > EMA ${ema_value}")
             return "bullish"
        else:
             reasoning.append(f"✓ Bias Bearish: Price ${current_price} < EMA ${ema_value}")
             return "bearish"
    
    def _detect_structure(
        self,
        candles_15m: List[Candle],
        candles_1h: List[Candle],
        bias: str,
        reasoning: List[str],
        symbol: Optional[str] = None,
    ) -> Optional[dict]:
        """
        Detect SMC structure (order blocks, FVGs, break of structure).
        
        Returns:
            Dict with structure details or None if no valid structure
        """
        if not candles_1h or len(candles_1h) < self.config.orderblock_lookback:
            reasoning.append("❌ Insufficient candles for structure detection")
            return None
        
        # Detect order blocks
        order_block = self._find_order_block(candles_1h, bias)
        
        if not order_block:
            reasoning.append("❌ No valid order block found")
            return None
        
        reasoning.append(
            f"✓ Order block detected: {order_block['type']} at ${order_block['price']}"
        )
        
        # Detect fair value gaps
        fvg = self._find_fair_value_gap(candles_1h, bias, symbol=symbol)
        
        if fvg:
            reasoning.append(f"✓ Fair value gap detected at ${fvg['price']}")
        
        # Detect break of structure (configurable requirement for trade validity)
        bos = self._detect_break_of_structure(candles_1h, bias)
        
        # Check if BOS is required (configurable)
        require_bos = getattr(self.config, 'require_bos_confirmation', False)
        
        if require_bos and not bos:
            reasoning.append("❌ No break of structure - waiting for confirmation (BOS required)")
            return None
        
        if bos:
            reasoning.append(f"✓ Break of structure confirmed")
        else:
            reasoning.append("○ No break of structure yet (not required)")
        
        return {
            'order_block': order_block,
            'fvg': fvg,
            'bos': bos,
        }
    
    def _find_order_block(self, candles: List[Candle], bias: str) -> Optional[dict]:
        """
        Find SMC-style Order Block:
        - Bullish OB: last DOWN candle before an impulsive UP move (displacement)
        - Bearish OB: last UP candle before an impulsive DOWN move (displacement)
        
        Returns a zone: {'type', 'index', 'low', 'high', 'timestamp', 'price'}
        """
        if len(candles) < 3:
            return None
            
        lookback = min(self.config.orderblock_lookback, len(candles) - 3)
        
        # Calculate volatility-adjusted displacement threshold
        recent_ranges = [abs(c.high - c.low) for c in candles[-20:]]
        typical_range = sorted(recent_ranges)[len(recent_ranges)//2]
        min_displacement = typical_range * Decimal("1.5")
        
        # Iterate backwards to find the most recent valid OB
        for i in range(len(candles) - 2, len(candles) - lookback - 2, -1):
            cand = candles[i]
            nxt = candles[i + 1]
            
            if bias == "bullish":
                # 1. Origin must be a bearish candle
                if cand.close < cand.open:
                    # 2. Must be followed by an impulsive move up (displacement)
                    # The displacement must break the high of the OB candle
                    move = nxt.close - cand.high
                    if move > 0 and (nxt.high - nxt.low) >= min_displacement:
                        # Calculate entry price based on configured mode
                        if self.config.ob_entry_mode == "mid":
                            entry_price = (cand.high + cand.low) / Decimal("2")
                        elif self.config.ob_entry_mode == "open":
                            entry_price = cand.open
                        elif self.config.ob_entry_mode == "discount":
                            # Enter at discount (lower in the OB zone)
                            discount_pct = Decimal(str(self.config.ob_discount_pct))
                            entry_price = cand.low + (cand.high - cand.low) * discount_pct
                        else:  # high_low (legacy)
                            entry_price = cand.high
                        
                        return {
                            "type": "bullish",
                            "index": i,
                            "timestamp": cand.timestamp,
                            "low": cand.low,
                            "high": cand.high,
                            "price": entry_price
                        }
            else: # bearish
                # 1. Origin must be a bullish candle
                if cand.close > cand.open:
                    # 2. Must be followed by an impulsive move down
                    move = cand.low - nxt.close
                    if move > 0 and (nxt.high - nxt.low) >= min_displacement:
                        # Calculate entry price based on configured mode
                        if self.config.ob_entry_mode == "mid":
                            entry_price = (cand.high + cand.low) / Decimal("2")
                        elif self.config.ob_entry_mode == "open":
                            entry_price = cand.open
                        elif self.config.ob_entry_mode == "discount":
                            # Enter at discount (higher in the OB zone for shorts)
                            discount_pct = Decimal(str(self.config.ob_discount_pct))
                            entry_price = cand.high - (cand.high - cand.low) * discount_pct
                        else:  # high_low (legacy)
                            entry_price = cand.low
                        
                        return {
                            "type": "bearish",
                            "index": i,
                            "timestamp": cand.timestamp,
                            "low": cand.low,
                            "high": cand.high,
                            "price": entry_price
                        }
        return None
    
    def _find_fair_value_gap(
        self,
        candles: List[Candle],
        bias: str,
        symbol: Optional[str] = None,
    ) -> Optional[dict]:
        """
        Find most recent UNMITIGATED FVG.
        """
        if len(candles) < 3:
            return None

        # Iterate backwards from current candle
        for i in range(len(candles) - 3, -1, -1):
            c1, c2, c3 = candles[i], candles[i+1], candles[i+2]
            
            # Check for gap formation
            gap_zone = None
            if bias == "bullish" and c3.low > c1.high:
                gap_zone = (c1.high, c3.low)
            elif bias == "bearish" and c1.low > c3.high:
                gap_zone = (c3.high, c1.low)
                
            if gap_zone:
                gap_size = gap_zone[1] - gap_zone[0]
                reference_price = abs(c2.close) if c2.close else abs(gap_zone[1])
                if reference_price <= 0:
                    continue
                gap_size_pct = gap_size / reference_price
                min_gap_size_pct = self._resolve_fvg_min_size_pct(symbol)
                if gap_size_pct < min_gap_size_pct:
                    continue

                # Check for mitigation by any candle AFTER the gap formation (from i+3 to end)
                # If any candle's wick enters the gap, it is mitigated.
                mitigated = False
                for j in range(i + 3, len(candles)):
                    fc = candles[j]
                    if bias == "bullish":
                        if fc.low <= gap_zone[1]: # Price returned to or below gap top
                            mitigated = True
                            break
                    else:
                        if fc.high >= gap_zone[0]: # Price returned to or above gap bottom
                            mitigated = True
                            break
                
                if not mitigated:
                    return {
                        "type": "bullish" if bias == "bullish" else "bearish",
                        "index": i,
                        "timestamp": c2.timestamp,
                        "bottom": gap_zone[0],
                        "top": gap_zone[1],
                        "size": gap_zone[1] - gap_zone[0],
                        "price": gap_zone[1] if bias == "bullish" else gap_zone[0]
                    }
        return None
    
    def _detect_break_of_structure(self, candles: List[Candle], bias: str) -> bool:
        """Detect break of structure (BOS), optionally with volume confirmation."""
        if len(candles) < self.config.bos_confirmation_candles + 5:
            return False

        # Simple BOS: recent candles break previous swing high/low
        recent = candles[-self.config.bos_confirmation_candles:]
        previous = candles[-10:-self.config.bos_confirmation_candles]

        if bias == "bullish":
            prev_high = max(c.high for c in previous)
            recent_high = max(c.high for c in recent)
            structure_broken = recent_high > prev_high
        else:  # bearish
            prev_low = min(c.low for c in previous)
            recent_low = min(c.low for c in recent)
            structure_broken = recent_low < prev_low

        if not structure_broken:
            return False

        # Volume confirmation gate (disabled by default for safe rollout)
        if self.config.bos_volume_confirmation_enabled:
            vol_lookback = min(20, len(candles) - self.config.bos_confirmation_candles)
            if vol_lookback >= 5:
                lookback_candles = candles[-(self.config.bos_confirmation_candles + vol_lookback):-self.config.bos_confirmation_candles]
                avg_vol = sum(c.volume for c in lookback_candles) / len(lookback_candles)
                if avg_vol > 0:
                    # Break candle = the candle that made the extreme in recent
                    if bias == "bullish":
                        break_candle = max(recent, key=lambda c: c.high)
                    else:
                        break_candle = min(recent, key=lambda c: c.low)
                    if break_candle.volume < avg_vol * Decimal(str(self.config.bos_volume_threshold_mult)):
                        return False

        return True
    
    def _apply_filters(self, candles: List[Candle], reasoning: List[str]) -> bool:
        """Apply ADX and ATR filters."""
        # ADX filter
        adx_df = self.indicators.calculate_adx(candles, self.config.adx_period)
        
        if adx_df.empty:
            reasoning.append("❌ ADX not available")
            return False
        
        adx_column = f'ADX_{self.config.adx_period}'
        adx_value = adx_df[adx_column].iloc[-1]
        
        # ATR check (ensure volatility is measurable)
        atr_values = self.indicators.calculate_atr(candles, self.config.atr_period)
        if atr_values.empty:
            reasoning.append("❌ ATR not available")
            return False
            
        return True
    
    def _calculate_levels(
        self,
        structure: dict,
        decision_candles: List[Candle],  # Decision TF for structure zones and ATR
        refine_candles_1h: List[Candle],    # 1H for swing point precision
        bias: str,
        reasoning: List[str],
        atr_value: Optional[Decimal] = None,
        symbol: Optional[str] = None,
    ) -> Tuple[SignalType, Decimal, Decimal, Optional[Decimal], List[Decimal], Dict]:
        """
        Calculate Levels: Entry, Stop, TP.
        
        Uses decision TF for structure zones and ATR (decision authority).
        Uses 1H for swing point detection (precision).
        
        Regime:
        - tight_smc: Stop = Invalid + 0.15-0.30 ATR, TP = 2.0R min
        - wide_structure: Stop = Invalid + 0.50-0.60 ATR, TP = 1.5R min
        """
        from src.domain.models import SetupType
        
        order_block = structure['order_block']
        fvg = structure['fvg']
        bos = structure['bos']
        
        # 1. Classify Setup Type & Regime First
        setup_type = SetupType.TREND
        regime = "wide_structure"
        
        if order_block:
            setup_type = SetupType.OB
            regime = "tight_smc"
        elif fvg:
            setup_type = SetupType.FVG
            regime = "tight_smc"
        elif bos:
            setup_type = SetupType.BOS
            regime = "wide_structure"
            
        # 2. Get ATR from decision timeframe - use cached if available
        if atr_value is None:
            atr_values = self.indicators.calculate_atr(decision_candles, self.config.atr_period)
            atr = Decimal(str(atr_values.iloc[-1])) if not atr_values.empty else Decimal("0")
        else:
            atr = Decimal(str(atr_value))  # Use cached decision TF ATR value, ensure Decimal
        
        # 3. Determine Stop Multiplier based on Regime
        if regime == "tight_smc":
            # Randomize or fixed? Use average or min for now to be safe.
            # Config has ranges: low/high. Let's use AVG of range for standard execution
            stop_mult = Decimal(str((self.config.tight_smc_atr_stop_min + self.config.tight_smc_atr_stop_max) / 2))
        else:
            stop_mult = Decimal(str((self.config.wide_structure_atr_stop_min + self.config.wide_structure_atr_stop_max) / 2))
        
        # 3b. Apply stop widening if symbol has recent stop-outs
        if getattr(self.config, 'stop_widen_enabled', True) and symbol:
            recent_stopouts = get_recent_stopouts(
                symbol, 
                lookback_hours=getattr(self.config, 'stop_widen_lookback_hours', 24)
            )
            threshold = getattr(self.config, 'stop_widen_threshold', 2)
            
            if recent_stopouts >= threshold:
                # Calculate widening factor
                base_factor = Decimal(str(getattr(self.config, 'stop_widen_factor', 1.5)))
                increment = Decimal(str(getattr(self.config, 'stop_widen_increment', 0.25)))
                max_factor = Decimal(str(getattr(self.config, 'stop_widen_max_factor', 2.0)))
                
                # Add increment for each stop-out above threshold
                extra_stopouts = recent_stopouts - threshold
                widen_factor = min(base_factor + (increment * extra_stopouts), max_factor)
                
                original_mult = stop_mult
                stop_mult = stop_mult * widen_factor
                
                logger.warning(
                    "Stop widening applied due to recent stop-outs",
                    symbol=symbol,
                    recent_stopouts=recent_stopouts,
                    threshold=threshold,
                    widen_factor=float(widen_factor),
                    original_mult=float(original_mult),
                    new_mult=float(stop_mult)
                )

        tp_candidates = []
        entry_price = Decimal("0")
        invalid_level = Decimal("0")
        signal_type = SignalType.NO_SIGNAL
        
        # 4. Calculate Entry & Stop Base (from 4H structure)
        if bias == "bullish":
            signal_type = SignalType.LONG
            if setup_type == SetupType.OB:
                entry_price = order_block['high']
                invalid_level = order_block['low']
            elif setup_type == SetupType.FVG:
                entry_price = fvg['top']
                invalid_level = fvg['bottom']
            else: # BOS/Trend
                # Fallback: Entry at decision TF close, stop at recent swing low
                entry_price = decision_candles[-1].close
                invalid_level = min(c.low for c in decision_candles[-20:])
                
            stop_loss = invalid_level - (atr * stop_mult)
            
        elif bias == "bearish":  # bearish
            signal_type = SignalType.SHORT
            if setup_type == SetupType.OB:
                entry_price = order_block['low']
                invalid_level = order_block['high']
            elif setup_type == SetupType.FVG:
                entry_price = fvg['bottom']
                invalid_level = fvg['top']
            else: # BOS/Trend
                entry_price = decision_candles[-1].close
                invalid_level = max(c.high for c in decision_candles[-20:])
                
            stop_loss = invalid_level + (atr * stop_mult)
            
        else:
             # Neutral bias - generally no trade unless counter-trend enabled?
             # Neutral can trade if score is high.
             # Assume logic mirrors bullish/bearish based on structure type
             if structure.get('order_block'):
                 ob = structure['order_block']
                 if ob['type'] == 'bullish':
                     signal_type = SignalType.LONG
                     entry_price = ob['high']
                     invalid_level = ob['low']
                     stop_loss = invalid_level - (atr * stop_mult)
                 else:
                     signal_type = SignalType.SHORT
                     entry_price = ob['low']
                     invalid_level = ob['high']
                     stop_loss = invalid_level + (atr * stop_mult)
             else:
                 return SignalType.NO_SIGNAL, Decimal("0"), Decimal("0"), None, [], {}
                 
        # 5. TP Logic - use 1H for swing point precision (finer granularity)
        lookback = 50
        risk = abs(entry_price - stop_loss)
        if risk == 0: risk = Decimal("1") # Avoid div/0
        
        if signal_type == SignalType.LONG:
            # Use 1H for swing point detection (precision) 
            swing_highs = self.indicators.find_swing_points(refine_candles_1h, lookback=lookback, find_highs=True)
            tp_candidates = sorted([h for h in swing_highs if h > entry_price])[:5]
            
            # Min RR
            min_rr = getattr(self.config, 'tight_smc_min_rr_multiple', 2.0) if regime == "tight_smc" else 1.5
            min_tp_dist = risk * Decimal(str(min_rr))
            
            # Filter TPs < Min RR
            valid_tps = [tp for tp in tp_candidates if (tp - entry_price) >= min_tp_dist]
            
            if valid_tps:
                take_profit = valid_tps[0] # Nearest valid
            else:
                take_profit = entry_price + min_tp_dist # Force Min RR
                 
        else: # SHORT
            # Use 1H for swing point detection (precision)
            swing_lows = self.indicators.find_swing_points(refine_candles_1h, lookback=lookback, find_highs=False)
            tp_candidates = sorted([l for l in swing_lows if l < entry_price], reverse=True)[:5]
            
            min_rr = getattr(self.config, 'tight_smc_min_rr_multiple', 2.0) if regime == "tight_smc" else 1.5
            min_tp_dist = risk * Decimal(str(min_rr))
            
            valid_tps = [tp for tp in tp_candidates if (entry_price - tp) >= min_tp_dist]
            
            if valid_tps:
                take_profit = valid_tps[0]
            else:
                take_profit = entry_price - min_tp_dist

        reasoning.append(
            f"✓ Levels ({regime}): Entry ${entry_price}, Stop ${stop_loss}, TP ${take_profit}"
        )
        
        class_info = {
            "setup_type": setup_type,
            "regime": regime
        }
        
        return signal_type, entry_price, stop_loss, take_profit, tp_candidates, class_info
    
    # Removed unused _check_rsi_divergence method - RSI divergence is checked inline
    # in generate_signal() where it's actually used (around line 297)

    @staticmethod
    def _infer_no_signal_gate(reasoning: List[str]) -> str:
        """Infer a normalized gate label from NO_SIGNAL reasoning text."""
        if not reasoning:
            return "unknown"
        reason = str(reasoning[-1]).lower()
        if "no valid 4h decision structure" in reason:
            return "decision_structure_required"
        if "outside weekly zone" in reason:
            return "outside_weekly_zone"
        if "waiting for structure break" in reason:
            return "waiting_structure_break"
        if "waiting for confirmation" in reason:
            return "waiting_structure_confirmation"
        if "waiting for reconfirmation" in reason:
            return "waiting_reconfirmation"
        if "ranging market: adx" in reason:
            return "adx_ranging_filter"
        if "tight_smc entry not in ote/key fib" in reason:
            return "fib_gate_reject"
        if "no fib structure found for tight_smc" in reason:
            return "fib_missing_reject"
        if "signal direction mismatch" in reason:
            return "direction_mismatch"
        if "score " in reason and "threshold" in reason:
            return "score_gate_reject"
        if "low conviction" in reason:
            return "conviction_gate_reject"
        return "other"
    
    def _no_signal(
        self,
        symbol: str,
        reasoning: List[str],
        current_candle: Optional[Candle],
        score_breakdown: Optional[Dict] = None,
        adx: float = 0.0,
        atr: Decimal = Decimal("0"),
        regime: Optional[str] = None
    ) -> Signal:
        """Create a NO_SIGNAL signal."""
        from src.domain.models import SetupType
        timestamp = current_candle.timestamp if current_candle else datetime.now(timezone.utc)
        
        # Determine regime if not provided (fallback for early rejections)
        if not regime:
            # If no data (no candle), state is undefined/no_data
            if not current_candle:
                regime = "no_data"
            else:
                # Use ADX-based heuristic for early rejections (before structure analysis)
                # ADX < 20: Very low trend strength → consolidation
                # ADX 20-25: Low trend strength → consolidation (ranging)
                # ADX 25-40: Moderate trend → wide_structure (could be trending)
                # ADX > 40: Strong trend → wide_structure (definitely trending)
                if adx > 0 and adx < 20:
                    regime = "consolidation"  # Very weak/no trend
                elif adx >= 20 and adx < 25:
                    regime = "consolidation"  # Ranging market
                else:
                    # ADX >= 25: Trending market
                    # Default to wide_structure since we don't have structure details
                    regime = "wide_structure"
                
            # SPECIAL DEBUG: If regime resulted in no_data but we HAD a candle, LOG IT
            if regime == "no_data" and current_candle:
                 reasoning.append(f"DEBUG_ERROR: Regime=no_data BUT Candle Exists! ADX={adx}")
                 # Force fix
                 regime = "consolidation"  # Conservative default

        if os.getenv("REPLAY_GATE_DIAGNOSTICS", "0") == "1":
            gate = self._infer_no_signal_gate(reasoning)
            logger.info(
                "REPLAY_GATE_REASON",
                symbol=symbol,
                gate=gate,
                reason_tail=str(reasoning[-1])[:240] if reasoning else "",
            )
        
        return Signal(
            timestamp=timestamp,
            symbol=symbol,
            signal_type=SignalType.NO_SIGNAL,
            entry_price=Decimal("0"),
            stop_loss=Decimal("0"),
            take_profit=None,
            reasoning="\n".join(reasoning),
            setup_type=SetupType.TREND,  # Default for no signal
            regime=regime,
            higher_tf_bias="neutral",
            adx=Decimal(str(adx)) if adx else Decimal("0"),
            atr=atr,

            ema200_slope="flat",
            score_breakdown=score_breakdown or {}
        )
