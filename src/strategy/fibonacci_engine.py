"""
Fibonacci level calculation and confluence detection.

V2 Feature: Adds Fibonacci as a CONFLUENCE factor (not a signal generator).
Used to score and validate SMC signals, improve TP/SL placement.
"""
from typing import List, Dict, Optional, Tuple
from decimal import Decimal
from dataclasses import dataclass
import pandas as pd

from src.domain.models import Candle
from src.monitoring.logger import get_logger

logger = get_logger(__name__)


@dataclass
class FibonacciLevels:
    """Fibonacci retracement/extension levels for a swing."""
    swing_high: Decimal
    swing_low: Decimal
    range_size: Decimal
    
    # Standard retracement levels
    fib_0_236: Decimal
    fib_0_382: Decimal
    fib_0_500: Decimal
    fib_0_618: Decimal
    fib_0_786: Decimal
    
    # OTE (Optimal Trade Entry) zone
    ote_low: Decimal  # 0.705
    ote_high: Decimal  # 0.79
    
    # Extension levels
    fib_1_272: Decimal
    fib_1_618: Decimal
    
    # Metadata
    swing_high_index: int
    swing_low_index: int
    timeframe: str


class FibonacciEngine:
    """
    Calculates Fibonacci levels from price swings.
    
    Key Design:
    - Deterministic: same candles → same levels
    - Does NOT generate signals
    - Used for confluence scoring only
    - Helps validate SMC entries and improve exits
    """
    
    def __init__(self, lookback_bars: int = 100):
        """
        Initialize Fibonacci engine.
        
        Args:
            lookback_bars: How far back to look for swing points
        """
        self.lookback_bars = lookback_bars
        logger.info("FibonacciEngine initialized", lookback_bars=lookback_bars)
    
    def calculate_levels(
        self,
        candles: List[Candle],
        timeframe: str = "1h"
    ) -> Optional[FibonacciLevels]:
        """
        Calculate Fibonacci levels from recent swing.
        
        Args:
            candles: Price data (must be in chronological order)
            timeframe: For logging/debugging
        
        Returns:
            FibonacciLevels or None if no valid swing found
        """
        if len(candles) < 20:
            logger.debug("Insufficient candles for Fibonacci", count=len(candles))
            return None
        
        # Find most recent significant swing
        swing_high_idx, swing_low_idx = self._find_swing_points(candles)
        
        if swing_high_idx is None or swing_low_idx is None:
            logger.debug("No valid swing points found")
            return None
        
        swing_high = candles[swing_high_idx].high
        swing_low = candles[swing_low_idx].low
        range_size = swing_high - swing_low
        
        if range_size <= Decimal("0"):
            logger.warning("Invalid swing range (high <= low)")
            return None
        
        # Calculate retracement levels (from swing_low upward)
        fib_levels = FibonacciLevels(
            swing_high=swing_high,
            swing_low=swing_low,
            range_size=range_size,
            
            # Retracements
            fib_0_236=swing_low + range_size * Decimal("0.236"),
            fib_0_382=swing_low + range_size * Decimal("0.382"),
            fib_0_500=swing_low + range_size * Decimal("0.5"),
            fib_0_618=swing_low + range_size * Decimal("0.618"),
            fib_0_786=swing_low + range_size * Decimal("0.786"),
            
            # OTE zone (0.705 - 0.79)
            ote_low=swing_low + range_size * Decimal("0.705"),
            ote_high=swing_low + range_size * Decimal("0.79"),
            
            # Extensions
            fib_1_272=swing_low + range_size * Decimal("1.272"),
            fib_1_618=swing_low + range_size * Decimal("1.618"),
            
            # Metadata
            swing_high_index=swing_high_idx,
            swing_low_index=swing_low_idx,
            timeframe=timeframe
        )
        
        logger.debug(
            "Fibonacci levels calculated",
            timeframe=timeframe,
            swing_high=str(swing_high),
            swing_low=str(swing_low),
            range_pct=float(range_size / swing_low * 100) if swing_low > Decimal("0") else 0.0
        )
        
        return fib_levels
    
    def check_confluence(
        self,
        price: Decimal,
        fib_levels: FibonacciLevels,
        tolerance_pct: float = 0.002  # 0.2% default
    ) -> Tuple[bool, List[str]]:
        """
        Check if price is near any Fibonacci level.
        
        Args:
            price: Current price to check
            fib_levels: Previously calculated Fib levels
            tolerance_pct: How close is "near" (default 0.2%)
        
        Returns:
            (has_confluence: bool, matched_levels: List[str])
        """
        tolerance = Decimal(str(tolerance_pct))
        matched = []
        
        # Check each level
        levels_to_check = {
            "0.236": fib_levels.fib_0_236,
            "0.382": fib_levels.fib_0_382,
            "0.500": fib_levels.fib_0_500,
            "0.618": fib_levels.fib_0_618,
            "0.786": fib_levels.fib_0_786,
            "1.272": fib_levels.fib_1_272,
            "1.618": fib_levels.fib_1_618,
        }
        
        for level_name, level_price in levels_to_check.items():
            if self._is_near(price, level_price, tolerance):
                matched.append(level_name)
        
        # Check OTE zone
        if fib_levels.ote_low <= price <= fib_levels.ote_high:
            matched.append("OTE")
        
        has_confluence = len(matched) > 0
        
        if has_confluence:
            logger.debug(
                "Fibonacci confluence detected",
                price=str(price),
                levels=matched
            )
        
        return has_confluence, matched
    
    def is_in_ote_zone(self, price: Decimal, fib_levels: FibonacciLevels) -> bool:
        """Check if price is in the Optimal Trade Entry zone (0.705-0.79)."""
        return fib_levels.ote_low <= price <= fib_levels.ote_high
    
    def get_nearest_extension(self, price: Decimal, fib_levels: FibonacciLevels) -> Decimal:
        """Get nearest extension level for TP targeting."""
        ext_1_272 = fib_levels.fib_1_272
        ext_1_618 = fib_levels.fib_1_618
        
        # Return closer extension
        if abs(price - ext_1_272) < abs(price - ext_1_618):
            return ext_1_272
        return ext_1_618
    
    def check_multi_tf_confluence(
        self,
        fib_4h: FibonacciLevels,
        fib_1h: FibonacciLevels,
        tolerance_bps: float = 30.0,
    ) -> bool:
        """Check if 4H OTE zone overlaps with 1H retracement zone.

        Per EmperorBTC scalping cheat sheet: draw Fibs on 4H swing, draw
        Fibs on 1H swing, find the confluence. Signal is stronger when
        both timeframes agree on the entry zone.

        Args:
            fib_4h: Fibonacci levels from 4H candles.
            fib_1h: Fibonacci levels from 1H candles.
            tolerance_bps: Allowed gap in basis points for "overlap".

        Returns:
            True if the 4H OTE zone overlaps the 1H 0.618-0.786 zone.
        """
        tol = Decimal(str(tolerance_bps / 10000))
        # 4H OTE zone: 0.705 - 0.79
        ote_low_4h = fib_4h.ote_low
        ote_high_4h = fib_4h.ote_high
        # 1H retracement zone: 0.618 - 0.786
        ret_low_1h = fib_1h.fib_0_618
        ret_high_1h = fib_1h.fib_0_786

        # Expand both zones by tolerance
        tol_4h = fib_4h.range_size * tol
        tol_1h = fib_1h.range_size * tol

        return (ote_low_4h - tol_4h) <= (ret_high_1h + tol_1h) and (
            ret_low_1h - tol_1h
        ) <= (ote_high_4h + tol_4h)

    def _find_swing_points(
        self,
        candles: List[Candle],
        swing_strength: int = 5
    ) -> Tuple[Optional[int], Optional[int]]:
        """
        Find most recent significant swing high and low.
        
        Uses a simple pivot detection: a high/low that is higher/lower
        than N bars before and after it.
        
        Args:
            candles: Price data
            swing_strength: Number of bars each side for pivot confirmation
        
        Returns:
            (swing_high_index, swing_low_index) or (None, None)
        """
        if len(candles) < swing_strength * 2 + 1:
            return None, None
        
        # Look in recent candles (not the very last few, they're incomplete swings)
        search_window = min(self.lookback_bars, len(candles) - swing_strength)
        
        swing_high_idx = None
        swing_low_idx = None
        
        # Find highest high and lowest low in window
        highs = [c.high for c in candles[-search_window:]]
        lows = [c.low for c in candles[-search_window:]]
        
        # Simple approach: use absolute highest/lowest in window
        # More sophisticated: use pivot highs/lows
        max_high = max(highs)
        min_low = min(lows)
        
        # Find indices (from end of list)
        for i in range(len(candles) - 1, len(candles) - search_window - 1, -1):
            if candles[i].high == max_high and swing_high_idx is None:
                swing_high_idx = i
            if candles[i].low == min_low and swing_low_idx is None:
                swing_low_idx = i
            
            if swing_high_idx is not None and swing_low_idx is not None:
                break
        
        return swing_high_idx, swing_low_idx
    
    def _is_near(
        self,
        price: Decimal,
        level: Decimal,
        tolerance_pct: Decimal
    ) -> bool:
        """Check if price is within tolerance % of a level."""
        diff = abs(price - level)
        threshold = level * tolerance_pct
        return diff <= threshold
