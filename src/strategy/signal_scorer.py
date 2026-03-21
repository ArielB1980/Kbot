"""
Signal quality scoring system.

Scores each signal on multiple factors to prioritize opportunities.
Used for dashboard display and future trade selection optimization.
"""
from typing import Dict, Optional, Tuple
from decimal import Decimal
from dataclasses import dataclass
import os

from src.domain.models import Signal, SignalType
from src.strategy.fibonacci_engine import FibonacciLevels
from src.monitoring.logger import get_logger

logger = get_logger(__name__)


@dataclass
class SignalScore:
    """Composite quality score for a signal with breakdown."""
    total_score: float  # 0-100
    smc_quality: float  # 0-25
    fib_confluence: float  # 0-20
    htf_alignment: float  # 0-20
    adx_strength: float  # 0-15
    cost_efficiency: float  # 0-20
    ema_slope: float = 0.0  # 0-15 (bonus when slope aligns with direction)
    
    def get_grade(self) -> str:
        """Convert score to letter grade."""
        if self.total_score >= 80:
            return "A"
        elif self.total_score >= 65:
            return "B"
        elif self.total_score >= 50:
            return "C"
        elif self.total_score >= 35:
            return "D"
        else:
            return "F"


class SignalScorer:
    """
    Scores trading signals on multiple quality factors.
    
    Philosophy:
    - High scores = better confluence, structure, and efficiency
    - HARD GATE: Signals below threshold are rejected
    - Used for prioritization and dashboard display
    """
    
    def __init__(self, config: "StrategyConfig"):
        """
        Initialize signal scorer.
        
        Args:
            config: Strategy configuration for thresholds
        """
        self.config = config
        logger.info("SignalScorer initialized", 
                    tight_aligned=config.min_score_tight_smc_aligned,
                    wide_aligned=config.min_score_wide_structure_aligned)
    
    def score_signal(
        self,
        signal: Signal,
        structures: Dict,
        fib_levels: Optional[FibonacciLevels],
        adx: float,
        cost_bps: Decimal,
        bias: str
    ) -> SignalScore:
        """
        Calculate composite quality score for a signal.
        
        Args:
            signal: Generated signal
            structures: SMC structures dict (OB, FVG, BOS)
            fib_levels: Fibonacci levels (if available)
            adx: ADX value for trend strength
            cost_bps: Estimated cost in basis points
            bias: HTF bias (bullish/bearish/neutral)
        
        Returns:
            SignalScore with total and component scores
        """
        # Score each component
        smc_score = self._score_smc_quality(structures)
        fib_score = self._score_fib_confluence(signal, fib_levels)
        htf_score = self._score_htf_alignment(signal, bias)
        adx_score = self._score_adx_strength(adx)
        cost_score = self._score_cost_efficiency(signal, cost_bps)
        ema_slope_score = self._score_ema_slope(signal)

        total = smc_score + fib_score + htf_score + adx_score + cost_score + ema_slope_score

        score = SignalScore(
            total_score=total,
            smc_quality=smc_score,
            fib_confluence=fib_score,
            htf_alignment=htf_score,
            adx_strength=adx_score,
            cost_efficiency=cost_score,
            ema_slope=ema_slope_score,
        )

        logger.debug(
            "Signal scored",
            symbol=signal.symbol,
            total=f"{total:.1f}",
            grade=score.get_grade(),
            breakdown={
                "smc": f"{smc_score:.1f}",
                "fib": f"{fib_score:.1f}",
                "htf": f"{htf_score:.1f}",
                "adx": f"{adx_score:.1f}",
                "cost": f"{cost_score:.1f}",
                "ema_slope": f"{ema_slope_score:.1f}",
            }
        )
        
        return score
    
    def check_score_gate(self, score: float, setup_type: str, bias: str) -> Tuple[bool, float]:
        """
        Check if signal score passes the hard gate.
        
        Returns:
            (passed: bool, threshold: float)
        """
        from src.domain.models import SetupType
        
        # Determine strictness based on regime/bias
        is_tight = setup_type in [SetupType.OB, SetupType.FVG]
        is_aligned = bias != "neutral"
        
        if is_tight:
            if is_aligned:
                threshold = self.config.min_score_tight_smc_aligned
            else:
                threshold = self.config.min_score_tight_smc_neutral
        else: # wide_structure (BOS/TREND)
            if is_aligned:
                threshold = self.config.min_score_wide_structure_aligned
            else:
                threshold = self.config.min_score_wide_structure_neutral

        override = os.getenv("REPLAY_OVERRIDE_SCORE_GATE_THRESHOLD")
        if override is not None and override.strip():
            try:
                threshold = float(override)
            except ValueError:
                logger.warning("Invalid REPLAY_OVERRIDE_SCORE_GATE_THRESHOLD", value=override)
        
        return score >= threshold, threshold

    def _score_smc_quality(self, structures: Dict) -> float:
        """
        Score SMC structure quality (0-25 points).
        
        Scoring:
        - Order Block present: +10
        - FVG present: +8
        - BOS confirmed: +7
        - Max: 25 (all structures)
        """
        score = 0.0
        
        if structures.get("order_block"):
            score += 10.0
        
        if structures.get("fvg"):
            score += 8.0
        
        if structures.get("bos"):
            score += 7.0
        
        return min(score, 25.0)
    
    def _effective_fib_proximity_bps(self, signal: Signal) -> float:
        """Return fib proximity tolerance in bps, optionally scaled by ATR ratio."""
        base = self.config.fib_proximity_bps
        if not self.config.fib_proximity_adaptive_enabled:
            return base
        atr_ratio = getattr(signal, "atr_ratio", None)
        if atr_ratio is None:
            return base
        ratio_float = float(atr_ratio)
        scale = self.config.fib_proximity_adaptive_scale
        effective = base * (1.0 + max(0.0, ratio_float - 1.0) * scale)
        return min(effective, self.config.fib_proximity_max_bps)

    def _score_fib_confluence(
        self,
        signal: Signal,
        fib_levels: Optional[FibonacciLevels]
    ) -> float:
        """
        Score Fibonacci confluence (0-20 points).

        Scoring:
        - In OTE zone: +15
        - Near any fib level: +10
        - Near extension: +5
        - No fib data: 0
        """
        if not fib_levels:
            return 0.0

        score = 0.0
        entry = signal.entry_price

        # Check OTE zone (highest value)
        if fib_levels.ote_low <= entry <= fib_levels.ote_high:
            score = 15.0
        else:
            # Check proximity to standard levels (adaptive or fixed)
            effective_bps = self._effective_fib_proximity_bps(signal)
            tolerance = Decimal(str(effective_bps)) / Decimal("10000")
            levels = [
                fib_levels.fib_0_382,
                fib_levels.fib_0_618,
                fib_levels.fib_0_500,
                fib_levels.fib_0_786
            ]
            
            for level in levels:
                if abs(entry - level) / level <= tolerance:
                    score = 10.0
                    break
            
            # Check extensions if no retracement match
            if score == 0:
                ext_levels = [fib_levels.fib_1_272, fib_levels.fib_1_618]
                for level in ext_levels:
                    if abs(entry - level) / level <= tolerance:
                        score = 5.0
                        break
        
        return score
    
    def _score_htf_alignment(self, signal: Signal, bias: str) -> float:
        """
        Score HTF alignment (0-20 points).

        Logic:
        - Direction aligned with Bias: +20
        - Bias Neutral: +10
        - Counter-trend: 0
        """
        from src.domain.models import SignalType
        
        if bias == "neutral":
            return 10.0
            
        is_bullish = bias == "bullish"
        is_long = signal.signal_type == SignalType.LONG
        
        if (is_bullish and is_long) or (not is_bullish and not is_long):
            return 20.0
            
        return 0.0
    
    def _score_adx_strength(self, adx: float) -> float:
        """
        Score ADX trend strength (0-15 points).

        Scoring thresholds:
        - ADX >= 40: 15
        - ADX >= 30: 12
        - ADX >= 25: 10
        - ADX >= 20: 7
        - ADX < 20: 0
        """
        if adx >= 40:
            return 15.0
        elif adx >= 30:
            return 12.0
        elif adx >= 25:
            return 10.0
        elif adx >= 20:
            return 7.0
        else:
            return 0.0
    
    def _score_cost_efficiency(self, signal: Signal, cost_bps: Decimal) -> float:
        """
        Score cost efficiency (0-20 points).

        Lower cost relative to potential reward = higher score.
        """
        if cost_bps <= Decimal("10"):
            return 20.0
        elif cost_bps <= Decimal("20"):
            return 15.0
        elif cost_bps <= Decimal("30"):
            return 10.0
        elif cost_bps <= Decimal("50"):
            return 5.0
        else:
            return 0.0

    def _score_ema_slope(self, signal: Signal) -> float:
        """Score EMA200 slope alignment with signal direction (0 to ema_slope_bonus points).

        Awards bonus points when the daily EMA200 slope confirms the trade
        direction (e.g. rising EMA + LONG, falling EMA + SHORT).  Returns 0
        when the slope is flat, counter-directional, or the bonus is disabled.
        """
        bonus = self.config.ema_slope_bonus
        if bonus <= 0:
            return 0.0

        slope = getattr(signal, "ema200_slope", "flat")
        if slope == "flat":
            return 0.0

        is_long = signal.signal_type == SignalType.LONG
        if (slope == "up" and is_long) or (slope == "down" and not is_long):
            return bonus

        return 0.0
