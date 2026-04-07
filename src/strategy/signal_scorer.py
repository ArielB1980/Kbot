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
    total_score: float  # 0-130
    smc_quality: float  # 0-25
    fib_confluence: float  # 0-20
    htf_alignment: float  # 0-20
    trend_confirmation: float  # 0-12 (structure confirmation replaces ADX)
    cost_efficiency: float  # 0-20
    volume_confirmation: float = 0.0  # 0-15 (volume replaces EMA slope)
    rsi_divergence: float = 0.0  # 0-10 (bonus when RSI divergence aligns with signal)
    fib_1h_confluence: float = 0.0  # 0-8 (multi-TF Fib overlap bonus)
    # Backward compat aliases (deprecated, read-only)
    adx_strength: float = 0.0
    ema_slope: float = 0.0
    
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
        bias: str,
        rsi_divergence: str = "none",
        volume_ratio: float = 0.0,
        market_structure_state: str = "neutral",
        fib_1h_overlap: bool = False,
    ) -> SignalScore:
        """Calculate composite quality score for a signal.

        Args:
            signal: Generated signal
            structures: SMC structures dict (OB, FVG, BOS)
            fib_levels: Fibonacci levels (if available)
            adx: ADX value for trend strength (legacy, used when adx_scoring_enabled=True)
            cost_bps: Estimated cost in basis points
            bias: HTF bias (bullish/bearish/neutral)
            rsi_divergence: RSI divergence state ("bullish", "bearish", or "none")
            volume_ratio: Breakout volume / 20-period avg volume (0 = no data)
            market_structure_state: From MarketStructureTracker ("bullish", "bearish", "neutral")
            fib_1h_overlap: True if 4H OTE overlaps 1H retracement zone

        Returns:
            SignalScore with total and component scores
        """
        smc_score = self._score_smc_quality(structures)
        fib_score = self._score_fib_confluence(signal, fib_levels)
        htf_score = self._score_htf_alignment(signal, bias)
        cost_score = self._score_cost_efficiency(signal, cost_bps)
        rsi_div_score = self._score_rsi_divergence(signal, rsi_divergence)

        # Volume confirmation replaces EMA slope; structure confirmation replaces ADX
        vol_score = self._score_volume_confirmation(volume_ratio)
        struct_score = self._score_structure_confirmation(signal, market_structure_state)

        # 1H Fib confluence bonus
        fib_1h_score = self._score_fib_1h_confluence(fib_1h_overlap)

        # Legacy fallbacks (enabled via config flags for A/B testing)
        adx_score = self._score_adx_strength(adx) if getattr(self.config, "adx_scoring_enabled", False) else 0.0
        ema_slope_score = self._score_ema_slope(signal) if not getattr(self.config, "volume_score_enabled", True) else 0.0

        total = (
            smc_score + fib_score + htf_score + cost_score + rsi_div_score
            + vol_score + struct_score + fib_1h_score
            + adx_score + ema_slope_score
        )

        score = SignalScore(
            total_score=total,
            smc_quality=smc_score,
            fib_confluence=fib_score,
            htf_alignment=htf_score,
            trend_confirmation=struct_score,
            cost_efficiency=cost_score,
            volume_confirmation=vol_score,
            rsi_divergence=rsi_div_score,
            fib_1h_confluence=fib_1h_score,
            adx_strength=adx_score,
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
                "fib_1h": f"{fib_1h_score:.1f}",
                "htf": f"{htf_score:.1f}",
                "volume": f"{vol_score:.1f}",
                "structure": f"{struct_score:.1f}",
                "cost": f"{cost_score:.1f}",
                "rsi_div": f"{rsi_div_score:.1f}",
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

        is_aligned = bias != "neutral"

        # Unified regime: single threshold pair for all setup types
        if getattr(self.config, "unified_regime_enabled", True):
            if is_aligned:
                threshold = getattr(self.config, "min_score_smc_aligned", 60.0)
            else:
                threshold = getattr(self.config, "min_score_smc_neutral", 65.0)
        else:
            # Legacy: tight_smc vs wide_structure thresholds
            is_tight = setup_type in [SetupType.OB, SetupType.FVG]
            if is_tight:
                if is_aligned:
                    threshold = self.config.min_score_tight_smc_aligned
                else:
                    threshold = self.config.min_score_tight_smc_neutral
            else:
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
        Score HTF alignment (-penalty to +20 points).

        Logic:
        - Direction aligned with Bias: +20
        - Bias Neutral: +10
        - Counter-trend: -counter_trend_score_penalty (default -5)
        """
        from src.domain.models import SignalType

        if bias == "neutral":
            return 10.0

        is_bullish = bias == "bullish"
        is_long = signal.signal_type == SignalType.LONG

        if (is_bullish and is_long) or (not is_bullish and not is_long):
            return 20.0

        # Counter-trend: apply negative penalty so signal must score higher elsewhere
        penalty = getattr(self.config, "counter_trend_score_penalty", 5.0)
        return -penalty
    
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

    def _score_fib_1h_confluence(self, fib_1h_overlap: bool) -> float:
        """Score 1H Fibonacci confluence with 4H OTE (0-8 points).

        Awards bonus when 4H OTE zone overlaps 1H retracement zone,
        per EmperorBTC's multi-TF Fibonacci methodology.
        """
        if not getattr(self.config, "fib_1h_confluence_enabled", True):
            return 0.0
        if fib_1h_overlap:
            return getattr(self.config, "fib_1h_confluence_bonus", 8.0)
        return 0.0

    def _score_volume_confirmation(self, volume_ratio: float) -> float:
        """Score breakout volume confirmation (0-15 points).

        Volume ratio is breakout candle volume / 20-period average volume.
        Thresholds from MoneyTaur: strong (1.5x) and moderate (1.2x).
        """
        if not getattr(self.config, "volume_score_enabled", True):
            return 0.0
        high_mult = getattr(self.config, "volume_score_high_mult", 1.5)
        low_mult = getattr(self.config, "volume_score_low_mult", 1.2)
        if volume_ratio >= high_mult:
            return 15.0
        if volume_ratio >= low_mult:
            return 8.0
        return 0.0

    def _score_structure_confirmation(self, signal: Signal, market_structure_state: str) -> float:
        """Score market structure confirmation (0-12 points).

        Awards points when MarketStructureTracker state (HH/HL or LH/LL)
        confirms the signal direction. Replaces ADX as trend filter.
        """
        if not getattr(self.config, "structure_confirmation_score_enabled", True):
            return 0.0
        points = getattr(self.config, "structure_confirmation_score_points", 12.0)
        is_long = signal.signal_type == SignalType.LONG
        if (market_structure_state == "bullish" and is_long) or (
            market_structure_state == "bearish" and not is_long
        ):
            return points
        return 0.0

    def _score_rsi_divergence(self, signal: Signal, divergence: str) -> float:
        """Score RSI divergence alignment with signal direction (0 to rsi_divergence_score_bonus).

        Awards bonus when 1H RSI divergence confirms signal direction:
        bullish divergence + LONG = bonus, bearish divergence + SHORT = bonus.
        """
        bonus = getattr(self.config, "rsi_divergence_score_bonus", 10.0)
        if bonus <= 0 or divergence == "none":
            return 0.0

        is_long = signal.signal_type == SignalType.LONG
        if (divergence == "bullish" and is_long) or (divergence == "bearish" and not is_long):
            return bonus

        return 0.0
