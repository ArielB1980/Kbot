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

SCORER_WEIGHTS = {
    "phase_ad": {
        "fib": 1.0, "htf": 1.0, "cost": 1.0, "rsi_div": 1.0,
        "volume": 1.0, "structure": 1.0, "fib_1h": 1.0,
        "adx": 1.0, "ema_slope": 1.0,
        "cost_max": 25.0, "structure_max": 12.0,
        "structure_gate": False,
        # Phase 1: freshness scoring disabled under phase_ad (backward compat)
        "freshness": 0.0, "freshness_max": 10.0,
    },
    "structure_primary": {
        "fib": 0.0, "htf": 0.0, "cost": 1.0, "rsi_div": 0.0,
        "volume": 0.0, "structure": 0.0, "fib_1h": 0.0,
        "adx": 0.0, "ema_slope": 0.0,
        "cost_max": 20.0, "structure_max": 0.0,
        "structure_gate": True,  # structure is a hard gate, not a scored component
        # Phase 1: level freshness adds a second scored dimension (0-30pt total)
        "freshness": 1.0, "freshness_max": 10.0,
    },
}


@dataclass
class SignalScore:
    """Composite quality score for a signal with breakdown.

    Two scorer versions (selected via config.scorer_version):
    - phase_ad: 8-component ~100pt scale (legacy, fib+htf+cost+vol+struct+fib_1h)
    - structure_primary: structure is a hard gate (pass/fail), scored total is
      cost_efficiency only (0-20pt scale). All other components computed for
      logging but excluded from total. Validated by 400-day forward return
      analysis showing structure confirmation positive across all symbols.
    """
    total_score: float  # 0-100
    smc_quality: float  # 0-25 (computed for logging, NOT in total)
    fib_confluence: float  # 0-20
    htf_alignment: float  # 0-20
    trend_confirmation: float  # 0-12 (structure confirmation)
    cost_efficiency: float  # 0-25 (signal quality composite — cluster representative)
    volume_confirmation: float = 0.0  # 0-15
    rsi_divergence: float = 0.0  # 0-10 (currently zeroed, kept for re-evaluation)
    fib_1h_confluence: float = 0.0  # 0-8
    adx_gradient: float = 0.0  # 0-6 (computed for logging, NOT in total)
    # Phase 1: level freshness quality (0-10 in structure_primary; unused in phase_ad)
    level_freshness: float = 0.0
    # Backward compat aliases (deprecated, read-only)
    adx_strength: float = 0.0
    ema_slope: float = 0.0
    scorer_version: str = "phase_ad"

    def get_grade(self) -> str:
        """Convert score to letter grade."""
        if self.scorer_version == "structure_primary":
            # 30-point scale (cost + freshness): A≥24, B≥18, C≥12, D≥7
            if self.total_score >= 24:
                return "A"
            elif self.total_score >= 18:
                return "B"
            elif self.total_score >= 12:
                return "C"
            elif self.total_score >= 7:
                return "D"
            else:
                return "F"
        # phase_ad: 100-point scale
        if self.total_score >= 65:
            return "A"
        elif self.total_score >= 50:
            return "B"
        elif self.total_score >= 35:
            return "C"
        elif self.total_score >= 25:
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
        version_override = os.getenv("REPLAY_OVERRIDE_SCORER_VERSION")
        self._scorer_version = version_override or getattr(config, "scorer_version", "phase_ad")
        self._weights = SCORER_WEIGHTS.get(self._scorer_version, SCORER_WEIGHTS["phase_ad"])
        logger.info("SignalScorer initialized",
                    scorer_version=self._scorer_version,
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
        w = self._weights

        # Compute all components (always, for logging regardless of version)
        smc_score = self._score_smc_quality(structures)
        fib_score = self._score_fib_confluence(signal, fib_levels)
        htf_score = self._score_htf_alignment(signal, bias)
        cost_score = self._score_cost_efficiency(signal, cost_bps, max_points=w["cost_max"])
        rsi_div_score = self._score_rsi_divergence(signal, rsi_divergence)
        vol_score = self._score_volume_confirmation(volume_ratio)
        # Always compute structure at its natural scale (12pts) for logging and gate checks.
        # In structure_primary mode, it's a hard gate not a scored component — weight is 0.
        struct_score = self._score_structure_confirmation(signal, market_structure_state, max_points=12.0)
        fib_1h_score = self._score_fib_1h_confluence(fib_1h_overlap)
        adx_gradient = self._score_adx_gradient(adx)
        freshness_score = self._score_level_freshness(
            structures, max_points=w.get("freshness_max", 10.0)
        )

        # Legacy fallbacks (enabled via config flags for A/B testing)
        adx_score = self._score_adx_strength(adx) if getattr(self.config, "adx_scoring_enabled", False) else 0.0
        ema_slope_score = self._score_ema_slope(signal) if not getattr(self.config, "volume_score_enabled", True) else 0.0

        # Apply version weights — structure_primary zeros everything except
        # structure + cost + freshness; phase_ad keeps all components at weight 1.0
        # (freshness still 0 under phase_ad for backward compat).
        # SMC and adx_gradient are always excluded from total (redundant cluster).
        total = (
            w["fib"] * fib_score
            + w["htf"] * htf_score
            + w["cost"] * cost_score
            + w["rsi_div"] * rsi_div_score
            + w["volume"] * vol_score
            + w["structure"] * struct_score
            + w["fib_1h"] * fib_1h_score
            + w["adx"] * adx_score
            + w["ema_slope"] * ema_slope_score
            + w.get("freshness", 0.0) * freshness_score
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
            adx_gradient=adx_gradient,
            level_freshness=freshness_score,
            adx_strength=adx_score,
            ema_slope=ema_slope_score,
            scorer_version=self._scorer_version,
        )

        logger.debug(
            "Signal scored",
            symbol=signal.symbol,
            scorer_version=self._scorer_version,
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
                "freshness": f"{freshness_score:.1f}",
            }
        )

        return score
    
    def check_score_gate(self, score: float, setup_type: str, bias: str, structure_confirmed: bool = True) -> Tuple[bool, float]:
        """
        Check if signal score passes the hard gate.

        Returns:
            (passed: bool, threshold: float)
        """
        from src.domain.models import SetupType

        # Structure-primary: structure confirmation is a hard gate
        if self._weights.get("structure_gate") and not structure_confirmed:
            return False, 0.0

        is_aligned = bias != "neutral"

        # Structure-primary scorer: cost-only thresholds (0-20 scale)
        if self._scorer_version == "structure_primary":
            if is_aligned:
                threshold = getattr(self.config, "min_score_structure_primary_aligned", 10.0)
            else:
                threshold = getattr(self.config, "min_score_structure_primary_neutral", 12.0)
        elif getattr(self.config, "unified_regime_enabled", True):
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

        Continuous scoring based on structure strength:
        - Order Block: 7.5–15 scaled by displacement ratio
        - FVG: 5–12 scaled by gap size
        - BOS: 5–10 scaled by confirmation strength
        - Max: 25 (capped)
        """
        score = 0.0

        ob = structures.get("order_block")
        if ob:
            displacement = ob.get("displacement_ratio", 1.5) if isinstance(ob, dict) else 1.5
            # Scale: 1.5x displacement = 7.5 pts, 3.0x = 15 pts
            score += min(10.0 * displacement / 2.0, 15.0)

        fvg = structures.get("fvg")
        if fvg:
            gap_pct = fvg.get("gap_size_pct", 0.001) if isinstance(fvg, dict) else 0.001
            # Scale: 0.07% = 5 pts, 0.3% = 12 pts
            score += min(5.0 + gap_pct * 2333.0, 12.0)

        bos = structures.get("bos")
        if bos:
            confirmation = bos.get("confirmation_strength", 1.0) if isinstance(bos, dict) else 1.0
            score += min(5.0 + confirmation * 2.5, 10.0)

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
        - Direction aligned with Bias AND inside weekly zone: +20
        - Direction aligned with Bias, outside weekly zone: +12
        - Bias Neutral: +10
        - Counter-trend: -counter_trend_score_penalty (default -5)
        """
        from src.domain.models import SignalType

        if bias == "neutral":
            return 10.0

        is_bullish = bias == "bullish"
        is_long = signal.signal_type == SignalType.LONG

        if (is_bullish and is_long) or (not is_bullish and not is_long):
            # Aligned — check if inside weekly zone for full bonus
            inside_zone = getattr(signal, "inside_weekly_zone", None)
            if inside_zone:
                return 20.0
            return 12.0  # Aligned but outside weekly zone

        # Counter-trend: apply negative penalty so signal must score higher elsewhere
        penalty = getattr(self.config, "counter_trend_score_penalty", 5.0)
        return -penalty
    
    def _score_adx_gradient(self, adx: float) -> float:
        """ADX gradient bonus (0-6 points).

        Uses ADX data already computed for the hard gate to add dynamic range.
        ADX 20-25: 0, ADX 25-35: +3, ADX 35+: +6.
        """
        if adx >= 35.0:
            return 6.0
        if adx >= 25.0:
            return 3.0
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
    
    def _score_cost_efficiency(self, signal: Signal, cost_bps: Decimal, max_points: float = 25.0) -> float:
        """Score signal quality via cost efficiency.

        Collapsed cluster representative for SMC/cost/adx_grad (Phase 2).
        Linear scale: 0 bps = max_points, 50 bps = 0 pts.
        """
        cost_float = float(cost_bps)
        if cost_float >= 50.0:
            return 0.0
        return max(0.0, max_points * (1.0 - cost_float / 50.0))

    def _score_level_freshness(self, structures: Dict, max_points: float = 10.0) -> float:
        """Score level freshness (Phase 1: Gap 1 — untouched levels outperform tested).

        Calibrated from 400-day replay (N=626 signals, FVG mode=full):
          - OB body_freshness (Moneytaur institutional zone) shows monotonic
            forward-return ordering: untouched +2.80%, partial +2.59%, tested +1.48%
          - OB wick_freshness showed a partial>untouched inversion driven by zone
            misclassification (39/92 wick-partial entries were body-untouched)
          - FVG freshness was non-monotonic (noise): +2.24% / +1.98% / +2.32%

        Scorer therefore reads OB body_freshness and drops FVG from the blend.
        FVG freshness is still captured in structure_info for Phase 2 multi-TF
        research but contributes 0 to the live score.

        Per-level base:
            fully_untouched      → 1.0
            partially_mitigated  → 0.85  (slightly lower mean return, higher hit rate)
            fully_tested         → 0.0
        """
        age_threshold = int(getattr(self.config, "freshness_age_bonus_threshold", 10))
        age_multiplier = float(getattr(self.config, "freshness_age_bonus_multiplier", 1.2))

        ob = structures.get("order_block")
        if not ob or not isinstance(ob, dict):
            return 0.0

        grade = ob.get("body_freshness") or ob.get("freshness", "fully_untouched")
        base = {
            "fully_untouched": 1.0,
            "partially_mitigated": 0.85,
            "fully_tested": 0.0,
        }.get(grade, 0.0)
        age = int(ob.get("age_candles", 0) or 0)
        if grade == "fully_untouched" and age >= age_threshold:
            base = min(1.0, base * age_multiplier)

        return base * max_points

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

    def _score_structure_confirmation(self, signal: Signal, market_structure_state: str, max_points: float = 12.0) -> float:
        """Score market structure confirmation.

        Awards points when MarketStructureTracker state (HH/HL or LH/LL)
        confirms the signal direction. Replaces ADX as trend filter.
        max_points is set by the scorer weight profile (12 for phase_ad, 50 for structure_primary).
        """
        if not getattr(self.config, "structure_confirmation_score_enabled", True):
            return 0.0
        is_long = signal.signal_type == SignalType.LONG
        if (market_structure_state == "bullish" and is_long) or (
            market_structure_state == "bearish" and not is_long
        ):
            return max_points
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
