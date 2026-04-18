import pytest
from decimal import Decimal
from unittest.mock import Mock
from src.strategy.signal_scorer import SignalScorer
from src.config.config import StrategyConfig
from src.domain.models import Signal, SignalType, SetupType
from src.strategy.fibonacci_engine import FibonacciLevels

@pytest.fixture
def scorer():
    config = StrategyConfig()
    # Gates
    config.min_score_tight_smc_aligned = 75
    config.min_score_tight_smc_neutral = 80
    config.min_score_wide_structure_aligned = 70
    config.min_score_wide_structure_neutral = 75
    return SignalScorer(config)

def test_score_components(scorer):
    """Test individual scoring components."""
    # SMC Quality
    structures_full = {"order_block": True, "fvg": True, "bos": True}
    assert scorer._score_smc_quality(structures_full) == 25.0
    
    structures_partial = {"order_block": True}
    assert scorer._score_smc_quality(structures_partial) == 10.0
    
    # ADX
    assert scorer._score_adx_strength(45.0) == 15.0
    assert scorer._score_adx_strength(22.0) == 7.0
    assert scorer._score_adx_strength(15.0) == 0.0
    
    # Cost
    assert scorer._score_cost_efficiency(Mock(), Decimal("5")) == 20.0 # <= 10 bps
    assert scorer._score_cost_efficiency(Mock(), Decimal("100")) == 0.0 # > 50 bps

def test_score_gate_unified(scorer):
    """Test scoring gates with unified regime (default)."""
    # Unified regime uses min_score_smc_aligned=60 / min_score_smc_neutral=65
    # Aligned (Threshold 60)
    passed, thresh = scorer.check_score_gate(61.0, SetupType.OB, "bullish")
    assert passed
    assert thresh == 60.0

    passed, thresh = scorer.check_score_gate(59.0, SetupType.OB, "bullish")
    assert not passed

    # Neutral (Threshold 65)
    passed, thresh = scorer.check_score_gate(63.0, SetupType.OB, "neutral")
    assert not passed
    assert thresh == 65.0

    passed, thresh = scorer.check_score_gate(66.0, SetupType.OB, "neutral")
    assert passed

    # BOS uses same unified thresholds
    passed, thresh = scorer.check_score_gate(61.0, SetupType.BOS, "bearish")
    assert passed
    assert thresh == 60.0

def test_fib_confluence_scoring(scorer):
    """Test fib confluence scoring logic."""
    signal = Mock(spec=Signal)
    signal.entry_price = Decimal("50000")
    
    # Case 1: In OTE (0.618-0.79)
    # 50000 in [49000, 51000]
    fibs = Mock(spec=FibonacciLevels)
    fibs.ote_low = Decimal("49000")
    fibs.ote_high = Decimal("51000")
    
    score = scorer._score_fib_confluence(signal, fibs)
    assert score == 15.0

    # Case 2: Near 0.382
    fibs.ote_low = Decimal("10000") # Far away
    fibs.ote_high = Decimal("11000")
    fibs.fib_0_382 = Decimal("50050") # 0.1% away (tolerance 0.2%)
    fibs.fib_0_618 = Decimal("10000")
    fibs.fib_0_500 = Decimal("10000")
    fibs.fib_0_786 = Decimal("10000")
    
    score = scorer._score_fib_confluence(signal, fibs)
    assert score == 10.0


def test_fib_confluence_uses_config_tolerance():
    """_score_fib_confluence uses config.fib_proximity_bps for tolerance."""
    config = StrategyConfig()
    config.fib_proximity_bps = 10.0  # 0.1%
    scorer = SignalScorer(config)
    signal = Mock(spec=Signal)
    signal.entry_price = Decimal("50000")
    fibs = Mock(spec=FibonacciLevels)
    fibs.ote_low = Decimal("10000")
    fibs.ote_high = Decimal("11000")
    fibs.fib_0_382 = Decimal("50075")   # 0.15% away from 50000
    fibs.fib_0_618 = fibs.fib_0_500 = fibs.fib_0_786 = Decimal("10000")
    fibs.fib_1_272 = fibs.fib_1_618 = Decimal("10000")
    # 0.15% > 0.1% tolerance -> no match
    score = scorer._score_fib_confluence(signal, fibs)
    assert score == 0.0
    config.fib_proximity_bps = 20.0  # 0.2%
    scorer.config = config
    score = scorer._score_fib_confluence(signal, fibs)
    assert score == 10.0


# --- EMA Slope Scoring Tests ---

def _make_signal_with_slope(signal_type, slope):
    """Helper to create a signal mock with ema200_slope."""
    sig = Mock(spec=Signal)
    sig.signal_type = signal_type
    sig.ema200_slope = slope
    return sig


def test_ema_slope_aligned_long():
    """Rising EMA + LONG signal awards full bonus."""
    config = StrategyConfig()
    config.ema_slope_bonus = 7.0
    scorer = SignalScorer(config)
    sig = _make_signal_with_slope(SignalType.LONG, "up")
    assert scorer._score_ema_slope(sig) == 7.0


def test_ema_slope_aligned_short():
    """Falling EMA + SHORT signal awards full bonus."""
    config = StrategyConfig()
    config.ema_slope_bonus = 10.0
    scorer = SignalScorer(config)
    sig = _make_signal_with_slope(SignalType.SHORT, "down")
    assert scorer._score_ema_slope(sig) == 10.0


def test_ema_slope_flat_returns_zero():
    """Flat slope always returns zero regardless of direction."""
    config = StrategyConfig()
    config.ema_slope_bonus = 7.0
    scorer = SignalScorer(config)
    sig = _make_signal_with_slope(SignalType.LONG, "flat")
    assert scorer._score_ema_slope(sig) == 0.0


def test_ema_slope_counter_direction_returns_zero():
    """Counter-directional slope (rising EMA + SHORT) returns zero."""
    config = StrategyConfig()
    config.ema_slope_bonus = 7.0
    scorer = SignalScorer(config)
    sig = _make_signal_with_slope(SignalType.SHORT, "up")
    assert scorer._score_ema_slope(sig) == 0.0
    sig = _make_signal_with_slope(SignalType.LONG, "down")
    assert scorer._score_ema_slope(sig) == 0.0


def test_ema_slope_disabled_returns_zero():
    """When bonus is 0 (disabled), always returns zero."""
    config = StrategyConfig()
    config.ema_slope_bonus = 0.0
    scorer = SignalScorer(config)
    sig = _make_signal_with_slope(SignalType.LONG, "up")
    assert scorer._score_ema_slope(sig) == 0.0


# --- Adaptive Fib Tolerance Tests ---

def test_adaptive_fib_scales_with_atr_ratio():
    """High ATR ratio widens tolerance."""
    config = StrategyConfig()
    config.fib_proximity_bps = 20.0
    config.fib_proximity_adaptive_enabled = True
    config.fib_proximity_adaptive_scale = 0.5
    config.fib_proximity_max_bps = 50.0
    scorer = SignalScorer(config)
    sig = Mock(spec=Signal)
    sig.atr_ratio = Decimal("2.0")  # ATR is 2x average
    # effective = 20 * (1 + max(0, 2.0-1.0) * 0.5) = 20 * 1.5 = 30
    assert scorer._effective_fib_proximity_bps(sig) == 30.0


def test_adaptive_fib_respects_max_cap():
    """Adaptive tolerance is capped at fib_proximity_max_bps."""
    config = StrategyConfig()
    config.fib_proximity_bps = 20.0
    config.fib_proximity_adaptive_enabled = True
    config.fib_proximity_adaptive_scale = 2.0
    config.fib_proximity_max_bps = 35.0
    scorer = SignalScorer(config)
    sig = Mock(spec=Signal)
    sig.atr_ratio = Decimal("3.0")  # Would give 20*(1+2*2)=100 uncapped
    assert scorer._effective_fib_proximity_bps(sig) == 35.0


def test_adaptive_fib_no_atr_ratio_uses_base():
    """When atr_ratio is None, falls back to base tolerance."""
    config = StrategyConfig()
    config.fib_proximity_bps = 20.0
    config.fib_proximity_adaptive_enabled = True
    config.fib_proximity_adaptive_scale = 0.5
    config.fib_proximity_max_bps = 50.0
    scorer = SignalScorer(config)
    sig = Mock(spec=Signal)
    sig.atr_ratio = None
    assert scorer._effective_fib_proximity_bps(sig) == 20.0


def test_adaptive_fib_disabled_ignores_atr_ratio():
    """When adaptive is disabled, always uses base regardless of ATR ratio."""
    config = StrategyConfig()
    config.fib_proximity_bps = 20.0
    config.fib_proximity_adaptive_enabled = False
    scorer = SignalScorer(config)
    sig = Mock(spec=Signal)
    sig.atr_ratio = Decimal("5.0")
    assert scorer._effective_fib_proximity_bps(sig) == 20.0


def test_adaptive_fib_low_atr_ratio_no_expansion():
    """ATR ratio below 1.0 does not expand tolerance (max(0, ratio-1))."""
    config = StrategyConfig()
    config.fib_proximity_bps = 20.0
    config.fib_proximity_adaptive_enabled = True
    config.fib_proximity_adaptive_scale = 0.5
    config.fib_proximity_max_bps = 50.0
    scorer = SignalScorer(config)
    sig = Mock(spec=Signal)
    sig.atr_ratio = Decimal("0.5")
    assert scorer._effective_fib_proximity_bps(sig) == 20.0
