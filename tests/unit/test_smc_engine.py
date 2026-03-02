"""
Test: SMC Engine deterministic behavior.

4H DECISION AUTHORITY HIERARCHY:
- 1D: Regime filter (EMA200 bias)
- 4H: Decision authority (OB/FVG/BOS, ATR for stops)
- 1H: Refinement (ADX, swing points)
- 15m: Refinement (entry timing)
"""
import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from src.domain.models import Candle
from src.strategy.smc_engine import SMCEngine
from src.config.config import StrategyConfig


def _make_candles(symbol: str, timeframe: str, count: int, base_price: Decimal = Decimal("50000")) -> list:
    """Helper to create test candles."""
    candles = []
    base_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    
    # Timeframe to hours mapping
    tf_hours = {"15m": 0.25, "1h": 1, "4h": 4, "1d": 24}
    hours = tf_hours.get(timeframe, 1)
    
    for i in range(count):
        candles.append(
            Candle(
                timestamp=base_time + timedelta(hours=i * hours),
                symbol=symbol,
                timeframe=timeframe,
                open=base_price,
                high=base_price + Decimal("500"),
                low=base_price - Decimal("500"),
                close=base_price,
                volume=Decimal("100"),
            )
        )
    return candles


def test_smc_engine_deterministic():
    """Test that same input produces same output."""
    # Create test candles for each timeframe
    candles_1d = _make_candles("BTC/USD", "1d", 30)
    candles_4h = _make_candles("BTC/USD", "4h", 100)
    candles_1h = _make_candles("BTC/USD", "1h", 200)
    candles_15m = _make_candles("BTC/USD", "15m", 200)
    
    config = StrategyConfig()
    engine = SMCEngine(config)
    
    # Generate signal twice with same inputs (4H Decision Authority)
    signal1 = engine.generate_signal(
        "BTC/USD",
        regime_candles_1d=candles_1d,
        decision_candles_4h=candles_4h,
        refine_candles_1h=candles_1h,
        refine_candles_15m=candles_15m,
    )
    signal2 = engine.generate_signal(
        "BTC/USD",
        regime_candles_1d=candles_1d,
        decision_candles_4h=candles_4h,
        refine_candles_1h=candles_1h,
        refine_candles_15m=candles_15m,
    )
    
    # Should be identical
    assert signal1.signal_type == signal2.signal_type
    assert signal1.entry_price == signal2.entry_price
    assert signal1.reasoning == signal2.reasoning


def test_smc_engine_no_4h_structure_returns_no_signal():
    """Test that without valid 4H structure, no signal is generated."""
    # Create flat candles (no structure)
    candles_1d = _make_candles("BTC/USD", "1d", 30)
    candles_4h = _make_candles("BTC/USD", "4h", 50)  # Fewer candles, flat
    candles_1h = _make_candles("BTC/USD", "1h", 200)
    candles_15m = _make_candles("BTC/USD", "15m", 200)
    
    config = StrategyConfig()
    engine = SMCEngine(config)
    
    signal = engine.generate_signal(
        "BTC/USD",
        regime_candles_1d=candles_1d,
        decision_candles_4h=candles_4h,
        refine_candles_1h=candles_1h,
        refine_candles_15m=candles_15m,
    )
    
    # Should return NO_SIGNAL due to 4H structure guard
    from src.domain.models import SignalType
    assert signal.signal_type == SignalType.NO_SIGNAL
    # Should mention 4H in reasoning
    assert "4H" in signal.reasoning or "no_4h_structure" in signal.regime


def test_fvg_min_size_global_threshold_applies_to_all_symbols():
    config = StrategyConfig(
        fvg_min_size_pct=0.0007,  # promoted global threshold (0.07%)
    )
    engine = SMCEngine(config)
    base_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    candles = [
        Candle(
            timestamp=base_time,
            symbol="BTC/USD",
            timeframe="4h",
            open=Decimal("0.9995"),
            high=Decimal("1.0000"),
            low=Decimal("0.9985"),
            close=Decimal("0.9998"),
            volume=Decimal("100"),
        ),
        Candle(
            timestamp=base_time + timedelta(hours=4),
            symbol="BTC/USD",
            timeframe="4h",
            open=Decimal("0.9999"),
            high=Decimal("1.0002"),
            low=Decimal("0.9992"),
            close=Decimal("1.0000"),
            volume=Decimal("100"),
        ),
        Candle(
            timestamp=base_time + timedelta(hours=8),
            symbol="BTC/USD",
            timeframe="4h",
            open=Decimal("1.0010"),
            high=Decimal("1.0014"),
            low=Decimal("1.0008"),
            close=Decimal("1.0012"),
            volume=Decimal("100"),
        ),
    ]

    # Gap size is 0.0008 (0.08%): should pass promoted global threshold (0.07%).
    assert engine._find_fair_value_gap(candles, "bullish", symbol="BTC/USD") is not None
    # Same setup should also pass for non-canary symbols under global threshold.
    assert engine._find_fair_value_gap(candles, "bullish", symbol="LINK/USD") is not None
