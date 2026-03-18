"""
Phase 1 Safety Net: SMC Engine golden output snapshot test.

Captures the exact output of the SMC engine on a fixed, deterministic
set of candle data. After refactoring, this test proves that trading
logic produces identical results.

If this test fails after a refactor:
  1. The refactor changed trading behavior (unintentional -- fix it)
  2. A legitimate behavior change was made (update the golden fixture)

To regenerate the golden fixture:
  pytest tests/unit/test_smc_golden_output.py --regenerate-golden -s
"""
import json
import os
import re
import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from pathlib import Path

from src.domain.models import Candle, SignalType
from src.strategy.smc_engine import SMCEngine
from src.config.config import StrategyConfig


GOLDEN_FIXTURE_PATH = Path(__file__).parent.parent / "fixtures" / "golden_smc_output.json"


def _normalize_reasoning_numerics(text: str) -> str:
    """Normalize decimal precision in reasoning text across Python/pandas builds."""

    def _round_decimal(match: re.Match[str]) -> str:
        raw = match.group(0)
        try:
            val = Decimal(raw)
        except Exception:
            return raw
        # Keep deterministic precision while ignoring sub-ulp drift.
        return format(val.quantize(Decimal("0.000000000001")), "f")

    return re.sub(r"\d+\.\d+", _round_decimal, text or "")


def _make_trending_candles(
    symbol: str,
    timeframe: str,
    count: int,
    start_price: Decimal,
    trend_pct_per_bar: Decimal = Decimal("0.002"),
    volatility_pct: Decimal = Decimal("0.01"),
) -> list:
    """Create candles with a clear uptrend and realistic OHLC structure.

    This produces candles that are more likely to trigger SMC signals
    than flat/random candles, giving us a richer golden output to snapshot.
    """
    candles = []
    base_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    tf_hours = {"15m": 0.25, "1h": 1, "4h": 4, "1d": 24}
    hours = tf_hours.get(timeframe, 1)

    price = start_price
    for i in range(count):
        # Uptrend with pullbacks every 5-7 bars
        if i % 7 < 5:
            move = price * trend_pct_per_bar
        else:
            move = -price * trend_pct_per_bar * Decimal("0.5")

        open_p = price
        close_p = price + move
        high_p = max(open_p, close_p) + price * volatility_pct * Decimal("0.5")
        low_p = min(open_p, close_p) - price * volatility_pct * Decimal("0.5")

        candles.append(
            Candle(
                timestamp=base_time + timedelta(hours=i * hours),
                symbol=symbol,
                timeframe=timeframe,
                open=open_p,
                high=high_p,
                low=low_p,
                close=close_p,
                volume=Decimal("1000") + Decimal(str(i * 10)),
            )
        )
        price = close_p

    return candles


def _generate_smc_output(symbol: str = "BTC/USD") -> dict:
    """Run SMC engine on fixed input and capture full output."""
    config = StrategyConfig()
    engine = SMCEngine(config)

    # Generate candles with enough history for all indicators
    candles_1d = _make_trending_candles(symbol, "1d", 250, Decimal("40000"))
    candles_4h = _make_trending_candles(symbol, "4h", 500, Decimal("40000"))
    candles_1h = _make_trending_candles(symbol, "1h", 500, Decimal("40000"))
    candles_15m = _make_trending_candles(symbol, "15m", 500, Decimal("40000"))

    signal = engine.generate_signal(
        symbol,
        regime_candles_1d=candles_1d,
        decision_candles_4h=candles_4h,
        refine_candles_1h=candles_1h,
        refine_candles_15m=candles_15m,
    )

    # Serialize signal to a comparable dict
    output = {
        "symbol": signal.symbol,
        "signal_type": signal.signal_type.value,
        "entry_price": str(signal.entry_price),
        "stop_loss": str(signal.stop_loss),
        "take_profit": str(signal.take_profit) if signal.take_profit else None,
        "setup_type": signal.setup_type.value,
        "regime": signal.regime,
        "higher_tf_bias": signal.higher_tf_bias,
        "adx": str(signal.adx),
        "atr": str(signal.atr),
        "ema200_slope": signal.ema200_slope,
        "score": signal.score,
        "reasoning": signal.reasoning,
        "tp_candidates": [str(tp) for tp in signal.tp_candidates],
        "score_breakdown": signal.score_breakdown,
    }
    return output


def test_smc_golden_output(request):
    """Compare SMC engine output against golden fixture.

    Use --regenerate-golden flag to create/update the fixture.
    """
    regenerate = request.config.getoption("--regenerate-golden", default=False)

    current_output = _generate_smc_output()

    if regenerate or not GOLDEN_FIXTURE_PATH.exists():
        # Save new golden fixture
        GOLDEN_FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(GOLDEN_FIXTURE_PATH, "w") as f:
            json.dump(current_output, f, indent=2, sort_keys=True)
        pytest.skip(
            f"Golden fixture {'regenerated' if regenerate else 'created'} "
            f"at {GOLDEN_FIXTURE_PATH}. Run again to validate."
        )

    # Load golden fixture and compare
    with open(GOLDEN_FIXTURE_PATH) as f:
        golden = json.load(f)

    # Fields that hold float-derived values subject to platform rounding
    _FLOAT_TOLERANCE_FIELDS = {"adx", "atr", "ema200_slope", "score"}

    # Compare each field for clear error messages
    for key in golden:
        assert key in current_output, f"Missing field '{key}' in current output"
        g_val, c_val = golden[key], current_output[key]
        if key == "reasoning" and isinstance(g_val, str) and isinstance(c_val, str):
            g_val = _normalize_reasoning_numerics(g_val)
            c_val = _normalize_reasoning_numerics(c_val)
        if g_val != c_val:
            if key in _FLOAT_TOLERANCE_FIELDS:
                try:
                    if abs(float(g_val) - float(c_val)) < 1e-10:
                        continue
                except (ValueError, TypeError):
                    pass
            pytest.fail(
                f"SMC output diverged on '{key}':\n"
                f"  Golden:  {g_val}\n"
                f"  Current: {c_val}\n\n"
                f"If this change is intentional, regenerate the fixture:\n"
                f"  pytest tests/unit/test_smc_golden_output.py "
                f"--regenerate-golden -s"
            )

    # Check no unexpected new fields
    for key in current_output:
        assert key in golden, (
            f"New field '{key}' in SMC output not in golden fixture. "
            f"Regenerate if intentional."
        )


def test_smc_output_is_deterministic():
    """Same input must produce identical output across multiple runs."""
    output1 = _generate_smc_output()
    output2 = _generate_smc_output()

    assert output1 == output2, (
        "SMC engine produced different output for identical input. "
        "This indicates non-deterministic behavior (e.g., random, time-dependent)."
    )



# Note: The --regenerate-golden CLI flag is registered in tests/conftest.py
