from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pandas as pd

from src.config.config import StrategyConfig
from src.domain.models import Candle, SetupType, SignalType
from src.strategy.signal_scorer import SignalScore
from src.strategy.smc_engine import HigherTFContext, SMCEngine


def _candles(symbol: str, tf: str, count: int, base: Decimal = Decimal("100")) -> list[Candle]:
    tf_hours = {"15m": Decimal("0.25"), "1h": Decimal("1"), "4h": Decimal("4"), "1d": Decimal("24")}
    step = tf_hours.get(tf, Decimal("1"))
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    out: list[Candle] = []
    for i in range(count):
        out.append(
            Candle(
                timestamp=start + timedelta(hours=float(step * i)),
                symbol=symbol,
                timeframe=tf,
                open=base,
                high=base + Decimal("2"),
                low=base - Decimal("2"),
                close=base + Decimal("0.1"),
                volume=Decimal("1000"),
            )
        )
    return out


class _FakeMemory:
    def __init__(self, conviction: float = 82.0):
        self.created = 0
        self.conviction = conviction

    def is_enabled_for_symbol(self, symbol: str) -> bool:
        return True

    def create_or_refresh_thesis(self, **kwargs):
        self.created += 1

    def update_conviction_for_symbol(self, symbol: str, **kwargs):
        return {
            "thesis_id": "thesis-abc",
            "symbol": symbol,
            "conviction": self.conviction,
            "status": "active",
            "time_decay": 3.0,
            "zone_rejection": 0.0,
            "volume_fade": 0.0,
        }

    def conviction_score_adjustment(self, conviction: float) -> float:
        return 3.5


def _patch_engine_for_signal(engine: SMCEngine) -> None:
    engine.indicators.calculate_adx = lambda candles, period: pd.DataFrame({f"ADX_{period}": [30.0]})
    engine.indicators.calculate_atr = lambda candles, period: pd.Series([Decimal("1.2")])
    engine.fibonacci_engine.calculate_levels = lambda candles, tf: None
    engine._determine_bias = lambda *args, **kwargs: "bullish"
    engine._detect_structure = lambda *args, **kwargs: {"order_block": {"high": Decimal("101"), "low": Decimal("99")}, "fvg": None, "bos": True}
    engine.ms_tracker.update_structure = lambda *args, **kwargs: (None, None)
    engine._apply_filters = lambda *args, **kwargs: True
    engine._calculate_levels = (
        lambda *args, **kwargs: (
            SignalType.LONG,
            Decimal("101"),
            Decimal("99"),
            Decimal("106"),
            [Decimal("106")],
            {"setup_type": SetupType.BOS, "regime": "wide_structure"},
        )
    )
    engine.signal_scorer.score_signal = (
        lambda *args, **kwargs: SignalScore(
            total_score=70.0,
            smc_quality=20.0,
            fib_confluence=10.0,
            htf_alignment=20.0,
            adx_strength=10.0,
            cost_efficiency=10.0,
        )
    )
    engine.signal_scorer.check_score_gate = lambda score, setup, bias: (True, 60.0)


def test_memory_conviction_is_attached_and_scored() -> None:
    cfg = StrategyConfig(
        higher_tf_enabled=True,
        higher_tf_mode="soft",
        require_ms_change_confirmation=False,
        memory_enabled=True,
        thesis_observe_only=False,
        thesis_score_enabled=True,
    )
    memory = _FakeMemory()
    engine = SMCEngine(cfg, institutional_memory=memory)
    _patch_engine_for_signal(engine)
    engine._detect_higher_tf_context = lambda symbol: HigherTFContext(
        weekly_fib_zone_low=Decimal("95"),
        weekly_fib_zone_high=Decimal("110"),
        daily_bias="bullish",
        allowed_entry=True,
        weekly_confluence_bonus=0.25,
    )

    sig = engine.generate_signal(
        "BTC/USD",
        regime_candles_1d=_candles("BTC/USD", "1d", 40),
        decision_candles_4h=_candles("BTC/USD", "4h", 260),
        refine_candles_1h=_candles("BTC/USD", "1h", 260),
        refine_candles_15m=_candles("BTC/USD", "15m", 260),
    )

    assert sig.signal_type == SignalType.LONG
    assert sig.score_breakdown["thesis_score_adj"] == 3.5
    assert sig.meta_info["thesis"]["conviction"] == 82.0
    assert memory.created == 1


def test_hard_mode_outside_zone_returns_no_signal() -> None:
    cfg = StrategyConfig(
        higher_tf_enabled=True,
        higher_tf_mode="hard",
        require_ms_change_confirmation=False,
        memory_enabled=True,
    )
    engine = SMCEngine(cfg, institutional_memory=_FakeMemory())
    _patch_engine_for_signal(engine)
    engine._detect_higher_tf_context = lambda symbol: HigherTFContext(
        weekly_fib_zone_low=Decimal("95"),
        weekly_fib_zone_high=Decimal("110"),
        daily_bias="bearish",
        allowed_entry=False,
        weekly_confluence_bonus=0.0,
    )

    sig = engine.generate_signal(
        "BTC/USD",
        regime_candles_1d=_candles("BTC/USD", "1d", 40),
        decision_candles_4h=_candles("BTC/USD", "4h", 260),
        refine_candles_1h=_candles("BTC/USD", "1h", 260),
        refine_candles_15m=_candles("BTC/USD", "15m", 260),
    )

    assert sig.signal_type == SignalType.NO_SIGNAL
    assert "Higher-TF hard reject" in sig.reasoning


def test_entry_blocked_when_conviction_below_minimum() -> None:
    cfg = StrategyConfig(
        higher_tf_enabled=True,
        higher_tf_mode="soft",
        require_ms_change_confirmation=False,
        memory_enabled=True,
        thesis_observe_only=False,
        thesis_management_enabled=True,
        conviction_min_for_entry=45.0,
    )
    engine = SMCEngine(cfg, institutional_memory=_FakeMemory(conviction=40.0))
    _patch_engine_for_signal(engine)
    engine._detect_higher_tf_context = lambda symbol: HigherTFContext(
        weekly_fib_zone_low=Decimal("95"),
        weekly_fib_zone_high=Decimal("110"),
        daily_bias="bullish",
        allowed_entry=True,
        weekly_confluence_bonus=0.25,
    )

    sig = engine.generate_signal(
        "BTC/USD",
        regime_candles_1d=_candles("BTC/USD", "1d", 40),
        decision_candles_4h=_candles("BTC/USD", "4h", 260),
        refine_candles_1h=_candles("BTC/USD", "1h", 260),
        refine_candles_15m=_candles("BTC/USD", "15m", 260),
    )

    assert sig.signal_type == SignalType.NO_SIGNAL
    assert "Entry blocked by thesis conviction gate" in sig.reasoning


def test_high_conviction_widens_stop_in_canary() -> None:
    cfg = StrategyConfig(
        higher_tf_enabled=True,
        higher_tf_mode="soft",
        require_ms_change_confirmation=False,
        memory_enabled=True,
        thesis_observe_only=False,
        thesis_management_enabled=True,
        conviction_min_for_entry=35.0,
        conviction_stop_sizing_enabled=True,
        conviction_stop_sizing_canary_symbols=["BTC/USD"],
        conviction_stop_high_threshold=60.0,
        conviction_stop_high_multiplier=1.5,
    )
    engine = SMCEngine(cfg, institutional_memory=_FakeMemory(conviction=82.0))
    _patch_engine_for_signal(engine)
    engine._detect_higher_tf_context = lambda symbol: HigherTFContext(
        weekly_fib_zone_low=Decimal("95"),
        weekly_fib_zone_high=Decimal("110"),
        daily_bias="bullish",
        allowed_entry=True,
        weekly_confluence_bonus=0.25,
    )

    sig = engine.generate_signal(
        "BTC/USD",
        regime_candles_1d=_candles("BTC/USD", "1d", 40),
        decision_candles_4h=_candles("BTC/USD", "4h", 260),
        refine_candles_1h=_candles("BTC/USD", "1h", 260),
        refine_candles_15m=_candles("BTC/USD", "15m", 260),
    )

    assert sig.signal_type == SignalType.LONG
    assert sig.stop_loss == Decimal("98.0")
    assert "Conviction stop widening applied" in sig.reasoning
