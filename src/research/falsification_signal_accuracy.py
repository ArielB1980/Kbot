"""Signal directional accuracy: measures if SMC signals predict direction.

For every signal the strategy generates, measure whether price moved in the
predicted direction over 1h, 4h, and 24h horizons. This intentionally avoids
the full replay execution stack because the diagnostic only needs candle
windows plus ``SMCEngine.generate_signal()``, not auction/reconciliation work.
"""

from __future__ import annotations

from bisect import bisect_right
from contextlib import contextmanager
import logging
import math
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.backtest.replay_harness.data_store import CandleBar, ReplayDataStore
from src.backtest.replay_harness.runner import BacktestRunner
from src.config.config import StrategyConfig
from src.domain.models import Candle, Signal, SignalType
from src.strategy.smc_engine import SMCEngine
from src.monitoring.logger import get_logger

logger = get_logger(__name__)

HORIZONS = {"1h": 4, "4h": 16, "24h": 96}  # Number of 15m ticks per horizon
_NOISY_LOGGERS = (
    "src.strategy.smc_engine",
    "src.memory.institutional_memory",
    "src.strategy.ev_engine",
)


@dataclass
class SignalCapture:
    """Captured signal from replay."""

    timestamp: str
    symbol: str
    direction: str  # "long" or "short"
    entry_price: float
    setup_type: str
    score: float


@dataclass
class HorizonResult:
    """Accuracy metrics for one time horizon."""

    horizon: str
    total_signals: int = 0
    correct: int = 0
    hit_rate: float = 0.0
    avg_correct_move_pct: float = 0.0
    avg_incorrect_move_pct: float = 0.0
    p_value: float = 1.0  # Binomial test vs 50%


def _bar_to_candle(symbol: str, timeframe: str, bar: CandleBar) -> Candle:
    """Convert a replay candle bar into the domain Candle model."""
    return Candle(
        timestamp=bar.timestamp,
        symbol=symbol,
        timeframe=timeframe,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
    )


def _slice_candles(
    candles: list[Candle],
    timestamps: list[datetime],
    current: datetime,
    *,
    limit: int = 500,
) -> list[Candle]:
    """Return the most recent candles up to ``current`` inclusive."""
    end_idx = bisect_right(timestamps, current)
    if end_idx <= 0:
        return []
    start_idx = max(0, end_idx - limit)
    return candles[start_idx:end_idx]


def _capture_signal(signals: list[SignalCapture], signal: Signal) -> None:
    """Append a directional signal to the capture list."""
    if signal.signal_type not in (SignalType.LONG, SignalType.SHORT):
        return
    signals.append(SignalCapture(
        timestamp=signal.timestamp.isoformat(),
        symbol=signal.symbol,
        direction=signal.signal_type.value,
        entry_price=float(signal.entry_price),
        setup_type=signal.setup_type.value if signal.setup_type else "unknown",
        score=signal.score,
    ))


@contextmanager
def _quiet_signal_scan_logs():
    """Suppress per-candle strategy chatter during bulk signal scans."""
    prior_levels: dict[str, int] = {}
    try:
        for name in _NOISY_LOGGERS:
            logger_obj = logging.getLogger(name)
            prior_levels[name] = logger_obj.level
            logger_obj.setLevel(logging.WARNING)
        yield
    finally:
        for name, level in prior_levels.items():
            logging.getLogger(name).setLevel(level)


def _scan_symbol_signals(
    engine: SMCEngine,
    symbol: str,
    store: ReplayDataStore,
    *,
    start: datetime,
    end: datetime,
) -> tuple[list[SignalCapture], list[tuple[datetime, float]]]:
    """Scan replay candles directly for one symbol and capture raw SMC signals."""
    required_tfs = ("15m", "1h", "4h", "1d")
    candles_by_tf: dict[str, list[Candle]] = {}
    timestamps_by_tf: dict[str, list[datetime]] = {}

    for timeframe in required_tfs:
        bars = list(store._candles.get(symbol, {}).get(timeframe, []))
        candles = [_bar_to_candle(symbol, timeframe, bar) for bar in bars]
        candles_by_tf[timeframe] = candles
        timestamps_by_tf[timeframe] = [c.timestamp for c in candles]

    candles_15m = candles_by_tf["15m"]
    if not candles_15m:
        return [], []

    captures: list[SignalCapture] = []
    price_history = [
        (c.timestamp, float(c.close))
        for c in candles_15m
        if start <= c.timestamp <= end
    ]

    for candle_15m in candles_15m:
        current = candle_15m.timestamp
        if current < start or current > end:
            continue

        signal = engine.generate_signal(
            symbol=symbol,
            regime_candles_1d=_slice_candles(
                candles_by_tf["1d"], timestamps_by_tf["1d"], current,
            ),
            decision_candles_4h=_slice_candles(
                candles_by_tf["4h"], timestamps_by_tf["4h"], current,
            ),
            refine_candles_1h=_slice_candles(
                candles_by_tf["1h"], timestamps_by_tf["1h"], current,
            ),
            refine_candles_15m=_slice_candles(
                candles_by_tf["15m"], timestamps_by_tf["15m"], current,
            ),
        )
        _capture_signal(captures, signal)

    return captures, price_history


def _binomial_p_value(n: int, k: int, p: float = 0.5) -> float:
    """One-sided binomial test: P(X >= k) under H0: p=0.5."""
    if n <= 0:
        return 1.0
    # Normal approximation for large n
    if n >= 30:
        z = (k - n * p) / math.sqrt(n * p * (1 - p))
        return 0.5 * math.erfc(z / math.sqrt(2))
    # Exact for small n
    total = 0.0
    for i in range(k, n + 1):
        total += math.comb(n, i) * (p ** i) * ((1 - p) ** (n - i))
    return total


def evaluate_accuracy(
    signals: list[SignalCapture],
    price_history: dict[str, list[tuple[datetime, float]]],
) -> dict[str, Any]:
    """Evaluate directional accuracy at multiple horizons."""
    results_by_horizon: dict[str, HorizonResult] = {}
    results_by_setup: dict[str, dict[str, HorizonResult]] = {}
    results_by_direction: dict[str, dict[str, HorizonResult]] = {}
    signal_details: list[dict[str, Any]] = []

    for horizon_name, n_ticks in HORIZONS.items():
        results_by_horizon[horizon_name] = HorizonResult(horizon=horizon_name)

    for sig in signals:
        sig_ts = datetime.fromisoformat(sig.timestamp)
        prices = price_history.get(sig.symbol, [])
        if not prices:
            continue

        # Find entry price index in price history
        entry_idx = None
        for i, (ts, _price) in enumerate(prices):
            if ts >= sig_ts:
                entry_idx = i
                break
        if entry_idx is None:
            continue

        detail: dict[str, Any] = {
            "timestamp": sig.timestamp,
            "symbol": sig.symbol,
            "direction": sig.direction,
            "entry_price": sig.entry_price,
            "setup_type": sig.setup_type,
        }

        for horizon_name, n_ticks in HORIZONS.items():
            future_idx = entry_idx + n_ticks
            if future_idx >= len(prices):
                continue

            future_price = prices[future_idx][1]
            move_pct = ((future_price - sig.entry_price) / sig.entry_price) * 100

            is_long = sig.direction == "long"
            correct = (move_pct > 0) if is_long else (move_pct < 0)

            hr = results_by_horizon[horizon_name]
            hr.total_signals += 1
            if correct:
                hr.correct += 1
                hr.avg_correct_move_pct += abs(move_pct)
            else:
                hr.avg_incorrect_move_pct += abs(move_pct)

            detail[f"{horizon_name}_move_pct"] = round(move_pct, 4)
            detail[f"{horizon_name}_correct"] = correct

            # By setup type
            if sig.setup_type not in results_by_setup:
                results_by_setup[sig.setup_type] = {}
            if horizon_name not in results_by_setup[sig.setup_type]:
                results_by_setup[sig.setup_type][horizon_name] = HorizonResult(horizon=horizon_name)
            shr = results_by_setup[sig.setup_type][horizon_name]
            shr.total_signals += 1
            if correct:
                shr.correct += 1

            # By direction
            if sig.direction not in results_by_direction:
                results_by_direction[sig.direction] = {}
            if horizon_name not in results_by_direction[sig.direction]:
                results_by_direction[sig.direction][horizon_name] = HorizonResult(horizon=horizon_name)
            dhr = results_by_direction[sig.direction][horizon_name]
            dhr.total_signals += 1
            if correct:
                dhr.correct += 1

        signal_details.append(detail)

    # Finalize averages and p-values
    for hr in results_by_horizon.values():
        if hr.total_signals > 0:
            hr.hit_rate = hr.correct / hr.total_signals
            if hr.correct > 0:
                hr.avg_correct_move_pct /= hr.correct
            incorrect_count = hr.total_signals - hr.correct
            if incorrect_count > 0:
                hr.avg_incorrect_move_pct /= incorrect_count
            hr.p_value = _binomial_p_value(hr.total_signals, hr.correct)

    def _finalize(hr: HorizonResult) -> dict[str, Any]:
        if hr.total_signals > 0:
            hr.hit_rate = hr.correct / hr.total_signals
            hr.p_value = _binomial_p_value(hr.total_signals, hr.correct)
        return asdict(hr)

    horizons_out = {k: _finalize(v) for k, v in results_by_horizon.items()}
    setup_out = {
        st: {h: _finalize(hr) for h, hr in horizons.items()}
        for st, horizons in results_by_setup.items()
    }
    direction_out = {
        d: {h: _finalize(hr) for h, hr in horizons.items()}
        for d, horizons in results_by_direction.items()
    }

    # Edge assessment
    best_horizon = max(
        results_by_horizon.values(),
        key=lambda h: h.hit_rate if h.total_signals >= 5 else 0,
        default=None,
    )
    has_edge = (
        best_horizon is not None
        and best_horizon.hit_rate > 0.55
        and best_horizon.p_value < 0.05
    )

    return {
        "total_signals": len(signals),
        "signals_evaluated": sum(hr.total_signals for hr in results_by_horizon.values()) // len(HORIZONS) if HORIZONS else 0,
        "horizons": horizons_out,
        "by_setup_type": setup_out,
        "by_direction": direction_out,
        "signal_details": signal_details,
        "edge_assessment": {
            "best_horizon": best_horizon.horizon if best_horizon else None,
            "best_hit_rate": round(best_horizon.hit_rate, 4) if best_horizon else None,
            "best_p_value": round(best_horizon.p_value, 4) if best_horizon else None,
            "has_directional_edge": has_edge,
        },
    }


async def run_falsification(
    data_dir: Path,
    symbols: list[str],
    days: int,
    timeframes: list[str],
    strategy_config: StrategyConfig,
) -> dict[str, Any]:
    """Run signal accuracy falsification test."""
    end = datetime.now(UTC)
    start = end - timedelta(days=days)
    harness = BacktestRunner(
        data_dir=data_dir,
        symbols=symbols,
        start=start,
        end=end,
        tick_interval_seconds=900,
        max_ticks=20000,
        timeframes=timeframes,
        config_overrides={},
        disable_cycle_guard_throttle=True,
        disable_db_mock=False,
    )

    logger.info("signal_accuracy_scan_start", symbols=symbols, days=days)
    harness._install_runtime_isolation()
    harness._setup_db_mock()
    try:
        store = ReplayDataStore(data_dir=data_dir, symbols=symbols, timeframes=timeframes)
        store.load()
        with _quiet_signal_scan_logs():
            engine = SMCEngine(strategy_config)

            captured: list[SignalCapture] = []
            price_history: dict[str, list[tuple[datetime, float]]] = {}
            for symbol in symbols:
                symbol_captured, symbol_prices = _scan_symbol_signals(
                    engine,
                    symbol,
                    store,
                    start=start,
                    end=end,
                )
                captured.extend(symbol_captured)
                if symbol_prices:
                    price_history[symbol] = symbol_prices
                logger.info(
                    "signal_accuracy_symbol_scan",
                    symbol=symbol,
                    signals_captured=len(symbol_captured),
                    price_points=len(symbol_prices),
                )
    finally:
        harness._teardown_db_mock()
        harness._restore_runtime_isolation()

    logger.info("signal_accuracy_scan_end", signals_captured=len(captured))

    if not captured:
        return {
            "generated_at": datetime.now(UTC).isoformat() + "Z",
            "total_signals": 0,
            "edge_assessment": {"has_directional_edge": None, "reason": "no_signals_generated"},
        }

    result = evaluate_accuracy(captured, price_history)
    result["generated_at"] = datetime.now(UTC).isoformat() + "Z"
    result["symbols"] = symbols
    result["days"] = days
    result["signal_detail_count"] = len(result.get("signal_details", []))
    result["signal_detail_symbol_count"] = len({
        row.get("symbol")
        for row in result.get("signal_details", [])
        if isinstance(row, dict) and row.get("symbol")
    })
    return result
