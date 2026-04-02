"""Signal directional accuracy: measures if SMC signals predict direction.

For every signal the strategy generates, measures whether price moved in the
predicted direction over 1h, 4h, and 24h horizons. If directional accuracy
is ~50%, the signals are noise regardless of parameter calibration.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.backtest.replay_harness.runner import BacktestRunner
from src.domain.models import Candle, Signal, SignalType
from src.monitoring.logger import get_logger

logger = get_logger(__name__)

HORIZONS = {"1h": 4, "4h": 16, "24h": 96}  # Number of 15m ticks per horizon


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


class SignalInterceptor:
    """Wraps SMCEngine.generate_signal to capture signals while passing through."""

    def __init__(self, original_fn: Any):
        self._original = original_fn
        self.captured: list[SignalCapture] = []

    def generate_signal(
        self,
        symbol: str,
        regime_candles_1d: list[Candle],
        decision_candles_4h: list[Candle],
        refine_candles_1h: list[Candle],
        refine_candles_15m: list[Candle],
    ) -> Signal:
        """Call original engine, capture non-NO_SIGNAL results."""
        signal = self._original(
            symbol=symbol,
            regime_candles_1d=regime_candles_1d,
            decision_candles_4h=decision_candles_4h,
            refine_candles_1h=refine_candles_1h,
            refine_candles_15m=refine_candles_15m,
        )
        if signal.signal_type in (SignalType.LONG, SignalType.SHORT):
            self.captured.append(SignalCapture(
                timestamp=signal.timestamp.isoformat(),
                symbol=symbol,
                direction=signal.signal_type.value,
                entry_price=float(signal.entry_price),
                setup_type=signal.setup_type.value if signal.setup_type else "unknown",
                score=signal.score,
            ))
        return signal


class SignalCaptureRunner(BacktestRunner):
    """BacktestRunner that intercepts signals during replay."""

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self.interceptor: SignalInterceptor | None = None

    async def _run_with_db_mock(self) -> Any:
        """Override to install signal interceptor after init."""
        await self._initialize()

        # Install the interceptor
        if self._live_trading is not None:
            original = self._live_trading.smc_engine.generate_signal
            self.interceptor = SignalInterceptor(original)
            self._live_trading.smc_engine.generate_signal = self.interceptor.generate_signal

        # Disable cycle guard throttle
        if (
            self._disable_cycle_guard_throttle
            and self._live_trading is not None
            and getattr(self._live_trading, "hardening", None) is not None
            and getattr(self._live_trading.hardening, "cycle_guard", None) is not None
        ):
            self._live_trading.hardening.cycle_guard.min_interval = timedelta(seconds=0)

        # Run tick loop
        tick_count = 0
        current = self._start

        # Store price history for forward-looking accuracy check
        self._price_history: dict[str, list[tuple[datetime, float]]] = {}

        while current <= self._end:
            if self._max_ticks and tick_count >= self._max_ticks:
                break
            self._clock.set(current)
            self._exchange.step(current)

            # Record prices for all symbols at this tick
            if self._live_trading is not None:
                for sym in self._symbols:
                    candles = self._live_trading.candle_manager.get_candles(sym, "15m")
                    if candles:
                        if sym not in self._price_history:
                            self._price_history[sym] = []
                        self._price_history[sym].append(
                            (current, float(candles[-1].close))
                        )

            try:
                await self._run_tick()
                if (
                    self._live_trading is not None
                    and getattr(self._live_trading, "execution_gateway", None) is not None
                ):
                    await self._live_trading.execution_gateway.poll_and_process_order_updates()
                self._metrics.total_ticks += 1
            except Exception:
                self._metrics.failed_ticks += 1

            tick_count += 1
            current += timedelta(seconds=self._tick_interval)

        return self._metrics


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
        "signal_details": signal_details[:100],  # Cap to avoid huge output
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
) -> dict[str, Any]:
    """Run signal accuracy falsification test."""
    end = datetime.now(UTC)
    start = end - timedelta(days=days)

    runner = SignalCaptureRunner(
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

    logger.info("signal_accuracy_replay_start", symbols=symbols, days=days)
    await runner.run()
    logger.info(
        "signal_accuracy_replay_end",
        signals_captured=len(runner.interceptor.captured) if runner.interceptor else 0,
    )

    if not runner.interceptor or not runner.interceptor.captured:
        return {
            "generated_at": datetime.now(UTC).isoformat() + "Z",
            "total_signals": 0,
            "edge_assessment": {"has_directional_edge": None, "reason": "no_signals_generated"},
        }

    result = evaluate_accuracy(runner.interceptor.captured, runner._price_history)
    result["generated_at"] = datetime.now(UTC).isoformat() + "Z"
    result["symbols"] = symbols
    result["days"] = days
    return result
