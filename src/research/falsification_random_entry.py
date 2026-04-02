"""Random-entry baseline: same risk management, random signals.

Proves whether SMC signal logic adds edge over random entry.
If random entries produce similar risk-adjusted returns, the signals
have no edge — it's the risk management doing all the work.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from src.backtest.replay_harness.runner import BacktestRunner
from src.domain.models import Candle, SetupType, Signal, SignalType
from src.monitoring.logger import get_logger
from src.research.evaluator import _metrics_from_replay

logger = get_logger(__name__)


def _compute_atr(candles: list[Candle], period: int = 14) -> Decimal:
    """Simple ATR from candle list."""
    if len(candles) < 2:
        return Decimal("1")
    trs: list[Decimal] = []
    for i in range(1, min(len(candles), period + 1)):
        high = candles[i].high
        low = candles[i].low
        prev_close = candles[i - 1].close
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    return sum(trs) / Decimal(str(len(trs))) if trs else Decimal("1")


class RandomSignalGenerator:
    """Replaces SMCEngine.generate_signal with random entries."""

    def __init__(self, signal_probability: float, seed: int):
        self._rng = random.Random(seed)
        self._signal_probability = signal_probability

    def _no_signal(self, symbol: str, candles: list[Candle]) -> Signal:
        ts = candles[-1].timestamp if candles else datetime.now(UTC)
        price = candles[-1].close if candles else Decimal("0")
        return Signal(
            timestamp=ts,
            symbol=symbol,
            signal_type=SignalType.NO_SIGNAL,
            entry_price=price,
            stop_loss=Decimal("0"),
            take_profit=None,
            reasoning="random_no_signal",
            setup_type=SetupType.OB,
            regime="tight_smc",
            higher_tf_bias="neutral",
            adx=Decimal("25"),
            atr=Decimal("0"),
            ema200_slope="flat",
        )

    def generate_signal(
        self,
        symbol: str,
        regime_candles_1d: list[Candle],
        decision_candles_4h: list[Candle],
        refine_candles_1h: list[Candle],
        refine_candles_15m: list[Candle],
    ) -> Signal:
        """Random entry at configured probability."""
        candles = refine_candles_15m
        if not candles:
            return self._no_signal(symbol, [])

        if self._rng.random() > self._signal_probability:
            return self._no_signal(symbol, candles)

        price = candles[-1].close
        atr = _compute_atr(refine_candles_1h or candles)
        direction = SignalType.LONG if self._rng.random() > 0.5 else SignalType.SHORT
        stop_dist = atr * Decimal("1.5")
        tp_dist = atr * Decimal("3.0")

        if direction == SignalType.LONG:
            stop_loss = price - stop_dist
            take_profit = price + tp_dist
        else:
            stop_loss = price + stop_dist
            take_profit = price - tp_dist

        return Signal(
            timestamp=candles[-1].timestamp,
            symbol=symbol,
            signal_type=direction,
            entry_price=price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            reasoning="random_entry_baseline",
            setup_type=self._rng.choice(list(SetupType)),
            regime="tight_smc",
            higher_tf_bias="neutral",
            adx=Decimal("25"),
            atr=atr,
            ema200_slope="flat",
            score=50.0,
        )


class RandomEntryRunner(BacktestRunner):
    """BacktestRunner that replaces signal generation with random entries."""

    def __init__(
        self,
        signal_probability: float,
        seed: int,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._signal_probability = signal_probability
        self._seed = seed

    async def _run_with_db_mock(self) -> Any:
        """Override to inject random signal generator after init."""
        await self._initialize()

        # Patch the signal engine
        gen = RandomSignalGenerator(self._signal_probability, self._seed)
        if self._live_trading is not None:
            self._live_trading.smc_engine.generate_signal = gen.generate_signal

        # Disable cycle guard throttle (same as parent)
        if (
            self._disable_cycle_guard_throttle
            and self._live_trading is not None
            and getattr(self._live_trading, "hardening", None) is not None
            and getattr(self._live_trading.hardening, "cycle_guard", None) is not None
        ):
            from datetime import timedelta as td
            self._live_trading.hardening.cycle_guard.min_interval = td(seconds=0)

        # Run the tick loop (copied from parent, minus the init)
        tick_count = 0
        current = self._start

        while current <= self._end:
            if self._max_ticks and tick_count >= self._max_ticks:
                break
            self._clock.set(current)
            self._exchange.step(current)
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


async def run_single_trial(
    data_dir: Path,
    symbols: list[str],
    start: datetime,
    end: datetime,
    signal_probability: float,
    seed: int,
    timeframes: list[str],
    starting_equity: Decimal,
) -> dict[str, Any]:
    """Run one random-entry replay trial and return metrics."""
    runner = RandomEntryRunner(
        signal_probability=signal_probability,
        seed=seed,
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
    replay = await runner.run()
    metrics = _metrics_from_replay(replay, starting_equity)
    return asdict(metrics)


async def run_falsification(
    data_dir: Path,
    symbols: list[str],
    days: int,
    num_trials: int,
    signal_probability: float,
    timeframes: list[str],
    starting_equity: Decimal = Decimal("10000"),
) -> dict[str, Any]:
    """Run N random-entry trials and return comparison report."""
    end = datetime.now(UTC)
    start = end - timedelta(days=days)

    trial_results: list[dict[str, Any]] = []
    for i in range(num_trials):
        logger.info("random_entry_trial_start", trial=i, seed=i + 42)
        try:
            result = await asyncio.wait_for(
                run_single_trial(
                    data_dir=data_dir,
                    symbols=symbols,
                    start=start,
                    end=end,
                    signal_probability=signal_probability,
                    seed=i + 42,
                    timeframes=timeframes,
                    starting_equity=starting_equity,
                ),
                timeout=1800,
            )
            trial_results.append(result)
            logger.info("random_entry_trial_end", trial=i, result=result)
        except Exception as exc:
            logger.warning("random_entry_trial_failed", trial=i, error=str(exc))
            trial_results.append({"error": str(exc)})

    successful = [t for t in trial_results if "error" not in t]
    if not successful:
        return {
            "trials": trial_results,
            "random_mean": None,
            "edge_assessment": {"has_edge": None, "reason": "all_trials_failed"},
        }

    avg: dict[str, float] = {}
    for key in ["net_return_pct", "max_drawdown_pct", "sharpe", "win_rate_pct", "trade_count"]:
        vals = [t[key] for t in successful if t.get(key) is not None]
        avg[key] = sum(vals) / len(vals) if vals else 0.0

    return {
        "generated_at": datetime.now(UTC).isoformat() + "Z",
        "trials": trial_results,
        "num_successful": len(successful),
        "random_mean": avg,
        "signal_probability": signal_probability,
        "num_trials": num_trials,
        "symbols": symbols,
        "days": days,
    }
