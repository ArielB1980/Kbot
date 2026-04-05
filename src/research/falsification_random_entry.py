"""Random-entry baseline: same risk management, random signals.

Proves whether SMC signal logic adds edge over random entry.
If random entries produce similar risk-adjusted returns, the signals
have no edge — it's the risk management doing all the work.
"""

from __future__ import annotations

import asyncio
import json
import random
from collections import deque
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


def _parse_setup_type(raw: Any) -> SetupType:
    """Best-effort SetupType normalization from a serialized signal record."""
    if isinstance(raw, SetupType):
        return raw
    raw_value = str(raw or "").strip().lower()
    for setup in SetupType:
        if setup.value == raw_value:
            return setup
    return SetupType.OB


def _load_strategy_signal_schedule(strategy_signal_file: Path | None) -> dict[str, deque[dict[str, Any]]]:
    """Load per-symbol signal timestamps from a falsification signal-accuracy artifact."""
    if strategy_signal_file is None or not strategy_signal_file.exists():
        return {}
    try:
        payload = json.loads(strategy_signal_file.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        logger.warning("random_entry_schedule_load_failed", path=str(strategy_signal_file))
        return {}

    detail_rows = payload.get("signal_details", []) or []
    expected_total = int(payload.get("total_signals") or 0)
    if expected_total and len(detail_rows) != expected_total:
        raise ValueError(
            f"strategy signal schedule is truncated: detail_rows={len(detail_rows)} total_signals={expected_total}"
        )

    schedule: dict[str, deque[dict[str, Any]]] = {}
    for row in detail_rows:
        symbol = str(row.get("symbol") or "").strip()
        timestamp_raw = str(row.get("timestamp") or "").strip()
        if not symbol or not timestamp_raw:
            continue
        try:
            ts = datetime.fromisoformat(timestamp_raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        schedule.setdefault(symbol, deque()).append({
            "timestamp": ts,
            "setup_type": _parse_setup_type(row.get("setup_type")),
            "score": float(row.get("score", 50.0) or 50.0),
        })
    return schedule


def _count_scheduled_signals(schedule: dict[str, deque[dict[str, Any]]]) -> int:
    return sum(len(queue) for queue in schedule.values())


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

    def __init__(
        self,
        signal_probability: float | None,
        seed: int,
        *,
        scheduled_signals: dict[str, deque[dict[str, Any]]] | None = None,
    ):
        self._rng = random.Random(seed)
        self._signal_probability = signal_probability
        self._scheduled_signals = {
            symbol: deque(events)
            for symbol, events in (scheduled_signals or {}).items()
        }

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

        scheduled = None
        current_ts = candles[-1].timestamp
        queue = self._scheduled_signals.get(symbol)
        if queue:
            while queue and queue[0]["timestamp"] < current_ts:
                queue.popleft()
            if queue and queue[0]["timestamp"] == current_ts:
                scheduled = queue.popleft()

        if scheduled is None:
            if self._signal_probability is None or self._rng.random() > self._signal_probability:
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
            setup_type=scheduled["setup_type"] if scheduled is not None else self._rng.choice(list(SetupType)),
            regime="tight_smc",
            higher_tf_bias="neutral",
            adx=Decimal("25"),
            atr=atr,
            ema200_slope="flat",
            score=float(scheduled["score"]) if scheduled is not None else 50.0,
        )


class RandomEntryRunner(BacktestRunner):
    """BacktestRunner that replaces signal generation with random entries."""

    def __init__(
        self,
        signal_probability: float | None,
        seed: int,
        scheduled_signals: dict[str, deque[dict[str, Any]]] | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._signal_probability = signal_probability
        self._seed = seed
        self._scheduled_signals = scheduled_signals or {}

    async def _run_with_db_mock(self) -> Any:
        """Override to inject random signal generator after init."""
        await self._initialize()

        # Patch the signal engine
        gen = RandomSignalGenerator(
            self._signal_probability,
            self._seed,
            scheduled_signals=self._scheduled_signals,
        )
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
    signal_probability: float | None,
    seed: int,
    timeframes: list[str],
    starting_equity: Decimal,
    scheduled_signals: dict[str, deque[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Run one random-entry replay trial and return metrics."""
    runner = RandomEntryRunner(
        signal_probability=signal_probability,
        seed=seed,
        scheduled_signals=scheduled_signals,
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
    signal_probability: float | None,
    timeframes: list[str],
    starting_equity: Decimal = Decimal("10000"),
    strategy_signal_file: Path | None = None,
) -> dict[str, Any]:
    """Run N random-entry trials and return comparison report."""
    end = datetime.now(UTC)
    start = end - timedelta(days=days)
    strategy_schedule = _load_strategy_signal_schedule(strategy_signal_file)
    scheduled_signal_count = _count_scheduled_signals(strategy_schedule)
    if strategy_schedule:
        logger.info(
            "random_entry_schedule_loaded",
            path=str(strategy_signal_file),
            scheduled_signals=scheduled_signal_count,
            symbols=len(strategy_schedule),
        )
    else:
        logger.info(
            "random_entry_schedule_missing",
            path=str(strategy_signal_file) if strategy_signal_file is not None else None,
            fallback_signal_probability=signal_probability,
        )

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
                    scheduled_signals=strategy_schedule,
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
        "strategy_signal_file": str(strategy_signal_file) if strategy_signal_file else None,
        "strategy_signal_count": scheduled_signal_count,
        "signal_schedule_mode": "strategy_schedule" if strategy_schedule else "probability",
        "num_trials": num_trials,
        "symbols": symbols,
        "days": days,
    }
