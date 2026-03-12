"""Candidate evaluation backends for sandbox autoresearch."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterable

from src.backtest.backtest_engine import BacktestEngine, BacktestMetrics
from src.config.config import Config
from src.monitoring.logger import get_logger
from src.research.kpi import metrics_from_backtest
from src.research.models import CandidateMetrics

logger = get_logger(__name__)


@dataclass(frozen=True)
class EvaluationSpec:
    """Inputs that define candidate evaluation."""

    symbols: tuple[str, ...]
    lookback_days: int
    starting_equity: Decimal
    mode: str = "backtest"  # backtest | mock
    sleep_between_symbols_seconds: float = 0.5


class CandidateEvaluator:
    """Evaluates candidate params against configured research backend."""

    def __init__(self, base_config: Config, spec: EvaluationSpec):
        self.base_config = base_config
        self.spec = spec

    async def evaluate(self, params: dict[str, float]) -> CandidateMetrics:
        """Evaluate one candidate and return normalized KPI payload."""
        if self.spec.mode == "mock":
            return self._evaluate_mock(params)
        return await self._evaluate_backtest(params)

    def _evaluate_mock(self, params: dict[str, float]) -> CandidateMetrics:
        """Deterministic mock evaluator used by tests and dry-runs."""
        payload = ",".join(f"{k}={v:.6f}" for k, v in sorted(params.items()))
        seed = int(hashlib.sha256(payload.encode("utf-8")).hexdigest()[:8], 16)
        # Stable pseudo-random-ish values.
        ret = ((seed % 500) / 100.0) - 1.5
        dd = 4.0 + ((seed // 7) % 700) / 100.0
        sharpe = ((seed // 11) % 300) / 100.0
        win_rate = 42.0 + ((seed // 13) % 400) / 10.0
        trades = 10 + ((seed // 17) % 180)
        sortino = sharpe * 1.05 if sharpe > 0 else None
        return CandidateMetrics(
            net_return_pct=ret,
            max_drawdown_pct=dd,
            sharpe=sharpe,
            sortino=sortino,
            win_rate_pct=win_rate,
            trade_count=trades,
            rejection_reasons=[],
        )

    async def _evaluate_backtest(self, params: dict[str, float]) -> CandidateMetrics:
        """Run aggregate symbol backtests and return averaged metrics."""
        config = self.base_config.model_copy(deep=True)
        _apply_params(config, params)
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=max(1, self.spec.lookback_days))

        all_metrics: list[BacktestMetrics] = []
        for symbol in self.spec.symbols:
            engine = BacktestEngine(config, symbol=symbol, starting_equity=self.spec.starting_equity)
            try:
                metrics = await engine.run(start_date=start_date, end_date=end_date)
                all_metrics.append(metrics)
            except Exception as exc:  # noqa: BLE001 - per-symbol failures are tolerated.
                logger.warning(
                    "Research symbol backtest failed",
                    symbol=symbol,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
            finally:
                if getattr(engine, "client", None):
                    try:
                        await engine.client.close()
                    except Exception:  # noqa: BLE001 - best effort close.
                        pass
            await asyncio.sleep(max(0.0, self.spec.sleep_between_symbols_seconds))

        if not all_metrics:
            return CandidateMetrics(
                net_return_pct=-999.0,
                max_drawdown_pct=100.0,
                sharpe=0.0,
                sortino=None,
                win_rate_pct=0.0,
                trade_count=0,
                rejection_reasons=["No successful symbol backtests"],
            )

        # Aggregate by averaging metric payloads.
        normalized = [metrics_from_backtest(m, self.spec.starting_equity) for m in all_metrics]
        sortinos = [m.sortino for m in normalized if m.sortino is not None]
        return CandidateMetrics(
            net_return_pct=sum(m.net_return_pct for m in normalized) / len(normalized),
            max_drawdown_pct=sum(m.max_drawdown_pct for m in normalized) / len(normalized),
            sharpe=sum(m.sharpe for m in normalized) / len(normalized),
            sortino=(sum(sortinos) / len(sortinos)) if sortinos else None,
            win_rate_pct=sum(m.win_rate_pct for m in normalized) / len(normalized),
            trade_count=int(sum(m.trade_count for m in normalized)),
            rejection_reasons=[],
        )


def _apply_params(config: Config, params: dict[str, float]) -> None:
    """Apply dot-path params to pydantic config object."""
    for key, value in params.items():
        head, _, tail = key.partition(".")
        if not tail:
            continue
        section = getattr(config, head)
        setattr(section, tail, value)

