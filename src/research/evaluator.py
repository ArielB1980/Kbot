"""Candidate evaluation backends for sandbox autoresearch."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable

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
    window_offsets_days: tuple[int, ...] = (0, 30, 60)
    holdout_ratio: float = 0.30


@dataclass
class EvaluationOutcome:
    """Evaluation result including diagnostics used for robust ranking."""

    metrics: CandidateMetrics
    diagnostics: dict[str, Any]


class CandidateEvaluator:
    """Evaluates candidate params against configured research backend."""

    def __init__(self, base_config: Config, spec: EvaluationSpec):
        self.base_config = base_config
        self.spec = spec

    async def evaluate(self, params: dict[str, float]) -> EvaluationOutcome:
        """Evaluate one candidate and return normalized KPI payload and diagnostics."""
        if self.spec.mode == "mock":
            metrics = self._evaluate_mock(params)
            return EvaluationOutcome(
                metrics=metrics,
                diagnostics={
                    "composite_score_inputs": {"train_weight": 0.4, "holdout_weight": 0.6},
                    "per_window": [],
                    "train_score": None,
                    "holdout_score": None,
                    "composite_score": None,
                },
            )
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

    async def _evaluate_backtest(self, params: dict[str, float]) -> EvaluationOutcome:
        """Run multi-window split backtests and return robust blended metrics."""
        config = self.base_config.model_copy(deep=True)
        _apply_params(config, params)

        holdout_ratio = min(0.8, max(0.1, float(self.spec.holdout_ratio)))
        now = datetime.now(timezone.utc)
        windows: list[dict[str, Any]] = []
        train_metrics_all: list[CandidateMetrics] = []
        holdout_metrics_all: list[CandidateMetrics] = []

        for offset_days in self.spec.window_offsets_days:
            end_date = now - timedelta(days=max(0, int(offset_days)))
            start_date = end_date - timedelta(days=max(1, self.spec.lookback_days))
            split_date = start_date + timedelta(days=self.spec.lookback_days * (1.0 - holdout_ratio))

            train_metrics = await self._run_aggregate_for_period(config, start_date, split_date)
            holdout_metrics = await self._run_aggregate_for_period(config, split_date, end_date)
            windows.append(
                {
                    "offset_days": int(offset_days),
                    "train": _metrics_to_dict(train_metrics),
                    "holdout": _metrics_to_dict(holdout_metrics),
                }
            )
            if train_metrics.trade_count > 0:
                train_metrics_all.append(train_metrics)
            if holdout_metrics.trade_count > 0:
                holdout_metrics_all.append(holdout_metrics)

        if not holdout_metrics_all:
            failed = CandidateMetrics(
                net_return_pct=-999.0,
                max_drawdown_pct=100.0,
                sharpe=0.0,
                sortino=None,
                win_rate_pct=0.0,
                trade_count=0,
                rejection_reasons=["No successful holdout backtests across windows"],
            )
            return EvaluationOutcome(
                metrics=failed,
                diagnostics={
                    "per_window": windows,
                    "train_score": None,
                    "holdout_score": None,
                    "composite_score": -10_000.0,
                },
            )

        train_agg = _average_metrics(train_metrics_all) if train_metrics_all else _average_metrics(holdout_metrics_all)
        holdout_agg = _average_metrics(holdout_metrics_all)
        blended = _blend_metrics(train_agg, holdout_agg, train_weight=0.4, holdout_weight=0.6)

        from src.research.kpi import score_candidate

        train_score = score_candidate(train_agg)
        holdout_score = score_candidate(holdout_agg)
        composite_score = (0.4 * train_score) + (0.6 * holdout_score)

        return EvaluationOutcome(
            metrics=blended,
            diagnostics={
                "per_window": windows,
                "train_score": train_score,
                "holdout_score": holdout_score,
                "composite_score": composite_score,
                "composite_score_inputs": {"train_weight": 0.4, "holdout_weight": 0.6},
            },
        )

    async def _run_aggregate_for_period(
        self,
        config: Config,
        start_date: datetime,
        end_date: datetime,
    ) -> CandidateMetrics:
        """Run symbol-level backtests for one period and aggregate to KPIs."""
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
                    start=start_date.isoformat(),
                    end=end_date.isoformat(),
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
        normalized = [metrics_from_backtest(m, self.spec.starting_equity) for m in all_metrics]
        return _average_metrics(normalized)


def _apply_params(config: Config, params: dict[str, float]) -> None:
    """Apply dot-path params to pydantic config object."""
    for key, value in params.items():
        head, _, tail = key.partition(".")
        if not tail:
            continue
        section = getattr(config, head)
        setattr(section, tail, value)


def _average_metrics(metrics_list: list[CandidateMetrics]) -> CandidateMetrics:
    """Average a list of candidate metric payloads."""
    sortinos = [m.sortino for m in metrics_list if m.sortino is not None]
    reasons: list[str] = []
    for m in metrics_list:
        reasons.extend(m.rejection_reasons)
    return CandidateMetrics(
        net_return_pct=sum(m.net_return_pct for m in metrics_list) / len(metrics_list),
        max_drawdown_pct=sum(m.max_drawdown_pct for m in metrics_list) / len(metrics_list),
        sharpe=sum(m.sharpe for m in metrics_list) / len(metrics_list),
        sortino=(sum(sortinos) / len(sortinos)) if sortinos else None,
        win_rate_pct=sum(m.win_rate_pct for m in metrics_list) / len(metrics_list),
        trade_count=int(sum(m.trade_count for m in metrics_list)),
        rejection_reasons=reasons,
    )


def _blend_metrics(
    train: CandidateMetrics,
    holdout: CandidateMetrics,
    *,
    train_weight: float,
    holdout_weight: float,
) -> CandidateMetrics:
    """Blend train and holdout metrics with holdout emphasis."""
    sortino_values = [x for x in [train.sortino, holdout.sortino] if x is not None]
    return CandidateMetrics(
        net_return_pct=(train.net_return_pct * train_weight) + (holdout.net_return_pct * holdout_weight),
        max_drawdown_pct=(train.max_drawdown_pct * train_weight) + (holdout.max_drawdown_pct * holdout_weight),
        sharpe=(train.sharpe * train_weight) + (holdout.sharpe * holdout_weight),
        sortino=(sum(sortino_values) / len(sortino_values)) if sortino_values else None,
        win_rate_pct=(train.win_rate_pct * train_weight) + (holdout.win_rate_pct * holdout_weight),
        trade_count=int((train.trade_count * train_weight) + (holdout.trade_count * holdout_weight)),
        rejection_reasons=list(dict.fromkeys(train.rejection_reasons + holdout.rejection_reasons)),
    )


def _metrics_to_dict(m: CandidateMetrics) -> dict[str, Any]:
    """Serialize metrics for diagnostics payloads."""
    return {
        "net_return_pct": m.net_return_pct,
        "max_drawdown_pct": m.max_drawdown_pct,
        "sharpe": m.sharpe,
        "sortino": m.sortino,
        "win_rate_pct": m.win_rate_pct,
        "trade_count": m.trade_count,
        "rejection_reasons": list(m.rejection_reasons),
    }

