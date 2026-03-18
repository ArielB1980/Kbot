"""Tests for replay no-progress timeout behavior."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.research.evaluator import CandidateEvaluator, EvaluationSpec


def _stub_config() -> SimpleNamespace:
    return SimpleNamespace(
        backtest=SimpleNamespace(starting_equity=Decimal("10000")),
        exchange=SimpleNamespace(
            api_key="",
            api_secret="",
            futures_api_key="",
            futures_api_secret="",
        ),
    )


class _SlowRunner:
    def __init__(self, **kwargs):  # noqa: D401, ANN003
        self.kwargs = kwargs

    async def run(self):  # noqa: D401
        await asyncio.sleep(2.0)
        return None


@pytest.mark.asyncio
async def test_replay_timeout_marks_failed_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    evaluator = CandidateEvaluator(
        base_config=_stub_config(),
        spec=EvaluationSpec(
            symbols=("BTC/USD",),
            lookback_days=2,
            starting_equity=Decimal("10000"),
            mode="replay",
            replay_eval_timeout_seconds=0,
            sleep_between_symbols_seconds=0.0,
        ),
    )
    evaluator._coverage_status["BTC/USD"] = {
        "available_start": "2026-03-10T00:00:00+00:00",
        "available_end": "2026-03-12T00:00:00+00:00",
    }
    monkeypatch.setattr("src.research.evaluator.BacktestRunner", _SlowRunner)

    start = datetime.now(timezone.utc) - timedelta(days=1)
    end = datetime.now(timezone.utc)
    agg = await evaluator._run_aggregate_replay_for_period({}, start, end)

    assert agg.metrics.net_return_pct < -900.0
    assert "replay_timeout_no_progress" in agg.metrics.rejection_reasons
