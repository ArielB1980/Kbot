"""Tests for replay eligibility tiers in candidate evaluator."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

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


def _coverage_payload(*, complete: bool, one_min_ratio: float, one_hour_ratio: float) -> dict:
    return {
        "symbol": "BTC/USD",
        "complete": complete,
        "timeframes": {
            "1m": {"coverage_ratio": one_min_ratio, "ok": one_min_ratio >= 0.95},
            "1h": {"coverage_ratio": one_hour_ratio, "ok": one_hour_ratio >= 0.95},
        },
        "available_start": "2026-03-10T00:00:00+00:00",
        "available_end": "2026-03-12T00:00:00+00:00",
    }


@pytest.mark.asyncio
async def test_assess_symbol_eligibility_allows_partial_with_available_window() -> None:
    evaluator = CandidateEvaluator(
        base_config=_stub_config(),
        spec=EvaluationSpec(
            symbols=("BTC/USD",),
            lookback_days=2,
            starting_equity=Decimal("10000"),
            mode="replay",
            min_partial_coverage_ratio=0.60,
        ),
    )
    evaluator.prepare_symbol_data = AsyncMock(  # type: ignore[method-assign]
        return_value=_coverage_payload(complete=False, one_min_ratio=0.75, one_hour_ratio=0.80)
    )
    evaluator._has_futures_ticker = AsyncMock(return_value=True)  # type: ignore[method-assign]

    status = await evaluator.assess_symbol_eligibility("BTC/USD")

    assert status["eligible"] is True
    assert status["eligibility_tier"] == "partial"
    assert status["comparability_score"] >= 0.60
    assert "partial_data_non_comparable" not in status["reasons"]


@pytest.mark.asyncio
async def test_assess_symbol_eligibility_rejects_low_comparability_partial() -> None:
    evaluator = CandidateEvaluator(
        base_config=_stub_config(),
        spec=EvaluationSpec(
            symbols=("BTC/USD",),
            lookback_days=2,
            starting_equity=Decimal("10000"),
            mode="replay",
            min_partial_coverage_ratio=0.60,
        ),
    )
    payload = _coverage_payload(complete=False, one_min_ratio=0.20, one_hour_ratio=0.30)
    payload["available_start"] = None
    payload["available_end"] = None
    evaluator.prepare_symbol_data = AsyncMock(return_value=payload)  # type: ignore[method-assign]
    evaluator._has_futures_ticker = AsyncMock(return_value=True)  # type: ignore[method-assign]

    status = await evaluator.assess_symbol_eligibility("BTC/USD")

    assert status["eligible"] is False
    assert status["eligibility_tier"] == "ineligible"
    assert status["has_available_window"] is False
    assert "partial_data_non_comparable" in status["reasons"]
