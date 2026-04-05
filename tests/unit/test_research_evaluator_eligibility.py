"""Tests for replay eligibility tiers in candidate evaluator."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.research.evaluator import (
    CandidateEvaluator,
    EvaluationSpec,
    _select_available_window_from_timeframes,
)


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


def test_select_available_window_ignores_provider_capped_timeframe_when_deeper_data_exists() -> None:
    start, end = _select_available_window_from_timeframes(
        {
            "1m": {
                "coverage_ratio": 1.0,
                "provider_cap": 720,
                "first_ts": "2026-03-10T00:00:00+00:00",
                "last_ts": "2026-03-12T00:00:00+00:00",
            },
            "15m": {
                "coverage_ratio": 0.95,
                "provider_cap": None,
                "first_ts": "2025-10-01T00:00:00+00:00",
                "last_ts": "2026-04-01T00:00:00+00:00",
            },
            "1h": {
                "coverage_ratio": 0.95,
                "provider_cap": None,
                "first_ts": "2025-12-01T00:00:00+00:00",
                "last_ts": "2026-04-01T00:00:00+00:00",
            },
        },
        min_coverage_ratio=0.60,
    )

    assert start is not None and end is not None
    assert start.isoformat() == "2025-12-01T00:00:00+00:00"
    assert end.isoformat() == "2026-04-01T00:00:00+00:00"


def test_select_available_window_falls_back_to_provider_capped_timeframe_when_needed() -> None:
    start, end = _select_available_window_from_timeframes(
        {
            "1m": {
                "coverage_ratio": 1.0,
                "provider_cap": 720,
                "first_ts": "2026-03-10T00:00:00+00:00",
                "last_ts": "2026-03-12T00:00:00+00:00",
            }
        },
        min_coverage_ratio=0.60,
    )

    assert start is not None and end is not None
    assert start.isoformat() == "2026-03-10T00:00:00+00:00"
    assert end.isoformat() == "2026-03-12T00:00:00+00:00"


@pytest.mark.asyncio
async def test_backfill_symbol_prefers_coinapi_for_lower_timeframes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evaluator = CandidateEvaluator(
        base_config=_stub_config(),
        spec=EvaluationSpec(
            symbols=("BTC/USD",),
            lookback_days=2,
            starting_equity=Decimal("10000"),
            mode="replay",
        ),
    )

    calls: list[str] = []

    class _FakeKrakenClient:
        def __init__(self, *args, **kwargs):  # noqa: D401, ANN003
            pass

        async def initialize(self) -> None:
            return None

        async def close(self) -> None:
            return None

    class _FakeCoinAPIClient:
        def __init__(self, *args, **kwargs):  # noqa: D401, ANN003
            pass

        async def close(self) -> None:
            return None

    class _FakeDataAcquisition:
        def __init__(self, *args, **kwargs):  # noqa: D401, ANN003
            pass

        async def fetch_spot_historical(self, *, source: str, **kwargs):  # noqa: D401, ANN003
            calls.append(source)
            if source == "coinapi":
                return []
            raise AssertionError("Kraken fallback should not be used when CoinAPI succeeds")

    monkeypatch.setenv("COINAPI_API_KEY", "test-key")
    monkeypatch.setattr("src.research.evaluator.KrakenClient", _FakeKrakenClient)
    monkeypatch.setattr("src.research.evaluator.CoinAPIClient", _FakeCoinAPIClient)
    monkeypatch.setattr("src.research.evaluator.DataAcquisition", _FakeDataAcquisition)

    await evaluator._backfill_symbol(
        "BTC/USD",
        "15m",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 1, 2, tzinfo=timezone.utc),
    )

    assert calls == ["coinapi"]
