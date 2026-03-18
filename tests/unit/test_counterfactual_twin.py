from __future__ import annotations

from datetime import datetime, timezone

from src.config.config import load_config
from src.research.counterfactual_twin import (
    TapeDecision,
    evaluate_candidate_batch,
    evaluate_counterfactual_uplift,
    load_decision_tape,
)


def test_evaluate_counterfactual_uplift_changes_open_count() -> None:
    config = load_config("src/config/config.yaml")
    tape = [
        TapeDecision(
            decision_id="d1",
            symbol="BTC/USD",
            timestamp=datetime.now(timezone.utc),
            signal="LONG",
            regime="tight_smc",
            bias="bullish",
            setup_quality=66.0,
            is_tradable=True,
            order_placed=False,
        ),
        TapeDecision(
            decision_id="d2",
            symbol="BTC/USD",
            timestamp=datetime.now(timezone.utc),
            signal="LONG",
            regime="tight_smc",
            bias="bullish",
            setup_quality=72.0,
            is_tradable=True,
            order_placed=True,
        ),
    ]

    report = evaluate_counterfactual_uplift(
        base_config=config,
        candidate_params={"strategy.min_score_tight_smc_aligned": 60.0},
        tape=tape,
    )

    assert report["candidate_open_count"] >= report["baseline_open_count"]
    assert report["delta_open_count"] >= 0


def test_load_decision_tape_stitches_action(monkeypatch) -> None:
    now = datetime.now(timezone.utc)

    def _fake_events(*args, **kwargs):  # noqa: ANN002,ANN003
        return [
            {
                "id": 1,
                "timestamp": now,
                "event_type": "COUNTERFACTUAL_DECISION",
                "symbol": "ETH/USD",
                "decision_id": "abc",
                "details": {"signal": "LONG", "regime": "tight_smc", "bias": "bullish", "setup_quality": 70.0},
            },
            {
                "id": 2,
                "timestamp": now,
                "event_type": "COUNTERFACTUAL_ACTION",
                "symbol": "ETH/USD",
                "decision_id": "abc",
                "details": {"order_placed": True},
            },
        ]

    monkeypatch.setattr("src.research.counterfactual_twin.get_system_events_since", _fake_events)
    tape = load_decision_tape(since_hours=24, symbols=("ETH/USD",))
    assert len(tape) == 1
    assert tape[0].decision_id == "abc"
    assert tape[0].order_placed is True


def test_evaluate_candidate_batch_sorts_highest_uplift_first() -> None:
    config = load_config("src/config/config.yaml")
    tape = [
        TapeDecision(
            decision_id="x1",
            symbol="BTC/USD",
            timestamp=datetime.now(timezone.utc),
            signal="LONG",
            regime="tight_smc",
            bias="bullish",
            setup_quality=62.0,
            is_tradable=True,
            order_placed=False,
        ),
        TapeDecision(
            decision_id="x2",
            symbol="BTC/USD",
            timestamp=datetime.now(timezone.utc),
            signal="LONG",
            regime="tight_smc",
            bias="bullish",
            setup_quality=68.0,
            is_tradable=True,
            order_placed=False,
        ),
    ]
    ranking = evaluate_candidate_batch(
        base_config=config,
        tape=tape,
        candidates={
            "strict": {"strategy.min_score_tight_smc_aligned": 80.0},
            "loose": {"strategy.min_score_tight_smc_aligned": 50.0},
        },
    )
    assert ranking[0]["candidate_id"] == "loose"
    assert ranking[0]["utility_uplift"] >= ranking[1]["utility_uplift"]
