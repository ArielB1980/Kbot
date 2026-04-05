"""Unit tests for campaign-level research stop gate."""

from __future__ import annotations

import importlib.util
import sys
import json
from pathlib import Path


def _load_module():
    module_path = Path("scripts/research_campaign_gate.py")
    spec = importlib.util.spec_from_file_location("research_campaign_gate", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_decide_stops_on_meaningful_result():
    module = _load_module()
    history = [
        {
            "nonbaseline_best": 4,
            "accepted_best": 1,
            "counterfactual_uplift": 0.2,
            "completed_symbols": 40,
            "total_symbols": 40,
        }
    ]
    decision, reason = module._decide(  # noqa: SLF001
        history,
        meaningful_nonbaseline_min=3,
        meaningful_accepted_min=1,
        proof_window_cycles=6,
        allow_falsification_stop=False,
    )
    assert decision == "stop_meaningful_result"
    assert reason == "meaningful_threshold_met"


def test_decide_stops_on_flat_falsification_window():
    module = _load_module()
    history = [
        {
            "nonbaseline_best": 0,
            "accepted_best": 0,
            "counterfactual_uplift": 0.0,
            "completed_symbols": 36,
            "total_symbols": 40,
            "comparable": True,
        }
        for _ in range(6)
    ]
    decision, reason = module._decide(  # noqa: SLF001
        history,
        meaningful_nonbaseline_min=3,
        meaningful_accepted_min=1,
        proof_window_cycles=6,
        allow_falsification_stop=True,
    )
    assert decision == "stop_not_profitable_supported"
    assert reason == "falsification_window_all_flat"


def test_decide_continues_when_signal_is_mixed():
    module = _load_module()
    history = [
        {
            "nonbaseline_best": 0,
            "accepted_best": 0,
            "counterfactual_uplift": 0.0,
            "completed_symbols": 40,
            "total_symbols": 40,
            "comparable": True,
        },
        {
            "nonbaseline_best": 1,
            "accepted_best": 0,
            "counterfactual_uplift": 0.1,
            "completed_symbols": 40,
            "total_symbols": 40,
            "comparable": True,
        },
    ]
    decision, reason = module._decide(  # noqa: SLF001
        history,
        meaningful_nonbaseline_min=3,
        meaningful_accepted_min=1,
        proof_window_cycles=2,
        allow_falsification_stop=True,
    )
    assert decision == "continue"
    assert reason == "campaign_still_informative"


def test_decide_continues_when_falsification_disabled():
    module = _load_module()
    history = [
        {
            "nonbaseline_best": 0,
            "accepted_best": 0,
            "counterfactual_uplift": 0.0,
            "completed_symbols": 40,
            "total_symbols": 40,
            "comparable": True,
        }
        for _ in range(6)
    ]
    decision, reason = module._decide(  # noqa: SLF001
        history,
        meaningful_nonbaseline_min=3,
        meaningful_accepted_min=1,
        proof_window_cycles=6,
        allow_falsification_stop=False,
    )
    assert decision == "continue"
    assert reason == "falsification_stop_disabled"


def test_decide_continues_when_window_non_comparable():
    module = _load_module()
    history = [
        {
            "nonbaseline_best": 0,
            "accepted_best": 0,
            "counterfactual_uplift": 0.0,
            "completed_symbols": 40,
            "total_symbols": 40,
            "comparable": False,
        }
        for _ in range(6)
    ]
    decision, reason = module._decide(  # noqa: SLF001
        history,
        meaningful_nonbaseline_min=3,
        meaningful_accepted_min=1,
        proof_window_cycles=6,
        allow_falsification_stop=True,
    )
    assert decision == "continue"
    assert reason == "falsification_window_non_comparable"


def test_cycle_stats_falls_back_to_nested_worker_best_files(tmp_path: Path):
    module = _load_module()
    state_file = tmp_path / "state.json"
    state_file.write_text(
        json.dumps(
            {
                "completed_symbols": ["BTC/USD"],
                "total_symbols": 1,
                "symbol_best_candidates": {"BTC/USD": "BTC_USD_c001"},
            }
        ),
        encoding="utf-8",
    )
    artifacts_dir = tmp_path / "artifacts"
    nested = artifacts_dir / "w0_1_BTC_USD"
    nested.mkdir(parents=True)
    (nested / "run_best_by_symbol.json").write_text(
        json.dumps(
            {
                "best_by_symbol": {
                    "BTC/USD": {
                        "candidate_id": "BTC_USD_c001",
                        "accepted": False,
                        "metrics": {"trade_count": 3, "net_return_pct": 1.25},
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    stats = module._cycle_stats("run1", state_file=state_file, artifacts_dir=artifacts_dir)

    assert stats.nonbaseline_best == 1
    assert stats.avg_best_trade_count == 3.0
