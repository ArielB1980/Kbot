"""Unit tests for campaign-level research stop gate."""

from __future__ import annotations

import importlib.util
import sys
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

