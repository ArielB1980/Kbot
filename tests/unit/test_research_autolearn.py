"""Tests for mini-autocontext research autolearn logic."""

from __future__ import annotations

from pathlib import Path
import importlib.util
import sys


def _load_module():
    path = Path(__file__).resolve().parents[2] / "scripts" / "research_autolearn.py"
    spec = importlib.util.spec_from_file_location("research_autolearn", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def test_build_overrides_prefers_trade_active_subset() -> None:
    mod = _load_module()
    snapshot = mod.RunSnapshot(
        run_id="r1",
        phase="finished",
        completed=40,
        total_symbols=40,
        eligible=40,
        skipped=0,
        baseline_best=40,
        nonbaseline_best=0,
        skipped_insufficient_signal=0,
        skipped_uninformative_surface=0,
        replay_timeout_mentions=0,
        paused_health_mentions=0,
        fib_gate_reject_mentions=0,
        score_gate_reject_mentions=0,
        dedupe_suppressed_mentions=0,
        conviction_gate_block_mentions=0,
        thesis_reentry_block_mentions=0,
        rr_multiple_reject_mentions=0,
        opens_planned_mentions=0,
        opens_executed_mentions=0,
        top_trade_symbols=[f"S{i}/USD" for i in range(12)],
    )

    overrides = mod._build_overrides(snapshot)

    assert overrides["SYMBOLS_FROM_LIVE_UNIVERSE"] == "0"
    assert "SYMBOLS" in overrides
    assert overrides["MAX_ITERS_PER_SYMBOL"] == "160"
    assert overrides["MAX_STAGNANT_ITERS"] == "24"

