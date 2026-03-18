"""Unit tests for research filter blocker report."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_module():
    module_path = Path("scripts/research_filter_report.py")
    spec = importlib.util.spec_from_file_location("research_filter_report", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_build_report_ranks_known_blockers(tmp_path: Path) -> None:
    module = _load_module()
    state_file = tmp_path / "state.json"
    run_log = tmp_path / "research.log"

    state_file.write_text(
        json.dumps(
            {
                "phase": "finished",
                "completed_symbols": ["BTC/USD", "ETH/USD"],
                "total_symbols": 2,
                "current_symbol": "ETH/USD",
                "symbol_progress": {
                    "BTC/USD": {"phase": "skipped_non_informative_baseline"},
                    "ETH/USD": {"phase": "skipped_non_informative_baseline"},
                },
            }
        ),
        encoding="utf-8",
    )
    run_log.write_text(
        "\n".join(
            [
                "reason=4H_STRUCTURE_REQUIRED",
                "reason=4H_STRUCTURE_REQUIRED",
                "reason=outside_weekly_zone",
                "Signal rejected: No decision structure reason=4H_STRUCTURE_REQUIRED",
            ]
        ),
        encoding="utf-8",
    )

    report = module.build_report(run_id="r1", state_path=state_file, run_log_path=run_log)
    assert report["run_id"] == "r1"
    assert report["state_summary"]["completed_symbols"] == 2
    assert report["ranked_blockers"][0]["blocker"] == "decision_structure_required"
    assert report["ranked_blockers"][0]["count"] == 3
    assert report["ranked_blockers"][1]["blocker"] == "outside_weekly_zone"
    assert report["ranked_blockers"][1]["count"] == 1
    assert report["state_summary"]["symbol_phase_counts"]["skipped_non_informative_baseline"] == 2
