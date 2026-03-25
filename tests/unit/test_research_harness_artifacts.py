import json
from types import SimpleNamespace

from src.research.harness import HarnessConfig, SandboxAutoresearchHarness
from src.research.models import CandidateMetrics, CandidateResult
from src.research.state_store import ResearchStateStore


def _base_config():
    strategy = SimpleNamespace(
        adx_threshold=20.0,
        fvg_min_size_pct=0.0007,
        entry_zone_tolerance_pct=0.05,
        entry_zone_tolerance_atr_mult=0.3,
        min_score_tight_smc_aligned=55.0,
        min_score_wide_structure_aligned=55.0,
        signal_cooldown_hours=1.0,
        tight_smc_atr_stop_min=0.15,
        tight_smc_atr_stop_max=0.30,
        wide_structure_atr_stop_min=0.5,
        wide_structure_atr_stop_max=0.6,
        ema_slope_bonus=10.0,
        bos_volume_threshold_mult=1.5,
        fib_proximity_adaptive_scale=0.5,
        fib_proximity_max_bps=80.0,
    )
    return SimpleNamespace(
        strategy=strategy,
        backtest=SimpleNamespace(starting_equity=10_000.0),
    )


def test_persist_leaderboard_writes_incremental_artifacts(tmp_path):
    out_dir = tmp_path / "artifacts"
    store = ResearchStateStore(tmp_path / "state.json")
    harness = SandboxAutoresearchHarness(
        base_config=_base_config(),
        harness_config=HarnessConfig(out_dir=str(out_dir), enable_telegram=False),
        state_store=store,
    )
    harness.run_id = "run_test"

    baseline = CandidateResult(
        candidate_id="baseline",
        symbol="BTC/USD",
        params={"strategy.adx_threshold": 20.0},
        metrics=CandidateMetrics(
            net_return_pct=0.1,
            max_drawdown_pct=1.0,
            sharpe=0.5,
            sortino=0.7,
            win_rate_pct=55.0,
            trade_count=12,
        ),
        score=0.1,
        accepted=True,
    )
    best = CandidateResult(
        candidate_id="c001",
        symbol="BTC/USD",
        params={"strategy.adx_threshold": 18.0},
        metrics=CandidateMetrics(
            net_return_pct=0.4,
            max_drawdown_pct=1.2,
            sharpe=0.8,
            sortino=1.1,
            win_rate_pct=58.0,
            trade_count=14,
        ),
        score=0.4,
        accepted=True,
    )

    harness.results = [baseline, best]
    harness.best_by_symbol = {"BTC/USD": best}

    harness._persist_leaderboard("c001")

    leaderboard_path = out_dir / "run_test_leaderboard.json"
    summary_path = out_dir / "run_test_summary.md"
    best_by_symbol_path = out_dir / "run_test_best_by_symbol.json"

    assert leaderboard_path.exists()
    assert summary_path.exists()
    assert best_by_symbol_path.exists()

    leaderboard = json.loads(leaderboard_path.read_text(encoding="utf-8"))
    assert leaderboard["run_id"] == "run_test"
    assert [row["candidate_id"] for row in leaderboard["candidates"]] == ["baseline", "c001"]

    best_by_symbol = json.loads(best_by_symbol_path.read_text(encoding="utf-8"))
    assert best_by_symbol["best_by_symbol"]["BTC/USD"]["candidate_id"] == "c001"

    state = store.read_state()
    assert state["best_candidate_id"] == "c001"
    assert [row["candidate_id"] for row in state["leaderboard"]] == ["c001", "baseline"]
