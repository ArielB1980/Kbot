import asyncio

from src.research.state_store import ResearchStateStore
from src.research.telegram_router import ResearchTelegramRouter


def test_state_store_prompt_roundtrip(tmp_path) -> None:
    store = ResearchStateStore(tmp_path / "state.json")
    prompt = store.create_prompt(prompt_type="tradeoff", message="Approve?")
    state = store.read_state()
    assert state["pending_prompt"]["token"] == prompt.token
    assert store.resolve_prompt(prompt.token, "approve") is True
    state = store.read_state()
    assert state["pending_prompt"]["resolved"] is True
    assert state["pending_prompt"]["resolution"] == "approve"


def test_router_commands_pause_resume_and_status(tmp_path) -> None:
    store = ResearchStateStore(tmp_path / "state.json")
    store.update(run_id="run1", phase="running", total_iterations=10, iteration=2, best_candidate_id="c001")
    router = ResearchTelegramRouter(store)

    status = asyncio.run(router.handle_command("/research_status"))
    assert "Research Status" in status
    assert "2/10" in status

    pause = asyncio.run(router.handle_command("/research_pause"))
    assert "paused" in pause.lower()
    assert store.read_state()["control"]["paused"] is True

    resume = asyncio.run(router.handle_command("/research_resume"))
    assert "resumed" in resume.lower()
    assert store.read_state()["control"]["paused"] is False


def test_router_diff_and_promotion(tmp_path) -> None:
    store = ResearchStateStore(tmp_path / "state.json")
    store.update(
        leaderboard=[
            {
                "candidate_id": "baseline",
                "score": 1.0,
                "params": {"strategy.adx_threshold": 25.0},
                "metrics": {"net_return_pct": 1.0, "max_drawdown_pct": 5.0, "sharpe": 0.5},
            },
            {
                "candidate_id": "c007",
                "score": 1.2,
                "params": {"strategy.adx_threshold": 24.0},
                "metrics": {"net_return_pct": 1.4, "max_drawdown_pct": 5.6, "sharpe": 0.6},
            },
        ]
    )
    router = ResearchTelegramRouter(store)
    diff = asyncio.run(router.handle_command("/research_diff c007"))
    assert "Diff c007 vs baseline" in diff
    promote = asyncio.run(router.handle_command("/research_promote c007"))
    assert "Queued" in promote
    assert "c007" in store.read_state()["promotion_queue"]

