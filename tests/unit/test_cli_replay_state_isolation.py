from pathlib import Path

from src.cli import _configure_replay_state_isolation


def test_replay_state_isolation_sets_defaults(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("KILL_SWITCH_STATE_PATH", raising=False)
    monkeypatch.delenv("SAFETY_STATE_PATH", raising=False)
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("DRY_RUN", raising=False)
    monkeypatch.delenv("SYSTEM_DRY_RUN", raising=False)

    state_file = tmp_path / "run" / "state.json"
    applied = _configure_replay_state_isolation("replay", state_file)

    assert "KILL_SWITCH_STATE_PATH" in applied
    assert "SAFETY_STATE_PATH" in applied
    assert applied["ENV"] == "dev"
    assert applied["ENVIRONMENT"] == "dev"
    assert applied["DRY_RUN"] == "1"
    assert applied["SYSTEM_DRY_RUN"] == "1"
    assert applied["KILL_SWITCH_STATE_PATH"].endswith("kill_switch_state.replay.json")
    assert applied["SAFETY_STATE_PATH"].endswith("safety_state.replay.json")


def test_replay_state_isolation_respects_existing_env(monkeypatch, tmp_path: Path) -> None:
    ks = str((tmp_path / "custom_ks.json").resolve())
    ss = str((tmp_path / "custom_ss.json").resolve())
    monkeypatch.setenv("KILL_SWITCH_STATE_PATH", ks)
    monkeypatch.setenv("SAFETY_STATE_PATH", ss)
    monkeypatch.setenv("ENVIRONMENT", "prod")
    monkeypatch.setenv("DRY_RUN", "0")

    applied = _configure_replay_state_isolation("replay", tmp_path / "state.json")

    assert applied["ENVIRONMENT"] == "dev"
    assert applied["DRY_RUN"] == "1"
    assert "KILL_SWITCH_STATE_PATH" not in applied
    assert "SAFETY_STATE_PATH" not in applied


def test_replay_state_isolation_noop_for_non_replay(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("KILL_SWITCH_STATE_PATH", raising=False)
    monkeypatch.delenv("SAFETY_STATE_PATH", raising=False)

    applied = _configure_replay_state_isolation("backtest", tmp_path / "state.json")

    assert applied == {}
