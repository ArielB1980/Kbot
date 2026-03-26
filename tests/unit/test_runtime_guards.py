from __future__ import annotations

import pytest

from src.runtime.guards import assert_prod_live_prereqs


def _set_base_prod_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENVIRONMENT", "prod")
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("CONFIRM_LIVE", "YES")
    monkeypatch.setenv("USE_STATE_MACHINE_V2", "true")


def test_prod_live_prereqs_rejects_replay_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_base_prod_env(monkeypatch)
    monkeypatch.setenv("REPLAY_ABLATE_DISABLE_WEEKLY_ZONE", "1")

    with pytest.raises(RuntimeError, match="REPLAY_\\* environment variables are not allowed"):
        assert_prod_live_prereqs()


def test_prod_live_prereqs_accepts_clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_base_prod_env(monkeypatch)
    monkeypatch.delenv("REPLAY_ABLATE_DISABLE_WEEKLY_ZONE", raising=False)
    monkeypatch.delenv("REPLAY_OVERRIDE_ADX_THRESHOLD", raising=False)

    assert_prod_live_prereqs()


def test_non_prod_allows_replay_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("REPLAY_ABLATE_DISABLE_WEEKLY_ZONE", "1")

    assert_prod_live_prereqs()
