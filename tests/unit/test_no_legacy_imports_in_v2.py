import os
import subprocess
import sys
import textwrap
from pathlib import Path


def test_live_trading_module_imports_unified_position_manager():
    """Verify live_trading imports the unified PositionManager (KBO-29), not raw V2."""
    repo_root = Path(__file__).resolve().parent.parent.parent

    code = textwrap.dedent(
        """
        import os, sys
        os.environ["ENVIRONMENT"] = "prod"
        os.environ["DRY_RUN"] = "1"
        os.environ["USE_STATE_MACHINE_V2"] = "true"

        import src.live.live_trading  # noqa: F401

        # Unified PositionManager should be imported
        assert "src.execution.position_manager" in sys.modules, "unified PositionManager not imported"
        print("OK")
        """
    ).strip()

    env = dict(os.environ)
    env["PYTHONPATH"] = str(repo_root)
    env["ENVIRONMENT"] = "prod"
    env["DRY_RUN"] = "1"
    env["USE_STATE_MACHINE_V2"] = "true"

    res = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(repo_root),
        env=env,
        capture_output=True,
        text=True,
    )

    assert res.returncode == 0, f"stdout={res.stdout}\nstderr={res.stderr}"
    assert "OK" in (res.stdout or "")
