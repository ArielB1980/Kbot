"""
Run replay harness over multiple seeds and publish summary to Telegram.

This is a publication gate utility: it does not auto-deploy or change runtime.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List


@dataclass
class SeedResult:
    seed: int
    passed: bool
    summary_line: str


def _run_seed(seed: int, data_dir: str, output_dir: Path) -> SeedResult:
    out = output_dir / f"seed_{seed}"
    out.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "src.backtest.replay_harness.run_episodes",
        "--data-dir",
        data_dir,
        "--output",
        str(out),
        "--seed",
        str(seed),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    stdout = proc.stdout or ""
    summary = ""
    for line in stdout.splitlines():
        if "REPLAY SUITE SUMMARY" in line or "/7 episodes passed" in line:
            summary = line.strip()
    return SeedResult(
        seed=seed,
        passed=proc.returncode == 0,
        summary_line=summary or f"seed={seed} returncode={proc.returncode}",
    )


def _publish(results: List[SeedResult], output_dir: Path) -> None:
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    best = [r.seed for r in results if r.passed][:3]
    worst = [r.seed for r in results if not r.passed][:3]
    median_seed = sorted([r.seed for r in results])[len(results) // 2] if results else None
    artifact_dir = output_dir.resolve()
    message = (
        "Replay seed sweep completed\n"
        f"passed={passed}/{total}\n"
        f"best_seeds={best}\n"
        f"worst_seeds={worst}\n"
        f"median_seed={median_seed}\n"
        f"artifact_dir={artifact_dir}\n"
        "equity_curve_artifact=TODO(export from report pipeline)\n"
    )
    try:
        from src.monitoring.alerting import send_alert_sync

        send_alert_sync("REPLAY_SEED_SWEEP", message, urgent=True)
    except Exception:
        print(message)


def main() -> int:
    p = argparse.ArgumentParser(description="Run replay seed sweep and publish summary to Telegram")
    p.add_argument("--data-dir", default="data/replay")
    p.add_argument("--output-dir", default="results/replay_seed_sweep")
    p.add_argument("--seeds", default="20250227,20250228,20250229,20250230,20250231,20250232")
    args = p.parse_args()

    seeds = [int(s.strip()) for s in args.seeds.split(",") if s.strip()]
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    results: List[SeedResult] = []
    for seed in seeds:
        result = _run_seed(seed, args.data_dir, output_dir)
        results.append(result)
        print(f"seed={seed} passed={result.passed} {result.summary_line}")

    _publish(results, output_dir)
    return 0 if all(r.passed for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
