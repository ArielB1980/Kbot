"""Sandbox autoresearch experiment harness."""

from __future__ import annotations

import asyncio
import random
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from src.config.config import Config
from src.monitoring.logger import get_logger
from src.monitoring.telegram_bot import send_telegram_message
from src.research.allowlist import AllowlistPolicy
from src.research.evaluator import CandidateEvaluator, EvaluationSpec
from src.research.kpi import score_candidate
from src.research.models import CandidateResult
from src.research.reporting import write_leaderboard, write_summary
from src.research.state_store import ResearchStateStore

logger = get_logger(__name__)


@dataclass(frozen=True)
class HarnessConfig:
    """Runtime config for sandbox autoresearch loop."""

    iterations: int = 12
    digest_every: int = 5
    out_dir: str = "data/research"
    decision_timeout_seconds: int = 90
    evaluation_mode: str = "backtest"
    lookback_days: int = 30
    symbols: tuple[str, ...] = ("BTC/USD", "ETH/USD", "SOL/USD")
    enable_telegram: bool = True


class SandboxAutoresearchHarness:
    """Main generate-evaluate-rank loop with Telegram interaction hooks."""

    def __init__(
        self,
        base_config: Config,
        harness_config: HarnessConfig,
        state_store: ResearchStateStore,
        allowlist_policy: AllowlistPolicy | None = None,
        rng: random.Random | None = None,
    ):
        self.base_config = base_config
        self.cfg = harness_config
        self.store = state_store
        self.policy = allowlist_policy or AllowlistPolicy()
        self.rng = rng or random.Random()
        self.run_id = datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")
        self.results: list[CandidateResult] = []

        self._evaluator = CandidateEvaluator(
            base_config=base_config,
            spec=EvaluationSpec(
                symbols=self.cfg.symbols,
                lookback_days=self.cfg.lookback_days,
                starting_equity=base_config.backtest.starting_equity,
                mode=self.cfg.evaluation_mode,
            ),
        )
        self._param_keys = tuple(self.policy.allowed_parameter_paths)
        self._baseline_params = self._read_params_from_config(base_config)

    async def run(self) -> tuple[Path, Path]:
        """Run the full sandbox autoresearch loop."""
        self.store.update(
            run_id=self.run_id,
            phase="running",
            iteration=0,
            total_iterations=self.cfg.iterations,
            best_candidate_id=None,
            leaderboard=[],
            last_error=None,
            pending_prompt=None,
            promotion_queue=[],
        )
        await self._notify(f"🔬 Research run started: <code>{self.run_id}</code>")

        baseline = await self._evaluate_candidate("baseline", self._baseline_params)
        self.results.append(baseline)
        best = baseline
        self._persist_leaderboard(best.candidate_id)
        await self._notify(self._format_milestone(best, is_baseline=True))

        for idx in range(1, self.cfg.iterations + 1):
            if self.store.read_state().get("control", {}).get("stop_requested"):
                self.store.update(phase="stopped")
                await self._notify("🛑 Research stopped by operator.")
                break

            await self._wait_while_paused()
            candidate_id = f"c{idx:03d}"
            params = self._mutate_params(best.params)
            result = await self._evaluate_candidate(candidate_id, params)
            if result.metrics.net_return_pct <= baseline.metrics.net_return_pct:
                if "Does not beat baseline return" not in result.metrics.rejection_reasons:
                    result.metrics.rejection_reasons.append("Does not beat baseline return")
                result.accepted = False
                result.score -= 100.0
            if result.accepted:
                result.metadata["required_verification_commands"] = self._required_verification_commands()
            self.results.append(result)

            if result.score > best.score:
                if await self._requires_operator_decision(best, result):
                    approved = await self._request_decision(
                        prompt_type="tradeoff",
                        prompt_message=(
                            f"Candidate <code>{result.candidate_id}</code> improves return "
                            f"{result.metrics.net_return_pct:+.2f}% but worsens drawdown to "
                            f"{result.metrics.max_drawdown_pct:.2f}%. "
                            "Approve for ranking? Reply with /approve &lt;token&gt; or /reject &lt;token&gt;."
                        ),
                    )
                    if approved:
                        best = result
                        await self._notify(self._format_milestone(best))
                else:
                    best = result
                    await self._notify(self._format_milestone(best))

            self.store.update(iteration=idx)
            self._persist_leaderboard(best.candidate_id)
            if idx % max(1, self.cfg.digest_every) == 0:
                await self._notify(self._format_digest(idx, best))

        phase = self.store.read_state().get("phase")
        if phase not in {"stopped", "failed"}:
            self.store.update(phase="finished")
        leaderboard_path, summary_path = self._write_reports(best)
        await self._notify(
            (
                "✅ Research run completed\n"
                f"Run: <code>{self.run_id}</code>\n"
                f"Best: <code>{best.candidate_id}</code>\n"
                f"Return: {best.metrics.net_return_pct:+.2f}% | "
                f"MaxDD: {best.metrics.max_drawdown_pct:.2f}% | "
                f"Sharpe: {best.metrics.sharpe:.2f}"
            )
        )
        return leaderboard_path, summary_path

    async def _evaluate_candidate(self, candidate_id: str, params: dict[str, float]) -> CandidateResult:
        violations = self.policy.validate_candidate_keys(params.keys())
        if violations:
            metrics = await self._evaluator.evaluate(self._baseline_params)
            metrics.rejection_reasons.extend(violations)
            return CandidateResult(
                candidate_id=candidate_id,
                params=params,
                metrics=metrics,
                score=-10_000.0,
                accepted=False,
                metadata={"policy_violations": violations},
            )

        metrics = await self._evaluator.evaluate(params)
        accepted, reasons = self._promotion_gate(metrics)
        metrics.rejection_reasons.extend(reasons)
        score = score_candidate(metrics) if accepted else score_candidate(metrics) - 250.0
        return CandidateResult(
            candidate_id=candidate_id,
            params=params,
            metrics=metrics,
            score=score,
            accepted=accepted,
        )

    def _promotion_gate(self, metrics) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        if metrics.trade_count < 3:
            reasons.append("Too few trades for confidence")
        if metrics.max_drawdown_pct > 35.0:
            reasons.append("Max drawdown exceeds hard cap (35%)")
        if metrics.net_return_pct <= -10.0:
            reasons.append("Net return is deeply negative")
        return (len(reasons) == 0, reasons)

    def _read_params_from_config(self, config: Config) -> dict[str, float]:
        out: dict[str, float] = {}
        for key in self._param_keys:
            section_name, _, attr = key.partition(".")
            section = getattr(config, section_name)
            out[key] = float(getattr(section, attr))
        return out

    def _mutate_params(self, source: dict[str, float]) -> dict[str, float]:
        candidate = dict(source)
        keys_to_mutate = self.rng.sample(self._param_keys, k=min(3, len(self._param_keys)))
        for key in keys_to_mutate:
            value = candidate[key]
            step = max(0.001, abs(value) * 0.10)
            delta = self.rng.uniform(-step, step)
            candidate[key] = round(value + delta, 6)
        return candidate

    async def _requires_operator_decision(self, baseline: CandidateResult, candidate: CandidateResult) -> bool:
        return (
            candidate.metrics.net_return_pct > baseline.metrics.net_return_pct
            and candidate.metrics.max_drawdown_pct > baseline.metrics.max_drawdown_pct + 1.0
        )

    async def _request_decision(self, prompt_type: str, prompt_message: str) -> bool:
        prompt = self.store.create_prompt(
            prompt_type=prompt_type,
            message=prompt_message,
            ttl_seconds=self.cfg.decision_timeout_seconds,
        )
        await self._notify(
            f"❓ {prompt_message}\nToken: <code>{prompt.token}</code>\n"
            f"Expires: {prompt.expires_at}"
        )
        deadline = datetime.fromisoformat(prompt.expires_at)
        while datetime.now(timezone.utc) < deadline:
            state = self.store.read_state()
            pending = state.get("pending_prompt") or {}
            if pending.get("resolved"):
                resolution = pending.get("resolution")
                return resolution == "approve"
            await asyncio.sleep(2)
        self.store.resolve_prompt(prompt.token, "timeout")
        return False

    def _persist_leaderboard(self, best_candidate_id: str) -> None:
        board = [
            {
                "candidate_id": r.candidate_id,
                "score": r.score,
                "accepted": r.accepted,
                "promoted": r.promoted,
                "params": r.params,
                "metrics": asdict(r.metrics),
            }
            for r in sorted(self.results, key=lambda x: x.score, reverse=True)
        ]
        self.store.update(leaderboard=board, best_candidate_id=best_candidate_id)

    def _write_reports(self, best: CandidateResult) -> tuple[Path, Path]:
        base = Path(self.cfg.out_dir)
        leaderboard_path = write_leaderboard(
            path=base / f"{self.run_id}_leaderboard.json",
            run_id=self.run_id,
            baseline_id="baseline",
            results=self.results,
        )
        summary_path = write_summary(
            path=base / f"{self.run_id}_summary.md",
            run_id=self.run_id,
            baseline=self.results[0],
            best=best,
        )
        return leaderboard_path, summary_path

    async def _wait_while_paused(self) -> None:
        while self.store.read_state().get("control", {}).get("paused"):
            self.store.update(phase="paused")
            await asyncio.sleep(2)
        self.store.update(phase="running")

    async def _notify(self, message: str) -> None:
        if self.cfg.enable_telegram:
            await send_telegram_message(message)
        logger.info("Research notification", message=message)

    def _format_digest(self, iteration: int, best: CandidateResult) -> str:
        rejected = sum(1 for r in self.results if r.metrics.rejection_reasons)
        return (
            f"📦 <b>Research Digest</b> {iteration}/{self.cfg.iterations}\n"
            f"Best: <code>{best.candidate_id}</code> score={best.score:.2f}\n"
            f"Return={best.metrics.net_return_pct:+.2f}% "
            f"MaxDD={best.metrics.max_drawdown_pct:.2f}% "
            f"Sharpe={best.metrics.sharpe:.2f}\n"
            f"Rejected candidates: {rejected}"
        )

    def _format_milestone(self, best: CandidateResult, *, is_baseline: bool = False) -> str:
        title = "Baseline established" if is_baseline else "New best candidate"
        return (
            f"🏁 <b>{title}</b>\n"
            f"Candidate: <code>{best.candidate_id}</code>\n"
            f"Return={best.metrics.net_return_pct:+.2f}% "
            f"MaxDD={best.metrics.max_drawdown_pct:.2f}% "
            f"Sharpe={best.metrics.sharpe:.2f} "
            f"Win={best.metrics.win_rate_pct:.1f}% "
            f"Trades={best.metrics.trade_count}"
        )

    def _required_verification_commands(self) -> list[str]:
        """Repository verification checklist before promoting a candidate."""
        return [
            "pytest tests/unit/test_config_load.py -v",
            "pytest tests/unit/test_runner_mode.py -v",
            "make smoke",
            "make replay SEED=42",
        ]


async def run_sandbox_autoresearch(
    base_config: Config,
    harness_config: HarnessConfig,
    state_store: ResearchStateStore,
    *,
    on_complete: Callable[[Path, Path], None] | None = None,
) -> tuple[Path, Path]:
    """Convenience helper to run the harness."""
    harness = SandboxAutoresearchHarness(
        base_config=base_config,
        harness_config=harness_config,
        state_store=state_store,
    )
    outputs = await harness.run()
    if on_complete:
        on_complete(outputs[0], outputs[1])
    return outputs

