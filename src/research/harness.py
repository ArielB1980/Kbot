"""Sandbox autoresearch experiment harness."""

from __future__ import annotations

import asyncio
import json
import os
import random
import shlex
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from src.config.config import Config
from src.monitoring.logger import get_logger
from src.monitoring.telegram_bot import send_telegram_message
from src.research.allowlist import PARAMETER_BOUNDS, AllowlistPolicy
from src.research.counterfactual_twin import evaluate_counterfactual_uplift, load_decision_tape
from src.research.evaluator import CandidateEvaluator, EvaluationSpec
from src.research.kpi import score_candidate
from src.research.models import CandidateResult
from src.research.reporting import write_leaderboard, write_per_symbol_best, write_summary
from src.research.state_store import ResearchStateStore

logger = get_logger(__name__)

_WARM_START_DIR = Path("data/research/warm_start")


class ParameterMemory:
    """Tracks per-parameter score deltas to guide future mutations.

    Instead of blind random search, this records which parameters moved score
    up or down and by how much.  Future mutations are biased toward parameters
    that historically produced improvements, and step sizes adapt over time.
    """

    def __init__(self, param_keys: tuple[str, ...]):
        self._keys = param_keys
        # Cumulative score improvement attributed to each param
        self._momentum: dict[str, float] = {k: 0.0 for k in param_keys}
        # Last direction that improved score (+1 or -1), 0 = unknown
        self._direction: dict[str, float] = {k: 0.0 for k in param_keys}
        # Count of times each param was part of an improving candidate
        self._win_count: dict[str, int] = {k: 0 for k in param_keys}
        # Total times each param was mutated
        self._try_count: dict[str, int] = {k: 0 for k in param_keys}

    def record(
        self,
        source_params: dict[str, float],
        candidate_params: dict[str, float],
        score_delta: float,
    ) -> None:
        """Record the outcome of a candidate evaluation."""
        for key in self._keys:
            s_val = source_params.get(key, 0.0)
            c_val = candidate_params.get(key, 0.0)
            if abs(c_val - s_val) < 1e-9:
                continue
            self._try_count[key] = self._try_count.get(key, 0) + 1
            if score_delta > 0:
                self._momentum[key] = self._momentum.get(key, 0.0) + score_delta
                self._win_count[key] = self._win_count.get(key, 0) + 1
                self._direction[key] = 1.0 if c_val > s_val else -1.0

    def pick_params_to_mutate(
        self, rng: random.Random, count: int
    ) -> list[str]:
        """Select parameters to mutate, biased toward historically productive ones.

        Uses a softmax weighting: params that have produced improvements are
        more likely to be picked, but all params retain a base probability to
        ensure exploration.
        """
        weights: list[float] = []
        for key in self._keys:
            tries = self._try_count.get(key, 0)
            wins = self._win_count.get(key, 0)
            # Base weight ensures every param gets explored
            base = 1.0
            # Bonus proportional to win rate (if tried enough)
            if tries >= 3:
                win_rate = wins / tries
                base += win_rate * 4.0
            # Small bonus from cumulative momentum
            base += min(self._momentum.get(key, 0.0) * 0.1, 3.0)
            weights.append(max(base, 0.1))

        keys_list = list(self._keys)
        count = min(count, len(keys_list))
        selected: list[str] = []
        remaining_keys = list(range(len(keys_list)))
        remaining_weights = list(weights)
        for _ in range(count):
            if not remaining_keys:
                break
            chosen = rng.choices(remaining_keys, weights=remaining_weights, k=1)[0]
            idx = remaining_keys.index(chosen)
            selected.append(keys_list[chosen])
            remaining_keys.pop(idx)
            remaining_weights.pop(idx)
        return selected

    def get_direction_bias(self, key: str) -> float:
        """Return directional bias for a parameter (-1, 0, or +1)."""
        return self._direction.get(key, 0.0)


def _warm_start_path(symbol: str) -> Path:
    """Path to the warm-start JSON for a given symbol."""
    safe = symbol.replace("/", "_").replace(":", "_")
    return _WARM_START_DIR / f"{safe}_best.json"


def load_warm_start(symbol: str) -> dict[str, float] | None:
    """Load best-known params for a symbol from disk, or None."""
    path = _warm_start_path(symbol)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        params = data.get("params")
        if isinstance(params, dict) and params:
            logger.info("WARM_START_LOADED", symbol=symbol, path=str(path))
            return {k: float(v) for k, v in params.items()}
    except Exception as exc:
        logger.warning("WARM_START_LOAD_FAILED", symbol=symbol, error=str(exc))
    return None


def save_warm_start(
    symbol: str, params: dict[str, float], score: float, candidate_id: str
) -> None:
    """Persist best-known params for a symbol to disk for cross-run memory."""
    path = _warm_start_path(symbol)
    data = {
        "symbol": symbol,
        "params": params,
        "score": score,
        "candidate_id": candidate_id,
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
        tmp.replace(path)
        logger.info("WARM_START_SAVED", symbol=symbol, score=score, path=str(path))
    except Exception as exc:
        logger.warning(
            "WARM_START_SAVE_FAILED",
            symbol=symbol,
            path=str(path),
            error=str(exc),
            error_type=type(exc).__name__,
        )


@dataclass(frozen=True)
class HarnessConfig:
    """Runtime config for sandbox autoresearch loop."""

    iterations: int = 12
    digest_every: int = 5
    out_dir: str = "data/research"
    decision_timeout_seconds: int = 90
    evaluation_mode: str = "backtest"
    lookback_days: int = 365
    symbols: tuple[str, ...] = ("BTC/USD", "ETH/USD", "SOL/USD")
    enable_telegram: bool = True
    evaluation_window_offsets_days: tuple[int, ...] = (96, 0)
    holdout_ratio: float = 0.30
    auto_replay_gate: bool = False
    replay_gate_seeds: tuple[int, ...] = (42,)
    replay_data_dir: str = "data/replay"
    replay_gate_timeout_seconds: int = 1200
    auto_queue_promotion_on_replay_pass: bool = False
    objective_mode: str = "risk_adjusted"
    symbol_by_symbol: bool = False
    until_convergence: bool = False
    max_stagnant_iterations: int = 20
    max_iterations_per_symbol: int = 300
    auto_backfill_data: bool = True
    replay_timeframes: tuple[str, ...] = ("15m", "1h", "4h", "1d")
    replay_prefilter_enabled: bool = True
    exploration_min_signal_trades: int = 2
    promotion_min_signal_trades: int = 5
    min_probe_iterations_before_skip: int = 8
    min_partial_coverage_ratio: float = 0.05
    min_promotion_comparability: float = 0.50
    replay_eval_timeout_seconds: int = 7200
    replay_max_ticks: int = 250000
    max_paused_candle_health_ratio: float = 0.80
    max_symbols_per_cycle: int = 40
    mutate_params_per_candidate: int = 6
    mutate_step_pct: float = 3.0
    random_restart_every_n: int = 10
    uninformative_surface_probe_count: int = 12


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
        self.best_by_symbol: dict[str, CandidateResult] = {}

        self._param_keys = tuple(self.policy.allowed_parameter_paths)
        self._baseline_params = self._read_params_from_config(base_config)
        self._param_memory: ParameterMemory | None = None

    async def run(self) -> tuple[Path, Path]:
        """Run the full sandbox autoresearch loop."""
        if self.cfg.symbol_by_symbol:
            return await self._run_symbol_by_symbol()
        return await self._run_global_loop()

    def _build_evaluator(self, symbols: tuple[str, ...]) -> CandidateEvaluator:
        return CandidateEvaluator(
            base_config=self.base_config,
            spec=EvaluationSpec(
                symbols=symbols,
                lookback_days=self.cfg.lookback_days,
                starting_equity=self.base_config.backtest.starting_equity,
                mode=self.cfg.evaluation_mode,
                objective_mode=self.cfg.objective_mode,
                window_offsets_days=self.cfg.evaluation_window_offsets_days,
                holdout_ratio=self.cfg.holdout_ratio,
                replay_data_dir=self.cfg.replay_data_dir,
                replay_timeframes=self.cfg.replay_timeframes,
                auto_backfill_data=self.cfg.auto_backfill_data,
                min_partial_coverage_ratio=self.cfg.min_partial_coverage_ratio,
                replay_eval_timeout_seconds=self.cfg.replay_eval_timeout_seconds,
                replay_max_ticks=self.cfg.replay_max_ticks,
                max_paused_candle_health_ratio=self.cfg.max_paused_candle_health_ratio,
            ),
        )

    async def _run_global_loop(self) -> tuple[Path, Path]:
        """Legacy global optimizer mode."""
        self._evaluator = self._build_evaluator(self.cfg.symbols)
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

        baseline = await self._evaluate_candidate(
            "baseline",
            self._baseline_params,
            evaluator=self._evaluator,
        )
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
            result = await self._evaluate_candidate(
                candidate_id,
                params,
                evaluator=self._evaluator,
            )
            if result.metrics.net_return_pct <= baseline.metrics.net_return_pct:
                if "Does not beat baseline return" not in result.metrics.rejection_reasons:
                    result.metrics.rejection_reasons.append("Does not beat baseline return")
                result.accepted = False
                result.score -= 100.0
            if result.accepted:
                result.metadata["required_verification_commands"] = self._required_verification_commands()
                result.metadata["replay_gate_passed"] = False
                result.metadata["promotion_ready"] = False
            result.metadata["window_deltas_vs_baseline"] = self._window_deltas(result, baseline)
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
                        await self._notify(self._format_milestone(best), important=True)
                else:
                    best = result
                    await self._notify(self._format_milestone(best), important=True)

            self.store.update(iteration=idx)
            self._persist_leaderboard(best.candidate_id)
            if idx % max(1, self.cfg.digest_every) == 0:
                await self._notify(self._format_digest(idx, best))

        phase = self.store.read_state().get("phase")
        if phase not in {"stopped", "failed"}:
            self.store.update(phase="finished")

        if self.cfg.auto_replay_gate and best.accepted and best.candidate_id != "baseline":
            replay_outcome = await self._run_replay_gate_for_best(best)
            best.metadata["replay_gate"] = replay_outcome
            best.metadata["replay_gate_passed"] = bool(replay_outcome.get("passed", False))
            best.metadata["promotion_ready"] = bool(replay_outcome.get("passed", False))
            if replay_outcome.get("passed", False):
                self.store.mark_replay_gate_passed(best.candidate_id)
                if self.cfg.auto_queue_promotion_on_replay_pass:
                    self.store.queue_promotion(best.candidate_id)
                    await self._notify(
                        f"📌 Auto-queued <code>{best.candidate_id}</code> for review after replay pass.",
                        important=True,
                    )
            else:
                best.accepted = False
                best.metrics.rejection_reasons.append("Replay gate failed")
            self._persist_leaderboard(best.candidate_id)

        leaderboard_path, summary_path = self._write_reports(best)
        await self._notify(
            (
                "✅ Research run completed\n"
                f"Run: <code>{self.run_id}</code>\n"
                f"Best: <code>{best.candidate_id}</code>\n"
                f"Return: {best.metrics.net_return_pct:+.2f}% | "
                f"MaxDD: {best.metrics.max_drawdown_pct:.2f}% | "
                f"Sharpe: {best.metrics.sharpe:.2f}"
            ),
            important=True,
        )
        return leaderboard_path, summary_path

    async def _run_symbol_by_symbol(self) -> tuple[Path, Path]:
        """Optimize each symbol independently until convergence/safety cap."""
        symbols_to_run = list(self.cfg.symbols)
        skipped_ineligible: dict[str, list[str]] = {}
        if self.cfg.evaluation_mode == "replay" and self.cfg.replay_prefilter_enabled and symbols_to_run:
            preflight = self._build_evaluator(tuple(symbols_to_run))
            eligible_statuses: list[dict] = []
            partial_symbols: list[str] = []
            await self._notify(
                f"🧪 Replay eligibility prefilter started ({len(symbols_to_run)} symbols)."
            )
            for symbol in symbols_to_run:
                status = await preflight.assess_symbol_eligibility(symbol)
                if bool(status.get("eligible", False)):
                    eligible_statuses.append(status)
                    if status.get("eligibility_tier") == "partial":
                        partial_symbols.append(symbol)
                else:
                    skipped_ineligible[symbol] = list(status.get("reasons") or ["ineligible"])
            eligible_statuses.sort(key=lambda x: float(x.get("comparability_score", 0.0)), reverse=True)
            symbols_to_run = [str(s.get("symbol")) for s in eligible_statuses]
            max_symbols = max(1, int(self.cfg.max_symbols_per_cycle))
            if len(symbols_to_run) > max_symbols:
                symbols_to_run = symbols_to_run[:max_symbols]
            await self._notify(
                f"🧪 Replay eligibility prefilter finished: {len(symbols_to_run)}/{len(self.cfg.symbols)} eligible "
                f"({len(partial_symbols)} partial)."
            )

        if not symbols_to_run:
            self.store.update(
                run_id=self.run_id,
                phase="finished",
                current_symbol=None,
                total_symbols=0,
                completed_symbols=[],
                symbol_progress={},
                symbol_best_candidates={},
                best_candidate_id=None,
                leaderboard=[],
                skipped_ineligible_symbols=skipped_ineligible,
            )
            await self._notify("⛔ No eligible symbols for replay optimization.")
            empty = CandidateResult(
                candidate_id="no_eligible_symbols",
                symbol=None,
                params=self._baseline_params,
                metrics=self._failed_metrics_no_eligible(),
                score=-10_000.0,
                accepted=False,
                metadata={"skipped_ineligible_symbols": skipped_ineligible},
            )
            self.results = [empty]
            leaderboard_path, summary_path = self._write_reports(empty)
            return leaderboard_path, summary_path

        total_symbols = len(symbols_to_run)
        self.store.update(
            run_id=self.run_id,
            phase="running",
            iteration=0,
            total_iterations=0,
            current_symbol=None,
            total_symbols=total_symbols,
            completed_symbols=[],
            symbol_progress={},
            symbol_best_candidates={},
            best_candidate_id=None,
            leaderboard=[],
            last_error=None,
            pending_prompt=None,
            promotion_queue=[],
            skipped_ineligible_symbols=skipped_ineligible,
            eligible_symbols=symbols_to_run,
        )
        await self._notify(
            f"🔬 Per-coin research run started: <code>{self.run_id}</code> "
            f"({total_symbols} symbols, objective={self.cfg.objective_mode})"
        )

        completed_symbols: list[str] = []
        for symbol in symbols_to_run:
            if self.store.read_state().get("control", {}).get("stop_requested"):
                self.store.update(phase="stopped")
                await self._notify("🛑 Research stopped by operator.")
                break
            await self._wait_while_paused()
            self.store.update(current_symbol=symbol)
            evaluator = self._build_evaluator((symbol,))
            if self.cfg.evaluation_mode == "replay":
                coverage = await evaluator.prepare_symbol_data(symbol)
                if not bool(coverage.get("complete", False)):
                    await self._notify(
                        f"⚠️ Replay data incomplete for <code>{symbol}</code>; "
                        "symbol marked partial_data_non_comparable."
                    )

            safe_symbol = symbol.replace("/", "_").replace(":", "_")

            # Cross-run warm start: if we found good params in a previous
            # cycle, start from those instead of config baseline.
            warm_params = load_warm_start(symbol)
            starting_params = warm_params if warm_params is not None else self._baseline_params
            # Ensure warm-start params include all current allowlist keys
            for key in self._param_keys:
                if key not in starting_params:
                    starting_params[key] = self._baseline_params[key]

            # Initialize per-symbol parameter memory for guided mutation
            self._param_memory = ParameterMemory(self._param_keys)

            baseline = await self._evaluate_candidate(
                candidate_id=f"{safe_symbol}_baseline",
                params=starting_params,
                evaluator=evaluator,
                symbol=symbol,
            )
            symbol_results = [baseline]
            best = baseline
            stagnation = 0
            uninformative_streak = 0
            iteration = 0
            zero_trade_baseline = int(baseline.metrics.trade_count) == 0
            hard_cap = max(1, int(self.cfg.max_iterations_per_symbol if self.cfg.until_convergence else self.cfg.iterations))
            self.store.update_symbol_progress(
                symbol=symbol,
                iteration=0,
                total_iterations=hard_cap,
                best_candidate_id=best.candidate_id,
                phase="running",
            )
            await self._notify(self._format_milestone(best, is_baseline=True))

            # Hard-fail non-informative symbols early: no trade surface to optimize.
            if self._is_non_informative_baseline(baseline):
                await self._notify(
                    f"⚠️ Non-informative baseline for <code>{symbol}</code>; "
                    "skipping symbol (return sentinel or zero trades)."
                )
                self.store.update_symbol_progress(
                    symbol=symbol,
                    iteration=0,
                    total_iterations=hard_cap,
                    best_candidate_id=best.candidate_id,
                    phase="skipped_non_informative_baseline",
                )
                completed_symbols.append(symbol)
                self.results.extend(symbol_results)
                self.best_by_symbol[symbol] = best
                symbol_best = dict(self.store.read_state().get("symbol_best_candidates") or {})
                symbol_best[symbol] = best.candidate_id
                self.store.update(
                    completed_symbols=completed_symbols,
                    symbol_best_candidates=symbol_best,
                    best_candidate_id=best.candidate_id,
                )
                self._persist_leaderboard(best.candidate_id)
                continue

            while iteration < hard_cap:
                if self.store.read_state().get("control", {}).get("stop_requested"):
                    self.store.update(phase="stopped")
                    await self._notify("🛑 Research stopped by operator.")
                    break
                await self._wait_while_paused()
                iteration += 1
                candidate_id = f"{safe_symbol}_c{iteration:03d}"
                if zero_trade_baseline and iteration <= 6:
                    # Aggressively explore when baseline produces no trades
                    params = self._widen_for_zero_trade_baseline()
                elif self.cfg.random_restart_every_n > 0 and iteration % int(self.cfg.random_restart_every_n) == 0:
                    params = self._random_restart_params(iteration=iteration, hard_cap=hard_cap)
                else:
                    params = self._mutate_params(best.params, iteration=iteration, hard_cap=hard_cap)
                result = await self._evaluate_candidate(
                    candidate_id=candidate_id,
                    params=params,
                    evaluator=evaluator,
                    symbol=symbol,
                )
                if result.metrics.net_return_pct <= baseline.metrics.net_return_pct:
                    if "Does not beat baseline return" not in result.metrics.rejection_reasons:
                        result.metrics.rejection_reasons.append("Does not beat baseline return")
                    result.accepted = False
                    result.score -= 100.0
                if result.accepted:
                    result.metadata["required_verification_commands"] = self._required_verification_commands()
                    result.metadata["replay_gate_passed"] = False
                    result.metadata["promotion_ready"] = False
                result.metadata["window_deltas_vs_baseline"] = self._window_deltas(result, baseline)
                symbol_results.append(result)

                # Record outcome in parameter memory for guided future mutations
                score_delta = result.score - best.score
                if self._param_memory is not None:
                    self._param_memory.record(best.params, params, score_delta)

                if self._is_behavior_unchanged(result, baseline):
                    uninformative_streak += 1
                else:
                    uninformative_streak = 0
                if uninformative_streak >= max(1, int(self.cfg.uninformative_surface_probe_count)):
                    await self._notify(
                        f"⚠️ Uninformative surface for <code>{symbol}</code> after {iteration} probes "
                        "(candidate behavior unchanged vs baseline)."
                    )
                    self.store.update_symbol_progress(
                        symbol=symbol,
                        iteration=iteration,
                        total_iterations=hard_cap,
                        best_candidate_id=best.candidate_id,
                        phase="skipped_uninformative_surface",
                    )
                    break

                best_signal_trades = max(r.metrics.trade_count for r in symbol_results)
                if (
                    iteration >= max(1, int(self.cfg.min_probe_iterations_before_skip))
                    and best_signal_trades < int(self.cfg.exploration_min_signal_trades)
                ):
                    await self._notify(
                        f"⚠️ Insufficient signal for <code>{symbol}</code> after {iteration} probes "
                        f"(max trades={best_signal_trades}, threshold={self.cfg.exploration_min_signal_trades})."
                    )
                    self.store.update_symbol_progress(
                        symbol=symbol,
                        iteration=iteration,
                        total_iterations=hard_cap,
                        best_candidate_id=best.candidate_id,
                        phase="skipped_insufficient_signal",
                    )
                    break

                if result.score > best.score:
                    best = result
                    stagnation = 0
                    await self._notify(self._format_milestone(best), important=True)
                else:
                    stagnation += 1

                self.store.update_symbol_progress(
                    symbol=symbol,
                    iteration=iteration,
                    total_iterations=hard_cap,
                    best_candidate_id=best.candidate_id,
                    phase="running",
                )
                if self.cfg.until_convergence and stagnation >= max(1, self.cfg.max_stagnant_iterations):
                    await self._notify(
                        f"✅ Converged for <code>{symbol}</code> after {iteration} candidates "
                        f"(stagnant={stagnation}).",
                        important=True,
                    )
                    break

            completed_symbols.append(symbol)
            self.results.extend(symbol_results)
            self.best_by_symbol[symbol] = best
            symbol_best = dict(self.store.read_state().get("symbol_best_candidates") or {})
            symbol_best[symbol] = best.candidate_id
            self.store.update(
                completed_symbols=completed_symbols,
                symbol_best_candidates=symbol_best,
                best_candidate_id=best.candidate_id,
            )
            self._persist_leaderboard(best.candidate_id)
            self.store.update_symbol_progress(
                symbol=symbol,
                iteration=iteration,
                total_iterations=hard_cap,
                best_candidate_id=best.candidate_id,
                phase="finished",
            )

            # Counterfactual twin: validate candidate against live decision tape
            # to ensure backtest improvements translate to real-world uplift.
            if best.candidate_id != f"{safe_symbol}_baseline" and best.accepted:
                twin_report = await self._run_counterfactual_twin(symbol, best)
                best.metadata["counterfactual_twin"] = twin_report
                if twin_report.get("utility_uplift", 0.0) <= 0:
                    best.metadata["counterfactual_twin_warning"] = (
                        "Candidate does not improve utility on live decision tape"
                    )
                    await self._notify(
                        f"⚠️ Counterfactual twin for <code>{symbol}</code>: "
                        f"utility uplift={twin_report.get('utility_uplift', 0):.2f} "
                        f"(no improvement on live tape)."
                    )
                else:
                    await self._notify(
                        f"✅ Counterfactual twin for <code>{symbol}</code>: "
                        f"utility uplift=+{twin_report.get('utility_uplift', 0):.2f}, "
                        f"delta opens={twin_report.get('delta_open_count', 0):+d}."
                    )

            # Persist best params for cross-run warm start so the next cycle
            # picks up where this one left off instead of starting from scratch.
            if best.score > -500.0:
                save_warm_start(symbol, best.params, best.score, best.candidate_id)

            # Clear per-symbol memory
            self._param_memory = None

        if self.store.read_state().get("phase") not in {"stopped", "failed"}:
            self.store.update(phase="finished")

        best_overall = max(self.best_by_symbol.values(), key=lambda x: x.score) if self.best_by_symbol else self.results[0]
        leaderboard_path, summary_path = self._write_reports(best_overall)
        await self._notify(
            (
                "✅ Per-coin research run completed\n"
                f"Run: <code>{self.run_id}</code>\n"
                f"Symbols done: {len(completed_symbols)}/{total_symbols}\n"
                f"Best overall: <code>{best_overall.candidate_id}</code>"
            ),
            important=True,
        )
        return leaderboard_path, summary_path

    async def _evaluate_candidate(
        self,
        candidate_id: str,
        params: dict[str, float],
        *,
        evaluator: CandidateEvaluator,
        symbol: str | None = None,
    ) -> CandidateResult:
        violations = self.policy.validate_candidate_keys(params.keys())
        if violations:
            outcome = await evaluator.evaluate(self._baseline_params)
            outcome.metrics.rejection_reasons.extend(violations)
            return CandidateResult(
                candidate_id=candidate_id,
                symbol=symbol,
                params=params,
                metrics=outcome.metrics,
                score=-10_000.0,
                accepted=False,
                metadata={"policy_violations": violations},
            )

        outcome = await evaluator.evaluate(params)
        metrics = outcome.metrics
        accepted, reasons = self._promotion_gate(metrics)
        insufficient_signal = metrics.trade_count < int(self.cfg.promotion_min_signal_trades)
        if insufficient_signal:
            reasons.append(f"insufficient_signal(<{self.cfg.promotion_min_signal_trades} trades)")
            accepted = False
        replay_comp = outcome.diagnostics.get("replay_comparability_score")
        if replay_comp is not None and float(replay_comp) < float(self.cfg.min_promotion_comparability):
            reasons.append(
                f"low_comparability_for_promotion(<{self.cfg.min_promotion_comparability:.2f})"
            )
            accepted = False
        metrics.rejection_reasons.extend(reasons)
        composite = outcome.diagnostics.get("composite_score")
        if self.cfg.objective_mode == "net_pnl_only":
            base_score = float(metrics.net_return_pct)
        else:
            base_score = float(composite) if composite is not None else score_candidate(metrics)
        score = base_score if accepted else base_score - 250.0
        if insufficient_signal:
            score -= 250.0
        return CandidateResult(
            candidate_id=candidate_id,
            symbol=symbol,
            params=params,
            metrics=metrics,
            score=score,
            accepted=accepted,
            metadata={"evaluation": outcome.diagnostics},
        )

    def _promotion_gate(self, metrics) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        min_trades = self.cfg.promotion_min_signal_trades
        if metrics.trade_count < min_trades:
            reasons.append(f"Too few trades for statistical confidence (<{min_trades})")
        if metrics.max_drawdown_pct > 35.0:
            reasons.append("Max drawdown exceeds hard cap (35%)")
        if metrics.net_return_pct <= -10.0:
            reasons.append("Net return is deeply negative")
        if metrics.max_drawdown_pct > 20.0 and metrics.net_return_pct < 2.0:
            reasons.append("Weak risk-adjusted profile across split windows")
        return (len(reasons) == 0, reasons)

    @staticmethod
    def _clamp_param(key: str, value: float) -> float:
        """Clamp a parameter to its allowed bounds if defined."""
        bounds = PARAMETER_BOUNDS.get(key)
        if bounds:
            return max(bounds[0], min(bounds[1], value))
        return value

    def _read_params_from_config(self, config: Config) -> dict[str, float]:
        out: dict[str, float] = {}
        for key in self._param_keys:
            section_name, _, attr = key.partition(".")
            section = getattr(config, section_name, None)
            if section is None:
                # Optional section (e.g. multi_tp) not present — use bound midpoint
                bounds = PARAMETER_BOUNDS.get(key)
                if bounds:
                    out[key] = round((bounds[0] + bounds[1]) / 2, 6)
                continue
            val = getattr(section, attr, None)
            if val is None:
                bounds = PARAMETER_BOUNDS.get(key)
                if bounds:
                    out[key] = round((bounds[0] + bounds[1]) / 2, 6)
                continue
            out[key] = float(val)
        return out

    def _mutate_params(
        self,
        source: dict[str, float],
        step_multiplier: float = 1.0,
        iteration: int = 0,
        hard_cap: int = 1,
    ) -> dict[str, float]:
        candidate = dict(source)
        mutate_count = max(1, int(self.cfg.mutate_params_per_candidate))

        # Use parameter memory to pick params weighted by past success
        mem = self._param_memory
        if mem is not None and iteration > 6:
            keys_to_mutate = mem.pick_params_to_mutate(self.rng, mutate_count)
        else:
            keys_to_mutate = self.rng.sample(
                self._param_keys, k=min(mutate_count, len(self._param_keys))
            )

        # Adaptive step decay: start wide, narrow as we approach the cap.
        # progress goes 0→1; decay goes 1.0→0.3 (never fully zero).
        progress = min(iteration / max(hard_cap, 1), 1.0)
        anneal = 1.0 - 0.7 * progress
        effective_mult = step_multiplier * anneal

        for key in keys_to_mutate:
            value = candidate[key]
            step = max(0.001, abs(value) * float(self.cfg.mutate_step_pct)) * effective_mult

            # Directional bias: if memory knows a productive direction,
            # bias the delta toward it (70% chance follow, 30% explore).
            direction_bias = mem.get_direction_bias(key) if mem else 0.0
            if direction_bias != 0.0 and self.rng.random() < 0.7:
                delta = abs(self.rng.gauss(0, step)) * direction_bias
            else:
                delta = self.rng.uniform(-step, step)

            candidate[key] = self._clamp_param(key, round(value + delta, 6))
        return candidate

    def _random_restart_params(self, iteration: int = 0, hard_cap: int = 1) -> dict[str, float]:
        """Sample a fresh candidate around baseline to escape local flats."""
        return self._mutate_params(self._baseline_params, iteration=iteration, hard_cap=hard_cap)

    def _widen_for_zero_trade_baseline(self) -> dict[str, float]:
        """Aggressively loosen gate parameters when baseline produces 0 trades.

        Instead of random mutation, this deliberately lowers score thresholds
        and ADX gates to find the region of parameter space where the strategy
        actually fires signals.
        """
        from src.research.allowlist import PARAMETER_BOUNDS

        candidate = dict(self._baseline_params)
        gate_keys = [
            k for k in self._param_keys
            if any(tok in k for tok in (
                "min_score", "adx_threshold", "cooldown",
                "fib_proximity", "structure_fallback_score_premium",
                "cost_cap", "min_rr",
            ))
        ]
        for key in gate_keys:
            bounds = PARAMETER_BOUNDS.get(key)
            if bounds:
                lo, hi = bounds
                # Bias toward the permissive end: lower thresholds, wider
                # tolerances, looser cost caps
                if "fib_proximity" in key and "max" not in key:
                    # Fib proximity: higher = more permissive
                    candidate[key] = round(self.rng.uniform(lo + (hi - lo) * 0.5, hi), 6)
                else:
                    candidate[key] = round(self.rng.uniform(lo, lo + (hi - lo) * 0.4), 6)
        return candidate

    def _is_behavior_unchanged(self, candidate: CandidateResult, baseline: CandidateResult) -> bool:
        """Detect when candidate produces no meaningful behavior change vs baseline."""
        c = candidate.metrics
        b = baseline.metrics
        return (
            int(c.trade_count) == int(b.trade_count)
            and round(float(c.net_return_pct), 6) == round(float(b.net_return_pct), 6)
            and round(float(c.max_drawdown_pct), 6) == round(float(b.max_drawdown_pct), 6)
            and round(float(c.win_rate_pct), 6) == round(float(b.win_rate_pct), 6)
        )

    @staticmethod
    def _is_non_informative_baseline(baseline: CandidateResult) -> bool:
        """Return True when baseline has no usable signal for optimization."""
        m = baseline.metrics
        # Treat only hard-failure sentinel baselines as non-informative.
        # Zero-trade baselines can still become informative after mutations.
        return float(m.net_return_pct) <= -900.0

    async def _run_counterfactual_twin(
        self, symbol: str, candidate: CandidateResult
    ) -> dict:
        """Run counterfactual twin against live decision tape for a symbol."""
        try:
            tape = load_decision_tape(since_hours=168, symbols=(symbol,))
            if len(tape) < 5:
                return {
                    "skipped": True,
                    "reason": f"Insufficient decision tape ({len(tape)} decisions)",
                    "samples": len(tape),
                }
            report = evaluate_counterfactual_uplift(
                base_config=self.base_config,
                candidate_params=candidate.params,
                tape=tape,
            )
            return report
        except Exception as exc:
            logger.warning(
                "COUNTERFACTUAL_TWIN_FAILED",
                symbol=symbol,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return {"skipped": True, "reason": str(exc)}

    async def _requires_operator_decision(self, baseline: CandidateResult, candidate: CandidateResult) -> bool:
        return (
            candidate.metrics.net_return_pct > baseline.metrics.net_return_pct
            and candidate.metrics.max_drawdown_pct > baseline.metrics.max_drawdown_pct + 1.0
        )

    async def _request_decision(self, prompt_type: str, prompt_message: str) -> bool:
        if not self.cfg.enable_telegram:
            # Autonomous/no-telegram mode cannot service interactive approvals.
            return False
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
                "symbol": r.symbol,
                "score": r.score,
                "accepted": r.accepted,
                "promoted": r.promoted,
                "params": r.params,
                "metrics": asdict(r.metrics),
                "metadata": r.metadata,
            }
            for r in sorted(self.results, key=lambda x: x.score, reverse=True)
        ]
        self.store.update(leaderboard=board, best_candidate_id=best_candidate_id)
        self._write_incremental_reports(best_candidate_id)

    def _write_incremental_reports(self, best_candidate_id: str) -> None:
        if not self.results:
            return

        base = Path(self.cfg.out_dir)
        write_leaderboard(
            path=base / f"{self.run_id}_leaderboard.json",
            run_id=self.run_id,
            baseline_id="baseline",
            results=self.results,
        )

        best = next((r for r in self.results if r.candidate_id == best_candidate_id), None)
        if best is not None:
            write_summary(
                path=base / f"{self.run_id}_summary.md",
                run_id=self.run_id,
                baseline=self.results[0],
                best=best,
            )

        if self.best_by_symbol:
            write_per_symbol_best(
                path=base / f"{self.run_id}_best_by_symbol.json",
                run_id=self.run_id,
                best_by_symbol=self.best_by_symbol,
            )

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
        if self.best_by_symbol:
            write_per_symbol_best(
                path=base / f"{self.run_id}_best_by_symbol.json",
                run_id=self.run_id,
                best_by_symbol=self.best_by_symbol,
            )
        return leaderboard_path, summary_path

    async def _wait_while_paused(self) -> None:
        while self.store.read_state().get("control", {}).get("paused"):
            self.store.update(phase="paused")
            await asyncio.sleep(2)
        self.store.update(phase="running")

    async def _notify(self, message: str, *, important: bool = False) -> None:
        """Send a research notification.

        Args:
            message: The notification text.
            important: If True, always send to Telegram. If False, only log
                locally. This keeps Telegram quiet unless the optimizer finds
                a meaningful result or hits a terminal event.
        """
        if self.cfg.enable_telegram and important:
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

    def _window_deltas(self, candidate: CandidateResult, baseline: CandidateResult) -> list[dict[str, float]]:
        """Compute baseline-vs-candidate split deltas for diagnostics."""
        cand_windows = (
            candidate.metadata.get("evaluation", {}).get("per_window", [])
            if isinstance(candidate.metadata, dict)
            else []
        )
        base_windows = (
            baseline.metadata.get("evaluation", {}).get("per_window", [])
            if isinstance(baseline.metadata, dict)
            else []
        )
        base_map: dict[int, dict] = {}
        for w in base_windows:
            try:
                base_map[int(w.get("offset_days", 0))] = w
            except Exception:  # noqa: BLE001 - diagnostics must not break run loop.
                continue

        deltas: list[dict[str, float]] = []
        for w in cand_windows:
            try:
                offset = int(w.get("offset_days", 0))
            except Exception:  # noqa: BLE001
                continue
            b = base_map.get(offset)
            if not b:
                continue
            c_hold = w.get("holdout", {})
            b_hold = b.get("holdout", {})
            try:
                deltas.append(
                    {
                        "offset_days": float(offset),
                        "delta_return_pct": float(c_hold.get("net_return_pct", 0.0))
                        - float(b_hold.get("net_return_pct", 0.0)),
                        "delta_max_drawdown_pct": float(c_hold.get("max_drawdown_pct", 0.0))
                        - float(b_hold.get("max_drawdown_pct", 0.0)),
                        "delta_sharpe": float(c_hold.get("sharpe", 0.0)) - float(b_hold.get("sharpe", 0.0)),
                    }
                )
            except Exception:  # noqa: BLE001
                continue
        return deltas

    def _required_verification_commands(self) -> list[str]:
        """Repository verification checklist before promoting a candidate."""
        return [
            "uv run pytest tests/unit/test_config_load.py -v",
            "uv run pytest tests/unit/test_runner_mode.py -v",
            "make smoke",
            "make replay SEED=42",
        ]

    def _failed_metrics_no_eligible(self):
        from src.research.models import CandidateMetrics

        return CandidateMetrics(
            net_return_pct=-999.0,
            max_drawdown_pct=100.0,
            sharpe=0.0,
            sortino=None,
            win_rate_pct=0.0,
            trade_count=0,
            rejection_reasons=["No eligible symbols after replay prefilter"],
        )

    async def _run_replay_gate_for_best(self, best: CandidateResult) -> dict:
        """Run replay episodes for configured seeds and return pass/fail outcome."""
        run_dir = Path(self.cfg.out_dir).resolve()
        replay_root = run_dir / "replay_gate"
        replay_root.mkdir(parents=True, exist_ok=True)
        python_bin = Path(os.environ.get("VIRTUAL_ENV", "")) / "bin" / "python3"
        if not python_bin.exists():
            python_bin = Path("python3")

        seed_results: list[dict[str, object]] = []
        await self._notify(
            f"🧪 Running replay gate for <code>{best.candidate_id}</code> "
            f"(seeds={','.join(str(s) for s in self.cfg.replay_gate_seeds)})"
        )

        for seed in self.cfg.replay_gate_seeds:
            out_dir = replay_root / f"seed_{seed}"
            out_dir.mkdir(parents=True, exist_ok=True)
            cmd = [
                str(python_bin),
                "-m",
                "src.backtest.replay_harness.run_episodes",
                "--seed",
                str(seed),
                "--data-dir",
                self.cfg.replay_data_dir,
                "--output",
                str(out_dir),
            ]
            logger.info("Replay gate command", seed=seed, command=" ".join(shlex.quote(c) for c in cmd))
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(),
                    timeout=max(30, int(self.cfg.replay_gate_timeout_seconds)),
                )
                return_code = proc.returncode
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                stdout = b""
                stderr = (
                    f"Replay gate timed out after {self.cfg.replay_gate_timeout_seconds}s for seed {seed}".encode(
                        "utf-8"
                    )
                )
                return_code = 124
            seed_results.append(
                {
                    "seed": seed,
                    "exit_code": return_code,
                    "stdout_tail": stdout.decode("utf-8", errors="replace")[-4000:],
                    "stderr_tail": stderr.decode("utf-8", errors="replace")[-2000:],
                }
            )

        passed = all(int(r.get("exit_code", 1)) == 0 for r in seed_results)
        await self._notify(
            (
                f"{'✅' if passed else '⛔'} Replay gate {'passed' if passed else 'failed'} for "
                f"<code>{best.candidate_id}</code> "
                f"(seeds={','.join(str(s) for s in self.cfg.replay_gate_seeds)})"
            ),
            important=True,
        )
        return {"passed": passed, "seeds": seed_results}


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
