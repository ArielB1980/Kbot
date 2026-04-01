"""Candidate evaluation backends for sandbox autoresearch."""

from __future__ import annotations

import asyncio
import csv
import hashlib
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from src.backtest.backtest_engine import BacktestEngine, BacktestMetrics
from src.backtest.replay_harness.runner import BacktestRunner
from src.config.config import Config
from src.data.data_acquisition import DataAcquisition
from src.data.kraken_client import KrakenClient
from src.data.symbol_utils import futures_candidate_symbols
from src.monitoring.logger import get_logger
from src.research.kpi import metrics_from_backtest
from src.research.models import CandidateMetrics
from src.storage.repository import get_candles

logger = get_logger(__name__)


@dataclass(frozen=True)
class EvaluationSpec:
    """Inputs that define candidate evaluation."""

    symbols: tuple[str, ...]
    lookback_days: int
    starting_equity: Decimal
    mode: str = "backtest"  # backtest | replay | mock
    objective_mode: str = "risk_adjusted"  # risk_adjusted | net_pnl_only
    sleep_between_symbols_seconds: float = 0.5
    window_offsets_days: tuple[int, ...] = (0, 30, 60)
    holdout_ratio: float = 0.30
    replay_data_dir: str = "data/replay"
    replay_timeframes: tuple[str, ...] = ("1m", "15m", "1h", "4h", "1d")
    auto_backfill_data: bool = True
    min_coverage_ratio: float = 0.95
    min_partial_coverage_ratio: float = 0.05
    replay_eval_timeout_seconds: int = 3600
    replay_max_ticks: int = 20000
    max_paused_candle_health_ratio: float = 0.80


@dataclass
class EvaluationOutcome:
    """Evaluation result including diagnostics used for robust ranking."""

    metrics: CandidateMetrics
    diagnostics: dict[str, Any]


@dataclass
class ReplayAggregate:
    """Aggregate result for one replay period."""

    metrics: CandidateMetrics
    paused_candle_health_ratio: float


class CandidateEvaluator:
    """Evaluates candidate params against configured research backend."""

    def __init__(self, base_config: Config, spec: EvaluationSpec):
        self.base_config = base_config
        self.spec = spec
        self._anchor_now = datetime.now(timezone.utc)
        self._prepared_symbols: set[str] = set()
        self._coverage_status: dict[str, dict[str, Any]] = {}
        self._futures_ticker_keys: set[str] | None = None

    async def prepare_symbol_data(self, symbol: str) -> dict[str, Any]:
        """Ensure replay data cache exists for a symbol over the full test horizon."""
        if self.spec.mode != "replay":
            return {"symbol": symbol, "mode": self.spec.mode, "prepared": True}

        if symbol in self._prepared_symbols:
            return self._coverage_status.get(symbol, {"symbol": symbol, "prepared": True})

        start, end = self._full_window_bounds()
        status = await self._ensure_replay_data_for_symbol(symbol, start, end)
        self._prepared_symbols.add(symbol)
        self._coverage_status[symbol] = status
        return status

    async def assess_symbol_eligibility(self, symbol: str) -> dict[str, Any]:
        """Assess whether replay optimization should run for a symbol."""
        coverage = await self.prepare_symbol_data(symbol)
        coverage_complete = bool(coverage.get("complete", False))
        comparability_score = self._coverage_comparability_score(coverage)
        has_available_window = bool(coverage.get("available_start")) and bool(coverage.get("available_end"))
        tradable_now = True
        reasons: list[str] = []
        if self.spec.mode == "replay":
            tradable_now = await self._has_futures_ticker(symbol)
            if not tradable_now:
                reasons.append("no_futures_ticker")
        if not coverage_complete and not has_available_window:
            reasons.append("partial_data_non_comparable")
        eligible = tradable_now and (coverage_complete or has_available_window)
        tier = "full" if coverage_complete else ("partial" if eligible else "ineligible")
        return {
            "symbol": symbol,
            "eligible": eligible,
            "eligibility_tier": tier,
            "coverage_complete": coverage_complete,
            "comparability_score": comparability_score,
            "has_available_window": has_available_window,
            "tradable_now": tradable_now,
            "reasons": reasons,
            "coverage": coverage,
        }

    async def evaluate(self, params: dict[str, float]) -> EvaluationOutcome:
        """Evaluate one candidate and return normalized KPI payload and diagnostics."""
        if self.spec.mode == "mock":
            metrics = self._evaluate_mock(params)
            return EvaluationOutcome(
                metrics=metrics,
                diagnostics={
                    "composite_score_inputs": {"train_weight": 0.4, "holdout_weight": 0.6},
                    "per_window": [],
                    "train_score": None,
                    "holdout_score": None,
                    "composite_score": None,
                },
            )
        if self.spec.mode == "replay":
            return await self._evaluate_replay(params)
        return await self._evaluate_backtest(params)

    def _evaluate_mock(self, params: dict[str, float]) -> CandidateMetrics:
        """Deterministic mock evaluator used by tests and dry-runs."""
        payload = ",".join(f"{k}={v:.6f}" for k, v in sorted(params.items()))
        seed = int(hashlib.sha256(payload.encode("utf-8")).hexdigest()[:8], 16)
        ret = ((seed % 500) / 100.0) - 1.5
        dd = 4.0 + ((seed // 7) % 700) / 100.0
        sharpe = ((seed // 11) % 300) / 100.0
        win_rate = 42.0 + ((seed // 13) % 400) / 10.0
        trades = 10 + ((seed // 17) % 180)
        sortino = sharpe * 1.05 if sharpe > 0 else None
        return CandidateMetrics(
            net_return_pct=ret,
            max_drawdown_pct=dd,
            sharpe=sharpe,
            sortino=sortino,
            win_rate_pct=win_rate,
            trade_count=trades,
            rejection_reasons=[],
        )

    async def _evaluate_backtest(self, params: dict[str, float]) -> EvaluationOutcome:
        """Run multi-window split backtests and return robust blended metrics."""
        config = self.base_config.model_copy(deep=True)
        _apply_params(config, params)
        return await self._evaluate_windows_backtest(config)

    async def _evaluate_replay(self, params: dict[str, float]) -> EvaluationOutcome:
        """Run multi-window split replay and return robust blended metrics."""
        return await self._evaluate_windows_replay(params)

    async def _evaluate_windows_backtest(self, config: Config) -> EvaluationOutcome:
        holdout_ratio = min(0.8, max(0.1, float(self.spec.holdout_ratio)))
        windows: list[dict[str, Any]] = []
        train_metrics_all: list[CandidateMetrics] = []
        holdout_metrics_all: list[CandidateMetrics] = []

        for offset_days in self.spec.window_offsets_days:
            start_date, split_date, end_date = self._window_bounds(offset_days, holdout_ratio)
            train_metrics = await self._run_aggregate_backtest_for_period(config, start_date, split_date)
            holdout_metrics = await self._run_aggregate_backtest_for_period(config, split_date, end_date)
            windows.append(
                {
                    "offset_days": int(offset_days),
                    "train": _metrics_to_dict(train_metrics),
                    "holdout": _metrics_to_dict(holdout_metrics),
                }
            )
            if _is_valid_window_metrics(train_metrics):
                train_metrics_all.append(train_metrics)
            if _is_valid_window_metrics(holdout_metrics):
                holdout_metrics_all.append(holdout_metrics)

        return _compose_window_outcome(train_metrics_all, holdout_metrics_all, windows)

    async def _evaluate_windows_replay(self, params: dict[str, float]) -> EvaluationOutcome:
        holdout_ratio = min(0.8, max(0.1, float(self.spec.holdout_ratio)))
        windows: list[dict[str, Any]] = []
        train_metrics_all: list[CandidateMetrics] = []
        holdout_metrics_all: list[CandidateMetrics] = []
        replay_coverage: dict[str, Any] = {}
        paused_ratios: list[float] = []

        for symbol in self.spec.symbols:
            replay_coverage[symbol] = await self.prepare_symbol_data(symbol)

        available_window = self._replay_available_window()
        for offset_days in self.spec.window_offsets_days:
            req_start, _, req_end = self._window_bounds(offset_days, holdout_ratio)
            start_date, split_date, end_date = self._window_bounds_with_available_data(
                req_start=req_start,
                req_end=req_end,
                holdout_ratio=holdout_ratio,
                available_window=available_window,
            )
            train_agg = await self._run_aggregate_replay_for_period(params, start_date, split_date)
            holdout_agg = await self._run_aggregate_replay_for_period(params, split_date, end_date)
            train_metrics = train_agg.metrics
            holdout_metrics = holdout_agg.metrics
            paused_ratios.extend([train_agg.paused_candle_health_ratio, holdout_agg.paused_candle_health_ratio])
            windows.append(
                {
                    "offset_days": int(offset_days),
                    "train": _metrics_to_dict(train_metrics),
                    "holdout": _metrics_to_dict(holdout_metrics),
                    "train_paused_candle_health_ratio": train_agg.paused_candle_health_ratio,
                    "holdout_paused_candle_health_ratio": holdout_agg.paused_candle_health_ratio,
                }
            )
            if _is_valid_window_metrics(train_metrics):
                train_metrics_all.append(train_metrics)
            if _is_valid_window_metrics(holdout_metrics):
                holdout_metrics_all.append(holdout_metrics)

        outcome = _compose_window_outcome(train_metrics_all, holdout_metrics_all, windows)
        outcome.diagnostics["replay_data_coverage"] = replay_coverage
        comp_by_symbol = {s: self._coverage_comparability_score(c) for s, c in replay_coverage.items()}
        outcome.diagnostics["replay_comparability_by_symbol"] = comp_by_symbol
        outcome.diagnostics["replay_comparability_score"] = min(comp_by_symbol.values()) if comp_by_symbol else 0.0
        outcome.diagnostics["paused_candle_health_ratio"] = (
            (sum(paused_ratios) / len(paused_ratios)) if paused_ratios else 0.0
        )
        outcome.diagnostics["replay_effective_window"] = {
            "available_start": available_window[0].isoformat() if available_window[0] else None,
            "available_end": available_window[1].isoformat() if available_window[1] else None,
        }
        return outcome

    async def _run_aggregate_backtest_for_period(
        self,
        config: Config,
        start_date: datetime,
        end_date: datetime,
    ) -> CandidateMetrics:
        """Run symbol-level backtests for one period and aggregate to KPIs."""
        all_metrics: list[BacktestMetrics] = []
        for symbol in self.spec.symbols:
            engine = BacktestEngine(config, symbol=symbol, starting_equity=self.spec.starting_equity)
            try:
                metrics = await engine.run(start_date=start_date, end_date=end_date)
                all_metrics.append(metrics)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Research symbol backtest failed",
                    symbol=symbol,
                    start=start_date.isoformat(),
                    end=end_date.isoformat(),
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
            finally:
                if getattr(engine, "client", None):
                    try:
                        await engine.client.close()
                    except Exception:  # noqa: BLE001
                        pass
            await asyncio.sleep(max(0.0, self.spec.sleep_between_symbols_seconds))

        if not all_metrics:
            return _failed_metrics("No successful symbol backtests")
        normalized = [metrics_from_backtest(m, self.spec.starting_equity) for m in all_metrics]
        return _average_metrics(normalized)

    @staticmethod
    @contextmanager
    def _suppress_replay_override_env_vars():
        """Temporarily remove REPLAY_OVERRIDE_* env vars so research mutations take effect.

        The continuous daemon exports REPLAY_OVERRIDE_* env vars for promotion
        validation.  These override the very config parameters that the research
        harness is mutating, making all candidates behave identically to
        baseline ("uninformative surface").  We strip them during evaluation so
        that config_overrides from the harness actually reach the strategy.
        """
        saved: dict[str, str] = {}
        for key in list(os.environ):
            if key.startswith("REPLAY_OVERRIDE_"):
                saved[key] = os.environ.pop(key)
        try:
            yield
        finally:
            os.environ.update(saved)

    async def _run_aggregate_replay_for_period(
        self,
        params: dict[str, float],
        start_date: datetime,
        end_date: datetime,
    ) -> ReplayAggregate:
        """Run symbol-level replay backtests for one period and aggregate."""
        if end_date <= start_date:
            return ReplayAggregate(
                metrics=_failed_metrics("No replay window overlap with available data"),
                paused_candle_health_ratio=0.0,
            )
        normalized: list[CandidateMetrics] = []
        paused_ratios: list[float] = []
        for symbol in self.spec.symbols:
            coverage = self._coverage_status.get(symbol, {})
            if not coverage.get("available_start") or not coverage.get("available_end"):
                # Don't produce a -999 sentinel for partial data — this causes
                # the optimizer to skip the symbol entirely.  Instead, return
                # 0-trade metrics so the optimizer can try loosening gates.
                normalized.append(CandidateMetrics(
                    net_return_pct=0.0,
                    max_drawdown_pct=0.0,
                    sharpe=0.0,
                    sortino=None,
                    win_rate_pct=0.0,
                    trade_count=0,
                    rejection_reasons=["partial_data_non_comparable"],
                ))
                paused_ratios.append(0.0)
                continue
            try:
                with self._suppress_replay_override_env_vars():
                    runner = BacktestRunner(
                        data_dir=Path(self.spec.replay_data_dir),
                        symbols=[symbol],
                        start=start_date,
                        end=end_date,
                        tick_interval_seconds=900,
                        max_ticks=max(1, int(self.spec.replay_max_ticks)),
                        timeframes=list(self.spec.replay_timeframes),
                        config_overrides=params,
                        disable_cycle_guard_throttle=True,
                        disable_db_mock=bool(os.getenv("DATABASE_URL", "").strip()),
                    )
                    replay = await asyncio.wait_for(
                        runner.run(),
                        timeout=max(1, int(self.spec.replay_eval_timeout_seconds)),
                    )
                summary = replay.summary()
                paused_ratio = float((summary.get("system") or {}).get("trade_paused_ratio", 0.0))
                paused_ratios.append(paused_ratio)
                if paused_ratio > float(self.spec.max_paused_candle_health_ratio):
                    normalized.append(_failed_metrics("paused_candle_health_ratio_high"))
                else:
                    normalized.append(_metrics_from_replay(replay, self.spec.starting_equity))
            except asyncio.TimeoutError:
                logger.warning(
                    "Research symbol replay timed out",
                    symbol=symbol,
                    start=start_date.isoformat(),
                    end=end_date.isoformat(),
                    timeout_seconds=int(self.spec.replay_eval_timeout_seconds),
                )
                normalized.append(_failed_metrics("replay_timeout_no_progress"))
                paused_ratios.append(1.0)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Research symbol replay failed",
                    symbol=symbol,
                    start=start_date.isoformat(),
                    end=end_date.isoformat(),
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                paused_ratios.append(1.0)
            await asyncio.sleep(max(0.0, self.spec.sleep_between_symbols_seconds))

        paused_ratio_agg = (sum(paused_ratios) / len(paused_ratios)) if paused_ratios else 0.0
        if not normalized:
            return ReplayAggregate(
                metrics=_failed_metrics("No successful symbol replays"),
                paused_candle_health_ratio=paused_ratio_agg,
            )
        return ReplayAggregate(
            metrics=_average_metrics(normalized),
            paused_candle_health_ratio=paused_ratio_agg,
        )

    async def _ensure_replay_data_for_symbol(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> dict[str, Any]:
        """Ensure DB and replay CSV cache coverage for a symbol across timeframes."""
        results: dict[str, Any] = {"symbol": symbol, "complete": True, "timeframes": {}}
        for timeframe in self.spec.replay_timeframes:
            coverage = _calculate_coverage(symbol, timeframe, start, end)
            if coverage["coverage_ratio"] < self.spec.min_coverage_ratio and self.spec.auto_backfill_data:
                await self._backfill_symbol(symbol, timeframe, start, end)
                coverage = _calculate_coverage(symbol, timeframe, start, end)

            candles = get_candles(symbol, timeframe, start, end)
            _write_replay_csv(Path(self.spec.replay_data_dir), symbol, timeframe, candles)
            timeframe_ok = coverage["coverage_ratio"] >= self.spec.min_coverage_ratio and bool(candles)
            if not timeframe_ok:
                results["complete"] = False
            results["timeframes"][timeframe] = {**coverage, "ok": timeframe_ok, "candle_count": len(candles)}
        base_timeframe = self.spec.replay_timeframes[0] if self.spec.replay_timeframes else None
        base_cov = results["timeframes"].get(base_timeframe or "")
        if base_cov and base_cov.get("first_ts") and base_cov.get("last_ts"):
            results["available_start"] = str(base_cov["first_ts"])
            results["available_end"] = str(base_cov["last_ts"])
        return results

    async def _backfill_symbol(self, symbol: str, timeframe: str, start: datetime, end: datetime) -> None:
        """Try to backfill missing symbol data into DB."""
        client = KrakenClient(
            api_key=self.base_config.exchange.api_key or "",
            api_secret=self.base_config.exchange.api_secret or "",
            futures_api_key=self.base_config.exchange.futures_api_key or "",
            futures_api_secret=self.base_config.exchange.futures_api_secret or "",
            use_testnet=False,
        )
        try:
            await client.initialize()
            acq = DataAcquisition(client, [symbol], [])
            await acq.fetch_spot_historical(
                symbol=symbol,
                timeframe=timeframe,
                start_time=start,
                end_time=end,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Replay preflight backfill failed",
                symbol=symbol,
                timeframe=timeframe,
                start=start.isoformat(),
                end=end.isoformat(),
                error=str(exc),
                error_type=type(exc).__name__,
            )
        finally:
            try:
                await client.close()
            except Exception:  # noqa: BLE001
                pass

    def _coverage_comparability_score(self, coverage: dict[str, Any]) -> float:
        """Compute comparable-data score in [0,1] from timeframe coverages."""
        timeframes = (coverage or {}).get("timeframes") or {}
        if not timeframes:
            return 0.0
        ratios: list[float] = []
        for tf in timeframes.values():
            try:
                ratios.append(float(tf.get("coverage_ratio", 0.0)))
            except Exception:  # noqa: BLE001
                ratios.append(0.0)
        if not ratios:
            return 0.0
        return max(0.0, min(1.0, sum(ratios) / len(ratios)))

    async def _has_futures_ticker(self, symbol: str) -> bool:
        """Check if symbol currently maps to a live futures ticker key."""
        keys = await self._load_futures_ticker_keys()
        if not keys:
            return False
        candidates = futures_candidate_symbols(symbol)
        return any(c.upper() in keys for c in candidates)

    async def _load_futures_ticker_keys(self) -> set[str]:
        """Load and cache futures ticker keys once per evaluator."""
        if self._futures_ticker_keys is not None:
            return self._futures_ticker_keys
        client = KrakenClient(
            api_key=self.base_config.exchange.api_key or "",
            api_secret=self.base_config.exchange.api_secret or "",
            futures_api_key=self.base_config.exchange.futures_api_key or "",
            futures_api_secret=self.base_config.exchange.futures_api_secret or "",
            use_testnet=False,
        )
        try:
            await client.initialize()
            tickers = await client.get_futures_tickers_bulk_full()
            self._futures_ticker_keys = {str(k).upper() for k in tickers.keys()}
            return self._futures_ticker_keys
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Replay eligibility futures ticker check failed",
                symbol_count=len(self.spec.symbols),
                error=str(exc),
                error_type=type(exc).__name__,
            )
            self._futures_ticker_keys = set()
            return self._futures_ticker_keys
        finally:
            try:
                await client.close()
            except Exception:  # noqa: BLE001
                pass

    def _window_bounds(self, offset_days: int, holdout_ratio: float) -> tuple[datetime, datetime, datetime]:
        end_date = self._anchor_now - timedelta(days=max(0, int(offset_days)))
        start_date = end_date - timedelta(days=max(1, self.spec.lookback_days))
        split_date = start_date + timedelta(days=self.spec.lookback_days * (1.0 - holdout_ratio))
        return start_date, split_date, end_date

    def _full_window_bounds(self) -> tuple[datetime, datetime]:
        max_offset = max(0, *(int(x) for x in self.spec.window_offsets_days))
        min_offset = min(0, *(int(x) for x in self.spec.window_offsets_days))
        start = self._anchor_now - timedelta(days=max_offset + max(1, self.spec.lookback_days))
        end = self._anchor_now - timedelta(days=min_offset)
        return start, end

    def _replay_available_window(self) -> tuple[datetime | None, datetime | None]:
        """Return common data overlap window across prepared replay symbols."""
        starts: list[datetime] = []
        ends: list[datetime] = []
        for symbol in self.spec.symbols:
            coverage = self._coverage_status.get(symbol, {})
            start_raw = coverage.get("available_start")
            end_raw = coverage.get("available_end")
            if start_raw and end_raw:
                starts.append(datetime.fromisoformat(str(start_raw)))
                ends.append(datetime.fromisoformat(str(end_raw)))
        if not starts or not ends:
            return None, None
        return max(starts), min(ends)

    def _window_bounds_with_available_data(
        self,
        *,
        req_start: datetime,
        req_end: datetime,
        holdout_ratio: float,
        available_window: tuple[datetime | None, datetime | None],
    ) -> tuple[datetime, datetime, datetime]:
        """Clip replay window to available data and recompute split point."""
        available_start, available_end = available_window
        start = max(req_start, available_start) if available_start else req_start
        end = min(req_end, available_end) if available_end else req_end
        if end <= start:
            return req_start, req_start, req_start
        split = start + ((end - start) * (1.0 - holdout_ratio))
        return start, split, end


def _metrics_from_replay(replay_metrics: Any, starting_equity: Decimal) -> CandidateMetrics:
    """Normalize replay metrics into candidate metrics contract."""
    summary = replay_metrics.summary().get("trading", {})
    net_pnl = Decimal(str(summary.get("net_pnl", 0.0)))
    equity = Decimal(str(starting_equity))
    net_return_pct = float((net_pnl / equity) * Decimal("100")) if equity > 0 else 0.0
    win_rate_pct = float(summary.get("win_rate", 0.0)) * 100.0
    return CandidateMetrics(
        net_return_pct=net_return_pct,
        max_drawdown_pct=float(summary.get("max_drawdown_pct", 0.0)),
        sharpe=0.0,
        sortino=None,
        win_rate_pct=win_rate_pct,
        trade_count=int(summary.get("total_trades", 0)),
        rejection_reasons=[],
    )


def _calculate_coverage(symbol: str, timeframe: str, start: datetime, end: datetime) -> dict[str, Any]:
    """Compute coverage ratio from DB candles."""
    candles = get_candles(symbol, timeframe, start, end)
    tf_minutes = _timeframe_to_minutes(timeframe)
    expected_raw = max(1, int(((end - start).total_seconds() / 60) / tf_minutes))
    # Kraken effectively caps recent 1m history to ~720 bars on many symbols.
    # Treat this as a provider-bound effective expectation for replay preflight.
    provider_cap = 720 if timeframe == "1m" else None
    expected_effective = min(expected_raw, provider_cap) if provider_cap else expected_raw
    ratio = min(1.0, float(len(candles)) / float(expected_effective)) if expected_effective > 0 else 0.0
    return {
        "expected_raw": expected_raw,
        "expected": expected_effective,
        "provider_cap": provider_cap,
        "actual": len(candles),
        "coverage_ratio": ratio,
        "first_ts": candles[0].timestamp.isoformat() if candles else None,
        "last_ts": candles[-1].timestamp.isoformat() if candles else None,
    }


def _write_replay_csv(data_dir: Path, symbol: str, timeframe: str, candles: list[Any]) -> None:
    """Write replay candle csv from DB candles."""
    safe_symbol = symbol.replace("/", "_").replace(":", "_")
    candles_dir = data_dir / "candles"
    candles_dir.mkdir(parents=True, exist_ok=True)
    out = candles_dir / f"{safe_symbol}_{timeframe}.csv"
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        for c in candles:
            writer.writerow(
                {
                    "timestamp": c.timestamp.isoformat(),
                    "open": str(c.open),
                    "high": str(c.high),
                    "low": str(c.low),
                    "close": str(c.close),
                    "volume": str(c.volume),
                }
            )


def _timeframe_to_minutes(timeframe: str) -> int:
    mapping = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440}
    return mapping.get(timeframe, 60)


def _apply_params(config: Config, params: dict[str, float]) -> None:
    """Apply dot-path params to pydantic config object."""
    for key, value in params.items():
        head, _, tail = key.partition(".")
        if not tail:
            continue
        section = getattr(config, head)
        setattr(section, tail, value)


def _failed_metrics(reason: str) -> CandidateMetrics:
    return CandidateMetrics(
        net_return_pct=-999.0,
        max_drawdown_pct=100.0,
        sharpe=0.0,
        sortino=None,
        win_rate_pct=0.0,
        trade_count=0,
        rejection_reasons=[reason],
    )


def _is_valid_window_metrics(metrics: CandidateMetrics) -> bool:
    """Treat hard-failure sentinel values as invalid window evaluations."""
    return metrics.net_return_pct > -900.0


def _compose_window_outcome(
    train_metrics_all: list[CandidateMetrics],
    holdout_metrics_all: list[CandidateMetrics],
    windows: list[dict[str, Any]],
) -> EvaluationOutcome:
    if not holdout_metrics_all:
        # Instead of returning a -999% sentinel that causes the optimizer to
        # skip this symbol entirely, return a 0-trade penalty score.  This
        # lets the optimizer try mutations that might produce trades.
        zero_trade = CandidateMetrics(
            net_return_pct=0.0,
            max_drawdown_pct=0.0,
            sharpe=0.0,
            sortino=None,
            win_rate_pct=0.0,
            trade_count=0,
            rejection_reasons=["No successful holdout evaluations across windows"],
        )
        return EvaluationOutcome(
            metrics=zero_trade,
            diagnostics={
                "per_window": windows,
                "train_score": None,
                "holdout_score": None,
                "composite_score": -500.0,
            },
        )

    train_agg = _average_metrics(train_metrics_all) if train_metrics_all else _average_metrics(holdout_metrics_all)
    holdout_agg = _average_metrics(holdout_metrics_all)
    blended = _blend_metrics(train_agg, holdout_agg, train_weight=0.4, holdout_weight=0.6)

    from src.research.kpi import score_candidate

    train_score = score_candidate(train_agg)
    holdout_score = score_candidate(holdout_agg)
    composite_score = (0.4 * train_score) + (0.6 * holdout_score)

    return EvaluationOutcome(
        metrics=blended,
        diagnostics={
            "per_window": windows,
            "train_score": train_score,
            "holdout_score": holdout_score,
            "composite_score": composite_score,
            "composite_score_inputs": {"train_weight": 0.4, "holdout_weight": 0.6},
        },
    )


def _average_metrics(metrics_list: list[CandidateMetrics]) -> CandidateMetrics:
    """Average a list of candidate metric payloads."""
    sortinos = [m.sortino for m in metrics_list if m.sortino is not None]
    reasons: list[str] = []
    for m in metrics_list:
        reasons.extend(m.rejection_reasons)
    return CandidateMetrics(
        net_return_pct=sum(m.net_return_pct for m in metrics_list) / len(metrics_list),
        max_drawdown_pct=sum(m.max_drawdown_pct for m in metrics_list) / len(metrics_list),
        sharpe=sum(m.sharpe for m in metrics_list) / len(metrics_list),
        sortino=(sum(sortinos) / len(sortinos)) if sortinos else None,
        win_rate_pct=sum(m.win_rate_pct for m in metrics_list) / len(metrics_list),
        trade_count=int(sum(m.trade_count for m in metrics_list)),
        rejection_reasons=reasons,
    )


def _blend_metrics(
    train: CandidateMetrics,
    holdout: CandidateMetrics,
    *,
    train_weight: float,
    holdout_weight: float,
) -> CandidateMetrics:
    """Blend train and holdout metrics with holdout emphasis."""
    sortino_values = [x for x in [train.sortino, holdout.sortino] if x is not None]
    return CandidateMetrics(
        net_return_pct=(train.net_return_pct * train_weight) + (holdout.net_return_pct * holdout_weight),
        max_drawdown_pct=(train.max_drawdown_pct * train_weight) + (holdout.max_drawdown_pct * holdout_weight),
        sharpe=(train.sharpe * train_weight) + (holdout.sharpe * holdout_weight),
        sortino=(sum(sortino_values) / len(sortino_values)) if sortino_values else None,
        win_rate_pct=(train.win_rate_pct * train_weight) + (holdout.win_rate_pct * holdout_weight),
        trade_count=int((train.trade_count * train_weight) + (holdout.trade_count * holdout_weight)),
        rejection_reasons=list(dict.fromkeys(train.rejection_reasons + holdout.rejection_reasons)),
    )


def _metrics_to_dict(m: CandidateMetrics) -> dict[str, Any]:
    """Serialize metrics for diagnostics payloads."""
    return {
        "net_return_pct": m.net_return_pct,
        "max_drawdown_pct": m.max_drawdown_pct,
        "sharpe": m.sharpe,
        "sortino": m.sortino,
        "win_rate_pct": m.win_rate_pct,
        "trade_count": m.trade_count,
        "rejection_reasons": list(m.rejection_reasons),
    }

