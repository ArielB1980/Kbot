"""
Tests for the health monitoring pipeline.

Covers:
- HealthDaemon registration, scheduling, and result tracking
- HealthDashboard snapshot and error rate computation
- Individual health check result generation
- Alert dispatch on non-healthy results
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from unittest.mock import patch

import pytest

from src.monitoring.health_coordinator import HealthCheckCoordinator
from src.monitoring.health_daemon import (
    HealthCheckResult,
    HealthDaemon,
    HealthStatus,
    Severity,
)
from src.monitoring.health_dashboard import HealthDashboard

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class _StubCheck:
    """Minimal check that returns a canned result."""

    _name: str
    _severity: Severity = Severity.WARNING
    _status: HealthStatus = HealthStatus.HEALTHY
    _message: str = "ok"
    _latency_ms: float = 1.0
    _metadata: dict | None = None
    call_count: int = 0

    @property
    def name(self) -> str:
        return self._name

    @property
    def severity(self) -> Severity:
        return self._severity

    async def run(self) -> HealthCheckResult:
        self.call_count += 1
        return HealthCheckResult(
            name=self._name,
            status=self._status,
            severity=self._severity,
            message=self._message,
            latency_ms=self._latency_ms,
            metadata=self._metadata or {},
        )


@dataclass
class _FailingCheck:
    """Check that raises an exception."""

    _name: str = "boom"
    _severity: Severity = Severity.CRITICAL

    @property
    def name(self) -> str:
        return self._name

    @property
    def severity(self) -> Severity:
        return self._severity

    async def run(self) -> HealthCheckResult:
        raise RuntimeError("kaboom")


# ---------------------------------------------------------------------------
# HealthDaemon tests
# ---------------------------------------------------------------------------


class TestHealthDaemon:
    def test_register_and_list(self):
        daemon = HealthDaemon()
        check = _StubCheck(_name="test_check")
        daemon.register(check, interval_seconds=10)
        assert "test_check" in daemon._checks

    def test_latest_result_empty(self):
        daemon = HealthDaemon()
        assert daemon.latest_result("missing") is None

    @pytest.mark.asyncio
    async def test_tick_runs_due_checks(self):
        daemon = HealthDaemon(default_interval=0)
        check = _StubCheck(_name="fast")
        daemon.register(check, interval_seconds=0)
        await daemon._tick()
        assert check.call_count == 1
        result = daemon.latest_result("fast")
        assert result is not None
        assert result.status == HealthStatus.HEALTHY

    @pytest.mark.asyncio
    async def test_tick_skips_non_due_checks(self):
        daemon = HealthDaemon(default_interval=9999)
        check = _StubCheck(_name="slow")
        daemon.register(check, interval_seconds=9999)
        # Simulate that it just ran.
        daemon._checks["slow"].last_run = 1e18
        await daemon._tick()
        assert check.call_count == 0

    @pytest.mark.asyncio
    async def test_alert_callback_on_unhealthy(self):
        alerts: list[HealthCheckResult] = []
        daemon = HealthDaemon(default_interval=0, alert_callback=alerts.append)
        check = _StubCheck(
            _name="bad",
            _status=HealthStatus.UNHEALTHY,
            _severity=Severity.CRITICAL,
            _message="fail",
        )
        daemon.register(check, interval_seconds=0)
        await daemon._tick()
        assert len(alerts) == 1
        assert alerts[0].status == HealthStatus.UNHEALTHY

    @pytest.mark.asyncio
    async def test_no_alert_on_healthy(self):
        alerts: list[HealthCheckResult] = []
        daemon = HealthDaemon(default_interval=0, alert_callback=alerts.append)
        check = _StubCheck(_name="good")
        daemon.register(check, interval_seconds=0)
        await daemon._tick()
        assert len(alerts) == 0

    @pytest.mark.asyncio
    async def test_failing_check_produces_unhealthy_result(self):
        alerts: list[HealthCheckResult] = []
        daemon = HealthDaemon(default_interval=0, alert_callback=alerts.append)
        check = _FailingCheck()
        daemon.register(check, interval_seconds=0)
        await daemon._tick()
        result = daemon.latest_result("boom")
        assert result is not None
        assert result.status == HealthStatus.UNHEALTHY
        assert "kaboom" in result.message
        assert len(alerts) == 1

    def test_overall_status_healthy(self):
        daemon = HealthDaemon()
        check_a = _StubCheck(_name="a")
        check_b = _StubCheck(_name="b")
        daemon.register(check_a)
        daemon.register(check_b)
        # Manually populate history.
        daemon._checks["a"].history.append(
            HealthCheckResult(
                name="a",
                status=HealthStatus.HEALTHY,
                severity=Severity.INFO,
                message="ok",
                latency_ms=1,
            )
        )
        daemon._checks["b"].history.append(
            HealthCheckResult(
                name="b",
                status=HealthStatus.HEALTHY,
                severity=Severity.INFO,
                message="ok",
                latency_ms=1,
            )
        )
        assert daemon.overall_status() == HealthStatus.HEALTHY

    def test_overall_status_worst_wins(self):
        daemon = HealthDaemon()
        check_a = _StubCheck(_name="a")
        check_b = _StubCheck(_name="b")
        daemon.register(check_a)
        daemon.register(check_b)
        daemon._checks["a"].history.append(
            HealthCheckResult(
                name="a",
                status=HealthStatus.HEALTHY,
                severity=Severity.INFO,
                message="ok",
                latency_ms=1,
            )
        )
        daemon._checks["b"].history.append(
            HealthCheckResult(
                name="b",
                status=HealthStatus.UNHEALTHY,
                severity=Severity.CRITICAL,
                message="bad",
                latency_ms=1,
            )
        )
        assert daemon.overall_status() == HealthStatus.UNHEALTHY

    def test_summary_structure(self):
        daemon = HealthDaemon()
        check = _StubCheck(_name="s")
        daemon.register(check)
        daemon._checks["s"].history.append(
            HealthCheckResult(
                name="s",
                status=HealthStatus.DEGRADED,
                severity=Severity.WARNING,
                message="slow",
                latency_ms=42.5,
            )
        )
        summary = daemon.summary()
        assert summary["overall_status"] == "degraded"
        assert "s" in summary["checks"]
        assert summary["checks"]["s"]["latency_ms"] == 42.5

    def test_history_returns_list(self):
        daemon = HealthDaemon()
        check = _StubCheck(_name="h")
        daemon.register(check)
        r1 = HealthCheckResult(
            name="h", status=HealthStatus.HEALTHY, severity=Severity.INFO, message="1", latency_ms=1
        )
        r2 = HealthCheckResult(
            name="h",
            status=HealthStatus.DEGRADED,
            severity=Severity.WARNING,
            message="2",
            latency_ms=2,
        )
        daemon._checks["h"].history.append(r1)
        daemon._checks["h"].history.append(r2)
        hist = daemon.history("h")
        assert len(hist) == 2
        assert hist[0].message == "1"
        assert hist[1].message == "2"

    @pytest.mark.asyncio
    async def test_start_and_stop(self):
        daemon = HealthDaemon(default_interval=0)
        check = _StubCheck(_name="loop")
        daemon.register(check, interval_seconds=0)

        async def _stop_after():
            await asyncio.sleep(0.1)
            daemon.stop()

        task = asyncio.create_task(daemon.start())
        stop_task = asyncio.create_task(_stop_after())
        await asyncio.gather(task, stop_task)
        assert check.call_count >= 1
        assert daemon.uptime_seconds > 0


# ---------------------------------------------------------------------------
# HealthDashboard tests
# ---------------------------------------------------------------------------


class TestHealthDashboard:
    def _make_daemon_with_results(self) -> HealthDaemon:
        daemon = HealthDaemon()
        check = _StubCheck(_name="order_execution_latency")
        daemon.register(check)
        daemon._checks["order_execution_latency"].history.append(
            HealthCheckResult(
                name="order_execution_latency",
                status=HealthStatus.HEALTHY,
                severity=Severity.WARNING,
                message="p50=100ms",
                latency_ms=1,
                metadata={"p50_ms": 100.0, "p95_ms": 200.0, "p99_ms": 350.0},
            )
        )
        return daemon

    def test_latency_percentiles(self):
        daemon = self._make_daemon_with_results()
        dashboard = HealthDashboard(daemon)
        p = dashboard.latency_percentiles()
        assert p["p50_ms"] == 100.0
        assert p["p95_ms"] == 200.0
        assert p["p99_ms"] == 350.0

    def test_latency_percentiles_missing(self):
        daemon = HealthDaemon()
        dashboard = HealthDashboard(daemon)
        p = dashboard.latency_percentiles()
        assert p["p50_ms"] is None

    def test_error_rate_all_healthy(self):
        daemon = HealthDaemon()
        check = _StubCheck(_name="x")
        daemon.register(check)
        for _ in range(5):
            daemon._checks["x"].history.append(
                HealthCheckResult(
                    name="x",
                    status=HealthStatus.HEALTHY,
                    severity=Severity.INFO,
                    message="ok",
                    latency_ms=1,
                )
            )
        dashboard = HealthDashboard(daemon)
        rates = dashboard.error_rate()
        assert rates["x"] == 0.0

    def test_error_rate_mixed(self):
        daemon = HealthDaemon()
        check = _StubCheck(_name="y")
        daemon.register(check)
        for i in range(10):
            status = HealthStatus.UNHEALTHY if i < 3 else HealthStatus.HEALTHY
            daemon._checks["y"].history.append(
                HealthCheckResult(
                    name="y",
                    status=status,
                    severity=Severity.WARNING,
                    message="m",
                    latency_ms=1,
                )
            )
        dashboard = HealthDashboard(daemon)
        rates = dashboard.error_rate()
        assert rates["y"] == 0.3

    @patch("src.monitoring.health_dashboard.get_latest_metrics_snapshot", create=True)
    def test_snapshot_structure(self, mock_snap):
        daemon = self._make_daemon_with_results()
        daemon._start_time = 0.0
        dashboard = HealthDashboard(daemon)
        snap = dashboard.snapshot()
        assert "overall_status" in snap
        assert "latency_percentiles" in snap
        assert "error_rate" in snap
        assert "system_state" in snap


# ---------------------------------------------------------------------------
# OrderExecutionLatencyCheck tests
# ---------------------------------------------------------------------------


class TestOrderExecutionLatencyCheck:
    @pytest.mark.asyncio
    async def test_no_samples(self):
        from src.monitoring.health_checks_impl import OrderExecutionLatencyCheck

        check = OrderExecutionLatencyCheck()
        result = await check.run()
        assert result.status == HealthStatus.HEALTHY
        assert result.metadata["sample_count"] == 0

    @pytest.mark.asyncio
    async def test_with_samples(self):
        from src.monitoring.health_checks_impl import OrderExecutionLatencyCheck

        check = OrderExecutionLatencyCheck()
        for v in [100, 200, 300, 400, 500]:
            check.record_latency(float(v))
        result = await check.run()
        assert result.status == HealthStatus.HEALTHY
        assert result.metadata["p50_ms"] is not None
        assert result.metadata["sample_count"] == 5

    @pytest.mark.asyncio
    async def test_high_latency_degraded(self):
        from src.monitoring.health_checks_impl import OrderExecutionLatencyCheck

        check = OrderExecutionLatencyCheck(warn_p95_ms=100.0)
        for v in [50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 5000]:
            check.record_latency(float(v))
        result = await check.run()
        assert result.status == HealthStatus.DEGRADED

    @pytest.mark.asyncio
    async def test_record_latency_capping(self):
        from src.monitoring.health_checks_impl import OrderExecutionLatencyCheck

        check = OrderExecutionLatencyCheck()
        for i in range(600):
            check.record_latency(float(i))
        assert len(check._samples) == 500


# ---------------------------------------------------------------------------
# ProcessHealthCheck tests
# ---------------------------------------------------------------------------


class TestProcessHealthCheck:
    @pytest.mark.asyncio
    async def test_healthy_when_process_alive_and_fresh(self):
        from src.monitoring.health_checks_impl import ProcessHealthCheck

        check = ProcessHealthCheck()
        with (
            patch.object(check, "_is_process_running", return_value=True),
            patch.object(check, "_get_trace_age", return_value=60),
        ):
            result = await check.run()
        assert result.status == HealthStatus.HEALTHY

    @pytest.mark.asyncio
    async def test_unhealthy_when_process_down(self):
        from src.monitoring.health_checks_impl import ProcessHealthCheck

        check = ProcessHealthCheck()
        with (
            patch.object(check, "_is_process_running", return_value=False),
            patch.object(check, "_get_trace_age", return_value=60),
        ):
            result = await check.run()
        assert result.status == HealthStatus.UNHEALTHY
        assert "process down" in result.message

    @pytest.mark.asyncio
    async def test_unhealthy_when_data_stale(self):
        from src.monitoring.health_checks_impl import ProcessHealthCheck

        check = ProcessHealthCheck()
        with (
            patch.object(check, "_is_process_running", return_value=True),
            patch.object(check, "_get_trace_age", return_value=9999),
        ):
            result = await check.run()
        assert result.status == HealthStatus.UNHEALTHY
        assert "data stale" in result.message


# ---------------------------------------------------------------------------
# HealthCheckCoordinator tests
# ---------------------------------------------------------------------------


class TestHealthCheckCoordinator:
    def test_register_and_query(self):
        coord = HealthCheckCoordinator()
        check = _StubCheck(_name="coord_test")
        coord.register(check, interval_seconds=10)
        assert coord.latest_result("coord_test") is None

    @pytest.mark.asyncio
    async def test_tick_via_coordinator(self):
        coord = HealthCheckCoordinator(default_interval=0)
        check = _StubCheck(_name="via_coord")
        coord.register(check, interval_seconds=0)
        await coord.daemon._tick()
        result = coord.latest_result("via_coord")
        assert result is not None
        assert result.status == HealthStatus.HEALTHY

    def test_overall_status_delegates(self):
        coord = HealthCheckCoordinator()
        check = _StubCheck(_name="c")
        coord.register(check)
        coord.daemon._checks["c"].history.append(
            HealthCheckResult(
                name="c",
                status=HealthStatus.DEGRADED,
                severity=Severity.WARNING,
                message="slow",
                latency_ms=1,
            )
        )
        assert coord.overall_status() == HealthStatus.DEGRADED

    @patch("src.monitoring.health_dashboard.get_latest_metrics_snapshot", create=True)
    def test_snapshot_delegates_to_dashboard(self, mock_snap):
        coord = HealthCheckCoordinator()
        check = _StubCheck(_name="order_execution_latency")
        coord.register(check)
        coord.daemon._checks["order_execution_latency"].history.append(
            HealthCheckResult(
                name="order_execution_latency",
                status=HealthStatus.HEALTHY,
                severity=Severity.WARNING,
                message="ok",
                latency_ms=1,
                metadata={"p50_ms": 50.0, "p95_ms": 100.0, "p99_ms": 200.0},
            )
        )
        snap = coord.snapshot()
        assert "overall_status" in snap
        assert "latency_percentiles" in snap
        assert snap["latency_percentiles"]["p50_ms"] == 50.0

    def test_error_rate_delegates(self):
        coord = HealthCheckCoordinator()
        check = _StubCheck(_name="e")
        coord.register(check)
        for _ in range(4):
            coord.daemon._checks["e"].history.append(
                HealthCheckResult(
                    name="e",
                    status=HealthStatus.HEALTHY,
                    severity=Severity.INFO,
                    message="ok",
                    latency_ms=1,
                )
            )
        coord.daemon._checks["e"].history.append(
            HealthCheckResult(
                name="e",
                status=HealthStatus.UNHEALTHY,
                severity=Severity.CRITICAL,
                message="bad",
                latency_ms=1,
            )
        )
        rates = coord.error_rate()
        assert rates["e"] == 0.2

    def test_alert_callback_wired(self):
        alerts: list[HealthCheckResult] = []
        coord = HealthCheckCoordinator(default_interval=0, alert_callback=alerts.append)
        assert coord.daemon._alert_callback is not None

    @pytest.mark.asyncio
    async def test_start_and_stop(self):
        coord = HealthCheckCoordinator(default_interval=0)
        check = _StubCheck(_name="coord_loop")
        coord.register(check, interval_seconds=0)

        async def _stop_after():
            await asyncio.sleep(0.1)
            coord.stop()

        task = asyncio.create_task(coord.start())
        stop_task = asyncio.create_task(_stop_after())
        await asyncio.gather(task, stop_task)
        assert check.call_count >= 1
        assert coord.uptime_seconds > 0
