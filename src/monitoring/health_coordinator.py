"""
Unified entry point for the health monitoring pipeline.

``HealthCheckCoordinator`` owns the ``HealthDaemon`` (scheduling) and
``HealthDashboard`` (query layer), providing a single import for consumers
that need to register checks, start monitoring, and query results.

Usage::

    coordinator = HealthCheckCoordinator()
    coordinator.register(ExchangeConnectivityCheck(...), interval_seconds=60)
    coordinator.register(BalanceCheck(...), interval_seconds=120)
    await coordinator.start()

    # Query
    snapshot = coordinator.snapshot()
    status = coordinator.overall_status()
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

from src.monitoring.health_daemon import (
    HealthCheck,
    HealthCheckResult,
    HealthDaemon,
    HealthStatus,
    Severity,
)
from src.monitoring.health_dashboard import HealthDashboard
from src.monitoring.logger import get_logger

logger = get_logger(__name__)

__all__ = [
    "HealthCheckCoordinator",
    "HealthCheck",
    "HealthCheckResult",
    "HealthDaemon",
    "HealthDashboard",
    "HealthStatus",
    "Severity",
]


class HealthCheckCoordinator:
    """Single entry point for health monitoring.

    Combines check scheduling (``HealthDaemon``) and dashboard queries
    (``HealthDashboard``) behind one interface. Consumers register checks
    through the coordinator and query results from it.

    Args:
        default_interval: Default seconds between check runs.
        alert_callback: Optional ``(result) -> None`` invoked on
            non-healthy results.
    """

    def __init__(
        self,
        default_interval: float = 30.0,
        alert_callback: Callable[[HealthCheckResult], None] | None = None,
    ) -> None:
        self._daemon = HealthDaemon(
            default_interval=default_interval,
            alert_callback=alert_callback,
        )
        self._dashboard = HealthDashboard(self._daemon)
        logger.info("HealthCheckCoordinator initialized", default_interval=default_interval)

    # -- registration -------------------------------------------------------

    def register(
        self,
        check: HealthCheck,
        interval_seconds: float | None = None,
    ) -> None:
        """Register a health check with an optional per-check interval.

        Args:
            check: An object satisfying the ``HealthCheck`` protocol.
            interval_seconds: Override the coordinator-wide default interval.
        """
        self._daemon.register(check, interval_seconds=interval_seconds)

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        """Start the daemon loop. Blocks until ``stop()`` is called."""
        await self._daemon.start()

    def stop(self) -> None:
        """Signal the daemon to exit its loop."""
        self._daemon.stop()

    @property
    def uptime_seconds(self) -> float:
        """Seconds since the coordinator was started."""
        return self._daemon.uptime_seconds

    # -- query interface (delegates to daemon) ------------------------------

    def latest_result(self, name: str) -> HealthCheckResult | None:
        """Return the most recent result for *name*, or *None*."""
        return self._daemon.latest_result(name)

    def latest_results(self) -> dict[str, HealthCheckResult]:
        """Return the most recent result for every registered check."""
        return self._daemon.latest_results()

    def history(self, name: str) -> Sequence[HealthCheckResult]:
        """Return the result history for *name* (newest last)."""
        return self._daemon.history(name)

    def overall_status(self) -> HealthStatus:
        """Aggregate status: worst status across all latest results."""
        return self._daemon.overall_status()

    def summary(self) -> dict[str, object]:
        """Dashboard-friendly summary dict from the daemon."""
        return self._daemon.summary()

    # -- dashboard queries --------------------------------------------------

    def snapshot(self) -> dict[str, object]:
        """Full dashboard snapshot including latency, error rates, system state."""
        return self._dashboard.snapshot()

    def latency_percentiles(
        self, check_name: str = "order_execution_latency"
    ) -> dict[str, float | None]:
        """Return p50/p95/p99 from the latency check metadata."""
        return self._dashboard.latency_percentiles(check_name)

    def error_rate(self, window: int = 50) -> dict[str, float]:
        """Per-check error rate over the last *window* results."""
        return self._dashboard.error_rate(window)

    # -- internal access (for advanced use) ---------------------------------

    @property
    def daemon(self) -> HealthDaemon:
        """Access the underlying daemon (for direct manipulation)."""
        return self._daemon

    @property
    def dashboard(self) -> HealthDashboard:
        """Access the underlying dashboard."""
        return self._dashboard
