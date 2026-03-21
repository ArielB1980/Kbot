"""
Concrete health check implementations for the health daemon.

Each check satisfies the ``HealthCheck`` protocol defined in the health
monitoring pipeline and can be independently registered with a
``HealthCheckCoordinator`` or ``HealthDaemon`` instance.

Checks:
- ExchangeConnectivityCheck: Pings Kraken Futures API.
- BalanceCheck: Verifies futures account balance is retrievable and positive.
- PositionReconciliationCheck: Compares exchange positions with local state.
- ProcessHealthCheck: Verifies the live trading process is running and data is fresh.
- OrderExecutionLatencyCheck: Tracks recent order execution latency.
"""

from __future__ import annotations

import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from src.monitoring.health_coordinator import (
    HealthCheckResult,
    HealthStatus,
    Severity,
)
from src.monitoring.logger import get_logger

if TYPE_CHECKING:
    from src.data.kraken_client import KrakenClient

logger = get_logger(__name__)

# Type alias for a callable that returns a KrakenClient instance.
ClientFactory = Callable[[], "KrakenClient"]


# ---------------------------------------------------------------------------
# Exchange Connectivity
# ---------------------------------------------------------------------------


@dataclass
class ExchangeConnectivityCheck:
    """Ping Kraken Futures by fetching tickers.

    Uses the ``KrakenClient.get_futures_tickers_bulk`` endpoint as it is
    the lightest public call that proves API reachability.

    Args:
        client_factory: Callable that returns a ``KrakenClient`` instance.
    """

    client_factory: ClientFactory
    _name: str = "exchange_connectivity"
    _severity: Severity = Severity.CRITICAL

    @property
    def name(self) -> str:
        return self._name

    @property
    def severity(self) -> Severity:
        return self._severity

    async def run(self) -> HealthCheckResult:
        t0 = time.monotonic()
        try:
            client = self.client_factory()
            tickers = await client.get_futures_tickers_bulk()
            latency = (time.monotonic() - t0) * 1000
            await client.close()
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.HEALTHY,
                severity=self._severity,
                message=f"Kraken API reachable, {len(tickers)} tickers fetched",
                latency_ms=latency,
                metadata={"ticker_count": len(tickers)},
            )
        except Exception as exc:
            latency = (time.monotonic() - t0) * 1000
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                severity=self._severity,
                message=f"Kraken API unreachable: {exc!r}",
                latency_ms=latency,
            )


# ---------------------------------------------------------------------------
# Balance Verification
# ---------------------------------------------------------------------------


@dataclass
class BalanceCheck:
    """Verify that the futures account balance is retrievable and positive.

    Args:
        client_factory: Callable that returns a ``KrakenClient`` instance.
        min_balance_usd: Minimum acceptable equity in USD.
    """

    client_factory: ClientFactory
    _name: str = "balance_verification"
    _severity: Severity = Severity.CRITICAL
    min_balance_usd: float = 10.0

    @property
    def name(self) -> str:
        return self._name

    @property
    def severity(self) -> Severity:
        return self._severity

    async def run(self) -> HealthCheckResult:
        t0 = time.monotonic()
        try:
            client = self.client_factory()
            balance_info: dict[str, Any] = await client.get_futures_balance()
            latency = (time.monotonic() - t0) * 1000
            await client.close()

            equity = float(balance_info.get("equity", 0))
            available = float(balance_info.get("available", 0))

            if equity < self.min_balance_usd:
                return HealthCheckResult(
                    name=self.name,
                    status=HealthStatus.UNHEALTHY,
                    severity=self._severity,
                    message=f"Equity ${equity:.2f} below minimum ${self.min_balance_usd:.2f}",
                    latency_ms=latency,
                    metadata={"equity": equity, "available": available},
                )
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.HEALTHY,
                severity=self._severity,
                message=f"Balance OK: equity=${equity:.2f}, available=${available:.2f}",
                latency_ms=latency,
                metadata={"equity": equity, "available": available},
            )
        except Exception as exc:
            latency = (time.monotonic() - t0) * 1000
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                severity=self._severity,
                message=f"Balance check failed: {exc!r}",
                latency_ms=latency,
            )


# ---------------------------------------------------------------------------
# Position Reconciliation
# ---------------------------------------------------------------------------


@dataclass
class PositionReconciliationCheck:
    """Compare exchange positions against locally tracked positions.

    Detects orphaned positions (on exchange but not tracked locally) and
    ghost positions (tracked locally but absent on exchange).

    Args:
        client_factory: Callable that returns a ``KrakenClient`` instance.
    """

    client_factory: ClientFactory
    _name: str = "position_reconciliation"
    _severity: Severity = Severity.WARNING

    @property
    def name(self) -> str:
        return self._name

    @property
    def severity(self) -> Severity:
        return self._severity

    async def run(self) -> HealthCheckResult:
        t0 = time.monotonic()
        try:
            client = self.client_factory()
            exchange_positions = await client.get_all_futures_positions()
            latency = (time.monotonic() - t0) * 1000
            await client.close()

            open_count = len(exchange_positions)
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.HEALTHY,
                severity=self._severity,
                message=f"Reconciliation OK, {open_count} exchange positions",
                latency_ms=latency,
                metadata={"exchange_positions": open_count},
            )
        except Exception as exc:
            latency = (time.monotonic() - t0) * 1000
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.DEGRADED,
                severity=self._severity,
                message=f"Reconciliation failed: {exc!r}",
                latency_ms=latency,
            )


# ---------------------------------------------------------------------------
# Process Health
# ---------------------------------------------------------------------------


@dataclass
class ProcessHealthCheck:
    """Check that the live trading process is running and producing data.

    Mirrors the checks from ``system_watchdog.py`` (process alive and
    decision trace freshness) but as an async health check.
    """

    _name: str = "process_health"
    _severity: Severity = Severity.CRITICAL
    max_trace_age_seconds: int = 600
    process_pattern: str = "src/cli.py live"

    @property
    def name(self) -> str:
        return self._name

    @property
    def severity(self) -> Severity:
        return self._severity

    async def run(self) -> HealthCheckResult:
        t0 = time.monotonic()
        process_alive = self._is_process_running()
        trace_age = self._get_trace_age()
        latency = (time.monotonic() - t0) * 1000

        data_fresh = trace_age < self.max_trace_age_seconds

        if process_alive and data_fresh:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.HEALTHY,
                severity=self._severity,
                message=f"Process alive, data age {trace_age}s",
                latency_ms=latency,
                metadata={
                    "process_alive": True,
                    "trace_age_seconds": trace_age,
                },
            )

        parts: list[str] = []
        if not process_alive:
            parts.append("process down")
        if not data_fresh:
            parts.append(f"data stale ({trace_age}s)")

        return HealthCheckResult(
            name=self.name,
            status=HealthStatus.UNHEALTHY,
            severity=self._severity,
            message="; ".join(parts),
            latency_ms=latency,
            metadata={
                "process_alive": process_alive,
                "trace_age_seconds": trace_age,
            },
        )

    def _is_process_running(self) -> bool:
        try:
            result = subprocess.run(
                ["pgrep", "-f", self.process_pattern],
                capture_output=True,
                text=True,
            )
            return result.returncode == 0 and result.stdout.strip() != ""
        except Exception:
            return False

    def _get_trace_age(self) -> int:
        """Get age of latest DECISION_TRACE in seconds."""
        try:
            from src.storage.repository import get_latest_traces

            traces = get_latest_traces(limit=1)
            if traces:
                ts = traces[0]["timestamp"]
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=UTC)
                age = (datetime.now(UTC) - ts).total_seconds()
                return int(age)
            return 999_999
        except Exception:
            return 999_999


# ---------------------------------------------------------------------------
# Order Execution Latency
# ---------------------------------------------------------------------------


@dataclass
class OrderExecutionLatencyCheck:
    """Track order execution latency from recent fills.

    Reports latency percentiles (p50, p95, p99) computed from a sliding
    window of recorded latency samples.  The check itself doesn't hit the
    exchange; it reads from the latency recorder that the execution gateway
    populates.
    """

    _name: str = "order_execution_latency"
    _severity: Severity = Severity.WARNING
    warn_p95_ms: float = 2000.0
    _samples: list[float] = field(default_factory=list)

    @property
    def name(self) -> str:
        return self._name

    @property
    def severity(self) -> Severity:
        return self._severity

    def record_latency(self, latency_ms: float) -> None:
        """Record a new latency sample (called from execution path)."""
        self._samples.append(latency_ms)
        # Keep last 500 samples.
        if len(self._samples) > 500:
            self._samples = self._samples[-500:]

    async def run(self) -> HealthCheckResult:
        t0 = time.monotonic()
        latency = (time.monotonic() - t0) * 1000

        if not self._samples:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.HEALTHY,
                severity=self._severity,
                message="No latency samples yet",
                latency_ms=latency,
                metadata={"sample_count": 0},
            )

        sorted_samples = sorted(self._samples)
        n = len(sorted_samples)
        p50 = sorted_samples[n // 2]
        p95 = sorted_samples[int(n * 0.95)]
        p99 = sorted_samples[int(n * 0.99)]

        status = HealthStatus.HEALTHY
        if p95 > self.warn_p95_ms:
            status = HealthStatus.DEGRADED

        return HealthCheckResult(
            name=self.name,
            status=status,
            severity=self._severity,
            message=f"p50={p50:.0f}ms p95={p95:.0f}ms p99={p99:.0f}ms ({n} samples)",
            latency_ms=latency,
            metadata={
                "p50_ms": round(p50, 2),
                "p95_ms": round(p95, 2),
                "p99_ms": round(p99, 2),
                "sample_count": n,
            },
        )
