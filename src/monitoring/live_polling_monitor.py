"""
LivePollingMonitor — unified entry point for live trading background monitors.

Wraps the standalone async functions from ``src.live.health_monitor`` behind
a single class that manages lifecycle (start/stop) and provides a consistent
interface for ``LiveTrading`` to interact with.

This is a pure structural refactoring: all runtime behavior is delegated to
the original functions in ``src.live.health_monitor``.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from src.monitoring.logger import get_logger

if TYPE_CHECKING:
    from src.live.live_trading import LiveTrading

logger = get_logger(__name__)

__all__ = ["LivePollingMonitor"]


class LivePollingMonitor:
    """Unified facade for live trading background monitoring tasks.

    Lifecycle:
        1. Instantiate with a ``LiveTrading`` reference.
        2. Call individual ``run_*`` coroutines as ``asyncio.create_task()``.
        3. Call ``stop()`` to cancel all managed tasks on shutdown.

    Each ``run_*`` method delegates to the corresponding free function in
    ``src.live.health_monitor``, preserving existing behavior exactly.
    """

    def __init__(self, lt: LiveTrading) -> None:
        self._lt = lt
        self._tasks: list[asyncio.Task[None]] = []
        logger.info("LivePollingMonitor initialized")

    # ------------------------------------------------------------------
    # Task management
    # ------------------------------------------------------------------

    def spawn(self, coro: object) -> asyncio.Task[None]:
        """Create and track an asyncio task for a monitor coroutine.

        Args:
            coro: An awaitable returned by one of the ``run_*`` methods.

        Returns:
            The created ``asyncio.Task``.
        """
        task: asyncio.Task[None] = asyncio.create_task(coro)  # type: ignore[arg-type]
        self._tasks.append(task)
        return task

    async def stop(self) -> None:
        """Cancel all managed tasks and wait for them to finish."""
        for task in self._tasks:
            if not task.done():
                task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        logger.info("LivePollingMonitor stopped", tasks_cancelled=len(self._tasks))

    @property
    def active_task_count(self) -> int:
        """Number of tasks still running."""
        return sum(1 for t in self._tasks if not t.done())

    # ------------------------------------------------------------------
    # Monitor coroutines (delegate to src.live.health_monitor)
    # ------------------------------------------------------------------

    async def run_order_polling(self, interval_seconds: int = 12) -> None:
        """Poll pending entry order status, process fills."""
        from src.live.health_monitor import run_order_polling

        await run_order_polling(self._lt, interval_seconds)

    async def run_protection_checks(self, interval_seconds: int = 30) -> None:
        """V2 protection monitor loop with escalation policy."""
        from src.live.health_monitor import run_protection_checks

        await run_protection_checks(self._lt, interval_seconds)

    async def run_trade_starvation_monitor(self, interval_seconds: int = 300) -> None:
        """Alert if signals generated but 0 orders placed."""
        from src.live.health_monitor import run_trade_starvation_monitor

        await run_trade_starvation_monitor(self._lt, interval_seconds)

    async def run_winner_churn_monitor(self, interval_seconds: int = 300) -> None:
        """Alert if same symbol wins auction repeatedly without entry."""
        from src.live.health_monitor import run_winner_churn_monitor

        await run_winner_churn_monitor(self._lt, interval_seconds)

    async def run_trade_recording_monitor(self, interval_seconds: int = 300) -> None:
        """Advisory monitor: positions closing but 0 trades recorded."""
        from src.live.health_monitor import run_trade_recording_monitor

        await run_trade_recording_monitor(self._lt, interval_seconds)

    async def run_daily_summary(self) -> None:
        """Send daily P&L summary via Telegram at midnight UTC."""
        from src.live.health_monitor import run_daily_summary

        await run_daily_summary(self._lt)

    async def get_system_status(self) -> dict:
        """Data provider for Telegram commands."""
        from src.live.health_monitor import get_system_status

        return await get_system_status(self._lt)

    async def validate_position_protection(self) -> None:
        """Startup safety gate: validate all positions have stops."""
        from src.live.health_monitor import validate_position_protection

        await validate_position_protection(self._lt)

    async def try_auto_recovery(self) -> bool:
        """Attempt auto-recovery from kill switch (margin_critical only)."""
        from src.live.health_monitor import try_auto_recovery

        return await try_auto_recovery(self._lt)
