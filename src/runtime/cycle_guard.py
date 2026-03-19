"""
CycleGuard: Guards against timing issues in the trading loop.

Prevents:
- Duplicate runs (same candle processed twice)
- Overlapping cycles (new cycle starts before previous ends)
- Partial data windows (stale or revised candles)
- Clock skew issues

Usage:
    guard = CycleGuard()
    
    # At start of each tick
    success, error = guard.start_cycle()
    if not success:
        logger.warning("Cycle skipped", reason=error)
        return
    
    try:
        # Process coins
        for symbol in symbols:
            if guard.is_candle_fresh(symbol, candle_timestamp):
                process_coin(symbol)
                guard.record_coin_processed()
    finally:
        guard.end_cycle()
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import uuid
import os

from src.monitoring.logger import get_logger

logger = get_logger(__name__)


@dataclass
class CycleState:
    """State of a single trading cycle."""
    cycle_id: str
    started_at: datetime
    expected_end: datetime
    coins_processed: int = 0
    signals_generated: int = 0
    orders_placed: int = 0
    orders_rejected: int = 0
    is_complete: bool = False
    overlapped_previous: bool = False
    error: Optional[str] = None
    
    def duration_seconds(self) -> float:
        """Get cycle duration in seconds."""
        end = datetime.now(timezone.utc) if not self.is_complete else self.expected_end
        return (end - self.started_at).total_seconds()


class CycleGuard:
    """
    Guards against timing issues in the trading loop.
    
    This class ensures:
    1. No overlapping cycles
    2. Minimum interval between cycles
    3. Maximum cycle duration enforcement
    4. Candle freshness validation
    5. Duplicate processing prevention
    """
    
    def __init__(
        self,
        min_cycle_interval_seconds: int = 60,
        max_cycle_duration_seconds: int = 300,
        max_candle_age_seconds: int = 120,
        max_clock_skew_seconds: int = 30,
    ):
        """
        Initialize CycleGuard.
        
        Args:
            min_cycle_interval_seconds: Minimum time between cycle starts
            max_cycle_duration_seconds: Maximum allowed cycle duration
            max_candle_age_seconds: Maximum age for a candle to be considered fresh
            max_clock_skew_seconds: Maximum allowed clock skew
        """
        self.min_interval = timedelta(seconds=min_cycle_interval_seconds)
        self.max_duration = timedelta(seconds=max_cycle_duration_seconds)
        self.max_candle_age = timedelta(seconds=max_candle_age_seconds)
        self.max_clock_skew = timedelta(seconds=max_clock_skew_seconds)
        
        self.current_cycle: Optional[CycleState] = None
        self.last_completed_cycle: Optional[CycleState] = None
        self.cycle_history: List[CycleState] = []
        
        # Candle deduplication: symbol -> last processed candle timestamp
        self._processed_candle_timestamps: Dict[str, datetime] = {}
        
        # Performance metrics
        self._total_cycles = 0
        self._overlapped_cycles = 0
        self._skipped_cycles = 0
        self._minimal_replay_logs = os.getenv("REPLAY_RESEARCH_MINIMAL_LOGS", "0") == "1"
        
        logger.info(
            "CycleGuard initialized",
            min_interval=min_cycle_interval_seconds,
            max_duration=max_cycle_duration_seconds,
            max_candle_age=max_candle_age_seconds,
        )
    
    def start_cycle(self) -> Tuple[bool, Optional[str]]:
        """
        Attempt to start a new trading cycle.
        
        Returns:
            (success, error_reason) - success is True if cycle started
        """
        now = datetime.now(timezone.utc)
        
        # Check for overlapping cycle
        if self.current_cycle and not self.current_cycle.is_complete:
            elapsed = now - self.current_cycle.started_at
            
            if elapsed < self.max_duration:
                # Previous cycle still running and not timed out
                self._skipped_cycles += 1
                return (False, f"OVERLAPPING_CYCLE: Previous cycle still running ({elapsed.total_seconds():.1f}s elapsed)")
            else:
                # Force-complete stale cycle
                logger.warning(
                    "CYCLE_FORCE_COMPLETE",
                    cycle_id=self.current_cycle.cycle_id,
                    elapsed_seconds=elapsed.total_seconds(),
                    max_duration=self.max_duration.total_seconds(),
                    coins_processed=self.current_cycle.coins_processed,
                )
                self.current_cycle.is_complete = True
                self.current_cycle.error = "TIMEOUT_FORCE_COMPLETED"
                self.last_completed_cycle = self.current_cycle
                self._overlapped_cycles += 1
        
        # Check minimum interval since last cycle
        if self.last_completed_cycle:
            since_last = now - self.last_completed_cycle.started_at
            if since_last < self.min_interval:
                self._skipped_cycles += 1
                return (False, f"TOO_SOON: Only {since_last.total_seconds():.1f}s since last cycle (min: {self.min_interval.total_seconds()}s)")
        
        # Start new cycle
        cycle_id = f"cycle_{now.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        self.current_cycle = CycleState(
            cycle_id=cycle_id,
            started_at=now,
            expected_end=now + self.max_duration,
            overlapped_previous=(self._overlapped_cycles > 0),
        )
        
        self._total_cycles += 1
        
        if (not self._minimal_replay_logs) or (self._total_cycles % 100 == 0):
            logger.info(
                "CYCLE_START",
                cycle_id=cycle_id,
                total_cycles=self._total_cycles,
            )
        
        return (True, None)
    
    def end_cycle(self) -> CycleState:
        """
        End the current trading cycle.
        
        Returns:
            The completed cycle state
        """
        if not self.current_cycle:
            raise RuntimeError("No cycle to end - start_cycle() must be called first")
        
        now = datetime.now(timezone.utc)
        self.current_cycle.is_complete = True
        self.current_cycle.expected_end = now
        self.last_completed_cycle = self.current_cycle
        
        # Add to history (keep last 100)
        self.cycle_history.append(self.current_cycle)
        if len(self.cycle_history) > 100:
            self.cycle_history = self.cycle_history[-100:]
        
        elapsed = now - self.current_cycle.started_at
        
        if (not self._minimal_replay_logs) or (self._total_cycles % 100 == 0):
            logger.info(
                "CYCLE_END",
                cycle_id=self.current_cycle.cycle_id,
                duration_seconds=round(elapsed.total_seconds(), 2),
                coins_processed=self.current_cycle.coins_processed,
                signals_generated=self.current_cycle.signals_generated,
                orders_placed=self.current_cycle.orders_placed,
                orders_rejected=self.current_cycle.orders_rejected,
            )
        
        completed = self.current_cycle
        self.current_cycle = None
        return completed
    
    def is_candle_fresh(
        self,
        symbol: str,
        candle_timestamp: datetime,
        max_age_override: Optional[int] = None,
    ) -> bool:
        """
        Check if a candle is fresh enough for decision making.
        
        Guards against:
        - Stale candles from API lag
        - Revised/historical candles
        - Clock skew issues
        - Duplicate processing of same candle
        
        Args:
            symbol: Trading symbol
            candle_timestamp: Timestamp of the candle (close time)
            max_age_override: Optional override for max age in seconds
            
        Returns:
            True if candle is fresh and should be processed
        """
        now = datetime.now(timezone.utc)
        max_age = timedelta(seconds=max_age_override) if max_age_override else self.max_candle_age
        
        age = now - candle_timestamp
        
        # Check for stale candle
        if age > max_age:
            logger.debug(
                "STALE_CANDLE",
                symbol=symbol,
                candle_timestamp=candle_timestamp.isoformat(),
                age_seconds=age.total_seconds(),
                max_age_seconds=max_age.total_seconds(),
            )
            return False
        
        # Check for future candle (clock skew)
        if age < -self.max_clock_skew:
            logger.warning(
                "FUTURE_CANDLE",
                symbol=symbol,
                candle_timestamp=candle_timestamp.isoformat(),
                skew_seconds=abs(age.total_seconds()),
                max_skew=self.max_clock_skew.total_seconds(),
            )
            return False
        
        # Check for duplicate processing
        last_processed = self._processed_candle_timestamps.get(symbol)
        if last_processed and candle_timestamp <= last_processed:
            logger.debug(
                "DUPLICATE_CANDLE",
                symbol=symbol,
                candle_timestamp=candle_timestamp.isoformat(),
                last_processed=last_processed.isoformat(),
            )
            return False
        
        # Update last processed
        self._processed_candle_timestamps[symbol] = candle_timestamp
        return True
    
    def record_coin_processed(self):
        """Record that a coin was processed this cycle."""
        if self.current_cycle:
            self.current_cycle.coins_processed += 1
    
    def record_signal_generated(self):
        """Record that a signal was generated this cycle."""
        if self.current_cycle:
            self.current_cycle.signals_generated += 1
    
    def record_order_placed(self):
        """Record that an order was placed this cycle."""
        if self.current_cycle:
            self.current_cycle.orders_placed += 1
    
    def record_order_rejected(self):
        """Record that an order was rejected this cycle."""
        if self.current_cycle:
            self.current_cycle.orders_rejected += 1
    
    def get_cycle_stats(self) -> Dict:
        """Get current cycle statistics."""
        return {
            "current_cycle_id": self.current_cycle.cycle_id if self.current_cycle else None,
            "current_cycle_running": self.current_cycle is not None and not self.current_cycle.is_complete,
            "current_cycle_elapsed": self.current_cycle.duration_seconds() if self.current_cycle else 0,
            "total_cycles": self._total_cycles,
            "overlapped_cycles": self._overlapped_cycles,
            "skipped_cycles": self._skipped_cycles,
            "last_completed_at": self.last_completed_cycle.expected_end.isoformat() if self.last_completed_cycle else None,
        }
    
    def get_recent_cycles(self, limit: int = 10) -> List[Dict]:
        """Get recent cycle history for debugging."""
        return [
            {
                "cycle_id": c.cycle_id,
                "started_at": c.started_at.isoformat(),
                "duration_seconds": c.duration_seconds(),
                "coins_processed": c.coins_processed,
                "signals_generated": c.signals_generated,
                "orders_placed": c.orders_placed,
                "orders_rejected": c.orders_rejected,
                "error": c.error,
            }
            for c in self.cycle_history[-limit:]
        ]
    
    def clear_candle_history(self, older_than_hours: int = 24):
        """
        Clear old candle timestamps to prevent memory growth.
        
        Args:
            older_than_hours: Clear timestamps older than this
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
        old_count = len(self._processed_candle_timestamps)
        
        self._processed_candle_timestamps = {
            symbol: ts
            for symbol, ts in self._processed_candle_timestamps.items()
            if ts > cutoff
        }
        
        new_count = len(self._processed_candle_timestamps)
        if old_count != new_count:
            logger.debug(
                "Cleared old candle timestamps",
                removed=old_count - new_count,
                remaining=new_count,
            )


# ===== GLOBAL SINGLETON =====
_cycle_guard: Optional[CycleGuard] = None


def get_cycle_guard() -> CycleGuard:
    """Get global cycle guard instance."""
    global _cycle_guard
    if _cycle_guard is None:
        _cycle_guard = CycleGuard()
    return _cycle_guard


def init_cycle_guard(
    min_cycle_interval_seconds: int = 60,
    max_cycle_duration_seconds: int = 300,
    max_candle_age_seconds: int = 120,
    max_clock_skew_seconds: int = 30,
) -> CycleGuard:
    """Initialize global cycle guard with custom settings."""
    global _cycle_guard
    _cycle_guard = CycleGuard(
        min_cycle_interval_seconds=min_cycle_interval_seconds,
        max_cycle_duration_seconds=max_cycle_duration_seconds,
        max_candle_age_seconds=max_candle_age_seconds,
        max_clock_skew_seconds=max_clock_skew_seconds,
    )
    return _cycle_guard
