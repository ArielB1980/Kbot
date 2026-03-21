"""
Kill switch with latching emergency stop.

Once triggered, system cannot auto-resume - manual acknowledgment required.
"""
import os
from enum import Enum
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from src.monitoring.logger import get_logger
from src.monitoring.alert_dispatcher import send_alert_sync
from src.exceptions import OperationalError, DataError, InvariantError

logger = get_logger(__name__)

# Deterministic state path: data/ under repo root, or env override (e.g. for systemd/Docker).
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_DATA_DIR = _REPO_ROOT / "data"
_DEFAULT_STATE_FILE = _DATA_DIR / "kill_switch_state.json"


def _kill_switch_state_path() -> Path:
    """State file path: KILL_SWITCH_STATE_PATH env, or data/kill_switch_state.json under repo root."""
    env_path = os.environ.get("KILL_SWITCH_STATE_PATH")
    if env_path:
        return Path(env_path)
    return _DEFAULT_STATE_FILE


class KillSwitchReason(str, Enum):
    """Reasons for kill switch activation.
    
    Categories (used by startup behavior logic):
    - EMERGENCY_RUNTIME: Invariant K broken, naked positions, reconciliation failure.
      On startup: allow emergency actions to finish IF recent (< 2 min).
    - MARGIN_CRITICAL / LIQUIDATION_BREACH: Risk pressure.
      On startup: enter SAFE_HOLD (cancel non-SL orders, verify stops, do NOT flatten).
    - OPERATIONAL_HALT: Manual halt, config mismatch, drawdown latch, data issues.
      On startup: enter SAFE_HOLD (cancel non-SL orders, verify stops, do NOT flatten).
    """
    MANUAL = "manual"
    API_ERROR = "api_error"
    MARGIN_CRITICAL = "margin_critical"
    LIQUIDATION_BREACH = "liquidation_breach"
    DATA_FAILURE = "data_failure"
    RECONCILIATION_FAILURE = "reconciliation_failure"

    @property
    def is_emergency_runtime(self) -> bool:
        """True if this reason is an emergency runtime failure that may need immediate action."""
        return self in (
            KillSwitchReason.RECONCILIATION_FAILURE,
            KillSwitchReason.LIQUIDATION_BREACH,
        )

    @property
    def allows_auto_flatten_on_startup(self) -> bool:
        """True only if this reason justifies auto-flattening positions on startup.
        
        CRITICAL: Most reasons should NOT auto-flatten. The safest posture on
        restart is 'freeze + preserve stops', not 'panic flatten'.
        Only recent emergency runtime failures may auto-flatten.
        """
        return self.is_emergency_runtime


class KillSwitch:
    """
    Latched emergency kill switch.

    Design: Once activated, requires manual acknowledgment to restart.
    Prevents oscillation. State persists across restarts.
    """

    def __init__(self, client=None):
        """
        Initialize kill switch.

        Args:
            client: KrakenClient instance (optional, for executing actions)
        """
        self.active = False
        self.latched = False
        self.reason: Optional[KillSwitchReason] = None
        self.activated_at: Optional[datetime] = None
        self.client = client

        # Load persisted state
        self._load_state()

        logger.info("Kill Switch initialized")
    
    def set_client(self, client):
        """Set the KrakenClient instance."""
        self.client = client

    def activate_sync(self, reason: KillSwitchReason):
        """
        Synchronously activate kill switch (for CLI use).
        Does not execute cancel/close actions (requires async client).

        Args:
            reason: Reason for activation
        """
        if not self.active:
            self.active = True
            self.latched = True
            self.reason = reason
            self.activated_at = datetime.now(timezone.utc)

            # Persist state immediately
            self._save_state()

            logger.critical(
                "🛑 KILL SWITCH ACTIVATED (sync)",
                reason=reason.value,
                timestamp=self.activated_at.isoformat(),
            )

            logger.critical(
                "Manual acknowledgment required to restart trading"
            )

    async def activate(self, reason: KillSwitchReason, emergency: bool = False):
        """
        Activate kill switch.
        
        Args:
            reason: Reason for activation
            emergency: If True, triggers emergency mode (flatten all positions)
        """
        if not self.active:
            self.active = True
            self.latched = True
            self.reason = reason
            self.activated_at = datetime.now(timezone.utc)
            
            # Persist state immediately
            self._save_state()

            logger.critical(
                "🛑 KILL SWITCH ACTIVATED",
                reason=reason.value,
                emergency=emergency,
                timestamp=self.activated_at.isoformat(),
            )
            
            # Send alert notification
            send_alert_sync(
                "KILL_SWITCH",
                f"Kill switch activated!\nReason: {reason.value}\nEmergency: {emergency}",
                urgent=True,
            )

            if self.client:
                try:
                    # 1. Cancel non-SL orders (PRESERVE stop losses to protect positions)
                    # Cancelling SL orders leaves positions naked and has caused
                    # repeated losses. SL orders can only limit losses, never cause them.
                    cancelled, preserved_sls = await self._cancel_non_sl_orders()
                    
                    # 2. VERIFICATION LOOP: "cancel submitted" is not "cancel effective".
                    # Poll open orders and retry cancellation for any non-SL stragglers.
                    # Max 3 retries with 2s backoff between each.
                    for retry in range(3):
                        import asyncio as _aio
                        await _aio.sleep(2)
                        
                        remaining = await self._count_non_sl_orders()
                        if remaining == 0:
                            logger.info(
                                "Kill switch: Cancellation verified — no non-SL orders remain",
                                verification_attempt=retry + 1,
                            )
                            break
                        
                        logger.warning(
                            "Kill switch: Non-SL orders still open after cancellation, retrying",
                            remaining_non_sl=remaining,
                            retry=retry + 1,
                        )
                        retry_cancelled, retry_preserved = await self._cancel_non_sl_orders()
                        cancelled += retry_cancelled
                        preserved_sls = retry_preserved  # Use latest count
                    else:
                        # All retries exhausted
                        final_remaining = await self._count_non_sl_orders()
                        if final_remaining > 0:
                            logger.critical(
                                "Kill switch: CANCELLATION VERIFICATION FAILED — non-SL orders persist",
                                remaining_non_sl=final_remaining,
                                total_cancelled=cancelled,
                                total_preserved_sls=preserved_sls,
                            )
                    
                    logger.info(
                        "Kill switch: Order cleanup complete",
                        cancelled=cancelled,
                        preserved_stop_losses=preserved_sls,
                    )
                    
                    # 3. If emergency: flatten all positions
                    if emergency:
                         positions = await self.client.get_all_futures_positions()
                         for pos in positions:
                             symbol = pos['symbol']
                             try:
                                 await self.client.close_position(symbol)
                                 logger.warning(f"Kill switch: Emergency closed position for {symbol}")
                             except InvariantError:
                                 raise  # Safety violation — must propagate
                             except OperationalError as e:
                                 logger.error("Kill switch: Failed to close position (transient)", kill_step="emergency_close", symbol=symbol, error=str(e), error_type=type(e).__name__)
                             except Exception as e:
                                 logger.exception("Kill switch: Unexpected error closing position", kill_step="emergency_close", symbol=symbol, error=str(e), error_type=type(e).__name__)
                                 raise
                except InvariantError:
                    raise  # Safety violation — must propagate
                except OperationalError as e:
                    logger.critical("Kill switch action failed (transient)", kill_step="activate", error=str(e), error_type=type(e).__name__)
                except Exception as e:
                    logger.exception("Kill switch: Unexpected error during activation", kill_step="activate", error=str(e), error_type=type(e).__name__)
                    raise
            else:
                 logger.critical("Kill switch: No client attached, cannot execute actions")

            logger.critical(
                "Manual acknowledgment required to restart trading"
            )
    
    @staticmethod
    def _is_stop_loss_order(order: dict) -> bool:
        """Classify whether an order is a protective stop loss.
        
        A stop-loss is: type contains "stop" (but NOT "take_profit"),
        and is reduce-only (protective, not an entry stop).
        """
        order_type = (order.get("type") or "").lower()
        info_type = ((order.get("info") or {}).get("orderType") or "").lower()
        is_reduce_only = order.get("reduceOnly", order.get("reduce_only", False))
        
        return (
            ("stop" in order_type or "stop" in info_type)
            and "take_profit" not in order_type
            and "take-profit" not in order_type
            and "take_profit" not in info_type
            and "take-profit" not in info_type
            and is_reduce_only
        )
    
    async def _cancel_non_sl_orders(self) -> tuple:
        """Cancel all non-SL orders, preserving stop losses.
        
        Returns:
            (cancelled_count, preserved_sl_count)
        """
        open_orders = await self.client.get_futures_open_orders()
        cancelled = 0
        preserved_sls = 0
        
        for order in open_orders:
            if self._is_stop_loss_order(order):
                preserved_sls += 1
                logger.info(
                    "Kill switch: PRESERVING stop loss order",
                    order_id=order.get("id"),
                    symbol=order.get("symbol"),
                    order_type=(order.get("type") or "").lower(),
                )
                continue
            
            try:
                await self.client.cancel_futures_order(order["id"], order.get("symbol"))
                cancelled += 1
                logger.info("Futures order cancelled", order_id=order["id"])
            except InvariantError:
                raise  # Safety violation — must propagate
            except OperationalError as e:
                logger.warning(
                    "Kill switch: Failed to cancel order (transient)",
                    kill_step="cancel_non_sl",
                    order_id=order.get("id"),
                    symbol=order.get("symbol"),
                    error=str(e),
                    error_type=type(e).__name__,
                )
            except Exception as e:
                logger.exception(
                    "Kill switch: Unexpected error cancelling order",
                    kill_step="cancel_non_sl",
                    order_id=order.get("id"),
                    error=str(e),
                    error_type=type(e).__name__,
                )
                raise
        
        return cancelled, preserved_sls
    
    async def _count_non_sl_orders(self) -> int:
        """Count remaining non-SL open orders. Used for verification."""
        try:
            open_orders = await self.client.get_futures_open_orders()
            return sum(1 for o in open_orders if not self._is_stop_loss_order(o))
        except OperationalError as e:
            logger.warning("Kill switch: Failed to count remaining orders (transient)", kill_step="verify_cancel", error=str(e), error_type=type(e).__name__)
            return -1  # Unknown — caller should treat as potential issue
    
    def acknowledge(self) -> bool:
        """
        Manually acknowledge kill switch to allow restart.
        
        Returns:
            True if acknowledged successfully
        """
        if not self.latched:
            logger.warning("Kill switch not latched, nothing to acknowledge")
            return False
        
        logger.info(
            "Kill switch acknowledged",
            reason=self.reason.value if self.reason else "unknown",
            activated_at=self.activated_at.isoformat() if self.activated_at else "unknown",
        )

        # Reset state
        self.active = False
        self.latched = False
        self.reason = None
        self.activated_at = None

        # Persist deactivated state
        self._save_state()

        return True
    
    def is_active(self) -> bool:
        """Check if kill switch is active."""
        return self.active
    
    def is_latched(self) -> bool:
        """Check if kill switch is latched (requires manual ack)."""
        return self.latched

    def get_status(self) -> dict:
        """
        Get kill switch status.

        Returns:
            Dict with status information
        """
        return {
            "active": self.active,
            "latched": self.latched,
            "activated_at": self.activated_at.isoformat() if self.activated_at else None,
            "reason": self.reason.value if self.reason else None,
            "duration_seconds": (datetime.now(timezone.utc) - self.activated_at).total_seconds()
                               if self.activated_at else 0
        }

    def _save_state(self) -> None:
        """Persist kill switch state to file (data/ under repo root, or KILL_SWITCH_STATE_PATH).
        
        If persistence fails, crash. Unpersisted kill switch state means a restart
        could resume trading when it shouldn't.
        """
        import json
        path = _kill_switch_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        state = {
            "active": self.active,
            "latched": self.latched,
            "activated_at": self.activated_at.isoformat() if self.activated_at else None,
            "reason": self.reason.value if self.reason else None
        }
        with open(path, "w") as f:
            json.dump(state, f)
        # No try/except: if we can't persist kill switch state, crash is correct.

    def _load_state(self) -> None:
        """Load persisted kill switch state from data/ or KILL_SWITCH_STATE_PATH.
        
        If state file is corrupt, default to ACTIVE (safest posture).
        If state file can't be read, crash before trading starts.
        """
        import json
        path = _kill_switch_state_path()
        if not path.exists():
            return

        try:
            with open(path, "r") as f:
                state = json.load(f)
        except (json.JSONDecodeError, ValueError) as e:
            # Corrupt state file — default to ACTIVE (safest posture)
            logger.critical("Kill switch state file corrupt — defaulting to ACTIVE", error=str(e), error_type=type(e).__name__)
            self.active = True
            self.latched = True
            self.reason = KillSwitchReason.DATA_FAILURE
            self.activated_at = datetime.now(timezone.utc)
            return
        # OSError (can't read) → crash (no try/except — READY gate prevents trading)

        self.active = state.get("active", False)
        self.latched = state.get("latched", False)

        reason_str = state.get("reason")
        if reason_str:
            try:
                self.reason = KillSwitchReason(reason_str)
            except ValueError:
                self.reason = None

        activated_at_str = state.get("activated_at")
        if activated_at_str:
            self.activated_at = datetime.fromisoformat(activated_at_str)

        if self.active:
            logger.warning(
                "Kill switch was active on startup",
                activated_at=activated_at_str,
                reason=self.reason.value if self.reason else "unknown"
            )


# Global instance
_kill_switch = KillSwitch()


def get_kill_switch() -> KillSwitch:
    """Get global kill switch instance."""
    return _kill_switch


def read_kill_switch_state() -> dict:
    """
    Read persisted kill switch state from file (no KillSwitch instance).
    Use from health/dashboard to check status without loading full module.
    Uses same path as KillSwitch: data/ under repo root or KILL_SWITCH_STATE_PATH.
    """
    import json
    out = {"active": False, "latched": False, "reason": None, "activated_at": None}
    path = _kill_switch_state_path()
    if not path.exists():
        return out
    try:
        with open(path, "r") as f:
            state = json.load(f)
        out["active"] = state.get("active", False)
        out["latched"] = state.get("latched", False)
        out["reason"] = state.get("reason")
        out["activated_at"] = state.get("activated_at")
    except (json.JSONDecodeError, ValueError, OSError) as e:
        # Corrupt or unreadable — return safest default (active=False but log)
        logger.warning("read_kill_switch_state: state file unreadable", error=str(e), error_type=type(e).__name__)
    return out
