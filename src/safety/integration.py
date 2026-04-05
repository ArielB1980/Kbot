"""
Production Hardening Integration V2.

This module provides integration hooks for the production hardening features:
1. InvariantMonitor - Hard safety limits
2. CycleGuard - Timing protection  
3. PositionDeltaReconciler - Strategy-execution decoupling
4. DecisionAuditLogger - Decision-complete logging

V2 Improvements:
- HardeningDecision enum (ALLOW, SKIP_TICK, HALT)
- Persistent HALT state (survives restarts)
- Gate assertion to prevent unguarded order emission
- Startup self-test
- Exception-safe audit logging

Usage:
    from src.safety.integration import ProductionHardeningLayer, HardeningDecision
    
    # In LiveTrading.__init__():
    self.hardening = ProductionHardeningLayer(config, kill_switch)
    
    # CRITICAL: Run self-test before starting
    if not self.hardening.self_test():
        raise RuntimeError("Hardening self-test failed")
    
    # In _tick():
    decision = await self.hardening.pre_tick_check(equity, positions, margin_util, margin)
    if decision == HardeningDecision.HALT:
        return  # System halted - manual reset required
    if decision == HardeningDecision.SKIP_TICK:
        return  # Skip this tick, try again next cycle
"""
import asyncio
import hashlib
import json
import os
import uuid
from dataclasses import dataclass, asdict, field
from decimal import Decimal
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.monitoring.logger import get_logger
from src.safety.invariant_monitor import (
    InvariantMonitor,
    SystemInvariants,
    SystemState,
    init_invariant_monitor,
    get_invariant_monitor,
)
from src.runtime.cycle_guard import (
    CycleGuard,
    init_cycle_guard,
    get_cycle_guard,
)
from src.reconciliation.position_delta import (
    PositionDeltaReconciler,
    PositionIntent,
    ExchangePosition,
    PositionDelta,
    DeltaAction,
    init_delta_reconciler,
    get_delta_reconciler,
)
from src.monitoring.decision_audit import (
    DecisionAuditLogger,
    get_decision_audit_logger,
)
from src.config.safety_config import (
    load_safety_config,
    create_system_invariants,
    get_cycle_guard_config,
    get_reconciliation_config,
    get_audit_config,
    log_safety_config_summary,
)
from src.domain.models import Side, Position
from src.exceptions import OperationalError, DataError, InvariantError

logger = get_logger(__name__)


class HardeningDecision(str, Enum):
    """
    Decision returned by pre_tick_check().
    
    ALLOW: Trading allowed, proceed with tick
    SKIP_TICK: Skip this tick, try again next cycle (timing issue)
    HALT: System halted, manual reset required
    """
    ALLOW = "allow"
    SKIP_TICK = "skip_tick"
    HALT = "halt"


@dataclass
class PersistedHaltState:
    """Persisted halt state for surviving restarts."""
    state: str  # "halted" or "emergency"
    reason: str
    violations: List[str]
    timestamp: str
    run_id: str
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PersistedHaltState":
        return cls(**data)


class HardeningGateError(RuntimeError):
    """Raised when order emission is attempted without passing the hardening gate."""
    pass


class ProductionHardeningLayer:
    """
    Unified interface for all production hardening features.
    
    V2 Features:
    - HardeningDecision enum for explicit state handling
    - Persistent HALT state
    - Gate assertion for order emission
    - Startup self-test
    - Async lock for cycle exclusion
    
    CRITICAL: Call self_test() before starting trading.
    """
    
    # Default state persistence directory
    DEFAULT_STATE_DIR = Path.home() / ".trading_system"
    STATE_FILE = "halt_state.json"
    
    def __init__(
        self,
        config: Any,  # Config object
        kill_switch: Any,  # KillSwitch instance
        safety_config_path: Optional[str] = None,
        state_dir: Optional[Path] = None,
    ):
        """
        Initialize production hardening layer.
        
        Args:
            config: Application config object
            kill_switch: KillSwitch instance for emergency stop
            safety_config_path: Optional path to safety.yaml
            state_dir: Directory for state persistence (default: ~/.trading_system)
        """
        self.config = config
        self.kill_switch = kill_switch
        self._run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        
        # State persistence
        self._state_dir = state_dir or self.DEFAULT_STATE_DIR
        self._state_file = self._state_dir / self.STATE_FILE
        # Replay / research harness: do not block ticks on production halt_state.json
        self.ignore_persisted_halt: bool = False
        self._replay_halt_skip_logged: bool = False
        # Replay / research harness: bypass invariant monitor checks entirely.
        # Simulated equity/concentration levels should not block research entries.
        self.disable_invariant_checks: bool = False

        # Gate tracking
        self._gate_checked_this_tick = False
        self._gate_decision: Optional[HardeningDecision] = None
        
        # Async lock for cycle exclusion
        self._cycle_lock = asyncio.Lock()
        self._lock_held = False
        
        # Executed actions store (idempotency)
        self._executed_action_ids: set = set()
        
        # Load safety configuration
        try:
            self.safety_config = load_safety_config(safety_config_path)
            log_safety_config_summary(self.safety_config)
        except (ValueError, TypeError, KeyError, FileNotFoundError, OSError) as e:
            logger.warning("Failed to load safety config, using defaults", error=str(e), error_type=type(e).__name__)
            self.safety_config = {"safety": {}}
        
        # Initialize InvariantMonitor
        try:
            invariants = create_system_invariants(self.safety_config)
            self.invariant_monitor = init_invariant_monitor(
                invariants=invariants,
                kill_switch=kill_switch,
            )
            logger.info(
                "InvariantMonitor initialized",
                max_drawdown=str(invariants.max_equity_drawdown_pct),
                max_positions=invariants.max_concurrent_positions,
            )
        except (ValueError, TypeError, KeyError) as e:
            logger.error("Failed to initialize InvariantMonitor", error=str(e), error_type=type(e).__name__)
            self.invariant_monitor = get_invariant_monitor()
        
        # Initialize CycleGuard
        try:
            cg_config = get_cycle_guard_config(self.safety_config)
            self.cycle_guard = init_cycle_guard(**cg_config)
            logger.info("CycleGuard initialized", **cg_config)
        except (ValueError, TypeError, KeyError) as e:
            logger.error("Failed to initialize CycleGuard", error=str(e), error_type=type(e).__name__)
            self.cycle_guard = get_cycle_guard()
        
        # Initialize PositionDeltaReconciler
        try:
            rec_config = get_reconciliation_config(self.safety_config)
            self.reconciler = init_delta_reconciler(
                min_delta_threshold_usd=rec_config["min_delta_threshold_usd"],
                max_delta_per_order_usd=rec_config["max_delta_per_order_usd"],
            )
            logger.info("PositionDeltaReconciler initialized", **{k: str(v) for k, v in rec_config.items()})
        except (ValueError, TypeError, KeyError) as e:
            logger.error("Failed to initialize PositionDeltaReconciler", error=str(e), error_type=type(e).__name__)
            self.reconciler = get_delta_reconciler()
        
        # Initialize DecisionAuditLogger
        try:
            audit_config = get_audit_config(self.safety_config)
            self.decision_logger = DecisionAuditLogger(
                max_buffer_size=audit_config["max_buffer_size"],
            )
            logger.info("DecisionAuditLogger initialized", **audit_config)
        except (ValueError, TypeError, KeyError) as e:
            logger.error("Failed to initialize DecisionAuditLogger", error=str(e), error_type=type(e).__name__)
            self.decision_logger = get_decision_audit_logger()
        
        # Track current cycle
        self._current_cycle_id: Optional[str] = None
        self._cycle_started = False
        
        logger.info("ProductionHardeningLayer V2 initialized", run_id=self._run_id)
    
    # ===== STARTUP SELF-TEST =====
    
    def self_test(self) -> Tuple[bool, List[str]]:
        """
        Perform startup self-test.
        
        MUST be called before starting trading.
        
        Validates:
        - Component initialization
        - State persistence directory
        - Persisted HALT state
        - Config-safety threshold compatibility (prevents Bug #1 class errors)
        
        Returns:
            (success, list of error messages)
        """
        errors = []
        
        # 1. Check InvariantMonitor
        if self.invariant_monitor is None:
            errors.append("InvariantMonitor not initialized")
        
        # 2. Check CycleGuard
        if self.cycle_guard is None:
            errors.append("CycleGuard not initialized")
        
        # 3. Check DecisionAuditLogger
        if self.decision_logger is None:
            errors.append("DecisionAuditLogger not initialized")
        
        # 4. Check state persistence directory
        try:
            self._state_dir.mkdir(parents=True, exist_ok=True)
            test_file = self._state_dir / ".write_test"
            test_file.write_text("test")
            test_file.unlink()
        except OSError as e:
            errors.append(f"State directory not writable: {e}")
        
        # 5. Check for persisted HALT state
        halt_state = self._load_halt_state()
        if halt_state:
            errors.append(
                f"PERSISTED HALT STATE EXISTS: {halt_state.state} - "
                f"Reason: {halt_state.reason} - "
                f"Time: {halt_state.timestamp} - "
                f"Call clear_halt() to resume trading"
            )
        
        # 6. CONFIG-SAFETY THRESHOLD COMPATIBILITY
        # Prevents the class of bug where safety thresholds are tighter than
        # operational thresholds, causing false HALTs every time the system
        # operates at its intended capacity. (Same class as Bug #1: margin
        # threshold mismatch.)
        threshold_errors = self._validate_config_safety_compatibility()
        errors.extend(threshold_errors)
        
        success = len(errors) == 0
        
        if success:
            logger.info("SELF_TEST_PASSED", run_id=self._run_id)
        else:
            logger.critical("SELF_TEST_FAILED", errors=errors, run_id=self._run_id)
        
        return success, errors
    
    def _validate_config_safety_compatibility(self) -> List[str]:
        """
        Validate that safety thresholds are compatible with operational config.
        
        Rules:
        1. safety.max_concurrent_positions >= risk.auction_max_positions
        2. safety.max_margin_utilization_pct > risk.auction_max_margin_util
        3. safety.degraded_margin_utilization_pct <= risk.auction_max_margin_util
        4. safety.degraded_concurrent_positions < safety.max_concurrent_positions
        
        Returns:
            List of error strings (empty = all good)
        """
        errors = []
        inv = self.invariant_monitor.invariants
        risk = getattr(self.config, "risk", None)
        
        if risk is None:
            logger.warning("Config-safety validation skipped: no risk config found")
            return errors
        
        # 1. Concurrent positions: safety halt must be >= operational max
        operational_max_positions = getattr(risk, "auction_max_positions", None)
        if operational_max_positions is None:
            operational_max_positions = getattr(risk, "max_concurrent_positions", 2)
        
        if inv.max_concurrent_positions < operational_max_positions:
            errors.append(
                f"THRESHOLD_MISMATCH: safety.max_concurrent_positions ({inv.max_concurrent_positions}) "
                f"< risk.auction_max_positions ({operational_max_positions}). "
                f"The invariant monitor will HALT the system before the auction fills its budget. "
                f"Set safety.max_concurrent_positions >= {operational_max_positions}."
            )
        
        # 2. Margin utilization: safety halt must be > auction budget ceiling
        auction_max_margin = Decimal(str(getattr(risk, "auction_max_margin_util", 0.90)))
        if inv.max_margin_utilization_pct <= auction_max_margin:
            errors.append(
                f"THRESHOLD_MISMATCH: safety.max_margin_utilization_pct ({inv.max_margin_utilization_pct}) "
                f"<= risk.auction_max_margin_util ({auction_max_margin}). "
                f"The invariant monitor will HALT when the auction uses its full margin budget. "
                f"Set safety.max_margin_utilization_pct > {auction_max_margin}."
            )
        
        # 3. Degraded margin should be <= auction max (warning, not error)
        if inv.degraded_margin_utilization_pct > auction_max_margin:
            logger.warning(
                "CONFIG_SAFETY_WARNING: degraded_margin_utilization_pct > auction_max_margin_util",
                degraded_margin=str(inv.degraded_margin_utilization_pct),
                auction_max_margin=str(auction_max_margin),
                note="System will enter DEGRADED before auction fills budget",
            )
        
        # 4. Degraded positions < max positions (internal consistency)
        if inv.degraded_concurrent_positions >= inv.max_concurrent_positions:
            errors.append(
                f"THRESHOLD_INCONSISTENCY: safety.degraded_concurrent_positions "
                f"({inv.degraded_concurrent_positions}) >= safety.max_concurrent_positions "
                f"({inv.max_concurrent_positions}). Degraded threshold must be strictly less."
            )
        
        if errors:
            logger.critical(
                "CONFIG_SAFETY_COMPATIBILITY_FAILED",
                errors=errors,
                safety_max_positions=inv.max_concurrent_positions,
                safety_max_margin=str(inv.max_margin_utilization_pct),
                operational_max_positions=operational_max_positions,
                auction_max_margin=str(auction_max_margin),
            )
        else:
            logger.info(
                "CONFIG_SAFETY_COMPATIBILITY_OK",
                safety_max_positions=inv.max_concurrent_positions,
                operational_max_positions=operational_max_positions,
                safety_max_margin=str(inv.max_margin_utilization_pct),
                auction_max_margin=str(auction_max_margin),
            )
        
        return errors
    
    # ===== HALT STATE PERSISTENCE =====
    
    def _load_halt_state(self) -> Optional[PersistedHaltState]:
        """Load persisted halt state from disk.
        
        If state can't be loaded, crash before trading starts.
        The READY gate ensures we never trade with corrupt halt state.
        """
        if not self._state_file.exists():
            return None
        
        try:
            data = json.loads(self._state_file.read_text())
            return PersistedHaltState.from_dict(data)
        except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
            # Corrupt state file — log and crash (safest: don't trade with unknown halt state)
            logger.critical("Halt state file corrupt — cannot determine if HALT was active", error=str(e), error_type=type(e).__name__)
            raise InvariantError(f"Corrupt halt state file: {e}") from e
        except OSError as e:
            logger.critical("Cannot read halt state file", error=str(e), error_type=type(e).__name__)
            raise InvariantError(f"Cannot read halt state file: {e}") from e
    
    def _persist_halt_state(self, state: SystemState, reason: str, violations: List[str]):
        """Persist halt state to disk.
        
        If persistence fails, crash. systemd restart + halt state is safer
        than continuing with unpersisted halt.
        """
        self._state_dir.mkdir(parents=True, exist_ok=True)
        
        halt_state = PersistedHaltState(
            state=state.value,
            reason=reason,
            violations=violations,
            timestamp=datetime.now(timezone.utc).isoformat(),
            run_id=self._run_id,
        )
        
        self._state_file.write_text(json.dumps(halt_state.to_dict(), indent=2))
        logger.critical(
            "HALT_STATE_PERSISTED",
            state=state.value,
            reason=reason,
            file=str(self._state_file),
        )
        
        # Send alert for halt state (alert failure must never block halt persistence)
        try:
            from src.monitoring.alert_dispatcher import send_alert_sync
            violations_str = "\n".join(f"• {v}" for v in violations[:5])
            send_alert_sync(
                "SYSTEM_HALTED",
                f"System entered HALT state!\nReason: {reason}\nViolations:\n{violations_str}",
                urgent=True,
            )
        except (OperationalError, ImportError, OSError) as e:
            logger.warning("Failed to send HALT alert (non-blocking)", error=str(e), error_type=type(e).__name__)
    
    def clear_halt(self, operator: str = "unknown", keep_kill_switch: bool = False) -> bool:
        """
        Clear persisted halt state AND kill switch state (MANUAL INTERVENTION REQUIRED).
        
        P0.5: By default, also clears the kill switch persisted state. The 2026-02-14
        incident showed that clearing halt without clearing kill switch causes the
        system to auto-flatten positions on restart. These two states must be cleared
        atomically unless the operator explicitly opts out.
        
        Args:
            operator: Name/ID of operator clearing the halt
            keep_kill_switch: If True, do NOT clear the kill switch state (rare)
            
        Returns:
            True if cleared successfully
        """
        cleared_halt = False
        cleared_ks = False
        
        # 1. Clear halt state
        if not self._state_file.exists():
            logger.info("No halt state to clear")
            cleared_halt = True
        else:
            try:
                halt_state = self._load_halt_state()
                logger.critical(
                    "HALT_STATE_CLEARED",
                    operator=operator,
                    previous_state=halt_state.state if halt_state else "unknown",
                    previous_reason=halt_state.reason if halt_state else "unknown",
                )
                self._state_file.unlink()
                cleared_halt = True
            except InvariantError:
                raise  # Corrupt state — must propagate
            except OSError as e:
                logger.error("Failed to clear halt state (filesystem)", error=str(e), error_type=type(e).__name__)
        
        # 2. Clear kill switch state (P0.5: default behavior)
        if not keep_kill_switch:
            try:
                if self.kill_switch and self.kill_switch.is_active():
                    self.kill_switch.acknowledge()
                    logger.critical(
                        "KILL_SWITCH_CLEARED_WITH_HALT",
                        operator=operator,
                        reason=self.kill_switch.reason.value if self.kill_switch.reason else "unknown",
                    )
                cleared_ks = True
            except Exception as e:
                logger.error("Failed to clear kill switch state", error=str(e), error_type=type(e).__name__)
        else:
            logger.info("keep_kill_switch=True — kill switch state preserved")
            cleared_ks = True
        
        # 3. Reset invariant monitor state
        self.invariant_monitor.state = SystemState.ACTIVE
        self.invariant_monitor.violations.clear()
        
        success = cleared_halt and cleared_ks
        logger.info(
            "SAFETY_STATE_CLEAR_RESULT",
            operator=operator,
            halt_cleared=cleared_halt,
            kill_switch_cleared=cleared_ks and not keep_kill_switch,
            success=success,
        )
        return success
    
    def is_halted(self) -> bool:
        """Check if system is in persisted HALT state."""
        return self._state_file.exists()
    
    # ===== GATE ENFORCEMENT =====
    
    def assert_gate_open(self):
        """
        Assert that the hardening gate has been checked this tick.
        
        Call this before any order emission.
        Raises HardeningGateError if gate not checked.
        """
        if not self._gate_checked_this_tick:
            raise HardeningGateError(
                "Order emission attempted before hardening gate check. "
                "Call pre_tick_check() before placing orders."
            )
        
        if self._gate_decision == HardeningDecision.HALT:
            raise HardeningGateError(
                "Order emission attempted while system is HALTED. "
                "Manual intervention required to clear halt state."
            )
    
    def is_gate_open(self) -> bool:
        """Check if gate is open (non-throwing version)."""
        return (
            self._gate_checked_this_tick and 
            self._gate_decision == HardeningDecision.ALLOW
        )
    
    # ===== PRE-TICK CHECKS =====
    
    async def pre_tick_check(
        self,
        current_equity: Decimal,
        open_positions: List[Position],
        margin_utilization: Decimal,
        available_margin: Decimal,
        refetch_equity_fn: Optional[Callable] = None,
    ) -> HardeningDecision:
        """
        Perform all pre-tick safety checks.
        
        Call this at the START of each _tick() method.
        
        Returns:
            HardeningDecision indicating what action to take
        """
        # Reset gate state
        self._gate_checked_this_tick = True
        self._gate_decision = HardeningDecision.ALLOW
        
        # 0. Check for persisted HALT state (survives restarts)
        if self.is_halted():
            if self.ignore_persisted_halt:
                if not self._replay_halt_skip_logged:
                    self._replay_halt_skip_logged = True
                    logger.info(
                        "REPLAY_IGNORE_PRODUCTION_PERSISTED_HALT",
                        message=(
                            "Replay harness ignores production halt_state.json; "
                            "live trading still requires clear_halt()"
                        ),
                    )
            else:
                halt_state = self._load_halt_state()
                logger.critical(
                    "PERSISTED_HALT_STATE_ACTIVE",
                    state=halt_state.state if halt_state else "unknown",
                    reason=halt_state.reason if halt_state else "unknown",
                    message="Call clear_halt() to resume trading",
                )
                self._gate_decision = HardeningDecision.HALT
                return HardeningDecision.HALT
        
        # 1. Acquire cycle lock (async)
        if not self._lock_held:
            acquired = self._cycle_lock.locked()
            if acquired:
                logger.warning("CYCLE_LOCK_CONTENTION: Previous cycle still running")
                self._gate_decision = HardeningDecision.SKIP_TICK
                return HardeningDecision.SKIP_TICK
            
            await self._cycle_lock.acquire()
            self._lock_held = True
        
        # 2. Start cycle (timing protection)
        if not self._cycle_started:
            success, error = self.cycle_guard.start_cycle()
            if not success:
                logger.debug("Cycle skipped", reason=error)
                self._release_lock()
                self._gate_decision = HardeningDecision.SKIP_TICK
                return HardeningDecision.SKIP_TICK
            
            self._cycle_started = True
            self._current_cycle_id = self.cycle_guard.current_cycle.cycle_id \
                if self.cycle_guard.current_cycle else f"fallback_{uuid.uuid4().hex[:8]}"
        
        # 3. Check invariants (pass refetch_equity_fn for implausibility guard)
        state = await self.invariant_monitor.check_all(
            current_equity=current_equity,
            open_positions=open_positions,
            margin_utilization=margin_utilization,
            available_margin=available_margin,
            refetch_equity_fn=refetch_equity_fn,
        )
        
        if state == SystemState.EMERGENCY:
            # Persist HALT state
            self._persist_halt_state(
                state=state,
                reason="Multiple critical invariant violations",
                violations=[str(v) for v in self.invariant_monitor.violations],
            )
            logger.critical(
                "SYSTEM_EMERGENCY_HALT",
                cycle_id=self._current_cycle_id,
                violations=[str(v) for v in self.invariant_monitor.violations],
            )
            self._gate_decision = HardeningDecision.HALT
            return HardeningDecision.HALT
        
        if state == SystemState.HALTED:
            # Persist HALT state
            self._persist_halt_state(
                state=state,
                reason="Critical invariant violation",
                violations=[str(v) for v in self.invariant_monitor.violations],
            )
            logger.critical(
                "SYSTEM_HALTED",
                cycle_id=self._current_cycle_id,
                violations=[str(v) for v in self.invariant_monitor.violations],
            )
            self._gate_decision = HardeningDecision.HALT
            return HardeningDecision.HALT
        
        if state == SystemState.DEGRADED:
            logger.warning(
                "SYSTEM_DEGRADED - Reduced exposure mode",
                cycle_id=self._current_cycle_id,
                violations=[str(v) for v in self.invariant_monitor.violations],
            )
            # DEGRADED allows trading, just with reduced exposure
        
        return HardeningDecision.ALLOW
    
    def _release_lock(self):
        """Release cycle lock if held."""
        if self._lock_held and self._cycle_lock.locked():
            self._cycle_lock.release()
            self._lock_held = False
    
    def is_trading_allowed(self) -> bool:
        """Check if new entries are allowed."""
        if self.disable_invariant_checks:
            return True
        return self.invariant_monitor.is_trading_allowed() and self._gate_decision == HardeningDecision.ALLOW

    def is_management_allowed(self) -> bool:
        """Check if position management is allowed."""
        if self.disable_invariant_checks:
            return True
        return self.invariant_monitor.is_management_allowed()
    
    # ===== IDEMPOTENCY =====
    
    def generate_action_id(
        self,
        symbol: str,
        action: str,
        target_size: Decimal,
    ) -> str:
        """
        Generate deterministic action ID for idempotency.
        
        Same inputs = same action_id = duplicate detection.
        """
        cycle_id = self._current_cycle_id or "unknown"
        data = f"{cycle_id}:{symbol}:{action}:{target_size}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]
    
    def is_action_executed(self, action_id: str) -> bool:
        """Check if action has already been executed this session."""
        return action_id in self._executed_action_ids
    
    def mark_action_executed(self, action_id: str):
        """Mark action as executed."""
        self._executed_action_ids.add(action_id)
        # Limit size to prevent memory issues
        if len(self._executed_action_ids) > 10000:
            # Remove oldest (convert to list, slice, convert back)
            self._executed_action_ids = set(list(self._executed_action_ids)[-5000:])
    
    # ===== SIGNAL RECONCILIATION =====
    
    def reconcile_signal(
        self,
        signal: Any,
        actual_position: Optional[Position],
        intended_size_notional: Decimal,
        intended_size_base: Decimal,
        current_price: Optional[Decimal] = None,
    ) -> Optional[PositionDelta]:
        """
        Reconcile a strategy signal with actual exchange position.
        
        Call this for each signal before execution.
        Includes idempotency check.
        """
        try:
            # Create intent from signal
            intent = self.reconciler.create_intent_from_signal(
                signal=signal,
                size_notional=intended_size_notional,
                size_base=intended_size_base,
            )
            
            # Convert actual position to ExchangePosition
            actual = None
            if actual_position and actual_position.size > 0:
                actual = ExchangePosition(
                    symbol=actual_position.symbol,
                    side=actual_position.side,
                    size=actual_position.size,
                    size_notional=actual_position.size_notional or Decimal("0"),
                    entry_price=actual_position.entry_price,
                    mark_price=current_price,
                )
            
            # Calculate delta
            delta = self.reconciler.calculate_delta(intent, actual, current_price)
            
            # Generate action_id for idempotency
            action_id = self.generate_action_id(
                symbol=delta.symbol,
                action=delta.action.value,
                target_size=delta.intended_size,
            )
            delta.action_id = action_id
            
            # Check idempotency
            if self.is_action_executed(action_id):
                logger.warning(
                    "DUPLICATE_ACTION_BLOCKED",
                    symbol=delta.symbol,
                    action=delta.action.value,
                    action_id=action_id,
                )
                delta.allowed = False
                delta.rejection_reason = "duplicate_action"
                return delta
            
            # Apply system state check
            if self.invariant_monitor.violations:
                self.reconciler.apply_system_state_check(
                    delta=delta,
                    system_state=self.invariant_monitor.state.value,
                    active_violations=[str(v) for v in self.invariant_monitor.violations],
                )
            
            return delta
            
        except InvariantError:
            raise  # Safety violation — propagate
        except DataError as e:
            logger.error("Failed to reconcile signal (data)", symbol=getattr(signal, 'symbol', 'unknown'), error=str(e), error_type=type(e).__name__)
            return None
        except Exception as e:
            logger.exception("Unexpected error in reconcile_signal", symbol=getattr(signal, 'symbol', 'unknown'), error=str(e), error_type=type(e).__name__)
            raise
    
    def reconcile_close(
        self,
        symbol: str,
        actual_position: Position,
        reason: str = "exit_signal",
    ) -> Optional[PositionDelta]:
        """Reconcile a close/exit signal."""
        try:
            intent = self.reconciler.create_flat_intent(symbol, reason)
            
            actual = ExchangePosition(
                symbol=actual_position.symbol,
                side=actual_position.side,
                size=actual_position.size,
                size_notional=actual_position.size_notional or Decimal("0"),
                entry_price=actual_position.entry_price,
            )
            
            delta = self.reconciler.calculate_delta(intent, actual)
            
            # Generate action_id
            action_id = self.generate_action_id(
                symbol=delta.symbol,
                action=delta.action.value,
                target_size=Decimal("0"),
            )
            delta.action_id = action_id
            
            return delta
            
        except InvariantError:
            raise  # Safety violation — propagate
        except DataError as e:
            logger.error("Failed to reconcile close (data)", symbol=symbol, error=str(e), error_type=type(e).__name__)
            return None
        except Exception as e:
            logger.exception("Unexpected error in reconcile_close", symbol=symbol, error=str(e), error_type=type(e).__name__)
            raise
    
    # ===== DECISION LOGGING (EXCEPTION-SAFE) =====
    
    def record_decision(
        self,
        symbol: str,
        signal: Any,
        delta: Optional[PositionDelta],
        decision: str,
        reason: str,
        thresholds: Optional[Dict[str, Any]] = None,
        alternatives: Optional[List[Dict[str, Any]]] = None,
        rejection_reasons: Optional[List[str]] = None,
        equity: Optional[Decimal] = None,
        margin: Optional[Decimal] = None,
        positions: Optional[List[str]] = None,
        spot_price: Optional[Decimal] = None,
    ):
        """Record a complete trading decision."""
        try:
            self.decision_logger.record_decision(
                symbol=symbol,
                cycle_id=self._current_cycle_id or "unknown",
                signal=signal,
                thresholds=thresholds or {},
                alternatives=alternatives or [],
                decision=decision,
                reason=reason,
                rejection_reasons=rejection_reasons,
                spot_price=spot_price,
                equity=equity,
                margin=margin,
                positions=positions,
                system_state=self.invariant_monitor.state.value,
                active_violations=[str(v) for v in self.invariant_monitor.violations],
            )
            
            if decision == "TRADE":
                self.cycle_guard.record_signal_generated()
        except (OperationalError, DataError) as e:
            logger.error("Failed to record decision", symbol=symbol, error=str(e), error_type=type(e).__name__)
        except Exception as e:
            logger.exception("Unexpected error in record_decision", symbol=symbol, error=str(e), error_type=type(e).__name__)
            raise
    
    def record_execution_started(self, symbol: str, action_id: str):
        """Record that execution has started (for exception-safe audit)."""
        logger.info("EXECUTION_STARTED", symbol=symbol, action_id=action_id)
    
    def record_execution_result(
        self,
        symbol: str,
        result: str,
        order_id: Optional[str] = None,
        fill_price: Optional[Decimal] = None,
        error: Optional[str] = None,
        action_id: Optional[str] = None,
    ):
        """Record execution outcome for a decision."""
        try:
            self.decision_logger.update_execution_result(
                symbol=symbol,
                result=result,
                order_id=order_id,
                fill_price=fill_price,
                error=error,
            )
            
            if result == "FILLED" and action_id:
                self.mark_action_executed(action_id)
                self.cycle_guard.record_order_placed()
            elif result in ("REJECTED", "ERROR"):
                self.cycle_guard.record_order_rejected()
                self.invariant_monitor.record_order_rejection()
        except (OperationalError, DataError) as e:
            logger.error("Failed to record execution result", symbol=symbol, error=str(e), error_type=type(e).__name__)
        except Exception as e:
            logger.exception("Unexpected error in record_execution_result", symbol=symbol, error=str(e), error_type=type(e).__name__)
            raise
    
    def record_execution_failed(
        self,
        symbol: str,
        action_id: str,
        error: str,
        error_type: str,
    ):
        """Record that execution failed with exception (for exception-safe audit)."""
        logger.critical(
            "EXECUTION_FAILED",
            symbol=symbol,
            action_id=action_id,
            error=error,
            error_type=error_type,
        )
        try:
            self.decision_logger.update_execution_result(
                symbol=symbol,
                result="EXCEPTION",
                error=f"{error_type}: {error}",
            )
        except (OperationalError, DataError) as e:
            logger.error("Failed to record execution failure", symbol=symbol, error=str(e), error_type=type(e).__name__)
        except Exception as e:
            logger.exception("Unexpected error in record_execution_failed", symbol=symbol, error=str(e), error_type=type(e).__name__)
            raise
    
    def record_api_error(self):
        """Record an API error for rate limiting."""
        self.invariant_monitor.record_api_error()
    
    def record_coin_processed(self):
        """Record that a coin was processed this cycle."""
        self.cycle_guard.record_coin_processed()
    
    # ===== CANDLE FRESHNESS =====
    
    def is_candle_fresh(
        self,
        symbol: str,
        candle_timestamp: datetime,
        max_age_override: Optional[int] = None,
    ) -> bool:
        """Check if a candle is fresh enough for decision making."""
        return self.cycle_guard.is_candle_fresh(
            symbol=symbol,
            candle_timestamp=candle_timestamp,
            max_age_override=max_age_override,
        )
    
    # ===== POST-TICK CLEANUP (CALL IN FINALLY BLOCK) =====
    
    def post_tick_cleanup(self):
        """
        Clean up after a tick.
        
        CRITICAL: Call this in a finally: block to ensure cleanup on exceptions.
        """
        try:
            if self._cycle_started and self.cycle_guard.current_cycle:
                self.cycle_guard.end_cycle()
                self._cycle_started = False
                self._current_cycle_id = None
            
            # Reset per-cycle counters
            self.invariant_monitor.reset_cycle_counters()
            
            # Reset gate state
            self._gate_checked_this_tick = False
            self._gate_decision = None
            
        except (OperationalError, DataError) as e:
            logger.error("Post-tick cleanup failed (transient)", error=str(e), error_type=type(e).__name__)
        except Exception as e:
            logger.exception("Unexpected error in post_tick_cleanup", error=str(e), error_type=type(e).__name__)
            raise
        finally:
            # Always release lock
            self._release_lock()
    
    # ===== STATUS AND DEBUGGING =====
    
    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive status for debugging/dashboard."""
        return {
            "run_id": self._run_id,
            "is_halted": self.is_halted(),
            "gate_open": self.is_gate_open(),
            "invariant_monitor": self.invariant_monitor.get_status(),
            "cycle_guard": self.cycle_guard.get_cycle_stats(),
            "current_cycle_id": self._current_cycle_id,
            "trading_allowed": self.is_trading_allowed(),
            "management_allowed": self.is_management_allowed(),
            "executed_actions_count": len(self._executed_action_ids),
        }
    
    def get_decision_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get decision audit summary."""
        return self.decision_logger.get_audit_summary(hours=hours)
    
    def get_recent_violations(self, limit: int = 20) -> List[Dict]:
        """Get recent invariant violations."""
        return self.invariant_monitor.get_violation_history(limit=limit)
    
    def get_recent_cycles(self, limit: int = 10) -> List[Dict]:
        """Get recent cycle history."""
        return self.cycle_guard.get_recent_cycles(limit=limit)


# ===== GLOBAL INSTANCE =====
_hardening_layer: Optional[ProductionHardeningLayer] = None


def get_hardening_layer() -> Optional[ProductionHardeningLayer]:
    """Get global hardening layer instance (if initialized)."""
    return _hardening_layer


def init_hardening_layer(
    config: Any,
    kill_switch: Any,
    safety_config_path: Optional[str] = None,
) -> ProductionHardeningLayer:
    """Initialize global hardening layer."""
    global _hardening_layer
    _hardening_layer = ProductionHardeningLayer(config, kill_switch, safety_config_path)
    return _hardening_layer
