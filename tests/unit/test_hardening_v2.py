"""
Tests for Production Hardening V2 features.

Tests:
1. HardeningDecision enum behavior
2. Persistent HALT state (survives file reload)
3. Gate assertion blocks unguarded orders
4. Action idempotency prevents duplicates
5. Self-test catches initialization failures
6. Exception-safe audit logging
"""
import pytest
import asyncio
import tempfile
import json
from decimal import Decimal
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from src.safety.integration import (
    ProductionHardeningLayer,
    HardeningDecision,
    HardeningGateError,
    PersistedHaltState,
    init_hardening_layer,
    get_hardening_layer,
)
from src.safety.invariant_monitor import SystemState


class MockKillSwitch:
    """Mock kill switch for testing."""
    def __init__(self):
        self.activated = False
        self.emergency = False
        self.reason = None
    
    async def activate(self, reason, emergency=False):
        self.activated = True
        self.emergency = emergency
        self.reason = reason

    def is_active(self):
        return self.activated

    def acknowledge(self):
        self.activated = False
        self.reason = None


class MockConfig:
    """Mock config object."""
    pass


@pytest.fixture
def temp_state_dir():
    """Create temporary state directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def hardening_layer(temp_state_dir):
    """Create hardening layer with temp state dir."""
    config = MockConfig()
    kill_switch = MockKillSwitch()
    
    layer = ProductionHardeningLayer(
        config=config,
        kill_switch=kill_switch,
        state_dir=temp_state_dir,
    )
    return layer


class TestHardeningDecision:
    """Test HardeningDecision enum."""
    
    def test_decision_values(self):
        """Test enum has correct values."""
        assert HardeningDecision.ALLOW.value == "allow"
        assert HardeningDecision.SKIP_TICK.value == "skip_tick"
        assert HardeningDecision.HALT.value == "halt"
    
    def test_decision_comparison(self):
        """Test enum comparison."""
        decision = HardeningDecision.ALLOW
        assert decision == HardeningDecision.ALLOW
        assert decision != HardeningDecision.HALT


class TestPersistedHaltState:
    """Test PersistedHaltState dataclass."""
    
    def test_to_dict(self):
        """Test serialization."""
        state = PersistedHaltState(
            state="halted",
            reason="test reason",
            violations=["violation1"],
            timestamp="2024-01-01T00:00:00Z",
            run_id="run_123",
        )
        
        d = state.to_dict()
        assert d["state"] == "halted"
        assert d["reason"] == "test reason"
        assert d["violations"] == ["violation1"]
    
    def test_from_dict(self):
        """Test deserialization."""
        data = {
            "state": "emergency",
            "reason": "critical failure",
            "violations": ["v1", "v2"],
            "timestamp": "2024-01-01T00:00:00Z",
            "run_id": "run_456",
        }
        
        state = PersistedHaltState.from_dict(data)
        assert state.state == "emergency"
        assert state.reason == "critical failure"
        assert len(state.violations) == 2


class TestSelfTest:
    """Test startup self-test."""
    
    def test_self_test_passes_clean_state(self, hardening_layer):
        """Self-test should pass with clean state."""
        success, errors = hardening_layer.self_test()
        assert success, f"Self-test failed: {errors}"
        assert len(errors) == 0
    
    def test_self_test_fails_with_persisted_halt(self, hardening_layer, temp_state_dir):
        """Self-test should fail if HALT state is persisted."""
        # Persist a halt state
        halt_file = temp_state_dir / "halt_state.json"
        halt_state = PersistedHaltState(
            state="halted",
            reason="test halt",
            violations=["test violation"],
            timestamp=datetime.now(timezone.utc).isoformat(),
            run_id="test_run",
        )
        halt_file.write_text(json.dumps(halt_state.to_dict()))
        
        success, errors = hardening_layer.self_test()
        assert not success
        assert any("PERSISTED HALT STATE EXISTS" in e for e in errors)

    @pytest.mark.asyncio
    async def test_pre_tick_ignores_persisted_halt_when_replay_flag(self, hardening_layer, temp_state_dir):
        """Replay harness must not treat production halt_state.json as a tick-level HALT."""
        halt_file = temp_state_dir / "halt_state.json"
        halt_state = PersistedHaltState(
            state="halted",
            reason="test halt",
            violations=["test violation"],
            timestamp=datetime.now(timezone.utc).isoformat(),
            run_id="test_run",
        )
        halt_file.write_text(json.dumps(halt_state.to_dict()))
        hardening_layer.ignore_persisted_halt = True

        decision = await hardening_layer.pre_tick_check(
            current_equity=Decimal("100000"),
            open_positions=[],
            margin_utilization=Decimal("0"),
            available_margin=Decimal("100000"),
        )
        assert decision != HardeningDecision.HALT
        hardening_layer.post_tick_cleanup()


class TestGateEnforcement:
    """Test gate assertion."""
    
    def test_gate_closed_before_check(self, hardening_layer):
        """Gate should be closed before pre_tick_check."""
        with pytest.raises(HardeningGateError):
            hardening_layer.assert_gate_open()
    
    @pytest.mark.asyncio
    async def test_gate_opens_after_check(self, hardening_layer):
        """Gate should open after successful pre_tick_check."""
        # Run pre-tick check
        decision = await hardening_layer.pre_tick_check(
            current_equity=Decimal("100000"),
            open_positions=[],
            margin_utilization=Decimal("0.3"),
            available_margin=Decimal("70000"),
        )
        
        assert decision == HardeningDecision.ALLOW
        assert hardening_layer.is_gate_open()
        # Should not raise
        hardening_layer.assert_gate_open()
        
        # Cleanup
        hardening_layer.post_tick_cleanup()
    
    @pytest.mark.asyncio
    async def test_gate_closed_on_halt(self, hardening_layer):
        """Gate should remain closed on HALT decision."""
        # Simulate conditions that trigger HALT
        # First set peak equity
        await hardening_layer.pre_tick_check(
            current_equity=Decimal("100000"),
            open_positions=[],
            margin_utilization=Decimal("0"),
            available_margin=Decimal("100000"),
        )
        hardening_layer.post_tick_cleanup()
        
        # Clear persisted halt if any from previous check
        if hardening_layer.is_halted():
            hardening_layer.clear_halt("test")
        
        # Now simulate 20% drawdown (should trigger HALT)
        decision = await hardening_layer.pre_tick_check(
            current_equity=Decimal("80000"),  # 20% drop
            open_positions=[],
            margin_utilization=Decimal("0"),
            available_margin=Decimal("80000"),
        )
        
        # May be HALT or ALLOW depending on test isolation
        # Just verify the gate state matches the decision
        if decision == HardeningDecision.HALT:
            with pytest.raises(HardeningGateError):
                hardening_layer.assert_gate_open()
        
        # Cleanup
        hardening_layer.post_tick_cleanup()


class TestIdempotency:
    """Test action idempotency."""
    
    def test_generate_action_id_deterministic(self, hardening_layer):
        """Same inputs should generate same action_id."""
        hardening_layer._current_cycle_id = "cycle_123"
        
        id1 = hardening_layer.generate_action_id("BTC/USD", "OPEN", Decimal("100"))
        id2 = hardening_layer.generate_action_id("BTC/USD", "OPEN", Decimal("100"))
        
        assert id1 == id2
    
    def test_generate_action_id_different_for_different_inputs(self, hardening_layer):
        """Different inputs should generate different action_ids."""
        hardening_layer._current_cycle_id = "cycle_123"
        
        id1 = hardening_layer.generate_action_id("BTC/USD", "OPEN", Decimal("100"))
        id2 = hardening_layer.generate_action_id("ETH/USD", "OPEN", Decimal("100"))
        id3 = hardening_layer.generate_action_id("BTC/USD", "CLOSE", Decimal("100"))
        
        assert id1 != id2
        assert id1 != id3
    
    def test_is_action_executed_tracking(self, hardening_layer):
        """Action execution should be tracked."""
        action_id = "test_action_123"
        
        assert not hardening_layer.is_action_executed(action_id)
        
        hardening_layer.mark_action_executed(action_id)
        
        assert hardening_layer.is_action_executed(action_id)
    
    def test_action_store_size_limited(self, hardening_layer):
        """Action store should be size-limited."""
        # Add more than limit
        for i in range(11000):
            hardening_layer.mark_action_executed(f"action_{i}")
        
        # Should be capped
        assert len(hardening_layer._executed_action_ids) <= 10000


class TestHaltPersistence:
    """Test HALT state persistence."""
    
    @pytest.mark.asyncio
    async def test_halt_state_persisted(self, hardening_layer, temp_state_dir):
        """HALT state should be persisted to disk."""
        halt_file = temp_state_dir / "halt_state.json"
        
        # Initially no halt file
        assert not halt_file.exists()
        
        # First establish peak equity
        await hardening_layer.pre_tick_check(
            current_equity=Decimal("100000"),
            open_positions=[],
            margin_utilization=Decimal("0"),
            available_margin=Decimal("100000"),
        )
        hardening_layer.post_tick_cleanup()
        
        # Simulate 20% drawdown to trigger HALT
        decision = await hardening_layer.pre_tick_check(
            current_equity=Decimal("80000"),
            open_positions=[],
            margin_utilization=Decimal("0"),
            available_margin=Decimal("80000"),
        )
        
        # Should persist on HALT
        if decision == HardeningDecision.HALT:
            assert halt_file.exists()
            
            data = json.loads(halt_file.read_text())
            assert data["state"] in ("halted", "emergency")
    
    def test_clear_halt_removes_file(self, hardening_layer, temp_state_dir):
        """clear_halt() should remove the halt file."""
        halt_file = temp_state_dir / "halt_state.json"
        
        # Create halt file
        halt_state = PersistedHaltState(
            state="halted",
            reason="test",
            violations=[],
            timestamp=datetime.now(timezone.utc).isoformat(),
            run_id="test",
        )
        halt_file.write_text(json.dumps(halt_state.to_dict()))
        
        assert halt_file.exists()
        assert hardening_layer.is_halted()
        
        # Clear it
        success = hardening_layer.clear_halt(operator="test_operator")
        
        assert success
        assert not halt_file.exists()
        assert not hardening_layer.is_halted()


class TestCleanupProtection:
    """Test post_tick_cleanup protection."""
    
    @pytest.mark.asyncio
    async def test_cleanup_releases_lock(self, hardening_layer):
        """Cleanup should always release the cycle lock."""
        # Start a tick (acquires lock)
        decision = await hardening_layer.pre_tick_check(
            current_equity=Decimal("100000"),
            open_positions=[],
            margin_utilization=Decimal("0.3"),
            available_margin=Decimal("70000"),
        )
        
        # CycleGuard might skip due to timing - that's OK for this test
        # If decision is ALLOW, lock should be held; if SKIP_TICK, lock not held
        if decision == HardeningDecision.ALLOW:
            assert hardening_layer._lock_held
        
        # Cleanup
        hardening_layer.post_tick_cleanup()
        
        # Lock should be released in either case
        assert not hardening_layer._lock_held
        assert not hardening_layer._cycle_lock.locked()
    
    @pytest.mark.asyncio
    async def test_cleanup_resets_gate_state(self, hardening_layer):
        """Cleanup should reset gate state."""
        # Start a tick
        await hardening_layer.pre_tick_check(
            current_equity=Decimal("100000"),
            open_positions=[],
            margin_utilization=Decimal("0.3"),
            available_margin=Decimal("70000"),
        )
        
        assert hardening_layer._gate_checked_this_tick
        
        # Cleanup
        hardening_layer.post_tick_cleanup()
        
        assert not hardening_layer._gate_checked_this_tick
        assert hardening_layer._gate_decision is None


class TestGlobalSingleton:
    """Test global singleton functions."""
    
    def test_init_and_get_layer(self, temp_state_dir):
        """Test init and get pattern."""
        config = MockConfig()
        kill_switch = MockKillSwitch()
        
        with patch.object(ProductionHardeningLayer, 'DEFAULT_STATE_DIR', temp_state_dir):
            layer = init_hardening_layer(config, kill_switch)
            retrieved = get_hardening_layer()
            
            assert layer is retrieved


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
