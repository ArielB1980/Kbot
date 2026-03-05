"""
Acceptance Tests for Production Safety Mechanisms.

Tests the 6 critical real-world edge case protections:
1. Atomic stop replace (new-first, then cancel old)
2. EXIT_PENDING timeout + escalation
3. Event ordering constraints
4. Write-ahead persistence for intents
5. Shadow mode truth source
6. Invariant K: Always protected after first fill
"""
import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.execution.production_safety import (
    SafetyConfig,
    AtomicStopReplacer,
    StopReplaceContext,
    ProtectionEnforcer,
    EventOrderingEnforcer,
    WriteAheadIntentLog,
    ActionIntent,
    ActionIntentStatus,
    ExitTimeoutManager,
    ExitEscalationLevel,
    ExitEscalationState,
    PositionProtectionMonitor,
    ALIVE_STOP_STATUSES,
    DEAD_STOP_STATUSES,
    FINAL_STOP_STATUSES,
)
from src.execution.position_state_machine import (
    ManagedPosition,
    PositionState,
    PositionRegistry,
    OrderEvent,
    OrderEventType,
    reset_position_registry,
    get_position_registry
)
from src.domain.models import Side


class TestAtomicStopReplace:
    """
    Test 1: Stop "replace semantics" must be atomic.
    
    Cancel-old, place-new creates naked window.
    Must: place-new-first, wait for ACK, then cancel-old.
    """
    
    @pytest.fixture
    def mock_client(self):
        client = AsyncMock()
        return client
    
    @pytest.fixture
    def config(self):
        return SafetyConfig(stop_replace_ack_timeout_seconds=2)
    
    @pytest.fixture
    def position(self):
        return ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-atomic",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
    
    @pytest.mark.asyncio
    async def test_new_stop_placed_before_old_cancelled(self, mock_client, config, position):
        """
        New stop must be placed and ACKed BEFORE old stop is cancelled.
        """
        position.stop_order_id = "old-stop-123"
        position.current_stop_price = Decimal("49000")
        
        # Mock: new stop succeeds immediately
        mock_client.place_futures_order.return_value = {"id": "new-stop-456"}
        mock_client.get_futures_open_orders.return_value = [{"id": "new-stop-456"}]
        mock_client.cancel_futures_order = AsyncMock()
        
        replacer = AtomicStopReplacer(mock_client, config)
        
        ctx = await replacer.replace_stop(
            position,
            Decimal("49500"),
            lambda pos_id, typ: f"stop-{pos_id}"
        )
        
        # New stop should be placed first
        assert mock_client.place_futures_order.called
        assert ctx.new_stop_order_id == "new-stop-456"
        assert ctx.new_stop_acked is True
        
        # Old stop cancelled only after new is confirmed
        # Old stop cancelled only after new is confirmed
        assert mock_client.cancel_futures_order.called
        cancel_call = mock_client.cancel_futures_order.call_args
        assert cancel_call[0][0] == "old-stop-123"
    
    @pytest.mark.asyncio
    async def test_new_stop_fails_keeps_old_stop(self, mock_client, config, position):
        """
        Acceptance test: cancel-old succeeds, place-new fails → 
        position remains protected with old stop.
        """
        position.stop_order_id = "old-stop-123"
        position.current_stop_price = Decimal("49000")
        
        # Mock: new stop placement FAILS (OperationalError — transient API failure)
        from src.exceptions import OperationalError
        mock_client.place_futures_order.side_effect = OperationalError("Exchange error")
        
        replacer = AtomicStopReplacer(mock_client, config)
        
        ctx = await replacer.replace_stop(
            position,
            Decimal("49500"),
            lambda pos_id, typ: f"stop-{pos_id}"
        )
        
        # Should fail safely
        assert ctx.failed is True
        assert "Exchange error" in str(ctx.error)
        
        # Old stop should NOT be cancelled
        assert not mock_client.cancel_futures_order.called
        
        # Position still has old stop
        assert position.stop_order_id == "old-stop-123"
    
    @pytest.mark.asyncio
    async def test_new_stop_not_acked_keeps_old_stop(self, mock_client, config, position):
        """
        If new stop is placed but never ACKed, keep old stop.
        """
        position.stop_order_id = "old-stop-123"
        
        # Mock: new stop placed but never acknowledged
        mock_client.place_futures_order.return_value = {"id": "new-stop-456"}
        mock_client.get_futures_open_orders.return_value = []  # Not in open orders -> not acked/live
        mock_client.cancel_futures_order = AsyncMock()
        
        replacer = AtomicStopReplacer(mock_client, SafetyConfig(stop_replace_ack_timeout_seconds=1))
        
        ctx = await replacer.replace_stop(
            position,
            Decimal("49500"),
            lambda pos_id, typ: f"stop-{pos_id}"
        )
        
        # Should fail because ACK timeout
        assert ctx.failed is True
        assert "not acknowledged" in ctx.error.lower()
        
        # Should try to cancel the failed new stop
        # Old stop should NOT be cancelled
        # The cancel_order call should be for new-stop-456 not old-stop-123
        cancel_calls = mock_client.cancel_futures_order.call_args_list
        if cancel_calls:
            # Only the new stop should be cancelled, not the old
            cancelled_ids = [call[0][0] for call in cancel_calls]
            assert "old-stop-123" not in cancelled_ids


class TestExitPendingTimeout:
    """
    Test 2: EXIT_PENDING must have timeout + escalation.
    """
    
    def test_exit_escalation_normal_to_aggressive(self):
        """After timeout, escalate from NORMAL to AGGRESSIVE."""
        config = SafetyConfig(exit_pending_timeout_seconds=1)
        
        state = ExitEscalationState(
            position_id="test-123",
            symbol="BTC/USD:USD",
            started_at=datetime.now(timezone.utc) - timedelta(seconds=2)
        )
        
        assert state.should_escalate(config) is True
        
        new_level = state.escalate()
        assert new_level == ExitEscalationLevel.AGGRESSIVE
    
    def test_exit_escalation_to_quarantine(self):
        """
        Acceptance test: Exit never fills → escalates to QUARANTINE.
        """
        config = SafetyConfig(
            exit_pending_timeout_seconds=1,
            exit_escalation_max_retries=2
        )
        manager = ExitTimeoutManager(config)
        
        # Simulate position stuck in EXIT_PENDING
        state = ExitEscalationState(
            position_id="test-123",
            symbol="BTC/USD:USD",
            started_at=datetime.now(timezone.utc) - timedelta(seconds=120)
        )
        manager._exit_states["BTC/USD:USD"] = state
        
        # Escalate multiple times
        level1 = manager.escalate("BTC/USD:USD")
        assert level1 == ExitEscalationLevel.AGGRESSIVE
        
        level2 = manager.escalate("BTC/USD:USD")
        assert level2 == ExitEscalationLevel.EMERGENCY
        
        level3 = manager.escalate("BTC/USD:USD")
        assert level3 == ExitEscalationLevel.QUARANTINE
        
        # Symbol should now be quarantined
        assert manager.is_quarantined("BTC/USD:USD") is True


class TestEventOrdering:
    """
    Test 3: Idempotent-by-hash + ordering constraints.
    """
    
    def test_reject_stale_events(self):
        """
        Acceptance test: Apply seq=10 then seq=9 → seq=9 ignored.
        """
        enforcer = EventOrderingEnforcer()
        
        # Process event with seq=10
        should_process = enforcer.should_process_event("order-123", 10)
        assert should_process is True
        enforcer.mark_processed("order-123", 10)
        
        # Try to process stale event with seq=9
        should_process = enforcer.should_process_event("order-123", 9)
        assert should_process is False
        
        # seq=11 should work
        should_process = enforcer.should_process_event("order-123", 11)
        assert should_process is True
    
    def test_deduplicate_by_fill_id(self):
        """Duplicate fills with same fill_id should be rejected."""
        enforcer = EventOrderingEnforcer()
        
        # First fill
        should_process = enforcer.should_process_event("order-123", 1, fill_id="fill-abc")
        assert should_process is True
        enforcer.mark_processed("order-123", 1, fill_id="fill-abc")
        
        # Duplicate fill with same fill_id
        should_process = enforcer.should_process_event("order-123", 2, fill_id="fill-abc")
        assert should_process is False  # Rejected by fill_id
        
        # Different fill_id should work
        should_process = enforcer.should_process_event("order-123", 2, fill_id="fill-xyz")
        assert should_process is True


class TestWriteAheadPersistence:
    """
    Test 4: Write-ahead persistence for action intents.
    """
    
    @pytest.fixture
    def mock_persistence(self):
        """Create mock persistence with in-memory connection."""
        import sqlite3
        
        mock = MagicMock()
        mock._conn = sqlite3.connect(":memory:")
        mock._conn.row_factory = sqlite3.Row
        
        # Create schema
        mock._conn.execute("""
            CREATE TABLE action_intents (
                intent_id TEXT PRIMARY KEY,
                position_id TEXT NOT NULL,
                action_type TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                size TEXT NOT NULL,
                price TEXT,
                created_at TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                exchange_order_id TEXT,
                error TEXT
            )
        """)
        mock._conn.commit()
        
        return mock
    
    def test_intent_persisted_before_execution(self, mock_persistence):
        """Intent must be persisted BEFORE sending to exchange."""
        wal = WriteAheadIntentLog(mock_persistence)
        
        intent = ActionIntent(
            intent_id="client-order-123",
            position_id="pos-abc",
            action_type="entry",
            symbol="BTC/USD:USD",
            side="long",
            size="0.1",
            price="50000",
            created_at=datetime.now(timezone.utc)
        )
        
        # Record intent (this should happen BEFORE exchange call)
        wal.record_intent(intent)
        
        # Verify it's in the database
        cursor = mock_persistence._conn.execute(
            "SELECT * FROM action_intents WHERE intent_id = ?",
            ("client-order-123",)
        )
        row = cursor.fetchone()
        
        assert row is not None
        assert row["status"] == "pending"
        assert row["position_id"] == "pos-abc"
    
    @pytest.mark.asyncio
    async def test_crash_recovery_detects_pending_intent(self, mock_persistence):
        """
        Acceptance test: Crash after "intent persisted" but before exchange call →
        restart must not double-send.
        """
        wal = WriteAheadIntentLog(mock_persistence)
        
        # Simulate: intent was persisted but never sent (crashed)
        intent = ActionIntent(
            intent_id="client-order-crash",
            position_id="pos-abc",
            action_type="entry",
            symbol="BTC/USD:USD",
            side="long",
            size="0.1",
            price="50000",
            created_at=datetime.now(timezone.utc),
            status=ActionIntentStatus.PENDING  # Never sent
        )
        wal.record_intent(intent)
        
        # On restart, should find this pending intent
        pending = wal.get_pending_intents()
        
        assert len(pending) == 1
        assert pending[0].intent_id == "client-order-crash"
        assert pending[0].status == ActionIntentStatus.PENDING
        
        # Reconciliation should mark as failed (never sent)
        mock_client = AsyncMock()
        mock_registry = MagicMock()
        
        resolutions = await wal.reconcile_on_startup(mock_client, mock_registry)
        
        assert resolutions["client-order-crash"] == "cancelled_unsent"


class TestInvariantK_AlwaysProtected:
    """
    Test 6: Invariant K - Always protected after first fill.
    """
    
    @pytest.fixture
    def mock_client(self):
        return AsyncMock()
    
    @pytest.fixture
    def config(self):
        return SafetyConfig(emergency_exit_on_stop_fail=True)
    
    def test_verify_protection_detects_naked_position(self):
        """Detect when position has exposure but no stop."""
        reset_position_registry()
        
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-naked",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        pos.entry_order_id = "entry-1"
        
        # Simulate entry fill - now have exposure
        from src.execution.position_state_machine import FillRecord
        pos.entry_fills.append(FillRecord(
            fill_id="fill-1",
            order_id="entry-1",
            side=Side.LONG,
            qty=Decimal("0.1"),
            price=Decimal("50000"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True
        ))
        pos.state = PositionState.OPEN
        
        # No stop orders on exchange
        exchange_orders = []
        
        enforcer = ProtectionEnforcer(AsyncMock(), SafetyConfig())
        
        # Should detect as unprotected
        import asyncio
        loop = asyncio.new_event_loop()
        is_protected = loop.run_until_complete(
            enforcer.verify_protection(pos, exchange_orders)
        )
        loop.close()
        
        assert is_protected is False
    
    @pytest.mark.asyncio
    async def test_emergency_exit_on_stop_fail(self, mock_client, config):
        """
        Acceptance test: Entry fills, stop fails → 
        system exits at market and quarantines.
        """
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-emergency",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        pos.state = PositionState.OPEN
        
        # Add entry fill
        from src.execution.position_state_machine import FillRecord
        pos.entry_fills.append(FillRecord(
            fill_id="fill-1",
            order_id="entry-1",
            side=Side.LONG,
            qty=Decimal("0.1"),
            price=Decimal("50000"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True
        ))
        
        mock_client.place_futures_order = AsyncMock(return_value={"id": "emergency-exit"})
        
        enforcer = ProtectionEnforcer(mock_client, config)
        
        # Execute emergency exit
        result = await enforcer.emergency_exit_naked_position(pos)
        
        assert result is True
        assert mock_client.place_futures_order.called
        
        # Verify it was a market order
        call_args = mock_client.place_futures_order.call_args
        assert call_args[1]["order_type"] == "market"
        assert call_args[1]["reduce_only"] is True
        
        # Position should be closed
        assert pos.state == PositionState.CLOSED


class TestApplyOrderEventTruthSource:
    """
    Test 5: apply_order_event truth source.
    State transitions happen via event application, not direct mutation.
    """
    
    def test_apply_order_event_transitions_state(self):
        """apply_order_event drives state transition (ACK -> OPEN on fill)."""
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-apply-event",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        pos.entry_order_id = "entry-1"
        
        event = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.FILLED,
            event_seq=1,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.1"),
            fill_price=Decimal("50000"),
            fill_id="fill-1"
        )
        
        result = pos.apply_order_event(event)
        
        assert result is True
        assert pos.state == PositionState.OPEN


class TestIntegrationScenarios:
    """
    Full integration scenarios combining multiple safeguards.
    """
    
    @pytest.mark.asyncio
    async def test_full_lifecycle_with_protection(self):
        """
        Full position lifecycle with all protection mechanisms:
        1. Entry with WAL
        2. Stop placed (protected)
        3. Stop updated atomically
        4. Exit with timeout tracking
        5. Cleanup
        """
        reset_position_registry()
        registry = get_position_registry()
        
        # Create position
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-full-lifecycle",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        registry.register_position(pos)
        
        # Entry filled
        pos.entry_order_id = "entry-1"
        entry_event = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.FILLED,
            event_seq=1,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.1"),
            fill_price=Decimal("50000"),
            fill_id="fill-1"
        )
        pos.apply_order_event(entry_event)
        
        assert pos.state == PositionState.OPEN
        assert pos.remaining_qty == Decimal("0.1")
        
        # Stop order placed (now protected)
        pos.stop_order_id = "stop-1"
        
        # Verify position is protected
        exchange_orders = [{"symbol": "BTC/USD:USD", "type": "stop", "status": "open"}]
        enforcer = ProtectionEnforcer(AsyncMock(), SafetyConfig())
        is_protected = await enforcer.verify_protection(pos, exchange_orders)
        
        assert is_protected is True
        
        # Exit the position
        from src.execution.position_state_machine import ExitReason
        pos.initiate_exit(ExitReason.STOP_LOSS, "exit-1")
        pos.pending_exit_order_id = "exit-1"
        
        assert pos.state == PositionState.EXIT_PENDING
        
        # Track exit timeout
        timeout_manager = ExitTimeoutManager(SafetyConfig())
        timeout_manager.start_exit_tracking(pos)
        
        # Exit fills
        exit_event = OrderEvent(
            order_id="exit-1",
            client_order_id="client-2",
            event_type=OrderEventType.FILLED,
            event_seq=2,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.1"),
            fill_price=Decimal("48500"),
            fill_id="exit-fill-1"
        )
        pos.apply_order_event(exit_event)
        
        assert pos.state == PositionState.CLOSED
        assert pos.remaining_qty == Decimal("0")
        
        # Cleanup exit tracking
        timeout_manager.exit_completed("BTC/USD:USD")


class TestStopFillNotNaked:
    """
    Test: Stop loss fill should NOT trigger NAKED POSITION kill switch.

    Root cause of the 2026-02-09 XRP/USD kill switch incident:
    Stop loss was filled by exchange (expected behavior), but the system
    treated the missing stop order as a safety violation.

    These tests verify the multi-layer defense:
    1. Stop order polling detects fills (poll_and_process_order_updates)
    2. Protection monitor verifies stop status before declaring naked
    3. Graceful position closure when stop fill is confirmed
    """

    @pytest.fixture
    def mock_client(self):
        client = AsyncMock()
        return client

    @pytest.fixture
    def position(self):
        pos = ManagedPosition(
            symbol="XRP/USD",
            side=Side.LONG,
            position_id="test-stop-fill",
            initial_size=Decimal("100"),
            initial_entry_price=Decimal("2.50"),
            initial_stop_price=Decimal("2.40"),
            initial_tp1_price=Decimal("2.70"),
            initial_tp2_price=None,
            initial_final_target=None,
        )
        pos.entry_order_id = "entry-xrp-1"
        pos.stop_order_id = "stop-xrp-1"
        pos.state = PositionState.OPEN
        # Add entry fill so remaining_qty > 0
        from src.execution.position_state_machine import FillRecord
        pos.entry_fills.append(FillRecord(
            fill_id="fill-entry-xrp",
            order_id="entry-xrp-1",
            side=Side.LONG,
            qty=Decimal("100"),
            price=Decimal("2.50"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True,
        ))
        return pos

    @pytest.mark.asyncio
    async def test_stop_filled_not_treated_as_naked(self, mock_client, position):
        """
        When stop loss fills and exchange still shows a brief position,
        the system should verify the stop was filled and NOT flag as naked.
        """
        reset_position_registry()
        registry = get_position_registry()
        registry.register_position(position)

        # Exchange state: position still briefly exists, but stop order is gone
        mock_client.get_futures_open_orders.return_value = []  # No open orders
        mock_client.get_all_futures_positions.return_value = [
            {"symbol": "PF_XRPUSD", "contracts": 100}  # Still shows position
        ]
        # Stop order was FILLED (this is the key verification)
        mock_client.fetch_order.return_value = {
            "id": "stop-xrp-1",
            "status": "closed",
            "filled": 100,
            "average": 2.40,
            "price": 2.40,
        }

        enforcer = ProtectionEnforcer(mock_client, SafetyConfig())
        mock_persistence = MagicMock()
        monitor = PositionProtectionMonitor(
            mock_client, registry, enforcer, persistence=mock_persistence,
        )

        results = await monitor.check_all_positions()

        # Should be treated as PROTECTED (stop filled = expected behavior)
        assert results.get("XRP/USD") is True, (
            f"Expected XRP/USD to be treated as protected, got: {results}"
        )

    @pytest.mark.asyncio
    async def test_stop_cancelled_still_treated_as_naked(self, mock_client, position):
        """
        If the stop was cancelled (not filled), it IS a genuinely naked position.
        """
        reset_position_registry()
        registry = get_position_registry()
        registry.register_position(position)

        # Exchange: position exists, no stop orders
        mock_client.get_futures_open_orders.return_value = []
        mock_client.get_all_futures_positions.return_value = [
            {"symbol": "PF_XRPUSD", "contracts": 100}
        ]
        # Stop was CANCELLED (not filled -- this is a real problem)
        mock_client.fetch_order.return_value = {
            "id": "stop-xrp-1",
            "status": "canceled",
            "filled": 0,
        }

        enforcer = ProtectionEnforcer(mock_client, SafetyConfig())
        monitor = PositionProtectionMonitor(mock_client, registry, enforcer)

        results = await monitor.check_all_positions()

        # Should be flagged as NAKED (genuinely dangerous)
        assert results.get("XRP/USD") is False, (
            f"Expected XRP/USD to be flagged as naked, got: {results}"
        )

    @pytest.mark.asyncio
    async def test_stop_fill_closes_position_gracefully(self, mock_client, position):
        """
        When a stop fill is confirmed, the position should be transitioned
        to CLOSED with exit_reason=STOP_LOSS.
        """
        reset_position_registry()
        registry = get_position_registry()
        registry.register_position(position)

        mock_client.get_futures_open_orders.return_value = []
        mock_client.get_all_futures_positions.return_value = [
            {"symbol": "PF_XRPUSD", "contracts": 100}
        ]
        mock_client.fetch_order.return_value = {
            "id": "stop-xrp-1",
            "status": "closed",
            "filled": 100,
            "average": 2.40,
            "price": 2.40,
        }

        enforcer = ProtectionEnforcer(mock_client, SafetyConfig())
        mock_persistence = MagicMock()
        monitor = PositionProtectionMonitor(
            mock_client, registry, enforcer, persistence=mock_persistence,
        )

        await monitor.check_all_positions()

        # Position should now be CLOSED
        from src.execution.position_state_machine import ExitReason
        assert position.state == PositionState.CLOSED
        assert position.exit_reason == ExitReason.STOP_LOSS
        assert len(position.exit_fills) == 1
        assert position.exit_fills[0].qty == Decimal("100")

        # Persistence: save_position called at least once for closed position (P3 may also call for trade_recorded)
        mock_persistence.save_position.assert_any_call(position)
        assert mock_persistence.save_position.call_count >= 1

    @pytest.mark.asyncio
    async def test_position_closed_on_exchange_treated_as_protected(self, mock_client, position):
        """
        If exchange position size = 0 (stop filled and position fully closed),
        should be treated as protected (Layer 1 defense).
        """
        reset_position_registry()
        registry = get_position_registry()
        registry.register_position(position)

        mock_client.get_futures_open_orders.return_value = []
        mock_client.get_all_futures_positions.return_value = [
            {"symbol": "PF_XRPUSD", "contracts": 0}  # Position gone
        ]

        enforcer = ProtectionEnforcer(mock_client, SafetyConfig())
        monitor = PositionProtectionMonitor(mock_client, registry, enforcer)

        results = await monitor.check_all_positions()

        # Layer 1: position closed on exchange = protected
        assert results.get("XRP/USD") is True

    @pytest.mark.asyncio
    async def test_fetch_order_failure_treats_as_naked(self, mock_client, position):
        """
        If we can't verify the stop order status (API failure),
        treat as naked (fail closed / safe).
        """
        reset_position_registry()
        registry = get_position_registry()
        registry.register_position(position)

        mock_client.get_futures_open_orders.return_value = []
        mock_client.get_all_futures_positions.return_value = [
            {"symbol": "PF_XRPUSD", "contracts": 100}
        ]
        # fetch_order fails (OperationalError — transient API failure)
        from src.exceptions import OperationalError
        mock_client.fetch_order.side_effect = OperationalError("API timeout")

        enforcer = ProtectionEnforcer(mock_client, SafetyConfig())
        monitor = PositionProtectionMonitor(mock_client, registry, enforcer)

        results = await monitor.check_all_positions()

        # Should fail closed: treat as naked when we can't verify
        assert results.get("XRP/USD") is False


class TestEnteredBookNotNaked:
    """
    Test: Kraken Futures 'entered_book' status must NOT trigger kill switch.

    Root cause of the 2026-02-13 ZRO/USD kill switch incident:
    Stop order was triggered (entered_book = resting on order book, waiting to fill)
    but the system treated the transitional status as "unexpected" → naked → kill switch.

    These tests verify:
    1. _check_stop_was_filled returns True for all ALIVE statuses
    2. _check_stop_was_filled returns False for DEAD statuses
    3. _check_stop_was_filled returns True (fail-safe) for unknown statuses
    4. verify_protection accepts entered_book in the order list
    5. Full monitor integration: entered_book → protected (no kill switch)
    """

    @pytest.fixture
    def mock_client(self):
        return AsyncMock()

    @pytest.fixture
    def position(self):
        pos = ManagedPosition(
            symbol="ZRO/USD",
            side=Side.LONG,
            position_id="test-entered-book",
            initial_size=Decimal("10"),
            initial_entry_price=Decimal("2.39"),
            initial_stop_price=Decimal("2.17"),
            initial_tp1_price=Decimal("2.60"),
            initial_tp2_price=None,
            initial_final_target=None,
        )
        pos.entry_order_id = "entry-zro-1"
        pos.stop_order_id = "stop-zro-1"
        pos.state = PositionState.OPEN
        from src.execution.position_state_machine import FillRecord
        pos.entry_fills.append(FillRecord(
            fill_id="fill-entry-zro",
            order_id="entry-zro-1",
            side=Side.LONG,
            qty=Decimal("10"),
            price=Decimal("2.39"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True,
        ))
        return pos

    # ------------------------------------------------------------------
    # Layer 2: _check_stop_was_filled status classification
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", list(ALIVE_STOP_STATUSES))
    async def test_check_stop_alive_statuses_return_true(self, mock_client, position, status):
        """Every ALIVE status must return True (protected)."""
        reset_position_registry()
        registry = get_position_registry()
        registry.register_position(position)

        mock_client.fetch_order.return_value = {
            "id": "stop-zro-1",
            "status": status,
            "filled": 0,
        }

        enforcer = ProtectionEnforcer(mock_client, SafetyConfig())
        monitor = PositionProtectionMonitor(mock_client, registry, enforcer)

        result = await monitor._check_stop_was_filled(position, exchange_size=10)
        assert result is True, f"Expected True for alive status '{status}', got False"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", list(DEAD_STOP_STATUSES))
    async def test_check_stop_dead_statuses_return_false(self, mock_client, position, status):
        """Every DEAD status must return False (genuinely naked)."""
        reset_position_registry()
        registry = get_position_registry()
        registry.register_position(position)

        mock_client.fetch_order.return_value = {
            "id": "stop-zro-1",
            "status": status,
            "filled": 0,
        }

        enforcer = ProtectionEnforcer(mock_client, SafetyConfig())
        monitor = PositionProtectionMonitor(mock_client, registry, enforcer)

        result = await monitor._check_stop_was_filled(position, exchange_size=10)
        assert result is False, f"Expected False for dead status '{status}', got True"

    @pytest.mark.asyncio
    async def test_check_stop_unknown_status_failsafe_returns_true(self, mock_client, position):
        """Unknown/unfamiliar status must fail-safe to True (don't kill-switch)."""
        reset_position_registry()
        registry = get_position_registry()
        registry.register_position(position)

        mock_client.fetch_order.return_value = {
            "id": "stop-zro-1",
            "status": "some_totally_unknown_status",
            "filled": 0,
        }

        enforcer = ProtectionEnforcer(mock_client, SafetyConfig())
        monitor = PositionProtectionMonitor(mock_client, registry, enforcer)

        result = await monitor._check_stop_was_filled(position, exchange_size=10)
        assert result is True, "Unknown status should fail-safe to protected (True)"

    @pytest.mark.asyncio
    async def test_check_stop_closed_zero_fills_failsafe_returns_true(self, mock_client, position):
        """Closed with 0 fills is ambiguous — fail-safe to True."""
        reset_position_registry()
        registry = get_position_registry()
        registry.register_position(position)

        mock_client.fetch_order.return_value = {
            "id": "stop-zro-1",
            "status": "closed",
            "filled": 0,
        }

        enforcer = ProtectionEnforcer(mock_client, SafetyConfig())
        monitor = PositionProtectionMonitor(mock_client, registry, enforcer)

        result = await monitor._check_stop_was_filled(position, exchange_size=10)
        assert result is True, "Closed with 0 fills should fail-safe to protected"

    # ------------------------------------------------------------------
    # Layer 1: verify_protection accepts ALIVE statuses
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_verify_protection_accepts_entered_book(self, mock_client, position):
        """If a stop order appears with status 'entered_book', it should count as protected."""
        exchange_orders = [
            {
                "symbol": "ZRO/USD:USD",
                "type": "stop",
                "status": "entered_book",
                "reduceOnly": True,
            }
        ]
        enforcer = ProtectionEnforcer(mock_client, SafetyConfig())
        result = await enforcer.verify_protection(position, exchange_orders)
        assert result is True, "entered_book stop should be recognized as protection"

    @pytest.mark.asyncio
    async def test_verify_protection_accepts_untouched(self, mock_client, position):
        """Kraken 'untouched' = stop not triggered yet → definitely protected."""
        exchange_orders = [
            {
                "symbol": "ZRO/USD:USD",
                "type": "stop",
                "status": "untouched",
                "reduceOnly": True,
            }
        ]
        enforcer = ProtectionEnforcer(mock_client, SafetyConfig())
        result = await enforcer.verify_protection(position, exchange_orders)
        assert result is True, "untouched stop should be recognized as protection"

    @pytest.mark.asyncio
    async def test_verify_protection_rejects_cancelled_stop(self, mock_client, position):
        """Cancelled stop should NOT count as protection."""
        exchange_orders = [
            {
                "symbol": "ZRO/USD:USD",
                "type": "stop",
                "status": "cancelled",
                "reduceOnly": True,
            }
        ]
        enforcer = ProtectionEnforcer(mock_client, SafetyConfig())
        result = await enforcer.verify_protection(position, exchange_orders)
        assert result is False, "cancelled stop should not count as protection"

    # ------------------------------------------------------------------
    # Full integration: entered_book through the complete monitor
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_full_monitor_entered_book_protected(self, mock_client, position):
        """
        Full integration test reproducing the 2026-02-13 ZRO/USD incident.
        Stop has status 'entered_book' (triggered, on book).
        Neither fetch_open_orders nor verify_protection find it.
        Layer 2 (_check_stop_was_filled) fetches by ID → 'entered_book' → protected.
        Kill switch must NOT fire.
        """
        reset_position_registry()
        registry = get_position_registry()
        registry.register_position(position)

        # Exchange state: position exists, stop NOT in open orders (entered_book is transitional)
        mock_client.get_futures_open_orders.return_value = []
        mock_client.get_all_futures_positions.return_value = [
            {"symbol": "PF_ZROUSD", "contracts": 10}
        ]
        # Layer 2: fetch_order finds the stop alive with entered_book
        mock_client.fetch_order.return_value = {
            "id": "stop-zro-1",
            "status": "entered_book",
            "filled": 0,
        }

        enforcer = ProtectionEnforcer(mock_client, SafetyConfig())
        monitor = PositionProtectionMonitor(mock_client, registry, enforcer)

        results = await monitor.check_all_positions()

        assert results.get("ZRO/USD") is True, (
            f"Expected ZRO/USD to be protected (entered_book), got: {results}. "
            "This would have caused a false kill-switch activation."
        )

    @pytest.mark.asyncio
    async def test_full_monitor_layer2_rescue_logged(self, mock_client, position):
        """Verify Layer 2 rescue produces the expected info log."""
        reset_position_registry()
        registry = get_position_registry()
        registry.register_position(position)

        mock_client.get_futures_open_orders.return_value = []
        mock_client.get_all_futures_positions.return_value = [
            {"symbol": "PF_ZROUSD", "contracts": 10}
        ]
        mock_client.fetch_order.return_value = {
            "id": "stop-zro-1",
            "status": "entered_book",
            "filled": 0,
        }

        enforcer = ProtectionEnforcer(mock_client, SafetyConfig())
        monitor = PositionProtectionMonitor(mock_client, registry, enforcer)

        with patch("src.execution.production_safety.logger") as mock_logger:
            results = await monitor.check_all_positions()

            # Should see the Layer 2 rescue log
            info_calls = [
                str(c) for c in mock_logger.info.call_args_list
            ]
            rescue_logged = any("Layer 2" in c and "ALIVE" in c for c in info_calls)
            assert rescue_logged, (
                f"Expected 'Layer 1 missed stop but Layer 2 confirmed ALIVE' log. "
                f"Info calls: {info_calls}"
            )


class TestStopPendingGrace:
    """Stop-pending grace should suppress only short-lived ambiguous naked states."""

    @pytest.fixture
    def mock_client(self):
        return AsyncMock()

    @pytest.fixture
    def position_no_stop(self):
        pos = ManagedPosition(
            symbol="TIA/USD",
            side=Side.SHORT,
            position_id="test-grace-no-stop",
            initial_size=Decimal("100"),
            initial_entry_price=Decimal("0.33"),
            initial_stop_price=Decimal("0.36"),
            initial_tp1_price=Decimal("0.31"),
            initial_tp2_price=None,
            initial_final_target=None,
        )
        pos.state = PositionState.OPEN
        pos.stop_order_id = None
        from src.execution.position_state_machine import FillRecord
        pos.entry_fills.append(FillRecord(
            fill_id="fill-entry-tia",
            order_id="entry-tia-1",
            side=Side.SHORT,
            qty=Decimal("100"),
            price=Decimal("0.33"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True,
        ))
        return pos

    @pytest.mark.asyncio
    async def test_recent_position_without_stop_uses_pending_grace(self, mock_client, position_no_stop):
        reset_position_registry()
        registry = get_position_registry()
        registry.register_position(position_no_stop)

        # Position still exists on exchange; no open stop visible yet.
        mock_client.get_futures_open_orders.return_value = []
        mock_client.get_all_futures_positions.return_value = [
            {"symbol": "PF_TIAUSD", "contracts": 100}
        ]

        enforcer = ProtectionEnforcer(mock_client, SafetyConfig(stop_pending_grace_seconds=120))
        monitor = PositionProtectionMonitor(mock_client, registry, enforcer)

        results = await monitor.check_all_positions()
        assert results.get("TIA/USD") is True

    @pytest.mark.asyncio
    async def test_fresh_reconcile_grace_suppresses_invariant_k_log(self, mock_client, position_no_stop):
        """
        Newly updated reconciled positions get a short grace window before
        emitting INVARIANT K, preventing false criticals during stop bind races.
        """
        reset_position_registry()
        registry = get_position_registry()
        registry.register_position(position_no_stop)

        # Freshly reconciled now.
        position_no_stop.updated_at = datetime.now(timezone.utc)
        position_no_stop.stop_order_id = None

        mock_client.get_futures_open_orders.return_value = []
        mock_client.get_all_futures_positions.return_value = [
            {"symbol": "PF_TIAUSD", "contracts": 100}
        ]

        enforcer = ProtectionEnforcer(
            mock_client,
            SafetyConfig(
                stop_pending_grace_seconds=0,
                reconcile_fresh_grace_seconds=3,
            ),
        )
        monitor = PositionProtectionMonitor(mock_client, registry, enforcer)

        with patch("src.execution.production_safety.logger") as mock_logger:
            results = await monitor.check_all_positions()

        assert results.get("TIA/USD") is True
        critical_logs = [str(c) for c in mock_logger.critical.call_args_list]
        assert not any("INVARIANT K VIOLATION" in c for c in critical_logs)

    @pytest.mark.asyncio
    async def test_old_position_without_stop_still_flags_naked(self, mock_client, position_no_stop):
        reset_position_registry()
        registry = get_position_registry()
        registry.register_position(position_no_stop)

        # Make the position old enough that grace no longer applies.
        old = datetime.now(timezone.utc) - timedelta(seconds=300)
        position_no_stop.created_at = old
        position_no_stop.updated_at = old

        mock_client.get_futures_open_orders.return_value = []
        mock_client.get_all_futures_positions.return_value = [
            {"symbol": "PF_TIAUSD", "contracts": 100}
        ]

        enforcer = ProtectionEnforcer(mock_client, SafetyConfig(stop_pending_grace_seconds=60))
        monitor = PositionProtectionMonitor(mock_client, registry, enforcer)

        results = await monitor.check_all_positions()
        assert results.get("TIA/USD") is False

    @pytest.mark.asyncio
    async def test_layer2_alive_status_stays_protected_without_grace(self, mock_client):
        reset_position_registry()
        registry = get_position_registry()

        pos = ManagedPosition(
            symbol="AAVE/USD",
            side=Side.SHORT,
            position_id="test-grace-layer2",
            initial_size=Decimal("1"),
            initial_entry_price=Decimal("110"),
            initial_stop_price=Decimal("114"),
            initial_tp1_price=Decimal("106"),
            initial_tp2_price=None,
            initial_final_target=None,
        )
        pos.state = PositionState.OPEN
        pos.stop_order_id = "stop-aave-1"
        pos.created_at = datetime.now(timezone.utc) - timedelta(seconds=600)
        pos.updated_at = datetime.now(timezone.utc) - timedelta(seconds=600)
        from src.execution.position_state_machine import FillRecord
        pos.entry_fills.append(FillRecord(
            fill_id="fill-entry-aave",
            order_id="entry-aave-1",
            side=Side.SHORT,
            qty=Decimal("1"),
            price=Decimal("110"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True,
        ))
        registry.register_position(pos)

        mock_client.get_futures_open_orders.return_value = []
        mock_client.get_all_futures_positions.return_value = [
            {"symbol": "PF_AAVEUSD", "contracts": 1}
        ]
        mock_client.fetch_order.return_value = {
            "id": "stop-aave-1",
            "status": "entered_book",
            "filled": 0,
        }

        enforcer = ProtectionEnforcer(mock_client, SafetyConfig(stop_pending_grace_seconds=30))
        monitor = PositionProtectionMonitor(mock_client, registry, enforcer)

        results = await monitor.check_all_positions()
        assert results.get("AAVE/USD") is True


class TestStopSemanticValidation:
    """
    Test: _warn_if_stop_semantically_wrong detects substantive mismatches
    without changing the protection verdict.

    These are warning/error-level log checks. The stop is still treated as
    protected, but logs make the issue observable for manual follow-up.

    Two severity bands:
      - CRITICAL (logger.error): wrong side, reduceOnly=False, amount < 25%, TP type
      - WARNING (logger.warning): amount < 90%, reduceOnly missing, type ambiguous
    """

    @pytest.fixture
    def long_position(self):
        pos = ManagedPosition(
            symbol="ZRO/USD",
            side=Side.LONG,
            position_id="test-semantic",
            initial_size=Decimal("10"),
            initial_entry_price=Decimal("2.39"),
            initial_stop_price=Decimal("2.17"),
            initial_tp1_price=Decimal("2.60"),
            initial_tp2_price=None,
            initial_final_target=None,
        )
        pos.state = PositionState.OPEN
        from src.execution.position_state_machine import FillRecord
        pos.entry_fills.append(FillRecord(
            fill_id="fill-1",
            order_id="entry-1",
            side=Side.LONG,
            qty=Decimal("10"),
            price=Decimal("2.39"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True,
        ))
        pos.stop_order_id = "stop-1"
        return pos

    def _make_monitor(self):
        mock_client = AsyncMock()
        enforcer = ProtectionEnforcer(mock_client, SafetyConfig())
        return PositionProtectionMonitor(mock_client, PositionRegistry(), enforcer)

    # ------------------------------------------------------------------
    # Correct stop → no warnings or errors
    # ------------------------------------------------------------------

    def test_correct_stop_no_warning(self, long_position):
        """A correct stop (sell, reduceOnly, full size, stop type) should not warn."""
        order_data = {
            "id": "stop-1", "side": "sell", "type": "stop",
            "amount": 10, "reduceOnly": True, "stopPrice": 2.17,
            "status": "entered_book", "filled": 0,
        }
        monitor = self._make_monitor()
        with patch("src.execution.production_safety.logger") as mock_logger:
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)
            assert mock_logger.warning.call_count == 0, "No warnings expected"
            assert mock_logger.error.call_count == 0, "No errors expected"

    # ------------------------------------------------------------------
    # CRITICAL issues → logger.error
    # ------------------------------------------------------------------

    def test_wrong_side_is_critical(self, long_position):
        """A long position with a BUY stop should log at ERROR level."""
        order_data = {
            "id": "stop-1", "side": "buy", "type": "stop",
            "amount": 10, "reduceOnly": True, "stopPrice": 2.17,
        }
        monitor = self._make_monitor()
        with patch("src.execution.production_safety.logger") as mock_logger:
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)
            assert mock_logger.error.call_count == 1
            call_str = str(mock_logger.error.call_args)
            assert "CRITICAL" in call_str
            assert "side=buy" in call_str

    def test_reduce_only_false_is_critical(self, long_position):
        """reduceOnly=False should log at ERROR level."""
        order_data = {
            "id": "stop-1", "side": "sell", "type": "stop",
            "amount": 10, "reduceOnly": False, "stopPrice": 2.17,
        }
        monitor = self._make_monitor()
        with patch("src.execution.production_safety.logger") as mock_logger:
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)
            assert mock_logger.error.call_count == 1
            call_str = str(mock_logger.error.call_args)
            assert "reduceOnly=False" in call_str

    def test_amount_below_25pct_is_critical(self, long_position):
        """Coverage < 25% should log at ERROR level (basically ineffective)."""
        order_data = {
            "id": "stop-1", "side": "sell", "type": "stop",
            "amount": 2, "reduceOnly": True, "stopPrice": 2.17,  # 20% of 10
        }
        monitor = self._make_monitor()
        with patch("src.execution.production_safety.logger") as mock_logger:
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)
            assert mock_logger.error.call_count == 1
            call_str = str(mock_logger.error.call_args)
            assert "basically ineffective" in call_str

    def test_take_profit_type_is_critical(self, long_position):
        """If the 'stop' is actually a take_profit, should log ERROR."""
        order_data = {
            "id": "stop-1", "side": "sell", "type": "take_profit",
            "amount": 10, "reduceOnly": True,
        }
        monitor = self._make_monitor()
        with patch("src.execution.production_safety.logger") as mock_logger:
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)
            assert mock_logger.error.call_count == 1
            call_str = str(mock_logger.error.call_args)
            assert "TP, not SL" in call_str

    # ------------------------------------------------------------------
    # Standard warnings → logger.warning
    # ------------------------------------------------------------------

    def test_amount_between_25_and_90_pct_warns(self, long_position):
        """Coverage 25-90% should log WARNING (partial, not critical)."""
        order_data = {
            "id": "stop-1", "side": "sell", "type": "stop",
            "amount": 7, "reduceOnly": True, "stopPrice": 2.17,  # 70% of 10
        }
        monitor = self._make_monitor()
        with patch("src.execution.production_safety.logger") as mock_logger:
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)
            assert mock_logger.error.call_count == 0, "Should not be critical"
            assert mock_logger.warning.call_count == 1
            call_str = str(mock_logger.warning.call_args)
            assert "won't fully protect" in call_str

    def test_reduce_only_missing_warns_softly(self, long_position):
        """reduceOnly=None (missing from response) should soft-warn, not error."""
        order_data = {
            "id": "stop-1", "side": "sell", "type": "stop",
            "amount": 10, "reduceOnly": None, "stopPrice": 2.17,
        }
        monitor = self._make_monitor()
        with patch("src.execution.production_safety.logger") as mock_logger:
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)
            assert mock_logger.error.call_count == 0, "Missing != False"
            assert mock_logger.warning.call_count == 1
            call_str = str(mock_logger.warning.call_args)
            assert "missing" in call_str

    def test_type_market_with_stop_price_no_warn(self, long_position):
        """type='market' but stopPrice present → legitimate stop-market, no warn."""
        order_data = {
            "id": "stop-1", "side": "sell", "type": "market",
            "amount": 10, "reduceOnly": True, "stopPrice": 2.17,
        }
        monitor = self._make_monitor()
        with patch("src.execution.production_safety.logger") as mock_logger:
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)
            assert mock_logger.warning.call_count == 0
            assert mock_logger.error.call_count == 0

    def test_type_limit_without_stop_price_warns(self, long_position):
        """type='limit' and no stopPrice → not a stop, should warn."""
        order_data = {
            "id": "stop-1", "side": "sell", "type": "limit",
            "amount": 10, "reduceOnly": True, "stopPrice": None,
        }
        monitor = self._make_monitor()
        with patch("src.execution.production_safety.logger") as mock_logger:
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)
            assert mock_logger.warning.call_count == 1
            call_str = str(mock_logger.warning.call_args)
            assert "expected stop variant" in call_str

    # ------------------------------------------------------------------
    # Counters and raw dump
    # ------------------------------------------------------------------

    def test_error_counter_increments(self, long_position):
        """Semantic error counter should increment per occurrence."""
        order_data = {
            "id": "stop-1", "side": "buy", "type": "stop",
            "amount": 10, "reduceOnly": True, "stopPrice": 2.17,
        }
        monitor = self._make_monitor()
        assert monitor._semantic_error_counts.get("ZRO/USD", 0) == 0

        with patch("src.execution.production_safety.logger"):
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)

        assert monitor._semantic_error_counts["ZRO/USD"] == 2

    def test_warning_counter_increments(self, long_position):
        """Semantic warning counter should increment per occurrence."""
        order_data = {
            "id": "stop-1", "side": "sell", "type": "stop",
            "amount": 7, "reduceOnly": True, "stopPrice": 2.17,
        }
        monitor = self._make_monitor()

        with patch("src.execution.production_safety.logger"):
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)

        assert monitor._semantic_warning_counts["ZRO/USD"] == 3

    def test_reduceonly_missing_counter(self, long_position):
        """reduceOnly=None should increment the missing counter."""
        order_data = {
            "id": "stop-1", "side": "sell", "type": "stop",
            "amount": 10, "reduceOnly": None, "stopPrice": 2.17,
        }
        monitor = self._make_monitor()

        with patch("src.execution.production_safety.logger"):
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)

        assert monitor._reduceonly_missing_counts["ZRO/USD"] == 1

    def test_raw_dump_emitted_once_per_symbol(self, long_position):
        """Raw CCXT dump should only be logged once per symbol per reset cycle."""
        order_data = {
            "id": "stop-1", "side": "buy", "type": "stop",
            "amount": 10, "reduceOnly": True, "stopPrice": 2.17,
        }
        monitor = self._make_monitor()

        with patch("src.execution.production_safety.logger") as mock_logger:
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)

            # raw dump logged exactly once (first error only)
            dump_calls = [
                c for c in mock_logger.warning.call_args_list
                if "raw order payload" in str(c).lower()
            ]
            assert len(dump_calls) == 1

    def test_reset_clears_counters(self, long_position):
        """reset_semantic_counters should clear all state."""
        order_data = {
            "id": "stop-1", "side": "buy", "type": "stop",
            "amount": 10, "reduceOnly": None, "stopPrice": 2.17,
        }
        monitor = self._make_monitor()

        with patch("src.execution.production_safety.logger"):
            monitor._warn_if_stop_semantically_wrong(long_position, order_data)

        assert monitor._semantic_error_counts.get("ZRO/USD", 0) > 0

        with patch("src.execution.production_safety.logger"):
            monitor.reset_semantic_counters()

        assert monitor._semantic_error_counts == {}
        assert monitor._semantic_warning_counts == {}
        assert monitor._reduceonly_missing_counts == {}
        assert monitor._raw_dump_emitted == set()

    def test_get_semantic_counts(self, long_position):
        """get_semantic_counts should return current state for monitoring."""
        monitor = self._make_monitor()
        monitor._semantic_error_counts["ZRO/USD"] = 3
        monitor._semantic_warning_counts["ZRO/USD"] = 1
        monitor._reduceonly_missing_counts["ETH/USD"] = 2

        counts = monitor.get_semantic_counts()
        assert counts["errors"] == {"ZRO/USD": 3}
        assert counts["warnings"] == {"ZRO/USD": 1}
        assert counts["reduceonly_missing"] == {"ETH/USD": 2}

    # ------------------------------------------------------------------
    # Integration: mismatches still don't change the verdict
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_semantic_check_does_not_change_verdict(self, long_position):
        """Even with all mismatches, the stop is still treated as protected."""
        reset_position_registry()
        registry = get_position_registry()
        registry.register_position(long_position)

        mock_client = AsyncMock()
        mock_client.get_futures_open_orders.return_value = []
        mock_client.get_all_futures_positions.return_value = [
            {"symbol": "PF_ZROUSD", "contracts": 10}
        ]
        # Stop is alive but semantically wrong in every way
        mock_client.fetch_order.return_value = {
            "id": "stop-1",
            "status": "entered_book",
            "filled": 0,
            "side": "buy",          # Wrong
            "type": "limit",        # Wrong
            "amount": 2,            # Undersized (<25%)
            "reduceOnly": False,    # Wrong
            "stopPrice": None,      # Missing
        }

        enforcer = ProtectionEnforcer(mock_client, SafetyConfig())
        monitor = PositionProtectionMonitor(mock_client, registry, enforcer)

        results = await monitor.check_all_positions()

        # Still protected — semantic issues don't trigger kill switch
        assert results.get("ZRO/USD") is True, (
            "Semantic mismatches should produce warnings, not change the verdict"
        )


class TestStopOrderPolling:
    """
    Test: poll_and_process_order_updates now polls stop orders too.
    """

    @pytest.mark.asyncio
    async def test_stop_orders_are_polled(self):
        """
        Stop orders (STOP_INITIAL, STOP_UPDATE) should be included in polling,
        not just ENTRY orders.
        """
        from src.execution.execution_gateway import (
            ExecutionGateway,
            PendingOrder,
            OrderPurpose,
        )
        from src.execution.position_manager_v2 import PositionManagerV2
        from src.domain.models import OrderType

        mock_client = AsyncMock()
        mock_registry = MagicMock()
        mock_persistence = MagicMock()

        gateway = ExecutionGateway.__new__(ExecutionGateway)
        gateway.client = mock_client
        gateway.registry = mock_registry
        gateway.persistence = mock_persistence
        gateway.position_manager = PositionManagerV2(mock_registry)
        gateway._on_partial_close = None
        gateway._on_trade_recorded = None
        gateway._wal = None
        gateway._event_enforcer = None
        gateway._startup_machine = None
        gateway._stop_replacer = None
        gateway._order_rate_limiter = MagicMock()
        gateway._order_rate_limiter.check_and_record = MagicMock()
        gateway.metrics = {
            "orders_submitted": 0, "orders_filled": 0,
            "orders_cancelled": 0, "orders_rejected": 0,
            "events_processed": 0,
        }
        gateway._pending_orders = {}
        gateway._order_id_map = {}

        # Add a stop order to pending
        stop_pending = PendingOrder(
            client_order_id="stop-client-1",
            position_id="pos-btc",
            symbol="BTC/USD",
            purpose=OrderPurpose.STOP_INITIAL,
            side=Side.SHORT,
            size=Decimal("0.1"),
            price=Decimal("49000"),
            order_type=OrderType.STOP_LOSS,
            submitted_at=datetime.now(timezone.utc),
            exchange_order_id="stop-exch-1",
            status="submitted",
            exchange_symbol="BTC/USD:USD",
        )
        gateway._pending_orders["stop-client-1"] = stop_pending

        # Mock fetch_order to return stop as filled
        mock_client.fetch_order.return_value = {
            "id": "stop-exch-1",
            "clientOrderId": "stop-client-1",
            "status": "closed",
            "filled": 0.1,
            "remaining": 0,
            "average": 49000,
            "trades": [{"id": "trade-1"}],
        }

        # Mock position for process_order_update
        mock_pos = MagicMock()
        mock_pos.stop_order_id = "stop-exch-1"
        mock_pos.stop_client_order_id = "stop-client-1"
        mock_pos.entry_order_id = None
        mock_pos.pending_exit_order_id = None
        mock_pos.pending_exit_client_order_id = None
        mock_pos.side = Side.LONG
        mock_pos.remaining_qty = Decimal("0.1")
        mock_pos.is_terminal = False
        mock_pos.processed_event_hashes = set()
        mock_registry.get_position.return_value = mock_pos

        # The stop order should be polled (not skipped)
        mock_client.fetch_order.assert_not_called()
        processed = await gateway.poll_and_process_order_updates()

        # fetch_order should have been called for the stop order
        mock_client.fetch_order.assert_called_once_with("stop-exch-1", "BTC/USD:USD")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
