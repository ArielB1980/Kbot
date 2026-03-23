"""
Tests for Position State Machine - Production Grade.

Tests cover:
1. Invariant A-E enforcement
2. In-flight states (EXIT_PENDING, etc.)
3. Idempotent event handling
4. Partial fills
5. Conditional BE logic
6. Persistence and recovery
7. Reconciliation
"""
import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta
import tempfile
import os

from src.execution.position_state_machine import (
    ManagedPosition,
    PositionState,
    PositionRegistry,
    ExitReason,
    OrderEvent,
    OrderEventType,
    FillRecord,
    InvariantViolation,
    reset_position_registry,
    get_position_registry,
    set_position_registry,
    check_invariant
)
from src.domain.models import Side


class TestInvariants:
    """Test Invariant A-E enforcement."""
    
    def setup_method(self):
        reset_position_registry()
    
    def _create_position(self, symbol: str = "BTC/USD:USD", side: Side = Side.LONG) -> ManagedPosition:
        return ManagedPosition(
            symbol=symbol,
            side=side,
            position_id=f"test-{symbol}-{side.value}",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000") if side == Side.LONG else Decimal("50000"),
            initial_stop_price=Decimal("49000") if side == Side.LONG else Decimal("51000"),
            initial_tp1_price=Decimal("52000") if side == Side.LONG else Decimal("48000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
    
    def test_invariant_a_single_position_per_symbol(self):
        """Invariant A: At most one non-terminal position per symbol."""
        registry = get_position_registry()
        
        pos1 = self._create_position("BTC/USD:USD", Side.LONG)
        registry.register_position(pos1)
        
        # Create a DIFFERENT position (different position_id) for the same symbol
        # This should fail because it's a different position, not the same one registered twice
        pos2 = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="different-position-id",  # Different position_id = different position
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        
        with pytest.raises(InvariantViolation, match="Cannot register position"):
            registry.register_position(pos2)
    
    def test_invariant_b_remaining_qty_never_negative(self):
        """Invariant B: remaining_qty = entry_qty - exit_qty >= 0."""
        pos = self._create_position()
        
        # Add entry fill
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
        pos.entry_order_id = "entry-1"
        pos.apply_order_event(entry_event)
        
        assert pos.remaining_qty == Decimal("0.1")
        
        # Add exit fill
        exit_event = OrderEvent(
            order_id="exit-1",
            client_order_id="client-2",
            event_type=OrderEventType.FILLED,
            event_seq=2,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.1"),
            fill_price=Decimal("52000"),
            fill_id="fill-2"
        )
        pos.pending_exit_order_id = "exit-1"
        pos.apply_order_event(exit_event)
        
        assert pos.remaining_qty == Decimal("0")

    def test_invariant_b_duplicate_terminal_exit_fill_is_ignored(self):
        """Terminal positions must ignore additional exit fills."""
        pos = self._create_position()

        entry_event = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.FILLED,
            event_seq=1,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.1"),
            fill_price=Decimal("50000"),
            fill_id="fill-1",
        )
        pos.entry_order_id = "entry-1"
        assert pos.apply_order_event(entry_event) is True

        exit_event = OrderEvent(
            order_id="exit-1",
            client_order_id="client-2",
            event_type=OrderEventType.FILLED,
            event_seq=2,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.1"),
            fill_price=Decimal("52000"),
            fill_id="fill-2",
        )
        pos.pending_exit_order_id = "exit-1"
        assert pos.apply_order_event(exit_event) is True
        assert pos.remaining_qty == Decimal("0")
        assert pos.is_terminal is True
        assert len(pos.exit_fills) == 1

        duplicate_exit_event = OrderEvent(
            order_id="exit-1",
            client_order_id="client-2",
            event_type=OrderEventType.FILLED,
            event_seq=3,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.05"),
            fill_price=Decimal("52000"),
            fill_id="fill-3",
        )
        assert pos.apply_order_event(duplicate_exit_event) is False
        assert pos.remaining_qty == Decimal("0")
        assert len(pos.exit_fills) == 1

    def test_invariant_b_overflow_exit_fill_is_capped(self):
        """Exit fill qty larger than remaining must be capped."""
        pos = self._create_position()

        entry_event = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.FILLED,
            event_seq=1,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.1"),
            fill_price=Decimal("50000"),
            fill_id="fill-1",
        )
        pos.entry_order_id = "entry-1"
        assert pos.apply_order_event(entry_event) is True
        assert pos.remaining_qty == Decimal("0.1")

        overflow_exit_event = OrderEvent(
            order_id="exit-1",
            client_order_id="client-2",
            event_type=OrderEventType.FILLED,
            event_seq=2,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.3"),
            fill_price=Decimal("52000"),
            fill_id="fill-2",
        )
        pos.pending_exit_order_id = "exit-1"
        assert pos.apply_order_event(overflow_exit_event) is True

        assert pos.remaining_qty == Decimal("0")
        assert pos.is_terminal is True
        assert len(pos.exit_fills) == 1
        assert pos.exit_fills[0].qty == Decimal("0.1")
    
    def test_invariant_c_immutables_locked_after_ack(self):
        """Invariant C: Immutable fields locked after entry acknowledgement."""
        pos = self._create_position()
        pos.entry_order_id = "entry-1"
        
        # Before ack - can still modify initial (though shouldn't need to)
        original_stop = pos.initial_stop_price
        
        # Acknowledge entry
        ack_event = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.ACKNOWLEDGED,
            event_seq=1,
            timestamp=datetime.now(timezone.utc)
        )
        pos.apply_order_event(ack_event)
        
        assert pos.entry_acknowledged is True
        
        # After ack - stop can only move TOWARD profit
        assert pos.update_stop(Decimal("49500")) is True  # Improve OK
        assert pos.update_stop(Decimal("49000")) is False  # Back to initial - blocked
        assert pos.update_stop(Decimal("48000")) is False  # Below initial - blocked
    
    def test_invariant_d_stop_monotonic_long(self):
        """Invariant D: LONG stop can only move UP."""
        pos = self._create_position(side=Side.LONG)
        pos.entry_order_id = "entry-1"
        pos.entry_acknowledged = True
        
        # Initial stop at 49000
        assert pos.current_stop_price == Decimal("49000")
        
        # Move up - allowed
        assert pos.update_stop(Decimal("49500")) is True
        assert pos.current_stop_price == Decimal("49500")
        
        # Move up more - allowed
        assert pos.update_stop(Decimal("50000")) is True
        
        # Move down - blocked
        assert pos.update_stop(Decimal("49800")) is False
        assert pos.current_stop_price == Decimal("50000")  # Unchanged
    
    def test_invariant_d_stop_monotonic_short(self):
        """Invariant D: SHORT stop can only move DOWN."""
        pos = self._create_position(side=Side.SHORT)
        pos.entry_order_id = "entry-1"
        pos.entry_acknowledged = True
        
        # Initial stop at 51000
        assert pos.current_stop_price == Decimal("51000")
        
        # Move down - allowed
        assert pos.update_stop(Decimal("50500")) is True
        
        # Move down more - allowed
        assert pos.update_stop(Decimal("50000")) is True
        
        # Move up - blocked
        assert pos.update_stop(Decimal("50200")) is False
        assert pos.current_stop_price == Decimal("50000")
    
    def test_invariant_e_no_reversal_without_close(self):
        """Invariant E: Cannot open opposite direction until current is terminal."""
        registry = get_position_registry()
        
        pos_long = self._create_position(side=Side.LONG)
        registry.register_position(pos_long)
        
        # Simulate entry fill
        pos_long.entry_order_id = "entry-1"
        fill_event = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.FILLED,
            event_seq=1,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.1"),
            fill_price=Decimal("50000"),
            fill_id="fill-1"
        )
        pos_long.apply_order_event(fill_event)
        
        # Try to open SHORT - blocked
        can_open, reason = registry.can_open_position("BTC/USD:USD", Side.SHORT)
        assert can_open is False
        assert "Must close existing" in reason
        
        # Request reversal
        registry.request_reversal("BTC/USD:USD", Side.SHORT)
        
        # Still blocked until confirmed
        can_open, reason = registry.can_open_position("BTC/USD:USD", Side.SHORT)
        assert can_open is False
        
        # Close the LONG position
        pos_long.force_close(ExitReason.DIRECTION_REVERSAL)
        
        # Confirm reversal
        new_side = registry.confirm_reversal_closed("BTC/USD:USD")
        assert new_side == Side.SHORT
        
        # Now can open SHORT
        can_open, reason = registry.can_open_position("BTC/USD:USD", Side.SHORT)
        assert can_open is True


class TestInFlightStates:
    """Test in-flight states (EXIT_PENDING, CANCEL_PENDING, ERROR, ORPHANED)."""
    
    def setup_method(self):
        reset_position_registry()
    
    def _create_filled_position(self) -> ManagedPosition:
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-123",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        
        pos.entry_order_id = "entry-1"
        fill_event = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.FILLED,
            event_seq=1,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.1"),
            fill_price=Decimal("50000"),
            fill_id="fill-1"
        )
        pos.apply_order_event(fill_event)
        
        return pos
    
    def test_exit_pending_state(self):
        """Test EXIT_PENDING transition."""
        pos = self._create_filled_position()
        assert pos.state == PositionState.OPEN
        
        # Initiate exit
        pos.initiate_exit(ExitReason.STOP_LOSS, "exit-order-1")
        
        assert pos.state == PositionState.EXIT_PENDING
        assert pos.pending_exit_order_id == "exit-order-1"
    
    def test_exit_pending_to_closed(self):
        """Test EXIT_PENDING → CLOSED on fill."""
        pos = self._create_filled_position()
        pos.initiate_exit(ExitReason.STOP_LOSS, "exit-order-1")
        
        # Exit fill
        exit_event = OrderEvent(
            order_id="exit-order-1",
            client_order_id="client-2",
            event_type=OrderEventType.FILLED,
            event_seq=2,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.1"),
            fill_price=Decimal("48500"),
            fill_id="fill-2"
        )
        pos.apply_order_event(exit_event)
        
        assert pos.state == PositionState.CLOSED
        assert pos.remaining_qty == Decimal("0")
    
    def test_error_state(self):
        """Test ERROR state marking."""
        pos = self._create_filled_position()
        
        pos.mark_error("Exchange returned unexpected response")
        
        assert pos.state == PositionState.ERROR
        assert pos.is_terminal is True
    
    def test_orphaned_state(self):
        """Test ORPHANED state marking."""
        pos = self._create_filled_position()
        
        pos.mark_orphaned()
        
        assert pos.state == PositionState.ORPHANED
        assert pos.is_terminal is True


class TestIdempotentEventHandling:
    """Test idempotent event processing."""
    
    def setup_method(self):
        reset_position_registry()
    
    def test_duplicate_event_is_noop(self):
        """Applying same event twice is a no-op."""
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-123",
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
        
        # First application
        result1 = pos.apply_order_event(event)
        assert result1 is True
        assert pos.filled_entry_qty == Decimal("0.1")
        
        # Second application - should be no-op
        result2 = pos.apply_order_event(event)
        assert result2 is False
        assert pos.filled_entry_qty == Decimal("0.1")  # Still same
    
    def test_out_of_order_events(self):
        """Handle events arriving out of order."""
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-123",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        pos.entry_order_id = "entry-1"
        
        # Fill arrives before ack (can happen)
        fill_event = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.FILLED,
            event_seq=2,  # Higher seq
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.1"),
            fill_price=Decimal("50000"),
            fill_id="fill-1"
        )
        pos.apply_order_event(fill_event)
        
        assert pos.state == PositionState.OPEN
        assert pos.filled_entry_qty == Decimal("0.1")
        
        # Late ack arrives
        ack_event = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.ACKNOWLEDGED,
            event_seq=1,  # Lower seq
            timestamp=datetime.now(timezone.utc)
        )
        result = pos.apply_order_event(ack_event)
        
        assert result is True
        assert pos.entry_acknowledged is True
        # Still in OPEN state (fill already processed)


class TestPartialFills:
    """Test partial fill handling."""
    
    def setup_method(self):
        reset_position_registry()
    
    def test_partial_entry_fill(self):
        """Test partial entry fill creates smaller position."""
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-123",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        pos.entry_order_id = "entry-1"
        
        # Partial fill 1
        event1 = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.PARTIAL_FILL,
            event_seq=1,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.05"),
            fill_price=Decimal("50000"),
            fill_id="fill-1"
        )
        pos.apply_order_event(event1)
        
        assert pos.state == PositionState.OPEN
        assert pos.filled_entry_qty == Decimal("0.05")
        assert pos.remaining_qty == Decimal("0.05")
        
        # Partial fill 2
        event2 = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.FILLED,
            event_seq=2,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.05"),
            fill_price=Decimal("50000"),
            fill_id="fill-2"
        )
        pos.apply_order_event(event2)
        
        assert pos.filled_entry_qty == Decimal("0.1")
        assert pos.remaining_qty == Decimal("0.1")


class TestConditionalBreakEven:
    """Test conditional BE logic."""
    
    def setup_method(self):
        reset_position_registry()
    
    def _create_filled_position(self, trade_type: str = "tight_smc") -> ManagedPosition:
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-123",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None,
            trade_type=trade_type
        )
        pos.entry_order_id = "entry-1"
        ack_event = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.ACKNOWLEDGED,
            event_seq=0,
            timestamp=datetime.now(timezone.utc),
        )
        pos.apply_order_event(ack_event)
        fill_event = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.FILLED,
            event_seq=1,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.1"),
            fill_price=Decimal("50000"),
            fill_id="fill-1"
        )
        pos.apply_order_event(fill_event)
        return pos
    
    def test_be_requires_tp1_filled(self):
        """BE not triggered without TP1 fill."""
        pos = self._create_filled_position()
        
        assert pos.should_trigger_break_even() is False
        assert pos.trigger_break_even() is False
    
    def test_be_requires_minimum_fill(self):
        """BE requires minimum fill percentage."""
        pos = self._create_filled_position()
        pos.tp1_filled = True
        pos.min_partial_for_be = Decimal("0.3")  # Need 30% fill
        
        # Add small exit fill (only 10%)
        exit_event = OrderEvent(
            order_id="exit-1",
            client_order_id="client-2",
            event_type=OrderEventType.FILLED,
            event_seq=2,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.01"),  # Only 10% of 0.1
            fill_price=Decimal("52000"),
            fill_id="fill-2"
        )
        pos.pending_exit_order_id = "exit-1"
        pos.apply_order_event(exit_event)
        
        # Should not trigger BE (only 10% < 30%)
        assert pos.should_trigger_break_even() is False
    
    def test_be_triggers_with_sufficient_fill(self):
        """BE triggers with sufficient fill and market confirmation (intent confirmed)."""
        pos = self._create_filled_position()
        pos.tp1_filled = True
        pos.min_partial_for_be = Decimal("0.3")
        
        # Add sufficient exit fill (50%)
        exit_event = OrderEvent(
            order_id="exit-1",
            client_order_id="client-2",
            event_type=OrderEventType.FILLED,
            event_seq=2,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.05"),  # 50% of 0.1
            fill_price=Decimal("52000"),
            fill_id="fill-2"
        )
        pos.pending_exit_order_id = "exit-1"
        pos.apply_order_event(exit_event)
        
        pos.confirm_intent()  # Market confirmation (BOS/level crossed) required for tight BE
        assert pos.should_trigger_break_even() is True
        assert pos.trigger_break_even() is True
        assert pos.break_even_triggered is True
        assert pos.current_stop_price == pos.avg_entry_price


class TestPersistence:
    """Test persistence and recovery."""
    
    def setup_method(self):
        reset_position_registry()
    
    def test_position_serialization(self):
        """Test position to_dict/from_dict."""
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-123",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=Decimal("54000"),
            initial_final_target=Decimal("56000"),
            setup_type="ob",
            regime="tight_smc",
            trade_type="tight_smc"
        )
        
        # Simulate some activity
        pos.entry_order_id = "entry-1"
        pos.entry_acknowledged = True
        pos.tp1_filled = True
        pos.break_even_triggered = True
        pos.current_stop_price = Decimal("50000")
        
        # Add a fill
        pos.entry_fills.append(FillRecord(
            fill_id="fill-1",
            order_id="entry-1",
            side=Side.LONG,
            qty=Decimal("0.1"),
            price=Decimal("50000"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True
        ))
        
        # Serialize
        data = pos.to_dict()
        
        # Deserialize
        restored = ManagedPosition.from_dict(data)
        
        assert restored.position_id == pos.position_id
        assert restored.symbol == pos.symbol
        assert restored.side == pos.side
        assert restored.initial_size == pos.initial_size
        assert restored.current_stop_price == pos.current_stop_price
        assert restored.entry_acknowledged == pos.entry_acknowledged
        assert restored.tp1_filled == pos.tp1_filled
        assert len(restored.entry_fills) == 1
    
    def test_registry_serialization(self):
        """Test registry to_dict/from_dict."""
        registry = get_position_registry()
        
        pos1 = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-1",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        
        pos2 = ManagedPosition(
            symbol="ETH/USD:USD",
            side=Side.SHORT,
            position_id="test-2",
            initial_size=Decimal("1.0"),
            initial_entry_price=Decimal("3000"),
            initial_stop_price=Decimal("3100"),
            initial_tp1_price=Decimal("2800"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        
        registry.register_position(pos1)
        registry.register_position(pos2)
        registry.request_reversal("BTC/USD:USD", Side.SHORT)
        
        # Serialize
        data = registry.to_dict()
        
        # Deserialize
        restored = PositionRegistry.from_dict(data)
        
        assert len(restored.get_all_active()) == 2
        assert "BTC/USD:USD" in restored._pending_reversals


class TestDatabasePersistence:
    """Test SQLite persistence."""
    
    def setup_method(self):
        reset_position_registry()
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.temp_db.close()
    
    def teardown_method(self):
        os.unlink(self.temp_db.name)
    
    def test_save_and_load_position(self):
        """Test saving and loading a position."""
        from src.execution.position_persistence import PositionPersistence
        
        persistence = PositionPersistence(self.temp_db.name)
        
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-persistence-123",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        
        # Add a fill
        pos.entry_order_id = "entry-1"
        pos.entry_fills.append(FillRecord(
            fill_id="fill-1",
            order_id="entry-1",
            side=Side.LONG,
            qty=Decimal("0.1"),
            price=Decimal("50000"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True
        ))
        
        # Save
        persistence.save_position(pos)
        
        # Load
        loaded = persistence.load_position("test-persistence-123")
        
        assert loaded is not None
        assert loaded.position_id == "test-persistence-123"
        assert loaded.symbol == "BTC/USD:USD"
        assert loaded.initial_size == Decimal("0.1")
        assert len(loaded.entry_fills) == 1
    
    def test_load_active_positions(self):
        """Test loading only active positions."""
        from src.execution.position_persistence import PositionPersistence
        
        persistence = PositionPersistence(self.temp_db.name)
        
        # Active position
        pos1 = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="active-1",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        pos1.state = PositionState.OPEN
        
        # Closed position
        pos2 = ManagedPosition(
            symbol="ETH/USD:USD",
            side=Side.SHORT,
            position_id="closed-1",
            initial_size=Decimal("1.0"),
            initial_entry_price=Decimal("3000"),
            initial_stop_price=Decimal("3100"),
            initial_tp1_price=Decimal("2800"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        pos2.state = PositionState.CLOSED
        
        persistence.save_position(pos1)
        persistence.save_position(pos2)
        
        # Load active only
        active = persistence.load_active_positions()
        
        assert len(active) == 1
        assert active[0].position_id == "active-1"


class TestReconciliation:
    """Test exchange reconciliation."""
    
    def setup_method(self):
        reset_position_registry()
    
    def test_detect_orphaned_position(self):
        """Detect position in registry but not on exchange."""
        registry = get_position_registry()
        
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-123",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        pos.entry_order_id = "entry-1"
        
        # Add entry fill so remaining_qty > 0
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
        
        registry.register_position(pos)
        
        # Exchange has no positions
        exchange_positions = {}
        
        issues = registry.reconcile_with_exchange(exchange_positions, [])
        assert len(issues) == 1
        assert "MISSING_ON_EXCHANGE_PENDING" in issues[0][1]
        assert "BTC/USD:USD" in registry._positions, "First miss should not orphan immediately"
        
        # Second consecutive miss should mark ORPHANED
        issues = registry.reconcile_with_exchange(exchange_positions, [])
        
        assert len(issues) == 1
        assert "ORPHANED" in issues[0][1]
        # Orphaned positions are moved from _positions to _closed_positions
        assert "BTC/USD:USD" not in registry._positions, "Orphaned should be removed from _positions"
        orphaned = [p for p in registry._closed_positions if p.symbol == "BTC/USD:USD"]
        assert len(orphaned) == 1, "Orphaned should be in _closed_positions"
        assert orphaned[0].state == PositionState.ORPHANED
    
    def test_detect_phantom_position(self):
        """Detect position on exchange but not in registry."""
        registry = get_position_registry()
        
        # Exchange has position we don't know about
        exchange_positions = {
            "BTC/USD:USD": {"side": "long", "qty": "0.1", "entry_price": "50000"}
        }
        
        issues = registry.reconcile_with_exchange(exchange_positions, [])
        
        assert len(issues) == 1
        assert "PHANTOM" in issues[0][1]
    
    def test_cross_format_symbol_matching(self):
        """Registry uses spot format (ADA/USD), exchange uses futures format (PF_ADAUSD)."""
        from datetime import datetime, timezone
        registry = get_position_registry()
        registry._positions.clear()
        registry._closed_positions.clear()
        
        # Create position with SPOT format symbol
        pos = ManagedPosition(
            symbol="ADA/USD",  # Spot format
            side=Side.LONG,
            position_id="test-ada-123",
            initial_size=Decimal("100"),
            initial_entry_price=Decimal("0.50"),
            initial_stop_price=Decimal("0.45"),
            initial_tp1_price=Decimal("0.60"),
            initial_tp2_price=None,
            initial_final_target=None,
        )
        # Add entry fill so remaining_qty > 0
        pos.entry_fills.append(FillRecord(
            fill_id="fill-1",
            order_id="order-1",
            side=Side.LONG,
            qty=Decimal("100"),
            price=Decimal("0.50"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True,
        ))
        registry.register_position(pos)
        
        # Exchange returns FUTURES format - should match via normalization
        exchange_positions = {
            "PF_ADAUSD": {"side": "long", "qty": "100", "entry_price": "0.50"}
        }
        
        issues = registry.reconcile_with_exchange(exchange_positions, [])
        
        # Should NOT be orphaned (formats normalized and matched)
        assert len(issues) == 0, f"Expected no issues but got: {issues}"
        assert "ADA/USD" in registry._positions, "Position should still be active"

    def test_stale_zero_qty_entry_is_not_reported_as_qty_mismatch(self):
        """Zero-qty non-terminal entries should be archived as stale during reconciliation."""
        registry = get_position_registry()
        registry._positions.clear()
        registry._closed_positions.clear()

        pos = ManagedPosition(
            symbol="ENA/USD",
            side=Side.SHORT,
            position_id="test-ena-stale",
            initial_size=Decimal("111"),
            initial_entry_price=Decimal("0.1546"),
            initial_stop_price=Decimal("0.1596"),
            initial_tp1_price=Decimal("0.1490"),
            initial_tp2_price=None,
            initial_final_target=None,
        )
        pos.state = PositionState.OPEN
        pos.entry_fills.append(FillRecord(
            fill_id="entry-1",
            order_id="entry-order-1",
            side=Side.SHORT,
            qty=Decimal("111"),
            price=Decimal("0.1546"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True,
        ))
        pos.exit_fills.append(FillRecord(
            fill_id="exit-1",
            order_id="exit-order-1",
            side=Side.LONG,
            qty=Decimal("111"),
            price=Decimal("0.1530"),
            timestamp=datetime.now(timezone.utc),
            is_entry=False,
        ))
        registry.register_position(pos)

        issues = registry.reconcile_with_exchange(
            {"PF_ENAUSD": {"side": "short", "qty": "111", "entry_price": "0.1546"}},
            [],
        )

        assert len(issues) == 1
        assert issues[0][0] == "ENA/USD"
        assert "STALE_ZERO_QTY" in issues[0][1]
        assert "QTY_MISMATCH" not in issues[0][1]
        assert "ENA/USD" not in registry._positions
        stale = [p for p in registry._closed_positions if p.symbol == "ENA/USD"]
        assert len(stale) == 1
        assert stale[0].state == PositionState.CLOSED

    def test_qty_mismatch_is_reconciled_to_exchange_without_repeating_error(self):
        """Qty drifts should converge to exchange qty using synthetic reconciliation fills."""
        registry = get_position_registry()
        registry._positions.clear()
        registry._closed_positions.clear()

        pos = ManagedPosition(
            symbol="ENA/USD",
            side=Side.SHORT,
            position_id="test-ena-reconcile",
            initial_size=Decimal("111"),
            initial_entry_price=Decimal("0.1546"),
            initial_stop_price=Decimal("0.1596"),
            initial_tp1_price=Decimal("0.1490"),
            initial_tp2_price=None,
            initial_final_target=None,
        )
        pos.state = PositionState.OPEN
        pos.entry_fills.append(FillRecord(
            fill_id="entry-1",
            order_id="entry-order-1",
            side=Side.SHORT,
            qty=Decimal("111"),
            price=Decimal("0.1546"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True,
        ))
        registry.register_position(pos)

        issues = registry.reconcile_with_exchange(
            {"PF_ENAUSD": {"side": "short", "qty": "72", "entry_price": "0.1546"}},
            [],
        )

        assert len(issues) == 1
        assert issues[0][0] == "ENA/USD"
        assert "QTY_SYNCED" in issues[0][1]
        assert "QTY_MISMATCH" not in issues[0][1]

        updated = registry.get_position("ENA/USD")
        assert updated is not None
        assert updated.remaining_qty == Decimal("72")
        assert updated.state == PositionState.OPEN
        assert len(updated.exit_fills) == 1

        # Reconciliation should be stable on subsequent passes.
        issues_again = registry.reconcile_with_exchange(
            {"PF_ENAUSD": {"side": "short", "qty": "72", "entry_price": "0.1546"}},
            [],
        )
        assert issues_again == []
    
    def test_orphaned_with_symbol_normalization(self):
        """Registry has spot format position, exchange has nothing - should orphan."""
        from datetime import datetime, timezone
        registry = get_position_registry()
        registry._positions.clear()
        registry._closed_positions.clear()
        
        pos = ManagedPosition(
            symbol="ADA/USD",
            side=Side.LONG,
            position_id="test-ada-456",
            initial_size=Decimal("100"),
            initial_entry_price=Decimal("0.50"),
            initial_stop_price=Decimal("0.45"),
            initial_tp1_price=Decimal("0.60"),
            initial_tp2_price=None,
            initial_final_target=None,
        )
        # Add entry fill so remaining_qty > 0
        pos.entry_fills.append(FillRecord(
            fill_id="fill-1",
            order_id="order-1",
            side=Side.LONG,
            qty=Decimal("100"),
            price=Decimal("0.50"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True,
        ))
        registry.register_position(pos)
        
        # Exchange has NO positions
        exchange_positions = {}
        
        issues = registry.reconcile_with_exchange(exchange_positions, [])
        
        assert len(issues) == 1
        assert "MISSING_ON_EXCHANGE_PENDING" in issues[0][1]
        assert "ADA/USD" in registry._positions
        
        # Second miss crosses orphan hysteresis threshold.
        issues = registry.reconcile_with_exchange(exchange_positions, [])
        
        assert len(issues) == 1
        assert "ORPHANED" in issues[0][1]
        assert "ADA/USD" not in registry._positions
        orphaned = [p for p in registry._closed_positions if p.symbol == "ADA/USD"]
        assert len(orphaned) == 1

    def test_exit_pending_missing_after_grace_closes_instead_of_orphan(self):
        """Exit-pending positions should close cleanly after grace+hysteresis."""
        registry = get_position_registry()
        registry._positions.clear()
        registry._closed_positions.clear()

        pos = ManagedPosition(
            symbol="XMR/USD",
            side=Side.SHORT,
            position_id="test-xmr-exit-pending",
            initial_size=Decimal("0.17"),
            initial_entry_price=Decimal("340"),
            initial_stop_price=Decimal("351"),
            initial_tp1_price=Decimal("330"),
            initial_tp2_price=None,
            initial_final_target=None,
        )
        pos.entry_fills.append(FillRecord(
            fill_id="entry-fill-1",
            order_id="entry-order-1",
            side=Side.SHORT,
            qty=Decimal("0.17"),
            price=Decimal("340"),
            timestamp=datetime.now(timezone.utc),
            is_entry=True,
        ))
        pos.state = PositionState.EXIT_PENDING
        # Force this position past exit-pending grace so hysteresis path is exercised.
        pos.updated_at = datetime.now(timezone.utc) - timedelta(seconds=31)
        registry.register_position(pos)

        # First miss remains pending.
        issues = registry.reconcile_with_exchange({}, [])
        assert len(issues) == 1
        assert "MISSING_ON_EXCHANGE_PENDING" in issues[0][1]
        assert registry.get_position_any_state("XMR/USD") is not None

        # Second miss closes (reconciliation) instead of orphaning.
        issues = registry.reconcile_with_exchange({}, [])
        assert len(issues) == 1
        assert "CLOSED_ON_EXCHANGE_MISSING_AFTER_EXIT" in issues[0][1]
        assert "ORPHANED" not in issues[0][1]

        closed = [p for p in registry._closed_positions if p.symbol == "XMR/USD"]
        assert len(closed) == 1
        assert closed[0].state == PositionState.CLOSED


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
