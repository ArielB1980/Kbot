"""
Production-Grade Tests for Invariants F-J.

These tests verify:
- F: State transitions driven only by exchange events
- G: Idempotent event handling (duplicates, replays, out-of-order)
- H: Exit is a first-class lifecycle (EXIT_PENDING until flat)
- I: Stop/TP orders linked to position with replace semantics
- J: Conditional break-even

And the specific scenarios:
- Entry partial fill (requested 100, filled 40 then 60)
- Exit partial fill (exit 100, fills 30 then 70)
- Duplicate fill event (same fill_id)
- Out-of-order events (FILL before ACK)
- Stop replace failure
- Reversal blocked while EXIT_PENDING
- Restart replay correctness
"""
import pytest
from decimal import Decimal
from datetime import datetime, timezone
import copy

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
    get_position_registry
)
from src.domain.models import Side


class TestInvariantF_EventDrivenStateTransitions:
    """
    Invariant F: State transitions are driven ONLY by exchange order events.
    
    A management "decision" may emit an action, but no state change occurs
    until you receive an ACK/FILL/CANCEL/REJECT event for the relevant order.
    """
    
    def setup_method(self):
        reset_position_registry()
    
    def _create_pending_position(self) -> ManagedPosition:
        """Create a position in PENDING state."""
        return ManagedPosition(
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
    
    def test_state_does_not_change_on_action_emit_only_on_event(self):
        """State should NOT change when we request an action, only on exchange event."""
        pos = self._create_pending_position()
        pos.entry_order_id = "entry-1"
        
        # Position is PENDING
        assert pos.state == PositionState.PENDING
        
        # We "decide" to submit the entry, but that's just a decision
        # State should still be PENDING (not OPEN)
        assert pos.state == PositionState.PENDING
        
        # Only when we receive the FILL event should state change
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
        
        # NOW state changes to OPEN
        assert pos.state == PositionState.OPEN
    
    def test_initiate_exit_sets_pending_not_closed(self):
        """initiate_exit should set EXIT_PENDING, not CLOSED."""
        pos = self._create_pending_position()
        pos.entry_order_id = "entry-1"
        
        # Fill entry first
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
        assert pos.state == PositionState.OPEN
        
        # Initiate exit - should be PENDING not CLOSED
        pos.initiate_exit(ExitReason.STOP_LOSS, "exit-1")
        
        assert pos.state == PositionState.EXIT_PENDING
        assert pos.state != PositionState.CLOSED
        assert pos.remaining_qty == Decimal("0.1")  # Still have exposure!
    
    def test_break_even_flag_set_but_state_only_on_event(self):
        """
        trigger_break_even sets the flag, but PROTECTED state should ideally
        only be confirmed when stop order is replaced successfully.
        
        This is a design choice - for now we allow immediate flag + state,
        but in production you might want to wait for stop replace confirmation.
        """
        pos = self._create_pending_position()
        pos.entry_order_id = "entry-1"
        pos.trade_type = "wide_structure"  # Allow earlier BE
        
        # Fill entry
        fill = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.FILLED,
            event_seq=1,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.1"),
            fill_price=Decimal("50000"),
            fill_id="fill-1"
        )
        pos.apply_order_event(fill)
        
        # Simulate TP1 partial close
        pos.tp1_filled = True
        pos.pending_exit_order_id = "exit-tp1"
        tp1_fill = OrderEvent(
            order_id="exit-tp1",
            client_order_id="client-2",
            event_type=OrderEventType.FILLED,
            event_seq=2,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.05"),
            fill_price=Decimal("52000"),
            fill_id="fill-2"
        )
        pos.apply_order_event(tp1_fill)
        
        # Trigger BE
        result = pos.trigger_break_even()
        
        # The flag is set and state changed
        # In strict event-driven model, you'd wait for stop replace event
        assert pos.break_even_triggered is True


class TestInvariantG_IdempotentEventHandling:
    """
    Invariant G: Idempotent event handling.
    
    - Same event applied twice = no-op
    - Out-of-order events must not corrupt state
    """
    
    def setup_method(self):
        reset_position_registry()
    
    def _create_open_position(self) -> ManagedPosition:
        """Create a position with entry filled."""
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
    
    def test_duplicate_fill_event_same_fill_id_is_noop(self):
        """Duplicate fill with same fill_id should not double-count."""
        pos = self._create_open_position()
        initial_qty = pos.remaining_qty
        
        # Initiate exit
        pos.initiate_exit(ExitReason.STOP_LOSS, "exit-1")
        pos.pending_exit_order_id = "exit-1"
        
        # First exit fill
        exit_fill = OrderEvent(
            order_id="exit-1",
            client_order_id="client-2",
            event_type=OrderEventType.FILLED,
            event_seq=2,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.1"),
            fill_price=Decimal("48500"),
            fill_id="exit-fill-1"  # Same fill_id
        )
        
        result1 = pos.apply_order_event(exit_fill)
        assert result1 is True
        assert pos.remaining_qty == Decimal("0")
        assert pos.state == PositionState.CLOSED
        
        # Duplicate event with same hash
        result2 = pos.apply_order_event(exit_fill)
        assert result2 is False  # No-op
        assert pos.remaining_qty == Decimal("0")  # Still zero, not negative
    
    def test_out_of_order_fill_before_ack(self):
        """FILL arriving before ACK should still work correctly."""
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-ooo",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        pos.entry_order_id = "entry-1"
        
        # FILL arrives first (out of order)
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
        
        # Position should be OPEN (fill processed)
        assert pos.state == PositionState.OPEN
        assert pos.filled_entry_qty == Decimal("0.1")
        
        # ACK arrives late
        ack_event = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.ACKNOWLEDGED,
            event_seq=1,  # Lower seq
            timestamp=datetime.now(timezone.utc)
        )
        result = pos.apply_order_event(ack_event)
        
        # ACK should still be processed
        assert result is True
        assert pos.entry_acknowledged is True
        # State stays OPEN (already transitioned)
        assert pos.state == PositionState.OPEN
    
    def test_restart_replay_produces_same_state(self):
        """Replaying events after restart should produce identical state."""
        # Create position and process events
        pos1 = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-replay",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        pos1.entry_order_id = "entry-1"
        
        events = [
            OrderEvent(
                order_id="entry-1",
                client_order_id="client-1",
                event_type=OrderEventType.ACKNOWLEDGED,
                event_seq=1,
                timestamp=datetime.now(timezone.utc)
            ),
            OrderEvent(
                order_id="entry-1",
                client_order_id="client-1",
                event_type=OrderEventType.PARTIAL_FILL,
                event_seq=2,
                timestamp=datetime.now(timezone.utc),
                fill_qty=Decimal("0.04"),
                fill_price=Decimal("50000"),
                fill_id="fill-1"
            ),
            OrderEvent(
                order_id="entry-1",
                client_order_id="client-1",
                event_type=OrderEventType.FILLED,
                event_seq=3,
                timestamp=datetime.now(timezone.utc),
                fill_qty=Decimal("0.06"),
                fill_price=Decimal("50000"),
                fill_id="fill-2"
            ),
        ]
        
        for event in events:
            pos1.apply_order_event(event)
        
        # Record final state
        final_state = pos1.state
        final_qty = pos1.filled_entry_qty
        final_ack = pos1.entry_acknowledged
        
        # Create new position (simulating restart)
        pos2 = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-replay",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        pos2.entry_order_id = "entry-1"
        
        # Replay same events
        for event in events:
            pos2.apply_order_event(event)
        
        # States should be identical
        assert pos2.state == final_state
        assert pos2.filled_entry_qty == final_qty
        assert pos2.entry_acknowledged == final_ack


class TestInvariantH_ExitLifecycle:
    """
    Invariant H: Exit is a first-class lifecycle.
    
    - Not CLOSED until remaining_qty == 0 confirmed by fills
    - While EXIT_PENDING, block new entries and reversals
    """
    
    def setup_method(self):
        reset_position_registry()
    
    def test_not_closed_until_fully_filled(self):
        """Position is EXIT_PENDING until all exit fills confirmed."""
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-123",
            initial_size=Decimal("1.0"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        pos.entry_order_id = "entry-1"
        
        # Fill entry with 1.0
        entry_fill = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.FILLED,
            event_seq=1,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("1.0"),
            fill_price=Decimal("50000"),
            fill_id="fill-1"
        )
        pos.apply_order_event(entry_fill)
        assert pos.state == PositionState.OPEN
        
        # Initiate exit
        pos.initiate_exit(ExitReason.STOP_LOSS, "exit-1")
        pos.pending_exit_order_id = "exit-1"
        assert pos.state == PositionState.EXIT_PENDING
        
        # Partial exit fill (30%)
        partial_exit = OrderEvent(
            order_id="exit-1",
            client_order_id="client-2",
            event_type=OrderEventType.PARTIAL_FILL,
            event_seq=2,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.3"),
            fill_price=Decimal("48500"),
            fill_id="exit-fill-1"
        )
        pos.apply_order_event(partial_exit)
        
        # Still EXIT_PENDING with remaining exposure
        assert pos.state == PositionState.EXIT_PENDING
        assert pos.remaining_qty == Decimal("0.7")
        
        # Another partial (40%)
        partial_exit_2 = OrderEvent(
            order_id="exit-1",
            client_order_id="client-2",
            event_type=OrderEventType.PARTIAL_FILL,
            event_seq=3,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.4"),
            fill_price=Decimal("48500"),
            fill_id="exit-fill-2"
        )
        pos.apply_order_event(partial_exit_2)
        
        # Still not closed
        assert pos.state == PositionState.EXIT_PENDING
        assert pos.remaining_qty == Decimal("0.3")
        
        # Final fill (30%)
        final_exit = OrderEvent(
            order_id="exit-1",
            client_order_id="client-2",
            event_type=OrderEventType.FILLED,
            event_seq=4,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.3"),
            fill_price=Decimal("48500"),
            fill_id="exit-fill-3"
        )
        pos.apply_order_event(final_exit)
        
        # NOW it's closed
        assert pos.state == PositionState.CLOSED
        assert pos.remaining_qty == Decimal("0")
    
    def test_reversal_blocked_while_exit_pending(self):
        """Cannot request reversal while EXIT_PENDING."""
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
        
        # Fill and register
        fill = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.FILLED,
            event_seq=1,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.1"),
            fill_price=Decimal("50000"),
            fill_id="fill-1"
        )
        pos.apply_order_event(fill)
        registry.register_position(pos)
        
        # Initiate exit
        pos.initiate_exit(ExitReason.STOP_LOSS, "exit-1")
        assert pos.state == PositionState.EXIT_PENDING
        
        # Try to open opposite direction - should be blocked
        can_open, reason = registry.can_open_position("BTC/USD:USD", Side.SHORT)
        assert can_open is False
        # Position is in EXIT_PENDING, not terminal, so blocked


class TestInvariantI_StopOrderLinking:
    """
    Invariant I: Stop/TP orders are linked to position.
    
    - Every stop/TP order carries client_order_id with position_id
    - Stop changes are replace semantics (cancel + replace)
    - Exactly one active stop per position (except PENDING)
    """
    
    def setup_method(self):
        reset_position_registry()
    
    def test_stop_order_id_linked_to_position(self):
        """Stop order ID should be trackable to position."""
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="pos-abc123",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        
        # Set stop order with position-linked ID
        stop_client_order_id = f"stop-{pos.position_id}"
        pos.stop_client_order_id = stop_client_order_id
        pos.stop_order_id = "exchange-stop-123"
        
        # Verify linkage
        assert pos.position_id in pos.stop_client_order_id
        assert pos.stop_order_id is not None
    
    def test_stop_update_replaces_order_id(self):
        """Stop update should track new order ID (replace semantics)."""
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="pos-abc123",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        pos.entry_acknowledged = True
        
        old_stop_order_id = "old-stop-123"
        pos.stop_order_id = old_stop_order_id
        
        # Update stop with new order ID
        new_stop_order_id = "new-stop-456"
        result = pos.update_stop(Decimal("49500"), new_stop_order_id)
        
        assert result is True
        assert pos.stop_order_id == new_stop_order_id
        assert pos.stop_order_id != old_stop_order_id


class TestInvariantJ_ConditionalBreakEven:
    """
    Invariant J: Break-even is conditional, not automatic.
    
    BE allowed only if:
    - tp1_filled_qty >= MIN_TP1_FILL_FOR_BE (30% of initial)
    - intent_confirmed OR trade_type == wide_structure
    - spread/slippage within tolerance (future enhancement)
    """
    
    def setup_method(self):
        reset_position_registry()
    
    def _create_position_with_tp1_partial(
        self,
        tp1_fill_pct: Decimal,
        trade_type: str = "tight_smc"
    ) -> ManagedPosition:
        """Create position with specified TP1 fill percentage."""
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-123",
            initial_size=Decimal("1.0"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None,
            trade_type=trade_type,
            min_partial_for_be=Decimal("0.3")  # 30% minimum
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
        entry_fill = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.FILLED,
            event_seq=1,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("1.0"),
            fill_price=Decimal("50000"),
            fill_id="fill-1"
        )
        pos.apply_order_event(entry_fill)
        # Mark TP1 as filled
        pos.tp1_filled = True
        
        # Add exit fill with specified percentage
        exit_qty = Decimal("1.0") * tp1_fill_pct
        pos.pending_exit_order_id = "exit-tp1"
        exit_fill = OrderEvent(
            order_id="exit-tp1",
            client_order_id="client-2",
            event_type=OrderEventType.FILLED,
            event_seq=2,
            timestamp=datetime.now(timezone.utc),
            fill_qty=exit_qty,
            fill_price=Decimal("52000"),
            fill_id="exit-fill-1"
        )
        pos.apply_order_event(exit_fill)
        
        return pos
    
    def test_be_not_triggered_if_fill_below_minimum(self):
        """BE should NOT trigger if TP1 fill is below minimum threshold."""
        pos = self._create_position_with_tp1_partial(
            tp1_fill_pct=Decimal("0.2"),  # 20% < 30% min
            trade_type="tight_smc"
        )
        
        assert pos.should_trigger_break_even() is False
    
    def test_be_triggered_if_fill_above_minimum(self):
        """BE should trigger if TP1 fill meets minimum and intent confirmed (market confirmation)."""
        pos = self._create_position_with_tp1_partial(
            tp1_fill_pct=Decimal("0.5"),  # 50% > 30% min
            trade_type="tight_smc"
        )
        pos.confirm_intent()  # Simulate market confirmation (BOS/level crossed)
        assert pos.should_trigger_break_even() is True
    
    def test_be_triggered_for_wide_trade_with_smaller_fill(self):
        """Wide trades allow BE with smaller fills."""
        pos = self._create_position_with_tp1_partial(
            tp1_fill_pct=Decimal("0.35"),  # Just above 30%
            trade_type="wide_structure"
        )
        
        assert pos.should_trigger_break_even() is True
    
    def test_be_not_triggered_tight_without_intent_confirmed(self):
        """BE should NOT trigger for tight_smc if intent_confirmed is False."""
        pos = self._create_position_with_tp1_partial(
            tp1_fill_pct=Decimal("0.5"),
            trade_type="tight_smc"
        )
        pos.intent_confirmed = False  # Simulate no ack / no BOS confirmation
        assert pos.should_trigger_break_even() is False

    def test_be_not_triggered_if_not_tp1_filled(self):
        """BE requires TP1 to be filled first."""
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-123",
            initial_size=Decimal("1.0"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        pos.entry_order_id = "entry-1"
        
        # Fill entry
        entry_fill = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.FILLED,
            event_seq=1,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("1.0"),
            fill_price=Decimal("50000"),
            fill_id="fill-1"
        )
        pos.apply_order_event(entry_fill)
        
        # TP1 NOT filled
        pos.tp1_filled = False
        
        assert pos.should_trigger_break_even() is False


class TestEntryPartialFillScenario:
    """Test: Entry partial fill (requested 100, filled 40 then 60)."""
    
    def setup_method(self):
        reset_position_registry()
    
    def test_entry_partial_fill_sizing_correct(self):
        """Partial entry fills should accumulate correctly."""
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-partial-entry",
            initial_size=Decimal("100"),  # Requested 100
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        pos.entry_order_id = "entry-1"
        
        # First partial: 40
        partial_1 = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.PARTIAL_FILL,
            event_seq=1,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("40"),
            fill_price=Decimal("50000"),
            fill_id="fill-1"
        )
        pos.apply_order_event(partial_1)
        
        assert pos.state == PositionState.OPEN
        assert pos.filled_entry_qty == Decimal("40")
        assert pos.remaining_qty == Decimal("40")
        
        # Second partial: 60
        partial_2 = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.FILLED,
            event_seq=2,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("60"),
            fill_price=Decimal("50000"),
            fill_id="fill-2"
        )
        pos.apply_order_event(partial_2)
        
        assert pos.filled_entry_qty == Decimal("100")
        assert pos.remaining_qty == Decimal("100")


class TestExitPartialFillScenario:
    """Test: Exit partial fill (exit 100, fills 30 then 70)."""
    
    def setup_method(self):
        reset_position_registry()
    
    def test_exit_partial_fill_stays_pending(self):
        """Exit partial fills should keep position in EXIT_PENDING until flat."""
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-partial-exit",
            initial_size=Decimal("100"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        pos.entry_order_id = "entry-1"
        
        # Fill entry
        entry_fill = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.FILLED,
            event_seq=1,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("100"),
            fill_price=Decimal("50000"),
            fill_id="fill-1"
        )
        pos.apply_order_event(entry_fill)
        
        # Initiate exit
        pos.initiate_exit(ExitReason.STOP_LOSS, "exit-1")
        pos.pending_exit_order_id = "exit-1"
        
        # Partial exit: 30
        partial_1 = OrderEvent(
            order_id="exit-1",
            client_order_id="client-2",
            event_type=OrderEventType.PARTIAL_FILL,
            event_seq=2,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("30"),
            fill_price=Decimal("48500"),
            fill_id="exit-fill-1"
        )
        pos.apply_order_event(partial_1)
        
        assert pos.state == PositionState.EXIT_PENDING
        assert pos.remaining_qty == Decimal("70")
        
        # Partial exit: 70
        partial_2 = OrderEvent(
            order_id="exit-1",
            client_order_id="client-2",
            event_type=OrderEventType.FILLED,
            event_seq=3,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("70"),
            fill_price=Decimal("48500"),
            fill_id="exit-fill-2"
        )
        pos.apply_order_event(partial_2)
        
        assert pos.state == PositionState.CLOSED
        assert pos.remaining_qty == Decimal("0")


class TestDuplicateFillEvent:
    """Test: Duplicate fill event (same fill_id) should not double-count."""
    
    def setup_method(self):
        reset_position_registry()
    
    def test_duplicate_fill_id_no_double_close(self):
        """Same fill_id applied twice should be no-op, no double-close."""
        pos = ManagedPosition(
            symbol="BTC/USD:USD",
            side=Side.LONG,
            position_id="test-dup",
            initial_size=Decimal("0.1"),
            initial_entry_price=Decimal("50000"),
            initial_stop_price=Decimal("49000"),
            initial_tp1_price=Decimal("52000"),
            initial_tp2_price=None,
            initial_final_target=None
        )
        pos.entry_order_id = "entry-1"
        
        # Fill entry
        entry_fill = OrderEvent(
            order_id="entry-1",
            client_order_id="client-1",
            event_type=OrderEventType.FILLED,
            event_seq=1,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.1"),
            fill_price=Decimal("50000"),
            fill_id="entry-fill-1"
        )
        pos.apply_order_event(entry_fill)
        
        # Initiate exit
        pos.initiate_exit(ExitReason.STOP_LOSS, "exit-1")
        pos.pending_exit_order_id = "exit-1"
        
        # Exit fill
        exit_fill = OrderEvent(
            order_id="exit-1",
            client_order_id="client-2",
            event_type=OrderEventType.FILLED,
            event_seq=2,
            timestamp=datetime.now(timezone.utc),
            fill_qty=Decimal("0.1"),
            fill_price=Decimal("48500"),
            fill_id="exit-fill-same"  # Same fill_id
        )
        
        # Apply once
        result1 = pos.apply_order_event(exit_fill)
        assert result1 is True
        assert pos.state == PositionState.CLOSED
        assert pos.remaining_qty == Decimal("0")
        
        # Apply duplicate
        result2 = pos.apply_order_event(exit_fill)
        assert result2 is False  # No-op
        
        # Remaining qty should still be 0, not negative
        assert pos.remaining_qty == Decimal("0")
        assert pos.filled_exit_qty == Decimal("0.1")  # Not doubled


class TestReconciliationRule:
    """
    Production Rule: Periodic reconciliation.
    
    - Registry OPEN but exchange flat → mark ORPHANED
    - Exchange has position but registry missing → import as ORPHANED + attach stop
    """
    
    def setup_method(self):
        reset_position_registry()
    
    def test_registry_open_exchange_flat_marks_orphaned(self):
        """If registry says OPEN but exchange is flat across consecutive checks, mark ORPHANED."""
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
        
        # Fill entry so remaining_qty > 0
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
        
        # Exchange shows NO position
        exchange_positions = {}
        
        issues = registry.reconcile_with_exchange(exchange_positions, [])
        assert len(issues) == 1
        assert "MISSING_ON_EXCHANGE_PENDING" in issues[0][1]
        assert registry.get_position("BTC/USD:USD") is not None
        
        # Second miss should mark orphaned and archive.
        issues = registry.reconcile_with_exchange(exchange_positions, [])
        
        assert len(issues) == 1
        assert "ORPHANED" in issues[0][1]
        # Registry moves orphaned positions to closed history (no longer in _positions)
        closed = registry.get_closed_history(limit=10)
        assert len(closed) == 1
        assert closed[0].symbol == "BTC/USD:USD"
        assert closed[0].state == PositionState.ORPHANED
    
    def test_exchange_has_position_registry_missing_detected(self):
        """If exchange has position but registry doesn't, detect as PHANTOM."""
        registry = get_position_registry()
        
        # Registry is empty
        assert len(registry.get_all()) == 0
        
        # Exchange has a position
        exchange_positions = {
            "BTC/USD:USD": {"side": "long", "qty": "0.1", "entry_price": "50000"}
        }
        
        issues = registry.reconcile_with_exchange(exchange_positions, [])
        
        assert len(issues) == 1
        assert "PHANTOM" in issues[0][1]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
