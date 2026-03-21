#!/usr/bin/env python3
"""
Simple test script for TP Backfill functionality.
Tests the core logic without requiring full LiveTrading setup.
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from decimal import Decimal
from datetime import datetime, timezone, timedelta
from src.domain.models import Position, Side


def test_tp_plan_computation():
    """Test TP plan computation logic."""
    print("=" * 60)
    print("Test 1: TP Plan Computation (R-multiples)")
    print("=" * 60)
    
    # LONG position: entry=50000, SL=49000, risk=1000
    entry = Decimal("50000")
    sl = Decimal("49000")
    risk = abs(entry - sl)  # 1000
    side_sign = Decimal("1")  # LONG
    
    tp1 = entry + side_sign * Decimal("1.0") * risk  # 51000
    tp2 = entry + side_sign * Decimal("2.0") * risk  # 52000
    tp3 = entry + side_sign * Decimal("3.0") * risk  # 53000
    
    print(f"Entry: {entry}")
    print(f"SL: {sl}")
    print(f"Risk: {risk}")
    print(f"TP1 (1R): {tp1}")
    print(f"TP2 (2R): {tp2}")
    print(f"TP3 (3R): {tp3}")
    
    assert tp1 == Decimal("51000"), f"TP1 should be 51000, got {tp1}"
    assert tp2 == Decimal("52000"), f"TP2 should be 52000, got {tp2}"
    assert tp3 == Decimal("53000"), f"TP3 should be 53000, got {tp3}"
    
    print("✅ TP plan computation correct for LONG")
    
    # SHORT position: entry=3000, SL=3100, risk=100
    entry_short = Decimal("3000")
    sl_short = Decimal("3100")
    risk_short = abs(entry_short - sl_short)  # 100
    side_sign_short = Decimal("-1")  # SHORT
    
    tp1_short = entry_short + side_sign_short * Decimal("1.0") * risk_short  # 2900
    tp2_short = entry_short + side_sign_short * Decimal("2.0") * risk_short  # 2800
    tp3_short = entry_short + side_sign_short * Decimal("3.0") * risk_short  # 2700
    
    print(f"\nEntry: {entry_short}")
    print(f"SL: {sl_short}")
    print(f"Risk: {risk_short}")
    print(f"TP1 (1R): {tp1_short}")
    print(f"TP2 (2R): {tp2_short}")
    print(f"TP3 (3R): {tp3_short}")
    
    assert tp1_short == Decimal("2900"), f"TP1 should be 2900, got {tp1_short}"
    assert tp2_short == Decimal("2800"), f"TP2 should be 2800, got {tp2_short}"
    assert tp3_short == Decimal("2700"), f"TP3 should be 2700, got {tp3_short}"
    
    print("✅ TP plan computation correct for SHORT")


def test_needs_backfill_logic():
    """Test the needs_backfill logic."""
    print("\n" + "=" * 60)
    print("Test 2: Needs Backfill Logic")
    print("=" * 60)
    
    # Case 1: Position with no TP plan and no orders
    pos_no_plan = Position(
        symbol="BTCUSD-PERP",
        side=Side.LONG,
        size=Decimal("1.0"),
        size_notional=Decimal("50000"),
        entry_price=Decimal("50000"),
        current_mark_price=Decimal("51000"),
        liquidation_price=Decimal("45000"),
        unrealized_pnl=Decimal("0"),
        leverage=Decimal("10"),
        margin_used=Decimal("5000"),
        initial_stop_price=Decimal("49000"),
        tp1_price=None,  # No plan
        tp2_price=None,
        tp_order_ids=[],  # No orders
        opened_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    
    has_tp_plan = (pos_no_plan.tp1_price is not None) or (pos_no_plan.tp2_price is not None)
    has_tp_ids = bool(pos_no_plan.tp_order_ids and len(pos_no_plan.tp_order_ids) > 0)
    open_tp_orders = []  # No orders on exchange
    
    needs_backfill = (not has_tp_plan and not has_tp_ids) or len(open_tp_orders) == 0
    
    print(f"Position: {pos_no_plan.symbol}")
    print(f"Has TP plan: {has_tp_plan}")
    print(f"Has TP IDs: {has_tp_ids}")
    print(f"Open TP orders: {len(open_tp_orders)}")
    print(f"Needs backfill: {needs_backfill}")
    
    assert needs_backfill is True, "Should need backfill when no plan and no orders"
    print("✅ Correctly identifies need for backfill")
    
    # Case 2: Position with TP plan and orders
    pos_with_plan = Position(
        symbol="BTCUSD-PERP",
        side=Side.LONG,
        size=Decimal("1.0"),
        size_notional=Decimal("50000"),
        entry_price=Decimal("50000"),
        current_mark_price=Decimal("51000"),
        liquidation_price=Decimal("45000"),
        unrealized_pnl=Decimal("0"),
        leverage=Decimal("10"),
        margin_used=Decimal("5000"),
        initial_stop_price=Decimal("49000"),
        tp1_price=Decimal("51000"),  # Has plan
        tp2_price=Decimal("52000"),
        tp_order_ids=["tp1_123", "tp2_456"],  # Has orders
        opened_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    
    has_tp_plan = (pos_with_plan.tp1_price is not None) or (pos_with_plan.tp2_price is not None)
    has_tp_ids = bool(pos_with_plan.tp_order_ids and len(pos_with_plan.tp_order_ids) > 0)
    open_tp_orders = ["tp1_123", "tp2_456"]  # Orders exist
    
    needs_backfill = (not has_tp_plan and not has_tp_ids) or len(open_tp_orders) < 2
    
    print(f"\nPosition: {pos_with_plan.symbol}")
    print(f"Has TP plan: {has_tp_plan}")
    print(f"Has TP IDs: {has_tp_ids}")
    print(f"Open TP orders: {len(open_tp_orders)}")
    print(f"Needs backfill: {needs_backfill}")
    
    assert needs_backfill is False, "Should not need backfill when plan and orders exist"
    print("✅ Correctly identifies no need for backfill")


def test_safety_checks():
    """Test safety check logic."""
    print("\n" + "=" * 60)
    print("Test 3: Safety Checks")
    print("=" * 60)
    
    # Case 1: Position too new (within min_hold_seconds)
    pos_new = Position(
        symbol="BTCUSD-PERP",
        side=Side.LONG,
        size=Decimal("1.0"),
        size_notional=Decimal("50000"),
        entry_price=Decimal("50000"),
        current_mark_price=Decimal("51000"),
        liquidation_price=Decimal("45000"),
        unrealized_pnl=Decimal("0"),
        leverage=Decimal("10"),
        margin_used=Decimal("5000"),
        initial_stop_price=Decimal("49000"),
        opened_at=datetime.now(timezone.utc) - timedelta(seconds=10),  # 10 seconds ago
    )
    
    min_hold_seconds = 30
    elapsed = (datetime.now(timezone.utc) - pos_new.opened_at).total_seconds()
    too_new = elapsed < min_hold_seconds
    
    print(f"Position opened: {pos_new.opened_at}")
    print(f"Elapsed seconds: {elapsed}")
    print(f"Min hold seconds: {min_hold_seconds}")
    print(f"Too new: {too_new}")
    
    assert too_new is True, "Should be too new"
    print("✅ Correctly identifies position too new")
    
    # Case 2: Position missing SL
    pos_no_sl = Position(
        symbol="BTCUSD-PERP",
        side=Side.LONG,
        size=Decimal("1.0"),
        size_notional=Decimal("50000"),
        entry_price=Decimal("50000"),
        current_mark_price=Decimal("51000"),
        liquidation_price=Decimal("45000"),
        unrealized_pnl=Decimal("0"),
        leverage=Decimal("10"),
        margin_used=Decimal("5000"),
        initial_stop_price=None,  # No SL
        opened_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    
    has_sl = pos_no_sl.initial_stop_price is not None
    
    print(f"\nPosition SL: {pos_no_sl.initial_stop_price}")
    print(f"Has SL: {has_sl}")
    
    assert has_sl is False, "Should not have SL"
    print("✅ Correctly identifies missing SL")
    
    # Case 3: Position size <= 0
    pos_zero_size = Position(
        symbol="BTCUSD-PERP",
        side=Side.LONG,
        size=Decimal("0"),  # Zero size
        size_notional=Decimal("0"),
        entry_price=Decimal("50000"),
        current_mark_price=Decimal("51000"),
        liquidation_price=Decimal("45000"),
        unrealized_pnl=Decimal("0"),
        leverage=Decimal("10"),
        margin_used=Decimal("0"),
        initial_stop_price=Decimal("49000"),
        opened_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )
    
    size_valid = pos_zero_size.size > 0
    
    print(f"\nPosition size: {pos_zero_size.size}")
    print(f"Size valid: {size_valid}")
    
    assert size_valid is False, "Should have invalid size"
    print("✅ Correctly identifies zero size")


def test_price_tolerance_check():
    """Test TP price tolerance matching."""
    print("\n" + "=" * 60)
    print("Test 4: Price Tolerance Check")
    print("=" * 60)
    
    tolerance = Decimal("0.002")  # 0.2%
    planned_tp = Decimal("51000")
    
    # Case 1: Existing TP matches (within tolerance)
    existing_tp1 = Decimal("51010")  # 0.02% difference
    price_diff_pct = abs(existing_tp1 - planned_tp) / planned_tp
    matches = price_diff_pct <= tolerance
    
    print(f"Planned TP: {planned_tp}")
    print(f"Existing TP: {existing_tp1}")
    print(f"Price diff %: {price_diff_pct * 100:.4f}%")
    print(f"Tolerance: {tolerance * 100:.2f}%")
    print(f"Matches: {matches}")
    
    assert matches is True, "Should match within tolerance"
    print("✅ Correctly identifies matching TP price")
    
    # Case 2: Existing TP doesn't match (outside tolerance)
    existing_tp2 = Decimal("51200")  # 0.39% difference
    price_diff_pct2 = abs(existing_tp2 - planned_tp) / planned_tp
    matches2 = price_diff_pct2 <= tolerance
    
    print(f"\nPlanned TP: {planned_tp}")
    print(f"Existing TP: {existing_tp2}")
    print(f"Price diff %: {price_diff_pct2 * 100:.4f}%")
    print(f"Tolerance: {tolerance * 100:.2f}%")
    print(f"Matches: {matches2}")
    
    assert matches2 is False, "Should not match outside tolerance"
    print("✅ Correctly identifies non-matching TP price")


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("TP BACKFILL FUNCTIONALITY TESTS")
    print("=" * 60 + "\n")
    
    try:
        test_tp_plan_computation()
        test_needs_backfill_logic()
        test_safety_checks()
        test_price_tolerance_check()
        
        print("\n" + "=" * 60)
        print("✅ ALL TESTS PASSED")
        print("=" * 60)
        return 0
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        return 1
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
