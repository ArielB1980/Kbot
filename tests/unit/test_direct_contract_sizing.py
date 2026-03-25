"""
Tests for direct contract sizing and unsplittable position handling.

Covers:
1. futures_adapter.place_order with size_contracts_override bypasses notional conversion
2. executor.update_protective_orders detects positions too small to split and returns SL-only
3. SL uses direct contracts when position_size_contracts is available
"""
from __future__ import annotations

import pytest
import pytest_asyncio
from decimal import Decimal, ROUND_DOWN
from unittest.mock import AsyncMock, Mock, MagicMock, patch

from src.execution.instrument_specs import InstrumentSpec, InstrumentSpecRegistry
from src.domain.models import Side


def _make_spec(symbol="PF_DOTUSD", min_size="0.1", size_step="0.1", max_leverage=50):
    return InstrumentSpec(
        symbol_raw=symbol,
        symbol_ccxt=f"{symbol.replace('PF_', '').replace('USD', '')}/USD:USD",
        base=symbol.replace("PF_", "").replace("USD", ""),
        quote="USD",
        contract_size=Decimal("1"),
        min_size=Decimal(min_size),
        size_step=Decimal(size_step),
        max_leverage=max_leverage,
        leverage_mode="unknown",
        allowed_leverages=[],
    )


def _make_registry(*specs):
    reg = Mock(spec=InstrumentSpecRegistry)
    spec_map = {s.symbol_raw: s for s in specs}
    reg.get_spec.side_effect = lambda sym: spec_map.get(sym)
    reg.get_effective_min_size.side_effect = lambda sym: spec_map[sym].min_size if sym in spec_map else Decimal("0.001")
    reg.ensure_loaded = Mock()
    reg.refresh = AsyncMock()
    reg.log_unknown_leverage_once = Mock()
    return reg


# ============================================================
# Test: size_contracts_override in futures_adapter.place_order
# ============================================================

class TestDirectContractSizing:
    """Test that size_contracts_override bypasses the notional→contract round-trip."""

    @pytest.mark.asyncio
    async def test_override_skips_notional_conversion(self):
        """When size_contracts_override is set, place_order uses it directly (no notional math)."""
        from src.execution.futures_adapter import FuturesAdapter, OrderType

        spec = _make_spec("PF_DOTUSD", min_size="0.1", size_step="0.1")
        registry = _make_registry(spec)

        kraken = AsyncMock()
        kraken.place_futures_order = AsyncMock(return_value={
            "id": "test-order-123",
            "info": {"cliOrdId": "order_abc123"},
        })

        adapter = FuturesAdapter.__new__(FuturesAdapter)
        adapter.kraken_client = kraken
        adapter.max_leverage = 5
        adapter.instrument_spec_registry = registry
        adapter.cached_futures_tickers = {}

        order = await adapter.place_order(
            symbol="PF_DOTUSD",
            side=Side.SHORT,
            size_notional=Decimal("0"),  # would fail if used
            leverage=Decimal("1"),
            order_type=OrderType.STOP_LOSS,
            price=Decimal("1.4"),
            reduce_only=True,
            size_contracts_override=Decimal("0.1"),
        )

        assert order.order_id == "test-order-123"
        call_args = kraken.place_futures_order.call_args
        assert call_args.kwargs.get("size") == Decimal("0.1")

    @pytest.mark.asyncio
    async def test_override_still_validates_min_size(self):
        """size_contracts_override below min_size still raises ValueError (entry order, rounds DOWN)."""
        from src.execution.futures_adapter import FuturesAdapter, OrderType

        spec = _make_spec("PF_DOTUSD", min_size="0.1", size_step="0.1")
        registry = _make_registry(spec)

        adapter = FuturesAdapter.__new__(FuturesAdapter)
        adapter.kraken_client = AsyncMock()
        adapter.max_leverage = 5
        adapter.instrument_spec_registry = registry
        adapter.cached_futures_tickers = {}

        # reduce_only=False → ROUND_DOWN → 0.05 rounds to 0.0 → alignment fails
        with pytest.raises(ValueError, match="alignment failed"):
            await adapter.place_order(
                symbol="PF_DOTUSD",
                side=Side.SHORT,
                size_notional=Decimal("0"),
                leverage=Decimal("1"),
                order_type=OrderType.STOP_LOSS,
                price=Decimal("1.4"),
                reduce_only=False,
                size_contracts_override=Decimal("0.05"),
            )

    @pytest.mark.asyncio
    async def test_override_applies_step_alignment(self):
        """size_contracts_override is rounded to size_step (ROUND_UP for reduce_only)."""
        from src.execution.futures_adapter import FuturesAdapter, OrderType

        spec = _make_spec("PF_DOTUSD", min_size="0.1", size_step="0.1")
        registry = _make_registry(spec)

        kraken = AsyncMock()
        kraken.place_futures_order = AsyncMock(return_value={
            "id": "test-order-456",
            "info": {"cliOrdId": "order_def456"},
        })

        adapter = FuturesAdapter.__new__(FuturesAdapter)
        adapter.kraken_client = kraken
        adapter.max_leverage = 5
        adapter.instrument_spec_registry = registry
        adapter.cached_futures_tickers = {}

        order = await adapter.place_order(
            symbol="PF_DOTUSD",
            side=Side.SHORT,
            size_notional=Decimal("0"),
            leverage=Decimal("1"),
            order_type=OrderType.STOP_LOSS,
            price=Decimal("1.4"),
            reduce_only=True,
            size_contracts_override=Decimal("0.15"),  # not aligned to 0.1 step
        )

        call_args = kraken.place_futures_order.call_args
        placed_size = call_args.kwargs.get("size")
        assert placed_size == Decimal("0.2")  # ROUND_UP for reduce_only

    @pytest.mark.asyncio
    async def test_notional_round_trip_fails_but_override_succeeds(self):
        """
        Proves the bug: 0.1 DOT at $1.36 → notional 0.136.
        With a slightly different mark price (1.361), notional→contracts rounds to 0.
        But size_contracts_override=0.1 works perfectly.
        """
        from src.execution.futures_adapter import FuturesAdapter, OrderType
        from src.execution.instrument_specs import compute_size_contracts

        spec = _make_spec("PF_DOTUSD", min_size="0.1", size_step="0.1")

        # The round-trip problem: notional 0.136 with price 1.361 → 0 contracts
        notional = Decimal("0.136")
        mark_price = Decimal("1.361")
        contracts, reason = compute_size_contracts(spec, notional, mark_price)
        assert reason == "SIZE_STEP_ROUND_TO_ZERO", "Should demonstrate the round-trip bug"

        # Direct contract override: 0.1 works fine
        registry = _make_registry(spec)
        kraken = AsyncMock()
        kraken.place_futures_order = AsyncMock(return_value={
            "id": "direct-ok",
            "info": {"cliOrdId": "order_direct"},
        })

        adapter = FuturesAdapter.__new__(FuturesAdapter)
        adapter.kraken_client = kraken
        adapter.max_leverage = 5
        adapter.instrument_spec_registry = registry
        adapter.cached_futures_tickers = {}

        order = await adapter.place_order(
            symbol="PF_DOTUSD",
            side=Side.SHORT,
            size_notional=notional,
            leverage=Decimal("1"),
            order_type=OrderType.STOP_LOSS,
            price=Decimal("1.4"),
            reduce_only=True,
            size_contracts_override=Decimal("0.1"),
        )
        assert order.order_id == "direct-ok"


# ============================================================
# Test: unsplittable position detection
# ============================================================

class TestUnsplittablePosition:
    """Test that positions too small for multi-TP splitting return SL-only."""

    @pytest.fixture
    def mock_executor(self):
        from src.execution.executor import Executor
        executor = Executor.__new__(Executor)
        executor.config = Mock()
        executor.config.tp_splits = [Decimal("0.35"), Decimal("0.35"), Decimal("0.30")]
        executor.futures_adapter = AsyncMock()
        executor.futures_adapter.cancel_order = AsyncMock()
        executor.futures_adapter.place_order = AsyncMock(return_value=Mock(order_id="sl-placed-123"))
        return executor

    @pytest.fixture
    def multi_tp_config(self):
        cfg = Mock()
        cfg.enabled = True
        cfg.runner_has_fixed_tp = False
        cfg.tp1_close_pct = 0.4
        cfg.tp2_close_pct = 0.4
        cfg.runner_pct = 0.2
        return cfg

    @pytest.mark.asyncio
    async def test_dust_position_degrades_to_single_tp(self, mock_executor, multi_tp_config):
        """
        0.1 DOT with size_step=0.1: partial splits round to zero, but the remainder
        (full position) is valid. System degrades to SL + 1 TP (for full size at first TP price).
        """
        spec = _make_spec("PF_DOTUSD", min_size="0.1", size_step="0.1")
        registry = _make_registry(spec)

        call_count = 0
        async def mock_place(**kwargs):
            nonlocal call_count
            call_count += 1
            return Mock(order_id=f"order-{call_count}")
        mock_executor.futures_adapter.place_order = mock_place

        sl_id, tp_ids = await mock_executor.update_protective_orders(
            symbol="PF_DOTUSD",
            side=Side.SHORT,
            current_sl_id=None,
            new_sl_price=Decimal("1.42"),
            current_tp_ids=[],
            new_tp_prices=[Decimal("1.35"), Decimal("1.33")],
            position_size_notional=Decimal("0.136"),
            position_size_contracts=Decimal("0.1"),
            current_price=Decimal("1.36"),
            multi_tp_config=multi_tp_config,
            instrument_spec_registry=registry,
        )

        assert sl_id == "order-1", "SL should be placed"
        assert len(tp_ids) == 1, "Should degrade to 1 TP for full position size"

    @pytest.mark.asyncio
    async def test_truly_unsplittable_gets_sl_only(self, mock_executor, multi_tp_config):
        """Position smaller than venue min → no TPs possible, SL only."""
        spec = _make_spec("PF_XYZUSD", min_size="1.0", size_step="1.0")
        registry = _make_registry(spec)

        sl_id, tp_ids = await mock_executor.update_protective_orders(
            symbol="PF_XYZUSD",
            side=Side.SHORT,
            current_sl_id=None,
            new_sl_price=Decimal("100"),
            current_tp_ids=[],
            new_tp_prices=[Decimal("90"), Decimal("85")],
            position_size_notional=Decimal("50"),
            position_size_contracts=Decimal("0.5"),  # below min_size of 1.0
            current_price=Decimal("100"),
            multi_tp_config=multi_tp_config,
            instrument_spec_registry=registry,
        )

        assert sl_id == "sl-placed-123", "SL should still be placed"
        assert tp_ids == [], "No TPs possible when all partials below venue min"

    @pytest.mark.asyncio
    async def test_normal_position_gets_sl_and_tps(self, mock_executor, multi_tp_config):
        """10 DOT with size_step=0.1: splits are large enough for TPs."""
        spec = _make_spec("PF_DOTUSD", min_size="0.1", size_step="0.1")
        registry = _make_registry(spec)

        # Mock place_order to return different IDs for SL and TPs
        call_count = 0
        async def mock_place(**kwargs):
            nonlocal call_count
            call_count += 1
            return Mock(order_id=f"order-{call_count}")
        mock_executor.futures_adapter.place_order = mock_place

        sl_id, tp_ids = await mock_executor.update_protective_orders(
            symbol="PF_DOTUSD",
            side=Side.SHORT,
            current_sl_id=None,
            new_sl_price=Decimal("1.42"),
            current_tp_ids=[],
            new_tp_prices=[Decimal("1.35"), Decimal("1.33")],
            position_size_notional=Decimal("13.6"),
            position_size_contracts=Decimal("10"),
            current_price=Decimal("1.36"),
            multi_tp_config=multi_tp_config,
            instrument_spec_registry=registry,
        )

        assert sl_id == "order-1", "SL should be placed"
        assert len(tp_ids) == 2, "Both TPs should be placed for normal-sized position"

    @pytest.mark.asyncio
    async def test_sl_uses_direct_contracts(self, mock_executor, multi_tp_config):
        """SL placement passes size_contracts_override instead of going through notional."""
        spec = _make_spec("PF_DOTUSD", min_size="0.1", size_step="0.1")
        registry = _make_registry(spec)

        place_calls = []
        async def capture_place(**kwargs):
            place_calls.append(kwargs)
            return Mock(order_id="sl-direct")
        mock_executor.futures_adapter.place_order = capture_place

        await mock_executor.update_protective_orders(
            symbol="PF_DOTUSD",
            side=Side.SHORT,
            current_sl_id=None,
            new_sl_price=Decimal("1.42"),
            current_tp_ids=[],
            new_tp_prices=[],
            position_size_notional=Decimal("0.136"),
            position_size_contracts=Decimal("0.1"),
            current_price=Decimal("1.36"),
            multi_tp_config=multi_tp_config,
            instrument_spec_registry=registry,
        )

        assert len(place_calls) == 1, "Only SL should be placed"
        sl_call = place_calls[0]
        assert sl_call["size_contracts_override"] == Decimal("0.1"), \
            "SL should use direct contract size, not notional"
