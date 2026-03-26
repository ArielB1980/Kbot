import pytest

from types import SimpleNamespace

from src.live.auction_runner import (
    _compute_symbol_churn_cooldowns,
    _db_backed_cooldowns_enabled,
    _split_reconcile_issues,
    _filter_strategic_closes_for_gate,
    _resolve_is_protective_orders_live,
    _resolve_symbol_cooldown_params,
    _score_std,
    _should_bypass_churn_cooldowns,
    _symbol_in_canary,
)


def test_split_reconcile_issues_only_orphaned_non_blocking():
    blocking, non_blocking = _split_reconcile_issues(
        [("SOL/USD", "ORPHANED: Registry has position, exchange does not")]
    )
    assert blocking == []
    assert len(non_blocking) == 1


def test_split_reconcile_issues_treats_resolved_cleanup_as_non_blocking():
    blocking, non_blocking = _split_reconcile_issues(
        [
            ("AAVE/USD", "CLOSED_ON_EXCHANGE_MISSING_AFTER_EXIT: miss_count=2"),
            ("PF_DASHUSD", "DEDUPED: canonical=DASH/USD"),
        ]
    )
    assert blocking == []
    assert len(non_blocking) == 2


def test_split_reconcile_issues_blocks_non_orphaned():
    blocking, non_blocking = _split_reconcile_issues(
        [
            ("SOL/USD", "ORPHANED: Registry has position, exchange does not"),
            ("PF_ETHUSD", "PHANTOM: Exchange has position, registry does not"),
            ("PF_DOTUSD", "QTY_MISMATCH: Registry 1 vs Exchange 2"),
        ]
    )
    assert len(non_blocking) == 1
    assert len(blocking) == 2


def test_split_reconcile_issues_qty_synced_non_blocking_after_convergence():
    """QTY_SYNCED is informational: qty was aligned in the same reconcile pass."""
    blocking, non_blocking = _split_reconcile_issues(
        [
            (
                "PF_ETHUSD",
                "QTY_SYNCED: entry+0.5 local=0.0 exchange=0.5 price=3000",
            ),
        ]
    )
    assert blocking == []
    assert len(non_blocking) == 1


def test_split_reconcile_issues_missing_exchange_pending_non_blocking():
    blocking, non_blocking = _split_reconcile_issues(
        [
            (
                "PF_SOLUSD",
                "MISSING_ON_EXCHANGE_PENDING: miss_count=1/3",
            ),
        ]
    )
    assert blocking == []
    assert len(non_blocking) == 1


def test_split_reconcile_issues_missing_exchange_exit_grace_non_blocking():
    blocking, non_blocking = _split_reconcile_issues(
        [
            (
                "PF_LINKUSD",
                "MISSING_ON_EXCHANGE_EXIT_GRACE: age=1.8s",
            ),
        ]
    )
    assert blocking == []
    assert len(non_blocking) == 1


def test_filter_strategic_closes_allows_when_trading_allowed():
    closes = ["PF_SOLUSD", "PF_XLMUSD"]
    assert _filter_strategic_closes_for_gate(closes, trading_allowed=True) == closes


def test_filter_strategic_closes_suppresses_when_gate_closed():
    closes = ["PF_SOLUSD", "PF_XLMUSD"]
    assert _filter_strategic_closes_for_gate(closes, trading_allowed=False) == []


def test_resolve_symbol_cooldown_params_uses_base_values_without_canary():
    cfg = SimpleNamespace(
        symbol_loss_lookback_hours=24,
        symbol_loss_threshold=3,
        symbol_loss_cooldown_hours=12,
        symbol_loss_min_pnl_pct=-0.5,
        symbol_loss_cooldown_canary_enabled=False,
        symbol_loss_cooldown_canary_symbols=["SOL/USD"],
        symbol_loss_cooldown_canary_lookback_hours=12,
        symbol_loss_cooldown_canary_threshold=3,
        symbol_loss_cooldown_canary_hours=6,
        symbol_loss_cooldown_canary_min_pnl_pct=-0.8,
    )
    params = _resolve_symbol_cooldown_params(cfg, "SOL/USD")
    assert params["lookback_hours"] == 24
    assert params["cooldown_hours"] == 12
    assert params["min_pnl_pct"] == -0.5
    assert params["canary_applied"] is False


def test_resolve_symbol_cooldown_params_applies_canary_for_matching_symbol():
    cfg = SimpleNamespace(
        symbol_loss_lookback_hours=24,
        symbol_loss_threshold=3,
        symbol_loss_cooldown_hours=12,
        symbol_loss_min_pnl_pct=-0.5,
        symbol_loss_cooldown_canary_enabled=True,
        symbol_loss_cooldown_canary_symbols=["SOL/USD"],
        symbol_loss_cooldown_canary_lookback_hours=12,
        symbol_loss_cooldown_canary_threshold=3,
        symbol_loss_cooldown_canary_hours=6,
        symbol_loss_cooldown_canary_min_pnl_pct=-0.8,
    )
    params = _resolve_symbol_cooldown_params(cfg, "PF_SOLUSD")
    assert params["lookback_hours"] == 12
    assert params["cooldown_hours"] == 6
    assert params["min_pnl_pct"] == -0.8
    assert params["canary_applied"] is True


def test_score_std_zero_for_single_value():
    assert _score_std([42.0]) == 0.0


def test_score_std_non_zero_for_spread_values():
    assert _score_std([10.0, 20.0, 30.0]) > 0.0


def test_symbol_in_canary_true_when_canary_empty():
    assert _symbol_in_canary("SOL/USD", []) is True


def test_symbol_in_canary_normalizes_symbols():
    assert _symbol_in_canary("PF_SOLUSD", ["SOL/USD"]) is True


def test_db_backed_cooldowns_enabled_by_default():
    assert _db_backed_cooldowns_enabled(SimpleNamespace()) is True


def test_should_bypass_churn_cooldowns_false_when_book_not_flat():
    lt = SimpleNamespace(
        _auction_no_signal_cycles=20,
        config=SimpleNamespace(
            risk=SimpleNamespace(auction_no_signal_close_persistence_cycles=8)
        ),
    )
    assert _should_bypass_churn_cooldowns(lt, raw_positions=[{"symbol": "ETH/USD"}]) is False


def test_should_bypass_churn_cooldowns_false_before_threshold():
    lt = SimpleNamespace(
        _auction_no_signal_cycles=7,
        config=SimpleNamespace(
            risk=SimpleNamespace(auction_no_signal_close_persistence_cycles=8)
        ),
    )
    assert _should_bypass_churn_cooldowns(lt, raw_positions=[]) is False


def test_should_bypass_churn_cooldowns_true_for_flat_sustained_no_signal_regime():
    lt = SimpleNamespace(
        _auction_no_signal_cycles=8,
        config=SimpleNamespace(
            risk=SimpleNamespace(auction_no_signal_close_persistence_cycles=8)
        ),
    )
    assert _should_bypass_churn_cooldowns(lt, raw_positions=[]) is True


def test_resolve_is_protective_orders_live_true_with_explicit_order_ids():
    pos = SimpleNamespace(
        stop_loss_order_id="sl-123",
        tp_order_ids=[],
        is_protected=False,
    )
    assert _resolve_is_protective_orders_live(pos, replay_relaxed=False) is True


def test_resolve_is_protective_orders_live_replay_fallback_uses_protected_flag():
    pos = SimpleNamespace(
        stop_loss_order_id=None,
        tp_order_ids=[],
        is_protected=True,
    )
    assert _resolve_is_protective_orders_live(pos, replay_relaxed=True) is True


def test_resolve_is_protective_orders_live_live_mode_keeps_strict_order_requirement():
    pos = SimpleNamespace(
        stop_loss_order_id=None,
        tp_order_ids=[],
        is_protected=True,
    )
    assert _resolve_is_protective_orders_live(pos, replay_relaxed=False) is False


@pytest.mark.asyncio
async def test_compute_symbol_churn_cooldowns_skips_live_db_in_replay(monkeypatch):
    def _unexpected_query(_since):
        raise AssertionError("replay cooldown path should not query live trade history")

    monkeypatch.setattr("src.live.auction_runner.get_trades_since", _unexpected_query)

    lt = SimpleNamespace(
        _replay_disable_db_backed_cooldowns=True,
        config=SimpleNamespace(
            risk=SimpleNamespace(
                auction_churn_guard_enabled=True,
                auction_churn_window_hours=6,
                auction_churn_hold_max_minutes=60,
                auction_churn_reopen_max_minutes=120,
                auction_churn_max_events=2,
                auction_churn_cooldown_tier1_minutes=30,
                auction_churn_cooldown_tier2_minutes=120,
                auction_churn_cooldown_tier3_minutes=360,
            )
        ),
    )

    assert await _compute_symbol_churn_cooldowns(lt) == {}
