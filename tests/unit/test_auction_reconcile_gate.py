from types import SimpleNamespace

from src.live.auction_runner import (
    _split_reconcile_issues,
    _filter_strategic_closes_for_gate,
    _resolve_symbol_cooldown_params,
    _score_std,
    _symbol_in_canary,
)


def test_split_reconcile_issues_only_orphaned_non_blocking():
    blocking, non_blocking = _split_reconcile_issues(
        [("SOL/USD", "ORPHANED: Registry has position, exchange does not")]
    )
    assert blocking == []
    assert len(non_blocking) == 1


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
