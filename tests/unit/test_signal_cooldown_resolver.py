from types import SimpleNamespace
from datetime import datetime, timezone
from decimal import Decimal

from src.live.live_trading import (
    _build_4h_warmup_skip_diagnostic,
    _resolve_post_close_cooldown_kind_and_minutes,
    _resolve_signal_cooldown_params,
)
from src.domain.models import Candle


def test_signal_cooldown_resolver_uses_base_values_without_canary():
    cfg = SimpleNamespace(
        signal_cooldown_hours=4.0,
        signal_cooldown_canary_enabled=False,
        signal_cooldown_canary_symbols=["BTC/USD"],
        signal_cooldown_hours_canary=1.0,
    )
    params = _resolve_signal_cooldown_params(cfg, "BTC/USD")
    assert params["cooldown_hours"] == 4.0
    assert params["canary_applied"] is False


def test_signal_cooldown_resolver_ignores_canary_override_for_matching_symbol():
    cfg = SimpleNamespace(
        signal_cooldown_hours=4.0,
        signal_cooldown_canary_enabled=True,
        signal_cooldown_canary_symbols=["BTC/USD", "ETH/USD"],
        signal_cooldown_hours_canary=1.0,
    )
    params = _resolve_signal_cooldown_params(cfg, "PF_BTCUSD")
    assert params["cooldown_hours"] == 4.0
    assert params["canary_applied"] is False


def test_signal_cooldown_resolver_keeps_base_for_non_canary_symbol():
    cfg = SimpleNamespace(
        signal_cooldown_hours=4.0,
        signal_cooldown_canary_enabled=True,
        signal_cooldown_canary_symbols=["BTC/USD", "ETH/USD"],
        signal_cooldown_hours_canary=1.0,
    )
    params = _resolve_signal_cooldown_params(cfg, "SOL/USD")
    assert params["cooldown_hours"] == 4.0
    assert params["canary_applied"] is False


def test_signal_cooldown_resolver_applies_symbol_override():
    cfg = SimpleNamespace(
        signal_cooldown_hours=4.0,
        symbol_overrides={
            "BTC/USD": SimpleNamespace(signal_cooldown_hours=1.5),
        },
    )
    params = _resolve_signal_cooldown_params(cfg, "PF_BTCUSD")
    assert params["cooldown_hours"] == 1.5
    assert params["canary_applied"] is True


def test_post_close_cooldown_classifies_stop_as_loss_bucket():
    cfg = SimpleNamespace(
        signal_post_close_cooldown_loss_minutes=180,
        signal_post_close_cooldown_win_minutes=20,
    )
    kind, minutes = _resolve_post_close_cooldown_kind_and_minutes("Stop Loss Hit", cfg)
    assert kind == "POST_CLOSE_LOSS"
    assert minutes == 180


def test_post_close_cooldown_classifies_tp_as_win_bucket():
    cfg = SimpleNamespace(
        signal_post_close_cooldown_loss_minutes=180,
        signal_post_close_cooldown_win_minutes=20,
    )
    kind, minutes = _resolve_post_close_cooldown_kind_and_minutes("Take Profit", cfg)
    assert kind == "POST_CLOSE_WIN"
    assert minutes == 20


def test_post_close_cooldown_classifies_strategic_close_with_dedicated_bucket():
    cfg = SimpleNamespace(
        signal_post_close_cooldown_loss_minutes=180,
        signal_post_close_cooldown_win_minutes=20,
        signal_post_close_cooldown_strategic_minutes=90,
    )
    kind, minutes = _resolve_post_close_cooldown_kind_and_minutes(
        "AUCTION_STRATEGIC_CLOSE time_based",
        cfg,
    )
    assert kind == "POST_CLOSE_STRATEGIC"
    assert minutes == 90


def test_4h_warmup_diagnostic_emits_for_canary_symbol_with_insufficient_candles():
    cfg = SimpleNamespace(
        signal_cooldown_canary_symbols=["BTC/USD", "ETH/USD"],
        fvg_min_size_pct_canary_symbols=["SOL/USD"],
    )
    candles = [
        Candle(
            timestamp=datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),
            symbol="BTC/USD",
            timeframe="4h",
            open=Decimal("1"),
            high=Decimal("1"),
            low=Decimal("1"),
            close=Decimal("1"),
            volume=Decimal("1"),
        )
    ]
    payload = _build_4h_warmup_skip_diagnostic(
        strategy_config=cfg,
        symbol="BTC/USD",
        futures_symbol="PF_XBTUSD",
        stage_b_reason="candles_4h=198 < 250",
        candles_4h=candles,
        required_candles=250,
        decision_tf="4h",
    )
    assert payload is not None
    assert payload["symbol"] == "BTC/USD"
    assert payload["candles_4h_count"] == 1
    assert payload["required_candles_4h"] == 250
    assert payload["skip_reason"] == "insufficient_4h_history"
    assert payload["is_canary"] is True


def test_4h_warmup_diagnostic_skips_non_canary_symbol():
    cfg = SimpleNamespace(
        signal_cooldown_canary_symbols=["BTC/USD", "ETH/USD"],
        fvg_min_size_pct_canary_symbols=["SOL/USD"],
    )
    payload = _build_4h_warmup_skip_diagnostic(
        strategy_config=cfg,
        symbol="XMR/USD",
        futures_symbol="PF_XMRUSD",
        stage_b_reason="candles_4h=10 < 250",
        candles_4h=[],
        required_candles=250,
        decision_tf="4h",
    )
    assert payload is None
