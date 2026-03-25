from src.backtest.replay_harness.runner import _build_replay_synthetic_spec


def test_replay_synthetic_spec_uses_xbt_alias_for_btc():
    spec = _build_replay_synthetic_spec("BTC/USD")

    assert spec.symbol_raw == "PF_XBTUSD"
    assert spec.symbol_ccxt == "XBT/USD:USD"
    assert spec.base == "BTC"


def test_replay_synthetic_spec_keeps_native_alias_for_non_btc_symbols():
    spec = _build_replay_synthetic_spec("ETH/USD")

    assert spec.symbol_raw == "PF_ETHUSD"
    assert spec.symbol_ccxt == "ETH/USD:USD"
    assert spec.base == "ETH"
