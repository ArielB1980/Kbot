from src.config.config import StrategyConfig
from src.strategy.ev_engine import EVEngine


def test_ev_engine_defaults_to_neutral_prior_for_empty_bucket() -> None:
    engine = EVEngine(StrategyConfig())
    result = engine.compute(
        {
            "symbol": "BTC/USD",
            "conviction": 62.0,
            "regime": "wide_structure",
            "risk_r": 1.0,
        }
    )
    assert result.prior_win_prob == 0.5
    assert result.posterior_win_prob == 0.5
    assert result.ev_r == 0.0
    assert result.ev_usd == 0.0
