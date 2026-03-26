"""
Phase 1 Safety Net: Configuration loading and validation test.

Verifies that the config system loads correctly, validates properly,
and respects environment variable overrides. This is a prerequisite
for all other tests -- if config loading is broken, nothing works.
"""
import os
import pytest
from decimal import Decimal
from pathlib import Path


CONFIG_PATH = "src/config/config.yaml"


def test_config_yaml_exists():
    """The config YAML file must exist at the expected path."""
    assert Path(CONFIG_PATH).exists(), (
        f"Config file not found at {CONFIG_PATH}. "
        f"Production startup depends on this file."
    )


def test_config_loads_successfully():
    """Config must load and parse without errors."""
    from src.config.config import load_config

    config = load_config(CONFIG_PATH)
    assert config is not None


def test_config_has_required_sections():
    """Config must have all required top-level sections."""
    from src.config.config import load_config

    config = load_config(CONFIG_PATH)

    # These sections are accessed throughout the codebase
    assert hasattr(config, "exchange"), "Missing exchange config section"
    assert hasattr(config, "risk"), "Missing risk config section"
    assert hasattr(config, "strategy"), "Missing strategy config section"
    assert hasattr(config, "execution"), "Missing execution config section"
    assert hasattr(config, "monitoring"), "Missing monitoring config section"


def test_config_risk_defaults_are_sane():
    """Risk config defaults must be within safe bounds.

    These are the critical risk parameters that protect capital.
    Changes to these defaults should be intentional and reviewed.
    """
    from src.config.config import load_config

    config = load_config(CONFIG_PATH)

    # Max leverage must not exceed exchange limits
    assert config.risk.max_leverage <= Decimal("10"), (
        f"Max leverage {config.risk.max_leverage} exceeds safe limit of 10x"
    )

    # Risk per trade must be reasonable
    assert Decimal("0") < config.risk.risk_per_trade_pct <= Decimal("0.05"), (
        f"Risk per trade {config.risk.risk_per_trade_pct} outside safe range (0, 0.05]"
    )


def test_config_strategy_defaults_loaded():
    """Strategy config must load with valid indicator parameters."""
    from src.config.config import load_config

    config = load_config(CONFIG_PATH)

    # EMA period must be positive
    assert config.strategy.ema_period > 0, "EMA period must be positive"

    # ADX threshold must be in reasonable range
    assert 0 < config.strategy.adx_threshold < 100, (
        f"ADX threshold {config.strategy.adx_threshold} outside valid range"
    )


def test_config_env_override():
    """Environment variables must override YAML values.

    This is how production configures different behavior
    (e.g., ENVIRONMENT=prod vs ENVIRONMENT=dev).
    """
    from src.config.config import load_config

    # Set env var to a valid non-default value and reload
    # Using "dev" (not "prod") to avoid strict startup validation
    # that requires DRY_RUN + DATABASE_URL in production mode
    original = os.environ.get("ENVIRONMENT")
    try:
        os.environ["ENVIRONMENT"] = "dev"
        config = load_config(CONFIG_PATH)
        assert config.environment == "dev", (
            "ENVIRONMENT env var did not override config value"
        )
    finally:
        if original is not None:
            os.environ["ENVIRONMENT"] = original
        else:
            os.environ.pop("ENVIRONMENT", None)


def test_config_schema_version_present():
    """Config schema version must be defined for migration tracking."""
    from src.config.config import CONFIG_SCHEMA_VERSION

    assert CONFIG_SCHEMA_VERSION is not None
    assert len(CONFIG_SCHEMA_VERSION) > 0




def test_strategy_config_tier2_structure_fallback_fields():
    """Tier 2 (KBO-13) 1H fallback flags are validated and default-off."""
    from src.config.config import StrategyConfig

    s = StrategyConfig()
    assert s.structure_fallback_enabled is False
    assert s.structure_fallback_score_premium == 15.0

    s2 = StrategyConfig(structure_fallback_enabled=True, structure_fallback_score_premium=12.0)
    assert s2.structure_fallback_enabled is True
    assert s2.structure_fallback_score_premium == 12.0

def test_symbol_override_resolvers():
    """Per-symbol override resolvers should apply only to target symbol."""
    from src.config.config import (
        RiskConfig,
        RiskSymbolOverride,
        StrategyConfig,
        StrategySymbolOverride,
        resolve_risk_for_symbol,
        resolve_strategy_for_symbol,
    )

    strategy = StrategyConfig(
        signal_cooldown_hours=4.0,
        symbol_overrides={"BTC/USD": StrategySymbolOverride(signal_cooldown_hours=1.0)},
    )
    risk = RiskConfig(
        target_leverage=7.0,
        symbol_overrides={"BTC/USD": RiskSymbolOverride(target_leverage=3.0)},
    )

    btc_strategy = resolve_strategy_for_symbol(strategy, "PF_BTCUSD")
    eth_strategy = resolve_strategy_for_symbol(strategy, "ETH/USD")
    assert btc_strategy.signal_cooldown_hours == 1.0
    assert eth_strategy.signal_cooldown_hours == 4.0

    btc_risk = resolve_risk_for_symbol(risk, "BTC/USD")
    eth_risk = resolve_risk_for_symbol(risk, "ETH/USD")
    assert btc_risk.target_leverage == 3.0
    assert eth_risk.target_leverage == 7.0


def test_live_research_overrides_are_clamped_on_load(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from src.config.config import load_config

    overrides_path = tmp_path / "live_research_overrides.yaml"
    overrides_path.write_text(
        """
strategy:
  symbol_overrides:
    ETH/USD:
      entry_zone_tolerance_pct: 0.051473
      fvg_min_size_pct: -0.000248
risk:
  symbol_overrides:
    ETH/USD:
      target_leverage: 999
""".strip()
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("RESEARCH_LIVE_OVERRIDES_PATH", str(overrides_path))
    config = load_config(CONFIG_PATH)

    eth_strategy = config.strategy.symbol_overrides["ETH/USD"]
    assert eth_strategy.entry_zone_tolerance_pct == 0.05
    assert eth_strategy.fvg_min_size_pct == 0.0001

    eth_risk = config.risk.symbol_overrides["ETH/USD"]
    assert eth_risk.target_leverage == 10.0


def test_live_research_overrides_drop_non_numeric_values(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from src.config.config import load_config

    overrides_path = tmp_path / "live_research_overrides.yaml"
    overrides_path.write_text(
        """
strategy:
  symbol_overrides:
    BTC/USD:
      adx_threshold: bad-input
      signal_cooldown_hours: 2.0
""".strip()
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("RESEARCH_LIVE_OVERRIDES_PATH", str(overrides_path))
    config = load_config(CONFIG_PATH)

    btc_strategy = config.strategy.symbol_overrides["BTC/USD"]
    assert btc_strategy.signal_cooldown_hours == 2.0
    assert btc_strategy.adx_threshold is None
