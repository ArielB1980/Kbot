"""
Configuration models for the Kraken Futures SMC Trading System.

Uses Pydantic for validation and type safety.
"""
from typing import Any, Dict, List, Literal, Optional
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
import yaml
from pathlib import Path
from decimal import Decimal
import os

CONFIG_SCHEMA_VERSION = "2026-02-01"

class ExchangeConfig(BaseSettings):
    """Exchange configuration."""
    model_config = SettingsConfigDict(extra="ignore")

    name: str = "kraken"
    
    # Market Discovery (for multi-asset expansion)
    use_market_discovery: bool = True
    discovery_refresh_hours: int = 24
    market_discovery_cache_minutes: int = 60
    allow_futures_only_universe: bool = False
    allow_futures_only_pairs: bool = False
    market_discovery_failure_log_cooldown_minutes: int = 60
    
    # Legacy: Hardcoded markets (used if use_market_discovery=False)
    spot_markets: List[str] = ["BTC/USD", "ETH/USD"]
    futures_markets: List[str] = ["BTCUSD-PERP", "ETHUSD-PERP"]
    
    # Skip OHLCV fetch for these spot symbols (delisted, unsupported, or consistently failing)
    spot_ohlcv_blocklist: List[str] = Field(default_factory=lambda: ["2Z/USD", "ANIME/USD"], description="Excluded from OHLCV")

    # When spot OHLCV unavailable (BadSymbol, no data), use futures OHLCV for signal analysis
    use_futures_ohlcv_fallback: bool = Field(default=True, description="Use futures candles when spot has 0")
    
    # Credentials (loaded from env or yaml)
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    futures_api_key: Optional[str] = None
    futures_api_secret: Optional[str] = None
    use_testnet: bool = False

    # Circuit breaker thresholds (P3.1)
    circuit_breaker_failure_threshold: int = Field(default=5, ge=2, le=20, description="Consecutive failures before opening breaker")
    circuit_breaker_rate_limit_threshold: int = Field(default=2, ge=1, le=10, description="Rate limit errors before opening breaker")
    circuit_breaker_cooldown_seconds: float = Field(default=60.0, ge=10.0, le=300.0, description="Seconds before half-open probe")
    
    # Position size format (for exchange compatibility)
    # If True: exchange returns position size as notional USD (don't multiply by price)
    # If False: exchange returns size in contracts/base units (multiply by price to get notional)
    # Default False for Kraken Futures (returns contracts)
    position_size_is_notional: bool = Field(default=False, description="True if exchange returns size as notional, False if contracts")


class StrategySymbolOverride(BaseSettings):
    """Optional per-symbol strategy overrides."""
    model_config = SettingsConfigDict(extra="ignore")

    adx_threshold: Optional[float] = Field(default=None, ge=10.0, le=40.0)
    fvg_min_size_pct: Optional[float] = Field(default=None, ge=0.0001, le=0.01)
    entry_zone_tolerance_pct: Optional[float] = Field(default=None, ge=0.005, le=0.05)
    entry_zone_tolerance_atr_mult: Optional[float] = Field(default=None, ge=0.1, le=1.0)
    min_score_tight_smc_aligned: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    min_score_wide_structure_aligned: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    min_score_wide_structure_neutral: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    signal_cooldown_hours: Optional[float] = Field(default=None, ge=0.0, le=24.0)
    tight_smc_atr_stop_min: Optional[float] = Field(default=None, ge=0.05, le=1.0)
    tight_smc_atr_stop_max: Optional[float] = Field(default=None, ge=0.05, le=1.0)
    wide_structure_atr_stop_min: Optional[float] = Field(default=None, ge=0.2, le=2.0)
    wide_structure_atr_stop_max: Optional[float] = Field(default=None, ge=0.2, le=2.0)
    ema_slope_bonus: Optional[float] = Field(default=None, ge=0.0, le=15.0)


class RiskSymbolOverride(BaseSettings):
    """Optional per-symbol risk overrides."""
    model_config = SettingsConfigDict(extra="ignore")

    risk_per_trade_pct: Optional[float] = Field(default=None, ge=0.0001, le=0.05)
    max_leverage: Optional[float] = Field(default=None, ge=1.0, le=10.0)
    target_leverage: Optional[float] = Field(default=None, ge=1.0, le=10.0)
    max_position_size_usd: Optional[float] = Field(default=None, ge=1000.0, le=1000000.0)
    tight_smc_cost_cap_bps: Optional[float] = Field(default=None, ge=10.0, le=50.0)
    tight_smc_min_rr_multiple: Optional[float] = Field(default=None, ge=1.5, le=5.0)
    wide_structure_max_distortion_pct: Optional[float] = Field(default=None, ge=0.10, le=0.25)


class RiskConfig(BaseSettings):
    """Risk management configuration."""
    model_config = SettingsConfigDict(extra="ignore")

    # Position sizing
    risk_per_trade_pct: float = Field(default=0.005, ge=0.0001, le=0.05)
    max_leverage: float = Field(default=10.0, ge=1.0, le=10.0)
    target_leverage: float = Field(default=7.0, ge=1.0, le=10.0)  # Actual leverage to use

    # Sizing Method: fixed, kelly, volatility, kelly_volatility, leverage_based
    sizing_method: Literal["fixed", "kelly", "volatility", "kelly_volatility", "leverage_based"] = "fixed"
    
    # Kelly Criterion Settings
    kelly_win_prob: float = Field(default=0.55, ge=0.1, le=0.9)
    kelly_win_loss_ratio: float = Field(default=2.0, ge=1.0)
    kelly_max_fraction: float = Field(default=0.25, ge=0.01, le=1.0) # Cap Kelly (Quarter Kelly usually safe)
    
    # Volatility Sizing Settings
    vol_sizing_atr_threshold_high: float = Field(default=1.5, ge=1.0) # Reduce size if ATR > 1.5x Avg
    vol_sizing_atr_threshold_low: float = Field(default=0.5, le=1.0) # Increase size if ATR < 0.5x Avg
    vol_sizing_high_vol_penalty: float = Field(default=0.30, le=0.9) # -30% size
    vol_sizing_low_vol_boost: float = Field(default=0.20, le=0.5) # +20% size
    
    # Position size caps
    max_position_size_usd: float = Field(default=100000.0, ge=1000.0, le=1000000.0)  # Max notional position
    max_risk_per_trade_entry_pct: float = Field(default=0.02, ge=0.001, le=0.10)  # Max risk per trade for Kelly
    max_loss_per_trade_usd: float = Field(
        default=500.0, ge=10.0, le=50000.0,
        description="Max dollar loss if stop hits. Rejects trades where abs(entry - stop) * size > this."
    )
    
    # Margin-based caps (replace notional caps; vs 7x leverage)
    max_single_position_margin_pct_equity: float = Field(
        default=0.25, ge=0.05, le=0.50,
        description="Max margin per position as % of equity (25% = 1.75x notional at 7x leverage)"
    )
    max_aggregate_margin_pct_equity: float = Field(
        default=2.0, ge=0.5, le=5.0,
        description="Max total margin across all positions as % of equity (200% = 14x notional at 7x leverage)"
    )

    # Liquidation safety
    min_liquidation_buffer_pct: float = Field(default=0.35, ge=0.30, le=0.50)
    
    # Portfolio limits
    max_concurrent_positions: int = Field(default=2, ge=1, le=100)
    replacement_enabled: bool = Field(
        default=False,
        description="If True, may close an existing position to make room for a new entry (disabled by default; not permitted in prod live unless explicitly enabled).",
    )
    daily_loss_limit_pct: float = Field(default=0.02, ge=0.01, le=0.10)
    
    # Auction mode portfolio limits
    auction_mode_enabled: bool = Field(default=False, description="Enable auction-based portfolio allocation")
    auction_max_positions: int = Field(default=50, ge=1, le=100)
    auction_max_margin_util: float = Field(default=0.90, ge=0.50, le=0.95)
    auction_max_per_cluster: int = Field(default=8, ge=1, le=50)  # Balanced for 25 positions (was 12)
    auction_max_per_symbol: int = Field(default=1, ge=1, le=5)
    auction_swap_threshold: float = Field(default=10.0, ge=0.0, le=50.0)
    auction_min_hold_minutes: int = Field(default=15, ge=0, le=60)
    auction_min_hold_high_conviction_minutes: int = Field(
        default=120,
        ge=0,
        le=360,
        description="Minimum hold for high-conviction positions before strategic auction closes",
    )
    auction_min_hold_high_conviction_threshold: float = Field(
        default=50.0,
        ge=0.0,
        le=100.0,
        description="Conviction threshold that activates the high-conviction minimum hold window",
    )
    auction_max_trades_per_cycle: int = Field(default=5, ge=1, le=20)
    auction_max_new_opens_per_cycle: int = Field(default=5, ge=1, le=20)
    auction_max_closes_per_cycle: int = Field(default=5, ge=1, le=20)
    # Capital reallocation after TP1/TP2 partial: auction sees freed margin on next tick.
    # Rate limit: 0 = no extra limit; when > 0, skip new opens for N seconds after partial close.
    auction_partial_close_cooldown_seconds: int = Field(default=0, ge=0, le=300)
    auction_entry_cost: float = Field(default=2.0, ge=0.0, le=10.0)
    auction_exit_cost: float = Field(default=2.0, ge=0.0, le=10.0)
    auction_direction_concentration_penalty: float = Field(default=10.0, ge=0.0, le=50.0, description="Score penalty at max directional imbalance (all positions same side)")
    auction_no_signal_persistence_enabled: bool = Field(
        default=False,
        description="When enabled, suppress strategic auction closes until no-signal streak reaches threshold",
    )
    auction_no_signal_close_persistence_cycles: int = Field(
        default=3,
        ge=1,
        le=50,
        description="Minimum consecutive no-signal cycles before strategic go-flat closes are allowed",
    )
    auction_no_signal_persistence_canary_symbols: List[str] = Field(
        default_factory=list,
        description="Optional symbol allowlist for no-signal persistence behavior (empty = all symbols)",
    )
    # Auction churn guard (DB-backed open->close->reopen loop protection)
    auction_churn_guard_enabled: bool = Field(
        default=False,
        description="Enable DB-backed churn cooldown guard for repeated fast round-trips per symbol",
    )
    auction_churn_window_hours: int = Field(default=6, ge=1, le=48)
    auction_churn_hold_max_minutes: int = Field(default=60, ge=5, le=240)
    auction_churn_reopen_max_minutes: int = Field(default=120, ge=5, le=360)
    auction_churn_max_events: int = Field(default=2, ge=1, le=10)
    auction_churn_cooldown_tier1_minutes: int = Field(default=30, ge=5, le=720)
    auction_churn_cooldown_tier2_minutes: int = Field(default=120, ge=5, le=1440)
    auction_churn_cooldown_tier3_minutes: int = Field(default=360, ge=5, le=2880)
    # Choppy-market reversal controls (telemetry-first rollout)
    auction_chop_guard_enabled: bool = Field(
        default=False,
        description="Enable CHOP/TREND-aware auction policy overrides",
    )
    auction_chop_telemetry_only: bool = Field(
        default=True,
        description="Log would-block decisions for chop controls without enforcement",
    )
    auction_chop_canary_symbols: List[str] = Field(
        default_factory=list,
        description="Optional symbol allowlist for chop enforcement (empty = all symbols)",
    )
    auction_chop_adx_threshold: float = Field(default=18.0, ge=5.0, le=50.0)
    auction_chop_score_std_threshold: float = Field(default=6.0, ge=0.1, le=50.0)
    auction_chop_reversal_window_hours: int = Field(default=12, ge=1, le=72)
    auction_chop_quick_reversal_hold_minutes: int = Field(default=60, ge=5, le=360)
    auction_chop_opposite_reentry_minutes: int = Field(default=120, ge=5, le=360)
    auction_chop_global_symbol_pct: float = Field(default=0.50, ge=0.10, le=1.0)
    auction_chop_swap_threshold_delta: float = Field(default=2.0, ge=0.0, le=20.0)
    auction_chop_min_hold_multiplier: float = Field(default=2.0, ge=1.0, le=5.0)
    auction_chop_max_new_opens_delta: int = Field(default=-1, ge=-10, le=10)
    auction_chop_no_signal_persistence_delta: int = Field(default=1, ge=0, le=10)
    auction_anti_flip_lock_enabled: bool = Field(
        default=False,
        description="Block strategic auction closes/reversals shortly after entry",
    )
    auction_anti_flip_lock_telemetry_only: bool = Field(
        default=True,
        description="Log anti-flip lock would-block counters without enforcement",
    )
    auction_anti_flip_lock_minutes: int = Field(default=45, ge=1, le=360)
    auction_anti_flip_canary_symbols: List[str] = Field(
        default_factory=list,
        description="Optional symbol allowlist for anti-flip lock enforcement (empty = all symbols)",
    )
    # Autonomous concentration rebalancer (auction path)
    auction_rebalancer_enabled: bool = Field(default=False, description="Enable autonomous concentration trims before opens")
    auction_rebalancer_shadow_mode: bool = Field(default=True, description="Log would-trim decisions without executing reductions")
    auction_rebalancer_trigger_pct_equity: float = Field(
        default=0.32, ge=0.10, le=0.80,
        description="Trim trigger when single-position concentration exceeds this % of equity"
    )
    auction_rebalancer_clear_pct_equity: float = Field(
        default=0.24, ge=0.05, le=0.70,
        description="Stop trimming once concentration is below this % of equity"
    )
    auction_rebalancer_per_symbol_trim_cooldown_cycles: int = Field(
        default=2, ge=0, le=50,
        description="Minimum cycles between trims for the same symbol"
    )
    auction_rebalancer_max_reductions_per_cycle: int = Field(
        default=1, ge=0, le=20,
        description="Maximum number of partial trims submitted per cycle"
    )
    auction_rebalancer_max_total_margin_reduced_per_cycle: float = Field(
        default=0.25, ge=0.0, le=1.0,
        description="Cap on aggregate estimated margin reduction per cycle as % of equity"
    )

    # Target margin utilisation band: when below target_min, boost notional (bounded) to deploy more capital.
    # Only applied when sizing_method is leverage_based (risk sanity: stop-distance-based sizing would violate risk-per-trade if we boosted).
    target_margin_util_min: float = Field(default=0.70, ge=0.50, le=0.90, description="Below this, utilisation boost may scale notional up (clamped to single/aggregate caps)")
    utilisation_boost_max_factor: float = Field(default=2.0, ge=1.0, le=3.0, description="Max factor to scale notional when under target_margin_util_min (e.g. 2.0 = double)")
    
    # Loss streak protection (time-based, not permanent block)
    loss_streak_cooldown: int = Field(default=3, ge=2, le=10)  # Trigger threshold
    loss_streak_pause_minutes: int = Field(default=240, ge=60, le=720)  # Pause duration (4h default)
    loss_streak_min_loss_bps: float = Field(default=20.0, ge=5.0, le=100.0)  # Only count losses > X bps
    
    # Basis guards
    basis_max_pct: float = Field(default=0.0075, ge=0.001, le=0.02)
    basis_max_post_pct: float = Field(default=0.0075, ge=0.001, le=0.02)
    
    # Fee & Funding Assumptions (configurable for accuracy)
    taker_fee_bps: float = Field(default=5.0, ge=1.0, le=10.0)
    maker_fee_bps: float = Field(default=2.0, ge=0.0, le=5.0)
    funding_rate_daily_bps: float = Field(default=10.0, ge=0.0, le=50.0)
    use_live_funding_rate: bool = Field(default=False)  # Future: fetch from API
    
    # Cost-aware validation
    max_fee_funding_rr_distortion_pct: float = Field(default=0.20, ge=0.05, le=0.30)
    rr_distortion_strict_limit_pct: float = Field(default=0.10, ge=0.05, le=0.30)
    tight_stop_threshold_pct: float = Field(default=0.015, ge=0.005, le=0.05)
    funding_cost_threshold_pct: float | None = Field(default=0.02, ge=0.0, le=0.10)
    # Deterministic edge-vs-cost gate (TP1 proxy)
    fee_edge_guard_enabled: bool = Field(default=False)
    fee_edge_multiple_k: float = Field(default=5.0, ge=1.0, le=20.0)
    fee_edge_use_conservative_taker: bool = Field(default=True)
    fee_edge_slippage_bps_est: float = Field(default=4.0, ge=0.0, le=50.0)
    fee_edge_funding_floor_bps: float = Field(default=2.0, ge=0.0, le=50.0)
    fee_edge_cost_buffer_multiplier: float = Field(default=1.2, ge=1.0, le=3.0)
    
    # Regime-specific settings (NEW for dual-regime strategy)
    # Tight-stop SMC regime (OB/FVG): 0.4-1.0% stops
    tight_smc_cost_cap_bps: float = Field(default=25.0, ge=10.0, le=50.0)  # Absolute cost cap
    tight_smc_min_rr_multiple: float = Field(default=2.0, ge=1.5, le=5.0)  # Min R:R required (reduced to 2.0)
    tight_smc_avg_hold_hours: float = Field(default=6.0, ge=1.0, le=24.0)  # For funding calc
    
    # Wide-stop structure regime (BOS/TREND): 1.5-3.0% stops
    wide_structure_max_distortion_pct: float = Field(default=0.15, ge=0.10, le=0.25)  # R:R distortion
    wide_structure_avg_hold_hours: float = Field(default=36.0, ge=12.0, le=72.0)  # For funding calc
    wide_structure_funding_hard_cap_bps: float = Field(
        default=15.0, ge=0.0, le=80.0, description="Hard reject wide-structure entries when projected funding burden exceeds this bps cap"
    )

    # Loss streak cooldown (Regime-Aware)
    loss_streak_cooldown_tight: int = Field(default=3, ge=2, le=10)
    loss_streak_cooldown_wide: int = Field(default=5, ge=2, le=10) # 4-5 losses
    loss_streak_pause_minutes_tight: int = Field(default=120, ge=30, le=300) # 120 minutes
    loss_streak_pause_minutes_wide: int = Field(default=90, ge=30, le=300) # 90 minutes
    
    # ShockGuard: Wick/Flash Move Protection
    shock_guard_enabled: bool = Field(default=True, description="Enable ShockGuard protection")
    shock_move_pct: float = Field(default=0.05, ge=0.01, le=0.10, description="1-minute move threshold (5.0%)")
    shock_range_pct: float = Field(default=0.04, ge=0.02, le=0.10, description="1-minute range threshold (4.0%)")
    basis_shock_pct: float = Field(default=0.015, ge=0.005, le=0.05, description="Basis divergence threshold (1.5%)")
    shock_cooldown_minutes: int = Field(default=30, ge=5, le=120, description="Cooldown after shock (minutes)")
    emergency_buffer_pct: float = Field(default=0.10, ge=0.05, le=0.20, description="Liquidation buffer for CLOSE (10%)")
    trim_buffer_pct: float = Field(default=0.18, ge=0.10, le=0.30, description="Liquidation buffer for TRIM (18%)")
    shock_marketwide_count: int = Field(default=3, ge=2, le=10, description="Symbols needed for market-wide shock")
    shock_marketwide_window_sec: int = Field(default=60, ge=30, le=300, description="Window for market-wide detection (seconds)")
    symbol_overrides: Dict[str, RiskSymbolOverride] = Field(default_factory=dict)


    @field_validator('max_leverage')
    @classmethod
    def validate_leverage(cls, v):
        if v > 10.0:
            raise ValueError("Leverage cap is 10× (hard limit, non-negotiable)")
        return v

    @model_validator(mode="after")
    def validate_rebalancer_hysteresis(self):
        if self.auction_rebalancer_clear_pct_equity >= self.auction_rebalancer_trigger_pct_equity:
            raise ValueError(
                "auction_rebalancer_clear_pct_equity must be below auction_rebalancer_trigger_pct_equity"
            )
        return self


class StrategyConfig(BaseSettings):
    """Strategy parameters configuration."""
    model_config = SettingsConfigDict(extra="ignore")

    # Timeframes - 4H DECISION AUTHORITY HIERARCHY
    # 1D: Regime filter only (EMA200 bias)
    # 4H: DECISION AUTHORITY - all SMC patterns (OB, FVG, BOS, ATR for stops)
    # 1H: Refinement only - ADX filter, swing point precision
    # 15m: Refinement only - entry timing
    regime_timeframes: List[str] = ["1d"]
    decision_timeframes: List[str] = ["4h"]
    refinement_timeframes: List[str] = ["1h", "15m"]
    # Legacy compatibility (deprecated)
    bias_timeframes: List[str] = ["4h", "1d"]
    execution_timeframes: List[str] = ["15m", "1h"]
    
    # Indicators
    ema_period: int = Field(default=200, ge=50, le=300)
    adx_period: int = Field(default=14, ge=7, le=30)
    adx_threshold: float = Field(default=20.0, ge=10.0, le=40.0)
    atr_period: int = Field(default=14, ge=7, le=30)
    
    # Stop buffering (Regime specific ranges - adjusted for 4H ATR)
    # 4H ATR is ~2-3x larger than 1H ATR, so multipliers are reduced
    # tight_smc: 0.15-0.30 ATR (4H) - was 0.3-0.6 on 1H
    # wide_structure: 0.50-0.60 ATR (4H) - was 1.0-1.2 on 1H
    tight_smc_atr_stop_min: float = Field(default=0.15, ge=0.05, le=1.0)
    tight_smc_atr_stop_max: float = Field(default=0.30, ge=0.05, le=1.0)
    wide_structure_atr_stop_min: float = Field(default=0.50, ge=0.2, le=2.0)
    wide_structure_atr_stop_max: float = Field(default=0.60, ge=0.2, le=2.0)
    
    # Legacy fallbacks
    atr_multiplier_stop: float = Field(default=1.5, ge=1.0, le=3.0)
    
    # Stop widening after repeated stop-outs
    stop_widen_enabled: bool = Field(default=True, description="Widen stops after repeated stop-outs")
    stop_widen_lookback_hours: int = Field(default=24, ge=6, le=72, description="Hours to look back for stop-outs")
    stop_widen_threshold: int = Field(default=2, ge=1, le=5, description="Number of stop-outs before widening")
    stop_widen_factor: float = Field(default=1.5, ge=1.1, le=2.5, description="Multiplier for stop distance after threshold")
    stop_widen_max_factor: float = Field(default=2.0, ge=1.5, le=3.0, description="Maximum widening factor")
    stop_widen_increment: float = Field(default=0.25, ge=0.1, le=0.5, description="Additional factor per stop-out above threshold")
    
    # Symbol-level loss tracking and cooldown
    symbol_loss_cooldown_enabled: bool = Field(default=True, description="Pause trading on symbols with repeated losses")
    symbol_loss_lookback_hours: int = Field(default=24, ge=6, le=72, description="Hours to look back for losses")
    symbol_loss_threshold: int = Field(default=3, ge=2, le=10, description="Consecutive losses before cooldown")
    symbol_loss_cooldown_hours: int = Field(default=12, ge=4, le=48, description="Hours to pause trading after threshold")
    symbol_loss_min_pnl_pct: float = Field(default=-0.5, ge=-5.0, le=0.0, description="Min loss % to count as a loss (-0.5 = -0.5%)")
    # Canary overrides for symbol-loss cooldown (disabled by default; scoped by symbol allowlist).
    symbol_loss_cooldown_canary_enabled: bool = Field(default=False)
    symbol_loss_cooldown_canary_symbols: List[str] = Field(default_factory=list)
    symbol_loss_cooldown_canary_lookback_hours: Optional[int] = Field(default=None, ge=6, le=72)
    symbol_loss_cooldown_canary_threshold: Optional[int] = Field(default=None, ge=2, le=10)
    symbol_loss_cooldown_canary_hours: Optional[int] = Field(default=None, ge=1, le=48)
    symbol_loss_cooldown_canary_min_pnl_pct: Optional[float] = Field(default=None, ge=-5.0, le=0.0)
    
    rsi_period: int = Field(default=14, ge=7, le=30)

    rsi_divergence_enabled: bool = False  # Single flag for RSI divergence (removed duplicate rsi_divergence_check)
    
    # SMC Parameters
    orderblock_lookback: int = Field(default=50, ge=20, le=200)
    ob_entry_mode: Literal["high_low", "mid", "open", "discount"] = Field(default="mid")
    ob_discount_pct: float = Field(default=0.25, ge=0.1, le=0.5)
    fvg_min_size_pct: float = Field(default=0.001, ge=0.0001, le=0.01)
    # Canary-scoped override for FVG minimum size threshold.
    # When enabled, applies fvg_min_size_pct_canary to symbols in fvg_min_size_pct_canary_symbols.
    # If symbol list is empty, override applies to all symbols.
    fvg_min_size_pct_canary_enabled: bool = Field(default=False)
    fvg_min_size_pct_canary_symbols: List[str] = Field(default_factory=list)
    fvg_min_size_pct_canary: Optional[float] = Field(default=None, ge=0.0001, le=0.01)
    # Signal cooldown gate to prevent repeated re-signals from the same symbol.
    signal_cooldown_hours: float = Field(default=4.0, ge=0.0, le=24.0)
    signal_cooldown_canary_enabled: bool = Field(default=False)
    signal_cooldown_canary_symbols: List[str] = Field(default_factory=list)
    signal_cooldown_hours_canary: Optional[float] = Field(default=None, ge=0.0, le=24.0)
    signal_post_close_cooldown_enabled: bool = Field(default=True)
    signal_post_close_lookback_hours: int = Field(default=24, ge=1, le=168)
    signal_post_close_cooldown_win_minutes: int = Field(default=30, ge=0, le=720)
    signal_post_close_cooldown_loss_minutes: int = Field(default=120, ge=0, le=1440)
    signal_post_close_cooldown_strategic_minutes: int = Field(default=120, ge=0, le=1440)
    signal_structure_dedupe_enabled: bool = Field(default=True)
    signal_structure_dedupe_minutes: int = Field(default=45, ge=0, le=240)
    bos_confirmation_candles: int = Field(default=3, ge=1, le=10)
    require_bos_confirmation: bool = Field(default=False)  # Optional filter for higher quality
    bos_volume_confirmation_enabled: bool = Field(
        default=False,
        description="Require break candle volume > threshold × avg volume for BOS",
    )
    bos_volume_threshold_mult: float = Field(
        default=1.5, ge=1.0, le=5.0,
        description="Break candle volume must exceed this multiple of 20-period avg volume",
    )
    fvg_mitigation_mode: Literal["touched", "partial", "full"] = "touched"
    fvg_partial_fill_pct: float = Field(default=0.5, ge=0.0, le=1.0)

    # Bias Logic
    ema_neutral_zone_bps: float = Field(default=10.0, ge=0.0, le=100.0)
    
    # Scoring Gates
    min_score_tight_smc_aligned: float = Field(default=75.0, ge=0.0, le=100.0)
    min_score_tight_smc_neutral: float = Field(default=80.0, ge=0.0, le=100.0)
    min_score_wide_structure_aligned: float = Field(default=70.0, ge=0.0, le=100.0)
    min_score_wide_structure_neutral: float = Field(default=75.0, ge=0.0, le=100.0)
    
    # Fib Enforcement
    fib_proximity_bps: float = Field(default=20.0, ge=0.0, le=100.0) # 0.2%
    fib_proximity_adaptive_enabled: bool = Field(
        default=False,
        description="Scale fib proximity tolerance with ATR ratio (wider in high vol)",
    )
    fib_proximity_adaptive_scale: float = Field(
        default=0.5, ge=0.0, le=2.0,
        description="Scale factor: effective_bps = base × (1 + max(0, atr_ratio-1) × scale)",
    )
    fib_proximity_max_bps: float = Field(
        default=50.0, ge=10.0, le=200.0,
        description="Hard cap on adaptive fib proximity (bps)",
    )
    
    # Higher-timeframe pyramid gate (1W -> 1D -> 4H)
    # Starts SOFT (score penalty only) so we can roll out safely.
    higher_tf_enabled: bool = Field(default=False, description="Enable weekly + daily context gate")
    higher_tf_mode: Literal["soft", "hard"] = Field(
        default="soft",
        description="soft=apply score penalty outside weekly zone, hard=reject signal outside weekly zone",
    )
    weekly_fib_confluence_weight: float = Field(default=0.25, ge=0.0, le=1.0)
    daily_bias_weight: float = Field(default=0.15, ge=0.0, le=1.0)
    min_weekly_zone_width_pct: float = Field(default=1.5, ge=0.1, le=20.0)
    higher_tf_penalty_outside_zone: float = Field(default=-18.0)

    # EMA slope scoring bonus
    ema_slope_bonus: float = Field(
        default=0.0, ge=0.0, le=15.0,
        description="Bonus points when EMA200 slope aligns with signal direction (0=disabled)",
    )

    # Persistent institutional memory (thesis + conviction decay)
    memory_enabled: bool = Field(default=False, description="Enable thesis memory tracking")
    thesis_observe_only: bool = Field(default=True, description="Observe-only mode (no behavior changes)")
    thesis_score_enabled: bool = Field(default=False, description="Apply conviction score adjustment")
    thesis_management_enabled: bool = Field(default=False, description="Apply conviction-driven position management")
    thesis_canary_symbols: List[str] = Field(default_factory=list, description="Optional symbol allowlist for thesis behaviors")
    thesis_time_decay_max_points: float = Field(default=45.0, ge=0.0, le=100.0)
    thesis_time_decay_window_hours: float = Field(default=12.0, ge=1.0, le=168.0)
    thesis_zone_breach_penalty_points: float = Field(default=35.0, ge=0.0, le=100.0)
    thesis_volume_fade_penalty_points: float = Field(default=15.0, ge=0.0, le=100.0)
    thesis_conviction_floor: float = Field(default=5.0, ge=0.0, le=50.0)
    thesis_score_neutral_conviction: float = Field(default=60.0, ge=1.0, le=99.0)
    thesis_score_max_bonus: float = Field(default=8.0, ge=0.0, le=30.0)
    thesis_score_max_penalty: float = Field(default=12.0, ge=0.0, le=30.0)
    conviction_min_for_entry: float = Field(
        default=35.0,
        ge=1.0,
        le=99.0,
        description="Minimum thesis conviction required to allow new entry when thesis management is enabled",
    )
    conviction_min_for_entry_canary: Optional[float] = Field(
        default=None,
        ge=1.0,
        le=99.0,
        description="Optional lower conviction entry threshold for canary symbols",
    )
    conviction_canary_symbols: List[str] = Field(
        default_factory=list,
        description="Optional symbol allowlist for conviction_min_for_entry_canary override",
    )
    conviction_stop_sizing_enabled: bool = Field(
        default=False,
        description="Enable conviction-aware stop widening for high-conviction entries",
    )
    conviction_stop_sizing_canary_symbols: List[str] = Field(
        default_factory=list,
        description="Optional symbol allowlist for conviction-aware stop sizing (empty = all symbols)",
    )
    conviction_stop_high_threshold: float = Field(
        default=60.0,
        ge=1.0,
        le=99.0,
        description="Conviction threshold for applying high-conviction stop multiplier",
    )
    conviction_stop_high_multiplier: float = Field(
        default=1.5,
        ge=1.0,
        le=3.0,
        description="Multiplier applied to stop distance when conviction is above high threshold",
    )
    ev_layer_enabled: bool = Field(default=False, description="Enable EV/Bayesian layer")
    ev_shadow_mode: bool = Field(
        default=True,
        description="Shadow-only EV mode; computes and logs without changing behavior",
    )
    ev_canary_symbols: List[str] = Field(
        default_factory=list,
        description="Optional symbol allowlist for EV layer (empty = all symbols)",
    )
    bayes_enabled: bool = Field(default=True, description="Enable Bayesian posterior computation in EV layer")
    bayes_prior_blend: float = Field(default=0.5, ge=0.0, le=1.0)
    bayes_decay_hours: float = Field(default=72.0, ge=1.0, le=720.0)
    thesis_early_exit_threshold: float = Field(default=35.0, ge=1.0, le=99.0)
    thesis_reentry_block_threshold: float = Field(default=25.0, ge=1.0, le=99.0)
    thesis_alerts_enabled: bool = Field(default=False, description="Enable thesis-only Telegram alerts")
    thesis_alert_open_positions_only: bool = Field(
        default=True,
        description="Only send thesis state alerts for symbols with active open positions",
    )
    thesis_low_conviction_tighten_factor: float = Field(default=0.75, ge=0.25, le=1.0)
    thesis_high_conviction_relax_factor: float = Field(default=1.1, ge=1.0, le=2.0)
    thesis_partial_reduce_high_conviction_factor: float = Field(default=0.85, ge=0.5, le=1.0)
    thesis_partial_reduce_low_conviction_factor: float = Field(default=1.25, ge=1.0, le=2.0)


    # Market Structure Change Confirmation (4H Decision Authority)
    # Now uses 4H candles - 1 candle = 4 hours, 2 candles = 8 hours
    require_ms_change_confirmation: bool = Field(default=True)
    ms_confirmation_candles: int = Field(default=1, ge=1, le=5)  # Base: 1 on 4H = 4 hours
    ms_confirmation_candles_high_vol: int = Field(default=2, ge=1, le=5)  # High vol: 2 on 4H = 8 hours
    ms_reconfirmation_candles: int = Field(default=1, ge=1, le=5)  # 1 on 4H = 4 hours
    
    # Adaptive Strategy Logic
    adaptive_enabled: bool = True
    atr_confirmation_threshold_high: float = Field(default=1.5, ge=1.0) # > 1.5x avg ATR -> High Vol
    atr_confirmation_threshold_low: float = Field(default=0.5, le=1.0) # < 0.5x avg ATR -> Low Vol
    base_confirmation_candles: int = 3
    max_confirmation_candles: int = 5
    min_confirmation_candles: int = 2
    
    # RSI Divergence (using rsi_divergence_enabled above, removed duplicate rsi_divergence_check)
    rsi_divergence_lookback: int = 20

    # Exits
    abandon_ship_enabled: bool = True
    time_based_exit_bars: int = Field(default=20, ge=5) # Bars to hold without TP before exit

    # Entry Zone Tolerance (for relaxed reconfirmation)
    # Allows entries when price is "near" OB/FVG zone, not strictly inside
    entry_zone_tolerance_pct: float = Field(default=0.015, ge=0.005, le=0.05)  # 1.5% buffer
    entry_zone_tolerance_adaptive: bool = Field(default=True)  # Scale with ATR
    entry_zone_tolerance_atr_mult: float = Field(default=0.3, ge=0.1, le=1.0)  # ATR multiplier
    entry_zone_tolerance_score_penalty: int = Field(default=-5, ge=-15, le=0)  # Score adjustment for tolerance entries
    
    # Skip Reconfirmation in Trending Markets
    # When True, enters immediately after MSS confirmation without waiting for retrace
    skip_reconfirmation_in_trends: bool = Field(default=True)  # Default True for trending markets
    symbol_overrides: Dict[str, StrategySymbolOverride] = Field(default_factory=dict)




class AssetConfig(BaseSettings):
    """Asset selection and filtering configuration."""
    model_config = SettingsConfigDict(extra="ignore")

    mode: Literal["auto", "whitelist", "blacklist"] = Field(default="auto")
    whitelist: List[str] = Field(default_factory=list)  # e.g., ["BTC/USD", "ETH/USD"]
    blacklist: List[str] = Field(default_factory=list)  # e.g., ["DOGE/USD"]
    
    @field_validator('mode')
    @classmethod
    def validate_mode(cls, v):
        if v not in ["auto", "whitelist", "blacklist"]:
            raise ValueError(f"Invalid mode: {v}. Must be auto, whitelist, or blacklist")
        return v


class CoinUniverseConfig(BaseSettings):
    """
    Coin universe configuration (V3 - Single Source of Truth).
    
    IMPORTANT: liquidity_tiers is DEPRECATED. Use candidate_symbols instead.
    The tiers in liquidity_tiers are for UNIVERSE SELECTION only - actual tier
    classification is done dynamically by MarketRegistry based on futures metrics.
    """
    model_config = SettingsConfigDict(extra="ignore")

    enabled: bool = True
    min_spot_volume_24h: Decimal = Field(default=Decimal("5000000"))
    
    # NEW: Preferred field - flat list of candidate symbols
    candidate_symbols: Optional[List[str]] = Field(default=None)
    
    # DEPRECATED: Keep for backward compatibility (one release)
    # These are CANDIDATES for discovery, NOT tier assignments
    liquidity_tiers: Optional[Dict[str, List[str]]] = Field(
        default_factory=lambda: {"A": ["BTC/USD"], "B": [], "C": []}
    )
    
    tier_max_leverage: Dict[str, float] = Field(
        default_factory=lambda: {"A": 10.0, "B": 5.0, "C": 2.0}
    )  # Global cap still applies
    
    @model_validator(mode='after')
    def normalize_candidates(self) -> 'CoinUniverseConfig':
        """Normalize liquidity_tiers to candidate_symbols with deprecation warning."""
        if self.liquidity_tiers and not self.candidate_symbols:
            import warnings
            import logging
            # Log deprecation warning
            logger = logging.getLogger(__name__)
            logger.warning(
                "DEPRECATION: coin_universe.liquidity_tiers is deprecated. "
                "Tiers are now assigned dynamically by MarketRegistry. "
                "These are treated as candidate_symbols for universe selection only."
            )
            warnings.warn(
                "coin_universe.liquidity_tiers is deprecated. "
                "Use coin_universe.candidate_symbols instead. "
                "Tiers are assigned dynamically by MarketRegistry.",
                DeprecationWarning,
                stacklevel=2
            )
        return self
    
    def get_all_candidates(self) -> List[str]:
        """
        Get all candidate symbols for universe discovery.
        
        This is the ONLY method consumers should use to get symbols.
        Returns symbols from candidate_symbols if set, otherwise flattens liquidity_tiers.
        """
        if self.candidate_symbols:
            return list(self.candidate_symbols)
        if self.liquidity_tiers:
            flattened = []
            for tier_list in self.liquidity_tiers.values():
                if tier_list:
                    flattened.extend(tier_list)
            return list(set(flattened))
        return []

class TierConfig(BaseSettings):
    """Per-tier risk limits for position sizing."""
    model_config = SettingsConfigDict(extra="ignore")

    max_leverage: float = Field(default=10.0)  # Maximum leverage for this tier
    max_position_size_usd: Decimal = Field(default=Decimal("100000"))  # Maximum position size in USD
    slippage_cap_pct: Decimal = Field(default=Decimal("0.001"))  # Maximum expected slippage
    allow_live_trading: bool = Field(default=True)  # Whether live trading is allowed for this tier


def _default_tier_configs() -> Dict[str, TierConfig]:
    """Default tier configurations with conservative limits for lower tiers."""
    return {
        "A": TierConfig(
            max_leverage=10.0,
            max_position_size_usd=Decimal("100000"),
            slippage_cap_pct=Decimal("0.001"),
            allow_live_trading=True,
        ),
        "B": TierConfig(
            max_leverage=5.0,
            max_position_size_usd=Decimal("50000"),
            slippage_cap_pct=Decimal("0.002"),
            allow_live_trading=True,
        ),
        "C": TierConfig(
            max_leverage=2.0,
            max_position_size_usd=Decimal("25000"),
            slippage_cap_pct=Decimal("0.003"),
            allow_live_trading=True,
        ),
    }


class LiquidityFilters(BaseSettings):
    """Market eligibility filters with tier-based risk limits."""
    model_config = SettingsConfigDict(extra="ignore")

    # Spot filters
    min_spot_volume_usd_24h: Decimal = Field(default=Decimal("1000000"))  # $1M minimum (relaxed from $5M)
    max_spread_pct: Decimal = Field(default=Decimal("0.0020"))  # 0.20% spot spread (relaxed from 0.05%)
    min_price_usd: Decimal = Field(default=Decimal("0.01"))  # Avoid dust coins
    
    # Futures-specific filters
    min_futures_open_interest: Decimal = Field(default=Decimal("500000"))  # $500k OI minimum
    max_futures_spread_pct: Decimal = Field(default=Decimal("0.0030"))  # 0.30% perp spread
    min_futures_volume_usd_24h: Decimal = Field(default=Decimal("500000"))  # $500k futures volume
    max_funding_rate_abs: Optional[Decimal] = Field(default=Decimal("0.001"))  # 0.1% funding cap
    
    # Filter mode: "spot_and_futures" (both must pass), "futures_primary" (futures required, spot optional)
    filter_mode: str = Field(default="futures_primary")
    
    # Tier-specific risk limits (A=high liquidity, B=medium, C=low)
    tier_configs: Dict[str, TierConfig] = Field(default_factory=_default_tier_configs)
    
    def get_tier_config(self, tier: str) -> TierConfig:
        """Get config for a tier, defaulting to tier C (most conservative) if not found."""
        return self.tier_configs.get(tier, self.tier_configs.get("C", TierConfig()))


class MultiTPConfig(BaseSettings):
    """Multi-TP configuration (YAML multi_tp section). When enabled, overrides execution TP splits and RR multiples."""
    model_config = SettingsConfigDict(extra="ignore")

    enabled: bool = False
    tp1_r_multiple: float = Field(default=1.0, ge=0.5, le=5.0)
    tp1_close_pct: float = Field(default=0.40, ge=0.1, le=0.6)
    tp2_r_multiple: float = Field(default=2.5, ge=1.0, le=10.0)
    tp2_close_pct: float = Field(default=0.40, ge=0.1, le=0.6)
    runner_pct: float = Field(default=0.20, ge=0.05, le=0.5)
    move_sl_to_be_after_tp1: bool = True
    trailing_stop_enabled: bool = True
    trailing_stop_atr_multiplier: float = Field(default=1.5, ge=1.0, le=3.0)
    # Guard for activating trailing at TP1: require ATR >= this (0 = no minimum)
    trailing_activation_atr_min: float = Field(default=0.0, ge=0.0, le=10.0)

    # Runner behavior: when False, runner has NO fixed TP order (trend-following mode)
    runner_has_fixed_tp: bool = False
    # R-multiple for runner TP; only used when runner_has_fixed_tp is True
    runner_tp_r_multiple: Optional[float] = Field(default=None, ge=1.0, le=20.0)
    # What happens when price hits final target level:
    #   tighten_trail = tighten trailing stop (default, best for trend-following)
    #   close_partial  = close ~50% of remaining runner
    #   close_full     = legacy full exit
    final_target_behavior: Literal["tighten_trail", "close_partial", "close_full"] = "tighten_trail"
    # ATR multiplier to use when tightening trail at final target
    tighten_trail_at_final_target_atr_mult: float = Field(default=1.2, ge=0.5, le=3.0)
    
    # Progressive trailing tightening at R-multiple milestones
    # Each level: (r_multiple_threshold, atr_multiplier_to_use)
    # When price reaches N*R profit, trail tightens to the specified ATR mult
    progressive_trail_enabled: bool = True
    progressive_trail_levels: list[dict] = Field(
        default=[
            {"r_threshold": 3.0, "atr_mult": 1.8},   # At 3R: moderate tighten
            {"r_threshold": 5.0, "atr_mult": 1.4},   # At 5R: tighter
            {"r_threshold": 8.0, "atr_mult": 1.0},   # At 8R: very tight (1x ATR)
        ]
    )
    
    # Regime-aware runner sizing overrides
    # When regime is "tight_smc" (consolidation/range), use smaller runner
    # When regime is "wide_structure" (trending), use larger runner
    regime_runner_sizing_enabled: bool = True
    regime_runner_overrides: dict = Field(
        default={
            "tight_smc": {"runner_pct": 0.10, "tp1_close_pct": 0.50, "tp2_close_pct": 0.40},
            "wide_structure": {"runner_pct": 0.30, "tp1_close_pct": 0.35, "tp2_close_pct": 0.35},
            "consolidation": {"runner_pct": 0.10, "tp1_close_pct": 0.50, "tp2_close_pct": 0.40},
        }
    )
    # Hybrid exit mode: regime-specific choice of runner vs fixed TP3.
    # When disabled, runner_has_fixed_tp remains the global behavior.
    hybrid_exit_mode_enabled: bool = Field(default=False)
    hybrid_exit_canary_symbols: List[str] = Field(
        default_factory=list,
        description="Optional allowlist of symbols where hybrid exit mode is applied (empty = all symbols)",
    )
    hybrid_exit_regime_mode_overrides: Dict[str, Literal["runner", "fixed_tp3"]] = Field(
        default_factory=lambda: {
            "tight_smc": "fixed_tp3",
            "wide_structure": "runner",
            "consolidation": "fixed_tp3",
        }
    )
    hybrid_exit_unknown_regime_fallback_mode: Literal["global_default", "runner", "fixed_tp3"] = "global_default"


class ExecutionConfig(BaseSettings):
    """Execution settings configuration."""
    model_config = SettingsConfigDict(extra="ignore")

    # Price conversion
    use_mark_price: bool = True
    
    # Order Structure
    default_order_type: Literal["market", "limit"] = "limit"
    slippage_tolerance_pct: float = Field(default=0.001, ge=0.0001, le=0.01)
    
    # Take Profit Settings
    tp_mode: Literal["structure_with_rr_fallback"] = "structure_with_rr_fallback"
    tp_splits: List[float] = [0.35, 0.35, 0.30]
    rr_fallback_multiples: List[float] = [1.0, 2.0, 3.0]
    
    # Dynamic Management
    break_even_trigger: Literal["tp1_fill"] = "tp1_fill"
    
    # TP Backfill / Reconciliation
    tp_backfill_enabled: bool = Field(default=True, description="Enable TP backfill reconciliation")
    tp_backfill_cooldown_minutes: int = Field(default=10, ge=1, le=60, description="Cooldown between backfill attempts per symbol")
    tp_price_tolerance: float = Field(default=0.002, ge=0.0001, le=0.01, description="TP price tolerance (0.2% default)")
    min_tp_distance_pct: float = Field(default=0.003, ge=0.001, le=0.01, description="Minimum TP distance from current price (0.3% default)")
    max_tp_distance_pct: Optional[float] = Field(default=None, ge=0.01, le=0.50, description="Maximum TP distance clamp (optional)")
    min_tp_orders_expected: int = Field(default=2, ge=1, le=5, description="Minimum expected TP orders in ladder")
    min_hold_seconds: int = Field(default=30, ge=0, le=300, description="Minimum hold time before backfill (avoid racing fills)")
    require_sl_for_tp_backfill: bool = Field(default=True, description="Require stop loss price before backfilling TPs (safety guard)")
    break_even_offset_ticks: int = 2
    
    trailing_enabled: bool = True
    trailing_trigger: Literal["tp1_fill"] = "tp1_fill"
    trailing_type: Literal["atr"] = "atr"
    trailing_atr_period: int = 14
    trailing_atr_mult: float = 2.0
    trailing_update_min_ticks: int = 2
    trail_tighten_after_tp2: bool = False
    trail_atr_mult_after_tp2: float = 1.6
    order_timeout_seconds: int = Field(default=120, ge=10, le=300)
    order_price_invalidation_pct: float = Field(default=0.03, ge=0.01, le=0.10)  # Cancel if price moves X% away
    max_retries: int = Field(default=3, ge=1, le=10)
    retry_backoff_seconds: int = Field(default=2, ge=1, le=10)

    # Pyramiding
    pyramiding_enabled: bool = False  # Default: no adding to positions

    # Hard entry blocks (defense-in-depth)
    # These block NEW entries only (they do not block reduce-only exits / risk management).
    entry_blocklist_spot_symbols: List[str] = Field(
        default_factory=list,
        description="Do not open NEW positions for these spot symbols (case-insensitive), e.g. ['USDT/USD']",
    )
    entry_blocklist_bases: List[str] = Field(
        default_factory=list,
        description="Do not open NEW positions for these base assets (case-insensitive), e.g. ['USDT']",
    )


class DataSanityConfig(BaseSettings):
    """Per-symbol data sanity gate configuration.

    Stage A (pre-I/O): futures spread + volume.
    Stage B (post-I/O): candle count + freshness.
    Also controls the DataQualityTracker state machine thresholds.
    """
    model_config = SettingsConfigDict(extra="ignore")

    max_spread_pct: float = Field(default=0.10, description="Max futures spread (10%)")
    min_volume_24h_usd: float = Field(default=10_000, description="Min 24h futures volume USD")
    min_decision_tf_candles: int = Field(default=250, description="Min candles on decision TF")
    decision_tf: str = Field(default="4h", description="Decision timeframe for candle checks")
    allow_spot_fallback: bool = Field(default=False, description="Fall back to spot ticker when futures missing")
    degraded_after_failures: int = Field(default=3, ge=1, le=20)
    suspend_after_hours: int = Field(default=6, ge=1, le=168)
    release_after_successes: int = Field(default=3, ge=1, le=20)
    probe_interval_minutes: int = Field(default=30, ge=5, le=1440)
    log_cooldown_seconds: int = Field(default=1800, ge=60, le=86400)
    degraded_skip_ratio: int = Field(default=4, ge=2, le=20)


class DataConfig(BaseSettings):
    """Data acquisition configuration."""
    model_config = SettingsConfigDict(extra="ignore")

    # WebSocket settings
    ws_reconnect_max_retries: int = Field(default=10, ge=3, le=50)
    ws_reconnect_backoff_seconds: int = Field(default=5, ge=1, le=30)
    
    # Data validation
    max_gap_seconds: int = Field(default=60, ge=10, le=300)
    
    # OHLCV resilience
    ohlcv_max_retries: int = Field(default=3, ge=1, le=10)
    ohlcv_failure_disable_after: int = Field(default=3, ge=1, le=20, description="Consecutive failures before symbol cooldown")
    ohlcv_symbol_cooldown_minutes: int = Field(default=60, ge=5, le=480)
    max_concurrent_ohlcv: int = Field(default=8, ge=1, le=20)
    ohlcv_min_delay_ms: int = Field(default=200, ge=50, le=1000)
    allow_futures_ohlcv_fallback: bool = Field(default=True, description="Use futures OHLCV when spot fails")
    min_healthy_coins: int = Field(default=30, ge=1, le=500, description="Min coins with sufficient candles to allow new entries")
    min_health_ratio: float = Field(default=0.25, ge=0.05, le=1.0, description="Min ratio sufficient/total to allow new entries")
    
    # Data sanity gate
    data_sanity: DataSanityConfig = Field(default_factory=DataSanityConfig)
    
    # Storage
    # database_url can be None in DigitalOcean if RUN_TIME secrets aren't immediately available
    database_url: Optional[str] = None


class ReconciliationConfig(BaseSettings):
    """Reconciliation configuration."""
    model_config = SettingsConfigDict(extra="ignore")

    reconcile_enabled: bool = Field(default=True, description="Run position reconciliation at startup and periodically")
    periodic_interval_seconds: int = Field(default=120, ge=5, le=600, description="Reconcile every N seconds (default 2 min)")
    unmanaged_position_policy: Literal["adopt", "force_close"] = Field(
        default="adopt",
        description="Adopt exchange positions into tracking, or force-close them",
    )
    unmanaged_position_adopt_place_protection: bool = Field(
        default=True,
        description="When adopting, attempt to place SL/TP protective orders",
    )


class MonitoringConfig(BaseSettings):
    """Monitoring and alerting configuration."""
    model_config = SettingsConfigDict(extra="ignore")

    # Logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_format: Literal["json", "text"] = "json"
    
    # Alerts
    alert_margin_usage_threshold_pct: float = Field(default=0.70, ge=0.50, le=0.90)
    alert_liquidation_buffer_threshold_pct: float = Field(default=0.35, ge=0.30, le=0.50)
    alert_repeated_rejections_count: int = Field(default=3, ge=2, le=10)
    alert_repeated_rejections_window_seconds: int = Field(default=300, ge=60, le=600)
    
    # Alert delivery
    alert_methods: List[str] = ["log"]
    slack_webhook_url: Optional[str] = None
    discord_webhook_url: Optional[str] = None


class BacktestConfig(BaseSettings):
    """Backtesting configuration."""
    model_config = SettingsConfigDict(extra="ignore")

    # Starting capital
    starting_equity: float = Field(default=10000.0, ge=1000.0, le=1000000.0)
    
    # Fill assumptions
    assume_maker_fills: bool = True
    slippage_bps: float = Field(default=2.0, ge=0.0, le=10.0)
    
    # Cost modeling
    maker_fee_bps: float = Field(default=2.0, ge=0.0, le=10.0)
    taker_fee_bps: float = Field(default=5.0, ge=0.0, le=20.0)
    
    # Basis modeling
    basis_model: Literal["static", "stochastic"] = "static"
    static_basis_bps: float = Field(default=5.0, ge=0.0, le=50.0)


class PaperConfig(BaseSettings):
    """Paper trading configuration."""
    model_config = SettingsConfigDict(extra="ignore")

    simulate_realistic_slippage: bool = True
    simulate_fill_delays_ms: int = Field(default=100, ge=0, le=1000)


class LiveConfig(BaseSettings):
    """Live trading configuration and safety gates."""
    model_config = SettingsConfigDict(extra="ignore")

    require_paper_success: bool = True
    min_paper_days: int = Field(default=30, ge=7, le=90)
    min_paper_trades: int = Field(default=50, ge=10, le=200)
    max_paper_drawdown_pct: float = Field(default=0.15, ge=0.10, le=0.30)


class SystemConfig(BaseSettings):
    """System metadata."""
    model_config = SettingsConfigDict(extra="ignore")

    name: str = "Trading System"
    version: str = "3.0.0"
    dry_run: bool = False  # If True, no real orders are placed


class SpotDCAConfig(BaseSettings):
    """Spot DCA (Dollar Cost Averaging) configuration.
    
    When enabled, the system uses the available USD balance on the spot account
    to purchase the configured asset at the scheduled time each day.
    """
    model_config = SettingsConfigDict(extra="ignore")

    enabled: bool = False
    asset: str = "SOL"                    # Base asset to purchase (e.g. "SOL", "BTC", "ETH")
    quote_currency: str = "USD"           # Quote currency to spend
    schedule_hour_utc: int = Field(default=0, ge=0, le=23)   # Hour (UTC) to execute
    schedule_minute_utc: int = Field(default=0, ge=0, le=59)  # Minute (UTC)
    use_full_balance: bool = True         # If True, spend all available quote balance
    fixed_amount_usd: Optional[float] = Field(default=None, ge=1.0)  # Fixed USD amount (overrides use_full_balance)
    min_purchase_usd: float = Field(default=5.0, ge=1.0)  # Skip if available < this
    max_purchase_usd: Optional[float] = Field(default=None, ge=1.0)  # Cap single purchase (None = no cap)
    reserve_usd: float = Field(default=0.0, ge=0.0)  # Keep this much USD in reserve, don't spend it


def _merge_live_research_overrides(config_dict: dict, yaml_path: Path) -> None:
    """Merge live_research_overrides.yaml into config_dict (strategy/risk symbol_overrides, optional assets)."""
    import os
    overrides_path = os.environ.get("RESEARCH_LIVE_OVERRIDES_PATH")
    if not overrides_path:
        overrides_path = Path(yaml_path).parent / "live_research_overrides.yaml"
    else:
        overrides_path = Path(overrides_path)
    if not overrides_path.exists():
        return
    with open(overrides_path, "r") as f:
        overrides = yaml.safe_load(f) or {}
    if overrides.get("strategy", {}).get("symbol_overrides"):
        if "strategy" not in config_dict:
            config_dict["strategy"] = {}
        existing = config_dict["strategy"].get("symbol_overrides") or {}
        config_dict["strategy"]["symbol_overrides"] = {**existing, **overrides["strategy"]["symbol_overrides"]}
    if overrides.get("risk", {}).get("symbol_overrides"):
        if "risk" not in config_dict:
            config_dict["risk"] = {}
        existing = config_dict["risk"].get("symbol_overrides") or {}
        config_dict["risk"]["symbol_overrides"] = {**existing, **overrides["risk"]["symbol_overrides"]}
    if overrides.get("assets"):
        if "assets" not in config_dict:
            config_dict["assets"] = {}
        for key in ("mode", "whitelist"):
            if key in overrides["assets"]:
                config_dict["assets"][key] = overrides["assets"][key]


class Config(BaseSettings):
    """Main configuration class."""
    model_config = SettingsConfigDict(
        env_nested_delimiter="__",
        extra="ignore",
    )
    
    system: SystemConfig = Field(default_factory=SystemConfig)
    exchange: ExchangeConfig
    risk: RiskConfig
    strategy: StrategyConfig
    assets: AssetConfig = Field(default_factory=AssetConfig)  # NEW
    coin_universe: CoinUniverseConfig = Field(default_factory=CoinUniverseConfig) # NEW
    liquidity_filters: LiquidityFilters = Field(default_factory=LiquidityFilters)  # NEW
    execution: ExecutionConfig
    multi_tp: Optional[MultiTPConfig] = None
    spot_dca: SpotDCAConfig = Field(default_factory=SpotDCAConfig)
    data: DataConfig
    reconciliation: ReconciliationConfig
    monitoring: MonitoringConfig
    backtest: BacktestConfig
    paper: PaperConfig
    live: LiveConfig
    environment: Literal["dev", "paper", "prod"] = "prod"

    @classmethod
    def from_yaml(cls, yaml_path: str | Path) -> "Config":
        """Load configuration from YAML file."""
        yaml_path = Path(yaml_path)
        if not yaml_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {yaml_path}")
        
        with open(yaml_path, "r") as f:
            raw_content = f.read()
            
        # Expand environment variables
        import os
        import re
        
        # Regex to find ${VAR} or $VAR
        pattern = re.compile(r'\$\{([^}]+)\}|\$([a-zA-Z_][a-zA-Z0-9_]*)')
        
        def replace_match(match):
            var_name = match.group(1) or match.group(2)
            return os.environ.get(var_name, match.group(0))  # Return original if not found
            
        expanded_content = pattern.sub(replace_match, raw_content)
        config_dict = yaml.safe_load(expanded_content)

        # Merge live research overrides if present (per-symbol strategy/risk + optional assets)
        _merge_live_research_overrides(config_dict, yaml_path)
            
        import os
        if "ENVIRONMENT" in os.environ:
            config_dict["environment"] = os.environ["ENVIRONMENT"]
            
        # Default local DB if not provided and not in prod
        # DATABASE_URL is optional at config load time - will be validated lazily when needed
        if not config_dict.get("data", {}).get("database_url"):
            import os
            from src.utils.secret_manager import is_cloud_platform
            
            db_url = os.getenv("DATABASE_URL")
            if not db_url:
                # In cloud platforms, allow missing DATABASE_URL - it will be injected later
                # Lazy validation will handle it when database is actually accessed
                is_cloud = is_cloud_platform()
                if is_cloud:
                    if "data" not in config_dict:
                        config_dict["data"] = {}
                    # Don't set database_url - let it be None and handle gracefully later
                    # The database connection will use lazy validation with retry logic
                else:
                    # Local development - allow None, will fail with clear error when accessed
                    if "data" not in config_dict:
                        config_dict["data"] = {}
                    # Don't set database_url - lazy validation will provide better error
            else:
                if "data" not in config_dict:
                    config_dict["data"] = {}
                config_dict["data"]["database_url"] = db_url

        # Force dry_run if not explicitly set in local/dev
        # BUT: Allow DRY_RUN env var to override
        if config_dict.get("environment") != "prod":
            if "system" not in config_dict:
                config_dict["system"] = {}
            if "dry_run" not in config_dict["system"]:
                # Check DRY_RUN env var first
                env_dry_run = os.getenv("DRY_RUN", os.getenv("SYSTEM_DRY_RUN", "1"))
                is_dry_run = env_dry_run in ("1", "true", "True", "TRUE")
                config_dict["system"]["dry_run"] = is_dry_run

        return cls(**config_dict)

    def validate_config(self) -> None:
        """Perform additional validation checks."""
        # Design lock: Mark price must be used
        if not self.execution.use_mark_price:
            raise ValueError("DESIGN LOCK VIOLATION: Mark price MUST be used for all safety-critical operations")
        
        # Design lock: Leverage cap enforcement
        if self.risk.max_leverage > 10.0:
            raise ValueError("DESIGN LOCK VIOLATION: Leverage cap is 10× (hard limit)")
        
        # Design lock: Pyramiding default
        if self.environment == "prod" and self.execution.pyramiding_enabled:
            raise Warning("Pyramiding is enabled in production - ensure this is intentional")
        
        # Validate basis guards are set
        if self.risk.basis_max_pct <= 0:
            raise ValueError("Basis guard must be configured (basis_max_pct > 0)")


def normalize_symbol_key(symbol: Optional[str]) -> str:
    """Normalize symbol key across spot/futures aliases."""
    if not symbol:
        return ""
    raw = str(symbol).strip().upper()
    if raw.startswith(("PF_", "PI_", "FI_")):
        raw = raw[3:]
    raw = raw.split(":")[0]
    if raw.endswith("USD") and "/" not in raw and len(raw) > 3:
        return f"{raw[:-3]}/USD"
    if "/" not in raw and raw:
        return raw
    return raw


def _resolve_symbol_override(
    overrides: Dict[str, Any],
    symbol: Optional[str],
) -> Optional[Any]:
    if not symbol or not overrides:
        return None
    key = normalize_symbol_key(symbol)
    if key in overrides:
        return overrides[key]
    for candidate_key, value in overrides.items():
        if normalize_symbol_key(candidate_key) == key:
            return value
    return None


def resolve_strategy_for_symbol(strategy: StrategyConfig, symbol: Optional[str]) -> StrategyConfig:
    """Return strategy config with symbol-level overrides applied."""
    override = _resolve_symbol_override(strategy.symbol_overrides, symbol)
    if not override:
        return strategy
    patch = {k: v for k, v in override.model_dump(exclude_none=True).items()}
    return strategy.model_copy(update=patch)


def resolve_risk_for_symbol(risk: RiskConfig, symbol: Optional[str]) -> RiskConfig:
    """Return risk config with symbol-level overrides applied."""
    override = _resolve_symbol_override(risk.symbol_overrides, symbol)
    if not override:
        return risk
    patch = {k: v for k, v in override.model_dump(exclude_none=True).items()}
    return risk.model_copy(update=patch)


def validate_required_env_vars() -> None:
    """
    Validate that required environment variables are set.
    
    This is a lightweight startup check that logs warnings but doesn't fail.
    Actual secret validation happens lazily when secrets are needed, with
    retry logic for cloud platform secret injection timing.
    
    In production (DigitalOcean), RUN_TIME secrets may not be immediately
    available at startup. This validation logs warnings but doesn't fail
    if we're in a deployment environment where secrets are injected later.
    """
    import os
    from src.utils.secret_manager import is_cloud_platform, check_secret_availability, get_environment
    
    # Standardize on ENVIRONMENT (not ENV)
    env = get_environment()
    dry_run = os.getenv("DRY_RUN", os.getenv("SYSTEM_DRY_RUN", "0"))
    
    # Convert dry_run to boolean
    is_dry_run = dry_run in ("1", "true", "True", "TRUE")
    
    # Only check in production mode and not dry run
    if is_dry_run or env != "prod":
        return  # Skip validation in dev/dry-run mode
    
    # Check required secrets (but don't fail - lazy validation will handle it)
    required_vars = ["DATABASE_URL", "KRAKEN_FUTURES_API_KEY", "KRAKEN_FUTURES_API_SECRET"]
    missing_vars = []
    unavailable_details = []
    
    for var in required_vars:
        is_available, error_msg = check_secret_availability(var)
        if not is_available:
            missing_vars.append(var)
            unavailable_details.append(f"  ❌ {var}: {error_msg}")
    
    if missing_vars:
        is_cloud = is_cloud_platform()
        from src.monitoring.logger import get_logger
        logger = get_logger(__name__)
        
        if is_cloud:
            # In cloud platform - log warning but don't fail
            logger.warning(
                "Some required environment variables are not yet available at startup",
                missing_vars=missing_vars,
                note="In cloud platforms, RUN_TIME secrets may be injected after startup. "
                     "The application will continue, and secrets will be validated with retry logic when actually needed. "
                     "Operations requiring these secrets will fail with clearer errors if secrets are truly missing."
            )
        else:
            # Local development - provide helpful error message
            error_msg = f"""
╔══════════════════════════════════════════════════════════════╗
║  CONFIGURATION WARNING: Missing Required Environment Variables ║
╚══════════════════════════════════════════════════════════════╝

The following required environment variables are not set:
{chr(10).join(unavailable_details)}

For local development:
  - Create .env.local file in project root
  - Add the following variables:
{chr(10).join(f"    {var}=your_value" for var in missing_vars)}

Example .env.local:
    DATABASE_URL=postgresql://user:password@localhost/tradingsystem
    KRAKEN_FUTURES_API_KEY=your_api_key
    KRAKEN_FUTURES_API_SECRET=your_api_secret

Note: Secrets will be validated with retry logic when actually needed.
      This warning is informational - the app will start but operations
      requiring these secrets will fail if they're not available.

Environment: {env}
"""
            logger.warning(error_msg)


def fail_fast_startup(strict: bool = True) -> None:
    """
    Production fail-fast startup validation.
    
    CRITICAL: This should be called at the very start of the application
    BEFORE any trading operations begin. In production, missing critical
    configuration causes immediate exit.
    
    Validates:
    1. ENVIRONMENT is set and valid
    2. Trading mode is unambiguous (DRY_RUN explicit)
    3. Required API credentials present
    4. Database URL configured
    5. Exchange type matches expectations (spot vs futures)
    
    Args:
        strict: If True (default), raises SystemExit on failure in production
    """
    import os
    from src.monitoring.logger import get_logger
    logger = get_logger(__name__)
    
    errors = []
    warnings = []
    
    # Get environment
    env = os.getenv("ENVIRONMENT", os.getenv("ENV", "")).lower()
    dry_run = os.getenv("DRY_RUN", os.getenv("SYSTEM_DRY_RUN", ""))
    
    # 1. ENVIRONMENT validation
    if not env:
        errors.append("ENVIRONMENT not set - must be 'prod', 'paper', 'dev', or 'local'")
    elif env not in ("prod", "paper", "dev", "local"):
        errors.append(f"ENVIRONMENT='{env}' invalid - must be 'prod', 'paper', 'dev', or 'local'")
    
    # 2. Trading mode validation (DRY_RUN must be explicit in production)
    if env == "prod":
        if dry_run == "":
            errors.append("DRY_RUN must be explicitly set in production (0 for live, 1 for dry-run)")
        elif dry_run not in ("0", "1", "true", "false", "True", "False"):
            errors.append(f"DRY_RUN='{dry_run}' ambiguous - use '0' or '1'")
    
    # 3. API credentials validation (in production)
    if env == "prod" and dry_run in ("0", "false", "False"):
        # Live trading requires real credentials
        futures_key = os.getenv("KRAKEN_FUTURES_API_KEY", "")
        futures_secret = os.getenv("KRAKEN_FUTURES_API_SECRET", "")
        
        if not futures_key or len(futures_key) < 20:
            errors.append("KRAKEN_FUTURES_API_KEY missing or invalid (too short)")
        if not futures_secret or len(futures_secret) < 30:
            errors.append("KRAKEN_FUTURES_API_SECRET missing or invalid (too short)")
    
    # 4. Database URL validation (in production/paper)
    if env in ("prod", "paper"):
        db_url = os.getenv("DATABASE_URL", "")
        if not db_url:
            errors.append("DATABASE_URL not set - required for production/paper trading")
        elif not db_url.startswith(("postgres", "postgresql")):
            warnings.append(f"DATABASE_URL does not appear to be PostgreSQL: {db_url[:30]}...")
    
    # 5. Exchange configuration validation
    # Check for conflicting env vars that might indicate misconfiguration
    testnet = os.getenv("KRAKEN_TESTNET", os.getenv("USE_TESTNET", ""))
    if env == "prod" and testnet in ("1", "true", "True"):
        errors.append("TESTNET enabled in production mode - this is likely misconfiguration")
    
    # Log results
    if warnings:
        for w in warnings:
            logger.warning("Startup validation warning", warning=w)
    
    if errors:
        error_block = "\n".join(f"  ❌ {e}" for e in errors)
        
        logger.critical(
            "STARTUP_VALIDATION_FAILED",
            environment=env,
            error_count=len(errors),
            errors=errors,
        )
        
        if strict and env == "prod":
            # HARD FAILURE in production
            raise SystemExit(f"""
╔══════════════════════════════════════════════════════════════╗
║  FATAL: PRODUCTION STARTUP FAILED - INVALID CONFIGURATION   ║
╚══════════════════════════════════════════════════════════════╝

The following configuration errors prevent safe startup:

{error_block}

The system CANNOT start in production mode with these errors.
Fix the configuration and restart.

Environment: {env}
DRY_RUN: {dry_run}
""")
        elif strict:
            # Log but don't exit in non-production
            logger.error(
                "Startup validation failed (non-production, continuing)",
                errors=errors
            )
    else:
        logger.info(
            "STARTUP_VALIDATION_PASSED",
            environment=env,
            dry_run=dry_run,
            mode="live" if dry_run in ("0", "false", "False") else "dry_run",
        )


def load_config(config_path: str | None = None) -> Config:
    """
    Load and validate configuration.
    
    Args:
        config_path: Path to config.yaml file. If None, uses src/config/config.yaml
    
    Returns:
        Validated Config object
    
    Raises:
        FileNotFoundError: If config file not found
        ValueError: If configuration validation fails
        SystemExit: If critical configuration missing in production
    """
    # CRITICAL: Fail-fast startup check (exits in production if invalid)
    fail_fast_startup(strict=True)
    
    # Legacy validation (warning-only)
    validate_required_env_vars()
    
    if config_path is None:
        config_path = Path(__file__).parent / "config.yaml"
    
    config = Config.from_yaml(config_path)
    config.validate_config()
    
    return config
