"""Allowlist and denylist constraints for sandbox autoresearch."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


IMMUTABLE_CODE_PATH_PREFIXES: tuple[str, ...] = (
    "src/risk/",
    "src/execution/",
    "src/live/protection_ops.py",
    "src/entrypoints/prod_live.py",
    "src/runtime/guards.py",
)

# Constrain autonomous changes to a curated strategy parameter subset only.
ALLOWED_PARAMETER_PATHS: tuple[str, ...] = (
    # ── Entry quality gates ──
    "strategy.fvg_min_size_pct",
    "strategy.entry_zone_tolerance_pct",
    "strategy.entry_zone_tolerance_atr_mult",
    "strategy.min_score_tight_smc_aligned",
    "strategy.min_score_tight_smc_neutral",
    "strategy.min_score_wide_structure_aligned",
    "strategy.min_score_wide_structure_neutral",
    "strategy.signal_cooldown_hours",
    "strategy.bos_volume_threshold_mult",
    # ── Volume confirmation scoring (replaces EMA slope) ──
    "strategy.volume_score_high_mult",
    "strategy.volume_score_low_mult",
    # ── Structure confirmation scoring (replaces ADX) ──
    "strategy.structure_confirmation_score_points",
    "strategy.fib_proximity_bps",
    "strategy.fib_proximity_adaptive_scale",
    "strategy.fib_proximity_max_bps",
    "strategy.structure_fallback_score_premium",
    # ── RSI divergence scoring ──
    "strategy.rsi_divergence_score_bonus",
    # ── 1H Fibonacci confluence scoring ──
    "strategy.fib_1h_confluence_bonus",
    "strategy.fib_multi_tf_tolerance_bps",
    # ── Higher TF filter (controls the -4 to -5 penalty on out-of-zone signals) ──
    "strategy.higher_tf_penalty_outside_zone",
    # ── Stop loss sizing — unified regime per-setup-type stops ──
    "strategy.smc_atr_stop_ob",
    "strategy.smc_atr_stop_fvg",
    "strategy.smc_atr_stop_bos",
    "strategy.smc_atr_stop_trend",
    "strategy.min_score_smc_aligned",
    "strategy.min_score_smc_neutral",
    # ── Legacy regime stop sizing (used when unified_regime_enabled=False) ──
    "strategy.tight_smc_atr_stop_min",
    "strategy.tight_smc_atr_stop_max",
    "strategy.wide_structure_atr_stop_min",
    "strategy.wide_structure_atr_stop_max",
    # ── Take profit / exit mechanics (previously locked, now optimizable) ──
    "multi_tp.tp1_r_multiple",
    "multi_tp.tp1_close_pct",
    "multi_tp.tp2_r_multiple",
    "multi_tp.tp2_close_pct",
    "multi_tp.runner_pct",
    "multi_tp.trailing_stop_atr_multiplier",
    # ── Hold time / auction dynamics (the time dimension of edge) ──
    "risk.auction_min_hold_minutes",
    "risk.auction_min_hold_high_conviction_minutes",
    "risk.auction_min_hold_high_conviction_threshold",
    "risk.auction_swap_threshold",
    # ── Trailing stop dynamics (how long winners run) ──
    "multi_tp.tighten_trail_at_final_target_atr_mult",
    # ── Post-close cooldowns (how soon re-entry after exit) ──
    "strategy.signal_post_close_cooldown_win_minutes",
    "strategy.signal_post_close_cooldown_loss_minutes",
    # ── Thesis decay (how long conviction holds) ──
    "strategy.thesis_time_decay_window_hours",
    # ── Risk / position sizing ──
    "risk.risk_per_trade_pct",
    "risk.target_leverage",
    # ── Cost / RR constraints (these live on RiskConfig, not StrategyConfig) ──
    "risk.tight_smc_cost_cap_bps",
    "risk.tight_smc_min_rr_multiple",
    "risk.fee_edge_multiple_k",
    # ── Fee/funding distortion threshold (controls how much fee drag is tolerated) ──
    "risk.wide_structure_max_distortion_pct",
)

# Min/max bounds per parameter to prevent degenerate values.
PARAMETER_BOUNDS: dict[str, tuple[float, float]] = {
    # Entry quality gates
    "strategy.fvg_min_size_pct": (0.0001, 0.01),
    "strategy.entry_zone_tolerance_pct": (0.5, 5.0),
    "strategy.entry_zone_tolerance_atr_mult": (0.05, 1.5),
    "strategy.min_score_tight_smc_aligned": (30.0, 95.0),
    "strategy.min_score_tight_smc_neutral": (30.0, 95.0),
    "strategy.min_score_wide_structure_aligned": (30.0, 95.0),
    "strategy.min_score_wide_structure_neutral": (30.0, 95.0),
    "strategy.signal_cooldown_hours": (0.0, 24.0),
    "strategy.bos_volume_threshold_mult": (0.5, 3.0),
    # Volume confirmation scoring (replaces EMA slope)
    "strategy.volume_score_high_mult": (0.8, 3.0),
    "strategy.volume_score_low_mult": (0.5, 2.0),
    # Structure confirmation scoring (replaces ADX)
    "strategy.structure_confirmation_score_points": (5.0, 20.0),
    "strategy.fib_proximity_bps": (20.0, 200.0),
    "strategy.fib_proximity_adaptive_scale": (0.0, 1.0),
    "strategy.fib_proximity_max_bps": (20.0, 200.0),
    "strategy.structure_fallback_score_premium": (0.0, 20.0),
    # RSI divergence scoring
    "strategy.rsi_divergence_score_bonus": (0.0, 20.0),
    # 1H Fibonacci confluence scoring
    "strategy.fib_1h_confluence_bonus": (0.0, 15.0),
    "strategy.fib_multi_tf_tolerance_bps": (10.0, 100.0),
    # Higher TF penalty — let optimizer reduce or eliminate it
    "strategy.higher_tf_penalty_outside_zone": (-10.0, 0.0),
    # Stop loss — unified regime per-setup-type stops
    "strategy.smc_atr_stop_ob": (0.1, 1.5),
    "strategy.smc_atr_stop_fvg": (0.1, 1.5),
    "strategy.smc_atr_stop_bos": (0.2, 2.0),
    "strategy.smc_atr_stop_trend": (0.2, 2.0),
    "strategy.min_score_smc_aligned": (30.0, 95.0),
    "strategy.min_score_smc_neutral": (30.0, 95.0),
    # Legacy regime stop sizing
    "strategy.tight_smc_atr_stop_min": (0.1, 1.5),
    "strategy.tight_smc_atr_stop_max": (0.2, 3.0),
    "strategy.wide_structure_atr_stop_min": (0.2, 2.5),
    "strategy.wide_structure_atr_stop_max": (0.5, 4.0),
    # Take profit / exit — full range exploration
    "multi_tp.tp1_r_multiple": (0.3, 3.0),
    "multi_tp.tp1_close_pct": (0.1, 0.8),
    "multi_tp.tp2_r_multiple": (0.5, 6.0),
    "multi_tp.tp2_close_pct": (0.1, 0.6),
    "multi_tp.runner_pct": (0.05, 0.5),
    "multi_tp.trailing_stop_atr_multiplier": (1.0, 3.0),
    # Hold time / auction dynamics
    "risk.auction_min_hold_minutes": (30.0, 2880.0),  # 30 min to 48 hours
    "risk.auction_min_hold_high_conviction_minutes": (60.0, 2880.0),  # 1h to 48h
    "risk.auction_min_hold_high_conviction_threshold": (30.0, 80.0),
    "risk.auction_swap_threshold": (5.0, 40.0),
    # Trailing stop dynamics
    "multi_tp.tighten_trail_at_final_target_atr_mult": (0.5, 3.0),
    # Post-close cooldowns
    "strategy.signal_post_close_cooldown_win_minutes": (0.0, 360.0),
    "strategy.signal_post_close_cooldown_loss_minutes": (0.0, 720.0),
    # Thesis decay
    "strategy.thesis_time_decay_window_hours": (1.0, 168.0),  # 1h to 1 week
    # Risk sizing
    "risk.risk_per_trade_pct": (0.005, 0.05),
    "risk.target_leverage": (2.0, 10.0),
    # Cost / RR constraints (RiskConfig)
    "risk.tight_smc_cost_cap_bps": (10.0, 50.0),
    "risk.tight_smc_min_rr_multiple": (1.5, 5.0),
    "risk.fee_edge_multiple_k": (1.0, 10.0),
    # Fee/funding distortion threshold
    "risk.wide_structure_max_distortion_pct": (0.10, 0.40),
}

# Hard lock these paths even if they appear in future candidate generation logic.
DENIED_PARAMETER_PATHS: tuple[str, ...] = (
    "strategy.decision_timeframes",
    "strategy.refinement_timeframes",
    "strategy.regime_timeframes",
    "risk.max_leverage",
    "risk.max_aggregate_margin_pct_equity",
    "risk.max_single_position_margin_pct_equity",
)


@dataclass(frozen=True)
class AllowlistPolicy:
    """Defines what the sandbox autoresearch loop is allowed to mutate."""

    allowed_parameter_paths: tuple[str, ...] = ALLOWED_PARAMETER_PATHS
    denied_parameter_paths: tuple[str, ...] = DENIED_PARAMETER_PATHS
    immutable_code_path_prefixes: tuple[str, ...] = IMMUTABLE_CODE_PATH_PREFIXES

    def validate_candidate_keys(self, keys: Iterable[str]) -> list[str]:
        """Return a list of policy violation messages for candidate keys."""
        violations: list[str] = []
        for key in keys:
            if key in self.denied_parameter_paths:
                violations.append(f"Denied parameter path: {key}")
            elif key not in self.allowed_parameter_paths:
                violations.append(f"Parameter path not allowlisted: {key}")
        return violations

