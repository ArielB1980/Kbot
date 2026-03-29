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
    "strategy.adx_threshold",
    "strategy.fvg_min_size_pct",
    "strategy.entry_zone_tolerance_pct",
    "strategy.entry_zone_tolerance_atr_mult",
    "strategy.min_score_tight_smc_aligned",
    "strategy.min_score_wide_structure_aligned",
    "strategy.signal_cooldown_hours",
    "strategy.tight_smc_atr_stop_min",
    "strategy.tight_smc_atr_stop_max",
    "strategy.wide_structure_atr_stop_min",
    "strategy.wide_structure_atr_stop_max",
    "strategy.ema_slope_bonus",
    "strategy.bos_volume_threshold_mult",
    "strategy.fib_proximity_adaptive_scale",
    "strategy.fib_proximity_max_bps",
    "strategy.structure_fallback_score_premium",
)

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

