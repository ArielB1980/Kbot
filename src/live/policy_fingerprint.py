"""
Policy snapshot hashing utilities for rollout auditability.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Tuple


def build_auction_policy_snapshot(config: Any) -> Dict[str, Any]:
    risk = getattr(config, "risk", None)
    if risk is None:
        return {}
    def _rollout_state(enabled: bool, telemetry_only: bool, canary_symbols: Any) -> str:
        if not enabled:
            return "off"
        if telemetry_only:
            return "shadow"
        if canary_symbols:
            return "canary"
        return "on"

    chop_canary = list(getattr(risk, "auction_chop_canary_symbols", []) or [])
    anti_flip_canary = list(getattr(risk, "auction_anti_flip_canary_symbols", []) or [])
    return {
        "auction_mode_enabled": bool(getattr(risk, "auction_mode_enabled", False)),
        "auction_swap_threshold": float(getattr(risk, "auction_swap_threshold", 0.0)),
        "auction_min_hold_minutes": int(getattr(risk, "auction_min_hold_minutes", 0)),
        "auction_min_hold_high_conviction_minutes": int(
            getattr(risk, "auction_min_hold_high_conviction_minutes", 0)
        ),
        "auction_min_hold_high_conviction_threshold": float(
            getattr(risk, "auction_min_hold_high_conviction_threshold", 0.0)
        ),
        "auction_pnl_positive_lock_enabled": bool(
            getattr(risk, "auction_pnl_positive_lock_enabled", False)
        ),
        "auction_pnl_positive_lock_max_minutes": int(
            getattr(risk, "auction_pnl_positive_lock_max_minutes", 60)
        ),
        "auction_max_new_opens_per_cycle": int(getattr(risk, "auction_max_new_opens_per_cycle", 0)),
        "auction_no_signal_close_persistence_cycles": int(
            getattr(risk, "auction_no_signal_close_persistence_cycles", 0)
        ),
        "auction_chop_guard_enabled": bool(getattr(risk, "auction_chop_guard_enabled", False)),
        "auction_chop_telemetry_only": bool(getattr(risk, "auction_chop_telemetry_only", True)),
        "auction_chop_rollout_state": _rollout_state(
            bool(getattr(risk, "auction_chop_guard_enabled", False)),
            bool(getattr(risk, "auction_chop_telemetry_only", True)),
            chop_canary,
        ),
        "auction_anti_flip_lock_enabled": bool(getattr(risk, "auction_anti_flip_lock_enabled", False)),
        "auction_anti_flip_lock_telemetry_only": bool(
            getattr(risk, "auction_anti_flip_lock_telemetry_only", True)
        ),
        "auction_anti_flip_rollout_state": _rollout_state(
            bool(getattr(risk, "auction_anti_flip_lock_enabled", False)),
            bool(getattr(risk, "auction_anti_flip_lock_telemetry_only", True)),
            anti_flip_canary,
        ),
        "auction_anti_flip_lock_minutes": int(getattr(risk, "auction_anti_flip_lock_minutes", 0)),
    }


def hash_policy_snapshot(snapshot: Dict[str, Any]) -> str:
    payload = json.dumps(snapshot, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def build_policy_hash(config: Any) -> Tuple[Dict[str, Any], str]:
    snapshot = build_auction_policy_snapshot(config)
    return snapshot, hash_policy_snapshot(snapshot)
