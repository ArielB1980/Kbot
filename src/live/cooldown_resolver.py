"""
Signal and post-close cooldown logic extracted from LiveTrading.

Handles:
- Symbol key normalization for cooldown matching
- Signal cooldown parameter resolution (global + per-symbol overrides)
- Post-close cooldown classification (win/loss/strategic buckets)
- Canary diagnostic symbol set resolution
- 4H warmup skip diagnostics
- Thesis trace field attachment
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from src.domain.models import Candle
from src.monitoring.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Symbol normalization
# ---------------------------------------------------------------------------


def normalize_symbol_key(symbol: str) -> str:
    """Normalize exchange symbols to canonical ``BASE/USD`` form for cooldown matching."""
    key = (symbol or "").strip().upper()
    key = key.split(":")[0]
    if key.startswith("PF_"):
        base = key.replace("PF_", "").replace("USD", "")
        if base:
            return f"{base}/USD"
    return key


# ---------------------------------------------------------------------------
# Cooldown resolution helpers
# ---------------------------------------------------------------------------


def resolve_signal_cooldown_params(strategy_config: Any, symbol: str) -> dict[str, Any]:
    """Resolve signal cooldown hours (global policy + per-symbol overrides)."""
    base_hours = float(getattr(strategy_config, "signal_cooldown_hours", 4.0))
    overrides = getattr(strategy_config, "symbol_overrides", {}) or {}
    normalized = normalize_symbol_key(symbol)
    for key, override in overrides.items():
        if normalize_symbol_key(key) != normalized:
            continue
        override_hours = getattr(override, "signal_cooldown_hours", None)
        if override_hours is not None:
            return {"cooldown_hours": float(override_hours), "canary_applied": True}
    return {
        "cooldown_hours": base_hours,
        "canary_applied": False,
    }


def resolve_post_close_cooldown_kind_and_minutes(
    exit_reason: str,
    strategy_config: Any,
) -> tuple[str, int]:
    """Classify post-close cooldown bucket from exit reason."""
    reason = (exit_reason or "").strip().lower()
    strategic_markers = ("time_based", "strategic_close", "auction_strategic_close")
    if any(marker in reason for marker in strategic_markers):
        minutes = int(getattr(strategy_config, "signal_post_close_cooldown_strategic_minutes", 120))
        return "POST_CLOSE_STRATEGIC", max(0, minutes)
    loss_markers = ("stop", "invalidation", "loss", "liquidation")
    if any(marker in reason for marker in loss_markers):
        minutes = int(getattr(strategy_config, "signal_post_close_cooldown_loss_minutes", 120))
        return "POST_CLOSE_LOSS", max(0, minutes)
    minutes = int(getattr(strategy_config, "signal_post_close_cooldown_win_minutes", 30))
    return "POST_CLOSE_WIN", max(0, minutes)


def resolve_canary_diagnostic_symbols(strategy_config: Any) -> set[str]:
    """Build normalized canary symbol set from strategy canary allowlists."""
    symbols: list[str] = []
    symbols.extend(getattr(strategy_config, "signal_cooldown_canary_symbols", []) or [])
    symbols.extend(getattr(strategy_config, "fvg_min_size_pct_canary_symbols", []) or [])
    return {normalize_symbol_key(s) for s in symbols if s}


def build_4h_warmup_skip_diagnostic(
    strategy_config: Any,
    symbol: str,
    futures_symbol: str | None,
    stage_b_reason: str,
    candles_4h: list[Candle],
    required_candles: int,
    decision_tf: str,
) -> dict[str, Any] | None:
    """Build canary-only warmup diagnostic payload for stage-B insufficient-candle skips."""
    canary_symbols = resolve_canary_diagnostic_symbols(strategy_config)
    if not canary_symbols:
        return None
    normalized_symbol = normalize_symbol_key(symbol)
    if normalized_symbol not in canary_symbols:
        return None

    reason = str(stage_b_reason or "")
    if not (reason.startswith(f"candles_{decision_tf}=") and "<" in reason):
        return None

    latest = candles_4h[-1].timestamp if candles_4h else None
    return {
        "symbol": symbol,
        "normalized_symbol": normalized_symbol,
        "resolved_futures_symbol": futures_symbol,
        "candles_4h_count": len(candles_4h),
        "required_candles_4h": int(required_candles),
        "last_4h_ts": latest.isoformat() if isinstance(latest, datetime) else None,
        "skip_reason": "insufficient_4h_history",
        "stage_b_reason": reason,
        "is_canary": True,
    }


def attach_thesis_trace_fields(
    trace_details: dict[str, Any],
    thesis_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    """Enrich DECISION_TRACE payload with thesis conviction telemetry."""
    if not thesis_snapshot:
        return trace_details
    trace_details["thesis_conviction"] = thesis_snapshot.get("conviction")
    trace_details["thesis_status"] = thesis_snapshot.get("status")
    trace_details["thesis_decay"] = {
        "time_decay": thesis_snapshot.get("time_decay"),
        "zone_rejection": thesis_snapshot.get("zone_rejection"),
        "volume_fade": thesis_snapshot.get("volume_fade"),
    }
    return trace_details


# ---------------------------------------------------------------------------
# CooldownResult and CooldownResolver class
# ---------------------------------------------------------------------------


class CooldownResult:
    """Result of a cooldown evaluation for a symbol."""

    __slots__ = (
        "suppressed",
        "cooldown_kind",
        "cooldown_until",
        "cooldown_hours",
        "cooldown_minutes",
        "canary_applied",
        "last_close_at",
        "last_close_reason",
    )

    def __init__(
        self,
        suppressed: bool,
        cooldown_kind: str | None = None,
        cooldown_until: datetime | None = None,
        cooldown_hours: float | None = None,
        cooldown_minutes: int | None = None,
        canary_applied: bool = False,
        last_close_at: datetime | None = None,
        last_close_reason: str | None = None,
    ):
        self.suppressed = suppressed
        self.cooldown_kind = cooldown_kind
        self.cooldown_until = cooldown_until
        self.cooldown_hours = cooldown_hours
        self.cooldown_minutes = cooldown_minutes
        self.canary_applied = canary_applied
        self.last_close_at = last_close_at
        self.last_close_reason = last_close_reason


class CooldownResolver:
    """Manages signal cooldown state and evaluation.

    Encapsulates signal cooldown tracking (per-symbol in-position cooldown)
    and post-close cooldown evaluation.
    """

    def __init__(self, strategy_config: Any):
        self._strategy_config = strategy_config
        # symbol -> cooldown expiry (in-position cooldown)
        self._signal_cooldown: dict[str, datetime] = {}

    def evaluate(
        self,
        symbol: str,
        has_position: bool,
        position_data: dict[str, Any] | None,
        close_ctx: dict[str, Any] | None,
        now: datetime | None = None,
        replay_relaxed: bool = False,
    ) -> CooldownResult:
        """Evaluate whether a signal for *symbol* is suppressed by cooldown.

        Args:
            symbol: Spot symbol (e.g. ``BTC/USD``).
            has_position: Whether symbol has an active position.
            position_data: Raw exchange position dict (may be ``None``).
            close_ctx: Recent close context dict with ``last_close_at``,
                ``last_close_reason``, ``cooldown_kind``, ``cooldown_minutes``.
            now: Current UTC time (defaults to ``utcnow``).
            replay_relaxed: If True, bypass cooldowns (replay research mode).

        Returns:
            CooldownResult with suppression decision and diagnostics.
        """
        if now is None:
            now = datetime.now(UTC)

        cooldown_params = resolve_signal_cooldown_params(self._strategy_config, symbol)
        in_position_cooldown_hours = float(cooldown_params["cooldown_hours"])
        canary_applied = bool(cooldown_params["canary_applied"])

        cooldown_kind: str | None = None
        cooldown_until: datetime | None = None
        last_close_at: datetime | None = None
        last_close_reason: str | None = None
        cooldown_minutes: int | None = None

        if has_position:
            cooldown_kind = "IN_POSITION"
            cooldown_until = self._signal_cooldown.get(symbol)
            if cooldown_until is None:
                cooldown_until = now + timedelta(hours=in_position_cooldown_hours)
                self._signal_cooldown[symbol] = cooldown_until
        else:
            # Clear stale in-position cooldown for flat symbols.
            self._signal_cooldown.pop(symbol, None)
            if close_ctx:
                cooldown_kind = str(close_ctx["cooldown_kind"])
                last_close_at = close_ctx["last_close_at"]
                last_close_reason = close_ctx["last_close_reason"]
                cooldown_minutes = int(close_ctx["cooldown_minutes"])
                cooldown_until = last_close_at + timedelta(minutes=cooldown_minutes)

        if replay_relaxed:
            cooldown_until = None

        suppressed = bool(cooldown_until and now < cooldown_until)

        return CooldownResult(
            suppressed=suppressed,
            cooldown_kind=cooldown_kind,
            cooldown_until=cooldown_until,
            cooldown_hours=in_position_cooldown_hours if cooldown_kind == "IN_POSITION" else None,
            cooldown_minutes=cooldown_minutes,
            canary_applied=canary_applied,
            last_close_at=last_close_at,
            last_close_reason=last_close_reason,
        )

    def record_entry_cooldown(self, symbol: str, hours: float | None = None) -> None:
        """Record an in-position cooldown for *symbol* after entry."""
        if hours is None:
            params = resolve_signal_cooldown_params(self._strategy_config, symbol)
            hours = float(params["cooldown_hours"])
        self._signal_cooldown[symbol] = datetime.now(UTC) + timedelta(hours=hours)
