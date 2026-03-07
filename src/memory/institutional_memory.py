from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional
import uuid

from src.memory.thesis import Thesis
from src.monitoring.logger import get_logger
from src.monitoring.alerting import send_alert_sync
from src.storage.repository import get_active_position, get_latest_thesis_for_symbol, upsert_thesis

logger = get_logger(__name__)


class InstitutionalMemoryManager:
    """Persistence + decay logic for institutional theses."""

    def __init__(self, strategy_config: Any):
        self.config = strategy_config
        self._cache: Dict[str, Thesis] = {}

    def _enabled(self) -> bool:
        return bool(getattr(self.config, "memory_enabled", False))

    def _alerts_enabled(self) -> bool:
        return bool(getattr(self.config, "thesis_alerts_enabled", False))

    def _should_alert_symbol(self, symbol: str) -> bool:
        """
        Guard thesis state alerts to actionable contexts.
        By default, only emit when the symbol has an active open position.
        """
        if not bool(getattr(self.config, "thesis_alert_open_positions_only", True)):
            return True
        try:
            return get_active_position(symbol) is not None
        except Exception:
            # Fail-open is unsafe for noise; fail-closed to avoid alert storms.
            return False

    def is_enabled_for_symbol(self, symbol: str) -> bool:
        if not self._enabled():
            return False
        canary = set(getattr(self.config, "thesis_canary_symbols", []) or [])
        if not canary:
            return True
        return symbol in canary

    @staticmethod
    def _thesis_id(symbol: str, zone_low: Decimal, zone_high: Decimal, daily_bias: str) -> str:
        key = f"{symbol}|{daily_bias}|{zone_low:.10f}|{zone_high:.10f}"
        return f"thesis-{uuid.uuid5(uuid.NAMESPACE_DNS, key).hex[:16]}"

    @staticmethod
    def _to_decimal(value: Any, fallback: Decimal = Decimal("0")) -> Decimal:
        try:
            return Decimal(str(value))
        except Exception:
            return fallback

    @staticmethod
    def _to_float(value: Any, fallback: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return fallback

    def _to_thesis(self, model: Any) -> Thesis:
        get = model.get if isinstance(model, dict) else lambda key: getattr(model, key)
        formed_at = get("formed_at")
        last_updated = get("last_updated")
        last_price_respect_ts = get("last_price_respect_ts")
        last_trade_at = get("last_trade_at")
        return Thesis(
            thesis_id=get("thesis_id"),
            symbol=get("symbol"),
            formed_at=formed_at.replace(tzinfo=timezone.utc) if formed_at and formed_at.tzinfo is None else formed_at,
            weekly_zone_low=self._to_decimal(get("weekly_zone_low")),
            weekly_zone_high=self._to_decimal(get("weekly_zone_high")),
            daily_bias=str(get("daily_bias")),
            initial_conviction=self._to_float(get("initial_conviction"), 100.0),
            current_conviction=self._to_float(get("current_conviction"), 100.0),
            last_updated=last_updated.replace(tzinfo=timezone.utc) if last_updated and last_updated.tzinfo is None else last_updated,
            last_price_respect_ts=(
                last_price_respect_ts.replace(tzinfo=timezone.utc)
                if last_price_respect_ts is not None and last_price_respect_ts.tzinfo is None
                else last_price_respect_ts
            ),
            original_signal_id=get("original_signal_id"),
            original_volume_avg=(
                self._to_decimal(get("original_volume_avg"), Decimal("0"))
                if get("original_volume_avg") is not None
                else None
            ),
            status=str(get("status")),
            invalidated_reason=get("invalidated_reason"),
            last_trade_id=get("last_trade_id"),
            last_trade_pnl=(
                self._to_decimal(get("last_trade_pnl"), Decimal("0"))
                if get("last_trade_pnl") is not None
                else None
            ),
            last_trade_at=(
                last_trade_at.replace(tzinfo=timezone.utc)
                if last_trade_at is not None and last_trade_at.tzinfo is None
                else last_trade_at
            ),
        )

    def _persist(self, thesis: Thesis) -> None:
        payload = asdict(thesis)
        upsert_thesis(payload)
        self._cache[thesis.symbol] = thesis

    def get_latest_thesis(self, symbol: str) -> Optional[Thesis]:
        cached = self._cache.get(symbol)
        if cached:
            return cached
        model = get_latest_thesis_for_symbol(symbol, statuses=["active", "decaying", "invalidated"])
        if not model:
            return None
        thesis = self._to_thesis(model)
        self._cache[symbol] = thesis
        return thesis

    def create_or_refresh_thesis(
        self,
        *,
        symbol: str,
        weekly_zone_low: Decimal,
        weekly_zone_high: Decimal,
        daily_bias: str,
        signal_id: str,
        current_volume_avg: Optional[Decimal] = None,
        now: Optional[datetime] = None,
    ) -> Thesis:
        now = now or datetime.now(timezone.utc)
        thesis_id = self._thesis_id(symbol, weekly_zone_low, weekly_zone_high, daily_bias)
        existing = self.get_latest_thesis(symbol)

        if existing and existing.thesis_id == thesis_id:
            existing.last_updated = now
            existing.original_signal_id = signal_id
            existing.status = "active"
            if current_volume_avg is not None:
                existing.original_volume_avg = current_volume_avg
            self._persist(existing)
            return existing

        thesis = Thesis(
            thesis_id=thesis_id,
            symbol=symbol,
            formed_at=now,
            weekly_zone_low=weekly_zone_low,
            weekly_zone_high=weekly_zone_high,
            daily_bias=daily_bias,  # type: ignore[arg-type]
            initial_conviction=100.0,
            current_conviction=100.0,
            last_updated=now,
            last_price_respect_ts=now,
            original_signal_id=signal_id,
            original_volume_avg=current_volume_avg,
            status="active",
        )
        self._persist(thesis)
        return thesis

    def update_conviction(
        self,
        thesis: Thesis,
        *,
        current_price: Decimal,
        current_volume_avg: Optional[Decimal],
        now: Optional[datetime] = None,
        emit_log: bool = True,
    ) -> Dict[str, Any]:
        now = now or datetime.now(timezone.utc)
        hours_old = max(0.0, (now - thesis.formed_at).total_seconds() / 3600.0)

        max_time_decay = self._to_float(getattr(self.config, "thesis_time_decay_max_points", 45.0), 45.0)
        decay_window = self._to_float(getattr(self.config, "thesis_time_decay_window_hours", 12.0), 12.0)
        zone_penalty_val = self._to_float(getattr(self.config, "thesis_zone_breach_penalty_points", 35.0), 35.0)
        volume_penalty_val = self._to_float(getattr(self.config, "thesis_volume_fade_penalty_points", 15.0), 15.0)
        conviction_floor = self._to_float(getattr(self.config, "thesis_conviction_floor", 5.0), 5.0)
        early_exit = self._to_float(getattr(self.config, "thesis_early_exit_threshold", 35.0), 35.0)

        previous_status = thesis.status
        previous_conviction = float(thesis.current_conviction)
        time_decay = min(max_time_decay, (hours_old / max(decay_window, 1.0)) * max_time_decay)
        inside_zone = thesis.weekly_zone_low <= current_price <= thesis.weekly_zone_high
        zone_rejection = 0.0 if inside_zone else zone_penalty_val
        volume_fade = 0.0
        if thesis.original_volume_avg is not None and current_volume_avg is not None:
            if current_volume_avg < thesis.original_volume_avg:
                volume_fade = volume_penalty_val

        decay = time_decay + zone_rejection + volume_fade
        conviction = max(conviction_floor, 100.0 - decay)
        thesis.current_conviction = conviction
        thesis.last_updated = now
        if inside_zone:
            thesis.last_price_respect_ts = now

        if conviction <= early_exit:
            thesis.status = "invalidated"
            thesis.invalidated_reason = "conviction_below_threshold"
        elif conviction < 100.0:
            thesis.status = "decaying"
            thesis.invalidated_reason = None
        else:
            thesis.status = "active"
            thesis.invalidated_reason = None

        self._persist(thesis)

        snapshot = {
            "thesis_id": thesis.thesis_id,
            "symbol": thesis.symbol,
            "conviction": conviction,
            "hours_old": hours_old,
            "time_decay": time_decay,
            "zone_rejection": zone_rejection,
            "volume_fade": volume_fade,
            "inside_weekly_zone": inside_zone,
            "status": thesis.status,
        }
        if emit_log:
            logger.info("thesis_conviction", **snapshot)
        self._maybe_send_thesis_state_alert(
            thesis=thesis,
            previous_status=previous_status,
            previous_conviction=previous_conviction,
            snapshot=snapshot,
        )
        return snapshot

    def _maybe_send_thesis_state_alert(
        self,
        *,
        thesis: Thesis,
        previous_status: str,
        previous_conviction: float,
        snapshot: Dict[str, Any],
    ) -> None:
        if not self._alerts_enabled():
            return
        if not self._should_alert_symbol(thesis.symbol):
            return
        threshold = self._to_float(getattr(self.config, "thesis_early_exit_threshold", 35.0), 35.0)
        conviction = float(snapshot.get("conviction", thesis.current_conviction))
        hours_old = float(snapshot.get("hours_old", 0.0))
        total_decay = max(0.0, 100.0 - conviction)
        symbol = thesis.symbol

        if previous_conviction > threshold and conviction <= threshold:
            msg = (
                f"[THESIS] {symbol} conviction collapsed to {conviction:.1f}% (weekly zone rejected)\n"
                f"Original thesis formed {hours_old:.1f}h ago | Current decay: {total_decay:.1f}%"
            )
            send_alert_sync(
                "THESIS_CONVICTION_COLLAPSE",
                msg,
                rate_limit_key=f"THESIS_CONVICTION_COLLAPSE:{symbol}",
                rate_limit_seconds=1800,
            )

        if previous_status != "invalidated" and thesis.status == "invalidated":
            msg = (
                f"[THESIS] {symbol} invalidated at {conviction:.1f}% conviction\n"
                f"Formed {hours_old:.1f}h ago | Decay: {total_decay:.1f}%"
            )
            send_alert_sync(
                "THESIS_INVALIDATED",
                msg,
                rate_limit_key=f"THESIS_INVALIDATED:{symbol}",
                rate_limit_seconds=1800,
            )

    def update_conviction_for_symbol(
        self,
        symbol: str,
        *,
        current_price: Decimal,
        current_volume_avg: Optional[Decimal] = None,
        now: Optional[datetime] = None,
        emit_log: bool = True,
    ) -> Optional[Dict[str, Any]]:
        thesis = self.get_latest_thesis(symbol)
        if not thesis:
            return None
        return self.update_conviction(
            thesis,
            current_price=current_price,
            current_volume_avg=current_volume_avg,
            now=now,
            emit_log=emit_log,
        )

    def conviction_score_adjustment(self, conviction: float) -> float:
        neutral = self._to_float(getattr(self.config, "thesis_score_neutral_conviction", 60.0), 60.0)
        floor = self._to_float(getattr(self.config, "thesis_conviction_floor", 5.0), 5.0)
        max_bonus = self._to_float(getattr(self.config, "thesis_score_max_bonus", 8.0), 8.0)
        max_penalty = self._to_float(getattr(self.config, "thesis_score_max_penalty", 12.0), 12.0)

        if conviction >= neutral:
            span = max(1.0, 100.0 - neutral)
            return max_bonus * ((conviction - neutral) / span)
        span = max(1.0, neutral - floor)
        return -max_penalty * ((neutral - conviction) / span)

    def should_block_reentry(self, symbol: str, conviction: Optional[float] = None) -> bool:
        threshold = self._to_float(getattr(self.config, "thesis_reentry_block_threshold", 25.0), 25.0)
        if conviction is None:
            thesis = self.get_latest_thesis(symbol)
            conviction = thesis.current_conviction if thesis else None
        if conviction is None:
            return False
        return conviction <= threshold

    def on_trade_recorded(
        self,
        *,
        symbol: str,
        trade_id: str,
        net_pnl: Decimal,
        exited_at: Optional[datetime] = None,
    ) -> None:
        thesis = self.get_latest_thesis(symbol)
        if not thesis:
            return
        thesis.last_trade_id = trade_id
        thesis.last_trade_pnl = net_pnl
        thesis.last_trade_at = exited_at or datetime.now(timezone.utc)
        thesis.last_updated = datetime.now(timezone.utc)
        if thesis.current_conviction <= self._to_float(getattr(self.config, "thesis_early_exit_threshold", 35.0), 35.0):
            thesis.status = "expired"
        self._persist(thesis)
