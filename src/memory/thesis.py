from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal, Optional


@dataclass
class Thesis:
    thesis_id: str
    symbol: str
    formed_at: datetime
    weekly_zone_low: Decimal
    weekly_zone_high: Decimal
    daily_bias: Literal["bullish", "bearish", "neutral"]
    current_conviction: float
    last_updated: datetime
    last_price_respect_ts: Optional[datetime]
    original_signal_id: str
    initial_conviction: float = 100.0
    original_volume_avg: Optional[Decimal] = None
    status: Literal["active", "decaying", "invalidated", "expired"] = "active"
    invalidated_reason: Optional[str] = None
    last_trade_id: Optional[str] = None
    last_trade_pnl: Optional[Decimal] = None
    last_trade_at: Optional[datetime] = None
