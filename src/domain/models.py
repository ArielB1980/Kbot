"""
Domain models for the trading system.

These are the core business objects used throughout the application.
All timestamps use UTC timezone-aware datetimes.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional, List


class Side(str, Enum):
    """Trade side."""
    LONG = "long"
    SHORT = "short"


class OrderType(str, Enum):
    """Order type."""
    MARKET = "market"
    LIMIT = "limit"
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"


class OrderStatus(str, Enum):
    """Order status."""
    PENDING = "pending"
    SUBMITTED = "submitted"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class SignalType(str, Enum):
    """Signal type."""
    LONG = "long"
    SHORT = "short"
    EXIT_LONG = "exit_long"
    EXIT_SHORT = "exit_short"
    NO_SIGNAL = "no_signal"


class SetupType(str, Enum):
    """
    Setup type for regime classification.
    
    Used to distinguish between tight-stop SMC and wide-stop structure trades.
    """
    OB = "ob"           # Order Block (tight-stop regime: 0.4-1.0%)
    FVG = "fvg"         # Fair Value Gap (tight-stop regime: 0.4-1.0%)
    BOS = "bos"         # Break of Structure (wide-stop regime: 1.5-3.0%)
    TREND = "trend"     # HTF Trend following (wide-stop regime: 1.5-3.0%)


@dataclass(frozen=True)
class Candle:
    """
    OHLCV candle from spot market (used for strategy analysis).
    """
    timestamp: datetime
    symbol: str  # e.g., "BTC/USD" (spot)
    timeframe: str  # e.g., "15m", "1h", "4h", "1d"
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    
    def __post_init__(self):
        """Validate candle data."""
        if self.timestamp.tzinfo is None:
            raise ValueError("Candle timestamp must be timezone-aware (UTC)")
        if self.high < self.low:
            raise ValueError(f"Invalid candle: high ({self.high}) < low ({self.low})")
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ValueError(f"Invalid candle: OHLC values inconsistent")


@dataclass
class Signal:
    """
    Trading signal generated from spot market analysis (SMC).
    """
    timestamp: datetime
    symbol: str  # Spot symbol (e.g., "BTC/USD")
    signal_type: SignalType
    entry_price: Decimal  # From spot analysis
    stop_loss: Decimal  # From spot analysis (SMC invalidation level)
    take_profit: Optional[Decimal]  # From spot analysis (next SMC level)
    reasoning: str  # Full reasoning for signal
    
    # Regime classification (NEW)
    setup_type: SetupType  # OB/FVG/BOS/TREND
    regime: str  # "tight_smc" or "wide_structure"
    
    # Metadata
    higher_tf_bias: str  # e.g., "bullish", "bearish", "neutral"
    adx: Decimal
    atr: Decimal
    ema200_slope: str  # e.g., "up", "down", "flat"
    atr_ratio: Optional[Decimal] = None # Ratio of Current ATR / Avg ATR (for Vol Sizing)
    score: float = 0.0  # Quality score (0-100)
    tp_candidates: list[Decimal] = field(default_factory=list)  # Structure-based TP levels
    score_breakdown: dict = field(default_factory=dict)
    
    # Detailed Analysis Data (for Dashboard/Debugging)
    structure_info: dict = field(default_factory=dict)  # raw OB/FVG/BOS data
    meta_info: dict = field(default_factory=dict)       # Fibs, filters, etc.  # Detailed score components (SMC, Fib, Cost, etc.)
    
    def __post_init__(self):
        """Validate signal."""
        if self.timestamp.tzinfo is None:
            raise ValueError("Signal timestamp must be timezone-aware (UTC)")


@dataclass
class OrderIntent:
    """
    Intent to place an order (before conversion to futures pricing).
    """
    timestamp: datetime
    signal: Signal
    side: Side
    size_notional: Decimal  # Position size in USD notional
    leverage: Decimal  # Actual leverage (≤10×)
    
    # Spot-derived levels (to be converted to futures prices)
    entry_price_spot: Decimal
    stop_loss_spot: Decimal
    take_profit_spot: Optional[Decimal]

    # Futures execution levels (converted)
    entry_price_futures: Optional[Decimal] = None
    stop_loss_futures: Optional[Decimal] = None
    take_profit_futures: Optional[Decimal] = None


@dataclass
class Order:
    """
    Actual order on futures exchange.
    """
    order_id: str
    client_order_id: str
    timestamp: datetime
    symbol: str  # Futures symbol (e.g., "BTCUSD-PERP")
    side: Side
    order_type: OrderType
    size: Decimal  # Size in contracts/units
    price: Optional[Decimal]  # Limit price (None for market orders)
    status: OrderStatus
    filled_size: Decimal = Decimal("0")
    filled_price: Optional[Decimal] = None
    filled_at: Optional[datetime] = None
    
    # Parent order (for SL/TP)
    parent_order_id: Optional[str] = None
    reduce_only: bool = False


@dataclass
class Position:
    """
    Open position on futures exchange.
    """
    symbol: str  # Futures symbol (e.g., "BTCUSD-PERP")
    side: Side
    size: Decimal  # Position size in contracts
    size_notional: Decimal  # Position size in USD notional
    entry_price: Decimal  # Average entry price (futures mark price)
    current_mark_price: Decimal  # Current mark price
    liquidation_price: Decimal  # Exchange-reported liquidation price
    unrealized_pnl: Decimal
    leverage: Decimal  # Effective leverage
    margin_used: Decimal
    
    # Associated orders
    stop_loss_order_id: Optional[str] = None
    take_profit_order_id: Optional[str] = None  # Legacy single TP (deprecated, use tp_order_ids)
    tp_order_ids: list[str] = field(default_factory=list)  # Multi-TP ladder
    
    # Execution State
    trailing_active: bool = False
    break_even_active: bool = False
    peak_price: Optional[Decimal] = None  # Highest/Lowest mark price since trail activation
    
    # Active Trade Management
    # Immutable Parameters (set at entry, never changed)
    initial_stop_price: Optional[Decimal] = None  # Original stop loss level
    trade_type: Optional[str] = None  # "breakout", "pullback", "reversal"
    tp1_price: Optional[Decimal] = None  # First TP target
    tp2_price: Optional[Decimal] = None  # Second TP target
    final_target_price: Optional[Decimal] = None  # Final TP target
    partial_close_pct: Decimal = Decimal("0.5")  # % to close at TP1
    original_size: Optional[Decimal] = None  # Original position size before any closes
    
    # Basis and Funding Tracking
    basis_at_entry: Optional[Decimal] = None  # Futures - Spot at entry (bps)
    basis_current: Optional[Decimal] = None  # Current basis (bps)
    funding_rate: Optional[Decimal] = None  # Current funding rate
    cumulative_funding: Decimal = Decimal("0")  # Total funding paid/received
    
    # State Flags
    intent_confirmed: bool = False
    premise_invalidated: bool = False
    tp1_hit: bool = False
    tp2_hit: bool = False
    
    # Protection Status
    is_protected: bool = False  # True if both initial_stop_price and stop_loss_order_id exist
    protection_reason: Optional[str] = None  # Why protection status (e.g., "SL_ORDER_MISSING", "SL_PRICE_MISSING", "NO_SL_ORDER_OR_PRICE")
    
    # Metadata
    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Metadata
    setup_type: Optional[str] = None # e.g. "ob", "fvg"
    regime: Optional[str] = None # "tight_smc" or "wide_structure"
    
    # Auction metadata (for portfolio allocation)
    entry_score: Optional[float] = None  # Signal score at entry
    cluster: Optional[str] = None  # e.g., "tight_smc_ob", "wide_structure_bos"
    initial_stop_distance_pct: Optional[Decimal] = None  # Risk in % at entry
    margin_used_at_entry: Optional[Decimal] = None  # Margin at entry (for tracking)
    
    # Runner tracking (backtest use)
    _tp_fills_count: int = 0  # Number of TPs filled (for runner exit detection)
    _initial_risk_per_unit: Optional[Decimal] = None  # |entry - initial_stop| per unit, for R-multiple calc
    
    def liquidation_distance_pct(self) -> Decimal:
        """
        Calculate liquidation distance as percentage.
        
        Returns direction-aware distance:
        - Long: (mark_price - liq_price) / mark_price
        - Short: (liq_price - mark_price) / mark_price
        """
        if self.side == Side.LONG:
            return (self.current_mark_price - self.liquidation_price) / self.current_mark_price
        else:  # SHORT
            return (self.liquidation_price - self.current_mark_price) / self.current_mark_price


@dataclass
class RiskDecision:
    """
    Risk management decision for a proposed trade.
    """
    approved: bool
    position_notional: Decimal
    leverage: Decimal
    margin_required: Decimal
    liquidation_buffer_pct: Decimal
    basis_divergence_pct: Decimal
    estimated_fees_funding: Decimal
    
    # Opportunity Cost Replacement
    should_close_existing: bool = False
    close_symbol: Optional[str] = None
    
    # Rejection reasons
    rejection_reasons: list[str] = field(default_factory=list)
    
    # Utilisation boost (auction mode): True if notional was increased toward target_margin_util_min
    utilisation_boost_applied: bool = False
    
    # Metadata
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class Trade:
    """
    Completed trade (entry → exit).
    
    One row per position lifecycle (round trip).  Written exactly once
    when the position transitions to CLOSED.
    """
    trade_id: str
    symbol: str  # Futures symbol
    side: Side
    entry_price: Decimal   # VWAP across entry fills
    exit_price: Decimal    # VWAP across exit fills
    size: Decimal          # Position size in contracts (filled_entry_qty)
    size_notional: Decimal # Notional value at entry (size * entry_price)
    leverage: Decimal
    
    # P&L — fees and funding kept SEPARATE (never mixed)
    gross_pnl: Decimal
    fees: Decimal          # Estimated trading fees (from maker/taker schedule)
    funding: Decimal       # Estimated funding cost (from holding period + rate)
    net_pnl: Decimal       # gross_pnl - fees - funding
    
    # Timing
    entered_at: datetime
    exited_at: datetime    # Timestamp of final exit fill (exchange time)
    holding_period_hours: Decimal
    
    # Exit reason
    exit_reason: str  # "stop_loss", "take_profit", "manual", "kill_switch", "reconciliation"
    
    # Fill-type breakdown (for fee accuracy tracking)
    maker_fills_count: int = 0
    taker_fills_count: int = 0
    # Source tracking (live vs replay)
    source: str = "live"
    
    # Metadata
    setup_type: Optional[str] = None
    regime: Optional[str] = None
