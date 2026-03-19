"""
ReplayMetrics — Collects safety, correctness, and trading metrics during replay.

Answers: "Would this have survived live?"
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class EquitySnapshot:
    """Point-in-time equity snapshot."""
    timestamp: datetime
    equity: Decimal
    margin_used: Decimal
    unrealized_pnl: Decimal
    open_positions: int


@dataclass
class ReplayMetrics:
    """Collects and reports all replay metrics.

    Categories:
    1. Safety & correctness (invariant violations, naked positions, kill switch)
    2. Trading performance (PnL, drawdown, win rate)
    3. Execution quality (slippage, fill delays, maker/taker ratio)
    4. System health (breaker opens, rate limits, error counts)
    """

    # -- Safety --
    invariant_k_violations: int = 0
    naked_position_detections: int = 0
    self_heal_attempts: int = 0
    self_heal_successes: int = 0
    self_heal_failures: int = 0
    kill_switch_activations: int = 0
    orders_blocked_by_rate_limiter: int = 0
    breaker_open_count: int = 0
    breaker_open_total_seconds: float = 0.0
    trades_recorded_total: int = 0
    trades_with_fills: int = 0

    # -- Order rejections --
    orders_rejected_total: int = 0
    reduce_only_rejections: int = 0
    insufficient_margin_rejections: int = 0
    min_size_rejections: int = 0

    # -- Trading --
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    gross_pnl: Decimal = field(default_factory=lambda: Decimal("0"))
    total_fees: Decimal = field(default_factory=lambda: Decimal("0"))
    total_funding: Decimal = field(default_factory=lambda: Decimal("0"))
    net_pnl: Decimal = field(default_factory=lambda: Decimal("0"))
    peak_equity: Decimal = field(default_factory=lambda: Decimal("0"))
    max_drawdown_pct: float = 0.0
    max_drawdown_usd: Decimal = field(default_factory=lambda: Decimal("0"))
    avg_holding_minutes: float = 0.0
    longest_trade_minutes: float = 0.0

    # -- Execution quality --
    total_fills: int = 0
    maker_fills: int = 0
    taker_fills: int = 0
    avg_slippage_bps: float = 0.0
    total_slippage_usd: Decimal = field(default_factory=lambda: Decimal("0"))

    # -- System health --
    total_ticks: int = 0
    failed_ticks: int = 0
    exceptions_caught: int = 0
    exceptions_by_type: Dict[str, int] = field(default_factory=dict)
    trade_paused_ticks: int = 0

    # -- Time series --
    equity_curve: List[EquitySnapshot] = field(default_factory=list)
    trade_log: List[Dict[str, Any]] = field(default_factory=list)
    event_log: List[Dict[str, Any]] = field(default_factory=list)

    # -- Methods --

    def record_equity(self, timestamp: datetime, equity: Decimal,
                      margin_used: Decimal, unrealized_pnl: Decimal, open_positions: int) -> None:
        """Record an equity snapshot."""
        snap = EquitySnapshot(
            timestamp=timestamp,
            equity=equity,
            margin_used=margin_used,
            unrealized_pnl=unrealized_pnl,
            open_positions=open_positions,
        )
        self.equity_curve.append(snap)

        # Update peak/drawdown
        if equity > self.peak_equity:
            self.peak_equity = equity
        if self.peak_equity > 0:
            dd = self.peak_equity - equity
            dd_pct = float(dd / self.peak_equity) * 100
            if dd > self.max_drawdown_usd:
                self.max_drawdown_usd = dd
            if dd_pct > self.max_drawdown_pct:
                self.max_drawdown_pct = dd_pct

    def record_trade(self, trade: Dict[str, Any]) -> None:
        """Record a completed trade."""
        self.total_trades += 1
        pnl = Decimal(str(trade.get("pnl", 0)))
        if pnl > 0:
            self.winning_trades += 1
        elif pnl < 0:
            self.losing_trades += 1
        self.gross_pnl += pnl
        self.trade_log.append(trade)

    def record_event(self, event_type: str, details: Optional[Dict] = None) -> None:
        """Record a safety/system event."""
        self.event_log.append({
            "type": event_type,
            "details": details or {},
        })

    def record_exception(self, exc_type: str) -> None:
        self.exceptions_caught += 1
        self.exceptions_by_type[exc_type] = self.exceptions_by_type.get(exc_type, 0) + 1

    # -- Computed metrics --

    @property
    def win_rate(self) -> float:
        if self.total_trades == 0:
            return 0.0
        return self.winning_trades / self.total_trades

    @property
    def profit_factor(self) -> float:
        wins = sum(Decimal(str(t.get("pnl", 0))) for t in self.trade_log if Decimal(str(t.get("pnl", 0))) > 0)
        losses = abs(sum(Decimal(str(t.get("pnl", 0))) for t in self.trade_log if Decimal(str(t.get("pnl", 0))) < 0))
        if losses == 0:
            return float("inf") if wins > 0 else 0.0
        return float(wins / losses)

    @property
    def maker_ratio(self) -> float:
        total = self.maker_fills + self.taker_fills
        if total == 0:
            return 0.0
        return self.maker_fills / total

    @property
    def fee_drag_pct(self) -> float:
        """Fee drag as % of gross PnL."""
        if self.gross_pnl == 0:
            return 0.0
        return float(self.total_fees / abs(self.gross_pnl)) * 100

    # -- Report --

    def summary(self) -> Dict[str, Any]:
        """Return full metrics summary as dict."""
        return {
            "safety": {
                "invariant_k_violations": self.invariant_k_violations,
                "naked_position_detections": self.naked_position_detections,
                "self_heal_attempts": self.self_heal_attempts,
                "self_heal_successes": self.self_heal_successes,
                "self_heal_failures": self.self_heal_failures,
                "kill_switch_activations": self.kill_switch_activations,
                "orders_blocked_by_rate_limiter": self.orders_blocked_by_rate_limiter,
                "breaker_opens": self.breaker_open_count,
                "breaker_open_total_seconds": self.breaker_open_total_seconds,
                "orders_rejected_total": self.orders_rejected_total,
                "reduce_only_rejections": self.reduce_only_rejections,
                "insufficient_margin_rejections": self.insufficient_margin_rejections,
                "min_size_rejections": self.min_size_rejections,
            },
            "trading": {
                "total_trades": self.total_trades,
                "winning_trades": self.winning_trades,
                "losing_trades": self.losing_trades,
                "win_rate": round(self.win_rate, 4),
                "profit_factor": round(self.profit_factor, 4),
                "gross_pnl": float(self.gross_pnl),
                "total_fees": float(self.total_fees),
                "total_funding": float(self.total_funding),
                "net_pnl": float(self.gross_pnl - self.total_fees - self.total_funding),
                "peak_equity": float(self.peak_equity),
                "max_drawdown_pct": round(self.max_drawdown_pct, 2),
                "max_drawdown_usd": float(self.max_drawdown_usd),
                "avg_holding_minutes": round(self.avg_holding_minutes, 1),
            },
            "execution": {
                "total_fills": self.total_fills,
                "maker_fills": self.maker_fills,
                "taker_fills": self.taker_fills,
                "maker_ratio": round(self.maker_ratio, 4),
                "avg_slippage_bps": round(self.avg_slippage_bps, 2),
                "total_slippage_usd": float(self.total_slippage_usd),
                "fee_drag_pct": round(self.fee_drag_pct, 2),
            },
            "system": {
                "total_ticks": self.total_ticks,
                "failed_ticks": self.failed_ticks,
                "exceptions_caught": self.exceptions_caught,
                "exceptions_by_type": dict(self.exceptions_by_type),
                "trade_paused_ticks": self.trade_paused_ticks,
                "trade_paused_ratio": round(
                    (float(self.trade_paused_ticks) / float(self.total_ticks)) if self.total_ticks > 0 else 0.0,
                    4,
                ),
            },
        }

    def print_report(self) -> None:
        """Print a formatted report to stdout."""
        s = self.summary()
        print("\n" + "=" * 70)
        print("REPLAY BACKTEST REPORT")
        print("=" * 70)

        print("\n--- SAFETY ---")
        for k, v in s["safety"].items():
            print(f"  {k}: {v}")

        print("\n--- TRADING ---")
        for k, v in s["trading"].items():
            print(f"  {k}: {v}")

        print("\n--- EXECUTION ---")
        for k, v in s["execution"].items():
            print(f"  {k}: {v}")

        print("\n--- SYSTEM ---")
        for k, v in s["system"].items():
            print(f"  {k}: {v}")

        print("=" * 70)

    def save(self, path: Path) -> None:
        """Save full metrics to JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = self.summary()
        data["equity_curve"] = [
            {"timestamp": s.timestamp.isoformat(), "equity": float(s.equity),
             "margin_used": float(s.margin_used), "open_positions": s.open_positions}
            for s in self.equity_curve
        ]
        data["trade_count"] = len(self.trade_log)
        data["event_count"] = len(self.event_log)
        path.write_text(json.dumps(data, indent=2, default=str))
