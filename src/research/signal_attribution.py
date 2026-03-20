"""Signal-to-trade performance attribution.

Links signals to executed trades and realized P&L, enabling
traceability: signal -> trade -> P&L, broken down by strategy,
regime, timeframe, and instrument.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass
class TradeRecord:
    """Record of an executed trade linked to its originating signal."""

    trade_id: str
    signal_timestamp: str
    symbol: str
    direction: str
    strategy_source: str
    regime: str
    setup_type: str
    signal_score: float
    entry_price: float
    exit_price: float
    realized_pnl: float
    size_notional: float
    entry_time: str
    exit_time: str
    exit_reason: str = ""  # stop_loss, take_profit, manual, etc.


@dataclass
class AttributionBreakdown:
    """Aggregated P&L attribution for a group of trades."""

    total_pnl: float = 0.0
    trade_count: int = 0
    win_count: int = 0
    loss_count: int = 0
    avg_pnl: float = 0.0
    total_notional: float = 0.0

    @property
    def win_rate(self) -> float:
        if self.trade_count == 0:
            return 0.0
        return self.win_count / self.trade_count

    def add_trade(self, pnl: float, notional: float) -> None:
        """Accumulate one trade into the breakdown."""
        self.total_pnl += pnl
        self.trade_count += 1
        self.total_notional += notional
        if pnl >= 0:
            self.win_count += 1
        else:
            self.loss_count += 1
        self.avg_pnl = self.total_pnl / self.trade_count


@dataclass
class AttributionReport:
    """Full attribution report with multiple breakdown dimensions."""

    period_start: str
    period_end: str
    total: AttributionBreakdown = field(default_factory=AttributionBreakdown)
    by_strategy: dict[str, AttributionBreakdown] = field(default_factory=dict)
    by_regime: dict[str, AttributionBreakdown] = field(default_factory=dict)
    by_symbol: dict[str, AttributionBreakdown] = field(default_factory=dict)
    by_setup_type: dict[str, AttributionBreakdown] = field(default_factory=dict)
    by_direction: dict[str, AttributionBreakdown] = field(default_factory=dict)


class SignalAttributionTracker:
    """Tracks signal-to-trade attribution and produces breakdown reports.

    Trades are stored in a JSON-lines file for persistence.
    """

    def __init__(self, storage_dir: str | Path) -> None:
        self._storage_dir = Path(storage_dir)
        self._storage_dir.mkdir(parents=True, exist_ok=True)
        self._trades_path = self._storage_dir / "trades.jsonl"

    def record_trade(self, trade: TradeRecord) -> None:
        """Persist a completed trade record."""
        line = json.dumps(asdict(trade), default=str)
        with self._trades_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    def load_trades(
        self,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> list[TradeRecord]:
        """Load trade records, optionally filtered by time range."""
        if not self._trades_path.exists():
            return []
        trades: list[TradeRecord] = []
        for line in self._trades_path.read_text(encoding="utf-8").strip().splitlines():
            if not line:
                continue
            d = json.loads(line)
            trade = TradeRecord(**d)
            if since is not None:
                exit_dt = datetime.fromisoformat(trade.exit_time)
                if exit_dt < since:
                    continue
            if until is not None:
                exit_dt = datetime.fromisoformat(trade.exit_time)
                if exit_dt > until:
                    continue
            trades.append(trade)
        return trades

    def build_report(
        self,
        trades: list[TradeRecord],
        period_start: datetime,
        period_end: datetime,
    ) -> AttributionReport:
        """Build an attribution report from a list of trades."""
        report = AttributionReport(
            period_start=period_start.isoformat(),
            period_end=period_end.isoformat(),
        )

        for trade in trades:
            pnl = trade.realized_pnl
            notional = trade.size_notional

            report.total.add_trade(pnl, notional)

            strat_key = trade.strategy_source or "unknown"
            if strat_key not in report.by_strategy:
                report.by_strategy[strat_key] = AttributionBreakdown()
            report.by_strategy[strat_key].add_trade(pnl, notional)

            if trade.regime not in report.by_regime:
                report.by_regime[trade.regime] = AttributionBreakdown()
            report.by_regime[trade.regime].add_trade(pnl, notional)

            if trade.symbol not in report.by_symbol:
                report.by_symbol[trade.symbol] = AttributionBreakdown()
            report.by_symbol[trade.symbol].add_trade(pnl, notional)

            if trade.setup_type not in report.by_setup_type:
                report.by_setup_type[trade.setup_type] = AttributionBreakdown()
            report.by_setup_type[trade.setup_type].add_trade(pnl, notional)

            if trade.direction not in report.by_direction:
                report.by_direction[trade.direction] = AttributionBreakdown()
            report.by_direction[trade.direction].add_trade(pnl, notional)

        return report

    def report_to_markdown(self, report: AttributionReport) -> str:
        """Render an attribution report as markdown."""
        lines = [
            "# Performance Attribution Report",
            f"Period: {report.period_start} — {report.period_end}",
            "",
            "## Overall",
            "",
            f"- Total P&L: ${report.total.total_pnl:,.2f}",
            f"- Trades: {report.total.trade_count}",
            f"- Win Rate: {report.total.win_rate * 100:.1f}%",
            f"- Avg P&L: ${report.total.avg_pnl:,.2f}",
            "",
        ]

        for label, breakdown_dict in [
            ("By Strategy", report.by_strategy),
            ("By Regime", report.by_regime),
            ("By Symbol", report.by_symbol),
            ("By Setup Type", report.by_setup_type),
            ("By Direction", report.by_direction),
        ]:
            lines += [f"## {label}", ""]
            lines.append("| Key | P&L | Trades | Win Rate | Avg P&L |")
            lines.append("|-----|-----|--------|----------|---------|")
            for key, bd in sorted(
                breakdown_dict.items(), key=lambda x: x[1].total_pnl, reverse=True
            ):
                lines.append(
                    f"| {key} | ${bd.total_pnl:,.2f} | {bd.trade_count} | "
                    f"{bd.win_rate * 100:.1f}% | ${bd.avg_pnl:,.2f} |"
                )
            lines.append("")

        return "\n".join(lines)

    def report_to_json(self, report: AttributionReport) -> str:
        """Serialize an attribution report to JSON."""
        data = asdict(report)
        # Add computed win_rate to each breakdown
        for dimension in ["by_strategy", "by_regime", "by_symbol", "by_setup_type", "by_direction"]:
            if dimension in data:
                for bd in data[dimension].values():
                    bd["win_rate"] = (
                        bd["win_count"] / bd["trade_count"] if bd["trade_count"] > 0 else 0.0
                    )
        total = data.get("total", {})
        tc = total.get("trade_count", 0)
        total["win_rate"] = total.get("win_count", 0) / tc if tc > 0 else 0.0
        return json.dumps(data, indent=2, default=str)
