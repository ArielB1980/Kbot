"""Daily signal summary report generation.

Produces structured daily reports from signal logs:
- Total signals, by strategy, by direction, by regime
- Hit-rate tracking over configurable horizons
- Markdown and JSON output formats
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path

from src.research.signal_logger import SignalFileLogger, SignalLogEntry


@dataclass
class HitRateStats:
    """Hit-rate statistics for a group of signals."""

    total_signals: int = 0
    hits: int = 0
    misses: int = 0
    pending: int = 0

    @property
    def hit_rate(self) -> float:
        """Hit rate as a fraction (0.0-1.0). Returns 0.0 if no resolved signals."""
        resolved = self.hits + self.misses
        if resolved == 0:
            return 0.0
        return self.hits / resolved


@dataclass
class DailySignalSummary:
    """Structured daily signal report."""

    date: str
    total_signals: int = 0
    by_direction: dict[str, int] = field(default_factory=dict)
    by_regime: dict[str, int] = field(default_factory=dict)
    by_strategy: dict[str, int] = field(default_factory=dict)
    by_symbol: dict[str, int] = field(default_factory=dict)
    by_setup_type: dict[str, int] = field(default_factory=dict)
    avg_score: float = 0.0
    score_distribution: dict[str, int] = field(default_factory=dict)
    hit_rates: dict[str, HitRateStats] = field(default_factory=dict)


def _score_bucket(score: float) -> str:
    """Bucket a 0-100 score into grade ranges."""
    if score >= 80:
        return "A (80-100)"
    if score >= 60:
        return "B (60-79)"
    if score >= 40:
        return "C (40-59)"
    if score >= 20:
        return "D (20-39)"
    return "F (0-19)"


def build_daily_summary(
    entries: list[SignalLogEntry],
    date: datetime,
    *,
    hit_rate_resolver: HitRateResolver | None = None,
) -> DailySignalSummary:
    """Build a daily summary from signal log entries.

    Args:
        entries: Signal log entries for the day.
        date: The date being summarized.
        hit_rate_resolver: Optional callable that resolves hit rates for signals.
    """
    summary = DailySignalSummary(date=date.strftime("%Y-%m-%d"))
    if not entries:
        return summary

    summary.total_signals = len(entries)

    dir_counts = Counter(e.direction for e in entries)
    summary.by_direction = dict(dir_counts)

    regime_counts = Counter(e.regime for e in entries)
    summary.by_regime = dict(regime_counts)

    strat_counts = Counter(e.strategy_source or "unknown" for e in entries)
    summary.by_strategy = dict(strat_counts)

    symbol_counts = Counter(e.symbol for e in entries)
    summary.by_symbol = dict(symbol_counts)

    setup_counts = Counter(e.setup_type for e in entries)
    summary.by_setup_type = dict(setup_counts)

    scores = [e.score for e in entries]
    summary.avg_score = sum(scores) / len(scores)

    bucket_counts = Counter(_score_bucket(s) for s in scores)
    summary.score_distribution = dict(bucket_counts)

    if hit_rate_resolver is not None:
        summary.hit_rates = hit_rate_resolver.resolve(entries)

    return summary


def summary_to_markdown(summary: DailySignalSummary) -> str:
    """Render a daily summary as markdown."""
    lines = [
        f"# Daily Signal Report — {summary.date}",
        "",
        f"**Total Signals:** {summary.total_signals}",
        f"**Average Score:** {summary.avg_score:.1f}",
        "",
        "## By Direction",
        "",
    ]
    for k, v in sorted(summary.by_direction.items()):
        lines.append(f"- {k}: {v}")

    lines += ["", "## By Regime", ""]
    for k, v in sorted(summary.by_regime.items()):
        lines.append(f"- {k}: {v}")

    lines += ["", "## By Strategy", ""]
    for k, v in sorted(summary.by_strategy.items()):
        lines.append(f"- {k}: {v}")

    lines += ["", "## By Symbol", ""]
    for k, v in sorted(summary.by_symbol.items()):
        lines.append(f"- {k}: {v}")

    lines += ["", "## By Setup Type", ""]
    for k, v in sorted(summary.by_setup_type.items()):
        lines.append(f"- {k}: {v}")

    lines += ["", "## Score Distribution", ""]
    for k, v in sorted(summary.score_distribution.items()):
        lines.append(f"- {k}: {v}")

    if summary.hit_rates:
        lines += ["", "## Hit Rates", ""]
        for horizon, stats in sorted(summary.hit_rates.items()):
            pct = stats.hit_rate * 100
            lines.append(
                f"- {horizon}: {pct:.1f}% "
                f"({stats.hits}/{stats.hits + stats.misses} resolved, "
                f"{stats.pending} pending)"
            )

    lines.append("")
    return "\n".join(lines)


def summary_to_json(summary: DailySignalSummary) -> str:
    """Serialize a daily summary to JSON."""
    data = asdict(summary)
    # Convert HitRateStats to plain dicts with computed hit_rate
    if "hit_rates" in data:
        for k, v in data["hit_rates"].items():
            resolved = v["hits"] + v["misses"]
            v["hit_rate"] = v["hits"] / resolved if resolved > 0 else 0.0
    return json.dumps(data, indent=2, default=str)


def generate_daily_report(
    signal_logger: SignalFileLogger,
    date: datetime,
    output_dir: str | Path,
    *,
    hit_rate_resolver: HitRateResolver | None = None,
) -> tuple[Path, Path]:
    """Generate daily report in both markdown and JSON formats.

    Returns:
        Tuple of (markdown_path, json_path).
    """
    entries = signal_logger.read_entries(date)
    summary = build_daily_summary(entries, date, hit_rate_resolver=hit_rate_resolver)

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    date_str = date.strftime("%Y-%m-%d")
    md_path = out / f"signal_report_{date_str}.md"
    json_path = out / f"signal_report_{date_str}.json"

    md_path.write_text(summary_to_markdown(summary), encoding="utf-8")
    json_path.write_text(summary_to_json(summary), encoding="utf-8")

    return md_path, json_path


class HitRateResolver:
    """Resolve signal hit rates by checking if price moved in signal direction.

    This is a pluggable interface. Subclass and override ``resolve()``
    to integrate with live price data or backtest results.
    """

    def resolve(self, entries: list[SignalLogEntry]) -> dict[str, HitRateStats]:
        """Return hit-rate stats keyed by horizon label (e.g. '1h', '4h', '24h').

        Default implementation returns empty — override with actual price checks.
        """
        return {}
