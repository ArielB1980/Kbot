"""Structured signal logging for research reporting.

Captures all generated signals with full metadata to JSON-lines files,
enabling downstream analysis: daily summaries, hit-rate tracking,
and performance attribution.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SignalLogEntry:
    """Flat, serializable record of one generated signal."""

    timestamp: str
    symbol: str
    signal_type: str
    direction: str  # "long" or "short" (derived from signal_type)
    entry_price: float
    stop_loss: float
    take_profit: float | None
    score: float
    score_breakdown: dict[str, float]
    setup_type: str
    regime: str
    higher_tf_bias: str
    adx: float
    atr: float
    ema200_slope: str
    strategy_source: str
    reasoning: str


def _direction_from_signal_type(signal_type: str) -> str:
    """Map signal_type enum value to simple direction."""
    if signal_type in ("long", "exit_short"):
        return "long"
    if signal_type in ("short", "exit_long"):
        return "short"
    return "neutral"


def signal_to_log_entry(
    signal: object,
    *,
    strategy_source: str = "",
) -> SignalLogEntry:
    """Convert a domain Signal to a flat log entry.

    Args:
        signal: A ``src.domain.models.Signal`` instance.
        strategy_source: Name of the originating strategy (for attribution).
    """
    tp = getattr(signal, "take_profit", None)
    signal_type_val = str(getattr(signal, "signal_type", "").value)
    return SignalLogEntry(
        timestamp=getattr(signal, "timestamp", datetime.now(UTC)).isoformat(),
        symbol=str(getattr(signal, "symbol", "")),
        signal_type=signal_type_val,
        direction=_direction_from_signal_type(signal_type_val),
        entry_price=float(getattr(signal, "entry_price", 0)),
        stop_loss=float(getattr(signal, "stop_loss", 0)),
        take_profit=float(tp) if tp is not None else None,
        score=float(getattr(signal, "score", 0)),
        score_breakdown=dict(getattr(signal, "score_breakdown", {})),
        setup_type=str(getattr(signal, "setup_type", "").value),
        regime=str(getattr(signal, "regime", "")),
        higher_tf_bias=str(getattr(signal, "higher_tf_bias", "")),
        adx=float(getattr(signal, "adx", 0)),
        atr=float(getattr(signal, "atr", 0)),
        ema200_slope=str(getattr(signal, "ema200_slope", "")),
        strategy_source=strategy_source,
        reasoning=str(getattr(signal, "reasoning", "")),
    )


class SignalFileLogger:
    """Append-only JSON-lines signal logger.

    Writes one ``SignalLogEntry`` per line to a date-partitioned file:
    ``<base_dir>/signals_YYYY-MM-DD.jsonl``.
    """

    def __init__(self, base_dir: str | Path) -> None:
        self._base = Path(base_dir)
        self._base.mkdir(parents=True, exist_ok=True)

    def _path_for_date(self, dt: datetime) -> Path:
        return self._base / f"signals_{dt.strftime('%Y-%m-%d')}.jsonl"

    def log(self, entry: SignalLogEntry) -> None:
        """Append a signal log entry to today's file."""
        ts = datetime.fromisoformat(entry.timestamp)
        path = self._path_for_date(ts)
        line = json.dumps(asdict(entry), default=str)
        with path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
        logger.debug("Signal logged: %s %s %s", entry.symbol, entry.signal_type, entry.score)

    def log_signal(
        self,
        signal: object,
        *,
        strategy_source: str = "",
    ) -> SignalLogEntry:
        """Convert a domain Signal and log it. Returns the entry."""
        entry = signal_to_log_entry(signal, strategy_source=strategy_source)
        self.log(entry)
        return entry

    def read_entries(self, date: datetime) -> list[SignalLogEntry]:
        """Read all signal entries for a given date."""
        path = self._path_for_date(date)
        if not path.exists():
            return []
        entries: list[SignalLogEntry] = []
        for line in path.read_text(encoding="utf-8").strip().splitlines():
            if not line:
                continue
            d = json.loads(line)
            entries.append(SignalLogEntry(**d))
        return entries
