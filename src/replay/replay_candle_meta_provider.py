"""
Replay candle metadata provider.

Provides a lightweight mock ``CandleManager``-like object whose
``get_candles()`` returns synthetic candle lists with the correct
**count** and **newest timestamp** -- the only two things
``check_candle_sanity()`` inspects.

All data comes from the recorded ``last_candle_ts_json`` and
``candle_count_json`` columns in ``market_snapshots``.
"""
from __future__ import annotations

import bisect
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.domain.models import Candle
from src.recording.models import MarketSnapshot


# ---------------------------------------------------------------------------
# Mock candle / candle manager
# ---------------------------------------------------------------------------

def _make_mock_candle(symbol: str, tf: str, ts: datetime) -> Candle:
    """Build a minimal valid ``Candle`` with the given timestamp."""
    return Candle(
        timestamp=ts,
        symbol=symbol,
        timeframe=tf,
        open=Decimal("1"),
        high=Decimal("1"),
        low=Decimal("1"),
        close=Decimal("1"),
        volume=Decimal("1"),
    )


class _ReplayCandleManager:
    """Duck-typed stand-in for the real ``CandleManager``.

    Only ``get_candles(symbol, tf)`` is implemented because that is
    the sole method ``check_candle_sanity()`` calls.
    """

    def __init__(
        self,
        symbol: str,
        last_ts_map: Dict[str, Optional[datetime]],
        count_map: Dict[str, int],
    ) -> None:
        self._symbol = symbol
        self._last_ts = last_ts_map     # tf -> newest candle ts
        self._counts = count_map        # tf -> candle count

    def get_candles(self, symbol: str, tf: str) -> List[Candle]:
        """Return a list of mock candles with correct count + newest ts.

        ``candles[-1].timestamp`` is the recorded latest candle timestamp.
        The remaining candles have arbitrary (but valid) earlier timestamps.
        """
        count = self._counts.get(tf, 0)
        newest_ts = self._last_ts.get(tf)

        if count <= 0 or newest_ts is None:
            return []

        # Build list: newest is last element (candles[-1])
        candles: List[Candle] = []
        for i in range(count):
            candles.append(_make_mock_candle(symbol, tf, newest_ts))
        return candles


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------

class ReplayCandleMetaProvider:
    """Loads candle metadata from recorded snapshots and provides mock
    ``CandleManager`` objects for ``check_candle_sanity()`` replay.
    """

    def __init__(self, db_url: str) -> None:
        self._engine = create_engine(db_url, pool_pre_ping=True)
        # symbol -> sorted list of _CandleMeta
        self._cache: Dict[str, List[_CandleMeta]] = {}

    def preload(
        self,
        symbols: List[str],
        start: datetime,
        end: datetime,
    ) -> None:
        """Bulk-load candle metadata from snapshots."""
        SessionFactory = sessionmaker(bind=self._engine)
        session = SessionFactory()
        try:
            rows = (
                session.query(MarketSnapshot)
                .filter(
                    MarketSnapshot.symbol.in_(symbols),
                    MarketSnapshot.ts_utc >= start,
                    MarketSnapshot.ts_utc <= end,
                )
                .order_by(MarketSnapshot.symbol, MarketSnapshot.ts_utc)
                .all()
            )
            # Extract all ORM attributes into plain cache entries while
            # the session is still open to avoid DetachedInstanceError.
            self._cache.clear()
            for row in rows:
                sym = row.symbol
                if sym not in self._cache:
                    self._cache[sym] = []

                ts_float = row.ts_utc.timestamp() if row.ts_utc.tzinfo else row.ts_utc.replace(tzinfo=timezone.utc).timestamp()

                # Parse JSON blobs
                try:
                    raw_ts = json.loads(row.last_candle_ts_json) if row.last_candle_ts_json else {}
                except (json.JSONDecodeError, TypeError):
                    raw_ts = {}

                try:
                    raw_counts = json.loads(row.candle_count_json) if row.candle_count_json else {}
                except (json.JSONDecodeError, TypeError):
                    raw_counts = {}

                # Convert ISO strings -> datetime
                last_ts: Dict[str, Optional[datetime]] = {}
                for tf, iso_str in raw_ts.items():
                    if iso_str:
                        try:
                            dt = datetime.fromisoformat(iso_str)
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            last_ts[tf] = dt
                        except (ValueError, TypeError):
                            last_ts[tf] = None
                    else:
                        last_ts[tf] = None

                counts: Dict[str, int] = {}
                for tf, c in raw_counts.items():
                    counts[tf] = int(c) if c else 0

                self._cache[sym].append(_CandleMeta(
                    ts=ts_float,
                    last_candle_ts=last_ts,
                    candle_counts=counts,
                ))
        finally:
            session.close()

    def get_mock_candle_manager(
        self,
        symbol: str,
        ts: datetime,
    ) -> _ReplayCandleManager:
        """Return a ``CandleManager``-compatible object for *symbol* at *ts*.

        Uses step-function hold: picks the latest snapshot ``<= ts``.
        """
        entries = self._cache.get(symbol, [])
        if not entries:
            return _ReplayCandleManager(symbol, {}, {})

        ts_f = ts.timestamp() if ts.tzinfo else ts.replace(tzinfo=timezone.utc).timestamp()
        idx = bisect.bisect_right([e.ts for e in entries], ts_f) - 1
        if idx < 0:
            return _ReplayCandleManager(symbol, {}, {})

        entry = entries[idx]
        return _ReplayCandleManager(symbol, entry.last_candle_ts, entry.candle_counts)


# ---------------------------------------------------------------------------
# Internal data
# ---------------------------------------------------------------------------

@dataclass
class _CandleMeta:
    ts: float                                       # snapshot ts as float
    last_candle_ts: Dict[str, Optional[datetime]]   # tf -> newest candle ts
    candle_counts: Dict[str, int]                   # tf -> count
