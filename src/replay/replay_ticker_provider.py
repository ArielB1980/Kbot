"""
Replay ticker provider -- deterministic step-function lookup.

Given a pre-loaded set of ``MarketSnapshot`` rows, ``get_ticker(symbol, ts)``
returns the latest snapshot whose ``ts_utc <= ts`` as a ``FuturesTicker``.
"""
from __future__ import annotations

import bisect
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from src.data.kraken_client import FuturesTicker
from src.recording.models import MarketSnapshot


class ReplayTickerProvider:
    """Step-function ticker provider backed by recorded snapshots."""

    def __init__(self, db_url: str) -> None:
        self._engine = create_engine(db_url, pool_pre_ping=True)
        # symbol -> list of (ts_utc_float, snapshot_row_dict)
        self._cache: Dict[str, List[_CacheEntry]] = {}

    # ------------------------------------------------------------------
    # Preload
    # ------------------------------------------------------------------

    def preload(
        self,
        symbols: List[str],
        start: datetime,
        end: datetime,
    ) -> None:
        """Bulk-load all snapshots in [start, end] for *symbols* into memory."""
        SessionLocal = sessionmaker(bind=self._engine)
        session = SessionLocal()
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
                self._cache[sym].append(_CacheEntry(
                    ts=ts_float,
                    bid=_to_dec(row.futures_bid),
                    ask=_to_dec(row.futures_ask),
                    volume_24h=_to_dec(row.futures_volume_usd_24h),
                    open_interest=_to_dec(row.open_interest_usd),
                    funding_rate=_to_dec(row.funding_rate),
                    error_code=row.error_code,
                ))
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def get_ticker(self, symbol: str, ts: datetime) -> Optional[FuturesTicker]:
        """Return the latest snapshot ``<= ts`` as a ``FuturesTicker``.

        Returns ``None`` if no snapshot exists for *symbol* at or before *ts*,
        or if the snapshot recorded an error (missing ticker data).
        """
        entries = self._cache.get(symbol)
        if not entries:
            return None

        ts_f = ts.timestamp() if ts.tzinfo else ts.replace(tzinfo=timezone.utc).timestamp()
        idx = bisect.bisect_right([e.ts for e in entries], ts_f) - 1
        if idx < 0:
            return None

        entry = entries[idx]
        if entry.error_code and entry.bid is None:
            return None

        bid = entry.bid or Decimal(0)
        ask = entry.ask or Decimal(0)
        vol = entry.volume_24h or Decimal(0)
        oi = entry.open_interest or Decimal(0)

        return FuturesTicker(
            symbol=f"PF_{symbol.split('/')[0].upper()}USD",
            mark_price=(bid + ask) / 2 if (bid and ask) else Decimal(0),
            bid=bid,
            ask=ask,
            volume_24h=vol,
            open_interest=oi,
            funding_rate=entry.funding_rate,
        )

    def get_spot_ticker(self, symbol: str, ts: datetime) -> Optional[Dict[str, Any]]:
        """Return minimal spot-like dict from snapshot (for spot fallback path)."""
        ft = self.get_ticker(symbol, ts)
        if ft is None:
            return None
        return {
            "bid": float(ft.bid),
            "ask": float(ft.ask),
            "quoteVolume": float(ft.volume_24h),
        }

    @property
    def symbols(self) -> List[str]:
        """Return the list of symbols loaded in the cache."""
        return list(self._cache.keys())


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

@dataclass
class _CacheEntry:
    ts: float
    bid: Optional[Decimal]
    ask: Optional[Decimal]
    volume_24h: Optional[Decimal]
    open_interest: Optional[Decimal]
    funding_rate: Optional[Decimal]
    error_code: Optional[str]


def _to_dec(val: Any) -> Optional[Decimal]:
    if val is None:
        return None
    try:
        return Decimal(str(val))
    except (ValueError, TypeError, InvalidOperation):
        return None
