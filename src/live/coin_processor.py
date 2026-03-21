"""
Coin/market universe operations extracted from LiveTrading.

Functions in this module handle:
- Market symbol filtering (blocklist, fiat exclusion)
- Static tier lookup (deprecated legacy helper)
- Market universe discovery and update

All functions receive the LiveTrading instance as their first argument (``lt``)
to access shared state, following the same delegate pattern used by the other
extracted modules.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, List, Optional

from src.exceptions import OperationalError, DataError
from src.data.fiat_currencies import has_disallowed_base
from src.monitoring.logger import get_logger

if TYPE_CHECKING:
    from src.live.live_trading import LiveTrading

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Market symbol filtering
# ---------------------------------------------------------------------------

def market_symbols(lt: "LiveTrading") -> List[str]:
    """Return list of spot symbols. Handles both list and dict. Excludes blocklist."""
    blocklist = set(
        s.strip().upper()
        for s in getattr(lt.config.exchange, "spot_ohlcv_blocklist", []) or []
    )
    # Also honor assets.blacklist
    blocklist |= set(
        s.strip().upper()
        for s in getattr(lt.config.assets, "blacklist", []) or []
    )
    # Also honor execution entry blocklist for universe filtering
    blocklist |= set(
        s.strip().upper().split(":")[0]
        for s in getattr(
            lt.config.execution, "entry_blocklist_spot_symbols", []
        )
        or []
    )
    blocked_bases = set(
        b.strip().upper()
        for b in getattr(lt.config.execution, "entry_blocklist_bases", [])
        or []
    )

    if isinstance(lt.markets, dict):
        raw = list(lt.markets.keys())
    else:
        raw = list(lt.markets)

    if not blocklist and not blocked_bases:
        return raw

    out: List[str] = []
    for s in raw:
        key = s.strip().upper().split(":")[0] if s else ""
        if not key:
            continue
        if key in blocklist:
            continue
        # Global exclusion: never include fiat/stablecoin-base instruments
        if has_disallowed_base(key):
            continue
        if blocked_bases:
            base = key.split("/")[0].strip() if "/" in key else key
            if base in blocked_bases:
                continue
        out.append(s)
    return out


# ---------------------------------------------------------------------------
# Static tier lookup (deprecated)
# ---------------------------------------------------------------------------

def get_static_tier(lt: "LiveTrading", symbol: str) -> Optional[str]:
    """
    DEPRECATED: Debug-only legacy tier lookup.

    Looks up the symbol in config coin_universe.liquidity_tiers (candidate
    groups, not tier assignments). For authoritative tier classification, use
    ``lt.market_discovery.get_symbol_tier(symbol)``.

    Returns "A", "B", "C", or None.
    """
    if not getattr(lt.config, "coin_universe", None) or not getattr(
        lt.config.coin_universe, "enabled", False
    ):
        return None
    tiers = getattr(lt.config.coin_universe, "liquidity_tiers", None) or {}
    for tier in ("A", "B", "C"):
        if symbol in tiers.get(tier, []):
            return tier
    return None


# ---------------------------------------------------------------------------
# Market universe discovery
# ---------------------------------------------------------------------------

async def update_market_universe(lt: "LiveTrading") -> None:
    """Discover and update trading universe."""
    if not lt.config.exchange.use_market_discovery:
        return

    try:
        logger.info("Executing periodic market discovery...")
        mapping = await lt.market_discovery.discover_markets()

        if not mapping:
            cooldown_min = getattr(
                lt.config.exchange,
                "market_discovery_failure_log_cooldown_minutes",
                60,
            )
            now = datetime.now(timezone.utc)
            should_log = (
                lt._last_discovery_error_log_time is None
                or (now - lt._last_discovery_error_log_time).total_seconds()
                >= cooldown_min * 60
            )
            if should_log:
                logger.critical(
                    "Market discovery empty; using existing universe; "
                    "check spot/futures market fetch (get_spot_markets/get_futures_markets)."
                )
                lt._last_discovery_error_log_time = now
            return

        # Shrink protection: if new universe is <50% of LAST DISCOVERED
        # universe, something is wrong (API issue, temporary outage).
        last_discovered_count = getattr(lt, "_last_discovered_count", 0)
        new_count = len(mapping)
        if (
            last_discovered_count > 10
            and new_count < last_discovered_count * 0.5
        ):
            logger.critical(
                "UNIVERSE_SHRINK_REJECTED: new universe is <50% of last discovery -- likely API issue, keeping old universe",
                last_discovered=last_discovered_count,
                new_count=new_count,
                dropped_pct=f"{(1 - new_count / last_discovered_count) * 100:.0f}%",
            )
            try:
                from src.monitoring.alert_dispatcher import send_alert

                await send_alert(
                    "UNIVERSE_SHRINK",
                    f"Discovery returned {new_count} coins vs {last_discovered_count} last discovery -- rejected",
                    urgent=True,
                )
            except (OperationalError, ImportError, OSError):
                pass
            return

        # Track last successful discovery count
        lt._last_discovered_count = new_count

        # Log added/removed symbols vs current universe
        prev_symbols = set(market_symbols(lt))
        supported = set(mapping.keys())
        dropped = prev_symbols - supported
        added = supported - prev_symbols
        for sym in sorted(dropped):
            logger.warning("SYMBOL_REMOVED", symbol=sym)
        for sym in sorted(added):
            logger.info("SYMBOL_ADDED", symbol=sym)

        # Update internal state (Maintain Spot -> Futures mapping)
        lt.markets = mapping
        lt.futures_adapter.set_spot_to_futures_override(mapping)

        # Update Data Acquisition
        new_spot_symbols = list(mapping.keys())
        new_futures_symbols = list(mapping.values())
        lt.data_acq.update_symbols(new_spot_symbols, new_futures_symbols)

        logger.info("Market universe updated", count=len(lt.markets))

    except (OperationalError, DataError) as e:
        logger.error("Failed to update market universe", error=str(e), error_type=type(e).__name__)
