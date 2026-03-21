#!/usr/bin/env python3
"""
Sync positions from Kraken to local database.

This script fetches all open positions from Kraken and ensures they are
stored in the database for tracking and management.
"""
import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data.kraken_client import KrakenClient
from config.config import load_config
from storage.repository import save_position
import structlog

logger = structlog.get_logger()


async def sync_positions():
    """Fetch positions from Kraken and save to database."""
    try:
        # Load config
        config = load_config()

        # Initialize Kraken client
        client = KrakenClient(
            api_key=config.exchange.futures_api_key,
            api_secret=config.exchange.futures_api_secret,
            use_testnet=config.exchange.use_testnet
        )

        # Fetch positions from exchange
        logger.info("Fetching positions from Kraken...")
        positions = await client.get_positions()

        logger.info(
            "Fetched positions from exchange",
            count=len(positions),
            symbols=[p.symbol for p in positions]
        )

        if not positions:
            logger.info("No positions found on exchange")
            return

        # Save each position to database
        for pos in positions:
            try:
                logger.info(
                    "Saving position to database",
                    symbol=pos.symbol,
                    side=pos.side.value,
                    size=str(pos.size),
                    entry_price=str(pos.entry_price),
                    leverage=str(pos.leverage),
                    unrealized_pnl=str(pos.unrealized_pnl)
                )

                save_position(pos)

                logger.info("✓ Saved position", symbol=pos.symbol)

            except Exception as e:
                logger.error(
                    "Failed to save position",
                    symbol=pos.symbol,
                    error=str(e)
                )

        logger.info(f"✓ Successfully synced {len(positions)} positions!")

    except Exception as e:
        logger.error("Sync failed", error=str(e))
        raise


if __name__ == "__main__":
    print("="*60)
    print("Sync Positions from Kraken")
    print("="*60)

    try:
        asyncio.run(sync_positions())
        print("\n✓ Sync complete!")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Sync failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
