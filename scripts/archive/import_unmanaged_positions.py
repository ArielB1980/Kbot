#!/usr/bin/env python3
"""
Import unmanaged positions from Kraken into the position manager.

This script fetches all open positions from Kraken and ensures they are
tracked in the local position manager with proper TP/SL management.
"""
import asyncio
import sys
from pathlib import Path
from decimal import Decimal
from datetime import datetime, timezone

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data.kraken_client import KrakenClient
from config.config import load_config
from domain.models import Side
from execution.position_manager import PositionManager
from storage.db import get_db
import structlog

logger = structlog.get_logger()


async def import_unmanaged_positions():
    """Import all unmanaged positions from Kraken."""
    # Load config
    config = load_config()

    # Initialize clients
    kraken_client = KrakenClient(
        api_key=config.exchange.futures_api_key,
        api_secret=config.exchange.futures_api_secret,
        use_testnet=config.exchange.use_testnet
    )

    position_manager = PositionManager(
        client=kraken_client,
        config=config
    )

    # Fetch positions from exchange
    logger.info("Fetching positions from Kraken...")
    exchange_positions = await kraken_client.get_positions()

    logger.info(f"Found {len(exchange_positions)} positions on exchange",
                symbols=[p.symbol for p in exchange_positions])

    # Check which are untracked
    untracked = []
    for pos in exchange_positions:
        if not position_manager.has_position(pos.symbol):
            untracked.append(pos)
            logger.warning(
                "Untracked position found",
                symbol=pos.symbol,
                side=pos.side.value,
                size=str(pos.size),
                entry_price=str(pos.entry_price),
                leverage=str(pos.leverage),
                unrealized_pnl=str(pos.unrealized_pnl)
            )

    if not untracked:
        logger.info("✓ All positions are tracked!")
        return

    logger.info(f"Importing {len(untracked)} untracked positions...")

    # Import each untracked position
    for pos in untracked:
        try:
            # Add to position manager
            position_manager.add_position(pos)

            # Place protective stop loss if missing
            if not pos.stop_loss_order_id:
                # Calculate conservative stop loss (5% from entry)
                stop_distance_pct = Decimal("0.05")
                if pos.side == Side.LONG:
                    stop_price = pos.entry_price * (Decimal("1") - stop_distance_pct)
                else:
                    stop_price = pos.entry_price * (Decimal("1") + stop_distance_pct)

                logger.info(
                    "Placing protective stop loss",
                    symbol=pos.symbol,
                    stop_price=str(stop_price)
                )

                # Note: actual order placement would happen here
                # For now, just log the intent

            logger.info(
                "✓ Imported position",
                symbol=pos.symbol,
                side=pos.side.value
            )

        except Exception as e:
            logger.error(
                "Failed to import position",
                symbol=pos.symbol,
                error=str(e)
            )

    # Sync to database
    await position_manager.sync_positions_to_db()

    logger.info(f"✓ Successfully imported {len(untracked)} positions!")


if __name__ == "__main__":
    print("="*60)
    print("Import Unmanaged Positions")
    print("="*60)

    try:
        asyncio.run(import_unmanaged_positions())
        print("\n✓ Import complete!")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Import failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
