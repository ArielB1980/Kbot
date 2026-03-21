#!/usr/bin/env python3
"""
Simple script to sync positions from Kraken to database.
Uses direct database operations - no complex dependencies.
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
from storage.db import get_db
from domain.models import Side
import structlog

logger = structlog.get_logger()


async def sync_positions():
    """Fetch positions from Kraken and save to database."""
    # Load config
    config = load_config()

    # Initialize Kraken client
    client = KrakenClient(
        api_key=config.exchange.futures_api_key,
        api_secret=config.exchange.futures_api_secret,
        use_testnet=config.exchange.use_testnet
    )

    # Initialize client
    await client.initialize()

    try:
        # Fetch positions from Kraken API
        print("Fetching positions from Kraken...")
        raw_positions = await client.get_all_futures_positions()

        print(f"Found {len(raw_positions)} positions on Kraken")

        if not raw_positions:
            print("No positions to sync")
            return

        # Get database connection
        db = get_db()

        # Save each position to database
        with db.get_session() as session:
            for pos_data in raw_positions:
                symbol = pos_data['symbol']
                size = pos_data['size']
                entry_price = pos_data['entry_price']
                liquidation_price = pos_data['liquidation_price']
                unrealized_pnl = pos_data['unrealized_pnl']
                side_str = pos_data['side']

                # Determine side
                side = Side.LONG if side_str.lower() == 'long' else Side.SHORT

                # Get current mark price
                try:
                    current_price = await client.get_futures_mark_price(symbol)
                except:
                    current_price = entry_price  # Fallback

                # Calculate notional size (approximate)
                size_notional = size * current_price

                # Calculate margin used (approximate)
                # For futures: margin = notional / leverage
                # We'll estimate leverage from liquidation distance
                leverage = Decimal("1.0")  # Default

                print(f"\nSyncing position: {symbol}")
                print(f"  Side: {side.value}")
                print(f"  Size: {size}")
                print(f"  Entry: {entry_price}")
                print(f"  Current: {current_price}")
                print(f"  PnL: {unrealized_pnl}")

                # Insert or update position in database
                from sqlalchemy import text
                session.execute(text("""
                    INSERT INTO positions (
                        symbol, side, size, size_notional, entry_price,
                        current_mark_price, liquidation_price, unrealized_pnl,
                        leverage, margin_used, opened_at, updated_at
                    ) VALUES (
                        :symbol, :side, :size, :size_notional, :entry_price,
                        :current_price, :liquidation_price, :unrealized_pnl,
                        :leverage, :margin_used, :opened_at, :updated_at
                    )
                    ON CONFLICT (symbol) DO UPDATE SET
                        size = :size,
                        size_notional = :size_notional,
                        current_mark_price = :current_price,
                        liquidation_price = :liquidation_price,
                        unrealized_pnl = :unrealized_pnl,
                        updated_at = :updated_at
                """), {
                    'symbol': symbol,
                    'side': side.value,
                    'size': float(size),
                    'size_notional': float(size_notional),
                    'entry_price': float(entry_price),
                    'current_price': float(current_price),
                    'liquidation_price': float(liquidation_price),
                    'unrealized_pnl': float(unrealized_pnl),
                    'leverage': float(leverage),
                    'margin_used': float(size_notional / leverage),
                    'opened_at': datetime.now(timezone.utc),
                    'updated_at': datetime.now(timezone.utc)
                })

                print(f"  ✓ Synced to database")

        print(f"\n✓ Successfully synced {len(raw_positions)} positions!")

    finally:
        await client.close()


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
