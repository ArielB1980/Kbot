#!/usr/bin/env python3
"""
Check which symbols have futures contracts available on Kraken.
"""
import sys
import os
import asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv('.env.local')

from src.data.kraken_client import KrakenClient
from src.execution.futures_adapter import FuturesAdapter

async def check_futures_availability():
    """Check which symbols have futures contracts."""
    client = KrakenClient()
    await client.initialize()
    
    # Get all futures tickers
    futures_tickers = await client.get_futures_tickers_bulk()
    
    print(f"Total futures tickers available: {len(futures_tickers)}")
    print()
    
    # Test symbols that are generating signals
    test_symbols = ['ATOM/USD', 'OGN/USD', 'TON/USD', 'ONDO/USD', 'ZETA/USD', 'API3/USD', 'YGG/USD']
    
    adapter = FuturesAdapter(client)
    
    print("Signal Symbols -> Futures Mapping Check:")
    print("-" * 60)
    
    for spot_symbol in test_symbols:
        futures_symbol = adapter.map_spot_to_futures(spot_symbol)
        has_futures = futures_symbol in futures_tickers
        
        status = "✅ AVAILABLE" if has_futures else "❌ NOT AVAILABLE"
        print(f"{spot_symbol:15} -> {futures_symbol:20} {status}")
        
        if not has_futures:
            # Check if similar symbols exist
            similar = [s for s in futures_tickers.keys() if spot_symbol.split('/')[0].upper() in s.upper()][:3]
            if similar:
                print(f"  Similar futures: {similar}")
    
    print()
    print("Sample of available futures tickers:")
    print("-" * 60)
    sample = list(futures_tickers.keys())[:20]
    for ticker in sorted(sample):
        print(f"  {ticker}")

if __name__ == "__main__":
    asyncio.run(check_futures_availability())
