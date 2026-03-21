import os
import sys
import asyncio
import ccxt.async_support as ccxt

async def check_pair(symbol):
    print(f"Checking {symbol} on Kraken...")
    
    exchange = ccxt.kraken({
        'apiKey': os.getenv('KRAKEN_API_KEY'),
        'secret': os.getenv('KRAKEN_API_SECRET'),
    })
    
    try:
        # 1. Load Markets first to see what CCXT sees
        markets = await exchange.load_markets()
        if symbol in markets:
            print(f"✅ Symbol '{symbol}' found in loaded markets.")
            print(f"   ID: {markets[symbol]['id']}")
            print(f"   Active: {markets[symbol].get('active', 'Unknown')}")
        else:
            print(f"❌ Symbol '{symbol}' NOT found in loaded markets.")
            # Try to find close matches
            matches = [m for m in markets.keys() if 'AGLD' in m]
            print(f"   Did you mean: {matches}")
            
        # 2. Try to fetch ticker specifically
        try:
            ticker = await exchange.fetch_ticker(symbol)
            print(f"✅ Ticker fetch successful: {ticker['last']}")
        except Exception as e:
            print(f"❌ Ticker fetch failed: {e}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    symbol = sys.argv[1] if len(sys.argv) > 1 else "AGLD/USD"
    asyncio.run(check_pair(symbol))
