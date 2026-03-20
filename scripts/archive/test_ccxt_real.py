"""
Test CCXT krakenfutures class.
"""
import os
import asyncio
import ccxt.async_support as ccxt  # Use async version

# If pytest collects this module, skip by default (unless explicitly enabled).
import sys
if (
    ("PYTEST_CURRENT_TEST" in os.environ or "pytest" in sys.modules)
    and os.getenv("RUN_REAL_EXCHANGE_TESTS", "0").strip() not in ("1", "true", "TRUE", "yes", "YES")
):
    import pytest  # type: ignore
    pytest.skip(
        "Skipping real-exchange CCXT test (set RUN_REAL_EXCHANGE_TESTS=1 to enable)",
        allow_module_level=True,
    )

def _require_env(name: str) -> str:
    v = os.getenv(name)
    if not v or not v.strip():
        raise SystemExit(f"Missing required env var: {name}")
    return v


def _ensure_allowed() -> None:
    """
    Defense-in-depth: never run real-exchange tests unless explicitly enabled.
    """
    if os.getenv("RUN_REAL_EXCHANGE_TESTS", "0").strip() not in ("1", "true", "TRUE", "yes", "YES"):
        raise SystemExit(
            "Refusing to run real-exchange test. Set RUN_REAL_EXCHANGE_TESTS=1 to enable."
        )

async def test_ccxt_futures_direct():
    """Test using ccxt.krakenfutures directly."""
    _ensure_allowed()
    api_key = _require_env("KRAKEN_FUTURES_API_KEY")
    api_secret = _require_env("KRAKEN_FUTURES_API_SECRET")

    print("\n=== Testing CCXT Kraken Futures ===\n")
    
    exchange = ccxt.krakenfutures({
        'apiKey': api_key,
        'secret': api_secret,
        'enableRateLimit': True,
        # 'verbose': True, # Uncomment to see raw requests!
    })
    
    try:
        # 1. Load markets
        print("[1] Loading markets...")
        markets = await exchange.load_markets()
        print(f"   Markets loaded: {len(markets)}")
        
        # Check BTC symbol
        symbol = 'BTC/USD:USD'
        if symbol in markets:
            print(f"   ✅ Found {symbol}")
        
        # 2. Fetch balance (private)
        print("\n[2] Fetching balance...")
        balance = await exchange.fetch_balance()
        print("   ✅ Balance fetched")

        # 3. Order placement is intentionally disabled by default.
        # If you want to test orders, require *additional* explicit gating.
        if os.getenv("RUN_REAL_EXCHANGE_ORDERS", "0").strip() in ("1", "true", "TRUE", "yes", "YES"):
            if os.getenv("CONFIRM_LIVE", "").strip().upper() != "YES":
                raise SystemExit("Refusing to place orders: set CONFIRM_LIVE=YES as well.")

            print("\n[3] Placing safe limit order (explicitly enabled)...")
            price = 50000.0  # Far from typical BTC price; adjust for current market
            amount = 1.0     # 1 contract

            try:
                order = await exchange.create_order(
                    symbol=symbol,
                    type='limit',
                    side='buy',
                    amount=amount,
                    price=price,
                    params={'cliOrdId': 'ccxt_test_001'}
                )
                print(f"   ✅ Order placed! ID: {order['id']}")

                # Cancel it
                print("   Cancelling...")
                await exchange.cancel_order(order['id'], symbol)
                print("   ✅ Order cancelled")
            except Exception as e:
                print(f"   ❌ Order creation failed: {e}")

    except Exception as e:
        print(f"   ❌ Error: {e}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(test_ccxt_futures_direct())
