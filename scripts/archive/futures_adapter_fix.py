# Fix for futures_adapter.py - Lines 155-189
# Replace the old code with this new code

FIXED_CODE = """
        # 1. Fetch instrument metadata to get contract size
        instruments = await self.kraken_client.get_futures_instruments()
        
        # Try to find instrument by symbol - instruments API may return different formats
        # Try: PF_AUDUSD, AUDUSD, AUD/USD:USD, etc.
        instr = None
        symbol_upper = symbol.upper()
        
        # First try exact match
        instr = next((i for i in instruments if i.get('symbol', '').upper() == symbol_upper), None)
        
        if not instr:
            # Try without PF_ prefix
            symbol_no_prefix = symbol_upper.replace('PF_', '')
            instr = next((i for i in instruments if i.get('symbol', '').upper() == symbol_no_prefix), None)
        
        if not instr:
            # Try with /USD:USD format (CCXT unified)
            base = symbol_upper.replace('PF_', '').replace('USD', '')
            if base:
                unified_format = f"{base}/USD:USD"
                instr = next((i for i in instruments if i.get('symbol', '').upper() == unified_format), None)
        
        if not instr:
            # Log available symbols for debugging (first 20 that contain similar base)
            base_part = symbol_upper.replace('PF_', '').replace('USD', '').replace('/', '')[:3]
            similar = [i.get('symbol', '') for i in instruments if base_part in i.get('symbol', '').upper()][:20]
            logger.error(
                "Instrument specs not found",
                requested_symbol=symbol,
                similar_symbols=similar,
                total_instruments=len(instruments),
            )
            raise ValueError(f"Instrument specs for {symbol} not found")
"""
