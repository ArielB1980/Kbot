#!/usr/bin/env python3
"""Check TP order coverage for all open positions."""
import sys
import os
import asyncio
import json
from decimal import Decimal
from typing import List, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.storage.db import get_db
from src.storage.repository import PositionModel, get_active_positions
from src.domain.models import Position, Side
from src.data.kraken_client import KrakenClient
from src.config.config import load_config


def collect_tp_orders(symbol_orders: List[Dict], position_side: Side) -> List[Dict]:
    """Collect TP-like reduce-only orders (same logic as add_tp_to_positions.py)."""
    open_tp_orders = [
        o for o in symbol_orders
        if o.get('reduceOnly', False) and 
        o.get('type', '').lower() in ('take_profit', 'take-profit', 'limit') and
        ((position_side == Side.LONG and o.get('side', '').lower() == 'sell') or
         (position_side == Side.SHORT and o.get('side', '').lower() == 'buy'))
    ]
    
    explicit_tp_orders = [
        o for o in open_tp_orders
        if o.get('type', '').lower() in ('take_profit', 'take-profit')
    ]
    
    if explicit_tp_orders:
        return explicit_tp_orders
    return open_tp_orders


def collect_sl_orders(symbol_orders: List[Dict]) -> List[Dict]:
    """Collect SL-like reduce-only orders."""
    return [
        o for o in symbol_orders
        if o.get('reduceOnly', False) and 
        ('stop' in str(o.get('type', '')).lower() or o.get('stopPrice') is not None)
    ]


def determine_status(
    db_pos: PositionModel,
    exchange_pos: Optional[Dict],
    exchange_sl_count: int,
    exchange_tp_count: int,
    exchange_tp_prices: List[Decimal],
    db_tp_prices: List[Decimal],
    position_side: Side
) -> str:
    """Determine status code for position."""
    if not exchange_pos or exchange_pos.get('size', 0) == 0:
        return "NO_EXCHANGE_POSITION"
    
    if not db_pos.is_protected:
        return "UNPROTECTED"
    
    if exchange_tp_count == 0:
        return "MISSING_TP"
    
    # Check if TP prices match (within 0.5% tolerance for display purposes)
    if db_tp_prices and exchange_tp_prices:
        tolerance = Decimal("0.005")  # 0.5% for status check
        matched = 0
        for db_price in db_tp_prices[:exchange_tp_count]:
            for ex_price in exchange_tp_prices:
                if abs(db_price - ex_price) / db_price <= tolerance:
                    matched += 1
                    break
        if matched < min(len(db_tp_prices), exchange_tp_count):
            return "MISMATCH_TP"
    
    return "OK"


async def check_coverage():
    """Check TP coverage for all positions."""
    # Load config
    config = load_config()
    
    # Initialize client
    client = KrakenClient(
        api_key=config.exchange.api_key,
        api_secret=config.exchange.api_secret,
        futures_api_key=config.exchange.futures_api_key,
        futures_api_secret=config.exchange.futures_api_secret,
        use_testnet=config.exchange.use_testnet
    )
    await client.initialize()
    
    # Load DB positions
    db_positions = await asyncio.to_thread(get_active_positions)
    
    # Fetch exchange state
    exchange_positions = await client.get_all_futures_positions()
    exchange_orders = await client.get_futures_open_orders()
    
    # Build lookup dicts
    ex_pos_by_symbol = {p.get('symbol'): p for p in exchange_positions if p.get('size', 0) != 0}
    orders_by_symbol: Dict[str, List[Dict]] = {}
    for order in exchange_orders:
        sym = order.get('symbol')
        if sym:
            if sym not in orders_by_symbol:
                orders_by_symbol[sym] = []
            orders_by_symbol[sym].append(order)
    
    # Get DB models for detailed fields - extract data while in session
    db = get_db()
    db_data = {}
    with db.get_session() as session:
        db_models = session.query(PositionModel).all()
        for pm in db_models:
            # Extract all needed data while in session
            import json
            db_data[pm.symbol] = {
                'is_protected': pm.is_protected,
                'initial_stop_price': pm.initial_stop_price,
                'stop_loss_order_id': pm.stop_loss_order_id,
                'tp_order_ids': json.loads(pm.tp_order_ids) if pm.tp_order_ids else [],
                'tp1_price': pm.tp1_price,
                'tp2_price': pm.tp2_price,
                'final_target_price': pm.final_target_price,
            }
    
    # Process each position
    print(f"\n=== TP Coverage Report ===")
    print(f"Total positions in DB: {len(db_positions)}\n")
    
    status_counts = {
        "OK": 0,
        "MISSING_TP": 0,
        "MISMATCH_TP": 0,
        "UNPROTECTED": 0,
        "NO_EXCHANGE_POSITION": 0
    }
    
    for db_pos in db_positions:
        symbol = db_pos.symbol
        db_info = db_data.get(symbol)
        if not db_info:
            continue
        
        exchange_pos = ex_pos_by_symbol.get(symbol)
        
        # Collect orders for this symbol and normalized formats
        symbol_orders = orders_by_symbol.get(symbol, [])
        # Also check normalized formats (PF_XBTUSD <-> BTC/USD:USD)
        normalized_symbols = []
        if symbol.startswith("PF_"):
            # PF_XBTUSD -> BTC/USD:USD
            base = symbol[3:-3]  # Remove "PF_" and "USD"
            if base == "XBT":
                base = "BTC"
            normalized_symbols.append(f"{base}/USD:USD")
        elif "/" in symbol and ":" in symbol:
            # BTC/USD:USD -> PF_XBTUSD
            base = symbol.split("/")[0]
            if base == "BTC":
                base = "XBT"
            normalized_symbols.append(f"PF_{base}USD")
        
        # Collect orders from all symbol formats
        all_orders = list(symbol_orders)
        for norm_symbol in normalized_symbols:
            all_orders.extend(orders_by_symbol.get(norm_symbol, []))
        
        # Collect orders
        sl_orders = collect_sl_orders(all_orders)
        tp_orders = collect_tp_orders(all_orders, db_pos.side)
        
        # Extract TP prices from exchange
        exchange_tp_prices = []
        for order in tp_orders:
            price = order.get('stopPrice') or order.get('price')
            if price:
                try:
                    exchange_tp_prices.append(Decimal(str(price)))
                except (ValueError, TypeError):
                    pass
        exchange_tp_prices.sort()
        
        # Extract TP prices from DB
        db_tp_prices = []
        if db_info['tp1_price']:
            db_tp_prices.append(Decimal(str(db_info['tp1_price'])))
        if db_info['tp2_price']:
            db_tp_prices.append(Decimal(str(db_info['tp2_price'])))
        if db_info['final_target_price']:
            db_tp_prices.append(Decimal(str(db_info['final_target_price'])))
        
        # Get tp_order_ids (already parsed)
        tp_order_ids = db_info['tp_order_ids']
        
        # Create a mock PositionModel-like object for determine_status
        class MockDBPos:
            def __init__(self, data):
                self.is_protected = data['is_protected']
        
        mock_db_pos = MockDBPos(db_info)
        
        # Determine status
        status = determine_status(
            mock_db_pos,
            exchange_pos,
            len(sl_orders),
            len(tp_orders),
            exchange_tp_prices,
            db_tp_prices,
            db_pos.side
        )
        status_counts[status] = status_counts.get(status, 0) + 1
        
        # Print details
        print(f"\n{symbol} ({db_pos.side.value})")
        print(f"  Size: {db_pos.size}")
        print(f"  Status: {status}")
        print(f"  is_protected: {db_info['is_protected']}")
        print(f"  DB - initial_stop_price: {db_info['initial_stop_price']}")
        print(f"  DB - stop_loss_order_id: {db_info['stop_loss_order_id']}")
        print(f"  DB - tp_order_ids: {db_info['tp_order_ids']}")
        print(f"  DB - tp1_price: {db_info['tp1_price']}")
        print(f"  DB - tp2_price: {db_info['tp2_price']}")
        print(f"  DB - final_target_price: {db_info['final_target_price']}")
        print(f"  Exchange - SL orders: {len(sl_orders)}")
        print(f"  Exchange - TP orders: {len(tp_orders)}")
        print(f"  Exchange - TP prices: {[str(p) for p in exchange_tp_prices]}")
    
    # Summary
    print(f"\n=== Summary ===")
    for status, count in status_counts.items():
        if count > 0:
            print(f"{status}: {count}")
    
    # Close client
    try:
        await client.close()
    except Exception as e:
        print(f"Warning: Failed to close client: {e}")


def main():
    """CLI entry point."""
    asyncio.run(check_coverage())


if __name__ == "__main__":
    main()
