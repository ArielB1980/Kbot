#!/usr/bin/env python3
"""
Recover SL order IDs from exchange and update database.

Matches open SL orders on exchange to positions and updates stop_loss_order_id.
"""
import sys
import os
import asyncio
from decimal import Decimal
from typing import List, Dict, Optional
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.storage.db import get_db
from src.storage.repository import PositionModel, save_position
from src.domain.models import Position, Side
from src.data.kraken_client import KrakenClient
from src.config.config import load_config
from src.monitoring.logger import get_logger

logger = get_logger(__name__)


def normalize_symbol_for_comparison(symbol: str) -> str:
    """
    Normalize symbol for comparison.
    Converts PF_BLURUSD <-> BLUR/USD:USD
    """
    if symbol.startswith("PF_"):
        # PF_BLURUSD -> BLUR/USD:USD
        base = symbol[3:-3]  # Remove "PF_" and "USD"
        if base == "XBT":
            base = "BTC"
        return f"{base}/USD:USD"
    elif "/" in symbol and ":" in symbol:
        # BLUR/USD:USD -> PF_BLURUSD
        base = symbol.split("/")[0]
        if base == "BTC":
            base = "XBT"
        return f"PF_{base}USD"
    return symbol


def detect_sl_order(orders: List[Dict], position_symbol: str) -> Optional[Dict]:
    """
    Detect stop loss order from open orders for a position.
    
    Returns first matching reduce-only stop order, or None.
    """
    normalized_pos_symbol = normalize_symbol_for_comparison(position_symbol)
    
    for order in orders:
        order_symbol = order.get('symbol')
        if not order_symbol:
            continue
        
        # Try both formats
        if order_symbol != position_symbol and order_symbol != normalized_pos_symbol:
            # Also try normalizing the order symbol
            normalized_order_symbol = normalize_symbol_for_comparison(order_symbol)
            if normalized_order_symbol != position_symbol and normalized_order_symbol != normalized_pos_symbol:
                continue
        
        is_reduce_only = order.get('reduceOnly', False)
        order_type = str(order.get('type', '')).lower()
        has_stop_price = order.get('stopPrice') is not None
        is_stop_type = any(stop_term in order_type for stop_term in ['stop', 'stop-loss', 'stop_loss', 'stp'])
        
        if is_reduce_only and (has_stop_price or is_stop_type):
            return order
    
    return None


async def recover_sl_order_ids():
    """
    Recover SL order IDs from exchange and update database.
    """
    logger.info("Starting SL order ID recovery")
    
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
    
    # 1. Load all positions
    db = get_db()
    positions_data = []
    with db.get_session() as session:
        all_positions = session.query(PositionModel).all()
        
        # Extract all needed data while in session
        for pm in all_positions:
            import json
            positions_data.append({
                'symbol': pm.symbol,
                'side': pm.side,
                'size': pm.size,
                'size_notional': pm.size_notional,
                'entry_price': pm.entry_price,
                'current_mark_price': pm.current_mark_price,
                'liquidation_price': pm.liquidation_price,
                'unrealized_pnl': pm.unrealized_pnl,
                'leverage': pm.leverage,
                'margin_used': pm.margin_used,
                'opened_at': pm.opened_at,
                'initial_stop_price': pm.initial_stop_price,
                'stop_loss_order_id': pm.stop_loss_order_id,
                'tp_order_ids': json.loads(pm.tp_order_ids) if pm.tp_order_ids else [],
            })
    
    if not positions_data:
        logger.info("No positions to process")
        return
    
    logger.info("Found positions to process", count=len(positions_data))
    
    # 2. Fetch open orders once
    try:
        open_orders = await client.get_futures_open_orders()
        logger.info("Fetched open orders", total_orders=len(open_orders))
    except Exception as e:
        logger.error("Failed to fetch open orders", error=str(e))
        return
    
    # 3. Process each position
    recovered_count = 0
    updated_count = 0
    errors = []
    
    for pos_data_dict in positions_data:
        symbol = pos_data_dict['symbol']
        try:
            # Convert to Position object
            pos = Position(
                symbol=pos_data_dict['symbol'],
                side=Side(pos_data_dict['side']),
                size=Decimal(str(pos_data_dict['size'])),
                size_notional=Decimal(str(pos_data_dict['size_notional'])),
                entry_price=Decimal(str(pos_data_dict['entry_price'])),
                current_mark_price=Decimal(str(pos_data_dict['current_mark_price'])),
                liquidation_price=Decimal(str(pos_data_dict['liquidation_price'])),
                unrealized_pnl=Decimal(str(pos_data_dict['unrealized_pnl'])),
                leverage=Decimal(str(pos_data_dict['leverage'])),
                margin_used=Decimal(str(pos_data_dict['margin_used'])),
                opened_at=pos_data_dict['opened_at'].replace(tzinfo=timezone.utc) if isinstance(pos_data_dict['opened_at'], datetime) else datetime.now(timezone.utc),
                initial_stop_price=Decimal(str(pos_data_dict['initial_stop_price'])) if pos_data_dict['initial_stop_price'] else None,
                stop_loss_order_id=pos_data_dict['stop_loss_order_id'],
                tp_order_ids=pos_data_dict['tp_order_ids'],
            )
            
            # Check if we need to recover order ID or update protection status
            current_order_id = pos.stop_loss_order_id
            needs_recovery = (
                current_order_id is None or 
                str(current_order_id).startswith('unknown')
            )
            
            # Also update protection status if position has valid SL but is_protected is False
            needs_protection_update = (
                pos.initial_stop_price is not None and
                current_order_id is not None and
                not str(current_order_id).startswith('unknown') and
                not pos.is_protected
            )
            
            if not needs_recovery and not needs_protection_update:
                continue
            
            # Try to find SL order on exchange
            sl_order = detect_sl_order(open_orders, symbol)
            
            if sl_order:
                order_id = sl_order.get('id')
                stop_price = sl_order.get('stopPrice') or sl_order.get('price')
                
                if order_id:
                    # Update position with recovered order ID (if needed)
                    if needs_recovery:
                        pos.stop_loss_order_id = order_id
                        if stop_price and not pos.initial_stop_price:
                            pos.initial_stop_price = Decimal(str(stop_price))
                        recovered_count += 1
                        logger.info(
                            "Recovered SL order ID",
                            symbol=symbol,
                            order_id=order_id,
                            sl_price=str(pos.initial_stop_price)
                        )
                    
                    # Mark as protected (always update if we found the order)
                    pos.is_protected = (pos.initial_stop_price is not None and pos.stop_loss_order_id is not None and not str(pos.stop_loss_order_id).startswith('unknown'))
                    pos.protection_reason = None if pos.is_protected else "SL_ORDER_MISSING"
                    
                    save_position(pos)
                    if needs_protection_update:
                        updated_count += 1
                        logger.info(
                            "Updated protection status",
                            symbol=symbol,
                            is_protected=pos.is_protected,
                            order_id=pos.stop_loss_order_id
                        )
                else:
                    logger.warning("SL order found but no order ID", symbol=symbol)
            else:
                # No SL order found - mark as unprotected if we have price but no order
                if pos.initial_stop_price:
                    pos.is_protected = False
                    pos.protection_reason = "SL_ORDER_MISSING"
                    save_position(pos)
                    updated_count += 1
                    logger.warning(
                        "No SL order found on exchange",
                        symbol=symbol,
                        has_sl_price=bool(pos.initial_stop_price)
                    )
        
        except Exception as e:
            logger.error("Failed to process position", symbol=symbol, error=str(e))
            errors.append({'symbol': symbol, 'error': str(e)})
    
    # 4. Summary report
    logger.info(
        "Recovery complete",
        total=len(positions_data),
        recovered=recovered_count,
        updated=updated_count,
        errors=len(errors)
    )
    
    if errors:
        logger.error("Errors during recovery", error_count=len(errors))
        for err in errors[:10]:
            logger.error("Recovery error", symbol=err['symbol'], error=err['error'])
    
    # Close client resources
    try:
        await client.close()
    except Exception as e:
        logger.warning("Failed to close client", error=str(e))


if __name__ == "__main__":
    asyncio.run(recover_sl_order_ids())
