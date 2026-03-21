"""
Script to create database indexes for performance optimization.

Run this once after updating the models to add indexes to existing databases.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.storage.db import get_db
from sqlalchemy import text

def create_indexes():
    """Add optimized indexes to existing tables."""
    db = get_db()
    
    indexes = [
        # Candle indexes
        "CREATE INDEX IF NOT EXISTS idx_candle_lookup ON candles (symbol, timeframe, timestamp);",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_candle_key ON candles (symbol, timeframe, timestamp);",
        
        # Trade indexes
        "CREATE INDEX IF NOT EXISTS idx_trade_symbol_date ON trades (symbol, entered_at);",
        "CREATE INDEX IF NOT EXISTS idx_trade_exit_reason ON trades (exit_reason);",
        "CREATE INDEX IF NOT EXISTS idx_trade_pnl ON trades (net_pnl);",
        
        # Event indexes
        "CREATE INDEX IF NOT EXISTS idx_event_type_time ON system_events (event_type, timestamp);",
        "CREATE INDEX IF NOT EXISTS idx_event_decision ON system_events (decision_id);",
        "CREATE INDEX IF NOT EXISTS idx_event_symbol ON system_events (symbol, timestamp);",
        
        # Account state index
        "CREATE INDEX IF NOT EXISTS idx_account_timestamp ON account_state (timestamp);",
    ]
    
    with db.engine.connect() as conn:
        for idx_sql in indexes:
            try:
                conn.execute(text(idx_sql))
                conn.commit()
                print(f"✓ Created index: {idx_sql.split('idx_')[1].split(' ')[0] if 'idx_' in idx_sql else idx_sql.split('uq_')[1].split(' ')[0]}")
            except Exception as e:
                print(f"✗ Error creating index: {e}")
    
    print("\n✅ Index creation complete!")

if __name__ == "__main__":
    create_indexes()
