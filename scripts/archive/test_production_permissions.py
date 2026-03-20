#!/usr/bin/env python3
"""
Test if database permissions are working after production upgrade.
"""
import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.storage.db import get_db, Base
from src.storage.repository import (
    CandleModel, TradeModel, PositionModel, SystemEventModel, AccountStateModel
)
from sqlalchemy import inspect

def test_permissions():
    """Test if we can create tables now."""
    print("=" * 60)
    print("Testing Database Permissions After Production Upgrade")
    print("=" * 60)
    
    try:
        # Import models to register them
        import src.storage.repository
        
        print("\n1. Connecting to database...")
        db = get_db()
        print(f"   ✅ Connected: {db.database_url.split('@')[0]}@***")
        
        print("\n2. Checking registered models...")
        tables = list(Base.metadata.tables.keys())
        print(f"   ✅ Found {len(tables)} registered tables: {tables}")
        
        print("\n3. Attempting to create tables...")
        try:
            db.create_all()
            print("   ✅ create_all() completed without errors")
        except Exception as e:
            error_str = str(e).lower()
            if "permission denied" in error_str or "insufficientprivilege" in error_str:
                print(f"   ❌ Permission denied: {e}")
                return False
            else:
                print(f"   ⚠️  Warning: {e}")
        
        print("\n4. Verifying tables exist...")
        inspector = inspect(db.engine)
        existing_tables = inspector.get_table_names()
        print(f"   Found {len(existing_tables)} tables in database")
        
        required_tables = ["candles", "trades", "positions", "system_events", "account_state"]
        missing = [t for t in required_tables if t not in existing_tables]
        
        if missing:
            print(f"   ❌ Missing tables: {missing}")
            print("\n   Tables still need to be created.")
            print("   You may need admin (doadmin) credentials to grant permissions.")
            return False
        else:
            print(f"   ✅ All required tables exist: {required_tables}")
            print("\n" + "=" * 60)
            print("✅ DATABASE IS READY!")
            print("=" * 60)
            print("\nAll tables are created. Your app should work correctly now.")
            return True
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_permissions()
    sys.exit(0 if success else 1)
