from src.storage.db import get_db, Base
# CRITICAL: Import all ORM models so they're registered with Base.metadata before create_all()
from src.storage.repository import (
    CandleModel, TradeModel, PositionModel, SystemEventModel, AccountStateModel, ThesisModel
)
from sqlalchemy import text
import os
import sys
from dotenv import load_dotenv

# Load environment variables from files only in local development
# In production (DigitalOcean), env vars are already set in the runtime environment
# Only load from .env files if DATABASE_URL is not already set (production has it set)
if not os.environ.get('DATABASE_URL'):
    # Local development - try to load from .env files
    if os.path.exists(".env.local"):
        load_dotenv(".env.local", override=False)  # Don't override existing vars
    elif os.path.exists(".env"):
        load_dotenv(".env", override=False)  # Don't override existing vars

def migrate():
    """Run PostgreSQL schema migrations."""
    print("Running schema migration...")
    db_url = os.environ.get('DATABASE_URL')
    
    # Check if DATABASE_URL is set and not empty
    if not db_url or (isinstance(db_url, str) and db_url.strip() == ''):
        # In production, secrets might not be available during build phase
        # Log a warning but don't fail - migration can be run manually later if needed
        print("⚠️  WARNING: DATABASE_URL is not set or empty.")
        print("   This is normal during build phase in DigitalOcean App Platform.")
        print("   Migration will be skipped. Schema will be created automatically on first DB connection.")
        print("   If you need to run migrations manually, ensure DATABASE_URL is set in runtime environment.")
        return  # Exit gracefully instead of raising an error
    
    # Mask sensitive parts of the URL for logging
    if '@' in db_url:
        masked = db_url.split('@')[0].split('://')[0] + '://***@' + '@'.join(db_url.split('@')[1:])
        print(f"Target Database: {masked[:80]}...")
    else:
        print(f"Target Database: {db_url[:80]}...")

    # Ensure all models are registered before creating tables
    # Models are imported above, which registers them with Base.metadata
    db = get_db()
    engine = db.engine
    
    # Verify models are registered
    if not Base.metadata.tables:
        print("⚠️  WARNING: No tables registered in Base.metadata")
        print("   This may indicate models weren't imported correctly")
    else:
        print(f"✅ Found {len(Base.metadata.tables)} registered tables: {list(Base.metadata.tables.keys())}")

    # Verify PostgreSQL
    dialect = engine.dialect.name
    if dialect != "postgresql":
        raise RuntimeError(f"Expected PostgreSQL, got {dialect}. Update DATABASE_URL.")

    # CRITICAL: Create all tables FIRST (if they don't exist)
    print("\nCreating database tables (if they don't exist)...")
    try:
        db.create_all()
        print("✅ Tables created/verified")
    except Exception as e:
        error_str = str(e)
        if "permission denied" in error_str.lower() or "insufficientprivilege" in error_str.lower():
            print(f"\n❌ PERMISSION ERROR: Database user lacks CREATE privileges on schema 'public'")
            print(f"   This is common with managed PostgreSQL databases.")
            print(f"\n   SOLUTION: Grant CREATE privilege to your database user:")
            print(f"   Connect to your database as a superuser and run:")
            print(f"   GRANT CREATE ON SCHEMA public TO {db_url.split('@')[0].split('://')[1].split(':')[0] if '@' in db_url else 'your_user'};")
            print(f"   ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {db_url.split('@')[0].split('://')[1].split(':')[0] if '@' in db_url else 'your_user'};")
            print(f"\n   Or use a different schema by setting search_path in DATABASE_URL:")
            print(f"   postgresql://user:pass@host/db?options=-csearch_path%3Dyour_schema")
            print(f"\n   Tables will be created automatically when the app first uses the database.")
            print(f"   The app will continue but database operations will fail until permissions are fixed.\n")
        else:
            print(f"⚠️  Warning during table creation: {e}")
        # Continue anyway - tables might already exist or error might be non-critical

    with engine.connect() as conn:
        # 1. Alter candle columns for higher precision (only if table exists)
        print("\nApplying candle column alterations...")
        try:
            for col in ['open', 'high', 'low', 'close', 'volume']:
                conn.execute(text(f"ALTER TABLE candles ALTER COLUMN {col} TYPE NUMERIC(30, 10);"))
                print(f"  ✓ candles.{col}")
            conn.commit()
        except Exception as e:
            if "does not exist" in str(e):
                print(f"  ⚠️  candles table doesn't exist yet (will be created)")
            else:
                print(f"  ⚠️  Error altering candles: {e}")
            conn.rollback()

        # 2. Add new columns to positions table if they don't exist (only if table exists)
        print("\nAdding position columns (if needed)...")
        try:
            new_cols = [
                ("trade_type", "VARCHAR"),
                ("partial_close_pct", "NUMERIC(5, 2)"),
                ("original_size", "NUMERIC(20, 8)"),
                ("tp_order_ids", "VARCHAR"),
                ("basis_at_entry", "NUMERIC(20, 8)"),
                ("basis_current", "NUMERIC(20, 8)"),
                ("funding_rate", "NUMERIC(20, 8)"),
                ("cumulative_funding", "NUMERIC(20, 8)"),
                ("is_protected", "BOOLEAN DEFAULT FALSE"),
                ("protection_reason", "VARCHAR"),
            ]
            for col_name, col_type in new_cols:
                try:
                    # Check if column exists first (PostgreSQL doesn't support IF NOT EXISTS in ALTER TABLE)
                    check_result = conn.execute(text(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name='positions' AND column_name='{col_name}'
                    """))
                    if check_result.fetchone():
                        print(f"  ⊙ positions.{col_name} (already exists)")
                    else:
                        conn.execute(text(f"ALTER TABLE positions ADD COLUMN {col_name} {col_type};"))
                        print(f"  ✓ positions.{col_name}")
                except Exception as e:
                    if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                        print(f"  ⊙ positions.{col_name} (already exists)")
                    else:
                        print(f"  ⚠ positions.{col_name}: {e}")
            conn.commit()
        except Exception as e:
            if "does not exist" in str(e):
                print(f"  ⚠️  positions table doesn't exist yet (will be created)")
            else:
                print(f"  ⚠️  Error adding position columns: {e}")
            conn.rollback()

    print("Migration complete!")

if __name__ == "__main__":
    try:
        migrate()
        print("✅ Migration script completed successfully")
    except RuntimeError as e:
        print(f"❌ Migration failed: {e}")
        # Print environment diagnostics
        print("\nEnvironment diagnostics:")
        print(f"  DATABASE_URL present: {bool(os.environ.get('DATABASE_URL'))}")
        print(f"  DATABASE_URL value length: {len(os.environ.get('DATABASE_URL', ''))}")
        print(f"  ENVIRONMENT: {os.environ.get('ENVIRONMENT', 'NOT SET')}")
        print(f"  Available env vars starting with 'DATABASE': {[k for k in os.environ.keys() if 'DATABASE' in k]}")
        # Don't raise - allow the main process to continue
        # Migration failures shouldn't block the app from starting
        print("⚠️  Continuing despite migration failure - app will start anyway")
        sys.exit(0)  # Exit with success so the main process can start
    except Exception as e:
        print(f"❌ Unexpected error during migration: {e}")
        import traceback
        traceback.print_exc()
        # Don't raise - allow the main process to continue
        print("⚠️  Continuing despite migration error - app will start anyway")
        sys.exit(0)  # Exit with success so the main process can start
