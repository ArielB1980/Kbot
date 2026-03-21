#!/usr/bin/env python3
"""
Verify database permissions for the current user.
"""
import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import create_engine, text
from urllib.parse import urlparse

def verify_permissions(database_url: str):
    """Verify current user's permissions."""
    print("=" * 60)
    print("Database Permissions Verification")
    print("=" * 60)
    
    parsed = urlparse(database_url)
    username = parsed.username
    
    print(f"\nDatabase URL: {parsed.scheme}://{username}@***")
    print(f"Current User: {username}")
    
    engine = create_engine(database_url)
    
    try:
        with engine.connect() as conn:
            # Check current user
            result = conn.execute(text("SELECT current_user, session_user;"))
            current_user, session_user = result.fetchone()
            print(f"\nCurrent User (from DB): {current_user}")
            print(f"Session User: {session_user}")
            
            # Check schema privileges
            print("\nChecking schema privileges...")
            result = conn.execute(text("""
                SELECT 
                    nspname as schema_name,
                    nspacl as privileges
                FROM pg_namespace 
                WHERE nspname = 'public';
            """))
            schema_info = result.fetchone()
            
            if schema_info:
                schema_name, privileges = schema_info
                print(f"Schema: {schema_name}")
                print(f"Privileges: {privileges}")
                
                if privileges:
                    # Check if user has CREATE privilege
                    has_create = False
                    for priv in privileges:
                        if username in str(priv) and 'C' in str(priv):
                            has_create = True
                            break
                    
                    if has_create:
                        print(f"\n✅ User '{username}' HAS CREATE privilege on 'public' schema")
                    else:
                        print(f"\n❌ User '{username}' does NOT have CREATE privilege on 'public' schema")
                        print(f"   Privileges found: {privileges}")
                else:
                    print(f"\n⚠️  No explicit privileges set (using defaults)")
            else:
                print("\n❌ Could not find 'public' schema")
            
            # Try to create a test table
            print("\n" + "=" * 60)
            print("Testing CREATE TABLE permission...")
            print("=" * 60)
            
            try:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS _permission_test (
                        id SERIAL PRIMARY KEY,
                        test_value VARCHAR(10)
                    );
                """))
                conn.commit()
                print("✅ CREATE TABLE test: SUCCESS")
                
                # Clean up
                conn.execute(text("DROP TABLE IF EXISTS _permission_test;"))
                conn.commit()
                print("✅ Test table cleaned up")
                
            except Exception as e:
                error_str = str(e).lower()
                if "permission denied" in error_str or "insufficientprivilege" in error_str:
                    print("❌ CREATE TABLE test: FAILED - Permission denied")
                    print(f"\n   Error: {e}")
                    print(f"\n   SOLUTION:")
                    print(f"   You need to connect as a SUPERUSER (usually 'doadmin')")
                    print(f"   and run:")
                    print(f"\n   GRANT CREATE ON SCHEMA public TO {username};")
                    print(f"   ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {username};")
                    print(f"   ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {username};")
                else:
                    print(f"❌ CREATE TABLE test: FAILED - {e}")
                    
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")

def main():
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL not found in environment")
        sys.exit(1)
    
    verify_permissions(database_url)

if __name__ == "__main__":
    main()
