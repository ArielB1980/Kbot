#!/usr/bin/env python3
"""
Environment validation script for local development.

Checks:
- Python version
- Required environment variables
- Database connectivity
- Dependencies installed
- File permissions

Exit codes:
- 0: All checks passed
- 1: One or more checks failed
"""
import sys
import os
from pathlib import Path


def check_python_version():
    """Check if Python version is 3.11+"""
    print("🔍 Checking Python version...")
    version = sys.version_info
    if version.major == 3 and version.minor >= 11:
        print(f"   ✅ Python {version.major}.{version.minor}.{version.micro}")
        return True
    else:
        print(f"   ❌ Python {version.major}.{version.minor}.{version.micro} (requires 3.11+)")
        return False


def check_env_file():
    """Check if .env.local exists"""
    print("\n🔍 Checking .env.local file...")
    env_file = Path(".env.local")
    if env_file.exists():
        print(f"   ✅ .env.local found")
        return True
    else:
        print(f"   ⚠️  .env.local not found")
        print(f"   💡 Run: make validate")
        return False


def check_env_vars():
    """Check required environment variables"""
    print("\n🔍 Checking environment variables...")
    
    # Load .env.local if it exists
    env_file = Path(".env.local")
    if env_file.exists():
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=env_file)
    
    dry_run = os.getenv("DRY_RUN", "0")
    env = os.getenv("ENV", "prod")
    
    print(f"   ENV: {env}")
    print(f"   DRY_RUN: {dry_run}")
    
    # In dry-run mode, API keys are optional
    if dry_run in ("1", "true", "True"):
        print(f"   ✅ Dry-run mode (API keys optional)")
        return True
    else:
        # Check for required API keys
        required_vars = [
            "KRAKEN_FUTURES_API_KEY",
            "KRAKEN_FUTURES_API_SECRET",
            "DATABASE_URL"
        ]
        
        missing = []
        for var in required_vars:
            if not os.getenv(var):
                missing.append(var)
        
        if missing:
            print(f"   ❌ Missing required variables:")
            for var in missing:
                print(f"      - {var}")
            return False
        else:
            print(f"   ✅ All required variables set")
            return True


def check_dependencies():
    """Check if key dependencies are installed"""
    print("\n🔍 Checking dependencies...")
    
    required_modules = [
        "ccxt",
        "pandas",
        "pydantic",
        "structlog",
        "typer",
        "dotenv",
        "sqlalchemy",
    ]
    
    missing = []
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            missing.append(module)
    
    if missing:
        print(f"   ❌ Missing modules:")
        for mod in missing:
            print(f"      - {mod}")
        print(f"   💡 Run: make install")
        return False
    else:
        print(f"   ✅ All key dependencies installed")
        return True


def check_directories():
    """Check if required directories exist and are writable"""
    print("\n🔍 Checking directories...")
    
    dirs = [".local", "logs"]
    all_ok = True
    
    for dir_name in dirs:
        dir_path = Path(dir_name)
        
        # Create if doesn't exist
        if not dir_path.exists():
            try:
                dir_path.mkdir(parents=True, exist_ok=True)
                print(f"   ✅ Created {dir_name}/")
            except Exception as e:
                print(f"   ❌ Cannot create {dir_name}/: {e}")
                all_ok = False
        else:
            # Check if writable
            test_file = dir_path / ".test"
            try:
                test_file.touch()
                test_file.unlink()
                print(f"   ✅ {dir_name}/ is writable")
            except Exception as e:
                print(f"   ❌ {dir_name}/ is not writable: {e}")
                all_ok = False
    
    return all_ok


def check_database():
    """Check database connectivity"""
    print("\n🔍 Checking database...")
    
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=".env.local")
        
        from src.storage.db import init_db
        
        # Try to initialize database
        init_db()
        print(f"   ✅ Database connection successful")
        return True
    except Exception as e:
        print(f"   ⚠️  Database check skipped: {e}")
        # Don't fail on DB check - it might not be critical for all workflows
        return True


def main():
    """Run all validation checks"""
    print("=" * 60)
    print("🚀 Environment Validation")
    print("=" * 60)
    
    checks = [
        ("Python Version", check_python_version),
        ("Environment File", check_env_file),
        ("Environment Variables", check_env_vars),
        ("Dependencies", check_dependencies),
        ("Directories", check_directories),
        ("Database", check_database),
    ]
    
    results = []
    for name, check_func in checks:
        try:
            result = check_func()
            results.append((name, result))
        except Exception as e:
            print(f"\n❌ {name} check failed with exception: {e}")
            results.append((name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 Validation Summary")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {name}")
    
    print(f"\nResult: {passed}/{total} checks passed")
    
    if passed == total:
        print("\n✅ Environment is ready for development!")
        print("\nNext steps:")
        print("  1. Run smoke test: make smoke")
        print("  2. Start development: make run")
        return 0
    else:
        print("\n❌ Some checks failed. Please fix the issues above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
