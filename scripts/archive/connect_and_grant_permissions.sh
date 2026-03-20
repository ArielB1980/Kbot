#!/bin/bash
# Script to connect to database and grant permissions
# Requires admin/superuser credentials (doadmin)

echo "============================================================"
echo "Database Permission Grant Script"
echo "============================================================"
echo ""
echo "This script requires ADMIN/SUPERUSER credentials."
echo "The 'dbtradingbot' user cannot grant privileges to itself."
echo ""
echo "You need to get the ADMIN connection string from:"
echo "  DigitalOcean Dashboard → Databases → Your Database → Connection Details"
echo "  Look for 'Admin' or 'Superuser' connection string"
echo "  (It will start with 'postgresql://doadmin:...')"
echo ""
echo "============================================================"
echo ""

# Check if admin connection string provided
if [ -z "$1" ]; then
    echo "Usage:"
    echo "  ./scripts/connect_and_grant_permissions.sh 'postgresql://doadmin:PASSWORD@host:port/dbtradingbot?sslmode=require'"
    echo ""
    echo "Or set ADMIN_DATABASE_URL environment variable:"
    echo "  export ADMIN_DATABASE_URL='postgresql://doadmin:PASSWORD@host:port/dbtradingbot?sslmode=require'"
    echo "  ./scripts/connect_and_grant_permissions.sh"
    exit 1
fi

ADMIN_URL="${1:-$ADMIN_DATABASE_URL}"
DB_USER="dbtradingbot"

echo "Connecting as admin to grant permissions to: $DB_USER"
echo ""

# Extract connection details for psql
# Parse the URL (basic parsing)
if [[ $ADMIN_URL =~ postgresql://([^:]+):([^@]+)@([^:]+):([^/]+)/([^?]+) ]]; then
    DB_USER_ADMIN="${BASH_REMATCH[1]}"
    DB_PASS="${BASH_REMATCH[2]}"
    DB_HOST="${BASH_REMATCH[3]}"
    DB_PORT="${BASH_REMATCH[4]}"
    DB_NAME="${BASH_REMATCH[5]}"
    
    echo "Host: $DB_HOST"
    echo "Port: $DB_PORT"
    echo "Database: $DB_NAME"
    echo "Admin User: $DB_USER_ADMIN"
    echo ""
    
    # Check if psql is available
    if ! command -v psql &> /dev/null; then
        echo "❌ psql not found. Install PostgreSQL client:"
        echo "   macOS: brew install postgresql"
        echo "   Ubuntu: apt-get install postgresql-client"
        echo ""
        echo "Or use the Python script instead:"
        echo "   python scripts/grant_database_permissions.py \"$ADMIN_URL\""
        exit 1
    fi
    
    # Set password via environment variable
    export PGPASSWORD="$DB_PASS"
    
    echo "Granting permissions..."
    echo ""
    
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER_ADMIN" -d "$DB_NAME" -c "
        GRANT CREATE ON SCHEMA public TO $DB_USER;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO $DB_USER;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO $DB_USER;
    "
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "============================================================"
        echo "✅ PERMISSIONS GRANTED SUCCESSFULLY"
        echo "============================================================"
        echo ""
        echo "You can now restart your app. Tables will be created automatically."
    else
        echo ""
        echo "============================================================"
        echo "❌ FAILED TO GRANT PERMISSIONS"
        echo "============================================================"
        echo ""
        echo "Make sure:"
        echo "1. You're using ADMIN credentials (doadmin, not dbtradingbot)"
        echo "2. The connection string is correct"
        echo "3. Network/firewall allows connections"
    fi
    
    unset PGPASSWORD
else
    echo "❌ Invalid connection string format"
    echo "Expected: postgresql://user:password@host:port/database"
    exit 1
fi
