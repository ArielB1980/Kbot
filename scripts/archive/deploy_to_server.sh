#!/bin/bash
#
# Deployment Script for Server Migration
#
# This script helps prepare the system for server deployment
# by creating a clean deployment package.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

DEPLOYMENT_DIR="$PROJECT_ROOT/deployment"
PACKAGE_NAME="trading-system-$(date +%Y%m%d-%H%M%S).tar.gz"

echo "📦 Creating deployment package..."
echo ""

# Create deployment directory
mkdir -p "$DEPLOYMENT_DIR"

# Create package
echo "Creating archive..."
tar -czf "$DEPLOYMENT_DIR/$PACKAGE_NAME" \
    --exclude='.git' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='*.pyo' \
    --exclude='.env' \
    --exclude='trading.db*' \
    --exclude='*.log' \
    --exclude='logs/*' \
    --exclude='.live_trading.pid' \
    --exclude='.watchdog.pid' \
    --exclude='venv' \
    --exclude='.venv' \
    --exclude='deployment' \
    --exclude='*.tar.gz' \
    --exclude='.DS_Store' \
    --exclude='*.swp' \
    --exclude='*.swo' \
    --exclude='*~' \
    .

echo "✅ Package created: $DEPLOYMENT_DIR/$PACKAGE_NAME"
echo ""
echo "📋 Next steps:"
echo "  1. Transfer package to server:"
echo "     scp $DEPLOYMENT_DIR/$PACKAGE_NAME user@server:/opt/trading-system/"
echo ""
echo "  2. On server, extract and set up:"
echo "     cd /opt/trading-system"
echo "     tar -xzf $PACKAGE_NAME"
echo "     python3 -m venv venv"
echo "     source venv/bin/activate"
echo "     pip install -r requirements.txt"
echo ""
echo "  3. Configure environment:"
echo "     cp .env.example .env"
echo "     nano .env  # Add your API credentials"
echo ""
echo "  4. Set up systemd service (see SERVER_MIGRATION_GUIDE.md)"
echo ""
