#!/bin/bash
#
# Deploy symbol format fix to production server
#
# This script:
# 1. Pulls latest code from GitHub on production
# 2. Restarts the trading system service
# 3. Verifies deployment
#

set -e

# Server details (update if needed)
SERVER="root@207.154.193.121"
TRADING_USER="trading"
TRADING_DIR="/home/trading/TradingSystem"
SERVICE_NAME="trading-system.service"

echo "🚀 Deploying fix to production server..."
echo ""

# Step 1: Pull latest code
echo "Step 1/3: Pulling latest code from GitHub..."
ssh $SERVER "su - $TRADING_USER -c 'cd $TRADING_DIR && git pull origin main'"
if [ $? -eq 0 ]; then
    echo "✅ Code pulled successfully"
else
    echo "❌ Failed to pull code"
    exit 1
fi

echo ""

# Step 2: Restart service
echo "Step 2/3: Restarting trading system service..."
ssh $SERVER "systemctl restart $SERVICE_NAME"
if [ $? -eq 0 ]; then
    echo "✅ Service restarted"
else
    echo "❌ Failed to restart service"
    exit 1
fi

echo ""

# Step 3: Verify service is running
echo "Step 3/3: Verifying service status..."
sleep 3
ssh $SERVER "systemctl status $SERVICE_NAME --no-pager | head -n 10"

echo ""
echo "✅ Deployment complete!"
echo ""
echo "Monitor logs with:"
echo "  ssh $SERVER 'sudo -u $TRADING_USER tail -f $TRADING_DIR/logs/run.log | grep -E \"Entry order submitted|Failed to submit|Instrument specs\"'"
