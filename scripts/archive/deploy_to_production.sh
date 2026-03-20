#!/bin/bash
# Deploy latest code to production server
# Usage: ./scripts/deploy_to_production.sh

set -e

SERVER="root@207.154.193.121"
SSH_KEY="$HOME/.ssh/trading_droplet"
TRADING_USER="trading"
TRADING_DIR="/home/trading/TradingSystem"
SERVICE_NAME="trading-system.service"

echo "🚀 Deploying to production server..."

# Step 1: Pull latest code from GitHub
echo "📦 Pulling latest code from GitHub..."
ssh -i "$SSH_KEY" $SERVER << 'DEPLOY_EOF'
    cd /home/trading/TradingSystem
    su - trading -c "cd /home/trading/TradingSystem && git fetch origin && git reset --hard origin/main"
    echo "✅ Code updated"
    su - trading -c "cd /home/trading/TradingSystem && git log --oneline -3"
DEPLOY_EOF

# Step 2: Restart service
echo "🔄 Restarting service..."
ssh -i "$SSH_KEY" $SERVER "systemctl restart $SERVICE_NAME"

# Step 3: Check status
echo "📊 Checking service status..."
sleep 2
ssh -i "$SSH_KEY" $SERVER "systemctl status $SERVICE_NAME --no-pager | head -n 15"

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📝 Monitor logs with:"
echo "  ssh -i $SSH_KEY $SERVER 'sudo -u $TRADING_USER tail -f $TRADING_DIR/logs/run.log | grep -E \"Entry order submitted|Failed to submit|Instrument specs\"'"
