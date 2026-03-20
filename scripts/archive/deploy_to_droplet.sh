#!/bin/bash
# Deployment script for Droplet
# Usage: ./scripts/deploy_to_droplet.sh [SSH_KEY_PATH] [DROPLET_IP] [DROPLET_USER]

set -e  # Exit on error

SSH_KEY="${1:-~/.ssh/trading_droplet}"
DROPLET_IP="${2:-207.154.193.121}"
DROPLET_USER="${3:-trading}"

echo "=========================================="
echo "DEPLOYMENT TO DROPLET"
echo "=========================================="
echo "SSH Key: $SSH_KEY"
echo "Droplet: $DROPLET_USER@$DROPLET_IP"
echo ""

# Step 1: Verify we're on main branch and up to date
echo "Step 1: Checking git status..."
if [ "$(git branch --show-current)" != "main" ]; then
    echo "❌ Not on main branch. Current branch: $(git branch --show-current)"
    exit 1
fi

git fetch origin
if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]; then
    echo "⚠️  Local branch is not up to date with origin/main"
    echo "   Local:  $(git rev-parse HEAD)"
    echo "   Remote: $(git rev-parse origin/main)"
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Step 2: Run pre-deployment tests
echo ""
echo "Step 2: Running pre-deployment tests..."
make pre-deploy || {
    echo "❌ Pre-deployment tests failed!"
    exit 1
}

# Step 3: Push to GitHub
echo ""
echo "Step 3: Pushing to GitHub..."
git push origin main || {
    echo "❌ Failed to push to GitHub!"
    exit 1
}
echo "✅ Pushed to GitHub"

# Step 4: Deploy to Droplet
echo ""
echo "Step 4: Deploying to Droplet..."

# Create a temporary directory for deployment files
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Copy necessary files (excluding .git, .venv, logs, etc.)
echo "   Preparing deployment package..."
tar --exclude='.git' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='.env' \
    --exclude='.env.local' \
    --exclude='logs' \
    --exclude='.venv' \
    --exclude='.local' \
    --exclude='*.log' \
    -czf "$TEMP_DIR/trading-system.tar.gz" .

# Transfer to server
echo "   Transferring files to server..."
scp -i "$SSH_KEY" "$TEMP_DIR/trading-system.tar.gz" "$DROPLET_USER@$DROPLET_IP:~/TradingSystem/"

# Extract and restart service
echo "   Extracting files and restarting service..."
ssh -i "$SSH_KEY" "$DROPLET_USER@$DROPLET_IP" << 'EOF'
cd ~/TradingSystem
tar -xzf trading-system.tar.gz
rm trading-system.tar.gz
echo "✅ Files extracted"
EOF

# Restart service as root
echo "   Restarting service..."
ssh -i "$SSH_KEY" "root@$DROPLET_IP" "systemctl restart trading-system.service && sleep 3 && systemctl status trading-system.service --no-pager | head -15"

echo ""
echo "=========================================="
echo "✅ DEPLOYMENT COMPLETE"
echo "=========================================="
echo ""
echo "To view logs:"
echo "  ssh -i $SSH_KEY $DROPLET_USER@$DROPLET_IP 'tail -f ~/TradingSystem/logs/trading.log'"
echo ""
echo "To check service status:"
echo "  ssh -i $SSH_KEY root@$DROPLET_IP 'systemctl status trading-system.service'"
