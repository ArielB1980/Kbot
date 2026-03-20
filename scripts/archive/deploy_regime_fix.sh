#!/bin/bash
#
# Quick Deployment Script - Regime Classification Fix
# Run this ON YOUR PRODUCTION SERVER (DigitalOcean)
#

set -e

echo "🚀 Deploying Regime Classification Fix..."
echo ""

# Navigate to project directory
cd ~/TradingSystem || cd /home/trading/TradingSystem || {
    echo "❌ Error: Could not find TradingSystem directory"
    exit 1
}

echo "📂 Current directory: $(pwd)"
echo ""

# Check current branch
echo "📋 Current branch:"
git branch --show-current
echo ""

# Pull latest changes
echo "⬇️  Pulling latest changes from GitHub..."
git pull origin main

# Show recent commits
echo ""
echo "📝 Recent commits:"
git log --oneline -3
echo ""

# Restart the trading system
echo "🔄 Restarting trading system..."
sudo systemctl restart trading-system

# Wait a moment for service to start
sleep 3

# Check status
echo ""
echo "✅ Service status:"
sudo systemctl status trading-system --no-pager -l

echo ""
echo "📊 Checking logs (last 20 lines):"
sudo journalctl -u trading-system -n 20 --no-pager

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📌 Next steps:"
echo "  1. Monitor logs: sudo journalctl -u trading-system -f"
echo "  2. Check dashboard for regime distribution"
echo "  3. Verify 'tight_smc' regime appears when OB/FVG detected"
echo ""
