#!/bin/bash
# Quick script to check dashboard deployment status

echo "🔍 Checking Dashboard Deployment Status"
echo "========================================"
echo ""

echo "1. Check App Platform Dashboard:"
echo "   https://cloud.digitalocean.com/apps/tradingbot-2tdzi/components"
echo ""

echo "2. Look for 'dashboard' component:"
echo "   - Status should be 'Running' (green)"
echo "   - Click on it to see the URL"
echo ""

echo "3. Test dashboard URL (once you have it):"
echo "   curl -I <dashboard-url>"
echo ""

echo "4. Current health check response:"
curl -s https://tradingbot-2tdzi.ondigitalocean.app/dashboard | python3 -m json.tool 2>/dev/null || curl -s https://tradingbot-2tdzi.ondigitalocean.app/dashboard
echo ""

echo "📝 Note: The /dashboard route returns a JSON message."
echo "   The actual dashboard is on a separate component URL."
echo "   Check App Platform Components tab for the dashboard URL."
