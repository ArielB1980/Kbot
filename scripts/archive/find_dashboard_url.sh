#!/bin/bash
# Script to help find dashboard URL

echo "🔍 Finding Dashboard URL"
echo "========================"
echo ""

echo "Method 1: Check App Platform Components Tab"
echo "1. Go to: https://cloud.digitalocean.com/apps/tradingbot-2tdzi/components"
echo "2. Look for 'dashboard' component"
echo "3. Click on it to see the URL"
echo ""

echo "Method 2: Check App Spec"
echo "1. Go to: https://cloud.digitalocean.com/apps/tradingbot-2tdzi/settings"
echo "2. Scroll to 'App Spec' section"
echo "3. Look for dashboard service configuration"
echo ""

echo "Method 3: Test Common Dashboard URLs"
echo "Testing possible dashboard URLs..."
echo ""

# Test main app URL with /dashboard route
echo "Testing: https://tradingbot-2tdzi.ondigitalocean.app/dashboard"
curl -s -I https://tradingbot-2tdzi.ondigitalocean.app/dashboard 2>&1 | grep -E "(HTTP|Content-Type)" | head -2
echo ""

echo "If you see 'Content-Type: text/html', that's your dashboard!"
echo "If you see 'Content-Type: application/json', dashboard is on separate URL."
echo ""

echo "📝 Next Steps:"
echo "- Check Components tab in App Platform"
echo "- Look for dashboard component with its own URL"
echo "- Dashboard URL will be something like: https://dashboard-xxxxx.ondigitalocean.app"
