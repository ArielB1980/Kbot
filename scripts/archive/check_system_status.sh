#!/bin/bash
# System Status Check Script
# Checks all endpoints and provides a comprehensive status report

BASE_URL="https://tradingbot-2tdzi.ondigitalocean.app"

echo "=========================================="
echo "TRADING SYSTEM STATUS CHECK"
echo "=========================================="
echo ""

echo "1. ROOT ENDPOINT"
echo "----------------"
curl -s "${BASE_URL}/" | python3 -m json.tool 2>/dev/null || curl -s "${BASE_URL}/"
echo ""
echo ""

echo "2. HEALTH CHECK"
echo "---------------"
curl -s "${BASE_URL}/health" | python3 -m json.tool 2>/dev/null || curl -s "${BASE_URL}/health"
echo ""
echo ""

echo "3. QUICK TEST"
echo "-------------"
curl -s "${BASE_URL}/quick-test" | python3 -m json.tool 2>/dev/null || curl -s "${BASE_URL}/quick-test"
echo ""
echo ""

echo "4. READINESS PROBE"
echo "------------------"
curl -s "${BASE_URL}/ready" | python3 -m json.tool 2>/dev/null || curl -s "${BASE_URL}/ready"
echo ""
echo ""

echo "5. RESPONSE TIMES"
echo "-----------------"
echo -n "Root: "
time curl -s -o /dev/null -w "%{time_total}s\n" "${BASE_URL}/" 2>/dev/null || echo "N/A"
echo -n "Health: "
time curl -s -o /dev/null -w "%{time_total}s\n" "${BASE_URL}/health" 2>/dev/null || echo "N/A"
echo ""

echo "=========================================="
echo "STATUS SUMMARY"
echo "=========================================="

# Check if endpoints are responding
ROOT_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}/" 2>/dev/null)
HEALTH_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}/health" 2>/dev/null)
QUICK_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}/quick-test" 2>/dev/null)

if [ "$ROOT_STATUS" = "200" ]; then
    echo "✅ Root endpoint: OK"
else
    echo "❌ Root endpoint: FAILED (HTTP $ROOT_STATUS)"
fi

if [ "$HEALTH_STATUS" = "200" ]; then
    echo "✅ Health endpoint: OK"
else
    echo "❌ Health endpoint: FAILED (HTTP $HEALTH_STATUS)"
fi

if [ "$QUICK_STATUS" = "200" ]; then
    echo "✅ Quick test endpoint: OK"
else
    echo "❌ Quick test endpoint: FAILED (HTTP $QUICK_STATUS)"
fi

echo ""
echo "For detailed logs, check App Platform Runtime Logs"
echo "URL: https://cloud.digitalocean.com/apps"
