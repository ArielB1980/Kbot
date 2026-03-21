#!/bin/bash
#
# Review production server logs for correct operation.
# Run from repo root: ./scripts/review_server_logs.sh [--tail N]
#
# Checks for:
# - DATABASE_CONNECTION_INIT (correct DB in use)
# - test_db / MagicMock / INVARIANT VIOLATION (should be 0 or minimal)
# - Errors and critical events
# - Service startup and auction/trade flow
#

set -e

# Load env for SSH key / server
if [ -f .env.local ]; then
  set -a
  source .env.local
  set +a
fi

SERVER="${DEPLOY_SERVER:-root@207.154.193.121}"
SSH_KEY="${DEPLOY_SSH_KEY:-$HOME/.ssh/trading_droplet}"
TRADING_DIR="${DEPLOY_TRADING_DIR:-/home/trading/TradingSystem}"
TAIL="${1:-2000}"
if [ "$1" = "--tail" ] && [ -n "$2" ]; then
  TAIL="$2"
fi

LOG_FILE="$TRADING_DIR/logs/run.log"

echo "=============================================="
echo "Server log review (last $TAIL lines)"
echo "Server: $SERVER"
echo "=============================================="

# Fetch recent logs
RAW=$(ssh -i "$SSH_KEY" -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new "$SERVER" \
  "sudo -u trading tail -n $TAIL $LOG_FILE 2>/dev/null" || true)

if [ -z "$RAW" ]; then
  echo "Could not fetch logs (SSH failed or empty log)."
  exit 1
fi

# Count critical patterns (post-fix these should be 0); normalize to one number per variable
TEST_DB_COUNT=$(echo "$RAW" | grep -c "test_db" 2>/dev/null || echo "0")
TEST_DB_COUNT=$(echo "$TEST_DB_COUNT" | head -1 | tr -d '\n')
MAGICMOCK_COUNT=$(echo "$RAW" | grep -c "MagicMock" 2>/dev/null || echo "0")
MAGICMOCK_COUNT=$(echo "$MAGICMOCK_COUNT" | head -1 | tr -d '\n')
INVARIANT_COUNT=$(echo "$RAW" | grep -c "INVARIANT VIOLATION" 2>/dev/null || echo "0")
INVARIANT_COUNT=$(echo "$INVARIANT_COUNT" | head -1 | tr -d '\n')
ERROR_COUNT=$(echo "$RAW" | grep -c '"level": "error"' 2>/dev/null || echo "0")
ERROR_COUNT=$(echo "$ERROR_COUNT" | head -1 | tr -d '\n')
CRITICAL_COUNT=$(echo "$RAW" | grep -c '"level": "critical"' 2>/dev/null || echo "0")
CRITICAL_COUNT=$(echo "$CRITICAL_COUNT" | head -1 | tr -d '\n')

# Database connection init (our new log)
DB_INIT=$(echo "$RAW" | grep "DATABASE_CONNECTION_INIT" | tail -1)
DB_INIT_COUNT=$(echo "$RAW" | grep -c "DATABASE_CONNECTION_INIT" 2>/dev/null || echo "0")
DB_INIT_COUNT=$(echo "$DB_INIT_COUNT" | head -1 | tr -d '\n')

echo ""
echo "--- Go-live gates (post-fix) ---"
echo "  test_db mentions:        $TEST_DB_COUNT  (expected: 0)"
echo "  MagicMock mentions:      $MAGICMOCK_COUNT  (expected: 0)"
echo "  INVARIANT VIOLATION:    $INVARIANT_COUNT  (expected: near 0)"
echo "  error level:            $ERROR_COUNT"
echo "  critical level:         $CRITICAL_COUNT"
echo "  DATABASE_CONNECTION_INIT: $DB_INIT_COUNT  (expected: ≥1 on startup)"
echo ""

if [ -n "$DB_INIT" ]; then
  echo "--- Last DATABASE_CONNECTION_INIT ---"
  echo "$DB_INIT"
  echo ""
fi

echo "--- Recent errors (last 10) ---"
echo "$RAW" | grep '"level": "error"' | tail -10 || echo "(none)"
echo ""

echo "--- Recent critical (last 10, excluding DATABASE_CONNECTION_INIT) ---"
echo "$RAW" | grep '"level": "critical"' | grep -v "DATABASE_CONNECTION_INIT" | grep -v "PRODUCTION_MODE_VERIFICATION" | tail -10 || echo "(none)"
echo ""

echo "--- Startup / health (last 5) ---"
echo "$RAW" | grep -E "Logging initialized|Live trading started|Worker health server started|STARTING LIVE TRADING" | tail -5
echo ""

echo "--- Auction / execution (last 5) ---"
echo "$RAW" | grep -E "Auction plan generated|Auction allocation executed|Entry order placed|Auction: Opened position|TRADING PAUSED" | tail -5
echo ""

# Summary verdict
FAIL=0
if [ "${TEST_DB_COUNT:-0}" -gt 0 ] 2>/dev/null; then
  echo "FAIL: test_db still present in logs."
  FAIL=1
fi
if [ "${MAGICMOCK_COUNT:-0}" -gt 0 ] 2>/dev/null; then
  echo "FAIL: MagicMock still present in logs."
  FAIL=1
fi
if [ "${DB_INIT_COUNT:-0}" -eq 0 ] 2>/dev/null; then
  echo "WARN: DATABASE_CONNECTION_INIT not found (old log or no DB access yet)."
fi

if [ $FAIL -eq 0 ]; then
  echo "Verdict: Go-live gates passed (no test_db, no MagicMock)."
else
  echo "Verdict: Fix required (see above)."
  exit 1
fi
