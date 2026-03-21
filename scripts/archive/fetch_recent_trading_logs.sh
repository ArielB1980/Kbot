#!/bin/bash
#
# Fetch recent production trading logs and output analysis-ready sections.
# Used by the "analyze recent trading" routine. Run from repo root.
#
# Usage: ./scripts/fetch_recent_trading_logs.sh [LINES]
#   LINES defaults to 3000.
#
# Outputs labeled sections so an AI or human can quickly summarize:
#   - Last CYCLE_SUMMARY lines (positions, system_state, duration)
#   - Auction activity (opens/closes, rejection_counts)
#   - Risk sizing binding constraint (final_binding_constraint, equity, final_notional)
#   - Trade approved/rejected
#   - Utilisation boost applied
#   - Errors and critical (excluding known benign)
#   - INVARIANT / HALT / kill_switch
#   - KRAKEN FUTURES FILLS (last 48h) — source of truth for executed trades (avoids log-tail discrepancy)
#

set -e

if [ -f .env.local ]; then
  set -a
  source .env.local
  set +a
fi

SERVER="${DEPLOY_SERVER:-root@207.154.193.121}"
SSH_KEY="${DEPLOY_SSH_KEY:-$HOME/.ssh/trading_droplet}"
TRADING_DIR="${DEPLOY_TRADING_DIR:-/home/trading/TradingSystem}"
LINES="${1:-3000}"
LOG_FILE="$TRADING_DIR/logs/run.log"

RAW=$(ssh -i "$SSH_KEY" -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new "$SERVER" \
  "sudo -u trading tail -n $LINES $LOG_FILE 2>/dev/null" || true)

if [ -z "$RAW" ]; then
  echo "Could not fetch logs (SSH failed or empty log)."
  exit 1
fi

echo "=== RECENT TRADING LOGS (last $LINES lines) ==="
echo ""

echo "--- CYCLE_SUMMARY (last 15) ---"
echo "$RAW" | grep "CYCLE_SUMMARY" | tail -15
echo ""

echo "--- AUCTION (last 15: plan, allocation, opens/closes) ---"
echo "$RAW" | grep -E "Auction allocation executed|AUCTION_END|Auction plan|opens_executed|opens_planned|rejection_counts" | tail -15
echo ""

echo "--- RISK SIZING BINDING CONSTRAINT (last 20) ---"
echo "$RAW" | grep "Risk sizing binding constraint" | tail -20
echo ""

echo "--- TRADE APPROVED / REJECTED (last 15) ---"
echo "$RAW" | grep -E "Trade approved|Trade rejected" | tail -15
echo ""

echo "--- UTILISATION BOOST APPLIED (last 10) ---"
echo "$RAW" | grep "Utilisation boost applied" | tail -10
echo ""

echo "--- ERRORS (level error, last 15) ---"
echo "$RAW" | grep '"level": "error"' | tail -15
echo ""

echo "--- CRITICAL (excl. PROD_INVARIANT_REPORT / PROD_LIVE_LOCK / DATABASE_CONNECTION) (last 10) ---"
echo "$RAW" | grep '"level": "critical"' | grep -v "PROD_INVARIANT_REPORT" | grep -v "PROD_LIVE_LOCK" | grep -v "DATABASE_CONNECTION" | tail -10
echo ""

echo "--- INVARIANT VIOLATION / HALT / KILL_SWITCH (last 10) ---"
echo "$RAW" | grep -iE "INVARIANT VIOLATION|HALT|kill_switch|TRADING PAUSED" | tail -10
echo ""

echo "--- RECENT POSITIONS / ACTIVE PORTFOLIO (last 5) ---"
echo "$RAW" | grep -E "Active Portfolio|positions=|registry_positions" | tail -5
echo ""

# Source of truth for what actually traded (avoids discrepancy when log tail scrolls past older trades)
echo "--- KRAKEN FUTURES FILLS (last 48h, source of truth for executed trades) ---"
FILLS=$(ssh -i "$SSH_KEY" -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new "$SERVER" \
  "sudo -u trading bash -c 'set -a; [ -f $TRADING_DIR/.env ] && source $TRADING_DIR/.env; set +a; cd $TRADING_DIR && ./venv/bin/python scripts/fetch_kraken_futures_trades.py --hours 48 2>/dev/null'" 2>/dev/null) || true
if [ -n "$FILLS" ]; then
  echo "$FILLS"
else
  echo "(Could not fetch Kraken fills from server; use logs above. Run fetch_kraken_futures_trades.py on server for trade history.)"
fi
echo ""

echo "--- END OF FETCH ==="
