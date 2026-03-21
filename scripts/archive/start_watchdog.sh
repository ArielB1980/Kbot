#!/bin/bash
#
# Start Watchdog for Live Trading System
#
# Starts the watchdog process that monitors and restarts the system if it crashes.

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

WATCHDOG_PID_FILE="$PROJECT_ROOT/.watchdog_live_trading.pid"
LOG_DIR="$PROJECT_ROOT/logs"
WATCHDOG_LOG="$LOG_DIR/watchdog_live_trading.log"

# Create logs directory
mkdir -p "$LOG_DIR"

# Check if watchdog is already running
if [ -f "$WATCHDOG_PID_FILE" ]; then
    WATCHDOG_PID=$(cat "$WATCHDOG_PID_FILE")
    if ps -p "$WATCHDOG_PID" > /dev/null 2>&1; then
        echo "⚠️  Watchdog is already running (PID: $WATCHDOG_PID)"
        exit 1
    else
        rm -f "$WATCHDOG_PID_FILE"
    fi
fi

echo "🚀 Starting watchdog for live trading system..."
echo "   Log: $WATCHDOG_LOG"
echo ""

# Start watchdog in background
nohup "$SCRIPT_DIR/watchdog_live_trading.sh" >> "$WATCHDOG_LOG" 2>&1 &
WATCHDOG_PID=$!

# Wait a moment to verify it started
sleep 2

if ps -p "$WATCHDOG_PID" > /dev/null 2>&1; then
    echo "✅ Watchdog started successfully"
    echo "   PID: $WATCHDOG_PID"
    echo "   PID file: $WATCHDOG_PID_FILE"
    echo ""
    echo "The watchdog will:"
    echo "  - Monitor the system every 30 seconds"
    echo "  - Restart if it crashes"
    echo "  - Prevent restart loops (max 10 restarts/hour)"
    echo ""
    echo "To stop watchdog:"
    echo "  scripts/stop_watchdog.sh"
    echo ""
    echo "To check status:"
    echo "  tail -f $WATCHDOG_LOG"
else
    echo "❌ Failed to start watchdog"
    echo "   Check log: $WATCHDOG_LOG"
    exit 1
fi
