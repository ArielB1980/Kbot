#!/bin/bash
#
# Stop Watchdog for Live Trading System
#

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WATCHDOG_PID_FILE="$PROJECT_ROOT/.watchdog_live_trading.pid"

# Check if PID file exists
if [ ! -f "$WATCHDOG_PID_FILE" ]; then
    echo "⚠️  Watchdog PID file not found. Watchdog may not be running."
    exit 1
fi

WATCHDOG_PID=$(cat "$WATCHDOG_PID_FILE")

# Check if process is running
if ! ps -p "$WATCHDOG_PID" > /dev/null 2>&1; then
    echo "⚠️  Watchdog process $WATCHDOG_PID is not running (stale PID file)"
    rm -f "$WATCHDOG_PID_FILE"
    exit 1
fi

echo "🛑 Stopping watchdog (PID: $WATCHDOG_PID)..."
echo ""

# Send SIGTERM for graceful shutdown
kill -TERM "$WATCHDOG_PID" 2>/dev/null || true

# Wait up to 10 seconds
for i in {1..10}; do
    if ! ps -p "$WATCHDOG_PID" > /dev/null 2>&1; then
        echo "✅ Watchdog stopped"
        rm -f "$WATCHDOG_PID_FILE"
        exit 0
    fi
    sleep 1
done

# Force kill if needed
if ps -p "$WATCHDOG_PID" > /dev/null 2>&1; then
    echo "⚠️  Force killing watchdog..."
    kill -KILL "$WATCHDOG_PID" 2>/dev/null || true
    sleep 1
    
    if ! ps -p "$WATCHDOG_PID" > /dev/null 2>&1; then
        echo "✅ Watchdog stopped (forced)"
        rm -f "$WATCHDOG_PID_FILE"
        exit 0
    else
        echo "❌ Failed to stop watchdog"
        exit 1
    fi
fi
