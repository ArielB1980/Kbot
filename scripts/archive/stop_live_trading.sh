#!/bin/bash
#
# Stop Live Trading System
#
# This script stops the running live trading system gracefully.

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PID_FILE="$PROJECT_ROOT/.live_trading.pid"

# Check if PID file exists
if [ ! -f "$PID_FILE" ]; then
    echo "⚠️  PID file not found. Live trading system may not be running."
    exit 1
fi

PID=$(cat "$PID_FILE")

# Check if process is running
if ! ps -p "$PID" > /dev/null 2>&1; then
    echo "⚠️  Process $PID is not running (may have crashed)"
    rm -f "$PID_FILE"
    exit 1
fi

echo "🛑 Stopping live trading system (PID: $PID)..."
echo ""

# Try graceful shutdown first (SIGTERM)
kill -TERM "$PID" 2>/dev/null || true

# Wait up to 30 seconds for graceful shutdown
for i in {1..30}; do
    if ! ps -p "$PID" > /dev/null 2>&1; then
        echo "✅ Live trading system stopped gracefully"
        rm -f "$PID_FILE"
        exit 0
    fi
    sleep 1
done

# If still running, force kill
if ps -p "$PID" > /dev/null 2>&1; then
    echo "⚠️  Process did not stop gracefully, forcing shutdown..."
    kill -KILL "$PID" 2>/dev/null || true
    sleep 1
    
    if ! ps -p "$PID" > /dev/null 2>&1; then
        echo "✅ Live trading system stopped (forced)"
        rm -f "$PID_FILE"
        exit 0
    else
        echo "❌ Failed to stop process"
        exit 1
    fi
fi
