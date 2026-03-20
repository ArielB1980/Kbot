#!/bin/bash
#
# Start Live Trading System in Background
#
# This script starts the live trading system in the background
# with proper logging and process management.

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Configuration
PID_FILE="$PROJECT_ROOT/.live_trading.pid"
LOG_DIR="$PROJECT_ROOT/logs"
LOG_FILE="$LOG_DIR/live_trading.log"
STDOUT_LOG="$LOG_DIR/live_trading_stdout.log"
STDERR_LOG="$LOG_DIR/live_trading_stderr.log"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Check if already running
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "⚠️  Live trading system is already running (PID: $PID)"
        echo "   Use 'scripts/stop_live_trading.sh' to stop it first"
        exit 1
    else
        # PID file exists but process is dead - remove it
        rm -f "$PID_FILE"
    fi
fi

# Start the system
echo "🚀 Starting live trading system in background..."
echo "   Log directory: $LOG_DIR"
echo "   Log file: $LOG_FILE"
echo ""

# Start with nohup to run in background
nohup python3 -m src.entrypoints.prod_live > "$STDOUT_LOG" 2> "$STDERR_LOG" &
PID=$!

# Save PID
echo $PID > "$PID_FILE"

# Wait a moment to check if it started successfully
sleep 3

if ps -p "$PID" > /dev/null 2>&1; then
    echo "✅ Live trading system started successfully"
    echo "   PID: $PID"
    echo "   PID file: $PID_FILE"
    echo ""
    echo "Logs:"
    echo "   Combined: $LOG_FILE"
    echo "   Stdout: $STDOUT_LOG"
    echo "   Stderr: $STDERR_LOG"
    echo ""
    echo "To check status:"
    echo "   tail -f $LOG_FILE"
    echo ""
    echo "To stop:"
    echo "   scripts/stop_live_trading.sh"
    echo ""
    echo "To check if running:"
    echo "   ps -p $PID"
else
    echo "❌ Failed to start live trading system"
    echo "   Check logs for errors:"
    echo "   cat $STDERR_LOG"
    rm -f "$PID_FILE"
    exit 1
fi
