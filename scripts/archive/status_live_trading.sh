#!/bin/bash
#
# Check Live Trading System Status
#
# This script checks if the live trading system is running and shows its status.

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PID_FILE="$PROJECT_ROOT/.live_trading.pid"
LOG_DIR="$PROJECT_ROOT/logs"
STDOUT_LOG="$LOG_DIR/live_trading_stdout.log"
STDERR_LOG="$LOG_DIR/live_trading_stderr.log"

echo "Live Trading System Status"
echo "=========================="
echo ""

# Check if PID file exists
if [ ! -f "$PID_FILE" ]; then
    echo "Status: ⚪️ Not Running (no PID file)"
    echo ""
    echo "To start:"
    echo "  scripts/start_live_trading.sh"
    exit 0
fi

PID=$(cat "$PID_FILE")

# Check if process is running
if ! ps -p "$PID" > /dev/null 2>&1; then
    echo "Status: ⚠️  Not Running (stale PID file)"
    echo "   PID: $PID (process not found)"
    echo ""
    echo "Cleaning up stale PID file..."
    rm -f "$PID_FILE"
    echo ""
    echo "To start:"
    echo "  scripts/start_live_trading.sh"
    exit 0
fi

# Process is running - show details
echo "Status: 🟢 Running"
echo "   PID: $PID"
echo ""

# Show process info
echo "Process Info:"
ps -p "$PID" -o pid,ppid,pcpu,pmem,etime,command | tail -1
echo ""

# Show log file sizes
if [ -f "$STDOUT_LOG" ]; then
    STDOUT_SIZE=$(du -h "$STDOUT_LOG" | cut -f1)
    echo "Logs:"
    echo "   Stdout: $STDOUT_LOG ($STDOUT_SIZE)"
fi

if [ -f "$STDERR_LOG" ]; then
    STDERR_SIZE=$(du -h "$STDERR_LOG" | cut -f1)
    echo "   Stderr: $STDERR_LOG ($STDERR_SIZE)"
fi

echo ""
echo "To view logs:"
echo "  tail -f $STDOUT_LOG"
echo ""
echo "To stop:"
echo "  scripts/stop_live_trading.sh"
