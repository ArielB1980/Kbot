#!/bin/bash
#
# Watchdog for Live Trading System
#
# Monitors the live trading system and restarts it if it crashes.
# This script runs continuously in the background.
#
# Usage:
#   ./scripts/watchdog_live_trading.sh        # Run in foreground (for testing)
#   nohup ./scripts/watchdog_live_trading.sh &  # Run in background

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Configuration
PID_FILE="$PROJECT_ROOT/.live_trading.pid"
WATCHDOG_PID_FILE="$PROJECT_ROOT/.watchdog_live_trading.pid"
LOG_DIR="$PROJECT_ROOT/logs"
WATCHDOG_LOG="$LOG_DIR/watchdog_live_trading.log"
CHECK_INTERVAL=30  # Check every 30 seconds
MAX_RESTARTS_PER_HOUR=10  # Prevent restart loops
RESTART_DELAY=10  # Wait 10 seconds before restarting

# Create logs directory
mkdir -p "$LOG_DIR"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$WATCHDOG_LOG"
}

# Cleanup function
cleanup() {
    log "Watchdog shutting down..."
    rm -f "$WATCHDOG_PID_FILE"
    exit 0
}

trap cleanup SIGTERM SIGINT

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

# Save watchdog PID
echo $$ > "$WATCHDOG_PID_FILE"

log "Watchdog started (PID: $$)"
log "Monitoring live trading system..."
log "Check interval: ${CHECK_INTERVAL}s"
log "Max restarts per hour: $MAX_RESTARTS_PER_HOUR"

# Track restart count (reset every hour)
restart_times=()

while true; do
    sleep "$CHECK_INTERVAL"
    
    # Check if PID file exists
    if [ ! -f "$PID_FILE" ]; then
        log "⚠️  PID file not found - system may have crashed"
        restart_times+=($(date +%s))
        # Clean up old restart times (older than 1 hour)
        current_time=$(date +%s)
        restart_times=($(printf '%s\n' "${restart_times[@]}" | awk -v now="$current_time" '$1 > (now - 3600)'))
        
        if [ ${#restart_times[@]} -gt $MAX_RESTARTS_PER_HOUR ]; then
            log "❌ Too many restarts in the last hour (${#restart_times[@]}) - stopping watchdog"
            log "   This may indicate a persistent issue. Check logs: $LOG_DIR/live_trading_stderr.log"
            break
        fi
        
        log "🔄 Restarting live trading system..."
        sleep "$RESTART_DELAY"
        "$SCRIPT_DIR/start_live_trading.sh" >> "$WATCHDOG_LOG" 2>&1 || {
            log "❌ Failed to restart system"
        }
        continue
    fi
    
    # Check if process is running
    PID=$(cat "$PID_FILE")
    if ! ps -p "$PID" > /dev/null 2>&1; then
        log "⚠️  Process $PID is not running - system crashed"
        rm -f "$PID_FILE"
        
        # Rate limiting
        restart_times+=($(date +%s))
        current_time=$(date +%s)
        restart_times=($(printf '%s\n' "${restart_times[@]}" | awk -v now="$current_time" '$1 > (now - 3600)'))
        
        if [ ${#restart_times[@]} -gt $MAX_RESTARTS_PER_HOUR ]; then
            log "❌ Too many restarts in the last hour (${#restart_times[@]}) - stopping watchdog"
            log "   This may indicate a persistent issue. Check logs: $LOG_DIR/live_trading_stderr.log"
            break
        fi
        
        log "🔄 Restarting live trading system..."
        sleep "$RESTART_DELAY"
        "$SCRIPT_DIR/start_live_trading.sh" >> "$WATCHDOG_LOG" 2>&1 || {
            log "❌ Failed to restart system"
        }
    fi
done

cleanup
