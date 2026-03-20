#!/bin/bash
# Schedule daily market discovery to run at midnight
# 
# This script sets up a cron job to run market discovery daily at midnight UTC
# Run this once to set up the scheduled task

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DISCOVER_SCRIPT="$SCRIPT_DIR/discover_markets.py"

# Create log directory if it doesn't exist
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

# Create cron entry (runs daily at 00:00 UTC)
CRON_ENTRY="0 0 * * * cd $PROJECT_ROOT && python3 $DISCOVER_SCRIPT >> $LOG_DIR/market_discovery.log 2>&1"

# Check if cron job already exists
if crontab -l 2>/dev/null | grep -q "$DISCOVER_SCRIPT"; then
    echo "Cron job already exists for market discovery"
    echo "Current cron entries:"
    crontab -l | grep "$DISCOVER_SCRIPT"
else
    # Add to crontab
    (crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -
    echo "✅ Market discovery scheduled to run daily at midnight UTC"
    echo ""
    echo "Cron entry added:"
    echo "  $CRON_ENTRY"
    echo ""
    echo "To view cron jobs: crontab -l"
    echo "To remove: crontab -e (then delete the line)"
    echo "To test immediately: python3 $DISCOVER_SCRIPT"
fi
