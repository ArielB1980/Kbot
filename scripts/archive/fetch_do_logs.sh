#!/bin/bash
# Fetch DigitalOcean App logs using doctl CLI
#
# Prerequisites:
# 1. Install doctl: brew install doctl
# 2. Authenticate: doctl auth init
# 3. Get your app ID from DigitalOcean dashboard

APP_ID="f592a2c8-1d78-4072-9e48-4d63bd83fcfb"  # Your app ID

# Check if doctl is installed
if ! command -v doctl &> /dev/null; then
    echo "❌ doctl is not installed"
    echo "Install with: brew install doctl"
    echo "Then authenticate: doctl auth init"
    exit 1
fi

# Fetch logs
echo "Fetching logs from DigitalOcean app: $APP_ID"
echo "================================================"

# Get last 500 lines of logs
doctl apps logs $APP_ID --tail 500 --type RUN

# Optional: Save to file
LOG_FILE="logs/digitalocean_$(date +%Y%m%d_%H%M%S).log"
echo ""
echo "Saving logs to: $LOG_FILE"
doctl apps logs $APP_ID --tail 1000 --type RUN > "$LOG_FILE"
echo "✓ Logs saved"
