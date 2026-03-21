#!/bin/bash
# Monitor live trading warmup progress

echo "Monitoring live trading warmup..."
echo "Started at: $(date)"
echo ""

# Wait for warmup to complete
while true; do
    if grep -q "Indicators warmed" live.log 2>/dev/null; then
        echo "✅ Warmup completed at: $(date)"
        echo ""
        
        # Show last few log lines
        echo "Recent activity:"
        tail -10 live.log
        echo ""
        
        # Check database
        echo "Latest account state:"
        sqlite3 trading.db "SELECT timestamp, equity FROM account_state ORDER BY timestamp DESC LIMIT 1;"
        echo ""
        
        echo "Dashboard should now be updating!"
        break
    fi
    
    # Show progress
    CURRENT=$(tail -1 live.log | grep -o "Fetching history for [A-Z/]*" | cut -d' ' -f4)
    if [ ! -z "$CURRENT" ]; then
        echo "$(date +%H:%M:%S) - Loading: $CURRENT"
    fi
    
    sleep 10
done
