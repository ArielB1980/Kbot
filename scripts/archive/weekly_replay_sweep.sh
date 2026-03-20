#!/bin/bash
#
# Weekly Replay Sweep — run by cron to catch brittleness across jitter seeds.
#
# Install on production server:
#   crontab -e
#   0 3 * * 0  /home/trading/TradingSystem/scripts/weekly_replay_sweep.sh
#
# Alerts only on failure (via Telegram or log).

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

LOG_FILE="logs/replay-sweep-$(date +%Y%m%d).log"
mkdir -p logs

echo "=== Weekly Replay Sweep: $(date) ===" | tee "$LOG_FILE"

# Source environment
if [ -f .env ]; then
    set -a; source .env; set +a
fi
export ENV=local DRY_RUN=0

# Detect venv
if [ -d "venv" ]; then
    PYTHON="venv/bin/python"
elif [ -d ".venv" ]; then
    PYTHON=".venv/bin/python"
else
    PYTHON="python3"
fi

FAILED=0
for SEED in 1 2 3 4 5; do
    echo "" | tee -a "$LOG_FILE"
    echo "--- Seed $SEED ---" | tee -a "$LOG_FILE"
    if $PYTHON -m src.backtest.replay_harness.run_episodes \
        --seed "$SEED" \
        --data-dir data/replay \
        --output "results/replay/weekly-seed-$SEED" \
        2>&1 | tee -a "$LOG_FILE"; then
        echo "Seed $SEED: PASS" | tee -a "$LOG_FILE"
    else
        echo "Seed $SEED: FAIL" | tee -a "$LOG_FILE"
        FAILED=$((FAILED + 1))
    fi
done

echo "" | tee -a "$LOG_FILE"
if [ $FAILED -gt 0 ]; then
    MSG="REPLAY SWEEP FAILED: $FAILED/5 seeds failed. Check $LOG_FILE"
    echo "$MSG" | tee -a "$LOG_FILE"

    # Alert via Telegram if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are set
    if [ -n "$TELEGRAM_BOT_TOKEN" ] && [ -n "$TELEGRAM_CHAT_ID" ]; then
        curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
            -d chat_id="$TELEGRAM_CHAT_ID" \
            -d text="⚠️ $MSG" \
            -d parse_mode="Markdown" > /dev/null 2>&1 || true
    fi
    exit 1
else
    echo "ALL SEEDS PASSED (5/5)" | tee -a "$LOG_FILE"
    exit 0
fi
