#!/usr/bin/env bash
#
# Daily job: apply latest research to live (overrides + promote new coins) and optionally restart live.
# - Uses latest completed research run (latest_run_id or most recent run dir).
# - Runs apply_research_to_live.py with --promote-new-coins and writes live_research_overrides.yaml.
# - If RESTART_LIVE_AFTER_APPLY=1 or --restart-live, restarts trading-bot.service.
#
# Cron example (6 AM daily, run as trading user):
#   0 6 * * * cd /home/trading/TradingSystem && bash scripts/daily_apply_research_to_live.sh >> data/research/continuous_daemon/logs/daily_apply.log 2>&1
#
# With restart (requires passwordless sudo for systemctl restart trading-bot.service):
#   0 6 * * * RESTART_LIVE_AFTER_APPLY=1 cd /home/trading/TradingSystem && bash scripts/daily_apply_research_to_live.sh >> data/research/continuous_daemon/logs/daily_apply.log 2>&1
#
set -euo pipefail

TRADING_DIR="${DEPLOY_TRADING_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "${TRADING_DIR}"

if [[ -f ".env" ]]; then
  # shellcheck disable=SC1091
  set -a
  source .env
  set +a
fi

STATE_ROOT="${TRADING_DIR}/data/research/continuous_daemon"
RUNS_DIR="${STATE_ROOT}/runs"
LOG_DIR="${STATE_ROOT}/logs"
LATEST_RUN_FILE="${STATE_ROOT}/latest_run_id"
SERVICE_NAME="${DAILY_APPLY_SERVICE_NAME:-trading-bot.service}"

RESTART_LIVE="${RESTART_LIVE_AFTER_APPLY:-0}"
for arg in "$@"; do
  if [[ "${arg}" == "--restart-live" ]]; then
    RESTART_LIVE=1
    break
  fi
done

mkdir -p "${LOG_DIR}"

send_notification() {
  local msg="${1:-}"
  local webhook="${ALERT_WEBHOOK_URL:-}"
  local chat_id="${ALERT_CHAT_ID:-}"
  [[ -n "${msg}" && -n "${webhook}" ]] || return 0
  if [[ "${webhook}" == *"api.telegram.org"* && -n "${chat_id}" ]]; then
    curl -sS -m 15 -X POST "${webhook}" \
      -d "chat_id=${chat_id}" \
      --data-urlencode "text=${msg}" >/dev/null || true
    return 0
  fi
  if [[ "${webhook}" == *"discord.com/api/webhooks"* || "${webhook}" == *"discordapp.com/api/webhooks"* ]]; then
    local safe_msg
    safe_msg="$(printf '%s' "${msg}" | sed 's/"/\\"/g')"
    curl -sS -m 15 -H "Content-Type: application/json" -X POST "${webhook}" \
      -d "{\"content\":\"${safe_msg}\"}" >/dev/null || true
  fi
}

# Resolve run dir: prefer latest_run_id, else most recent run by mtime
RUN_DIR=""
if [[ -f "${LATEST_RUN_FILE}" ]]; then
  RUN_ID="$(cat "${LATEST_RUN_FILE}" 2>/dev/null || true)"
  if [[ -n "${RUN_ID}" && -d "${RUNS_DIR}/${RUN_ID}" ]]; then
    RUN_DIR="${RUNS_DIR}/${RUN_ID}"
  fi
fi
if [[ -z "${RUN_DIR}" && -d "${RUNS_DIR}" ]]; then
  # Portable: most recent continuous_* dir by mtime (GNU find -printf not on macOS)
  RUN_DIR=""
  for d in "${RUNS_DIR}"/continuous_*; do
    [[ -d "${d}" ]] || continue
    if [[ -z "${RUN_DIR}" || "${d}" -nt "${RUN_DIR}" ]]; then
      RUN_DIR="${d}"
    fi
  done
fi
if [[ -z "${RUN_DIR}" || ! -d "${RUN_DIR}" ]]; then
  echo "[$(date -u +%FT%TZ)] No research run dir found under ${RUNS_DIR}; skipping."
  exit 0
fi

echo "[$(date -u +%FT%TZ)] Applying research from run_dir=${RUN_DIR}"

# Apply: overrides for existing live coins + promote new coins that meet bar
if ! APPLY_OUTPUT="$("${TRADING_DIR}/venv/bin/python3" "${TRADING_DIR}/scripts/apply_research_to_live.py" \
  --run-dir "${RUN_DIR}" \
  --promote-new-coins \
  --promotion-min-trades 10 \
  --min-trades 2 2>&1)"; then
  echo "${APPLY_OUTPUT}"
  echo "[$(date -u +%FT%TZ)] apply_research_to_live.py failed"
  send_notification "❌ Daily apply failed (${RUN_DIR##*/}). Check ${LOG_DIR}/daily_apply.log"
  exit 1
fi
echo "${APPLY_OUTPUT}"

PROMOTION_LINE=""
while IFS= read -r line; do
  case "${line}" in
    Promoting\ *new\ coin* )
      PROMOTION_LINE="${line}"
      break
      ;;
  esac
done <<< "${APPLY_OUTPUT}"
if [[ -z "${PROMOTION_LINE}" ]]; then
  PROMOTION_LINE="No new coin promotions"
fi

echo "[$(date -u +%FT%TZ)] RESTART_LIVE=${RESTART_LIVE}; will_restart=$([[ "${RESTART_LIVE}" == "1" ]] && echo yes || echo no)"
RESTART_RESULT="not_requested"
if [[ "${RESTART_LIVE}" == "1" ]]; then
  if command -v systemctl >/dev/null 2>&1; then
    echo "[$(date -u +%FT%TZ)] Restarting ${SERVICE_NAME}"
    if sudo systemctl restart "${SERVICE_NAME}"; then
      RESTART_RESULT="ok"
    else
      RESTART_RESULT="failed"
    fi
  else
    echo "[$(date -u +%FT%TZ)] systemctl not available; skip restart"
    RESTART_RESULT="skipped_no_systemctl"
  fi
fi

echo "[$(date -u +%FT%TZ)] daily_apply_research_to_live done"
send_notification "✅ Daily apply done (${RUN_DIR##*/}) | ${PROMOTION_LINE} | restart=${RESTART_RESULT}"
