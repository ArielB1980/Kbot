#!/usr/bin/env bash
#
# Nightly sandbox autoresearch runner (server-side).
# Intended to be invoked by trading user's crontab.
#
set -euo pipefail

TRADING_DIR="${DEPLOY_TRADING_DIR:-/home/trading/TradingSystem}"
cd "${TRADING_DIR}"

mkdir -p data/research

if [[ -f ".env" ]]; then
  # shellcheck disable=SC1091
  set -a
  source .env
  set +a
fi

MODE="${RESEARCH_NIGHTLY_MODE:-backtest}"
ITERATIONS="${RESEARCH_NIGHTLY_ITER:-30}"
DAYS="${RESEARCH_NIGHTLY_DAYS:-30}"
SYMBOLS="${RESEARCH_NIGHTLY_SYMBOLS:-BTC/USD,ETH/USD,SOL/USD}"
TELEGRAM="${RESEARCH_NIGHTLY_TELEGRAM:-0}"
DIGEST_EVERY="${RESEARCH_NIGHTLY_DIGEST_EVERY:-10}"

TS="$(date -u +%Y%m%d_%H%M%S)"
RUN_ID="nightly_${TS}"
RUN_DIR="data/research/${RUN_ID}"
STATE_FILE="${RUN_DIR}/state.json"
OUT_DIR="${RUN_DIR}/artifacts"
LOCK_FILE="data/research/nightly.lock"
LOG_FILE="${RUN_DIR}/run.log"

mkdir -p "${RUN_DIR}" "${OUT_DIR}"

# Ensure no overlap between nightly runs.
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "[$(date -u +%FT%TZ)] nightly run skipped: lock already held"
  exit 0
fi

python3 - <<PY
import json
state = {
  "run_id": None,
  "phase": "idle",
  "iteration": 0,
  "total_iterations": 0,
  "best_candidate_id": None,
  "leaderboard": [],
  "control": {"paused": False, "stop_requested": False},
  "pending_prompt": None,
  "promotion_queue": [],
  "last_error": None,
  "updated_at": ""
}
with open("${STATE_FILE}", "w", encoding="utf-8") as f:
    json.dump(state, f, indent=2)
PY

if [[ "${TELEGRAM}" == "1" ]]; then
  TELEGRAM_FLAG="--telegram"
else
  TELEGRAM_FLAG="--no-telegram"
fi

# Force isolated sqlite for research to avoid production DB writes.
export DATABASE_URL="sqlite:///${TRADING_DIR}/${RUN_DIR}/research.db"

echo "[$(date -u +%FT%TZ)] nightly run start id=${RUN_ID} mode=${MODE} iterations=${ITERATIONS}"
"${TRADING_DIR}/venv/bin/python3" "${TRADING_DIR}/run.py" research \
  --mode "${MODE}" \
  --iterations "${ITERATIONS}" \
  --days "${DAYS}" \
  --symbols "${SYMBOLS}" \
  --digest-every "${DIGEST_EVERY}" \
  ${TELEGRAM_FLAG} \
  --state-file "${STATE_FILE}" \
  --out-dir "${OUT_DIR}" \
  > "${LOG_FILE}" 2>&1

echo "[$(date -u +%FT%TZ)] nightly run done id=${RUN_ID}"

