#!/usr/bin/env bash
#
# Continuous research supervisor:
# - Runs symbol-by-symbol replay research in cycles
# - After each cycle, runs Counterfactual Twin reports
# - Repeats until stop file is present
#
set -euo pipefail

TRADING_DIR="${DEPLOY_TRADING_DIR:-/home/trading/TradingSystem}"
cd "${TRADING_DIR}"

STATE_ROOT="data/research/continuous_daemon"
RUNS_DIR="${STATE_ROOT}/runs"
LOG_DIR="${STATE_ROOT}/logs"
PID_FILE="${STATE_ROOT}/daemon.pid"
LOCK_FILE="${STATE_ROOT}/daemon.lock"
STOP_FILE="${STATE_ROOT}/stop_requested"
LATEST_RUN_FILE="${STATE_ROOT}/latest_run_id"
AUTOCONTEXT_DIR="${STATE_ROOT}/autocontext"
AUTOCONTEXT_LESSONS_FILE="${AUTOCONTEXT_DIR}/lessons.jsonl"
AUTOCONTEXT_OVERRIDES_FILE="${AUTOCONTEXT_DIR}/overrides.env"
CAMPAIGN_HISTORY_FILE="${AUTOCONTEXT_DIR}/campaign_history.jsonl"
CAMPAIGN_DECISION_FILE="${AUTOCONTEXT_DIR}/campaign_decision.json"

MEANINGFUL_NONBASELINE_MIN="${RESEARCH_CONT_MEANINGFUL_NONBASELINE_MIN:-3}"
MEANINGFUL_ACCEPTED_MIN="${RESEARCH_CONT_MEANINGFUL_ACCEPTED_MIN:-1}"
PROOF_WINDOW_CYCLES="${RESEARCH_CONT_PROOF_WINDOW_CYCLES:-6}"
ALLOW_FALSIFICATION_STOP="${RESEARCH_CONT_ALLOW_FALSIFICATION_STOP:-0}"

mkdir -p "${RUNS_DIR}" "${LOG_DIR}" "data/research/counterfactual_twin" "${AUTOCONTEXT_DIR}"

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "[$(date -u +%FT%TZ)] continuous daemon already running"
  exit 0
fi

echo $$ > "${PID_FILE}"
trap 'rm -f "${PID_FILE}"' EXIT
rm -f "${STOP_FILE}"

if [[ -f ".env" ]]; then
  # shellcheck disable=SC1091
  set -a
  source .env
  set +a
fi

DEFAULT_MODE="${RESEARCH_CONT_MODE:-replay}"
DEFAULT_ITERATIONS="${RESEARCH_CONT_ITER:-50}"
DEFAULT_DAYS="${RESEARCH_CONT_DAYS:-120}"
DEFAULT_SYMBOLS="${RESEARCH_CONT_SYMBOLS:-BTC/USD,ETH/USD,SOL/USD,XRP/USD,ADA/USD,LINK/USD}"
DEFAULT_OBJECTIVE_MODE="${RESEARCH_CONT_OBJECTIVE_MODE:-net_pnl_only}"
DEFAULT_SYMBOL_BY_SYMBOL="${RESEARCH_CONT_SYMBOL_BY_SYMBOL:-1}"
DEFAULT_SYMBOLS_FROM_LIVE_UNIVERSE="${RESEARCH_CONT_SYMBOLS_FROM_LIVE_UNIVERSE:-0}"
DEFAULT_SYMBOLS_FROM_CONFIG_UNIVERSE="${RESEARCH_CONT_SYMBOLS_FROM_CONFIG_UNIVERSE:-0}"
# When research uses full config universe, keep apply_research_to_live whitelist to this set (do not push 80+ symbols to live).
LIVE_UNIVERSE_FOR_APPLY="${RESEARCH_CONT_LIVE_UNIVERSE_FOR_APPLY:-BTC/USD,ETH/USD,SOL/USD,XRP/USD,ADA/USD,LINK/USD,DOGE/USD,AVAX/USD,LTC/USD,TRX/USD,DOT/USD,BCH/USD}"
DEFAULT_UNTIL_CONVERGENCE="${RESEARCH_CONT_UNTIL_CONVERGENCE:-1}"
DEFAULT_MAX_STAGNANT_ITERS="${RESEARCH_CONT_MAX_STAGNANT_ITERS:-20}"
DEFAULT_MAX_ITERS_PER_SYMBOL="${RESEARCH_CONT_MAX_ITERS_PER_SYMBOL:-120}"
DEFAULT_AUTO_BACKFILL_DATA="${RESEARCH_CONT_AUTO_BACKFILL_DATA:-0}"
DEFAULT_REPLAY_TIMEFRAMES="${RESEARCH_CONT_REPLAY_TIMEFRAMES:-1m,15m,1h,4h,1d}"
DEFAULT_WINDOW_OFFSETS="${RESEARCH_CONT_WINDOW_OFFSETS:-0,90,180}"
DEFAULT_HOLDOUT_RATIO="${RESEARCH_CONT_HOLDOUT_RATIO:-0.30}"
DEFAULT_TELEGRAM="${RESEARCH_CONT_TELEGRAM:-0}"
DEFAULT_SLEEP_SECONDS="${RESEARCH_CONT_SLEEP_SECONDS:-30}"
RUN_RETENTION_COUNT="${RESEARCH_CONT_RUN_RETENTION_COUNT:-8}"
RUN_LOG_MAX_BYTES="${RESEARCH_CONT_RUN_LOG_MAX_BYTES:-262144000}"
RUN_LOG_TRIM_TO_BYTES="${RESEARCH_CONT_RUN_LOG_TRIM_TO_BYTES:-104857600}"

# Counterfactual post-run settings
CF_HOURS="${RESEARCH_CONT_CF_HOURS:-24}"
CF_SYMBOLS="${RESEARCH_CONT_CF_SYMBOLS:-}"
CF_TOP_N="${RESEARCH_CONT_CF_TOP_N:-20}"
CF_CANDIDATES_DIR="${RESEARCH_CONT_CF_CANDIDATES_DIR:-data/research/counterfactual_twin/candidates}"
CF_TIMEOUT_SECONDS="${RESEARCH_CONT_CF_TIMEOUT_SECONDS:-300}"
CF_BATCH_TIMEOUT_SECONDS="${RESEARCH_CONT_CF_BATCH_TIMEOUT_SECONDS:-300}"
TG_NOTIFY="${RESEARCH_CONT_TG_NOTIFY:-1}"
PROMOTION_ENABLED="${RESEARCH_CONT_PROMOTION_ENABLED:-1}"
PROMOTION_MIN_OPEN_SYMBOLS="${RESEARCH_CONT_PROMOTION_MIN_OPEN_SYMBOLS:-5}"
PROMOTION_STABLE_CYCLES="${RESEARCH_CONT_PROMOTION_STABLE_CYCLES:-3}"
PROMOTION_STATE_FILE="${AUTOCONTEXT_DIR}/promotion_state.json"
PROMOTION_VALIDATION_SCORE_THRESHOLD="${RESEARCH_CONT_PROMOTION_VALIDATION_SCORE_THRESHOLD:-47}"
PROMOTION_VALIDATION_FIB_BPS="${RESEARCH_CONT_PROMOTION_VALIDATION_FIB_BPS:-70}"
PROMOTION_VALIDATION_CONVICTION_MIN="${RESEARCH_CONT_PROMOTION_VALIDATION_CONVICTION_MIN:-25}"
PROMOTION_VALIDATION_TIGHT_RR="${RESEARCH_CONT_PROMOTION_VALIDATION_TIGHT_RR:-1.5}"
PROMOTION_VALIDATION_REENTRY_THRESHOLD="${RESEARCH_CONT_PROMOTION_VALIDATION_REENTRY_THRESHOLD:-10}"
SELF_HEAL_ENABLE="${RESEARCH_CONT_SELF_HEAL_ENABLE:-1}"
SELF_HEAL_CONVICTION_FLOOR="${RESEARCH_CONT_SELF_HEAL_CONVICTION_FLOOR:-20}"
SELF_HEAL_AUTO_BACKFILL="${RESEARCH_CONT_SELF_HEAL_AUTO_BACKFILL:-1}"

log_daemon() {
  local msg="$1"
  echo "[$(date -u +%FT%TZ)] ${msg}" | tee -a "${LOG_DIR}/daemon.log"
}

write_latest_run_id() {
  local run_id="$1"
  local tmp_file="${LATEST_RUN_FILE}.tmp.$$"
  printf '%s\n' "${run_id}" > "${tmp_file}"
  mv "${tmp_file}" "${LATEST_RUN_FILE}"
}

bool_flag() {
  local raw="${1:-0}"
  local on="$2"
  local off="$3"
  case "${raw}" in
    1|true|TRUE|yes|YES|on|ON) echo "${on}" ;;
    *) echo "${off}" ;;
  esac
}

send_tg() {
  local text="$1"
  if [[ "${TG_NOTIFY}" != "1" ]]; then
    return 0
  fi
  TELEGRAM_TEXT="${text}" "${TRADING_DIR}/venv/bin/python3" - <<'PY' >/dev/null 2>&1 || true
import os
from src.monitoring.telegram_bot import send_telegram_message_sync

msg = os.environ.get("TELEGRAM_TEXT", "").strip()
if msg:
    send_telegram_message_sync(msg)
PY
}

poll_symbol_progress() {
  local state_file="$1"
  "${TRADING_DIR}/venv/bin/python3" - <<PY 2>/dev/null || true
import json
from pathlib import Path

p = Path("${state_file}")
if not p.exists():
    print("-1|-1||")
    raise SystemExit(0)

s = json.loads(p.read_text())
completed = s.get("completed_symbols") or []
total = int(s.get("total_symbols") or 0)
current = str(s.get("current_symbol") or "")
last_completed = str(completed[-1]) if completed else ""
print(f"{len(completed)}|{total}|{current}|{last_completed}")
PY
}

load_notify_state() {
  local notify_state_file="$1"
  if [[ -f "${notify_state_file}" ]]; then
    # shellcheck disable=SC1090
    source "${notify_state_file}" || true
  fi
  : "${LAST_DONE:--1}"
  : "${LAST_SYMBOL:=}"
}

save_notify_state() {
  local notify_state_file="$1"
  local last_done="$2"
  local last_symbol="$3"
  cat > "${notify_state_file}" <<EOF
LAST_DONE=${last_done}
LAST_SYMBOL=${last_symbol}
EOF
}

repair_autocontext_overrides() {
  local changes
  changes="$("${TRADING_DIR}/venv/bin/python3" - <<PY 2>/dev/null || true
from pathlib import Path

path = Path("${AUTOCONTEXT_OVERRIDES_FILE}")
path.parent.mkdir(parents=True, exist_ok=True)
raw = path.read_text(encoding="utf-8") if path.exists() else ""
lines = raw.splitlines()
header = []
pairs = {}
for line in lines:
    stripped = line.strip()
    if not stripped:
        continue
    if stripped.startswith("#"):
        header.append(line.rstrip())
        continue
    if "=" in line:
        key, value = line.split("=", 1)
        pairs[key.strip()] = value.strip()

changes = []
target_auto = "${SELF_HEAL_AUTO_BACKFILL}"
if pairs.get("RESEARCH_CONT_AUTO_BACKFILL_DATA") != target_auto:
    pairs["RESEARCH_CONT_AUTO_BACKFILL_DATA"] = target_auto
    changes.append(f"RESEARCH_CONT_AUTO_BACKFILL_DATA={target_auto}")
if pairs.get("AUTO_BACKFILL_DATA") != target_auto:
    pairs["AUTO_BACKFILL_DATA"] = target_auto
    changes.append(f"AUTO_BACKFILL_DATA={target_auto}")

floor = float("${SELF_HEAL_CONVICTION_FLOOR}")
current_raw = pairs.get("REPLAY_OVERRIDE_CONVICTION_MIN_FOR_ENTRY")
current = None
if current_raw is not None:
    try:
        current = float(current_raw)
    except ValueError:
        current = None
if current is None or current > floor:
    val = str(int(floor) if floor.is_integer() else floor)
    pairs["REPLAY_OVERRIDE_CONVICTION_MIN_FOR_ENTRY"] = val
    changes.append(f"REPLAY_OVERRIDE_CONVICTION_MIN_FOR_ENTRY={val}")

if changes:
    out_header = header or [
        "# Auto-generated by scripts/research_autolearn.py",
        "# Format: shell variable assignments consumed by research_continuous.sh",
    ]
    body = [f"{k}={pairs[k]}" for k in sorted(pairs)]
    path.write_text("\\n".join(out_header + body) + "\\n", encoding="utf-8")
print(",".join(changes))
PY
)"

  if [[ -n "${changes}" ]]; then
    log_daemon "SELF_HEAL_OVERRIDE_REPAIRED changes=${changes}"
    send_tg "🛠️ Research self-heal override repair
changes=${changes}
file=${AUTOCONTEXT_OVERRIDES_FILE}"
  fi
}

preflight_and_self_heal() {
  if [[ "${SELF_HEAL_ENABLE}" != "1" ]]; then
    return 0
  fi

  # Repair PID drift if another process clobbered the pid file.
  if [[ -f "${PID_FILE}" ]]; then
    local pid_raw
    pid_raw="$(cat "${PID_FILE}" 2>/dev/null || true)"
    if [[ -n "${pid_raw}" ]] && [[ "${pid_raw}" != "$$" ]]; then
      log_daemon "SELF_HEAL_PID_REPAIR old_pid=${pid_raw} new_pid=$$"
    fi
  fi
  echo $$ > "${PID_FILE}"

  repair_autocontext_overrides
}

prune_old_runs() {
  local keep_count="$1"
  "${TRADING_DIR}/venv/bin/python3" - <<PY 2>/dev/null || true
from pathlib import Path
import shutil

runs = Path("${RUNS_DIR}")
keep_count = max(1, int("${keep_count}"))
all_runs = sorted(
    [p for p in runs.glob("continuous_*") if p.is_dir()],
    key=lambda p: p.stat().st_mtime,
    reverse=True,
)
for p in all_runs[keep_count:]:
    shutil.rmtree(p, ignore_errors=True)
PY
}

cap_log_file() {
  local log_path="$1"
  local max_bytes="$2"
  local trim_to_bytes="$3"
  "${TRADING_DIR}/venv/bin/python3" - <<PY 2>/dev/null || true
from pathlib import Path

path = Path("${log_path}")
max_bytes = int("${max_bytes}")
trim_to = int("${trim_to_bytes}")
if not path.exists() or max_bytes <= 0 or trim_to <= 0 or trim_to >= max_bytes:
    raise SystemExit(0)
size = path.stat().st_size
if size <= max_bytes:
    raise SystemExit(0)

with path.open("rb") as f:
    f.seek(max(0, size - trim_to))
    data = f.read()
with path.open("wb") as f:
    f.write(data)
print(f"trimmed {path} from={size} to={len(data)}")
PY
}

update_promotion_state() {
  local run_log="$1"
  "${TRADING_DIR}/venv/bin/python3" - <<PY 2>/dev/null || true
import json
import re
from pathlib import Path

log_path = Path("${run_log}")
state_path = Path("${PROMOTION_STATE_FILE}")
min_open = max(1, int("${PROMOTION_MIN_OPEN_SYMBOLS}"))
stable_cycles = max(1, int("${PROMOTION_STABLE_CYCLES}"))

opened_symbols = set()
if log_path.exists():
    for line in log_path.read_text(errors="replace").splitlines():
        if "Auction: Opened position" not in line:
            continue
        m = re.search(r"symbol=([^\\s]+)", line)
        if m:
            opened_symbols.add(m.group(1).strip())

opens = len(opened_symbols)
state = {"streak": 0, "last_validation_streak": 0}
if state_path.exists():
    try:
        state.update(json.loads(state_path.read_text()))
    except Exception:
        pass

if opens >= min_open:
    state["streak"] = int(state.get("streak", 0)) + 1
else:
    state["streak"] = 0

last_validation_streak = int(state.get("last_validation_streak", 0))
should_validate = bool(state["streak"] >= stable_cycles and last_validation_streak < state["streak"])
state["last_open_symbols"] = sorted(opened_symbols)
state["last_open_count"] = opens
state["last_should_validate"] = should_validate
state_path.parent.mkdir(parents=True, exist_ok=True)
state_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
print(f"{opens}|{state['streak']}|{1 if should_validate else 0}")
PY
}

run_promotion_validation() {
  local run_id="$1"
  local symbols_csv="$2"
  local validation_dir="${RUNS_DIR}/${run_id}/promotion_validation"
  local validation_state="${validation_dir}/state.json"
  local validation_out="${validation_dir}/artifacts"
  local validation_log="${validation_dir}/research.log"
  mkdir -p "${validation_out}"

  env \
    REPLAY_GATE_DIAGNOSTICS=1 \
    REPLAY_ABLATE_DISABLE_WEEKLY_ZONE=0 \
    REPLAY_ABLATE_DISABLE_DECISION_STRUCTURE=1 \
    REPLAY_ABLATE_DISABLE_MS_CONFIRMATION=1 \
    REPLAY_ABLATE_DISABLE_WAIT_STRUCTURE_BREAK=1 \
    REPLAY_ABLATE_DISABLE_RECONFIRMATION=1 \
    REPLAY_OVERRIDE_STRUCTURE_DEDUPE_MINUTES=0 \
    REPLAY_OVERRIDE_ADX_THRESHOLD=12 \
    REPLAY_OVERRIDE_SCORE_GATE_THRESHOLD="${PROMOTION_VALIDATION_SCORE_THRESHOLD}" \
    REPLAY_OVERRIDE_FIB_PROXIMITY_BPS="${PROMOTION_VALIDATION_FIB_BPS}" \
    REPLAY_OVERRIDE_CONVICTION_MIN_FOR_ENTRY="${PROMOTION_VALIDATION_CONVICTION_MIN}" \
    REPLAY_OVERRIDE_TIGHT_SMC_MIN_RR="${PROMOTION_VALIDATION_TIGHT_RR}" \
    REPLAY_OVERRIDE_THESIS_REENTRY_BLOCK_THRESHOLD="${PROMOTION_VALIDATION_REENTRY_THRESHOLD}" \
    "${TRADING_DIR}/venv/bin/python3" "${TRADING_DIR}/run.py" research \
      --mode "${MODE}" \
      --iterations 1 \
      --days "${DAYS}" \
      --symbols "${symbols_csv}" \
      --objective-mode "${OBJECTIVE_MODE}" \
      --symbol-by-symbol \
      --no-symbols-from-live-universe \
      --until-convergence \
      --max-stagnant-iterations 1 \
      --max-iterations-per-symbol 1 \
      --window-offsets "${WINDOW_OFFSETS}" \
      --holdout-ratio "${HOLDOUT_RATIO}" \
      --no-auto-backfill-data \
      --replay-timeframes "${REPLAY_TIMEFRAMES}" \
      --no-telegram \
      --state-file "${validation_state}" \
      --out-dir "${validation_out}" \
      > "${validation_log}" 2>&1 || true

  "${TRADING_DIR}/venv/bin/python3" - <<PY 2>/dev/null || true
import json
import re
from pathlib import Path

log_path = Path("${validation_log}")
summary_path = Path("${validation_dir}/promotion_validation.json")
opened_symbols = set()
if log_path.exists():
    for line in log_path.read_text(errors="replace").splitlines():
        if "Auction: Opened position" not in line:
            continue
        m = re.search(r"symbol=([^\\s]+)", line)
        if m:
            opened_symbols.add(m.group(1).strip())

summary = {
    "run_id": "${run_id}",
    "opened_symbols": sorted(opened_symbols),
    "opened_count": len(opened_symbols),
    "required_min": int("${PROMOTION_MIN_OPEN_SYMBOLS}"),
    "passed": len(opened_symbols) >= int("${PROMOTION_MIN_OPEN_SYMBOLS}"),
    "profile": {
        "score_threshold": float("${PROMOTION_VALIDATION_SCORE_THRESHOLD}"),
        "fib_proximity_bps": float("${PROMOTION_VALIDATION_FIB_BPS}"),
        "conviction_min_for_entry": float("${PROMOTION_VALIDATION_CONVICTION_MIN}"),
        "tight_smc_min_rr": float("${PROMOTION_VALIDATION_TIGHT_RR}"),
        "thesis_reentry_block_threshold": float("${PROMOTION_VALIDATION_REENTRY_THRESHOLD}"),
    },
}
summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
print(json.dumps(summary, sort_keys=True))
PY
}

while true; do
  if [[ -f "${STOP_FILE}" ]]; then
    echo "[$(date -u +%FT%TZ)] stop file detected, exiting continuous daemon"
    break
  fi

  prune_old_runs "${RUN_RETENTION_COUNT}"
  preflight_and_self_heal

  TS="$(date -u +%Y%m%d_%H%M%S)"
  RUN_ID="continuous_${TS}"
  RUN_DIR="${RUNS_DIR}/${RUN_ID}"
  STATE_FILE="${RUN_DIR}/state.json"
  OUT_DIR="${RUN_DIR}/artifacts"
  RUN_LOG="${RUN_DIR}/research.log"
  POST_LOG="${RUN_DIR}/posthook.log"
  NOTIFY_STATE_FILE="${RUN_DIR}/notify_state.env"
  mkdir -p "${RUN_DIR}" "${OUT_DIR}"

  MODE="${DEFAULT_MODE}"
  ITERATIONS="${DEFAULT_ITERATIONS}"
  DAYS="${DEFAULT_DAYS}"
  SYMBOLS="${DEFAULT_SYMBOLS}"
  OBJECTIVE_MODE="${DEFAULT_OBJECTIVE_MODE}"
  SYMBOL_BY_SYMBOL="${DEFAULT_SYMBOL_BY_SYMBOL}"
  SYMBOLS_FROM_LIVE_UNIVERSE="${DEFAULT_SYMBOLS_FROM_LIVE_UNIVERSE}"
  SYMBOLS_FROM_CONFIG_UNIVERSE="${DEFAULT_SYMBOLS_FROM_CONFIG_UNIVERSE}"
  UNTIL_CONVERGENCE="${DEFAULT_UNTIL_CONVERGENCE}"
  MAX_STAGNANT_ITERS="${DEFAULT_MAX_STAGNANT_ITERS}"
  MAX_ITERS_PER_SYMBOL="${DEFAULT_MAX_ITERS_PER_SYMBOL}"
  AUTO_BACKFILL_DATA="${DEFAULT_AUTO_BACKFILL_DATA}"
  REPLAY_TIMEFRAMES="${DEFAULT_REPLAY_TIMEFRAMES}"
  WINDOW_OFFSETS="${DEFAULT_WINDOW_OFFSETS}"
  HOLDOUT_RATIO="${DEFAULT_HOLDOUT_RATIO}"
  TELEGRAM="${DEFAULT_TELEGRAM}"
  SLEEP_SECONDS="${DEFAULT_SLEEP_SECONDS}"

  if [[ -f "${AUTOCONTEXT_OVERRIDES_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${AUTOCONTEXT_OVERRIDES_FILE}" || true
    # Only export replay-tuning variables for the research subprocess.
    # Do NOT blanket-export all overrides (e.g. MODE), which can collide
    # with unrelated config env fields such as assets.mode.
    while IFS= read -r var_name; do
      export "${var_name}"
    done < <(compgen -A variable REPLAY_ || true)
    MODE="${MODE:-$DEFAULT_MODE}"
    ITERATIONS="${ITERATIONS:-$DEFAULT_ITERATIONS}"
    DAYS="${DAYS:-$DEFAULT_DAYS}"
    SYMBOLS="${SYMBOLS:-$DEFAULT_SYMBOLS}"
    OBJECTIVE_MODE="${OBJECTIVE_MODE:-$DEFAULT_OBJECTIVE_MODE}"
    SYMBOL_BY_SYMBOL="${SYMBOL_BY_SYMBOL:-$DEFAULT_SYMBOL_BY_SYMBOL}"
    SYMBOLS_FROM_LIVE_UNIVERSE="${SYMBOLS_FROM_LIVE_UNIVERSE:-$DEFAULT_SYMBOLS_FROM_LIVE_UNIVERSE}"
    SYMBOLS_FROM_CONFIG_UNIVERSE="${SYMBOLS_FROM_CONFIG_UNIVERSE:-$DEFAULT_SYMBOLS_FROM_CONFIG_UNIVERSE}"
    UNTIL_CONVERGENCE="${UNTIL_CONVERGENCE:-$DEFAULT_UNTIL_CONVERGENCE}"
    MAX_STAGNANT_ITERS="${MAX_STAGNANT_ITERS:-$DEFAULT_MAX_STAGNANT_ITERS}"
    MAX_ITERS_PER_SYMBOL="${MAX_ITERS_PER_SYMBOL:-$DEFAULT_MAX_ITERS_PER_SYMBOL}"
    AUTO_BACKFILL_DATA="${AUTO_BACKFILL_DATA:-$DEFAULT_AUTO_BACKFILL_DATA}"
    REPLAY_TIMEFRAMES="${REPLAY_TIMEFRAMES:-$DEFAULT_REPLAY_TIMEFRAMES}"
    WINDOW_OFFSETS="${WINDOW_OFFSETS:-$DEFAULT_WINDOW_OFFSETS}"
    HOLDOUT_RATIO="${HOLDOUT_RATIO:-$DEFAULT_HOLDOUT_RATIO}"
    TELEGRAM="${TELEGRAM:-$DEFAULT_TELEGRAM}"
    SLEEP_SECONDS="${SLEEP_SECONDS:-$DEFAULT_SLEEP_SECONDS}"
  fi

  # Ensure replay includes decision/regime horizons used by strategy scoring.
  REPLAY_TIMEFRAMES="$("${TRADING_DIR}/venv/bin/python3" - <<PY
base = [x.strip() for x in "${REPLAY_TIMEFRAMES}".split(",") if x.strip()]
required = ["15m", "1h", "4h", "1d"]
seen = set()
ordered = []
for tf in base + required:
    if tf not in seen:
        seen.add(tf)
        ordered.append(tf)
print(",".join(ordered))
PY
)"

  TELEGRAM_FLAG="$(bool_flag "${TELEGRAM}" "--telegram" "--no-telegram")"
  SYMBOL_MODE_FLAG="$(bool_flag "${SYMBOL_BY_SYMBOL}" "--symbol-by-symbol" "--no-symbol-by-symbol")"
  SYMBOLS_LIVE_FLAG="$(bool_flag "${SYMBOLS_FROM_LIVE_UNIVERSE}" "--symbols-from-live-universe" "--no-symbols-from-live-universe")"
  SYMBOLS_CONFIG_FLAG="$(bool_flag "${SYMBOLS_FROM_CONFIG_UNIVERSE}" "--symbols-from-config-universe" "--no-symbols-from-config-universe")"
  CONVERGENCE_FLAG="$(bool_flag "${UNTIL_CONVERGENCE}" "--until-convergence" "--no-until-convergence")"
  BACKFILL_FLAG="$(bool_flag "${AUTO_BACKFILL_DATA}" "--auto-backfill-data" "--no-auto-backfill-data")"

  write_latest_run_id "${RUN_ID}"
  echo "[$(date -u +%FT%TZ)] cycle start run_id=${RUN_ID}" | tee -a "${LOG_DIR}/daemon.log"

  # ---------------------------------------------------------------------------
  # Parallel worker launch: split symbols across N workers (one per available
  # CPU minus 1 for the candle collector / OS).  Each worker gets its own
  # state file, log, and output directory.  After all workers finish we merge
  # state files into the canonical STATE_FILE so post-run hooks work unchanged.
  # ---------------------------------------------------------------------------
  NCPU="$(nproc)"
  NWORKERS=$(( NCPU > 1 ? NCPU - 1 : 1 ))

  # Split SYMBOLS (comma-separated) into NWORKERS roughly-equal groups.
  IFS=',' read -ra ALL_SYMBOLS <<< "${SYMBOLS}"
  NSYMS=${#ALL_SYMBOLS[@]}
  # Cap workers to symbol count.
  if (( NWORKERS > NSYMS )); then
    NWORKERS=${NSYMS}
  fi

  declare -a WORKER_PIDS=()
  declare -a WORKER_STATE_FILES=()
  declare -a WORKER_LOGS=()
  declare -a WORKER_OUTDIRS=()

  start_idx=0
  for (( w=0; w < NWORKERS; w++ )); do
    # Compute this worker's symbol slice (round-robin remainder distribution).
    base_count=$(( NSYMS / NWORKERS ))
    remainder=$(( NSYMS % NWORKERS ))
    if (( w < remainder )); then
      count=$(( base_count + 1 ))
    else
      count=${base_count}
    fi
    slice=()
    for (( s=start_idx; s < start_idx + count; s++ )); do
      slice+=("${ALL_SYMBOLS[$s]}")
    done
    start_idx=$(( start_idx + count ))
    WORKER_SYMBOLS="$(IFS=','; echo "${slice[*]}")"

    W_STATE="${RUN_DIR}/state_w${w}.json"
    W_LOG="${RUN_DIR}/research_w${w}.log"
    W_OUT="${OUT_DIR}/w${w}"
    mkdir -p "${W_OUT}"

    WORKER_STATE_FILES+=("${W_STATE}")
    WORKER_LOGS+=("${W_LOG}")
    WORKER_OUTDIRS+=("${W_OUT}")

    log_daemon "  worker ${w}: symbols=${WORKER_SYMBOLS} state=${W_STATE}"

    (
      exec 9>&-
      "${TRADING_DIR}/venv/bin/python3" "${TRADING_DIR}/run.py" research \
        --mode "${MODE}" \
        --iterations "${ITERATIONS}" \
        --days "${DAYS}" \
        --symbols "${WORKER_SYMBOLS}" \
        --objective-mode "${OBJECTIVE_MODE}" \
        ${SYMBOL_MODE_FLAG} \
        ${SYMBOLS_LIVE_FLAG} \
        ${SYMBOLS_CONFIG_FLAG} \
        ${CONVERGENCE_FLAG} \
        --max-stagnant-iterations "${MAX_STAGNANT_ITERS}" \
        --max-iterations-per-symbol "${MAX_ITERS_PER_SYMBOL}" \
        --window-offsets "${WINDOW_OFFSETS}" \
        --holdout-ratio "${HOLDOUT_RATIO}" \
        ${BACKFILL_FLAG} \
        --replay-timeframes "${REPLAY_TIMEFRAMES}" \
        ${TELEGRAM_FLAG} \
        --state-file "${W_STATE}" \
        --out-dir "${W_OUT}" \
        > "${W_LOG}" 2>&1
    ) &
    WORKER_PIDS+=($!)
  done

  log_daemon "  launched ${NWORKERS} parallel workers for ${NSYMS} symbols"

  # Monitor loop: poll all workers, report per-worker progress, cap logs.
  LAST_DONE=-1
  LAST_SYMBOL=""
  load_notify_state "${NOTIFY_STATE_FILE}"

  any_alive() {
    for pid in "${WORKER_PIDS[@]}"; do
      kill -0 "${pid}" 2>/dev/null && return 0
    done
    return 1
  }

  # Disable errexit in the monitoring loop — poll/progress commands may return
  # non-zero legitimately (e.g. empty string tests, python poll scripts).
  set +e
  while any_alive; do
    write_latest_run_id "${RUN_ID}"

    # Aggregate progress across all worker state files.
    TOTAL_DONE=0
    TOTAL_SYMS=0
    AGG_CURRENT=""
    AGG_LAST_COMPLETED=""
    for wf in "${WORKER_STATE_FILES[@]}"; do
      PROG="$(poll_symbol_progress "${wf}")"
      D="$(echo "${PROG}" | cut -d'|' -f1)"
      T="$(echo "${PROG}" | cut -d'|' -f2)"
      C="$(echo "${PROG}" | cut -d'|' -f3)"
      L="$(echo "${PROG}" | cut -d'|' -f4)"
      if [[ "${D}" =~ ^[0-9]+$ ]]; then
        TOTAL_DONE=$(( TOTAL_DONE + D ))
        TOTAL_SYMS=$(( TOTAL_SYMS + T ))
      fi
      if [[ -n "${C}" ]]; then AGG_CURRENT="${C}"; fi
      if [[ -n "${L}" ]]; then AGG_LAST_COMPLETED="${L}"; fi
    done

    if [[ "${TOTAL_DONE}" -gt "${LAST_DONE}" ]]; then
      if [[ -n "${AGG_LAST_COMPLETED}" ]] && [[ "${AGG_LAST_COMPLETED}" != "${LAST_SYMBOL}" ]]; then
        send_tg "📈 Symbol progress
run_id=${RUN_ID}
done=${TOTAL_DONE}/${TOTAL_SYMS}
completed=${AGG_LAST_COMPLETED}
current=${AGG_CURRENT:-none}"
        LAST_SYMBOL="${AGG_LAST_COMPLETED}"
        save_notify_state "${NOTIFY_STATE_FILE}" "${LAST_DONE}" "${LAST_SYMBOL}"
      fi
      LAST_DONE="${TOTAL_DONE}"
      save_notify_state "${NOTIFY_STATE_FILE}" "${LAST_DONE}" "${LAST_SYMBOL}"
    fi

    for wl in "${WORKER_LOGS[@]}"; do
      CAP_MSG="$(cap_log_file "${wl}" "${RUN_LOG_MAX_BYTES}" "${RUN_LOG_TRIM_TO_BYTES}")"
      if [[ -n "${CAP_MSG}" ]]; then
        echo "[$(date -u +%FT%TZ)] ${CAP_MSG}" | tee -a "${LOG_DIR}/daemon.log"
      fi
    done
    sleep 15
  done
  set -e

  # Collect exit codes — worst one wins.
  RESEARCH_RC=0
  set +e
  for pid in "${WORKER_PIDS[@]}"; do
    wait "${pid}"
    rc=$?
    (( rc > RESEARCH_RC )) && RESEARCH_RC=${rc}
  done
  set -e

  # Merge worker state files into the canonical STATE_FILE for post-run hooks.
  "${TRADING_DIR}/venv/bin/python3" - <<PY || log_daemon "WARNING: state merge failed, continuing"
import json
from pathlib import Path

worker_states = [$(printf '"%s",' "${WORKER_STATE_FILES[@]}" | sed 's/,$//')]
merged = {
    "best_candidate_id": None,
    "completed_symbols": [],
    "control": {"paused": False, "stop_requested": False},
    "current_symbol": None,
    "eligible_symbols": [],
    "iteration": 0,
    "last_error": None,
    "leaderboard": [],
    "pending_prompt": None,
    "phase": "done",
    "promotion_queue": [],
    "run_id": "$(echo "${RUN_ID}")",
    "skipped_ineligible_symbols": {},
    "symbol_best_candidates": {},
    "symbol_progress": {},
    "total_iterations": 0,
    "total_symbols": 0,
    "updated_at": None,
}
best_composite = None
for wf in worker_states:
    p = Path(wf)
    if not p.exists():
        continue
    ws = json.loads(p.read_text())
    merged["completed_symbols"].extend(ws.get("completed_symbols") or [])
    merged["eligible_symbols"].extend(ws.get("eligible_symbols") or [])
    merged["leaderboard"].extend(ws.get("leaderboard") or [])
    merged["promotion_queue"].extend(ws.get("promotion_queue") or [])
    merged["total_symbols"] += ws.get("total_symbols", 0)
    merged["total_iterations"] += ws.get("total_iterations", 0)
    for sym, prog in (ws.get("symbol_progress") or {}).items():
        merged["symbol_progress"][sym] = prog
    for sym, cand in (ws.get("symbol_best_candidates") or {}).items():
        merged["symbol_best_candidates"][sym] = cand
    if ws.get("skipped_ineligible_symbols"):
        merged["skipped_ineligible_symbols"].update(ws["skipped_ineligible_symbols"])
    if ws.get("last_error"):
        merged["last_error"] = ws["last_error"]
    if ws.get("updated_at"):
        if not merged["updated_at"] or ws["updated_at"] > merged["updated_at"]:
            merged["updated_at"] = ws["updated_at"]
    wb = ws.get("best_candidate_id")
    if wb:
        merged["best_candidate_id"] = wb
merged["phase"] = "done"
Path("${STATE_FILE}").write_text(json.dumps(merged, indent=2, sort_keys=True))
PY

  # Concatenate worker logs into canonical RUN_LOG for post-run hooks.
  for (( w=0; w < NWORKERS; w++ )); do
    echo "=== WORKER ${w} ===" >> "${RUN_LOG}"
    cat "${WORKER_LOGS[$w]}" >> "${RUN_LOG}" 2>/dev/null || true
  done

  write_latest_run_id "${RUN_ID}"
  echo "[$(date -u +%FT%TZ)] cycle end run_id=${RUN_ID} rc=${RESEARCH_RC}" | tee -a "${LOG_DIR}/daemon.log"

  RUN_SUMMARY="$("${TRADING_DIR}/venv/bin/python3" - <<PY 2>/dev/null || true
import json
from pathlib import Path
p = Path("${STATE_FILE}")
if not p.exists():
    print("state=missing")
else:
    s = json.loads(p.read_text())
    best = s.get("best_candidate_id")
    completed = len(s.get("completed_symbols") or [])
    total = s.get("total_symbols") or 0
    phase = s.get("phase")
    print(f"phase={phase} symbols={completed}/{total} best={best}")
PY
)"
  send_tg "🔁 Continuous research cycle done
run_id=${RUN_ID}
rc=${RESEARCH_RC}
${RUN_SUMMARY}
state=${STATE_FILE}
artifacts=${OUT_DIR}"

  # Apply research best-by-symbol to live overrides (strategy symbol_overrides + assets whitelist)
  # Use LIVE_UNIVERSE_FOR_APPLY so we don't push full config universe to live whitelist when research runs on 80+ symbols.
  "${TRADING_DIR}/venv/bin/python3" "${TRADING_DIR}/scripts/apply_research_to_live.py" \
    --run-dir "${RUN_DIR}" \
    --live-universe "${LIVE_UNIVERSE_FOR_APPLY:-$SYMBOLS}" \
    >> "${POST_LOG}" 2>&1 || true

  FILTER_REPORT_FILE="${OUT_DIR}/filter_report.json"
  "${TRADING_DIR}/venv/bin/python3" "${TRADING_DIR}/scripts/research_filter_report.py" \
    --run-id "${RUN_ID}" \
    --state-file "${STATE_FILE}" \
    --run-log "${RUN_LOG}" \
    --out-file "${FILTER_REPORT_FILE}" \
    >> "${POST_LOG}" 2>&1 || true
  FILTER_SUMMARY="$("${TRADING_DIR}/venv/bin/python3" - <<PY 2>/dev/null || true
import json
from pathlib import Path
p = Path("${FILTER_REPORT_FILE}")
if not p.exists():
    print("filter_report=missing")
else:
    d = json.loads(p.read_text())
    ranked = d.get("ranked_blockers") or []
    if ranked:
        top = ranked[0]
        print(
            "top_blocker={} count={} known_total={}".format(
                top.get("blocker"),
                top.get("count"),
                d.get("known_blocker_total"),
            )
        )
    else:
        print("top_blocker=none count=0 known_total=0")
PY
)"
  send_tg "🧪 Filter blocker report
run_id=${RUN_ID}
${FILTER_SUMMARY}
report=${FILTER_REPORT_FILE}"

  "${TRADING_DIR}/venv/bin/python3" "${TRADING_DIR}/scripts/research_autolearn.py" \
    --run-id "${RUN_ID}" \
    --state-file "${STATE_FILE}" \
    --artifacts-dir "${OUT_DIR}" \
    --run-log "${RUN_LOG}" \
    --lessons-file "${AUTOCONTEXT_LESSONS_FILE}" \
    --overrides-file "${AUTOCONTEXT_OVERRIDES_FILE}" \
    >> "${POST_LOG}" 2>&1 || true

  # Post-run counterfactual reports (best-effort)
  {
    echo "counterfactual_single_start run_id=${RUN_ID} timeout=${CF_TIMEOUT_SECONDS}s"
    CF_CMD=("${TRADING_DIR}/venv/bin/python3" "${TRADING_DIR}/run.py" counterfactual-twin "--hours" "${CF_HOURS}")
    if [[ -n "${CF_SYMBOLS}" ]]; then
      CF_CMD+=("--symbols" "${CF_SYMBOLS}")
    fi
    CF_CMD+=("--out-file" "${OUT_DIR}/counterfactual_single.json")
    timeout "${CF_TIMEOUT_SECONDS}" "${CF_CMD[@]}" || echo "counterfactual_single timed_out_or_failed rc=$?"
    echo "counterfactual_single_end run_id=${RUN_ID}"

    if compgen -G "${CF_CANDIDATES_DIR}/*.json" > /dev/null; then
      echo "counterfactual_batch_start run_id=${RUN_ID} timeout=${CF_BATCH_TIMEOUT_SECONDS}s"
      CF_BATCH_CMD=(
        "${TRADING_DIR}/venv/bin/python3" "${TRADING_DIR}/run.py" counterfactual-twin-batch
        "--hours" "${CF_HOURS}"
        "--candidates-dir" "${CF_CANDIDATES_DIR}"
        "--top-n" "${CF_TOP_N}"
        "--out-file" "${OUT_DIR}/counterfactual_batch.json"
      )
      if [[ -n "${CF_SYMBOLS}" ]]; then
        CF_BATCH_CMD+=("--symbols" "${CF_SYMBOLS}")
      fi
      timeout "${CF_BATCH_TIMEOUT_SECONDS}" "${CF_BATCH_CMD[@]}" || echo "counterfactual_batch timed_out_or_failed rc=$?"
      echo "counterfactual_batch_end run_id=${RUN_ID}"
    else
      echo "No candidate files found in ${CF_CANDIDATES_DIR}; skipping batch report."
    fi
  } > "${POST_LOG}" 2>&1 || true

  CF_SUMMARY="$("${TRADING_DIR}/venv/bin/python3" - <<PY 2>/dev/null || true
import json
from pathlib import Path
single = Path("${OUT_DIR}/counterfactual_single.json")
if not single.exists():
    print("counterfactual_single=missing")
else:
    d = json.loads(single.read_text())
    r = d.get("report", {})
    print(
        "counterfactual_single "
        f"samples={r.get('samples')} "
        f"eligible={r.get('eligible_opportunities')} "
        f"uplift={r.get('utility_uplift')}"
    )
batch = Path("${OUT_DIR}/counterfactual_batch.json")
print(f"counterfactual_batch={'ready' if batch.exists() else 'missing'}")
PY
)"
  send_tg "📊 Post-run counterfactual reports
run_id=${RUN_ID}
${CF_SUMMARY}
single=${OUT_DIR}/counterfactual_single.json
batch=${OUT_DIR}/counterfactual_batch.json"

  "${TRADING_DIR}/venv/bin/python3" "${TRADING_DIR}/scripts/research_campaign_gate.py" \
    --run-id "${RUN_ID}" \
    --state-file "${STATE_FILE}" \
    --artifacts-dir "${OUT_DIR}" \
    --run-log "${RUN_LOG}" \
    --history-file "${CAMPAIGN_HISTORY_FILE}" \
    --decision-file "${CAMPAIGN_DECISION_FILE}" \
    --meaningful-nonbaseline-min "${MEANINGFUL_NONBASELINE_MIN}" \
    --meaningful-accepted-min "${MEANINGFUL_ACCEPTED_MIN}" \
    --proof-window-cycles "${PROOF_WINDOW_CYCLES}" \
    $(bool_flag "${ALLOW_FALSIFICATION_STOP}" "--allow-falsification-stop" "") \
    >> "${POST_LOG}" 2>&1 || true

  if [[ -f "${CAMPAIGN_DECISION_FILE}" ]]; then
    DECISION_TEXT="$("${TRADING_DIR}/venv/bin/python3" - <<PY 2>/dev/null || true
import json
from pathlib import Path
p = Path("${CAMPAIGN_DECISION_FILE}")
if not p.exists():
    raise SystemExit(0)
d = json.loads(p.read_text())
print(f"decision={d.get('decision')} reason={d.get('reason')}")
PY
)"
    if [[ -n "${DECISION_TEXT}" ]]; then
      send_tg "🧠 Campaign gate
run_id=${RUN_ID}
${DECISION_TEXT}"
    fi
    if [[ "${DECISION_TEXT}" == decision=stop_* ]]; then
      touch "${STOP_FILE}"
      echo "[$(date -u +%FT%TZ)] campaign gate requested stop: ${DECISION_TEXT}" | tee -a "${LOG_DIR}/daemon.log"
      break
    fi
  fi

  if [[ -f "${STOP_FILE}" ]]; then
    echo "[$(date -u +%FT%TZ)] stop file detected post-cycle, exiting daemon" | tee -a "${LOG_DIR}/daemon.log"
    break
  fi

  if [[ "${PROMOTION_ENABLED}" == "1" ]]; then
    PROMO_STATUS="$(update_promotion_state "${RUN_LOG}")"
    PROMO_OPENS="$(echo "${PROMO_STATUS}" | cut -d'|' -f1)"
    PROMO_STREAK="$(echo "${PROMO_STATUS}" | cut -d'|' -f2)"
    PROMO_VALIDATE="$(echo "${PROMO_STATUS}" | cut -d'|' -f3)"
    send_tg "🚦 Promotion gate
run_id=${RUN_ID}
opens=${PROMO_OPENS}
streak=${PROMO_STREAK}/${PROMOTION_STABLE_CYCLES}
validate=${PROMO_VALIDATE}"
    if [[ "${PROMO_VALIDATE}" == "1" ]]; then
      VALIDATION_SUMMARY="$(run_promotion_validation "${RUN_ID}" "${SYMBOLS}")"
      send_tg "✅ Promotion validation
run_id=${RUN_ID}
${VALIDATION_SUMMARY}
report=${RUNS_DIR}/${RUN_ID}/promotion_validation/promotion_validation.json"
      "${TRADING_DIR}/venv/bin/python3" - <<PY 2>/dev/null || true
import json
from pathlib import Path
state_path = Path("${PROMOTION_STATE_FILE}")
if state_path.exists():
    data = json.loads(state_path.read_text())
    data["last_validation_streak"] = int(data.get("streak", 0))
    state_path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
PY
    fi
  fi

  sleep "${SLEEP_SECONDS}"
done
