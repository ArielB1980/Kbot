#!/usr/bin/env bash
#
# Safe orchestration wrapper for sandbox autoresearch on production server.
# Runs research in an isolated state/output directory and supports control ops.
#
# Examples:
#   ./scripts/research_control.sh start --mode mock --iterations 500 --telegram --paused-start
#   ./scripts/research_control.sh status
#   ./scripts/research_control.sh resume
#   ./scripts/research_control.sh pause
#   ./scripts/research_control.sh promote --candidate c042
#   ./scripts/research_control.sh stop
#   ./scripts/research_control.sh logs
#   ./scripts/research_control.sh cleanup
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -f "${ROOT_DIR}/.env.local" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env.local"
fi

SERVER="${DEPLOY_SERVER:-root@207.154.193.121}"
SSH_KEY="${DEPLOY_SSH_KEY:-$HOME/.ssh/trading_droplet}"
TRADING_USER="${DEPLOY_TRADING_USER:-trading}"
TRADING_DIR="${DEPLOY_TRADING_DIR:-/home/trading/TradingSystem}"
RESEARCH_ROOT="${TRADING_DIR}/data/research"
CURRENT_RUN_FILE="${RESEARCH_ROOT}/current_run_id"

MODE="replay"
ITERATIONS="50"
DAYS="90"
SYMBOLS="BTC/USD,ETH/USD,SOL/USD,XRP/USD,ADA/USD,LINK/USD"
TELEGRAM="false"
PAUSED_START="false"
AUTO_REPLAY_GATE="true"
REPLAY_SEEDS="42,43"
REPLAY_DATA_DIR="data/replay"
REPLAY_TIMEOUT_SECONDS="1200"
AUTO_QUEUE_PROMOTION="true"
OBJECTIVE_MODE="net_pnl_only"
SYMBOL_BY_SYMBOL="true"
SYMBOLS_FROM_LIVE_UNIVERSE="true"
UNTIL_CONVERGENCE="true"
MAX_STAGNANT_ITERS="20"
MAX_ITERS_PER_SYMBOL="300"
AUTO_BACKFILL_DATA="true"
REPLAY_TIMEFRAMES="1m,15m,1h,4h,1d"
RUN_ID=""
CANDIDATE_ID=""
ALLOW_OVERLAP="false"
CRON_EXPR="15 2 * * *"
WATCHDOG_DRY_RUN="false"
WATCHDOG_CRON_EXPR="*/10 * * * *"

usage() {
  cat <<'EOF'
Usage:
  scripts/research_control.sh <command> [options]

Commands:
  start     Start isolated autoresearch run on server
  start-continuous  Start continuous supervisor loop on server
  status-continuous Show continuous supervisor status
  stop-continuous   Request continuous supervisor stop
  watchdog-run      Run research recovery watchdog once
  watchdog-install-schedule  Install watchdog cron on server
  watchdog-schedule-status   Show watchdog cron entries
  watchdog-remove-schedule   Remove watchdog cron entries
  status    Print current run status
  pause     Pause current run
  resume    Resume current run
  stop      Request graceful stop for current run
  promote   Queue candidate for review promotion
  logs      Tail or print run logs
  cleanup   Stop current run and remove artifacts
  install-schedule  Install optional nightly cron trigger on server
  schedule-status   Show configured nightly trigger schedule
  remove-schedule   Remove scheduled trigger from server

Options:
  --run-id <id>           Explicit run id (for all commands)
  --mode <mock|backtest>  Research evaluation mode (start only)
  --iterations <n>        Candidate iterations (used when not convergence mode)
  --days <n>              Lookback days (start only)
  --symbols <csv>         Comma-separated symbol list (start only)
  --objective-mode <m>    risk_adjusted|net_pnl_only (default: net_pnl_only)
  --symbol-by-symbol      Optimize each symbol independently
  --symbols-from-live-universe  Build symbol list from live config universe
  --until-convergence     Run each symbol until stagnant threshold
  --max-stagnant-iters <n> Stop symbol search after N no-improve iterations
  --max-iters-per-symbol <n> Safety cap per symbol under convergence mode
  --no-auto-backfill-data Disable replay data backfill preflight
  --replay-timeframes <csv> Timeframes to validate/export for replay
  --telegram              Enable telegram for research process (start only)
  --paused-start          Create run and start in paused mode (start only)
  --no-auto-replay-gate  Disable automatic replay gate run at end of loop
  --replay-seeds <csv>   Comma-separated replay seeds (default: 42,43)
  --replay-data-dir <p>  Replay harness data dir (default: data/replay)
  --replay-timeout-seconds <n> Timeout per replay seed (default: 1200)
  --no-auto-queue-promotion  Do not auto-queue promotion on replay pass
  --allow-overlap         Allow start even if another research process exists
  --cron "<expr>"         Cron expression for install-schedule (default: 15 2 * * *)
  --candidate <id>        Candidate id for promote command
  --follow                Follow logs continuously (logs only)
  --dry-run               Watchdog-only: evaluate and log without mutations

Notes:
  - This script writes run data under /home/trading/TradingSystem/data/research/<run_id>/
  - It sets a per-run sqlite DATABASE_URL to avoid touching production DB.
EOF
}

require_ssh() {
  if [[ ! -f "${SSH_KEY}" ]]; then
    echo "SSH key not found: ${SSH_KEY}" >&2
    exit 1
  fi
  ssh -i "${SSH_KEY}" -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new "${SERVER}" "echo ok" >/dev/null
}

current_run_id() {
  if [[ -n "${RUN_ID}" ]]; then
    echo "${RUN_ID}"
    return 0
  fi
  local rid
  rid="$(ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} bash -lc 'test -f ${CURRENT_RUN_FILE} && cat ${CURRENT_RUN_FILE} || true'")"
  if [[ -z "${rid}" ]]; then
    echo "No current run id found. Pass --run-id or run 'start' first." >&2
    exit 1
  fi
  echo "${rid}"
}

parse_common_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --run-id)
        RUN_ID="$2"
        shift 2
        ;;
      --mode)
        MODE="$2"
        shift 2
        ;;
      --iterations)
        ITERATIONS="$2"
        shift 2
        ;;
      --days)
        DAYS="$2"
        shift 2
        ;;
      --symbols)
        SYMBOLS="$2"
        shift 2
        ;;
      --objective-mode)
        OBJECTIVE_MODE="$2"
        shift 2
        ;;
      --symbol-by-symbol)
        SYMBOL_BY_SYMBOL="true"
        shift
        ;;
      --symbols-from-live-universe)
        SYMBOLS_FROM_LIVE_UNIVERSE="true"
        shift
        ;;
      --until-convergence)
        UNTIL_CONVERGENCE="true"
        shift
        ;;
      --max-stagnant-iters)
        MAX_STAGNANT_ITERS="$2"
        shift 2
        ;;
      --max-iters-per-symbol)
        MAX_ITERS_PER_SYMBOL="$2"
        shift 2
        ;;
      --no-auto-backfill-data)
        AUTO_BACKFILL_DATA="false"
        shift
        ;;
      --replay-timeframes)
        REPLAY_TIMEFRAMES="$2"
        shift 2
        ;;
      --telegram)
        TELEGRAM="true"
        shift
        ;;
      --paused-start)
        PAUSED_START="true"
        shift
        ;;
      --no-auto-replay-gate)
        AUTO_REPLAY_GATE="false"
        shift
        ;;
      --replay-seeds)
        REPLAY_SEEDS="$2"
        shift 2
        ;;
      --replay-data-dir)
        REPLAY_DATA_DIR="$2"
        shift 2
        ;;
      --replay-timeout-seconds)
        REPLAY_TIMEOUT_SECONDS="$2"
        shift 2
        ;;
      --no-auto-queue-promotion)
        AUTO_QUEUE_PROMOTION="false"
        shift
        ;;
      --candidate)
        CANDIDATE_ID="$2"
        shift 2
        ;;
      --allow-overlap)
        ALLOW_OVERLAP="true"
        shift
        ;;
      --cron)
        CRON_EXPR="$2"
        shift 2
        ;;
      --follow)
        FOLLOW="true"
        shift
        ;;
      --dry-run)
        WATCHDOG_DRY_RUN="true"
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "Unknown option: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
}

cmd_start() {
  require_ssh
  if [[ -z "${RUN_ID}" ]]; then
    RUN_ID="research_$(date -u +%Y%m%d_%H%M%S)"
  fi
  local run_dir="${RESEARCH_ROOT}/${RUN_ID}"
  local state_file="${run_dir}/state.json"
  local log_file="${run_dir}/run.log"
  local pid_file="${run_dir}/pid"
  local out_dir="${run_dir}/artifacts"
  local telegram_flag="--no-telegram"
  local auto_replay_flag="--auto-replay-gate"
  local auto_queue_flag="--auto-queue-promotion"
  local symbol_mode_flag="--no-symbol-by-symbol"
  local symbols_live_flag="--no-symbols-from-live-universe"
  local convergence_flag="--no-until-convergence"
  local backfill_flag="--auto-backfill-data"
  if [[ "${TELEGRAM}" == "true" ]]; then
    telegram_flag="--telegram"
  fi
  if [[ "${AUTO_REPLAY_GATE}" != "true" ]]; then
    auto_replay_flag="--no-auto-replay-gate"
  fi
  if [[ "${AUTO_QUEUE_PROMOTION}" != "true" ]]; then
    auto_queue_flag="--no-auto-queue-promotion"
  fi
  if [[ "${SYMBOL_BY_SYMBOL}" == "true" ]]; then
    symbol_mode_flag="--symbol-by-symbol"
  fi
  if [[ "${SYMBOLS_FROM_LIVE_UNIVERSE}" == "true" ]]; then
    symbols_live_flag="--symbols-from-live-universe"
  fi
  if [[ "${UNTIL_CONVERGENCE}" == "true" ]]; then
    convergence_flag="--until-convergence"
  fi
  if [[ "${AUTO_BACKFILL_DATA}" != "true" ]]; then
    backfill_flag="--no-auto-backfill-data"
  fi

  if [[ "${ALLOW_OVERLAP}" != "true" ]]; then
    local existing
    existing="$(ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} pgrep -af 'run.py research' || true")"
    if [[ -n "${existing}" ]]; then
      echo "Another research process is already running. Use --allow-overlap to bypass." >&2
      echo "${existing}" >&2
      exit 1
    fi
  fi

  ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} bash -lc '
    set -euo pipefail
    mkdir -p \"${run_dir}\" \"${out_dir}\"
    python3 - <<\"PY\"
import json
state = {
  \"run_id\": None,
  \"phase\": \"idle\",
  \"iteration\": 0,
  \"total_iterations\": 0,
  \"best_candidate_id\": None,
  \"leaderboard\": [],
  \"control\": {\"paused\": ${PAUSED_START}, \"stop_requested\": False},
  \"pending_prompt\": None,
  \"promotion_queue\": [],
  \"last_error\": None,
  \"updated_at\": \"\"
}
with open(\"${state_file}\", \"w\", encoding=\"utf-8\") as f:
    json.dump(state, f, indent=2)
PY
    if [ ! -f \"${TRADING_DIR}/.env\" ]; then
      echo \"Missing ${TRADING_DIR}/.env on server\" >&2
      exit 1
    fi
    set -a
    source \"${TRADING_DIR}/.env\"
    set +a
    export DATABASE_URL=\"sqlite:///${run_dir}/research.db\"
    nohup \"${TRADING_DIR}/venv/bin/python3\" \"${TRADING_DIR}/run.py\" research \
      --mode \"${MODE}\" \
      --iterations \"${ITERATIONS}\" \
      --days \"${DAYS}\" \
      --symbols \"${SYMBOLS}\" \
      --objective-mode \"${OBJECTIVE_MODE}\" \
      ${symbol_mode_flag} \
      ${symbols_live_flag} \
      ${convergence_flag} \
      --max-stagnant-iterations \"${MAX_STAGNANT_ITERS}\" \
      --max-iterations-per-symbol \"${MAX_ITERS_PER_SYMBOL}\" \
      ${auto_replay_flag} \
      --replay-seeds \"${REPLAY_SEEDS}\" \
      --replay-data-dir \"${REPLAY_DATA_DIR}\" \
      --replay-timeframes \"${REPLAY_TIMEFRAMES}\" \
      --replay-timeout-seconds \"${REPLAY_TIMEOUT_SECONDS}\" \
      ${backfill_flag} \
      ${auto_queue_flag} \
      ${telegram_flag} \
      --state-file \"${state_file}\" \
      --out-dir \"${out_dir}\" \
      > \"${log_file}\" 2>&1 &
    echo \$! > \"${pid_file}\"
    echo \"${RUN_ID}\" > \"${CURRENT_RUN_FILE}\"
    echo \"started run_id=${RUN_ID} pid=\$(cat ${pid_file})\"
    echo \"state=${state_file}\"
    echo \"log=${log_file}\"
  '"
}

cmd_start_continuous() {
  require_ssh
  ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} bash -lc '
    set -euo pipefail
    cd \"${TRADING_DIR}\"
    chmod +x \"${TRADING_DIR}/scripts/research_continuous.sh\"
    mkdir -p data/research/continuous_daemon/logs
    if [ -f data/research/continuous_daemon/daemon.pid ]; then
      PID=\$(cat data/research/continuous_daemon/daemon.pid || true)
      if [ -n \"\${PID:-}\" ] && kill -0 \"\${PID}\" 2>/dev/null; then
        echo \"continuous daemon already running pid=\${PID}\"
        exit 0
      fi
    fi
    rm -f data/research/continuous_daemon/stop_requested
    nohup \"${TRADING_DIR}/scripts/research_continuous.sh\" > data/research/continuous_daemon/logs/daemon.out 2>&1 &
    sleep 1
    if [ -f data/research/continuous_daemon/daemon.pid ]; then
      echo \"continuous daemon started pid=\$(cat data/research/continuous_daemon/daemon.pid)\"
    else
      echo \"continuous daemon start requested (pid file pending)\"
    fi
  '"
}

cmd_status_continuous() {
  require_ssh
  ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} bash -lc '
    set -euo pipefail
    cd \"${TRADING_DIR}\"
    PID_FILE=data/research/continuous_daemon/daemon.pid
    LATEST_FILE=data/research/continuous_daemon/latest_run_id
    echo \"daemon_pid=\$(cat \"\${PID_FILE}\" 2>/dev/null || echo none)\"
    if [ -f \"\${PID_FILE}\" ]; then
      PID=\$(cat \"\${PID_FILE}\" || true)
      if [ -n \"\${PID:-}\" ] && kill -0 \"\${PID}\" 2>/dev/null; then
        echo \"daemon_running=true\"
      else
        echo \"daemon_running=false\"
      fi
    else
      echo \"daemon_running=false\"
    fi
    echo \"latest_run_id=\$(cat \"\${LATEST_FILE}\" 2>/dev/null || echo none)\"
  '"
}

cmd_stop_continuous() {
  require_ssh
  ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} bash -lc '
    set -euo pipefail
    cd \"${TRADING_DIR}\"
    mkdir -p data/research/continuous_daemon
    touch data/research/continuous_daemon/stop_requested
    if [ -f data/research/continuous_daemon/daemon.pid ]; then
      PID=\$(cat data/research/continuous_daemon/daemon.pid || true)
      if [ -n \"\${PID:-}\" ]; then
        kill \"\${PID}\" 2>/dev/null || true
      fi
    fi
    echo \"continuous daemon stop requested\"
  '"
}

cmd_watchdog_run() {
  require_ssh
  local dry_flag=""
  if [[ "${WATCHDOG_DRY_RUN}" == "true" ]]; then
    dry_flag="--dry-run"
  fi
  ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} bash -lc '
    set -euo pipefail
    cd \"${TRADING_DIR}\"
    mkdir -p data/research/continuous_daemon/logs
    bash \"${TRADING_DIR}/scripts/research_recovery_watchdog.sh\" ${dry_flag}
  '"
}

cmd_watchdog_install_schedule() {
  require_ssh
  local cron_line="${WATCHDOG_CRON_EXPR} cd ${TRADING_DIR} && bash scripts/research_recovery_watchdog.sh >> data/research/continuous_daemon/logs/watchdog.log 2>&1"
  ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} bash -lc '
    set -euo pipefail
    EXISTING=\$(crontab -l 2>/dev/null || true)
    FILTERED=\$(printf \"%s\n\" \"\${EXISTING}\" | grep -v \"scripts/research_recovery_watchdog.sh\" || true)
    { printf \"%s\n\" \"\${FILTERED}\"; printf \"%s\n\" \"${cron_line}\"; } | crontab -
    echo \"installed watchdog schedule: ${cron_line}\"
  '"
}

cmd_watchdog_schedule_status() {
  require_ssh
  ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} bash -lc '
    CRON=\$(crontab -l 2>/dev/null || true)
    echo \"Current watchdog schedule entries:\"
    echo \"\${CRON}\" | grep \"scripts/research_recovery_watchdog.sh\" || echo \"(none)\"
  '"
}

cmd_watchdog_remove_schedule() {
  require_ssh
  ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} bash -lc '
    set -euo pipefail
    EXISTING=\$(crontab -l 2>/dev/null || true)
    FILTERED=\$(printf \"%s\n\" \"\${EXISTING}\" | grep -v \"scripts/research_recovery_watchdog.sh\" || true)
    printf \"%s\n\" \"\${FILTERED}\" | crontab -
    echo \"removed watchdog schedule entries\"
  '"
}

cmd_status() {
  require_ssh
  local rid
  rid="$(current_run_id)"
  ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} python3 - <<'PY'
import json
from pathlib import Path
rid = '${rid}'
p = Path('${RESEARCH_ROOT}') / rid / 'state.json'
if not p.exists():
    raise SystemExit(f'state file not found: {p}')
s = json.load(open(p))
print('run_id=', rid)
print('phase=', s.get('phase'))
print('iter=', s.get('iteration'), '/', s.get('total_iterations'))
print('current_symbol=', s.get('current_symbol'))
print('symbols_done=', len(s.get('completed_symbols') or []), '/', s.get('total_symbols'))
print('best=', s.get('best_candidate_id'))
print('paused=', (s.get('control') or {}).get('paused'))
print('stop_requested=', (s.get('control') or {}).get('stop_requested'))
print('pending_prompt=', bool(s.get('pending_prompt')))
print('promotion_queue=', len(s.get('promotion_queue') or []))
print('updated_at=', s.get('updated_at'))
PY"
}

cmd_set_paused() {
  require_ssh
  local rid="$1"
  local paused="$2"
  ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} python3 - <<'PY'
import json
from pathlib import Path
rid='${rid}'
p = Path('${RESEARCH_ROOT}') / rid / 'state.json'
s = json.load(open(p))
s.setdefault('control', {})
s['control']['paused'] = ${paused}
json.dump(s, open(p, 'w'), indent=2)
print('updated paused=', s['control']['paused'], 'run_id=', rid)
PY"
}

cmd_stop() {
  require_ssh
  local rid
  rid="$(current_run_id)"
  ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} python3 - <<'PY'
import json
from pathlib import Path
rid='${rid}'
p = Path('${RESEARCH_ROOT}') / rid / 'state.json'
s = json.load(open(p))
s.setdefault('control', {})
s['control']['stop_requested'] = True
s['control']['paused'] = False
json.dump(s, open(p, 'w'), indent=2)
print('stop requested for', rid)
PY"
}

cmd_promote() {
  require_ssh
  if [[ -z "${CANDIDATE_ID}" ]]; then
    echo "promote requires --candidate <id>" >&2
    exit 1
  fi
  local rid
  rid="$(current_run_id)"
  ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} python3 - <<'PY'
import json
from pathlib import Path
rid='${rid}'
candidate='${CANDIDATE_ID}'
p = Path('${RESEARCH_ROOT}') / rid / 'state.json'
s = json.load(open(p))
queue = list(s.get('promotion_queue') or [])
if candidate not in queue:
    queue.append(candidate)
s['promotion_queue'] = queue
json.dump(s, open(p, 'w'), indent=2)
print('queued candidate', candidate, 'run_id=', rid)
PY"
}

cmd_logs() {
  require_ssh
  local rid
  rid="$(current_run_id)"
  local log_path="${RESEARCH_ROOT}/${rid}/run.log"
  if [[ "${FOLLOW:-false}" == "true" ]]; then
    ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} tail -n 120 -f '${log_path}'"
  else
    ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} tail -n 120 '${log_path}'"
  fi
}

cmd_cleanup() {
  require_ssh
  local rid
  rid="$(current_run_id)"
  local run_dir="${RESEARCH_ROOT}/${rid}"
  ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} bash -lc '
    set -euo pipefail
    if [ -f \"${run_dir}/pid\" ]; then
      PID=\$(cat \"${run_dir}/pid\" || true)
      if [ -n \"\${PID:-}\" ]; then
        kill \"\${PID}\" 2>/dev/null || true
      fi
    fi
    pkill -f \"state-file ${run_dir}/state.json\" 2>/dev/null || true
    rm -rf \"${run_dir}\"
    CUR=\"${CURRENT_RUN_FILE}\"
    if [ -f \"\${CUR}\" ] && [ \"\$(cat \"\${CUR}\")\" = \"${rid}\" ]; then
      rm -f \"\${CUR}\"
    fi
    echo \"cleaned run_id=${rid}\"
  '"
}

cmd_install_schedule() {
  require_ssh
  local cron_line="${CRON_EXPR} ${TRADING_DIR}/scripts/research_nightly.sh >> ${TRADING_DIR}/data/research/nightly_cron.log 2>&1"
  ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} bash -lc '
    set -euo pipefail
    mkdir -p \"${RESEARCH_ROOT}\"
    EXISTING=\$(crontab -l 2>/dev/null || true)
    FILTERED=\$(printf \"%s\n\" \"\${EXISTING}\" | grep -v \"scripts/research_nightly.sh\" || true)
    { printf \"%s\n\" \"\${FILTERED}\"; printf \"%s\n\" \"${cron_line}\"; } | crontab -
    echo \"installed schedule: ${cron_line}\"
  '"
}

cmd_schedule_status() {
  require_ssh
  ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} bash -lc '
    CRON=\$(crontab -l 2>/dev/null || true)
    echo \"Current schedule entries:\"
    echo \"\${CRON}\" | grep \"scripts/research_nightly.sh\" || echo \"(none)\"
  '"
}

cmd_remove_schedule() {
  require_ssh
  ssh -i "${SSH_KEY}" "${SERVER}" "sudo -u ${TRADING_USER} bash -lc '
    set -euo pipefail
    EXISTING=\$(crontab -l 2>/dev/null || true)
    FILTERED=\$(printf \"%s\n\" \"\${EXISTING}\" | grep -v \"scripts/research_nightly.sh\" || true)
    printf \"%s\n\" \"\${FILTERED}\" | crontab -
    echo \"removed schedule entries for scripts/research_nightly.sh\"
  '"
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

COMMAND="$1"
shift
FOLLOW="false"
parse_common_args "$@"

case "${COMMAND}" in
  start)
    cmd_start
    ;;
  start-continuous)
    cmd_start_continuous
    ;;
  status-continuous)
    cmd_status_continuous
    ;;
  stop-continuous)
    cmd_stop_continuous
    ;;
  watchdog-run)
    cmd_watchdog_run
    ;;
  watchdog-install-schedule)
    cmd_watchdog_install_schedule
    ;;
  watchdog-schedule-status)
    cmd_watchdog_schedule_status
    ;;
  watchdog-remove-schedule)
    cmd_watchdog_remove_schedule
    ;;
  status)
    cmd_status
    ;;
  pause)
    cmd_set_paused "$(current_run_id)" "True"
    ;;
  resume)
    cmd_set_paused "$(current_run_id)" "False"
    ;;
  stop)
    cmd_stop
    ;;
  promote)
    cmd_promote
    ;;
  logs)
    cmd_logs
    ;;
  cleanup)
    cmd_cleanup
    ;;
  install-schedule)
    cmd_install_schedule
    ;;
  schedule-status)
    cmd_schedule_status
    ;;
  remove-schedule)
    cmd_remove_schedule
    ;;
  *)
    echo "Unknown command: ${COMMAND}" >&2
    usage
    exit 1
    ;;
esac

