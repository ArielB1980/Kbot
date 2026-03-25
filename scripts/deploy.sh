#!/usr/bin/env bash
set -euo pipefail

REMOTE_HOST="${DEPLOY_HOST:-kbot}"
REMOTE_DIR="${DEPLOY_DIR:-/home/trading/TradingSystem}"
SERVICE_NAME="${DEPLOY_SERVICE_NAME:-trading-bot.service}"
LOG_LINES="${DEPLOY_LOG_LINES:-40}"
SSH_OPTS=("-o" "BatchMode=yes" "-o" "ConnectTimeout=15")

step() {
  printf '\n[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1"
}

step "Deploying to ${REMOTE_HOST}:${REMOTE_DIR}"

LOCAL_ORIGIN_URL="$(git remote get-url origin)"

ssh "${SSH_OPTS[@]}" "${REMOTE_HOST}" "bash -s" <<REMOTE_EOF
set -euo pipefail

REPO_DIR="${REMOTE_DIR}"
SERVICE="${SERVICE_NAME}"
LOG_LINES="${LOG_LINES}"
FALLBACK_ORIGIN_URL="${LOCAL_ORIGIN_URL}"

cd "\${REPO_DIR}"
echo "remote_pwd=\$(pwd)"
echo "remote_head_before=\$(git rev-parse HEAD)"
echo "remote_origin_before=\$(git remote get-url origin)"

if ! git ls-remote --exit-code origin >/dev/null 2>&1; then
  if [ -n "\${FALLBACK_ORIGIN_URL}" ]; then
    echo "Origin remote unreachable; switching to fallback: \${FALLBACK_ORIGIN_URL}"
    git remote set-url origin "\${FALLBACK_ORIGIN_URL}"
  fi
fi

echo "Running: git pull origin main"
git pull origin main

echo "remote_head_after=\$(git rev-parse HEAD)"

if command -v systemctl >/dev/null 2>&1; then
  if systemctl restart "\${SERVICE}" >/dev/null 2>&1; then
    echo "Restarted systemd service: \${SERVICE}"
    systemctl is-active --quiet "\${SERVICE}"
    systemctl status "\${SERVICE}" --no-pager | sed -n '1,20p'
    echo "Recent journal logs:"
    journalctl -u "\${SERVICE}" -n "\${LOG_LINES}" --no-pager
    exit 0
  fi
fi

if command -v supervisorctl >/dev/null 2>&1; then
  if supervisorctl status 2>/dev/null | grep -q '^trading-bot'; then
    echo "Restarting supervisor service: trading-bot"
    supervisorctl restart trading-bot
    supervisorctl status trading-bot
    exit 0
  fi
fi

echo "No recognized service manager entry found for trading bot" >&2
exit 1
REMOTE_EOF

step "Deploy finished successfully"
