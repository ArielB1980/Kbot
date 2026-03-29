#!/usr/bin/env bash
set -euo pipefail

REMOTE_HOST="${DEPLOY_HOST:-kbot}"
REMOTE_DIR="${DEPLOY_DIR:-/home/trading/TradingSystem}"
SERVICE_NAME="${DEPLOY_SERVICE_NAME:-trading-bot.service}"
DASHBOARD_SERVICE_NAME="${DEPLOY_DASHBOARD_SERVICE_NAME:-trading-dashboard.service}"
SYSTEMD_DIR="${DEPLOY_SYSTEMD_DIR:-/etc/systemd/system}"
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
DASHBOARD_SERVICE="${DASHBOARD_SERVICE_NAME}"
REMOTE_SYSTEMD_DIR="${SYSTEMD_DIR}"
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
  if command -v sudo >/dev/null 2>&1; then
    if [ -f scripts/trading-system.service ]; then
      sudo cp scripts/trading-system.service "\${REMOTE_SYSTEMD_DIR}/trading-bot.service"
      echo "Installed unit: \${REMOTE_SYSTEMD_DIR}/trading-bot.service"
    fi
    if [ -f scripts/trading-dashboard.service ]; then
      sudo cp scripts/trading-dashboard.service "\${REMOTE_SYSTEMD_DIR}/\${DASHBOARD_SERVICE}"
      echo "Installed unit: \${REMOTE_SYSTEMD_DIR}/\${DASHBOARD_SERVICE}"
    fi
    sudo systemctl daemon-reload
    sudo systemctl restart "\${SERVICE}"
    if systemctl list-unit-files | awk '{print \$1}' | grep -Fxq "\${DASHBOARD_SERVICE}"; then
      sudo systemctl restart "\${DASHBOARD_SERVICE}"
    else
      echo "Dashboard unit not installed on host, skipping restart: \${DASHBOARD_SERVICE}"
    fi

    echo "Service status:"
    systemctl status "\${SERVICE}" --no-pager | sed -n '1,20p'
    if systemctl list-unit-files | awk '{print \$1}' | grep -Fxq "\${DASHBOARD_SERVICE}"; then
      systemctl status "\${DASHBOARD_SERVICE}" --no-pager | sed -n '1,20p'
    fi

    if command -v curl >/dev/null 2>&1; then
      for path in /api /api/health /api/ready /api/metrics; do
        code=\$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:8080\${path}" || true)
        echo "health_probe \${path} -> \${code}"
      done
    fi

    echo "Recent journal logs (\${SERVICE}):"
    journalctl -u "\${SERVICE}" -n "\${LOG_LINES}" --no-pager
    if systemctl list-unit-files | awk '{print \$1}' | grep -Fxq "\${DASHBOARD_SERVICE}"; then
      echo "Recent journal logs (\${DASHBOARD_SERVICE}):"
      journalctl -u "\${DASHBOARD_SERVICE}" -n "\${LOG_LINES}" --no-pager
    fi
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
