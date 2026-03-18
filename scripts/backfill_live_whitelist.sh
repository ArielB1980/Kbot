#!/usr/bin/env bash
#
# Backfill candle data for the live whitelist so the trading bot has OHLCV (avoids "No candles for X").
# Reads symbols from live_research_overrides.yaml assets.whitelist, or uses default 12-coin list.
# Run on the server (or anywhere with DATABASE_URL and Kraken API) before or after starting live.
#
# Usage:
#   ./scripts/backfill_live_whitelist.sh [--bootstrap-days 60]
#   BOOTSTRAP_DAYS=90 ./scripts/backfill_live_whitelist.sh
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
CONFIG_DIR="${TRADING_DIR}/src/config"
OVERRIDES="${CONFIG_DIR}/live_research_overrides.yaml"
BOOTSTRAP_DAYS="${BOOTSTRAP_DAYS:-60}"

# Default live whitelist if YAML missing or no whitelist
DEFAULT_SYMBOLS="BTC/USD,ETH/USD,SOL/USD,XRP/USD,ADA/USD,LINK/USD,DOGE/USD,AVAX/USD,LTC/USD,TRX/USD,DOT/USD,BCH/USD"

SYMBOLS="${DEFAULT_SYMBOLS}"
if [[ -f "${OVERRIDES}" ]]; then
  PARSED="$("${TRADING_DIR}/venv/bin/python3" - "${OVERRIDES}" <<'PY'
import sys, yaml
from pathlib import Path
p = Path(sys.argv[1])  # OVERRIDES path
if p.exists():
    d = yaml.safe_load(p.read_text()) or {}
    w = d.get("assets", {}).get("whitelist") or []
    if w:
        print(",".join(str(s) for s in w))
PY
)" || true
  if [[ -n "${PARSED}" ]]; then
    SYMBOLS="${PARSED}"
  fi
fi

echo "[$(date -u +%FT%TZ)] Backfilling live whitelist (bootstrap_days=${BOOTSTRAP_DAYS}): ${SYMBOLS}"
"${TRADING_DIR}/venv/bin/python3" "${TRADING_DIR}/scripts/collect_kraken_candles_continuous.py" \
  --symbols "${SYMBOLS}" \
  --timeframes "15m,1h,4h,1d" \
  --bootstrap-days "${BOOTSTRAP_DAYS}" \
  --once
echo "[$(date -u +%FT%TZ)] Backfill done"
