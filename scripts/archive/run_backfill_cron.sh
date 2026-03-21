#!/usr/bin/env bash
# Cron-friendly wrapper for historical backfill.
# Usage: run from project root, or pass WORKDIR.
#   */0 3 * * 0 cd /path/to/TradingSystem && ./scripts/run_backfill_cron.sh
# Requires .env.local (DATABASE_URL, Kraken keys). Uses .venv.

set -e
WORKDIR="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$WORKDIR"

if [ -f .env.local ]; then
  set -a
  # shellcheck source=/dev/null
  source .env.local
  set +a
fi

export ENV="${ENV:-local}"
export ENVIRONMENT="${ENVIRONMENT:-dev}"
PYTHON="${WORKDIR}/.venv/bin/python"
BACKFILL="${WORKDIR}/scripts/backfill_historical_data.py"

if [ ! -x "$PYTHON" ] || [ ! -f "$BACKFILL" ]; then
  echo "run_backfill_cron: .venv or backfill script missing" >&2
  exit 1
fi

"$PYTHON" "$BACKFILL"
