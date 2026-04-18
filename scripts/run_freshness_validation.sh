#!/usr/bin/env bash
# Parallel freshness validation replay (sandbox /home/trading/TS_freshness on droplet).
# Runs one alpha_combination_analysis process per symbol to saturate 4 vCPU,
# waits for all to finish, concatenates per-symbol JSONLs,
# then computes forward returns once on the combined file.
set -euo pipefail

cd /home/trading/TS_freshness
set -a; source .env; set +a

export REPLAY_OVERRIDE_FVG_MITIGATION_MODE=full
export PYTHONUNBUFFERED=1
export PYTHONPATH=/home/trading/TS_freshness

OUT=reports/freshness_validation
mkdir -p "$OUT"
SYMS=(ETH_USD BTC_USD SOL_USD LINK_USD XRP_USD)
SYM_PAIRS=(ETH/USD BTC/USD SOL/USD LINK/USD XRP/USD)

echo "[$(date -Is)] Starting 5 parallel symbol replays (400d, FVG mode=full)"

pids=()
for i in "${!SYMS[@]}"; do
  SAFE="${SYMS[$i]}"
  SYM="${SYM_PAIRS[$i]}"
  mkdir -p "$OUT/$SAFE"
  (
    echo "[$(date -Is)] $SYM: start"
    venv/bin/python scripts/alpha_combination_analysis.py \
      --generate-data \
      --lookback-days 400 \
      --symbols "$SYM" \
      --output-dir "$OUT/$SAFE" \
      > "$OUT/$SAFE/run.log" 2>&1
    rows=$(wc -l < "$OUT/$SAFE/decision_data.jsonl" 2>/dev/null || echo 0)
    echo "[$(date -Is)] $SYM: done (rows=$rows)"
  ) &
  pids+=($!)
done

fails=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    fails=$((fails+1))
  fi
done
echo "[$(date -Is)] Parallel phase done. Failures: $fails"

COMBINED_DIR="$OUT/combined"
mkdir -p "$COMBINED_DIR"
cat "$OUT"/*_USD/decision_data.jsonl > "$COMBINED_DIR/decision_data.jsonl" 2>/dev/null || true
TOTAL=$(wc -l < "$COMBINED_DIR/decision_data.jsonl" 2>/dev/null || echo 0)
echo "[$(date -Is)] Combined rows: $TOTAL"

if [ "$TOTAL" -eq 0 ]; then
  echo "[$(date -Is)] ERROR: no rows combined"
  exit 1
fi

echo "[$(date -Is)] Computing forward returns"
venv/bin/python scripts/alpha_combination_analysis.py \
  --input "$COMBINED_DIR/decision_data.jsonl" \
  --compute-forward-returns \
  --output-dir "$COMBINED_DIR" \
  > "$COMBINED_DIR/forward_returns.log" 2>&1

echo "[$(date -Is)] All done. Enriched: $COMBINED_DIR/decision_data_with_returns.jsonl"
