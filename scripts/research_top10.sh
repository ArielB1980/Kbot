#!/usr/bin/env bash
# Research run: Top 10 coins with optimized R2 settings.
# Runs coins in batches of 3 to avoid API rate limiting.
# Uses the settings that produced ETH +8.28% and SOL +0.56%.

set -euo pipefail
cd /home/trading/TradingSystem
set -a && source .env && set +a

PYTHON=/home/trading/TradingSystem/venv/bin/python
LOG_DIR="/tmp/research_top10"
mkdir -p "$LOG_DIR"

log() { echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] $*" | tee -a "$LOG_DIR/orchestrator.log"; }

# Clear old warm-start files for all symbols
rm -f data/research/warm_start/*_best.json 2>/dev/null
log "Cleared all warm-start files"

run_research() {
    local SYMBOL="$1"
    local SAFE=$(echo "$SYMBOL" | tr '/' '_')
    log "RESEARCH START: $SYMBOL"
    $PYTHON -c "
import asyncio
import json
from pathlib import Path
from src.config.config import load_config
from src.research.harness import SandboxAutoresearchHarness, HarnessConfig
from src.research.state_store import ResearchStateStore

async def run():
    config = load_config()
    harness_config = HarnessConfig(
        iterations=60,
        lookback_days=365,
        symbols=('${SYMBOL}',),
        evaluation_mode='backtest',
        evaluation_window_offsets_days=(96, 0),
        holdout_ratio=0.30,
        symbol_by_symbol=True,
        until_convergence=True,
        max_stagnant_iterations=30,
        max_iterations_per_symbol=60,
        mutate_step_pct=5.0,
        mutate_params_per_candidate=8,
        promotion_min_signal_trades=5,
        replay_timeframes=('15m', '1h', '4h', '1d'),
        enable_telegram=True,
        objective_mode='risk_adjusted',
        out_dir='data/research',
        replay_eval_timeout_seconds=7200,
        replay_max_ticks=250000,
    )
    store = ResearchStateStore(path=Path('data/research/state'))
    harness = SandboxAutoresearchHarness(config, harness_config, store)
    leaderboard_path, summary_path = await harness.run()
    print(f'LEADERBOARD: {leaderboard_path}')
    print(f'SUMMARY: {summary_path}')

    results_file = Path('${LOG_DIR}/${SAFE}_results.json')
    best = harness.best_by_symbol.get('${SYMBOL}')
    if best:
        results_file.write_text(json.dumps({
            'candidate_id': best.candidate_id,
            'score': best.score,
            'accepted': best.accepted,
            'params': best.params,
            'metrics': {
                'net_return_pct': best.metrics.net_return_pct,
                'max_drawdown_pct': best.metrics.max_drawdown_pct,
                'sharpe': best.metrics.sharpe,
                'sortino': best.metrics.sortino,
                'win_rate_pct': best.metrics.win_rate_pct,
                'trade_count': best.metrics.trade_count,
            },
            'metadata': {k: v for k, v in best.metadata.items() if not isinstance(v, bytes)},
        }, indent=2, default=str))
        print(f'Best result saved to {results_file}')

asyncio.run(run())
" >> "$LOG_DIR/research_${SAFE}.log" 2>&1
    log "RESEARCH DONE: $SYMBOL"
}

ALL_SYMBOLS=(
    "BTC/USD"  "ETH/USD"  "SOL/USD"
    "XRP/USD"  "ADA/USD"  "DOGE/USD"
    "AVAX/USD" "DOT/USD"  "LINK/USD"
    "SUI/USD"
)

log "=== Top 10 Research Run ==="
log "Symbols: ${ALL_SYMBOLS[*]}"
log "Settings: promotion_min_trades=5, mutate_step=5%, htf_penalty mutable, 60 iterations"

# Batch 1: The proven trio (already have deep data)
log "--- Batch 1: BTC, ETH, SOL ---"
run_research "BTC/USD" &
P1=$!
run_research "ETH/USD" &
P2=$!
run_research "SOL/USD" &
P3=$!
log "Batch 1 PIDs: $P1 $P2 $P3"
wait $P1 2>/dev/null && log "BTC done." || log "BTC error."
wait $P2 2>/dev/null && log "ETH done." || log "ETH error."
wait $P3 2>/dev/null && log "SOL done." || log "SOL error."

# Batch 2: Mid-cap alts
log "--- Batch 2: XRP, ADA, DOGE ---"
run_research "XRP/USD" &
P4=$!
run_research "ADA/USD" &
P5=$!
run_research "DOGE/USD" &
P6=$!
log "Batch 2 PIDs: $P4 $P5 $P6"
wait $P4 2>/dev/null && log "XRP done." || log "XRP error."
wait $P5 2>/dev/null && log "ADA done." || log "ADA error."
wait $P6 2>/dev/null && log "DOGE done." || log "DOGE error."

# Batch 3: Infrastructure / DeFi
log "--- Batch 3: AVAX, DOT, LINK ---"
run_research "AVAX/USD" &
P7=$!
run_research "DOT/USD" &
P8=$!
run_research "LINK/USD" &
P9=$!
log "Batch 3 PIDs: $P7 $P8 $P9"
wait $P7 2>/dev/null && log "AVAX done." || log "AVAX error."
wait $P8 2>/dev/null && log "DOT done." || log "DOT error."
wait $P9 2>/dev/null && log "LINK done." || log "LINK error."

# Batch 4: New gen
log "--- Batch 4: SUI ---"
run_research "SUI/USD" &
P10=$!
log "Batch 4 PID: $P10"
wait $P10 2>/dev/null && log "SUI done." || log "SUI error."

# Summary
log "=== All batches complete ==="
log "Generating summary..."

$PYTHON -c "
import json
from pathlib import Path

symbols = ['BTC_USD','ETH_USD','SOL_USD','XRP_USD','ADA_USD','DOGE_USD','AVAX_USD','DOT_USD','LINK_USD','SUI_USD']
report = []
report.append('=' * 70)
report.append('TOP 10 RESEARCH RESULTS')
report.append('=' * 70)
report.append(f'{\"Symbol\":<12s} {\"Return\":>8s} {\"MaxDD\":>8s} {\"Sharpe\":>8s} {\"Win%\":>8s} {\"Trades\":>8s} {\"Candidate\":<20s}')
report.append('-' * 70)

results = []
for sym in symbols:
    path = Path('/tmp/research_top10') / f'{sym}_results.json'
    if not path.exists():
        report.append(f'{sym:<12s}  NO RESULTS')
        continue
    d = json.loads(path.read_text())
    m = d['metrics']
    report.append(f'{sym:<12s} {m[\"net_return_pct\"]:>+7.2f}% {m[\"max_drawdown_pct\"]:>7.2f}% {m[\"sharpe\"]:>8.2f} {m[\"win_rate_pct\"]:>7.1f}% {m[\"trade_count\"]:>8d} {d[\"candidate_id\"]:<20s}')
    results.append({'symbol': sym, **m, 'candidate_id': d['candidate_id']})

report.append('-' * 70)
profitable = [r for r in results if r['net_return_pct'] > 0]
report.append(f'Profitable: {len(profitable)}/{len(results)} symbols')
if profitable:
    avg_return = sum(r['net_return_pct'] for r in profitable) / len(profitable)
    report.append(f'Avg return (profitable): {avg_return:+.2f}%')

report.append('=' * 70)
text = '\n'.join(report)
print(text)
Path('/tmp/research_top10/SUMMARY.txt').write_text(text)
" 2>&1 | tee -a "$LOG_DIR/orchestrator.log"

log "Top 10 research complete."
