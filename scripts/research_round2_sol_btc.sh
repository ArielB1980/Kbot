#!/usr/bin/env bash
# Round 2: SOL/BTC research with relaxed signal gates.
# - promotion_min_signal_trades lowered from 50 to 5
# - mutate_step_pct raised from 3% to 5% (more aggressive exploration)
# - higher_tf_penalty_outside_zone now in allowlist (optimizer can zero it out)
# - Fresh warm-start (delete old poisoned files)

set -euo pipefail
cd /home/trading/TradingSystem
set -a && source .env && set +a

PYTHON=/home/trading/TradingSystem/venv/bin/python
LOG_DIR="/tmp/research_round2"
mkdir -p "$LOG_DIR"

log() { echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] $*" | tee -a "$LOG_DIR/orchestrator.log"; }

# Clean warm-start for SOL/BTC only
rm -f data/research/warm_start/SOL_USD_best.json
rm -f data/research/warm_start/BTC_USD_best.json
log "Cleared SOL/BTC warm-start files"

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
        iterations=80,
        lookback_days=365,
        symbols=('${SYMBOL}',),
        evaluation_mode='backtest',
        evaluation_window_offsets_days=(96, 0),
        holdout_ratio=0.30,
        symbol_by_symbol=True,
        until_convergence=True,
        max_stagnant_iterations=40,
        max_iterations_per_symbol=80,
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

log "=== Round 2: SOL/BTC with relaxed gates ==="
log "Changes: promotion_min_signal_trades=5, mutate_step=5%, htf_penalty in allowlist"

# Run SOL and BTC in parallel
run_research "SOL/USD" &
SOL_PID=$!
run_research "BTC/USD" &
BTC_PID=$!

log "SOL PID=$SOL_PID, BTC PID=$BTC_PID"

wait $SOL_PID 2>/dev/null && log "SOL research finished." || log "SOL research exited with error."
wait $BTC_PID 2>/dev/null && log "BTC research finished." || log "BTC research exited with error."

log "Round 2 complete. Results in $LOG_DIR/"
