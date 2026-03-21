# Operations Runbook

## Promoted Tools (`src/tools/`)

All tools default to **dry-run**. Pass `--execute` to take real action.
If live API keys are detected, set `I_UNDERSTAND_LIVE=1` to proceed.

| Tool | Purpose | Modifies State? |
|------|---------|----------------|
| `python -m src.tools.sync_positions` | Sync exchange positions to local registry | Yes (with --execute) |
| `python -m src.tools.recover_sl_order_ids` | Fix positions with missing stop-loss order IDs | Yes (with --execute) |
| `python -m src.tools.check_tp_coverage` | Report TP order coverage for all positions | No (read-only) |
| `python -m src.tools.check_live_readiness` | Validate API connection and system readiness | No (read-only) |
| `python -m src.tools.pre_flight_check` | Full pre-flight check before live trading | No (read-only) |
| `python -m src.tools.check_db_health` | Database integrity and health report | No (read-only) |
| `python -m src.tools.monitor_trade_execution` | Execution quality metrics (slippage, fills) | No (read-only) |
| `python -m src.tools.backfill_historical_data` | Backfill OHLCV gaps in candle database | Yes (with --execute) |

## Deploy

```bash
make deploy          # commit, push, SSH pull, restart service
# or manually:
./scripts/deploy.sh
```

## Research -> Live Automation (Production)

### Daily apply job

Applies latest completed research run to live config (`live_research_overrides.yaml`):
- updates strategy overrides for existing live symbols
- optionally promotes new symbols when they pass promotion gates
- optionally restarts `trading-bot.service`

```bash
# one-off
cd /home/trading/TradingSystem
RESTART_LIVE_AFTER_APPLY=1 bash scripts/daily_apply_research_to_live.sh
```

Current production cron (user `trading`):

```cron
0 6 * * * RESTART_LIVE_AFTER_APPLY=1 cd /home/trading/TradingSystem && bash scripts/daily_apply_research_to_live.sh >> data/research/continuous_daemon/logs/daily_apply.log 2>&1
```

### Live candle reliability jobs

Backfills only live whitelist symbols (`assets.whitelist` in `src/config/live_research_overrides.yaml`) for `15m,1h,4h,1d`.

```bash
# one-off backfill
cd /home/trading/TradingSystem
BOOTSTRAP_DAYS=60 bash scripts/backfill_live_whitelist.sh
```

Current production cron (user `trading`):

```cron
*/20 * * * * cd /home/trading/TradingSystem && BOOTSTRAP_DAYS=2 bash scripts/backfill_live_whitelist.sh >> data/research/continuous_daemon/logs/live_whitelist_refresh.log 2>&1
15 2 * * 0 cd /home/trading/TradingSystem && BOOTSTRAP_DAYS=90 bash scripts/backfill_live_whitelist.sh >> data/research/continuous_daemon/logs/live_whitelist_weekly_backfill.log 2>&1
```

### Monitoring commands (live + research)

```bash
# Live
systemctl is-active trading-bot.service
journalctl -u trading-bot.service --since "30 minutes ago" --no-pager | grep -E "ENTRY_FUNNEL_SUMMARY|No candles for|ENTRY_BLOCKED_LOW_CONVICTION" | tail -50

# Research
pgrep -af "research_continuous|run.py research"
cat /home/trading/TradingSystem/data/research/continuous_daemon/latest_run_id
tail -50 /home/trading/TradingSystem/data/research/continuous_daemon/logs/daemon.log

# Daily apply / refresh logs
tail -50 /home/trading/TradingSystem/data/research/continuous_daemon/logs/daily_apply.log
tail -50 /home/trading/TradingSystem/data/research/continuous_daemon/logs/live_whitelist_refresh.log
```

### Automated baseline recovery loop (continuous research)

Continuous research now has two automated recovery layers:

- **In-daemon self-heal** (`scripts/research_continuous.sh`):
  - Pre-cycle override repair to enforce:
    - `RESEARCH_CONT_AUTO_BACKFILL_DATA=1`
    - `REPLAY_OVERRIDE_CONVICTION_MIN_FOR_ENTRY<=20`
  - PID drift repair (`daemon.pid` forced to current daemon process).
  - Bounded counterfactual post-hooks with timeouts:
    - `RESEARCH_CONT_CF_TIMEOUT_SECONDS` (default `300`)
    - `RESEARCH_CONT_CF_BATCH_TIMEOUT_SECONDS` (default `300`)
- **External watchdog** (`scripts/research_recovery_watchdog.sh`):
  - Detects daemon down, stale run state, stagnation, and override drift.
  - Auto-remediates by repairing overrides, restarting daemon, and optionally running targeted candle backfill.

Watchdog controls:

```bash
# Run once now
./scripts/research_control.sh watchdog-run

# Dry-run (detection only)
./scripts/research_control.sh watchdog-run --dry-run

# Install/remove cron for watchdog
./scripts/research_control.sh watchdog-install-schedule
./scripts/research_control.sh watchdog-schedule-status
./scripts/research_control.sh watchdog-remove-schedule
```

Key watchdog tuning env vars (server `.env`):

- `WATCHDOG_MAX_STATE_AGE_SECONDS` (default `1800`)
- `WATCHDOG_MAX_LOG_QUIET_SECONDS` (default `600`)
- `WATCHDOG_MAX_STAGNANT_SECONDS` (default `2400`)
- `WATCHDOG_CANONICAL_CONVICTION` (default `20`)
- `WATCHDOG_FORCE_AUTO_BACKFILL` (default `1`)
- `WATCHDOG_ENABLE_TARGETED_BACKFILL` (default `1`)
- `WATCHDOG_NO_CANDLES_SPIKE_THRESHOLD` (default `50`)
- `WATCHDOG_BACKFILL_BOOTSTRAP_DAYS` (default `30`)

## Kill Switch Recovery

1. **Check status**: `ssh <droplet> systemctl status trading-bot`
2. **View logs**: `ssh <droplet> journalctl -u trading-bot -n 200`
3. **Check halt reason**: Search logs for `KILL_SWITCH_ACTIVATED`
4. **If margin_critical (most common)**:
   - System auto-recovers if margin drops below threshold
   - Manual: restart service (`systemctl restart trading-bot`)
5. **If invariant_violation**:
   - Review FORAI.md for known patterns
   - Fix root cause before restart
   - Run `make smoke` locally to verify

## Position Import

When the system discovers exchange positions it doesn't track:

```bash
python -m src.tools.sync_positions --execute
```

This queries Kraken, creates registry entries, and places protective stops.

## TP Backfill

If positions are missing take-profit orders:

```bash
python -m src.tools.check_tp_coverage  # identify gaps
# then use the execution gateway's TP placement logic
```

## Telegram Commands

| Command | Action |
|---------|--------|
| `/status` | System status, positions, equity |
| `/positions` | Detailed position list |
| `/help` | Available commands |

## Sandbox Autoresearch Runbook

Use `scripts/research_control.sh` (or `make research-*`) for a repeatable workflow.

### Safety defaults

- Runs in isolated per-run directory under `data/research/<run_id>/`.
- Uses per-run SQLite DB (`DATABASE_URL=sqlite:///.../research.db`) to avoid touching production DB.
- Supports pause/resume/stop/promote via state file control.
- Optional Telegram control plane is available, but using a dedicated Telegram bot/chat is strongly recommended to avoid update polling contention with live bot process.
- Nightly scheduling uses `scripts/research_nightly.sh` with lockfile (`data/research/nightly.lock`) to prevent overlapping runs.
- Robust scoring uses split windows (train + holdout) across offsets (default `0,30,60` days) to reduce short-window overfit.
- Nightly and scripted runs can auto-run replay gate and auto-queue promotion only on replay pass.

### Standard workflow

```bash
# 1) Start (safe default: mock + paused start)
make research-start RESEARCH_MODE=mock RESEARCH_ITER=500 RESEARCH_PAUSED_START=1 RESEARCH_TELEGRAM=1

# 2) Inspect state
make research-status

# 3) Resume run
make research-resume

# 4) Monitor
make research-logs FOLLOW=1

# 5) Queue a candidate for promotion review
## If auto queue is disabled, manually queue after replay pass:
make research-promote CID=c042

# 6) Pause or stop
make research-pause
make research-stop

# 7) Cleanup temp artifacts when done
make research-cleanup

# 8) Install nightly schedule (default: 02:15 UTC daily)
make research-schedule-install
make research-schedule-status
```

### Direct script usage

```bash
./scripts/research_control.sh start --mode mock --iterations 500 --telegram --paused-start --replay-seeds 42,43
./scripts/research_control.sh status
./scripts/research_control.sh resume
./scripts/research_control.sh logs --follow
./scripts/research_control.sh promote --candidate c042
./scripts/research_control.sh stop
./scripts/research_control.sh cleanup
./scripts/research_control.sh install-schedule --cron "15 2 * * *"
./scripts/research_control.sh schedule-status
./scripts/research_control.sh remove-schedule
```

### Nightly tuning knobs (server `.env`)

`scripts/research_nightly.sh` reads optional env vars:

- `RESEARCH_NIGHTLY_MODE` (`backtest` or `mock`, default `backtest`)
- `RESEARCH_NIGHTLY_ITER` (default `30`)
- `RESEARCH_NIGHTLY_DAYS` (default `90`)
- `RESEARCH_NIGHTLY_SYMBOLS` (default `BTC/USD,ETH/USD,SOL/USD,XRP/USD,ADA/USD,LINK/USD`)
- `RESEARCH_NIGHTLY_WINDOW_OFFSETS` (default `0,30,60`)
- `RESEARCH_NIGHTLY_HOLDOUT_RATIO` (default `0.30`)
- `RESEARCH_NIGHTLY_TELEGRAM` (`0`/`1`, default `0`)
- `RESEARCH_NIGHTLY_DIGEST_EVERY` (default `10`)
- `RESEARCH_NIGHTLY_AUTO_REPLAY_GATE` (`0`/`1`, default `1`)
- `RESEARCH_NIGHTLY_REPLAY_SEEDS` (default `42,43`)
- `RESEARCH_NIGHTLY_REPLAY_DATA_DIR` (default `data/replay`)
- `RESEARCH_NIGHTLY_REPLAY_TIMEOUT_SECONDS` (default `1200`)
- `RESEARCH_NIGHTLY_AUTO_QUEUE_PROMOTION` (`0`/`1`, default `1`)

## REPLAY_* Toggle Reference

Environment variables used to control strategy behavior during replay/backtest runs.
These are **not** set in production — they only apply inside the replay harness and research automation.

### Override flags (parameterize thresholds)

Set by `research_continuous.sh`, `research_autolearn.py`, and replay validation scripts.

| Flag | Location | Purpose |
|------|----------|---------|
| `REPLAY_OVERRIDE_CONVICTION_MIN_FOR_ENTRY` | `src/strategy/smc_engine.py` | Minimum conviction score for entry |
| `REPLAY_OVERRIDE_SCORE_GATE_THRESHOLD` | `src/strategy/signal_scorer.py` | Minimum signal score to pass gate |
| `REPLAY_OVERRIDE_ADX_THRESHOLD` | `src/strategy/smc_engine.py` | ADX threshold for trend detection |
| `REPLAY_OVERRIDE_FIB_PROXIMITY_BPS` | `src/strategy/smc_engine.py` | Fibonacci proximity tolerance (bps) |
| `REPLAY_OVERRIDE_STRUCTURE_DEDUPE_MINUTES` | `src/strategy/smc_engine.py` | Structure signal deduplication window |
| `REPLAY_OVERRIDE_TIGHT_SMC_MIN_RR` | `src/risk/risk_manager.py` | Minimum R:R for tight SMC setups |
| `REPLAY_OVERRIDE_THESIS_REENTRY_BLOCK_THRESHOLD` | `src/memory/institutional_memory.py` | Thesis re-entry block threshold |

### Ablation flags (disable features for isolation testing)

Set to `"1"` to disable specific features. Used by `research_continuous.sh` for automated ablation.

| Flag | Location |
|------|----------|
| `REPLAY_ABLATE_DISABLE_WEEKLY_ZONE` | `src/strategy/smc_engine.py` |
| `REPLAY_ABLATE_DISABLE_DECISION_STRUCTURE` | `src/strategy/smc_engine.py` |
| `REPLAY_ABLATE_DISABLE_MS_CONFIRMATION` | `src/strategy/smc_engine.py` |
| `REPLAY_ABLATE_DISABLE_WAIT_STRUCTURE_BREAK` | `src/strategy/smc_engine.py` |
| `REPLAY_ABLATE_DISABLE_RECONFIRMATION` | `src/strategy/smc_engine.py` |

### Diagnostic flags

| Flag | Location | Purpose |
|------|----------|---------|
| `REPLAY_GATE_DIAGNOSTICS` | `src/strategy/smc_engine.py` | Log gate rejection reasons per tick |

### Infrastructure flags (replay harness internals)

Set automatically by `src/backtest/replay_harness/runner.py`. Do not set manually unless debugging the harness itself.

| Flag | Location | Default | Purpose |
|------|----------|---------|---------|
| `REPLAY_FORCE_MARKET_ENTRY` | `src/execution/execution_gateway.py` | `0` | Force market orders (no limit) |
| `REPLAY_RELAX_ORDER_RATE_LIMITER` | `src/execution/execution_gateway.py` | `0` | Bypass order rate limits |
| `REPLAY_SKIP_STALE_PENDING_CLOSE` | `src/execution/position_state_machine.py` | `0` | Skip stale pending close checks |
| `REPLAY_RESEARCH_MINIMAL_LOGS` | `src/runtime/cycle_guard.py` | `0` | Reduce log noise in research |
| `REPLAY_FORCE_FLAT_AT_END` | `src/backtest/replay_harness/runner.py` | `1` | Close all positions at replay end |
| `REPLAY_DISABLE_DB_MOCK` | `src/backtest/replay_harness/runner.py` | `0` | Use real DB instead of mock |
| `REPLAY_RELAX_MIN_SCORES` | `src/backtest/replay_harness/runner.py` | `1` | Relax minimum score requirements |

## Log Patterns

| Pattern | Meaning |
|---------|---------|
| `CYCLE_SUMMARY` | Per-tick metrics (duration, positions, state) |
| `DECISION_TRACE` | Per-coin signal analysis result |
| `KILL_SWITCH_ACTIVATED` | Emergency halt triggered |
| `INVARIANT_VIOLATION` | Safety limit breached |
| `ORDER_REJECTED_BY_VENUE` | Exchange rejected order |
| `API circuit breaker OPENED` | Circuit breaker tripped (API outage) |
| `THRESHOLD_MISMATCH` | Config/safety limit inconsistency |

## Common Troubleshooting

### System halted with margin_critical
- Check if positions are over-leveraged
- Review `auction_max_margin_util` vs `max_margin_utilization_pct`
- System auto-recovers once margin usage drops

### Circuit breaker open
- API outage detected; system waits 60s then probes
- If persistent: check Kraken status page
- Force close: restart the service

### Position not tracked
- Run `python -m src.tools.sync_positions` to reconcile
- Check `position_registry.db` for stale entries

### Missing stop-loss orders
- Run `python -m src.tools.recover_sl_order_ids --execute`
- Verify with `python -m src.tools.check_tp_coverage`
