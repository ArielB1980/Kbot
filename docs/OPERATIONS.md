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
- Promotion is replay-gated: a candidate must be marked replay-pass before `/research_promote` is allowed.

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
## After replay gate passes, mark candidate:
/research_mark_replay_pass c042
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
./scripts/research_control.sh start --mode mock --iterations 500 --telegram --paused-start
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
- `RESEARCH_NIGHTLY_DAYS` (default `30`)
- `RESEARCH_NIGHTLY_SYMBOLS` (default `BTC/USD,ETH/USD,SOL/USD`)
- `RESEARCH_NIGHTLY_WINDOW_OFFSETS` (default `0,30,60`)
- `RESEARCH_NIGHTLY_HOLDOUT_RATIO` (default `0.30`)
- `RESEARCH_NIGHTLY_TELEGRAM` (`0`/`1`, default `0`)
- `RESEARCH_NIGHTLY_DIGEST_EVERY` (default `10`)

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
