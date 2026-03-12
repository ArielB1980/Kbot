# Makefile for Local Development

SHELL := /bin/bash
PYTHON := .venv/bin/python
PIP := .venv/bin/pip
DEPLOY_SERVER ?= root@207.154.193.121
DEPLOY_SSH_KEY ?= $(HOME)/.ssh/trading_droplet
DEPLOY_TRADING_USER ?= trading
DEPLOY_TRADING_DIR ?= /home/trading/TradingSystem

.PHONY: help venv install run smoke logs smoke-logs test test-server lint format integration pre-deploy deploy deploy-quick deploy-live backfill backtest-quick backtest-full replay replay-episode replay-sweep research-start research-status research-pause research-resume research-stop research-promote research-logs research-cleanup research-schedule-install research-schedule-status research-schedule-remove audit audit-cancel audit-orphaned place-missing-stops place-missing-stops-live cancel-all-place-stops cancel-all-place-stops-live list-needing-protection check-signals safety-reset safety-reset-soft safety-reset-hard clean clean-logs status validate

help:
	@echo "Available commands:"
	@echo "  make venv          Create virtual environment"
	@echo "  make install       Install dependencies"
	@echo "  make validate      Validate environment configuration"
	@echo "  make backfill      Download 250 days of historical data for all coins"
	@echo "  make backtest-quick  Quick backtest (scripts/backtest/run_quick_backtest.py)"
	@echo "  make backtest-full   Full backtest (scripts/backtest/run_full_backtest.py)"
	@echo "  make replay          Run all 6 replay episodes (SEED=42 default)"
	@echo "  make replay-episode  Run a single episode: make replay-episode EP=1_normal SEED=42"
	@echo "  make replay-sweep    Run all episodes across seeds 1-5 (robustness)"
	@echo "  make research-start   Start isolated sandbox autoresearch session on server"
	@echo "  make research-status  Show current sandbox autoresearch state"
	@echo "  make research-pause   Pause current sandbox autoresearch run"
	@echo "  make research-resume  Resume current sandbox autoresearch run"
	@echo "  make research-stop    Request graceful stop for current research run"
	@echo "  make research-promote Queue candidate for review (CID=<candidate_id>)"
	@echo "  make research-logs    Show/tail current research run logs (FOLLOW=1 optional)"
	@echo "  make research-cleanup Stop and remove current research run artifacts"
	@echo "  make research-schedule-install Install nightly research cron on server"
	@echo "  make research-schedule-status  Show nightly schedule on server"
	@echo "  make research-schedule-remove  Remove nightly schedule on server"
	@echo "  make run           Run bot in local mode (dry-run)"
	@echo "  make smoke         Run smoke test (30s)"
	@echo "  make integration   Run integration test (5 mins, tests all code paths)"
	@echo "  make pre-deploy    Run all pre-deployment tests (REQUIRED before push to main)"
	@echo "  make deploy        Full deployment: tests + commit + push + deploy to server"
	@echo "  make deploy-quick  Quick deployment: skip tests, commit + push + deploy"
	@echo "  make deploy-live   Enable live flags on DO tradingbot, track deploy, health-check (needs DO_API_TOKEN)"
	@echo "  make audit           Audit open futures orders (read-only)"
	@echo "  make audit-cancel    Audit + cancel redundant stop orders"
	@echo "  make audit-orphaned  Audit + cancel orphaned stops (when 0 positions)"
	@echo "  make place-missing-stops     List naked positions, dry-run place stops (STOP_PCT=2)"
	@echo "  make place-missing-stops-live Place missing stops for naked positions (STOP_PCT=2)"
	@echo "  make list-needing-protection List symbols from logs that need SL (TP backfill skipped)"
	@echo "  make cancel-all-place-stops       Cancel ALL orders, dry-run place SL per position"
	@echo "  make cancel-all-place-stops-live  Cancel ALL orders, then place SL per position (STOP_PCT=2)"
	@echo "  make check-signals  Fetch worker logs, verify system is scanning for signals (needs DO_API_TOKEN)"
	@echo "  make safety-reset  Show current safety state (dry-run)"
	@echo "  make safety-reset-soft  Clear halt + kill switch + peak (requires --i-understand)"
	@echo "  make test          Run unit tests"
	@echo "  make lint          Lint code with ruff"
	@echo "  make format        Format code with ruff"
	@echo "  make logs          Tail run logs"
	@echo "  make smoke-logs    Tail smoke logs"
	@echo "  make status        Check if bot is running"
	@echo "  make clean         Remove .venv and caches"
	@echo "  make clean-logs    Remove log files"

backfill:
	@echo "Backfilling historical candle data (250 days)..."
	@if [ -f .env.local ]; then \
		set -a; source .env.local; set +a; \
		ENV=local ENVIRONMENT=dev $(PYTHON) scripts/backfill_historical_data.py; \
	else \
		echo "❌ .env.local not found. Run 'make validate' first."; \
		exit 1; \
	fi

backtest-quick:
	@if [ -f .env.local ]; then \
		set -a; source .env.local; set +a; \
		$(PYTHON) scripts/backtest/run_quick_backtest.py; \
	else \
		echo "❌ .env.local not found. Run 'make validate' first."; exit 1; \
	fi

backtest-full:
	@if [ -f .env.local ]; then \
		set -a; source .env.local; set +a; \
		$(PYTHON) scripts/backtest/run_full_backtest.py; \
	else \
		echo "❌ .env.local not found. Run 'make validate' first."; exit 1; \
	fi

EP ?=
SEED ?= 42

replay:
	@mkdir -p results/replay
	@echo "Running all 6 replay backtest episodes (seed=$(SEED))..."
	@if [ -f .env.local ]; then set -a; source .env.local; set +a; fi; \
	ENV=local DRY_RUN=0 $(PYTHON) -m src.backtest.replay_harness.run_episodes \
		--seed $(SEED) --data-dir data/replay --output results/replay 2>&1 | tee results/replay/run.log

replay-episode:
	@if [ -z "$(EP)" ]; then \
		echo "Usage: make replay-episode EP=1_normal"; \
		echo "Available: 1_normal, 2_high_vol, 3_drought, 4_outage, 5_restart, 6_bug"; \
		echo "Optional: SEED=N (default 42)"; \
		exit 1; \
	fi
	@mkdir -p results/replay
	@echo "Running replay episode: $(EP) (seed=$(SEED))..."
	@if [ -f .env.local ]; then set -a; source .env.local; set +a; fi; \
	ENV=local DRY_RUN=0 $(PYTHON) -m src.backtest.replay_harness.run_episodes \
		--episode $(EP) --seed $(SEED) --data-dir data/replay --output results/replay 2>&1 | tee results/replay/$(EP).log

replay-sweep:
	@echo "Running replay across seeds 1-5 (robustness sweep)..."
	@for s in 1 2 3 4 5; do \
		echo ""; echo "=== SEED $$s ==="; \
		$(MAKE) replay SEED=$$s || exit 1; \
	done
	@echo ""; echo "All seeds passed."

RESEARCH_MODE ?= mock
RESEARCH_ITER ?= 500
RESEARCH_DAYS ?= 90
RESEARCH_SYMBOLS ?= BTC/USD,ETH/USD,SOL/USD,XRP/USD,ADA/USD,LINK/USD
RESEARCH_TELEGRAM ?= 0
RESEARCH_PAUSED_START ?= 1
CID ?=
FOLLOW ?= 0
RESEARCH_CRON ?= 15 2 * * *

research-start:
	@TELEGRAM_FLAG=""; \
	PAUSE_FLAG=""; \
	if [ "$(RESEARCH_TELEGRAM)" = "1" ]; then TELEGRAM_FLAG="--telegram"; fi; \
	if [ "$(RESEARCH_PAUSED_START)" = "1" ]; then PAUSE_FLAG="--paused-start"; fi; \
	./scripts/research_control.sh start \
		--mode "$(RESEARCH_MODE)" \
		--iterations "$(RESEARCH_ITER)" \
		--days "$(RESEARCH_DAYS)" \
		--symbols "$(RESEARCH_SYMBOLS)" \
		$$TELEGRAM_FLAG $$PAUSE_FLAG

research-status:
	./scripts/research_control.sh status

research-pause:
	./scripts/research_control.sh pause

research-resume:
	./scripts/research_control.sh resume

research-stop:
	./scripts/research_control.sh stop

research-promote:
	@if [ -z "$(CID)" ]; then \
		echo "Usage: make research-promote CID=c123"; \
		exit 1; \
	fi
	./scripts/research_control.sh promote --candidate "$(CID)"

research-logs:
	@if [ "$(FOLLOW)" = "1" ]; then \
		./scripts/research_control.sh logs --follow; \
	else \
		./scripts/research_control.sh logs; \
	fi

research-cleanup:
	./scripts/research_control.sh cleanup

research-schedule-install:
	./scripts/research_control.sh install-schedule --cron "$(RESEARCH_CRON)"

research-schedule-status:
	./scripts/research_control.sh schedule-status

research-schedule-remove:
	./scripts/research_control.sh remove-schedule

venv:
	python3 -m venv .venv
	@echo "✅ Virtual environment created in .venv"
	@echo "To activate: source .venv/bin/activate"

install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "✅ Dependencies installed"

validate:
	@echo "Validating environment configuration..."
	@if [ ! -f .env.local ]; then \
		echo "⚠️  .env.local not found. Creating from template..."; \
		cp .env.local.example .env.local; \
		echo "✅ Created .env.local - please review and update if needed"; \
	fi
	@echo "✅ Environment validation complete"

run:
	@mkdir -p logs
	@echo "Starting local run (Dry Run)..."
	@if [ -f .env.local ]; then \
		set -a; source .env.local; set +a; \
		ENV=local ENVIRONMENT=dev DRY_RUN=1 LOG_LEVEL=INFO $(PYTHON) run.py live --force 2>&1 | tee -a logs/run.log; \
	else \
		echo "❌ .env.local not found. Run 'make validate' first."; \
		exit 1; \
	fi

smoke:
	@mkdir -p logs
	@echo "Starting smoke test (30s)..."
	@if [ -f .env.local ]; then \
		set -a; source .env.local; set +a; \
		ENV=local ENVIRONMENT=dev DRY_RUN=1 RUN_SECONDS=30 LOG_LEVEL=INFO $(PYTHON) run.py live --force 2>&1 | tee logs/smoke.log; \
		EXIT_CODE=$$?; \
		if [ $$EXIT_CODE -eq 0 ]; then \
			echo ""; \
			echo "✅ SMOKE TEST PASSED"; \
			echo "Exit code: $$EXIT_CODE"; \
		else \
			echo ""; \
			echo "❌ SMOKE TEST FAILED"; \
			echo "Exit code: $$EXIT_CODE"; \
			echo "Check logs/smoke.log for details"; \
		fi; \
		exit $$EXIT_CODE; \
	else \
		echo "❌ .env.local not found. Run 'make validate' first."; \
		exit 1; \
	fi

integration:
	@mkdir -p logs
	@echo "Starting integration test (5 minutes, full live pipeline in dry-run)..."
	@if [ -f .env.local ]; then \
		set -a; source .env.local; set +a; \
		ENV=local ENVIRONMENT=dev DRY_RUN=1 RUN_SECONDS=300 LOG_LEVEL=INFO $(PYTHON) run.py live --force 2>&1 | tee logs/integration.log; \
		EXIT_CODE=$$?; \
		if [ $$EXIT_CODE -eq 0 ]; then \
			echo ""; \
			echo "✅ INTEGRATION TEST PASSED"; \
			echo "Exit code: $$EXIT_CODE"; \
		else \
			echo ""; \
			echo "❌ INTEGRATION TEST FAILED"; \
			echo "Exit code: $$EXIT_CODE"; \
			echo "Check logs/integration.log for details"; \
		fi; \
		exit $$EXIT_CODE; \
	else \
		echo "❌ .env.local not found. Run 'make validate' first."; \
		exit 1; \
	fi

deploy-live:
	@echo "Enable live trading on DO (tradingbot), track deployment, health-check..."
	@if [ -f .env.local ]; then set -a; source .env.local; set +a; fi; \
	if [ -z "$$DO_API_TOKEN" ] && [ -z "$$DIGITALOCEAN_API_TOKEN" ]; then \
		echo "Set DO_API_TOKEN or DIGITALOCEAN_API_TOKEN (e.g. in .env.local)"; exit 1; \
	fi; \
	$(PYTHON) scripts/enable_live_trading_do.py && \
	$(PYTHON) scripts/do_track_and_logs.py --track && \
	$(PYTHON) scripts/do_track_and_logs.py --check-health

deploy:
	@echo "=========================================="
	@echo "FULL DEPLOYMENT"
	@echo "=========================================="
	@echo ""
	@echo "This will:"
	@echo "  1. Run pre-deployment tests"
	@echo "  2. Run replay gate (seed=42)"
	@echo "  3. Commit and push to GitHub"
	@echo "  4. Deploy to production server"
	@echo "  (Set SKIP_REPLAY=1 to bypass replay gate)"
	@echo ""
	@if [ -f .env.local ]; then set -a; source .env.local; set +a; fi; \
	SKIP_REPLAY=$${SKIP_REPLAY:-0} ./scripts/deploy.sh

deploy-quick:
	@echo "=========================================="
	@echo "QUICK DEPLOYMENT (SKIPS TESTS)"
	@echo "=========================================="
	@echo ""
	@echo "This will:"
	@echo "  1. Commit and push to GitHub (skip tests)"
	@echo "  2. Deploy to production server"
	@echo ""
	@if [ -f .env.local ]; then set -a; source .env.local; set +a; fi; \
	./scripts/deploy.sh --skip-tests

pre-deploy:
	@echo "=========================================="
	@echo "PRE-DEPLOYMENT TEST SUITE"
	@echo "=========================================="
	@echo ""
	@echo "Step 1/2: Running smoke test (30s)..."
	@$(MAKE) smoke
	@echo ""
	@echo "Step 2/2: Running integration test (5 mins)..."
	@$(MAKE) integration
	@echo ""
	@echo "=========================================="
	@echo "✅ ALL PRE-DEPLOYMENT TESTS PASSED"
	@echo "=========================================="
	@echo ""
	@echo "Safe to push to main and deploy to production."

audit:
	@if [ -f .env.local ]; then \
		set -a; source .env.local; set +a; \
		$(PYTHON) -m src.tools.audit_open_orders; \
	else \
		echo "❌ .env.local not found. Run 'make validate' first."; \
		exit 1; \
	fi

audit-cancel:
	@if [ -f .env.local ]; then \
		set -a; source .env.local; set +a; \
		$(PYTHON) -m src.tools.audit_open_orders --cancel-redundant-stops; \
	else \
		echo "❌ .env.local not found. Run 'make validate' first."; \
		exit 1; \
	fi

audit-orphaned:
	@if [ -f .env.local ]; then \
		set -a; source .env.local; set +a; \
		$(PYTHON) -m src.tools.audit_open_orders --cancel-orphaned-stops; \
	else \
		echo "❌ .env.local not found. Run 'make validate' first."; \
		exit 1; \
	fi

STOP_PCT ?= 2.0

place-missing-stops:
	@if [ -f .env.local ]; then \
		set -a; source .env.local; set +a; \
		$(PYTHON) -m src.tools.place_missing_stops --dry-run --stop-pct $(STOP_PCT); \
	else \
		echo "❌ .env.local not found. Run 'make validate' first."; \
		exit 1; \
	fi

place-missing-stops-live:
	@if [ -f .env.local ]; then \
		set -a; source .env.local; set +a; \
		$(PYTHON) -m src.tools.place_missing_stops --stop-pct $(STOP_PCT); \
	else \
		echo "❌ .env.local not found. Run 'make validate' first."; \
		exit 1; \
	fi

list-needing-protection:
	$(PYTHON) scripts/list_positions_needing_protection.py

cancel-all-place-stops:
	@if [ -f .env.local ]; then \
		set -a; source .env.local; set +a; \
		$(PYTHON) -m src.tools.place_missing_stops --cancel-all-first --dry-run --stop-pct $(STOP_PCT); \
	else \
		echo "❌ .env.local not found. Run 'make validate' first."; \
		exit 1; \
	fi

cancel-all-place-stops-live:
	@if [ -f .env.local ]; then \
		set -a; source .env.local; set +a; \
		$(PYTHON) -m src.tools.place_missing_stops --cancel-all-first --stop-pct $(STOP_PCT); \
	else \
		echo "❌ .env.local not found. Run 'make validate' first."; \
		exit 1; \
	fi

check-signals:
	@if [ -f .env.local ]; then \
		set -a; source .env.local; set +a; \
		$(PYTHON) scripts/check_signal_scanning.py; \
	else \
		echo "❌ .env.local not found. Add DO_API_TOKEN for remote log fetch."; \
		exit 1; \
	fi

safety-reset:
	@if [ -f .env.local ]; then set -a; source .env.local; set +a; fi; \
	$(PYTHON) -m src.tools.safety_reset --dry-run

safety-reset-soft:
	@if [ -f .env.local ]; then set -a; source .env.local; set +a; fi; \
	$(PYTHON) -m src.tools.safety_reset --mode soft --reset-peak-to-current --i-understand

safety-reset-hard:
	@if [ -f .env.local ]; then set -a; source .env.local; set +a; fi; \
	$(PYTHON) -m src.tools.safety_reset --mode hard --reset-peak-to-current --i-understand

test:
	@echo "Running unit tests (server-dependent tests skipped)..."
	$(PYTHON) -m pytest tests/ -v --tb=short

test-server:
	@echo "Running server-only tests (DB + exchange API)..."
	ssh -i $(DEPLOY_SSH_KEY) $(DEPLOY_SERVER) "cd $(DEPLOY_TRADING_DIR) && \
		sudo -u $(DEPLOY_TRADING_USER) bash -c 'set -a; source .env; set +a; \
		venv/bin/python -m pytest tests/ -m server -v --tb=short -q' 2>&1"

lint:
	@echo "Running ruff linter..."
	$(PYTHON) -m ruff check src/ tests/ --fix

format:
	@echo "Formatting code with ruff..."
	$(PYTHON) -m ruff format src/ tests/

logs:
	@if [ -f logs/run.log ]; then \
		tail -n 200 -f logs/run.log; \
	else \
		echo "❌ logs/run.log not found. Run 'make run' first."; \
	fi

smoke-logs:
	@if [ -f logs/smoke.log ]; then \
		tail -n 200 logs/smoke.log; \
	else \
		echo "❌ logs/smoke.log not found. Run 'make smoke' first."; \
	fi

status:
	@echo "Checking bot status..."
	@ps aux | grep "[p]ython run.py" || echo "Bot is not running"

clean:
	rm -rf .venv
	rm -rf __pycache__
	rm -rf .local
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "✅ Cleaned up virtual environment and caches"

clean-logs:
	rm -rf logs/
	@echo "✅ Cleaned up log files"

