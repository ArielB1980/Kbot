#!/usr/bin/env python3
"""
Trading System Watchdog

Monitors the LiveTrading process and auto-restarts if it crashes.
Runs continuously in the background and logs all activity.
"""
import subprocess
import time
import sys
from datetime import datetime
from pathlib import Path

# Configuration
CHECK_INTERVAL = 300  # 5 minutes
LOG_FILE = "watchdog.log"
LIVE_TRADING_CMD = ["env", "PYTHONPATH=.", "python3", "src/cli.py", "live", "--force"]
WORKING_DIR = Path(__file__).parent


def log(message: str):
    """Write timestamped message to log file and stdout."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"[{timestamp}] {message}"
    print(log_message)
    with open(WORKING_DIR / LOG_FILE, "a") as f:
        f.write(log_message + "\n")


def is_process_running() -> bool:
    """Check if LiveTrading process is running."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "src/cli.py live"],
            capture_output=True,
            text=True
        )
        return result.returncode == 0 and result.stdout.strip() != ""
    except Exception as e:
        log(f"Error checking process: {e}")
        return False


def get_latest_trace_age() -> int:
    """Get age of latest DECISION_TRACE in seconds."""
    try:
        result = subprocess.run(
            [
                "env", "PYTHONPATH=.", "python3", "-c",
                "from src.storage.repository import get_latest_traces; "
                "from datetime import datetime, timezone; "
                "traces = get_latest_traces(limit=1); "
                "if traces: print((datetime.now(timezone.utc) - traces[0]['timestamp']).total_seconds())"
            ],
            capture_output=True,
            text=True,
            cwd=WORKING_DIR
        )
        if result.stdout.strip():
            return int(float(result.stdout.strip()))
        return 999999  # No traces found
    except Exception as e:
        log(f"Error checking trace age: {e}")
        return 999999


def start_live_trading():
    """Start the LiveTrading process."""
    try:
        log("Starting LiveTrading process...")
        subprocess.Popen(
            LIVE_TRADING_CMD,
            stdout=open(WORKING_DIR / "live.log", "w"),
            stderr=subprocess.STDOUT,
            cwd=WORKING_DIR
        )
        time.sleep(10)  # Give it time to start
        if is_process_running():
            log("✅ LiveTrading started successfully")
            return True
        else:
            log("❌ LiveTrading failed to start")
            return False
    except Exception as e:
        log(f"❌ Error starting LiveTrading: {e}")
        return False


def main():
    """Main watchdog loop."""
    log("=" * 60)
    log("🐕 Trading System Watchdog Started")
    log(f"Check interval: {CHECK_INTERVAL}s ({CHECK_INTERVAL/60:.0f} minutes)")
    log("=" * 60)
    
    consecutive_failures = 0
    max_failures = 3
    
    while True:
        try:
            # Check 1: Is process running?
            process_alive = is_process_running()
            
            # Check 2: Is data fresh?
            trace_age = get_latest_trace_age()
            data_fresh = trace_age < 600  # < 10 minutes
            
            # Status
            status = "🟢 HEALTHY" if (process_alive and data_fresh) else "🔴 UNHEALTHY"
            log(f"{status} | Process: {'UP' if process_alive else 'DOWN'} | "
                f"Latest data: {trace_age:.0f}s ago")
            
            # Action required?
            if not process_alive:
                log("⚠️  Process not running - attempting restart...")
                if start_live_trading():
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    
            elif not data_fresh and trace_age > 1800:  # > 30 minutes
                log("⚠️  Data is stale (>30m) - restarting process...")
                subprocess.run(["pkill", "-f", "src/cli.py live"])
                time.sleep(5)
                if start_live_trading():
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
            else:
                consecutive_failures = 0
            
            # Safety: Stop if too many failures
            if consecutive_failures >= max_failures:
                log(f"❌ CRITICAL: {max_failures} consecutive restart failures")
                log("❌ Stopping watchdog - manual intervention required")
                sys.exit(1)
            
            # Wait for next check
            time.sleep(CHECK_INTERVAL)
            
        except KeyboardInterrupt:
            log("🛑 Watchdog stopped by user")
            break
        except Exception as e:
            log(f"❌ Unexpected error in watchdog loop: {e}")
            time.sleep(60)  # Wait a bit before retrying


if __name__ == "__main__":
    main()
