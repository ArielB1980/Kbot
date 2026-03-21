# Watchdog and Process Management

This document describes how to ensure the live trading system runs continuously and restarts automatically if it crashes.

## Options

### Option 1: Bash Watchdog Script (Simple, Recommended)

The bash watchdog script monitors the system and restarts it if it crashes.

**Start the watchdog:**
```bash
./scripts/start_watchdog.sh
```

**Stop the watchdog:**
```bash
./scripts/stop_watchdog.sh
```

**Features:**
- Monitors system every 30 seconds
- Automatically restarts on crash
- Rate limiting (max 10 restarts/hour to prevent loops)
- Logs all activity to `logs/watchdog_live_trading.log`

**Note:** The watchdog itself must be kept running. If you want the watchdog to survive system reboots, use Option 2 (launchd) for the watchdog itself.

### Option 2: macOS launchd Service (Native, Recommended for Production)

macOS launchd is the native process manager that can automatically restart services on crash and on system boot.

**Install the service:**
```bash
# Copy the plist file to LaunchAgents directory
cp scripts/com.tradingsystem.live.plist ~/Library/LaunchAgents/

# Load the service
launchctl load ~/Library/LaunchAgents/com.tradingsystem.live.plist

# Start the service
launchctl start com.tradingsystem.live
```

**Uninstall the service:**
```bash
launchctl unload ~/Library/LaunchAgents/com.tradingsystem.live.plist
rm ~/Library/LaunchAgents/com.tradingsystem.live.plist
```

**Check status:**
```bash
launchctl list | grep com.tradingsystem.live
```

**View logs:**
```bash
tail -f logs/live_trading_stdout.log
tail -f logs/live_trading_stderr.log
```

**Features:**
- Automatically restarts on crash
- Starts on system boot
- Native macOS integration
- Better resource management

**Note:** Before using launchd, update the paths in `scripts/com.tradingsystem.live.plist` to match your system.

## Recommendation

For development/testing: Use the bash watchdog script (Option 1)
For production: Use macOS launchd (Option 2)

## Current Status

Check if the system is running:
```bash
./scripts/status_live_trading.sh
```

## Troubleshooting

If the system keeps crashing and restarting:
1. Check error logs: `tail -f logs/live_trading_stderr.log`
2. Check watchdog logs: `tail -f logs/watchdog_live_trading.log`
3. The watchdog will stop after 10 restarts/hour to prevent loops
4. Fix the underlying issue before restarting the watchdog
