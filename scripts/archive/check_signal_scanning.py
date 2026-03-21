#!/usr/bin/env python3
"""
Fetch worker RUN logs from DigitalOcean, analyze for signal-scanning activity, and report.

Requires DO_API_TOKEN or DIGITALOCEAN_API_TOKEN (same as do_track_and_logs).

Usage:
  export DO_API_TOKEN=your_token
  python scripts/check_signal_scanning.py
  python scripts/check_signal_scanning.py --tail 500
  python scripts/check_signal_scanning.py --show-logs   # print raw log lines used for analysis
"""
from __future__ import annotations

import argparse
import asyncio
import os
import re
import ssl
import sys
import time
from typing import Any

import requests

BASE_URL = "https://api.digitalocean.com/v2"
DEFAULT_APP_NAME = "tradingbot"


def _token() -> str:
    t = os.environ.get("DO_API_TOKEN") or os.environ.get("DIGITALOCEAN_API_TOKEN")
    if not t:
        print("Set DO_API_TOKEN or DIGITALOCEAN_API_TOKEN")
        sys.exit(1)
    return t


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {_token()}", "Content-Type": "application/json"}


async def _collect_live_logs(ws_url: str, seconds: int) -> list[str]:
    try:
        import websockets
    except ImportError:
        print("Install websockets: pip install websockets")
        sys.exit(1)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    lines: list[str] = []
    try:
        async with websockets.connect(ws_url, ssl=ctx) as ws:
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                remaining = max(1.0, deadline - time.monotonic())
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=min(10.0, remaining))
                    for line in msg.splitlines():
                        line = line.strip()
                        if line:
                            lines.append(line)
                except asyncio.TimeoutError:
                    continue
    except Exception as e:
        print(f"WebSocket stream error: {e}", file=sys.stderr)
    return lines


def _fetch_worker_log_lines(tail: int, stream_seconds: int) -> list[str]:
    headers = _headers()
    r = requests.get(f"{BASE_URL}/apps", headers=headers, timeout=30)
    r.raise_for_status()
    apps = r.json().get("apps", [])
    app = None
    for a in apps:
        if (a.get("spec") or {}).get("name", "").strip().lower() == DEFAULT_APP_NAME.strip().lower():
            app = a
            break
    if not app:
        print(f"App '{DEFAULT_APP_NAME}' not found.")
        sys.exit(1)
    app_id = app["id"]
    r = requests.get(f"{BASE_URL}/apps/{app_id}/deployments", headers=headers, timeout=30)
    r.raise_for_status()
    deployments = r.json().get("deployments", [])
    if not deployments:
        print("No deployments; cannot fetch logs.")
        sys.exit(1)
    deployment_id = deployments[0]["id"]
    r = requests.get(f"{BASE_URL}/apps/{app_id}", headers=headers, timeout=30)
    r.raise_for_status()
    full = r.json().get("app", {})
    comps: list[str] = []
    for kind in ("services", "workers", "jobs"):
        for c in (full.get("spec") or {}).get(kind) or []:
            n = c.get("name")
            if n:
                comps.append(n)
    worker = "worker" if any(c.lower() == "worker" for c in comps) else (comps[0] if comps else None)
    if not worker:
        print("No worker component found.")
        sys.exit(1)
    url = f"{BASE_URL}/apps/{app_id}/deployments/{deployment_id}/components/{worker}/logs"
    params: dict[str, Any] = {"type": "RUN", "follow": "false", "tail_lines": min(tail, 500)}
    r = requests.get(url, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    data: dict[str, Any] = r.json()
    lines: list[str] = []
    for u in data.get("historic_urls") or []:
        r2 = requests.get(u, timeout=30)
        r2.raise_for_status()
        lines.extend(r2.text.splitlines())
    if lines:
        return lines
    live_url = data.get("live_url")
    if live_url and stream_seconds > 0:
        ws_url = live_url.replace("https://", "wss://")
        print(f"Historic logs unavailable. Streaming live for {stream_seconds}s...", file=sys.stderr)
        lines = asyncio.run(_collect_live_logs(ws_url, stream_seconds))
    return lines


# Patterns that indicate the system is scanning for signals (live_trading._tick → SMC)
SCAN_OK = [
    (r"Coin processing status summary", "periodic status (every 5 min)"),
    (r"SMC Analysis \S+: NO_SIGNAL", "SMC scanning symbols (throttled log)"),
    (r"New signal detected", "signal found"),
    (r"Processing signal via State Machine V2", "signal found (V2)"),
    (r"Active Portfolio: \d+ positions", "position sync"),
    (r"Hydration complete", "hydration summary (DB candle load)"),
    (r"Ticker coverage:.*skipped", "ticker coverage (symbols with/without spot ticker)"),
    (r"coins_futures_fallback_used", "futures OHLCV fallback used for some symbols"),
]
# Patterns that indicate problems (tick blocked or unhealthy)
SCAN_BAD = [
    (r"Error in live trading tick", "tick loop error"),
    (r"Data acquisition unhealthy", "data unhealthy"),
    (r"Kill switch is active", "kill switch halting"),
    (r"Failed batch data fetch", "data fetch failed"),
    (r"Failed to sync positions", "position sync failed"),
]


def analyze(lines: list[str], show_logs: bool) -> None:
    if show_logs:
        print("--- Log lines (last 200) ---")
        for line in lines[-200:]:
            print(line)
        print("--- End logs ---\n")

    print("Signal-scanning analysis")
    print("=======================")
    print(f"Log lines analyzed: {len(lines)}\n")

    found_ok: list[tuple[str, str]] = []
    found_bad: list[tuple[str, str]] = []
    for line in lines:
        for pat, label in SCAN_OK:
            if re.search(pat, line):
                found_ok.append((label, line.strip()[:120]))
                break
        for pat, label in SCAN_BAD:
            if re.search(pat, line):
                found_bad.append((label, line.strip()[:120]))
                break

    print("Evidence of signal scanning (recent):")
    if not found_ok:
        print("  None found in tail of logs.")
    else:
        seen = set()
        for label, snippet in reversed(found_ok[-30:]):
            k = (label, snippet[:80])
            if k in seen:
                continue
            seen.add(k)
            print(f"  [{label}] {snippet}")

    print()
    print("Evidence of problems:")
    if not found_bad:
        print("  None found.")
    else:
        for label, snippet in found_bad[-15:]:
            print(f"  [! {label}] {snippet}")

    print()
    if found_ok and not found_bad:
        print("Conclusion: System appears to be scanning for signals (tick loop running, SMC analysis).")
    elif found_bad:
        print("Conclusion: Issues detected. Tick loop may be blocked or unhealthy.")
    else:
        print("Conclusion: No clear signal-scanning logs in tail. Worker may be idle, still warming up, or logs rotated.")


def main() -> int:
    ap = argparse.ArgumentParser(description="Check worker logs for signal-scanning activity.")
    ap.add_argument("--tail", type=int, default=400, help="Tail N log lines (default 400)")
    ap.add_argument("--stream-seconds", type=int, default=90, help="When using live stream, collect for N seconds (default 90)")
    ap.add_argument("--show-logs", action="store_true", help="Print raw log lines used for analysis")
    args = ap.parse_args()

    lines = _fetch_worker_log_lines(tail=args.tail, stream_seconds=args.stream_seconds)
    analyze(lines, show_logs=args.show_logs)
    return 0


if __name__ == "__main__":
    sys.exit(main())
