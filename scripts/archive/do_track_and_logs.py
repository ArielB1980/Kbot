#!/usr/bin/env python3
"""
Connect to DigitalOcean via API: find app 'tradingbot', track deployment success, fetch live logs.

Usage:
  export DO_API_TOKEN=your_token   # or DIGITALOCEAN_API_TOKEN (do not commit)
  python scripts/do_track_and_logs.py --track              # poll until ACTIVE/FAILED
  python scripts/do_track_and_logs.py --logs               # fetch recent RUN logs
  python scripts/do_track_and_logs.py --logs --follow      # stream live RUN logs (WebSocket)
  python scripts/do_track_and_logs.py --track --logs       # deployment + logs
  python scripts/do_track_and_logs.py --logs --component worker --tail 200
  python scripts/do_track_and_logs.py --check-health   # GET /api, /api/health, /dashboard

App is on App Platform (not a droplet). Default app name: tradingbot.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import ssl
import sys
import time
from typing import Any

import requests

BASE_URL = "https://api.digitalocean.com/v2"
DEFAULT_APP_NAME = "tradingbot"


def get_token(args: argparse.Namespace) -> str:
    token = args.token or os.environ.get("DO_API_TOKEN") or os.environ.get("DIGITALOCEAN_API_TOKEN")
    if not token:
        print("Set DO_API_TOKEN or DIGITALOCEAN_API_TOKEN, or pass --token")
        sys.exit(1)
    return token


def get_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def list_apps(headers: dict[str, str]) -> list[dict[str, Any]]:
    r = requests.get(f"{BASE_URL}/apps", headers=headers, timeout=30)
    r.raise_for_status()
    return r.json().get("apps", [])


def find_app(apps: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    name_lower = name.strip().lower()
    for a in apps:
        spec_name = (a.get("spec") or {}).get("name") or ""
        if spec_name.strip().lower() == name_lower:
            return a
    return None


def get_deployments(app_id: str, headers: dict[str, str]) -> list[dict[str, Any]]:
    r = requests.get(f"{BASE_URL}/apps/{app_id}/deployments", headers=headers, timeout=30)
    r.raise_for_status()
    return r.json().get("deployments", [])


def get_app(app_id: str, headers: dict[str, str]) -> dict[str, Any]:
    r = requests.get(f"{BASE_URL}/apps/{app_id}", headers=headers, timeout=30)
    r.raise_for_status()
    return r.json().get("app", {})


def component_names(app: dict[str, Any]) -> list[str]:
    """Collect service, worker, and job component names from app spec."""
    spec = app.get("spec") or {}
    names: list[str] = []
    for kind in ("services", "workers", "jobs"):
        for c in spec.get(kind) or []:
            n = c.get("name")
            if n:
                names.append(n)
    return names


def check_health(app_id: str, headers: dict[str, str]) -> bool:
    """GET worker / and /health (prod-live entrypoint with WITH_HEALTH=1); optional /api, /api/health, /dashboard.
    Returns True if worker health (/ and /health) ok."""
    full = get_app(app_id, headers)
    base = full.get("default_ingress") or full.get("live_url") or full.get("live_domain")
    if not base:
        print("No default_ingress/live_url/live_domain on app; cannot check health.")
        return False
    base = base.rstrip("/")
    if not base.startswith("http"):
        base = "https://" + base
    print(f"  Base: {base}")
    ok = True
    # Worker health (prod-live entrypoint with WITH_HEALTH=1): GET / and /health
    for path in ("/", "/health"):
        url = base + path
        try:
            r = requests.get(url, timeout=15)
            status = "ok" if 200 <= r.status_code < 300 else "fail"
            if r.status_code >= 400:
                ok = False
            print(f"  {path or '/'}: {r.status_code} ({status})")
            if path == "/health" and r.status_code == 200:
                try:
                    j = r.json()
                    for k in ("uptime_seconds", "environment", "service"):
                        if k in j:
                            print(f"    {k}: {j[k]}")
                except Exception:
                    pass
        except Exception as e:
            print(f"  {path or '/'}: error – {e}")
            ok = False
    # Optional: web /api, /api/health, /dashboard (404 if app is worker-only)
    for path in ("/api", "/api/health", "/dashboard"):
        url = base + path
        try:
            r = requests.get(url, timeout=10)
            status = "ok" if 200 <= r.status_code < 300 else "skip" if r.status_code == 404 else "fail"
            print(f"  {path}: {r.status_code} ({status})")
            if r.status_code >= 500:
                ok = False
        except Exception as e:
            print(f"  {path}: error – {e}")
    return ok


def track_deployment(app_id: str, headers: dict[str, str], poll_interval: int = 10) -> bool:
    """Poll deployments until ACTIVE, FAILED, or CANCELED. Returns True if ACTIVE."""
    print("Tracking deployment (poll until ACTIVE / FAILED / CANCELED)...")
    while True:
        deployments = get_deployments(app_id, headers)
        if not deployments:
            print("No deployments found.")
            return False
        latest = deployments[0]
        phase = latest.get("phase", "UNKNOWN")
        created = latest.get("created_at", "")
        cause = latest.get("cause", "")
        print(f"  Deployment {latest.get('id', '')[:8]}... | phase={phase} | cause={cause} | created={created}")

        if phase == "ACTIVE":
            print("Deployment SUCCESS – ACTIVE.")
            return True
        if phase in ("FAILED", "CANCELED", "ERROR"):
            print(f"Deployment FAILED – phase={phase}.")
            return False
        print(f"  Waiting {poll_interval}s...")
        time.sleep(poll_interval)


def fetch_logs(
    app_id: str,
    deployment_id: str,
    component: str,
    headers: dict[str, str],
    log_type: str = "RUN",
    tail_lines: int = 500,
    follow: bool = False,
) -> dict[str, Any]:
    url = f"{BASE_URL}/apps/{app_id}/deployments/{deployment_id}/components/{component}/logs"
    params: dict[str, str | int] = {
        "type": log_type,
        "follow": "true" if follow else "false",
        "tail_lines": tail_lines,
    }
    r = requests.get(url, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def stream_live_logs(
    app_id: str,
    deployment_id: str,
    component: str,
    headers: dict[str, str],
    tail_lines: int = 200,
    save_path: str | None = None,
) -> None:
    """Stream RUN logs via live_url WebSocket."""
    data = fetch_logs(app_id, deployment_id, component, headers, log_type="RUN", tail_lines=tail_lines, follow=True)
    live_url = data.get("live_url")
    if not live_url:
        print("No live_url in response; cannot stream. Fetching historic only.")
        if "historic_urls" in data:
            for u in data["historic_urls"]:
                resp = requests.get(u, timeout=30)
                resp.raise_for_status()
                text = resp.text
                print(text)
                if save_path:
                    with open(save_path, "a", encoding="utf-8") as f:
                        f.write(text)
        return

    ws_url = live_url.replace("https://", "wss://")
    try:
        import websockets
    except ImportError:
        print("Install websockets: pip install websockets")
        sys.exit(1)

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    async def _run() -> None:
        fh = open(save_path, "a", encoding="utf-8") if save_path else None
        try:
            async with websockets.connect(ws_url, ssl=ssl_ctx) as ws:
                print(f"Connected to live logs for {component}. Streaming (Ctrl+C to stop)...")
                while True:
                    msg = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    print(msg, end="" if msg.endswith("\n") else "\n")
                    if fh:
                        fh.write(msg if msg.endswith("\n") else msg + "\n")
                        fh.flush()
        except asyncio.TimeoutError:
            print("Stream idle timeout.")
        except KeyboardInterrupt:
            print("\nStopped.")
        finally:
            if fh:
                fh.close()

    asyncio.run(_run())


def main() -> None:
    ap = argparse.ArgumentParser(
        description="DO App Platform: track deployment & fetch live logs for tradingbot",
    )
    ap.add_argument("--app-name", default=DEFAULT_APP_NAME, help=f"App name (default: {DEFAULT_APP_NAME})")
    ap.add_argument("--token", help="DO API token (default: DO_API_TOKEN / DIGITALOCEAN_API_TOKEN)")
    ap.add_argument("--track", action="store_true", help="Poll deployment until ACTIVE/FAILED")
    ap.add_argument("--logs", action="store_true", help="Fetch recent RUN logs")
    ap.add_argument("--deploy-logs", action="store_true", help="Fetch DEPLOY logs (build/deploy)")
    ap.add_argument("--follow", action="store_true", help="Stream live RUN logs (WebSocket)")
    ap.add_argument("--component", help="Component name (default: all services/workers)")
    ap.add_argument("--tail", type=int, default=500, help="Tail lines (default: 500)")
    ap.add_argument("--save", help="Append logs to this file (with --follow)")
    ap.add_argument("--poll-interval", type=int, default=10, help="Seconds between deployment polls (default: 10)")
    ap.add_argument("--check-health", action="store_true", help="GET /api, /api/health, /dashboard and report status")
    args = ap.parse_args()

    token = get_token(args)
    headers = get_headers(token)

    apps = list_apps(headers)
    app = find_app(apps, args.app_name)
    if not app:
        names = [ (a.get("spec") or {}).get("name") for a in apps ]
        print(f"App '{args.app_name}' not found. Apps: {names}")
        sys.exit(1)

    app_id = app["id"]
    spec_name = (app.get("spec") or {}).get("name", "")
    print(f"App: {spec_name} (id: {app_id})")

    if args.check_health:
        print("Health check (web / dashboard):")
        h_ok = check_health(app_id, headers)
        if not (args.track or args.logs or args.deploy_logs):
            sys.exit(0 if h_ok else 1)

    if args.track:
        ok = track_deployment(app_id, headers, poll_interval=args.poll_interval)
        if not ok and not (args.logs or args.deploy_logs):
            sys.exit(1)

    if not (args.logs or args.deploy_logs):
        return

    deployments = get_deployments(app_id, headers)
    if not deployments:
        print("No deployments; cannot fetch logs.")
        sys.exit(1)
    deployment_id = deployments[0]["id"]
    full_app = get_app(app_id, headers)
    components = [args.component] if args.component else component_names(full_app)
    if not components:
        print("No components found.")
        sys.exit(1)

    for comp in components:
        print(f"\n--- Logs: {comp} ---")
        try:
            if args.follow and args.logs:
                stream_live_logs(
                    app_id, deployment_id, comp, headers,
                    tail_lines=args.tail, save_path=args.save,
                )
                continue
            for log_type in (["DEPLOY"] if args.deploy_logs else []) + (["RUN"] if args.logs else []):
                data = fetch_logs(
                    app_id, deployment_id, comp, headers,
                    log_type=log_type, tail_lines=args.tail, follow=False,
                )
                if "historic_urls" in data and data["historic_urls"]:
                    for u in data["historic_urls"]:
                        resp = requests.get(u, timeout=30)
                        resp.raise_for_status()
                        print(resp.text)
                elif "live_url" in data and not args.follow:
                    print("(Logs available via live_url only; use --follow to stream)")
        except requests.HTTPError as e:
            print(f"HTTP error for {comp}: {e}")
        except Exception as e:
            print(f"Error for {comp}: {e}")


if __name__ == "__main__":
    main()
