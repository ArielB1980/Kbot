#!/usr/bin/env python3
"""
Server-side status checks. Run inside the worker container (e.g. DigitalOcean Console).

- No DO API token, .env.local, or .venv required.
- Uses only stdlib (urllib). Curls the worker health server (prod-live entrypoint with WITH_HEALTH=1) on
  PORT (default 8080): GET / and GET /health.

Usage (in worker container):
  python scripts/run_server_checks.py
  python3 scripts/run_server_checks.py

Or with curl, if available:
  curl -s http://127.0.0.1:8080/ && curl -s http://127.0.0.1:8080/health
"""
from __future__ import annotations

import os
import sys
import urllib.error
import urllib.request


def main() -> int:
    port = int(os.environ.get("PORT", "8080"))
    base = f"http://127.0.0.1:{port}"
    ok = 0
    fail = 0

    print("Server-side checks (worker health)")
    print("==================================")
    print(f"Base URL: {base} (PORT={port})")
    print()

    for path, label in [("/", "GET /"), ("/health", "GET /health")]:
        url = base + path
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=5) as r:
                code = r.getcode()
                body = r.read().decode("utf-8", errors="replace")[:200]
        except urllib.error.URLError as e:
            print(f"  {label}: FAIL – {e}")
            fail += 1
            continue
        except OSError as e:
            print(f"  {label}: FAIL – {e}")
            fail += 1
            continue

        if 200 <= code < 300:
            print(f"  {label}: OK (HTTP {code})")
            ok += 1
        else:
            print(f"  {label}: FAIL (HTTP {code}) {body[:80]!r}")
            fail += 1

    print()
    if fail == 0:
        print("All checks passed.")
        return 0
    print(f"Failed: {fail}, passed: {ok}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
