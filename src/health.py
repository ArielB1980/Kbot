"""
Health check endpoint for Droplet deployment.

Two FastAPI apps are provided:
1. `app` - Standalone health server (for `python -m src.health`)
   - Comprehensive health checks with kill switch and worker liveness
   
2. `worker_health_app` - Worker-integrated health server (for `python -m src.entrypoints.prod_live` with `WITH_HEALTH=1`)
   - Used by prod-live worker when `WITH_HEALTH=1`
   - Simpler health checks optimized for worker context

Production uses: worker_health_app (via `python -m src.entrypoints.prod_live` with `WITH_HEALTH=1`)
Standalone: app (via python -m src.health)
"""
import html as html_module
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

from src.exceptions import DataError, OperationalError
from src.health_checks import check_required_secrets_and_db, quick_connectivity_snapshot

# Global coordinator reference — set at runtime by live trading or entrypoints.
_coordinator: object | None = None


def set_coordinator(coordinator: object) -> None:
    """Register the ``HealthCheckCoordinator`` for HTTP exposure.

    Args:
        coordinator: A ``HealthCheckCoordinator`` instance.
    """
    global _coordinator
    _coordinator = coordinator


def get_coordinator() -> object | None:
    """Return the registered coordinator, or *None*."""
    return _coordinator

# Standalone health app (for python -m src.health - legacy, kept for backwards compatibility)
app = FastAPI(title="Trading System Health Check")

_worker_start = time.time()

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _metrics_json() -> Tuple[dict, int]:
    """Shared logic for /api/metrics. Returns (content, status_code)."""
    try:
        from src.storage.repository import get_latest_metrics_snapshot
        snap = get_latest_metrics_snapshot()
        out = snap if snap is not None else {}
        return ({"source": "worker_snapshot", "metrics": out}, 200)
    except (OperationalError, DataError, OSError) as e:
        return ({"source": "worker_snapshot", "metrics": {}, "error": str(e)[:100]}, 200)


def _metrics_prometheus() -> Tuple[str, int]:
    """Shared logic for /metrics (Prometheus-style). Returns (body, status_code)."""
    try:
        from src.storage.repository import get_latest_metrics_snapshot
        snap = get_latest_metrics_snapshot()
        if not snap:
            return ("", 200)
        lines = ["# Worker metrics (latest snapshot)"]
        for k, v in sorted(snap.items()):
            if v is None:
                continue
            if isinstance(v, (int, float)):
                lines.append(f"trading_{k} {v}")
            elif isinstance(v, str) and k == "last_tick_at":
                lines.append(f'trading_{k}_iso "{v}"')
            else:
                continue
        return ("\n".join(lines) + "\n", 200)
    except (OperationalError, DataError, OSError) as e:
        return (f"# error: {e}\n", 200)


def get_worker_health_app(enable_debug: bool = False) -> FastAPI:
    """
    Health app for worker (prod-live entrypoint with `WITH_HEALTH=1`).
    Serves /, /health. Also /api, /api/health, /api/debug/signals, /debug/signals
    so the default app URL (which routes to worker) can serve debug endpoints.
    """
    w = FastAPI(title="Worker Health")

    @w.get("/")
    async def root():
        return {"status": "ok", "service": "trading-worker"}

    @w.get("/health")
    async def health():
        return JSONResponse(
            content={
                "status": "healthy",
                "uptime_seconds": int(time.time() - _worker_start),
                "environment": os.getenv("ENVIRONMENT", "unknown"),
            },
            status_code=200,
        )

    @w.get("/api")
    async def api_root():
        return {"status": "ok", "service": "trading-worker"}

    @w.get("/api/health")
    async def api_health():
        """Quick health: DB ping, environment, secrets availability."""
        database, secrets, environment = check_required_secrets_and_db()
        out = {
            "status": "healthy",
            "database": database,
            "environment": environment,
            "secrets": secrets,
        }

        if database != "connected":
            out["status"] = "unhealthy"

        # Overall status: unhealthy if any required secret is missing
        required_secrets = ["DATABASE_URL", "KRAKEN_FUTURES_API_KEY", "KRAKEN_FUTURES_API_SECRET"]
        if not all(out["secrets"].get(s, {}).get("available") for s in required_secrets):
            out["status"] = "degraded"  # Degraded, not unhealthy, since secrets may be injected later
        
        status_code = 200 if out["status"] == "healthy" else (503 if out["status"] == "unhealthy" else 200)
        return JSONResponse(content=out, status_code=status_code)

    @w.get("/api/debug/signals")
    async def api_debug_signals(request: Request, symbol: Optional[str] = None, format: Optional[str] = None):
        if not enable_debug:
            return JSONResponse(content={"error": "Debug endpoints disabled"}, status_code=403)
        data = _debug_signals_impl(symbol_filter=symbol)
        return _debug_signals_respond(data, request, format_param=format)

    @w.get("/debug/signals")
    async def debug_signals_route(request: Request, symbol: Optional[str] = None, format: Optional[str] = None):
        if not enable_debug:
            return JSONResponse(content={"error": "Debug endpoints disabled"}, status_code=403)
        data = _debug_signals_impl(symbol_filter=symbol)
        return _debug_signals_respond(data, request, format_param=format)

    @w.get("/api/monitoring/status")
    async def api_monitoring_status():
        """Health monitoring pipeline status from HealthCheckCoordinator."""
        if _coordinator is None:
            return JSONResponse(
                content={"status": "unavailable", "reason": "no coordinator registered"},
                status_code=200,
            )
        try:
            snapshot = _coordinator.snapshot()  # type: ignore[union-attr]
            return JSONResponse(content=snapshot, status_code=200)
        except (OperationalError, DataError, OSError, RuntimeError) as e:
            return JSONResponse(
                content={"status": "error", "message": str(e)[:200]},
                status_code=500,
            )

    @w.get("/api/metrics")
    async def api_metrics():
        content, status = _metrics_json()
        return JSONResponse(content=content, status_code=status)

    @w.get("/metrics")
    async def metrics_prometheus():
        body, status = _metrics_prometheus()
        return PlainTextResponse(body, status_code=status)

    return w


# Secure defaults: disable debug unless explicitly enabled.
worker_health_app = get_worker_health_app(
    enable_debug=_env_bool("WORKER_HEALTH_ENABLE_DEBUG", default=False),
)


@app.get("/api")
async def root():
    """Root endpoint."""
    return {"status": "ok", "service": "trading-system"}


@app.get("/api/health")
async def health():
    """Health check. Pings DB; reports kill switch and worker liveness from metrics."""
    database, secrets, environment = check_required_secrets_and_db()
    checks = {
        "status": "healthy",
        "database": database,
        "environment": environment,
        "kill_switch_active": False,
        "worker_last_tick_at": None,
        "worker_stale": None,
        "secrets": secrets,
    }
    if database != "connected":
        checks["status"] = "unhealthy"

    try:
        from src.utils.kill_switch import read_kill_switch_state
        ks = read_kill_switch_state()
        checks["kill_switch_active"] = bool(ks.get("active"))
    except (OperationalError, OSError, ValueError, RuntimeError):
        pass

    try:
        from src.storage.repository import get_latest_metrics_snapshot
        snap = get_latest_metrics_snapshot()
        if snap and snap.get("last_tick_at"):
            checks["worker_last_tick_at"] = snap["last_tick_at"]
            try:
                ts = datetime.fromisoformat(snap["last_tick_at"].replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                age_sec = (datetime.now(timezone.utc) - ts).total_seconds()
                checks["last_tick_age_seconds"] = round(age_sec, 1)
                checks["worker_stale"] = age_sec > 300
            except (ValueError, TypeError, KeyError):
                checks["worker_stale"] = None
    except (OperationalError, DataError, OSError):
        pass

    status_code = 200 if checks["status"] == "healthy" else 503
    return JSONResponse(content=checks, status_code=status_code)


@app.get("/api/ready")
async def ready():
    """Readiness probe endpoint."""
    return {"status": "ready"}


@app.get("/api/metrics")
async def metrics():
    """Observability: latest metrics snapshot from worker (DB). Includes last_tick_at, signals_last_min, api_fetch_latency_ms, markets_count, coins_futures_fallback_used."""
    content, status = _metrics_json()
    return JSONResponse(content=content, status_code=status)


@app.get("/metrics")
async def metrics_prometheus():
    """Prometheus-style plain text metrics from latest worker snapshot."""
    body, status = _metrics_prometheus()
    return PlainTextResponse(body, status_code=status)



@app.get("/api/monitoring/status")
async def monitoring_status():
    """Health monitoring pipeline status from HealthCheckCoordinator.

    Returns the coordinator snapshot when available, or a minimal
    response when no coordinator has been registered.
    """
    if _coordinator is None:
        return JSONResponse(
            content={"status": "unavailable", "reason": "no coordinator registered"},
            status_code=200,
        )
    try:
        snapshot = _coordinator.snapshot()  # type: ignore[union-attr]
        return JSONResponse(content=snapshot, status_code=200)
    except (OperationalError, DataError, OSError, RuntimeError) as e:
        return JSONResponse(
            content={"status": "error", "message": str(e)[:200]},
            status_code=500,
        )


@app.get("/api/quick-test")
async def quick_test():
    """Quick system connectivity test."""
    return JSONResponse(content=quick_connectivity_snapshot())


@app.get("/api/test")
async def test_system():
    """Run system tests (API, data, signals)."""
    import asyncio
    import os
    import subprocess
    import sys
    
    results = {
        "status": "running",
        "tests": {}
    }
    
    try:
        # Run test script as subprocess to avoid event loop conflicts
        test_script = os.path.join(os.path.dirname(os.path.dirname(__file__)), "src", "test_system.py")
        
        # Use subprocess to run tests in separate process
        process = subprocess.Popen(
            [sys.executable, test_script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        try:
            stdout, stderr = process.communicate(timeout=120)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            raise
        
        # Parse results (simple check for pass/fail indicators)
        results["status"] = "completed"
        results["output"] = stdout[:1000]  # Limit output
        results["exit_code"] = process.returncode
        results["all_passed"] = process.returncode == 0
        
        if stderr:
            results["errors"] = stderr[:500]
        
        return JSONResponse(content=results)
        
    except subprocess.TimeoutExpired:
        return JSONResponse(
            content={"status": "timeout", "message": "Tests took too long (120s timeout)"},
            status_code=504
        )
    except (OperationalError, DataError, OSError, ValueError, TypeError, KeyError) as e:
        return JSONResponse(
            content={
                "status": "error",
                "message": str(e),
                "type": type(e).__name__
            },
            status_code=500
        )



def _debug_signals_impl(symbol_filter: Optional[str] = None) -> dict:
    """Shared logic for /api/debug/signals and /debug/signals."""
    import json

    try:
        from src.storage.repository import get_recent_events

        limit = 20 if symbol_filter else 50
        events = get_recent_events(
            limit=limit,
            event_type="DECISION_TRACE",
            symbol=symbol_filter,
        )
    except (OperationalError, DataError, OSError) as e:
        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__,
            "last_signal": None,
            "checked_events": 0,
            "recent_decisions": [],
        }

    results: dict = {
        "status": "success",
        "last_signal": None,
        "checked_events": 0,
        "recent_decisions": [],
    }

    for ev in events:
        results["checked_events"] += 1
        ts = ev.get("timestamp", "")
        sym = ev.get("symbol", "")
        data = ev.get("details") or {}
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except (ValueError, TypeError, KeyError):
                data = {"error": "failed to parse details"}
        signal = data.get("signal", "NONE")
        quality = data.get("setup_quality", 0)
        reason = data.get("reason")
        if isinstance(reason, list) and reason:
            reasoning_tip = reason[-1]
        elif isinstance(reason, str) and reason.strip():
            reasoning_tip = reason.strip().split("\n")[-1] if "\n" in reason else reason.strip()
        else:
            reasoning_tip = "No reasoning logged"

        op = data.get("order_placed")
        op_reason = data.get("order_fail_reason") or ""
        results["recent_decisions"].append({
            "time": ts,
            "symbol": sym,
            "signal": signal,
            "quality": quality,
            "reasoning": reasoning_tip,
            "order_placed": op,
            "order_fail_reason": op_reason,
        })

        if signal and str(signal).upper() in ("LONG", "SHORT") and results["last_signal"] is None:
            results["last_signal"] = {
                "timestamp": ts,
                "symbol": sym,
                "signal": signal,
                "quality": quality,
                "details": data,
                "reasoning": data.get("reason"),
            }

    return results


_CET = ZoneInfo("Europe/Berlin")


def _format_ts_cet(ts: str) -> str:
    """Format ISO timestamp as human-readable date/time in CET. Returns original if parse fails."""
    if ts is None:
        return "—"
    if not isinstance(ts, str):
        return str(ts)
    ts = ts.strip()
    if not ts:
        return "—"
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local = dt.astimezone(_CET)
        return local.strftime("%d %b %Y, %H:%M %Z")
    except (ValueError, TypeError, KeyError):
        return ts


def _debug_signals_html(data: dict) -> str:
    """Render debug signals payload as human-readable HTML."""
    def esc(s: str) -> str:
        if s is None:
            return ""
        return html_module.escape(str(s))

    status = data.get("status", "unknown")
    checked = data.get("checked_events", 0)
    last = data.get("last_signal")
    decisions = data.get("recent_decisions") or []

    status_class = "ok" if status == "success" else "err"
    status_label = "OK" if status == "success" else esc(status)

    html_parts = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>",
        "<title>Debug Signals</title>",
        "<style>"
        "body{font-family:system-ui,-apple-system,sans-serif;max-width:900px;margin:0 auto;padding:1rem;background:#0f0f12;color:#e4e4e7;}",
        "h1{font-size:1.25rem;font-weight:600;margin:0 0 0.5rem;}",
        ".meta{color:#71717a;font-size:0.875rem;margin-bottom:1.5rem;}",
        ".badge{display:inline-block;padding:0.2rem 0.5rem;border-radius:4px;font-size:0.75rem;font-weight:600;}",
        ".badge.ok{background:#166534;color:#bbf7d0;}",
        ".badge.err{background:#7f1d1d;color:#fecaca;}",
        "h2{font-size:1rem;font-weight:600;margin:1.5rem 0 0.5rem;color:#a1a1aa;}",
        ".card{background:#18181b;border:1px solid #27272a;border-radius:8px;padding:1rem;margin-bottom:1rem;}",
        ".card .row{display:flex;flex-wrap:wrap;gap:1rem;margin-bottom:0.5rem;}",
        ".card .k{color:#71717a;font-size:0.8rem;}",
        ".card .v{font-weight:500;}",
        ".signal-long{color:#22c55e;}",
        ".signal-short{color:#ef4444;}",
        ".signal-none{color:#71717a;}",
        "pre{background:#09090b;border:1px solid #27272a;border-radius:6px;padding:0.75rem;overflow-x:auto;font-size:0.8rem;line-height:1.4;white-space:pre-wrap;word-break:break-word;}",
        "table{width:100%;border-collapse:collapse;font-size:0.875rem;}",
        "th,td{padding:0.5rem;text-align:left;border-bottom:1px solid #27272a;}",
        "th{color:#71717a;font-weight:500;}",
        "tr:hover{background:#18181b;}",
        ".reason{max-width:420px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}",
        "a{color:#3b82f6;}",
        ".notice{background:#1c1917;border:1px solid #44403c;border-radius:6px;padding:0.75rem 1rem;margin:0.5rem 0;font-size:0.875rem;color:#a8a29e;}",
        ".notice strong{color:#fafaf9;}",
        "code{background:#27272a;padding:0.1em 0.35em;border-radius:4px;font-size:0.85em;}",
        "</style></head><body>",
        f"<h1>Debug Signals</h1>",
        f"<div class='meta'>Status: <span class='badge {status_class}'>{status_label}</span> · "
        f"Checked events: {checked} · "
        f"All times CET · "
        f"<a href='?format=json'>JSON</a></div>",
    ]

    if data.get("message"):
        html_parts.append(f"<p class='meta'>{esc(data['message'])}</p>")

    if last:
        d = last.get("details") or {}
        reason = d.get("reason") or ""
        if isinstance(reason, list):
            reason = "\n".join(str(x) for x in reason) if reason else ""
        sym = last.get("symbol", "—")
        sig = (last.get("signal") or "no_signal").lower()
        sig_cls = "signal-long" if sig == "long" else ("signal-short" if sig == "short" else "signal-none")
        html_parts.append("<h2>Last signal</h2>")
        html_parts.append("<div class='card'>")
        html_parts.append("<div class='row'>")
        html_parts.append(f"<span class='k'>Symbol</span><span class='v'>{esc(sym)}</span>")
        html_parts.append(f"<span class='k'>Signal</span><span class='v {sig_cls}'>{esc(str(last.get('signal', '—')))}</span>")
        html_parts.append(f"<span class='k'>Quality</span><span class='v'>{esc(str(last.get('quality', '—')))}</span>")
        html_parts.append(f"<span class='k'>Time</span><span class='v'>{esc(_format_ts_cet(last.get('timestamp') or ''))}</span>")
        html_parts.append("</div>")
        order_placed = d.get("order_placed")
        order_fail_reason = d.get("order_fail_reason") or ""
        html_parts.append("<div class='row' style='margin-top:0.5rem;padding-top:0.5rem;border-top:1px solid #27272a;'>")
        if order_placed is True:
            html_parts.append("<span class='k'>Order opened</span><span class='v' style='color:#22c55e;font-weight:600'>Yes</span>")
        elif order_placed is False:
            fail = esc(order_fail_reason) if order_fail_reason else "No (reason not recorded)"
            html_parts.append("<span class='k'>Order opened</span><span class='v' style='color:#ef4444;font-weight:600'>No</span>")
            if order_fail_reason:
                html_parts.append(f"<span class='k'>Reason</span><span class='v' style='color:#fca5a5'>{fail}</span>")
        else:
            html_parts.append("<span class='k'>Order opened</span><span class='v' style='color:#71717a'>—</span>")
        html_parts.append("</div>")
        if reason:
            html_parts.append("<pre>" + esc(reason) + "</pre>")
        html_parts.append("</div>")
        skipped = d.get("skipped")
        skip_reason = d.get("skip_reason") or ""
        if skipped and skip_reason:
            rr = "no_futures_ticker"
            human = "No Kraken Futures market for this symbol; we skip trading it." if skip_reason == rr else esc(skip_reason)
            html_parts.append(
                f"<div class='notice'><strong>Why no trade?</strong> This signal was not traded: {human}</div>"
            )
        elif order_placed is False and order_fail_reason:
            html_parts.append(
                f"<div class='notice'><strong>Why no order?</strong> {esc(order_fail_reason)}</div>"
            )
        elif order_placed is True:
            html_parts.append(
                "<div class='notice'><strong>Order opened.</strong> Entry was submitted via Execution Gateway.</div>"
            )
        else:
            html_parts.append(
                "<div class='notice'><strong>If this signal didn't result in a Kraken trade</strong>, possible "
                "causes: Risk Manager rejected, State Machine rejected (e.g. max positions), Entry failed "
                "(exchange error), or no futures ticker (we skip). Check worker logs for "
                "<code>Signal skipped (no futures ticker)</code>, <code>Trade rejected by Risk Manager</code>, "
                "<code>Entry REJECTED by State Machine</code>, <code>Entry failed</code>.</div>"
            )

    html_parts.append("<h2>Recent decisions</h2>")
    html_parts.append("<div class='card'><table><thead><tr><th>Time</th><th>Symbol</th><th>Signal</th><th>Quality</th><th>Order opened</th><th>Reasoning</th></tr></thead><tbody>")
    for dec in decisions[:40]:
        t = _format_ts_cet(dec.get("time") or "")
        s = dec.get("symbol", "")
        sig = (dec.get("signal") or "no_signal").lower()
        sig_cls = "signal-long" if sig == "long" else ("signal-short" if sig == "short" else "signal-none")
        q = dec.get("quality", "")
        r = dec.get("reasoning") or "—"
        op = dec.get("order_placed")
        op_reason = dec.get("order_fail_reason") or ""
        if op is True:
            op_cell = "<span style='color:#22c55e'>Yes</span>"
        elif op is False:
            op_cell = f"<span style='color:#ef4444'>No</span>" + (f" <span style='color:#fca5a5;font-size:0.8em' title='{esc(op_reason)}'>{esc(op_reason)[:50]}{'…' if len(op_reason) > 50 else ''}</span>" if op_reason else "")
        else:
            op_cell = "—"
        html_parts.append(
            f"<tr><td>{esc(t)}</td><td>{esc(s)}</td><td class='{sig_cls}'>{esc(str(dec.get('signal', '—')))}</td>"
            f"<td>{esc(str(q))}</td><td>{op_cell}</td><td class='reason' title='{esc(r)}'>{esc(r)}</td></tr>"
        )
    html_parts.append("</tbody></table></div>")
    html_parts.append("</body></html>")
    return "".join(html_parts)


def _debug_signals_respond(data: dict, request: Request, format_param: Optional[str] = None):
    """Return HTML or JSON based on Accept header or ?format=html|json."""
    use_html = False
    if format_param == "html":
        use_html = True
    elif format_param == "json":
        use_html = False
    else:
        accept = (request.headers.get("accept") or "").lower()
        use_html = "text/html" in accept
    if use_html:
        return HTMLResponse(_debug_signals_html(data))
    return JSONResponse(content=data, status_code=200)


@app.get("/api/debug/signals")
async def debug_signals(request: Request, symbol: Optional[str] = None, format: Optional[str] = None):
    """
    Debug endpoint to find the last generated signal.
    Uses system_events DECISION_TRACE. Optional ?symbol=... to filter.
    Browser (Accept: text/html) or ?format=html → human-readable HTML; ?format=json → raw JSON.
    """
    data = _debug_signals_impl(symbol_filter=symbol)
    return _debug_signals_respond(data, request, format_param=format)


@app.get("/debug/signals")
async def debug_signals_no_api(request: Request, symbol: Optional[str] = None, format: Optional[str] = None):
    """Same as /api/debug/signals, for deployments that strip /api prefix."""
    data = _debug_signals_impl(symbol_filter=symbol)
    return _debug_signals_respond(data, request, format_param=format)


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
