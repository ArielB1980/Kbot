"""
Production runtime guards.

This module is intentionally dependency-light so it can be imported early by entrypoints
without triggering heavy side effects (e.g. config loading, exchange clients).
"""

from __future__ import annotations

import hashlib
import os
import signal
import threading
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

from src.exceptions import OperationalError
from src.monitoring.logger import get_logger
from src.utils.secret_manager import get_database_url

logger = get_logger(__name__)


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    return v


def _parse_bool(value: Optional[str], *, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def is_dry_run_env() -> bool:
    # Normalize to a single boolean source-of-truth.
    # Prefer DRY_RUN, fall back to SYSTEM_DRY_RUN for compatibility with older deploy envs.
    return _parse_bool(_env("DRY_RUN", _env("SYSTEM_DRY_RUN", "0")), default=False)


def environment_env() -> str:
    return str(_env("ENVIRONMENT", "prod") or "prod").strip().lower()


def is_prod_live_env() -> bool:
    # Define “prod live” once and use it everywhere.
    return environment_env() == "prod" and not is_dry_run_env()


def use_state_machine_v2_env() -> bool:
    return str(_env("USE_STATE_MACHINE_V2", "false") or "false").strip().lower() == "true"


def confirm_live_env() -> bool:
    # Exact confirmation string to prevent accidental live deploys.
    return str(_env("CONFIRM_LIVE", "") or "").strip().upper() == "YES"


def assert_prod_live_prereqs() -> None:
    """
    Fail-fast safety checks for production live trading.

    Intentionally evaluates only env vars so it can run before importing heavy runtime code.
    """
    if not is_prod_live_env():
        return

    if not confirm_live_env():
        raise RuntimeError(
            "PROD_LIVE_GUARD_FAILED: CONFIRM_LIVE=YES is required to run live trading in production."
        )

    if not use_state_machine_v2_env():
        raise RuntimeError(
            "PROD_LIVE_GUARD_FAILED: USE_STATE_MACHINE_V2=true is required in prod live "
            "(legacy position management is not permitted)."
        )

    replay_env_keys = sorted(
        k for k, v in os.environ.items() if k.startswith("REPLAY_") and str(v).strip() != ""
    )
    if replay_env_keys:
        sample = ", ".join(replay_env_keys[:8])
        extra = len(replay_env_keys) - 8
        suffix = f" (+{extra} more)" if extra > 0 else ""
        raise RuntimeError(
            "PROD_LIVE_GUARD_FAILED: REPLAY_* environment variables are not allowed in prod live. "
            f"Remove these before restart: {sample}{suffix}."
        )


def _account_fingerprint_seed() -> str:
    # Prefer explicit, stable, non-secret seed (recommended for multi-venue setups).
    seed = _env("ACCOUNT_FINGERPRINT_SEED")
    if seed and seed.strip():
        return seed.strip()

    # Fallback to venue-specific env vars (never log raw values).
    candidates = [
        "KRAKEN_FUTURES_API_KEY",
        "BINANCE_API_KEY",
        "OKX_API_KEY",
        "BYBIT_API_KEY",
    ]
    for k in candidates:
        v = _env(k)
        if v and v.strip():
            return v.strip()

    # In prod live, require an account-scoped seed to avoid cross-account lock collisions.
    if is_prod_live_env():
        raise RuntimeError(
            "ACCOUNT_FINGERPRINT_SEED (preferred) or a venue API key env var is required in prod live "
            "to compute an account-scoped advisory lock key."
        )

    return "unknown-account"


def account_fingerprint() -> str:
    seed = _account_fingerprint_seed().encode("utf-8")
    return hashlib.sha256(seed).hexdigest()[:12]


def _lock_key_string(*, exchange_name: str, market_type: str) -> str:
    # Stable, deterministic, account-scoped lock identity.
    return f"exchange={exchange_name}|market={market_type}|acct={account_fingerprint()}"


def _int64_from_sha256(s: str) -> tuple[int, str]:
    h = hashlib.sha256(s.encode("utf-8")).digest()
    raw8 = h[:8]
    u = int.from_bytes(raw8, "big", signed=False)
    # Convert to signed int64 for Postgres BIGINT.
    i = u if u < (1 << 63) else u - (1 << 64)
    return i, raw8.hex()  # short hex for logging/ops


@dataclass
class ProdLiveAdvisoryLock:
    """
    Holds a Postgres advisory lock for process lifetime.

    - Uses pg_try_advisory_lock(bigint) to acquire without hanging.
    - Stores a dedicated connection (NullPool) so it isn't recycled.
    - Release is explicit on graceful shutdown; crash releases automatically when session closes.
    """

    database_url: str
    lock_key: int
    lock_key_short: str
    exchange_name: str
    market_type: str
    account_fingerprint: str

    _engine: any = None
    _conn: any = None
    _monitor_stop: threading.Event | None = None
    _monitor_thread: threading.Thread | None = None

    def acquire(self) -> None:
        if self._conn is not None:
            return

        self._engine = create_engine(self.database_url, poolclass=NullPool, future=True)
        self._conn = self._engine.connect()
        ok = bool(self._conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": self.lock_key}).scalar())
        if not ok:
            try:
                self._conn.close()
            finally:
                self._conn = None
                if self._engine is not None:
                    self._engine.dispose()
                    self._engine = None
            raise RuntimeError(
                "PROD_LIVE_LOCK_NOT_ACQUIRED: Another live trading process is already running for this account. "
                f"lock_key_short={self.lock_key_short}"
            )

        logger.critical(
            "PROD_LIVE_LOCK_ACQUIRED",
            lock_key_short=self.lock_key_short,
            exchange=self.exchange_name,
            market_type=self.market_type,
            account_fingerprint=self.account_fingerprint,
        )

    def ping(self) -> None:
        if self._conn is None:
            raise RuntimeError("PROD_LIVE_LOCK_LOST: lock connection is not initialized")
        # If the connection is dead, the advisory lock is already lost (session-level lock).
        self._conn.execute(text("SELECT 1"))

    def db_identity(self) -> dict:
        """
        Return sanitized DB identity info (no password).
        """
        try:
            p = urlparse(self.database_url)
            return {
                "db_host": p.hostname or "unknown",
                "db_port": p.port or 5432,
                "db_name": (p.path or "").lstrip("/") or "unknown",
                "db_user": p.username or "unknown",
            }
        except (ValueError, TypeError, KeyError):
            return {"db_host": "unknown", "db_port": None, "db_name": "unknown", "db_user": "unknown"}

    def schema_fingerprint(self) -> Optional[str]:
        """
        Best-effort schema fingerprint (sha256 hex) from information_schema.
        Returns None on failure (permissions, connectivity).
        """
        if self._conn is None:
            return None
        try:
            rows = self._conn.execute(
                text(
                    """
                    SELECT table_name, column_name, data_type, is_nullable, ordinal_position
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    ORDER BY table_name, ordinal_position
                    """
                )
            ).fetchall()
            payload = "|".join(
                f"{r[0]}:{r[1]}:{r[2]}:{r[3]}:{r[4]}" for r in rows
            ).encode("utf-8")
            return hashlib.sha256(payload).hexdigest()[:12]
        except (OperationalError, OSError, ValueError):
            return None

    def release(self) -> None:
        self.stop_monitor()
        if self._conn is None:
            return
        try:
            self._conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": self.lock_key})
        except (OperationalError, OSError) as e:
            logger.warning("PROD_LIVE_LOCK_RELEASE_FAILED", error=str(e), lock_key_short=self.lock_key_short)
        finally:
            try:
                self._conn.close()
            finally:
                self._conn = None
                if self._engine is not None:
                    self._engine.dispose()
                    self._engine = None

    def start_monitor(
        self,
        *,
        interval_seconds: int = 30,
        on_lost_signal: int = signal.SIGTERM,
    ) -> None:
        """
        Best-effort “still held” check.

        If the lock connection dies, we consider the lock lost and request process termination.
        We do NOT attempt automatic re-acquire in prod live.
        """
        if self._monitor_thread is not None:
            return
        self._monitor_stop = threading.Event()

        def _run():
            while self._monitor_stop and not self._monitor_stop.is_set():
                try:
                    self.ping()
                except Exception as e:
                    logger.critical(
                        "PROD_LIVE_LOCK_LOST",
                        error=str(e),
                        lock_key_short=self.lock_key_short,
                        action="signal_process",
                        signal=on_lost_signal,
                    )
                    # Fail closed: latch kill switch even if process doesn't exit cleanly.
                    try:
                        from src.utils.kill_switch import KillSwitch, KillSwitchReason
                        KillSwitch().activate_sync(KillSwitchReason.RECONCILIATION_FAILURE)
                    except (OperationalError, ImportError, OSError):
                        pass
                    try:
                        os.kill(os.getpid(), on_lost_signal)
                    except (OSError, ValueError):
                        pass
                    return
                time.sleep(max(1, int(interval_seconds)))

        self._monitor_thread = threading.Thread(target=_run, name="prod-live-lock-monitor", daemon=True)
        self._monitor_thread.start()

    def stop_monitor(self) -> None:
        if self._monitor_stop is not None:
            self._monitor_stop.set()
        self._monitor_stop = None
        self._monitor_thread = None


def acquire_prod_live_lock(
    *,
    exchange_name: str = "kraken",
    market_type: str = "futures",
) -> Optional[ProdLiveAdvisoryLock]:
    """
    Acquire the prod-live distributed lock if applicable.
    Returns the lock object (caller should release on shutdown).
    """
    if not is_prod_live_env():
        return None

    db_url = get_database_url()
    acct_fp = account_fingerprint()
    key_s = _lock_key_string(exchange_name=exchange_name, market_type=market_type)
    lock_key, short_hex = _int64_from_sha256(key_s)
    lock = ProdLiveAdvisoryLock(
        database_url=db_url,
        lock_key=lock_key,
        lock_key_short=short_hex,
        exchange_name=exchange_name,
        market_type=market_type,
        account_fingerprint=acct_fp,
    )
    lock.acquire()
    # Start a lightweight monitor: if connection drops, signal termination.
    lock.start_monitor(interval_seconds=int(_env("PROD_LIVE_LOCK_MONITOR_SECONDS", "30") or "30"))
    return lock
