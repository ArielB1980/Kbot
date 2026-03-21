#!/usr/bin/env python3
"""
One-time cleanup utility for malformed trade rows.

Safety model:
- Default mode is DRY RUN (no data changes).
- APPLY mode requires both --apply and --yes.
- Rows are copied into trades_quarantine before deletion from trades.

Targets:
1) Malformed rows (default):
   - entry_price <= 0 OR size_notional <= 0
2) Optional:
   - exit_reason = 'unknown' (enable with --include-unknown-exit)
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for env_name in (".env.local", ".env"):
        env_path = os.path.join(repo_root, env_name)
        if os.path.isfile(env_path):
            load_dotenv(env_path, override=False)
            break


def _require_db_url() -> str:
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        raise RuntimeError("DATABASE_URL is required")
    return db_url


def _build_where_clause(include_unknown_exit: bool) -> str:
    base = "(entry_price <= 0 OR size_notional <= 0)"
    if include_unknown_exit:
        return f"({base} OR exit_reason = 'unknown')"
    return base


def _preview_candidates(conn, where_clause: str, cutoff: datetime) -> None:
    summary = conn.execute(
        text(
            f"""
            SELECT
                COUNT(*) AS rows,
                COALESCE(SUM(net_pnl), 0) AS total_net_pnl,
                COALESCE(SUM(gross_pnl), 0) AS total_gross_pnl,
                COALESCE(SUM(fees), 0) AS total_fees,
                COALESCE(SUM(funding), 0) AS total_funding
            FROM trades
            WHERE exited_at >= :cutoff
              AND {where_clause}
            """
        ),
        {"cutoff": cutoff},
    ).mappings().one()

    print("=== Candidate Summary ===")
    print(
        {
            "rows": int(summary["rows"]),
            "total_net_pnl": float(summary["total_net_pnl"]),
            "total_gross_pnl": float(summary["total_gross_pnl"]),
            "total_fees": float(summary["total_fees"]),
            "total_funding": float(summary["total_funding"]),
        }
    )

    sample = conn.execute(
        text(
            f"""
            SELECT
                trade_id, symbol, side, entry_price, exit_price,
                size_notional, net_pnl, exit_reason, entered_at, exited_at
            FROM trades
            WHERE exited_at >= :cutoff
              AND {where_clause}
            ORDER BY exited_at DESC
            LIMIT 20
            """
        ),
        {"cutoff": cutoff},
    ).mappings().all()

    print("\n=== Candidate Sample (up to 20) ===")
    if not sample:
        print("(none)")
        return
    for row in sample:
        print(dict(row))


def _apply_quarantine(conn, where_clause: str, cutoff: datetime, reason: str) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS trades_quarantine
            (LIKE trades INCLUDING ALL)
            """
        )
    )
    conn.execute(
        text(
            """
            ALTER TABLE trades_quarantine
            ADD COLUMN IF NOT EXISTS quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            """
        )
    )
    conn.execute(
        text(
            """
            ALTER TABLE trades_quarantine
            ADD COLUMN IF NOT EXISTS quarantine_reason TEXT NOT NULL DEFAULT 'data_integrity'
            """
        )
    )

    inserted = conn.execute(
        text(
            f"""
            INSERT INTO trades_quarantine (
                trade_id, symbol, side, entry_price, exit_price, size, size_notional,
                leverage, gross_pnl, fees, funding, net_pnl, entered_at, exited_at,
                holding_period_hours, exit_reason, maker_fills_count, taker_fills_count,
                quarantined_at, quarantine_reason
            )
            SELECT
                trade_id, symbol, side, entry_price, exit_price, size, size_notional,
                leverage, gross_pnl, fees, funding, net_pnl, entered_at, exited_at,
                holding_period_hours, exit_reason, maker_fills_count, taker_fills_count,
                NOW(), :reason
            FROM trades
            WHERE exited_at >= :cutoff
              AND {where_clause}
            ON CONFLICT (trade_id) DO NOTHING
            """
        ),
        {"cutoff": cutoff, "reason": reason},
    )

    deleted = conn.execute(
        text(
            f"""
            DELETE FROM trades
            WHERE exited_at >= :cutoff
              AND {where_clause}
            """
        ),
        {"cutoff": cutoff},
    )

    print("\n=== Apply Result ===")
    print({"inserted_into_quarantine": inserted.rowcount, "deleted_from_trades": deleted.rowcount})


def main() -> int:
    parser = argparse.ArgumentParser(description="Quarantine malformed trade rows safely.")
    parser.add_argument(
        "--days",
        type=int,
        default=3650,
        help="Lookback window in days (default: 3650 / ~10 years)",
    )
    parser.add_argument(
        "--include-unknown-exit",
        action="store_true",
        help="Also target rows with exit_reason='unknown'.",
    )
    parser.add_argument(
        "--reason",
        type=str,
        default="trade_data_integrity_cleanup_v1",
        help="Reason tag stored in trades_quarantine.quarantine_reason.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply quarantine+delete changes (otherwise dry-run).",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Required with --apply to confirm destructive step.",
    )
    args = parser.parse_args()

    _load_env()
    db_url = _require_db_url()

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    where_clause = _build_where_clause(args.include_unknown_exit)

    print("=== Cleanup Configuration ===")
    print(
        {
            "mode": "APPLY" if args.apply else "DRY_RUN",
            "days": args.days,
            "cutoff_utc": cutoff.isoformat(),
            "include_unknown_exit": args.include_unknown_exit,
            "reason": args.reason,
        }
    )

    engine = create_engine(db_url, poolclass=NullPool)
    try:
        with engine.begin() as conn:
            _preview_candidates(conn, where_clause, cutoff)

            if not args.apply:
                print("\nDry run only. No changes were made.")
                return 0

            if not args.yes:
                raise RuntimeError("Refusing to apply without --yes confirmation")

            _apply_quarantine(conn, where_clause, cutoff, args.reason)
            print("Cleanup transaction committed.")
            return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
