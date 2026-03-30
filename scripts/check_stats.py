import argparse
import os
import statistics
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from sqlalchemy.exc import OperationalError

# Add src to path
sys.path.append(os.getcwd())

from src.storage.db import get_db
from src.storage.repository import CandleModel, SystemEventModel, TradeModel


def _connection_refused_hint(exc: OperationalError) -> None:
    err = str(exc).lower()
    if "connection refused" not in err and "could not connect" not in err:
        return
    print(
        "\nHint: Postgres is not reachable at DATABASE_URL. Common cases:\n"
        "  • Local docker-compose: use port 5432 (see docker-compose.yml), not 5433.\n"
        "  • Production telemetry on kbot: run this script on the droplet with\n"
        "    `cd /home/trading/TradingSystem && set -a && . ./.env && set +a && ./venv/bin/python scripts/check_stats.py ...`\n"
        "    or SSH-tunnel: `ssh -L 5433:127.0.0.1:5432 kbot` then DATABASE_URL with localhost:5433.\n",
        file=sys.stderr,
    )


def main():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("Error: DATABASE_URL environment variable not set.")
        print("Please export DATABASE_URL='...' and run again.")
        return

    print("Connecting to database...")

    try:
        db = get_db()
    except OperationalError as e:
        _connection_refused_hint(e)
        raise

    # Debug Connection
    url_str = str(db.engine.url)
    masked_url = url_str.split("@")[-1] if "@" in url_str else url_str
    print(f"DB Engine: {db.engine.dialect.name}")
    print(f"DB Host/Info: {masked_url}")

    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=7, help="Lookback hours (default: 7)")
    args = parser.parse_args()

    cutoff = datetime.now(UTC) - timedelta(hours=args.hours)

    try:
        with db.get_session() as session:
            _run_stats_queries(session, args, cutoff)
    except OperationalError as e:
        _connection_refused_hint(e)
        raise


def _run_stats_queries(session, args, cutoff) -> None:
    """Queries for check_stats (separated so first DB use can raise OperationalError with hints)."""
    # Debug Totals
    total_events = session.query(SystemEventModel).count()
    total_trades = session.query(TradeModel).count()
    total_candles = session.query(CandleModel).count()
    print("\n--- Total Database State ---")
    print(f"Total System Events: {total_events}")
    print(f"Total Trades: {total_trades}")
    print(f"Total Candles Stored: {total_candles}")

    print(f"\n--- Statistics for last {args.hours} hours (since {cutoff.strftime('%Y-%m-%d %H:%M:%S UTC')}) ---")

    # 1. Count Signals
    signals_query = session.query(SystemEventModel).filter(
        SystemEventModel.timestamp >= cutoff,
        SystemEventModel.event_type.in_(['SIGNAL', 'SIGNAL_GENERATED'])
    )
    signal_count = signals_query.count()

    # 1b. Count Recent Candles (Ingestion Velocity)
    ingested_candles = session.query(CandleModel).filter(
        CandleModel.timestamp >= cutoff
    ).count()

    # 2. Trades & Performance
    trades_query = session.query(TradeModel).filter(
        TradeModel.exited_at >= cutoff
    ).order_by(TradeModel.exited_at.desc())

    trades = trades_query.all()
    trade_count = len(trades)

    total_pnl = sum(t.net_pnl for t in trades)
    winners = sum(1 for t in trades if t.net_pnl > 0)
    win_rate = (winners / trade_count * 100) if trade_count > 0 else 0.0

    print(f"Signals Found: {signal_count}")
    print(f"Candles Ingested: {ingested_candles} (active collection)")
    print(f"Trades Executed: {trade_count}")
    print(f"Total PnL: ${total_pnl:.2f}")
    print(f"Win Rate: {win_rate:.1f}%")

    # Fee/churn observability
    if trade_count > 0:
        total_fees = sum(float(t.fees or 0) for t in trades)
        total_gross = sum(float(t.gross_pnl or 0) for t in trades)
        winners = [t for t in trades if float(t.gross_pnl or 0) > 0]
        winner_fees = sum(float(t.fees or 0) for t in winners)
        winner_gross = sum(float(t.gross_pnl or 0) for t in winners)
        fee_drag_abs_gross = (total_fees / abs(total_gross) * 100.0) if total_gross else 0.0
        fee_drag_winners = (winner_fees / winner_gross * 100.0) if winner_gross else 0.0
        holds_h = [float(t.holding_period_hours or 0) for t in trades]
        fees_bps = []
        gross_edge_abs_bps = []
        maker_count = 0
        taker_count = 0
        for t in trades:
            notional = float(t.size_notional or 0)
            if notional > 0:
                fees_bps.append(float(t.fees or 0) / notional * 10000.0)
                gross_edge_abs_bps.append(abs(float(t.gross_pnl or 0)) / notional * 10000.0)
            maker_count += int(getattr(t, "maker_fills_count", 0) or 0)
            taker_count += int(getattr(t, "taker_fills_count", 0) or 0)

        def pct(values, p):
            if not values:
                return 0.0
            vals = sorted(values)
            idx = max(int(len(vals) * p) - 1, 0)
            return vals[idx]

        lt_30m = sum(1 for h in holds_h if h < 0.5)
        lt_1h = sum(1 for h in holds_h if h < 1.0)
        h_1_4 = sum(1 for h in holds_h if 1.0 <= h < 4.0)
        ge_4h = sum(1 for h in holds_h if h >= 4.0)

        print("\nFee & Churn KPIs:")
        print(f"  Fee Drag (abs gross): {fee_drag_abs_gross:.1f}%")
        print(f"  Fee Drag (winners):   {fee_drag_winners:.1f}%")
        print(f"  Fees bps p50/p90:     {statistics.median(fees_bps) if fees_bps else 0.0:.2f}/{pct(fees_bps, 0.9):.2f}")
        print(f"  Edge bps p50/p90:     {statistics.median(gross_edge_abs_bps) if gross_edge_abs_bps else 0.0:.2f}/{pct(gross_edge_abs_bps, 0.9):.2f}")
        print(f"  Median hold:          {statistics.median(holds_h) if holds_h else 0.0:.2f}h")
        print(f"  Hold bins:            <30m={lt_30m}, <1h={lt_1h}, 1-4h={h_1_4}, >=4h={ge_4h}")
        print(f"  Maker/Taker fills:    {maker_count}/{taker_count}")

        # Per-symbol fee drag and churn counts
        by_symbol = defaultdict(list)
        for t in sorted(trades, key=lambda x: x.entered_at):
            by_symbol[t.symbol].append(t)

        per_symbol_rows = []
        for sym, ts in by_symbol.items():
            sym_fees = sum(float(t.fees or 0) for t in ts)
            sym_gross = sum(float(t.gross_pnl or 0) for t in ts)
            sym_net = sum(float(t.net_pnl or 0) for t in ts)
            churn_events = 0
            for i in range(len(ts) - 1):
                cur = ts[i]
                nxt = ts[i + 1]
                hold_min = float(cur.holding_period_hours or 0) * 60.0
                reopen_gap_min = (nxt.entered_at - cur.exited_at).total_seconds() / 60.0
                if hold_min <= 60.0 and 0 <= reopen_gap_min <= 120.0:
                    churn_events += 1
            per_symbol_rows.append(
                (
                    sym,
                    len(ts),
                    churn_events,
                    sym_fees,
                    (sym_fees / abs(sym_gross) * 100.0) if sym_gross else 0.0,
                    sym_net,
                )
            )

        per_symbol_rows.sort(key=lambda r: (r[2], r[3]), reverse=True)
        print("\nTop Symbols (churn/fees):")
        for row in per_symbol_rows[:8]:
            sym, ntr, ch, sf, sfd, sn = row
            print(f"  {sym:12s} trades={ntr:2d} churn={ch:2d} fees=${sf:5.2f} fee_drag={sfd:5.1f}% net=${sn:6.2f}")

        print(
            f"\nROLLUP|hours={args.hours}|trades={trade_count}|fee_drag_winners_pct={fee_drag_winners:.1f}|"
            f"lt1h_pct={(lt_1h/trade_count*100.0):.1f}|maker_fills={maker_count}|taker_fills={taker_count}"
        )

    if trade_count > 0:
        print("\nRecent Trades:")
        for t in trades[:5]:
            print(f"  {t.symbol} ({t.side}): ${t.net_pnl:.2f} [{t.exit_reason}]")

    # 3. Market Coverage
    unique_symbols = session.query(SystemEventModel.symbol).distinct().count()
    print(f"Unique Symbols Active: {unique_symbols}")

    if signal_count == 0:
        print("\nDebug: No signals found. Checking recent event types:")
        recent = session.query(SystemEventModel.event_type).distinct().limit(10).all()
        print([r[0] for r in recent])

if __name__ == "__main__":
    main()
