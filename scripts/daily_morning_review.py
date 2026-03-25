#!/usr/bin/env python3
"""Daily Morning Review — 6am CET team standup.

Pulls last-24h trading data from Kraken and the local DB,
generates a structured review with actionable analysis.

Sections:
  1. Account snapshot (equity, margin, available)
  2. Open positions with risk flags
  3. Fills in last 24h (count, volume, fees)
  4. Trade performance: P&L, win rate, exit reason breakdown
  5. Symbol-level attribution
  6. Holding period analysis
  7. Fee drag analysis
  8. Risk flags and alerts

Requires env vars: DATABASE_URL, KRAKEN_FUTURES_API_KEY, KRAKEN_FUTURES_API_SECRET
(loaded from .env.local or .env automatically).
"""

import os
from collections import defaultdict
from datetime import UTC, datetime, timedelta


def _load_env():
    """Load env from .env.local or .env."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for name in (".env.local", ".env"):
        path = os.path.join(repo, name)
        if os.path.isfile(path):
            load_dotenv(path, override=False)
            break


def section(title: str) -> None:
    """Print a section header."""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def fetch_account_and_positions():
    """Query Kraken Futures for account balance and open positions via ccxt."""
    import ccxt

    api_key = os.environ.get("KRAKEN_FUTURES_API_KEY", "").strip()
    api_secret = os.environ.get("KRAKEN_FUTURES_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("  [SKIP] KRAKEN_FUTURES_API_KEY / SECRET not set")
        return None, []

    exchange = ccxt.krakenfutures(
        {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {"defaultType": "future"},
        }
    )

    # Account balance
    section("ACCOUNT SNAPSHOT")
    total_equity = 0.0
    try:
        bal = exchange.fetch_balance()
        for currency, total in bal.get("total", {}).items():
            if total and float(total) > 0:
                free = bal["free"].get(currency, 0)
                used = bal["used"].get(currency, 0)
                total_equity += float(total)
                print(f"  {currency}: total={total}  free={free}  used(margin)={used}")
        if total_equity > 0:
            margin_used = sum(float(bal["used"].get(c, 0) or 0) for c in bal.get("total", {}))
            util = (margin_used / total_equity * 100) if total_equity > 0 else 0
            print(f"\n  Margin utilization: {util:.1f}%")
    except Exception as e:
        print(f"  [ERROR] fetch_balance: {e}")

    # Open positions
    section("OPEN POSITIONS")
    open_pos = []
    try:
        positions = exchange.fetch_positions()
        open_pos = [p for p in positions if p.get("contracts") and float(p["contracts"]) > 0]
        if open_pos:
            total_upnl = 0.0
            for p in open_pos:
                sym = p["symbol"]
                side = p["side"]
                size = p["contracts"]
                entry = p.get("entryPrice", "?")
                upnl = float(p.get("unrealizedPnl") or 0)
                liq = p.get("liquidationPrice", "?")
                leverage = p.get("leverage", "?")
                total_upnl += upnl
                print(
                    f"  {sym}: {side} {size} @ {entry}  "
                    f"uPnL=${upnl:.2f}  liq={liq}  lev={leverage}x"
                )

                # Risk flag: liquidation proximity
                if liq and liq != "?" and entry and entry != "?":
                    entry_f = float(entry)
                    liq_f = float(liq)
                    if entry_f > 0:
                        dist_pct = abs(entry_f - liq_f) / entry_f * 100
                        if dist_pct < 5:
                            print(f"    *** ALERT: liquidation {dist_pct:.1f}% away")

            print(f"\n  Total unrealized P&L: ${total_upnl:.2f}")
            print(f"  Position count: {len(open_pos)}")
        else:
            print("  Flat — no open positions")
    except Exception as e:
        print(f"  [ERROR] fetch_positions: {e}")

    # Recent fills
    section("FILLS (LAST 24H)")
    since_ms = int((datetime.now(UTC) - timedelta(hours=24)).timestamp() * 1000)
    try:
        trades = exchange.fetch_my_trades(since=since_ms, limit=500)
        trades.sort(key=lambda t: t["timestamp"])

        if not trades:
            print("  No fills in last 24h")
            return exchange, open_pos

        total_volume = 0.0
        total_fees = 0.0
        by_symbol: dict[str, dict] = {}

        for t in trades:
            sym = t.get("symbol", "?")
            side = t.get("side", "?")
            amount = float(t.get("amount") or 0)
            cost = float(t.get("cost") or 0)
            fee = float((t.get("fee") or {}).get("cost") or 0)
            total_volume += cost
            total_fees += fee

            if sym not in by_symbol:
                by_symbol[sym] = {
                    "buys": 0,
                    "sells": 0,
                    "volume": 0.0,
                    "fees": 0.0,
                    "count": 0,
                }
            by_symbol[sym]["count"] += 1
            by_symbol[sym]["volume"] += cost
            by_symbol[sym]["fees"] += fee
            if side == "buy":
                by_symbol[sym]["buys"] += amount
            else:
                by_symbol[sym]["sells"] += amount

        print(f"  {len(trades)} fills, ${total_volume:.2f} volume, ${total_fees:.4f} fees")
        if total_volume > 0:
            fee_bps = total_fees / total_volume * 10000
            print(f"  Fee drag: {fee_bps:.1f} bps")

        print()
        for sym, data in sorted(by_symbol.items()):
            net = data["buys"] - data["sells"]
            direction = "net long" if net > 0 else "net short" if net < 0 else "flat"
            print(
                f"  {sym}: {data['count']} fills, "
                f"${data['volume']:.2f} vol, {direction} ({abs(net):.4f})"
            )

    except Exception as e:
        print(f"  [ERROR] fetch_my_trades: {e}")

    return exchange, open_pos


def fetch_db_analysis():
    """Pull trade analytics from the trading database."""
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("  [SKIP] DATABASE_URL not set")
        return

    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(db_url)
        cutoff = datetime.now(UTC) - timedelta(hours=24)
        cutoff_7d = datetime.now(UTC) - timedelta(days=7)

        with engine.connect() as conn:
            # --- Trade performance (24h) ---
            section("TRADE PERFORMANCE (24H)")
            result = conn.execute(
                text(
                    "SELECT symbol, side, entry_price, exit_price, "
                    "net_pnl, gross_pnl, fees, funding, "
                    "exit_reason, holding_period_hours, "
                    "entered_at, exited_at "
                    "FROM trades "
                    "WHERE exited_at > :cutoff "
                    "ORDER BY exited_at DESC"
                ),
                {"cutoff": cutoff},
            )
            rows = result.fetchall()

            if not rows:
                print("  No closed trades in last 24h")
            else:
                total_pnl = 0.0
                total_gross = 0.0
                total_fees_db = 0.0
                total_funding = 0.0
                wins = 0
                losses = 0
                win_pnl = 0.0
                loss_pnl = 0.0
                exit_reasons: dict[str, int] = defaultdict(int)
                by_sym: dict[str, float] = defaultdict(float)
                holding_hours: list[float] = []

                for r in rows:
                    pnl = float(r[4] or 0)
                    gross = float(r[5] or 0)
                    fee = float(r[6] or 0)
                    fund = float(r[7] or 0)
                    reason = r[8] or "unknown"
                    hold_h = float(r[9] or 0)

                    total_pnl += pnl
                    total_gross += gross
                    total_fees_db += fee
                    total_funding += fund
                    exit_reasons[reason] += 1
                    by_sym[r[0]] += pnl
                    holding_hours.append(hold_h)

                    if pnl > 0:
                        wins += 1
                        win_pnl += pnl
                    elif pnl < 0:
                        losses += 1
                        loss_pnl += pnl

                    status_str = f"${pnl:+.2f}"
                    print(
                        f"  {r[0]} {r[1]} "
                        f"entry={r[2]} exit={r[3]} "
                        f"{status_str} [{reason}] {hold_h:.1f}h"
                    )

                total_trades = wins + losses
                win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
                avg_win = (win_pnl / wins) if wins > 0 else 0
                avg_loss = (loss_pnl / losses) if losses > 0 else 0
                profit_factor = abs(win_pnl / loss_pnl) if loss_pnl != 0 else float("inf")
                avg_hold = sum(holding_hours) / len(holding_hours) if holding_hours else 0

                print("\n  --- Summary ---")
                print(f"  Net P&L:        ${total_pnl:+.2f}")
                print(f"  Gross P&L:      ${total_gross:+.2f}")
                print(f"  Fees:           ${total_fees_db:.2f}")
                print(f"  Funding:        ${total_funding:+.2f}")
                print(f"  Win rate:       {win_rate:.0f}% ({wins}W / {losses}L)")
                print(f"  Avg win:        ${avg_win:+.2f}")
                print(f"  Avg loss:       ${avg_loss:+.2f}")
                print(f"  Profit factor:  {profit_factor:.2f}")
                print(f"  Avg hold time:  {avg_hold:.1f}h")

            # --- Exit reason breakdown ---
            if rows:
                section("EXIT REASON BREAKDOWN (24H)")
                for reason, count in sorted(exit_reasons.items(), key=lambda x: -x[1]):
                    pct = count / len(rows) * 100
                    print(f"  {reason}: {count} ({pct:.0f}%)")

                # Flag excessive stop-outs
                stop_count = exit_reasons.get("stop_loss", 0)
                if stop_count > 0 and len(rows) > 0:
                    stop_pct = stop_count / len(rows) * 100
                    if stop_pct > 60:
                        print(
                            f"\n  *** ALERT: {stop_pct:.0f}% stop-outs — "
                            f"review entry quality or stop placement"
                        )

            # --- Symbol-level P&L attribution ---
            if rows:
                section("P&L BY SYMBOL (24H)")
                for sym, pnl in sorted(by_sym.items(), key=lambda x: x[1], reverse=True):
                    bar = (
                        "+" * max(1, int(abs(pnl) * 2))
                        if pnl > 0
                        else "-" * max(1, int(abs(pnl) * 2))
                    )
                    print(f"  {sym:20s}  ${pnl:+.2f}  {bar}")

            # --- 7-day trailing performance for context ---
            section("TRAILING 7-DAY PERFORMANCE")
            result_7d = conn.execute(
                text(
                    "SELECT COUNT(*), "
                    "SUM(net_pnl), "
                    "SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END), "
                    "SUM(CASE WHEN net_pnl < 0 THEN 1 ELSE 0 END), "
                    "AVG(holding_period_hours), "
                    "SUM(fees), "
                    "SUM(funding) "
                    "FROM trades WHERE exited_at > :cutoff"
                ),
                {"cutoff": cutoff_7d},
            )
            r7 = result_7d.fetchone()
            if r7 and r7[0] and int(r7[0]) > 0:
                t7_count = int(r7[0])
                t7_pnl = float(r7[1] or 0)
                t7_wins = int(r7[2] or 0)
                t7_losses = int(r7[3] or 0)
                t7_avg_hold = float(r7[4] or 0)
                t7_fees = float(r7[5] or 0)
                t7_funding = float(r7[6] or 0)
                t7_wr = t7_wins / (t7_wins + t7_losses) * 100 if (t7_wins + t7_losses) > 0 else 0
                print(f"  Trades:     {t7_count}")
                print(f"  Net P&L:    ${t7_pnl:+.2f}")
                print(f"  Win rate:   {t7_wr:.0f}% ({t7_wins}W/{t7_losses}L)")
                print(f"  Fees:       ${t7_fees:.2f}")
                print(f"  Funding:    ${t7_funding:+.2f}")
                print(f"  Avg hold:   {t7_avg_hold:.1f}h")

                # Daily P&L trend
                daily_result = conn.execute(
                    text(
                        "SELECT DATE(exited_at) as d, "
                        "SUM(net_pnl), COUNT(*) "
                        "FROM trades WHERE exited_at > :cutoff "
                        "GROUP BY DATE(exited_at) ORDER BY d"
                    ),
                    {"cutoff": cutoff_7d},
                )
                daily_rows = daily_result.fetchall()
                if daily_rows:
                    print("\n  Daily P&L trend:")
                    for dr in daily_rows:
                        dpnl = float(dr[1] or 0)
                        print(f"    {dr[0]}: ${dpnl:+.2f} ({dr[2]} trades)")
            else:
                print("  No trades in last 7 days")

            # --- System events ---
            section("SYSTEM EVENTS (24H)")
            result = conn.execute(
                text(
                    "SELECT event_type, COUNT(*) as cnt "
                    "FROM system_events "
                    "WHERE timestamp > :cutoff "
                    "GROUP BY event_type ORDER BY cnt DESC"
                ),
                {"cutoff": cutoff},
            )
            event_rows = result.fetchall()
            if event_rows:
                for row in event_rows:
                    print(f"  {row[0]}: {row[1]}")
            else:
                print("  No system events in last 24h")

    except Exception as e:
        print(f"  [ERROR] DB analysis failed: {e}")
        import traceback

        traceback.print_exc()


def generate_alerts(open_pos: list) -> list[str]:
    """Generate risk alerts based on current state."""
    alerts = []

    if len(open_pos) > 5:
        alerts.append(f"HIGH position count: {len(open_pos)} open")

    total_upnl = sum(float(p.get("unrealizedPnl") or 0) for p in open_pos)
    if total_upnl < -50:
        alerts.append(f"Significant unrealized loss: ${total_upnl:.2f}")

    # Check for concentrated risk (single symbol > 40% of positions)
    if len(open_pos) > 1:
        notionals = {}
        for p in open_pos:
            sym = p.get("symbol", "?")
            notional = abs(float(p.get("notional") or p.get("contracts") or 0))
            notionals[sym] = notionals.get(sym, 0) + notional
        total_notional = sum(notionals.values())
        if total_notional > 0:
            for sym, val in notionals.items():
                pct = val / total_notional * 100
                if pct > 40:
                    alerts.append(f"Concentration risk: {sym} is {pct:.0f}% of book")

    return alerts


def main():
    """Run the daily morning review."""
    _load_env()

    now = datetime.now(UTC)
    print(f"DAILY MORNING REVIEW — {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'=' * 60}")

    exchange, open_pos = fetch_account_and_positions()
    fetch_db_analysis()

    # Risk alerts
    alerts = generate_alerts(open_pos) if open_pos else []
    section("ALERTS")
    if alerts:
        for a in alerts:
            print(f"  *** {a}")
    else:
        print("  No alerts")

    # Action items
    section("ACTION ITEMS")
    print("  Review the above data and consider:")
    print("  1. Are stop-out rates acceptable? Adjust stops or entry quality?")
    print("  2. Is fee drag eating into edge? Shift to more maker fills?")
    print("  3. Any symbols consistently losing? Consider removing from universe.")
    print("  4. Is holding period optimal? Too short = overtrading, too long = capital drag.")
    print("  5. Is the 7-day trend improving or deteriorating?")
    print()


if __name__ == "__main__":
    main()
