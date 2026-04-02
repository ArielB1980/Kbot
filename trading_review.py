"""One-shot trading performance review script for production."""
import os
import psycopg2

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

print("=" * 60)
print("OPEN POSITIONS")
print("=" * 60)
cur.execute("""
    SELECT symbol, side, entry_price, size_notional, leverage, unrealized_pnl, opened_at, is_protected, protection_reason
    FROM positions
    ORDER BY opened_at DESC
""")
rows = cur.fetchall()
if rows:
    for r in rows:
        print(f"  {r[0]} {r[1]} @ {r[2]}  notional={r[3]}  lev={r[4]}  upnl={r[5]}  opened={r[6]}  protected={r[7]} {r[8] or ''}")
else:
    print("  (none)")

print("\n" + "=" * 60)
print("LAST 20 CLOSED TRADES")
print("=" * 60)
cur.execute("""
    SELECT symbol, side, entry_price, exit_price, net_pnl, holding_period_hours, exit_reason, exited_at
    FROM trades
    WHERE exited_at IS NOT NULL
    ORDER BY exited_at DESC
    LIMIT 20
""")
rows = cur.fetchall()
for r in rows:
    print(f"  {r[7]}  {r[0]} {r[1]}  entry={r[2]} exit={r[3]}  pnl={r[4]}  hrs={r[5]}  reason={r[6]}")

print("\n" + "=" * 60)
print("PNL SUMMARY (all closed trades)")
print("=" * 60)
cur.execute("""
    SELECT
        COUNT(*) AS total_trades,
        SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END) AS winners,
        SUM(CASE WHEN net_pnl <= 0 THEN 1 ELSE 0 END) AS losers,
        ROUND(SUM(net_pnl)::numeric, 2) AS total_pnl,
        ROUND(AVG(net_pnl)::numeric, 2) AS avg_pnl,
        ROUND(MAX(net_pnl)::numeric, 2) AS best_trade,
        ROUND(MIN(net_pnl)::numeric, 2) AS worst_trade,
        ROUND(AVG(holding_period_hours)::numeric, 1) AS avg_hold_hrs,
        ROUND(SUM(fees)::numeric, 2) AS total_fees,
        ROUND(SUM(funding)::numeric, 2) AS total_funding
    FROM trades
    WHERE exited_at IS NOT NULL
""")
r = cur.fetchone()
if r:
    total, wins, losses = r[0], r[1], r[2]
    wr = round(100.0 * wins / total, 1) if total > 0 else 0
    print(f"  Total trades: {total}  |  Winners: {wins}  |  Losers: {losses}  |  Win rate: {wr}%")
    print(f"  Total PnL: ${r[3]}  |  Avg PnL: ${r[4]}")
    print(f"  Best: ${r[5]}  |  Worst: ${r[6]}")
    print(f"  Avg hold: {r[7]} hrs  |  Total fees: ${r[8]}  |  Total funding: ${r[9]}")

print("\n" + "=" * 60)
print("MONTHLY PNL BREAKDOWN")
print("=" * 60)
cur.execute("""
    SELECT
        TO_CHAR(exited_at, 'YYYY-MM') AS month,
        COUNT(*) AS trades,
        SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END) AS wins,
        ROUND(SUM(net_pnl)::numeric, 2) AS pnl,
        ROUND(SUM(fees)::numeric, 2) AS fees
    FROM trades
    WHERE exited_at IS NOT NULL
    GROUP BY TO_CHAR(exited_at, 'YYYY-MM')
    ORDER BY month
""")
for r in cur.fetchall():
    wr = round(100.0 * r[2] / r[1], 1) if r[1] > 0 else 0
    print(f"  {r[0]}  trades={r[1]}  wins={r[2]}  wr={wr}%  pnl=${r[3]}  fees=${r[4]}")

print("\n" + "=" * 60)
print("PNL BY SYMBOL")
print("=" * 60)
cur.execute("""
    SELECT
        symbol,
        COUNT(*) AS trades,
        SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END) AS wins,
        ROUND(SUM(net_pnl)::numeric, 2) AS pnl,
        ROUND(AVG(net_pnl)::numeric, 2) AS avg_pnl
    FROM trades
    WHERE exited_at IS NOT NULL
    GROUP BY symbol
    ORDER BY pnl DESC
""")
for r in cur.fetchall():
    wr = round(100.0 * r[2] / r[1], 1) if r[1] > 0 else 0
    print(f"  {r[0]:20s}  trades={r[1]:3d}  wr={wr:5.1f}%  pnl=${r[3]:>10}  avg=${r[4]}")

print("\n" + "=" * 60)
print("ACCOUNT STATE (latest)")
print("=" * 60)
cur.execute("""
    SELECT * FROM account_state ORDER BY 1 DESC LIMIT 1
""")
r = cur.fetchone()
if r:
    cols = [d[0] for d in cur.description]
    for c, v in zip(cols, r):
        print(f"  {c}: {v}")

print("\n" + "=" * 60)
print("RECENT SYSTEM EVENTS (last 20)")
print("=" * 60)
cur.execute("""
    SELECT * FROM system_events ORDER BY 1 DESC LIMIT 20
""")
rows = cur.fetchall()
if rows:
    cols = [d[0] for d in cur.description]
    for r in rows:
        print("  " + " | ".join(f"{c}={v}" for c, v in zip(cols, r)))
else:
    print("  (none)")

print("\n" + "=" * 60)
print("QUARANTINED TRADES")
print("=" * 60)
cur.execute("SELECT COUNT(*) FROM trades_quarantine")
qcount = cur.fetchone()[0]
print(f"  Total quarantined: {qcount}")
if qcount > 0:
    cur.execute("SELECT * FROM trades_quarantine ORDER BY 1 DESC LIMIT 5")
    cols = [d[0] for d in cur.description]
    for r in cur.fetchall():
        print("  " + " | ".join(f"{c}={v}" for c, v in zip(cols, r)))

cur.close()
conn.close()
