#!/usr/bin/env python3
"""Analyze fees from Kraken Futures fills over a given window."""
import os
import sys
import json
from datetime import datetime, timezone, timedelta
from decimal import Decimal

# Allow running from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv
    for env_path in [".env.local", ".env"]:
        if os.path.exists(env_path):
            load_dotenv(env_path)
            break
except ImportError:
    pass

import ccxt

HOURS = int(os.environ.get("ANALYSIS_HOURS", "72"))
MAKER_FEE_RATE = Decimal("0.0002")   # 0.02%
TAKER_FEE_RATE = Decimal("0.0005")   # 0.05%

api_key = os.environ.get("KRAKEN_FUTURES_API_KEY")
api_secret = os.environ.get("KRAKEN_FUTURES_API_SECRET")
if not api_key or not api_secret:
    print("ERROR: KRAKEN_FUTURES_API_KEY and KRAKEN_FUTURES_API_SECRET required", file=sys.stderr)
    sys.exit(1)

exchange = ccxt.krakenfutures({"apiKey": api_key, "secret": api_secret})

cutoff = datetime.now(timezone.utc) - timedelta(hours=HOURS)
cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%S")

# Fetch all raw fills with fillType
all_fills = []
last_time = None
for _ in range(20):  # safety limit
    params = {"lastFillTime": last_time} if last_time else {}
    result = exchange.privateGetFills(params=params)
    fills = result.get("fills", [])
    if not fills:
        break
    new_fills = [f for f in fills if f["fillTime"] >= cutoff_str]
    all_fills.extend(new_fills)
    if len(fills) < 100 or not new_fills:
        break
    last_time = fills[-1]["fillTime"]

all_fills.sort(key=lambda x: x["fillTime"])
print(f"=== KRAKEN FUTURES FEE ANALYSIS ({HOURS}h, {len(all_fills)} fills) ===\n")

# Compute per-fill fees
total_volume = Decimal(0)
total_maker_volume = Decimal(0)
total_taker_volume = Decimal(0)
total_maker_fees = Decimal(0)
total_taker_fees = Decimal(0)
total_fills_maker = 0
total_fills_taker = 0
by_symbol = {}

for f in all_fills:
    sym = f["symbol"]
    size = Decimal(str(f["size"]))
    price = Decimal(str(f["price"]))
    cost = size * price
    fill_type = f.get("fillType", "taker")

    if fill_type == "maker":
        fee = cost * MAKER_FEE_RATE
        total_maker_volume += cost
        total_maker_fees += fee
        total_fills_maker += 1
    else:
        fee = cost * TAKER_FEE_RATE
        total_taker_volume += cost
        total_taker_fees += fee
        total_fills_taker += 1

    total_volume += cost

    if sym not in by_symbol:
        by_symbol[sym] = {
            "buys": Decimal(0), "sells": Decimal(0),
            "buy_cost": Decimal(0), "sell_cost": Decimal(0),
            "fees": Decimal(0), "maker": 0, "taker": 0,
        }
    by_symbol[sym]["fees"] += fee
    if f["side"] == "buy":
        by_symbol[sym]["buys"] += size
        by_symbol[sym]["buy_cost"] += cost
    else:
        by_symbol[sym]["sells"] += size
        by_symbol[sym]["sell_cost"] += cost
    if fill_type == "maker":
        by_symbol[sym]["maker"] += 1
    else:
        by_symbol[sym]["taker"] += 1

total_fees = total_maker_fees + total_taker_fees
total_fills = total_fills_maker + total_fills_taker

print("--- VOLUME & FEES ---")
print(f"  Total volume:        USD {total_volume:>10.2f}")
print(f"  Maker fills: {total_fills_maker:>4d}     volume: USD {total_maker_volume:>10.2f}    fees: USD {total_maker_fees:>.4f} (@ {MAKER_FEE_RATE*100}%)")
print(f"  Taker fills: {total_fills_taker:>4d}     volume: USD {total_taker_volume:>10.2f}    fees: USD {total_taker_fees:>.4f} (@ {TAKER_FEE_RATE*100}%)")
print(f"  Total est. fees:     USD {total_fees:>10.4f}")
if total_volume > 0:
    print(f"  Effective blended rate: {total_fees/total_volume*100:.4f}%")
if total_fills > 0:
    print(f"  Maker/Taker ratio:   {total_fills_maker}/{total_fills_taker} ({total_fills_maker/total_fills*100:.0f}% / {total_fills_taker/total_fills*100:.0f}%)")

print(f"\n--- PER SYMBOL ---")
header = f"{'Symbol':15s} | {'Buy Vol':>10s} | {'Sell Vol':>10s} | {'Gross PnL':>10s} | {'Fees':>8s} | {'Net PnL':>10s} | M/T"
print(header)
print("-" * len(header))

total_gross_pnl = Decimal(0)
for sym in sorted(by_symbol.keys()):
    d = by_symbol[sym]
    gross = d["sell_cost"] - d["buy_cost"]
    net = gross - d["fees"]
    total_gross_pnl += gross
    print(f"{sym:15s} | {d['buy_cost']:>10.2f} | {d['sell_cost']:>10.2f} | {gross:>+10.2f} | {d['fees']:>8.4f} | {net:>+10.2f} | {d['maker']}/{d['taker']}")

print("-" * len(header))
net_total = total_gross_pnl - total_fees
print(f"{'TOTAL':15s} | {'':>10s} | {'':>10s} | {total_gross_pnl:>+10.2f} | {total_fees:>8.4f} | {net_total:>+10.2f} |")

if abs(total_gross_pnl) > 0:
    print(f"\n  Fee drag (fees / |gross PnL|): {total_fees / abs(total_gross_pnl) * 100:.1f}%")

# What if ALL fills were taker?
hypothetical_all_taker = total_volume * TAKER_FEE_RATE
# What if ALL fills were maker?
hypothetical_all_maker = total_volume * MAKER_FEE_RATE

print(f"\n--- SENSITIVITY ---")
print(f"  If ALL taker (0.05%):  USD {hypothetical_all_taker:.4f}")
print(f"  If ALL maker (0.02%):  USD {hypothetical_all_maker:.4f}")
print(f"  Actual (blended):      USD {total_fees:.4f}")
print(f"  Maker-vs-taker savings: USD {hypothetical_all_taker - total_fees:.4f} saved by maker fills")

print(f"\n--- IMPORTANT CAVEATS ---")
print(f"  * Kraken Futures fills API does NOT include fee amounts — fees computed from fillType + schedule")
print(f"  * Actual fee schedule confirmed: maker=0.02%, taker=0.05% (tier 1, < $5M volume)")
print(f"  * Funding costs are NOT included (separate from trading fees)")
print(f"  * Open positions (not yet closed) show incomplete round-trip PnL")
print(f"  * Gross PnL per symbol = sum(sell_cost) - sum(buy_cost); NEGATIVE means net buyer (open position)")
