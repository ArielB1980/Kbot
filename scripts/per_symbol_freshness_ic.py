"""Per-symbol breakout of OB body_freshness forward returns + simple IC.

Checks whether the aggregate freshness signal is broad-based or concentrated
in one or two symbols (prior experience: fib_1h was ETH-carried, HTF was
symbol-split). Computes, per symbol:
  - Body_freshness grade distribution
  - Mean forward return per grade
  - Spearman-ish IC: correlation between ordinal freshness score
    (untouched=1.0, partial=0.85, tested=0.0) and forward_return_5bar.
"""
import json
import sys
from collections import defaultdict


GRADE_SCORE = {
    "fully_untouched": 1.0,
    "partially_mitigated": 0.85,
    "fully_tested": 0.0,
}


def corr(xs, ys):
    if len(xs) < 2:
        return 0.0
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    denom = (vx * vy) ** 0.5
    return cov / denom if denom else 0.0


def main(path: str) -> None:
    rows = []
    with open(path) as f:
        for line in f:
            rows.append(json.loads(line))

    valid = [
        r for r in rows
        if r.get("forward_return_5bar") is not None
        and r.get("ob_body_freshness") in GRADE_SCORE
    ]

    by_symbol: dict = defaultdict(list)
    for r in valid:
        by_symbol[r.get("symbol", "UNKNOWN")].append(r)

    symbols = sorted(by_symbol.keys())
    print(f"Total N={len(valid)} across {len(symbols)} symbols\n")

    # Per-symbol table
    print(f"{'symbol':10s} {'N':>4s}  "
          f"{'untouched':>22s}  {'partial':>22s}  {'tested':>22s}  "
          f"{'IC_5b':>7s}  {'IC_10b':>7s}")
    print("-" * 120)

    for sym in symbols + ["ALL"]:
        rs = valid if sym == "ALL" else by_symbol[sym]
        buckets = defaultdict(list)
        for r in rs:
            buckets[r["ob_body_freshness"]].append(r)

        cells = []
        for g in ("fully_untouched", "partially_mitigated", "fully_tested"):
            bs = buckets.get(g, [])
            if not bs:
                cells.append(f"{'--':>22s}")
                continue
            r5 = [b["forward_return_5bar"] for b in bs]
            mean = sum(r5) / len(r5) * 100
            hit = sum(1 for x in r5 if x > 0) / len(r5) * 100
            cells.append(f"N={len(bs):3d} {mean:+6.2f}% {hit:4.1f}%h")

        xs = [GRADE_SCORE[r["ob_body_freshness"]] for r in rs]
        ys5 = [r["forward_return_5bar"] for r in rs]
        ys10 = [r.get("forward_return_10bar") or 0.0 for r in rs]
        ic5 = corr(xs, ys5)
        ic10 = corr(xs, ys10)

        print(f"{sym:10s} {len(rs):>4d}  {cells[0]}  {cells[1]}  {cells[2]}  "
              f"{ic5:+7.3f}  {ic10:+7.3f}")

    print("\nIC = correlation(freshness_score, forward_return). Positive → higher freshness → higher return.")

    # Monotonicity check per symbol: untouched > partial > tested on mean 5b
    print("\n=== Monotonicity per symbol (untouched ≥ partial ≥ tested on mean 5b) ===")
    for sym in symbols:
        rs = by_symbol[sym]
        means = {}
        for g in ("fully_untouched", "partially_mitigated", "fully_tested"):
            bs = [r for r in rs if r["ob_body_freshness"] == g]
            means[g] = (sum(r["forward_return_5bar"] for r in bs) / len(bs) * 100) if bs else None
        u, p, t = means["fully_untouched"], means["partially_mitigated"], means["fully_tested"]
        u_vs_t = "?" if u is None or t is None else ("PASS" if u > t else "FAIL")
        monotonic = "?" if None in (u, p, t) else (
            "PASS" if u >= p >= t else
            "PARTIAL" if u > t else
            "FAIL"
        )
        ustr = f"{u:+6.2f}%" if u is not None else "  --  "
        pstr = f"{p:+6.2f}%" if p is not None else "  --  "
        tstr = f"{t:+6.2f}%" if t is not None else "  --  "
        print(f"  {sym:10s}  untouched={ustr}  partial={pstr}  tested={tstr}  "
              f"u>t:{u_vs_t}  monotonic:{monotonic}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1
         else "reports/freshness_validation/combined/decision_data_with_returns.jsonl")
