"""One-shot bucketing analysis for freshness validation replay output."""
import json
import sys
from collections import defaultdict


def main(path: str) -> None:
    rows = []
    with open(path) as f:
        for line in f:
            rows.append(json.loads(line))

    valid = [r for r in rows if r.get("forward_return_5bar") is not None]

    def summarize(name: str, key: str) -> None:
        print(f"\n=== {name} ===")
        b = defaultdict(list)
        for r in valid:
            g = r.get(key) or "null"
            b[g].append(r)
        for g in ["fully_untouched", "partially_mitigated", "fully_tested", "null"]:
            rs = b.get(g, [])
            if not rs:
                continue
            r5 = [r["forward_return_5bar"] for r in rs]
            r10 = [r["forward_return_10bar"] for r in rs if r.get("forward_return_10bar") is not None]
            hit5 = sum(1 for x in r5 if x > 0) / len(r5) * 100
            mean5 = sum(r5) / len(r5) * 100
            mean10 = (sum(r10) / len(r10) * 100) if r10 else 0
            hit10 = (sum(1 for x in r10 if x > 0) / len(r10) * 100) if r10 else 0
            print(
                f"  {g:26s} N={len(rs):4d}  mean5b={mean5:+6.2f}%  hit5b={hit5:5.1f}%  "
                f"mean10b={mean10:+6.2f}%  hit10b={hit10:5.1f}%"
            )

    print(f"Total N={len(valid)}")
    summarize("OB wick-zone (ob_freshness)", "ob_freshness")
    summarize("OB body-zone (ob_body_freshness)", "ob_body_freshness")
    summarize("FVG (fvg_freshness)", "fvg_freshness")

    print("\n=== WICK -> BODY grade migration (OB) ===")
    ct: dict = defaultdict(lambda: defaultdict(int))
    for r in valid:
        w = r.get("ob_freshness") or "null"
        b2 = r.get("ob_body_freshness") or "null"
        ct[w][b2] += 1
    header = "wick\\body"
    print(f"  {header:26s} {'untouched':>12s} {'partial':>12s} {'tested':>12s}")
    for w in ["fully_untouched", "partially_mitigated", "fully_tested"]:
        row = ct.get(w, {})
        u = row.get("fully_untouched", 0)
        p = row.get("partially_mitigated", 0)
        t = row.get("fully_tested", 0)
        print(f"  {w:26s} {u:12d} {p:12d} {t:12d}")

    print("\n=== Age buckets within OB body_freshness=fully_untouched ===")
    untouched = [r for r in valid if r.get("ob_body_freshness") == "fully_untouched"]
    age_buckets = [(0, 5), (5, 10), (10, 20), (20, 50), (50, 10000)]
    for lo, hi in age_buckets:
        rs = [r for r in untouched if lo <= (r.get("ob_age_candles") or 0) < hi]
        if not rs:
            continue
        r5 = [r["forward_return_5bar"] for r in rs]
        mean5 = sum(r5) / len(r5) * 100
        hit5 = sum(1 for x in r5 if x > 0) / len(r5) * 100
        print(f"  age[{lo:3d},{hi:5d}) N={len(rs):4d}  mean5b={mean5:+6.2f}%  hit5b={hit5:5.1f}%")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "reports/freshness_validation/combined/decision_data_with_returns.jsonl")
