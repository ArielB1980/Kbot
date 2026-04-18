# Phase 2A: Multi-TF OB Detection + Stacking Score

**Author**: Head of Research (draft)
**Date**: 2026-04-18
**Status**: Proposed — awaiting sign-off before implementation
**Predecessor**: Phase 1 (freshness + body/wick) — shipped in PR #18
**Successor**: Phase 2B (refinement cascade) — blocked on 2A validation

---

## 1. Motivation

Phase 1 validated that **OB body_freshness** is a real discriminator (IC +0.118 aggregate; clean monotonic ordering on 4 of 5 symbols). The bot is now scoring 4H OBs by freshness. But the methodology fidelity brief flagged that ICT practitioners stack levels across timeframes: a 4H OB that sits inside a 1D OB is institutionally higher conviction than a 4H OB alone.

Phase 2A extends OB/FVG detection from single-TF (4H) to multi-TF and scores the stacking depth. If the multi-TF thesis is correct, *stacked* 4H signals should show materially higher forward returns than *lone* 4H signals — measurable with the same bucketing approach that validated Phase 1.

## 2. Three Lessons from Phase 1 That Shape This Brief

1. **Symbol-conditional logic from the start.** Build multi-TF scanning, log per-symbol, validate per-symbol. Extend the `freshness_disabled_symbols` pattern to stacking. Expect at least one symbol to misbehave.
2. **Body zones, not wick zones.** All new multi-TF level detection uses `body_low`/`body_high` as the primary zone. Wick ranges are logged but not scored. This avoids another wick-zone artifact debugging cycle.
3. **Log what we don't score yet.** FVG freshness was noise at 4H but could be real at 1D/1W. Log FVG freshness at every new TF scanned — it's free — and re-evaluate after data is in.

## 3. Data Pipeline Status (from recon)

| Timeframe | Storage | Fetch | Backfill | Status |
|---|---|---|---|---|
| 1D | ✅ already fetched | ✅ 365-day lookback | ✅ `backfill_candles.py` | **Free** |
| 1W | ✅ aggregated from 1D in `smc_engine._to_weekly_candles` | ✅ derived | N/A | **Free** |
| 4H | ✅ live | ✅ 180-day lookback | ✅ | Current base |
| 12H | ❌ not in hardcoded CandleManager dict | ❌ not verified on Kraken | ❌ needs new code | Needs engineering |
| 8H | ❌ same | ❌ same | ❌ same | Needs engineering |

**Implication**: Phase 2A has two natural milestones based on what's free vs. what needs engineering.

## 4. Scope

### Milestone 2A-1 (strategy-only; no data pipeline work)

Add OB + FVG detection on **1D and 1W** alongside the existing 4H detection. Stack 4H signals against 1D and 1W levels.

Deliverables:
- `SMCEngine._find_order_block` and `_find_fair_value_gap` called on 1D candles (per-symbol)
- 1W OB/FVG detection (reuse existing `_to_weekly_candles` aggregation)
- Body-zone scanning from day one (no wick-zone artifact risk)
- Level overlap detection: does a 4H OB body zone sit inside a 1D OB body zone?
- Stacking score logged in `structure_info`
- Per-symbol logging of all new grades + counts
- Data captured via the same replay harness used for Phase 1 validation

**This milestone is measurable with zero exchange work.** 1D is already cached; 1W is already derived. The whole milestone lives in `src/strategy/smc_engine.py` + `src/strategy/signal_scorer.py` + the research capture path.

### Milestone 2A-2 (after 2A-1 validates)

Add 12H and 8H to the candle pipeline and re-run stacking validation with the full TF set. Decision gate: only execute 2A-2 if 2A-1 shows stacking produces measurable lift. If 1D/1W stacking is noise, adding 12H/8H is cargo culting.

Deliverables:
- Kraken API verification for 12H and 8H OHLCV availability
- Extend `CandleManager` hardcoded TF dict
- Extend `scripts/backfill_candles.py` `TIMEFRAME_SECONDS`
- Backfill 400 days × 5 symbols × 2 new TFs
- Re-run 2A-1 validation with 12H/8H added to the stacking score

## 5. Stacking Score Formula

The scoring must be measurable, logged in full detail, and initially conservative. Concrete proposal for 2A-1:

**Zone-overlap relation** (per HTF level):
- `contained`: 4H OB body range is fully inside the HTF OB body range. The clean stacking case — Moneytaur's charts show this consistently. **Scored in v1**.
- `overlapping`: 4H OB body range partially overlaps HTF OB body range but is not contained. Ambiguous; could dilute the signal the way wick-zone scanning diluted freshness in Phase 1. **Logged only in v1**; revisit after seeing the distribution.
- `none`: no overlap.

**Per-signal stacking metadata** (logged for every signal):
- `tf_stack_relation` dict: `{tf: "contained" | "overlapping" | "none"}` for each HTF checked
- `tf_stack_depth_contained` (int 0-2 in v1): count of HTFs where 4H is *contained*
- `tf_stack_depth_overlapping` (int 0-2 in v1): count of HTFs where 4H is *overlapping* (non-contained)
- `htf_ob_bias` dict: `{tf: "bullish" | "bearish"}` — the bias of the HTF OB the 4H stacks within
- `tf_stack_bias_conflict` (bool): True if the 4H signal is bullish but stacks inside a bearish HTF OB (or vice versa). Conflicting-bias stacks are logged separately — the ICT methodology treats them as a warning, not confluence. Useful negative info for Phase 2B's thesis formation logic.
- `htf_ob_freshness` dict: `{tf: body_freshness_grade}` for each HTF OB (regardless of contained vs. overlapping)
- `htf_ob_age_candles` dict: `{tf: age}` for each HTF OB
- `htf_fvg_freshness` dict: same for FVGs (logged only, not scored at 4H)

**Initial scored component** (conservative — small points so bad formulas don't wreck the gate):
- `stacking_bonus`: 0 points in `structure_primary` v1, only logged.
- Post-validation: if the data shows stacked signals outperform, award up to +5pts scaled by containment depth × HTF freshness.

**Rationale for deferring scoring**: the stacking formula is a calibration decision, not a design decision. Phase 1 calibrated untouched=1.0, partial=0.85 from data. Phase 2A should do the same — ship the detection and logging, validate the signal, then set weights.

## 6. Validation Plan

Uses the same infrastructure as Phase 1 — `scripts/run_freshness_validation.sh` + `scripts/bucket_freshness_analysis.py` variants.

**Primary question**: does 4H OB stacking depth predict forward returns?

**Analysis dimensions**:
1. Bucket by `tf_stack_depth_contained` (0, 1, 2). Expected: depth 2 > depth 1 > depth 0 on mean 5b and 10b forward return.
2. Separately bucket by `tf_stack_depth_overlapping`. Expected: overlapping shows weaker lift than contained (or none). Decides whether overlapping joins the scored formula in 2A-2.
3. Condition on Phase 1 grade: within `ob_body_freshness=fully_untouched`, does stacking still add signal? Within `fully_tested`, does stacking *rescue* the signal?
4. Per-symbol IC (mandatory — don't repeat the SOL aggregate-only mistake). Log stacking distribution and IC per symbol. Expect symbol-dependent behavior.
5. **HTF freshness × stacking joint distribution (the most important analysis).** Does a 4H signal inside an *untouched* 1D OB massively outperform one inside a *tested* 1D OB? If yes, depth alone is insufficient — the stacking score needs to be freshness-weighted, not just a count. Phase 1 already showed freshness is the real discriminator at 4H; it would be surprising if it didn't matter at 1D/1W. Design logging for this from day one so no second replay is needed.
6. Conflicting-bias stacks: bucket `tf_stack_bias_conflict=True` signals separately. If their forward returns are *worse* than lone 4H signals (depth 0), that's load-bearing negative info for Phase 2B's thesis formation logic. If they behave like lone 4H signals, conflicting bias is a wash — cheaper to ignore than flag.
7. FVG stacking logged but not primarily analyzed. Revisit if 1D FVG freshness behaves differently than 4H FVG freshness did.

**Gate criteria for enabling stacking in the live scorer**:
- Aggregate mean forward return must show monotonic lift with depth (depth 2 > depth 1 > depth 0 by ≥20% on 5b mean)
- At least 3 of 5 symbols must show u>t-style ordering on stacking (analogous to Phase 1's symbol check)
- If the lift is entirely within `fully_untouched` Phase 1 grade, weight stacking only there; otherwise weight across all grades
- Symbols that fail the per-symbol check go into `stacking_disabled_symbols` (new config list, parallel to `freshness_disabled_symbols`)

**What makes this different from Phase 1's validation**: Phase 1 had one discriminator (freshness grade). 2A-1 introduces two (stacking depth × HTF freshness) that can interact. Analysis should check the joint distribution and not average them into a single score prematurely.

## 7. Out of Scope for Phase 2A

- **Refinement cascade (Phase 2B)**: drilling from an HTF thesis zone down to a sub-4H entry. Requires entry/stop recalculation, not just scoring. Blocked on 2A-1 validation.
- **12H/8H OBs**: deferred to 2A-2.
- **New FVG scoring weights**: FVGs are captured but not weighted. Decision deferred until multi-TF FVG data is in.
- **SOL diagnosis**: still a follow-up session. Phase 2A extends the symbol-conditional pattern rather than solving it.
- **Revisiting the `fully_tested` OBs**: Phase 1 showed tested OBs are weak; 2A-1 data may suggest stacking rescues them. Any revival decision is 2A-2 scope at earliest.

## 8. Implementation Sketch

Not a full plan yet — just the rough shape for sign-off:

1. **New methods in `SMCEngine`**: `_find_multi_tf_levels(symbol, signal_timestamp, bias)` returns `{tf: {order_block, fvg}}` for `["4h", "1d", "1w"]`. Reuses existing `_find_order_block` / `_find_fair_value_gap` per TF's candles.
2. **New helper**: `_compute_zone_relation(inner_body, outer_body)` returns `"contained"` / `"overlapping"` / `"none"` on body ranges only. Bias bias-aware: pairs a bullish inner with any outer (conflict is logged, not rejected).
3. **`structure_info` enrichment**: adds the `tf_stack` block as proposed in §5. Logs every dimension (contained/overlapping/none × freshness × age × bias) so the validation analysis has everything it needs without a second replay.
4. **Scorer integration**: `_score_multi_tf_stacking(structures, symbol)` exists but returns 0.0 in v1 (logging pass). Wire into `score_signal` with weight 0.
5. **Capture path update**: `scripts/alpha_combination_analysis.py` logs the new fields (analog to the `ob_body_freshness` addition in Phase 1).
6. **Config additions**: `stacking_scoring_enabled` (default False in v1), `stacking_disabled_symbols` (default empty, populated from validation).

### 8a. Lookahead bias guarantee (critical — must be verified before validation replay)

A 4H OB detected at Tuesday 12:00 UTC must check HTF candles as of the **most recent completed close at or before that timestamp**, never the in-progress HTF candle. Getting this wrong would silently inject future information into the stacking detection and falsify the validation.

Two guarantees Phase 2A implementation must establish:

- **1D lookup**: at signal time T, use 1D candles with `close_time ≤ T`. If T is mid-day, the current day's 1D candle is *in progress* and must be excluded from OB detection. Verify by passing T explicitly into `_find_multi_tf_levels` and slicing `candles_1d` to completed candles only — do not rely on the live `CandleManager` state, which may include the current in-progress bar.
- **1W lookup**: `_to_weekly_candles` aggregates by ISO week from 1D. For a mid-week signal, the current week's synthetic 1W candle is incomplete. Verify the aggregation excludes the partial current week, or explicitly trims it before OB detection runs.

This check is a required step in the implementation plan (probably a unit test plus a targeted replay spot-check) before running the 400-day validation replay. The replay harness already runs off historical candles, so the risk is isolated to the signal-time slicing, not the historical data itself.

**Scope estimate**: 2-3 day implementation session + 1-day validation replay + analysis. Smaller than Phase 1 because the zone-classification helpers already exist.

## 9. Decision Points for Sign-Off

Before writing the implementation plan:
1. **1D and 1W only in 2A-1, or push through to 12H/8H in one pass?** Recommended: 1D/1W only. Let the data decide whether 12H/8H is worth the exchange work.
2. **Stacking logged-only in v1 vs. scored immediately?** Recommended: logged-only. Calibrate weights from validation data, like Phase 1 did.
3. **Keep FVG out of the stacking score?** Recommended: yes, same as Phase 1. Log only.
4. **How many days of replay data for validation?** 400 days for consistency with Phase 1 — want direct comparability to the N=626 baseline.
