# Phase 2A Implementation Plan — Multi-TF OB Stacking (logging pass)

**Companion to**: `phase2a-multi-tf-stacking.md`
**Branch**: `ArielB1980/phase2a-multi-tf-stacking` (off v3 @ c7ab549, includes PR #18)
**Goal**: Detect 1D and 1W OBs/FVGs, classify 4H→1D and 4H→1W zone relations, log everything, score 0 in v1. Validate on 400-day replay; calibrate weights from data before enabling.

---

## Files Touched

| File | Change |
|---|---|
| `src/strategy/smc_engine.py` | `_compute_zone_relation` helper, multi-TF OB/FVG detection in `_detect_structure`, `tf_stack` dict in returned structure, lookahead-safe slicing, `tf_stack_*` keys in all 3 `score_breakdown` sites |
| `src/strategy/signal_scorer.py` | `stacking` / `stacking_max` keys in `SCORER_WEIGHTS`, `tf_stack_score` field on `SignalScore`, `_score_tf_stacking` method (returns 0 in v1), wiring in `score_signal` |
| `src/config/config.py` | `stacking_scoring_enabled`, `stacking_disabled_symbols`, `stacking_max_points` |
| `scripts/alpha_combination_analysis.py` | Extract `tf_stack` fields in `_patched_generate`, flatten into decision rows |
| `tests/unit/test_smc_engine_multi_tf.py` (new) | Lookahead-safety unit test |
| `scripts/bucket_stacking_analysis.py` (new) | Validation bucketing for stacking |

---

## Step 0 — Lookahead safety (prerequisite, write before anything else)

**Why first**: if the 1D/1W slicing is wrong, validation replay results are worthless.

**File**: new `tests/unit/test_smc_engine_multi_tf.py`

**Test 1**: given a synthetic candle set where the last 1D candle is mid-day (incomplete), the OB detection pipeline must not use that in-progress candle. Build 10 days of 1D candles where day 10's close_time is one hour *after* the signal timestamp; assert that `_detect_multi_tf_levels` at the signal timestamp sees only 9 completed days.

**Test 2**: given daily candles spanning Tuesday–Tuesday (partial current week), `_to_weekly_candles` produces only 1 full-week bar, not a 2-day partial-week bar. Current implementation at `smc_engine.py:309` includes partial weeks — this test will fail until we add the trim (Step 2c below).

**Gate**: these tests must pass before running any 400-day replay.

---

## Step 1 — Zone relation helper

**File**: `src/strategy/smc_engine.py` (near the other `_classify_*` helpers around line 1700)

```python
@staticmethod
def _compute_zone_relation(
    inner_low: float, inner_high: float,
    outer_low: float, outer_high: float,
) -> str:
    """Classify how an inner body range relates to an outer body range.

    Returns one of:
      - "contained"   : inner is fully inside outer (inner_low >= outer_low and inner_high <= outer_high)
      - "overlapping" : any overlap that isn't containment
      - "none"        : no overlap (inner_high < outer_low or inner_low > outer_high)

    Body-range only — do NOT pass wick values. Direction/bias is NOT checked
    here (callers check separately so bias conflict can be logged not rejected).
    """
```

Small, pure, unit-testable. Add a focused unit test with 4 cases (contained, overlap-from-below, overlap-from-above, no-overlap).

---

## Step 2 — Multi-TF level detection with lookahead-safe slicing

**File**: `src/strategy/smc_engine.py`

### 2a. New method `_detect_multi_tf_levels`

Insert near `_detect_structure` (~line 1632):

```python
def _detect_multi_tf_levels(
    self,
    symbol: str,
    signal_timestamp: datetime,
    bias: str,
) -> Dict[str, Dict[str, Optional[Dict]]]:
    """Run OB and FVG detection on HTFs. Returns {tf: {"order_block": ..., "fvg": ...}}.

    Pulls 1D candles from self._higher_tf_candle_context[symbol]["1d"] (already
    cached at generate_signal:485). 1W is re-derived here with the partial-week
    trim to avoid lookahead. Only TFs strictly higher than the decision TF (4h)
    are included — this method is not called when decision_tf >= 1d.

    Returns empty nested dicts if HTF data is missing or too short.
    """
```

Reuses existing `_find_order_block` and `_find_fair_value_gap`, both of which already carry freshness/body_freshness and are safe to call on any candle series. Pass the sliced HTF candles in — same function signature.

### 2b. Lookahead-safe HTF candle slicing

Before calling `_find_order_block` on HTF candles:

```python
def _slice_completed(candles: List[Candle], cutoff: datetime) -> List[Candle]:
    """Return candles whose close_time is <= cutoff. Excludes in-progress bar."""
    return [c for c in candles if c.timestamp + c.duration <= cutoff]
```

`c.duration` is not currently a field on `Candle`. Two options:
1. **Preferred**: derive from TF string (`"1d"` → `timedelta(days=1)`, `"1w"` → `timedelta(weeks=1)`). Keep Candle dataclass unchanged.
2. **Alternative**: if `c.timestamp` already represents close-time (not open-time), the check is just `c.timestamp <= cutoff`. **Verify this before implementing** — check how `Candle.timestamp` is populated in `src/data/`.

### 2c. 1W partial-week exclusion

Modify `_to_weekly_candles` at `smc_engine.py:309` (or add a variant `_to_weekly_candles_completed`) to drop the current ISO week if its day count is < 7. This is the partial-week trim that Test 2 requires.

Do NOT change the existing `self._higher_tf_candle_context[symbol]["1w"]` caching at line 485–489 — that's used for weekly_confluence_bonus and may want partial weeks. Phase 2A uses a separate, trimmed view.

### 2d. Wire into `_detect_structure`

After the existing 4H OB/FVG detection returns, call `_detect_multi_tf_levels` with the signal timestamp and attach the result as:

```python
return {
    "order_block": ob,
    "fvg": fvg,
    "bos": bos,
    "tf_stack": {...},  # see Step 3
}
```

---

## Step 3 — Stacking metadata computation

**File**: `src/strategy/smc_engine.py`, new method `_compute_tf_stack`

Input: the 4H OB (already containing `body_low`, `body_high`, `type`), the result of `_detect_multi_tf_levels`.

Output dict (exactly the fields specified in the brief §5):

```python
{
    "tf_stack_relation": {"1d": "contained"|"overlapping"|"none", "1w": ...},
    "tf_stack_depth_contained": 0|1|2,
    "tf_stack_depth_overlapping": 0|1|2,
    "htf_ob_bias": {"1d": "bullish"|"bearish"|None, "1w": ...},
    "tf_stack_bias_conflict": bool,
    "htf_ob_freshness": {"1d": "fully_untouched"|..., "1w": ...},
    "htf_ob_body_freshness": {"1d": ..., "1w": ...},  # Moneytaur institutional
    "htf_ob_age_candles": {"1d": int, "1w": int},
    "htf_fvg_freshness": {"1d": ..., "1w": ...},     # logged only
    "htf_fvg_age_candles": {"1d": int, "1w": int},    # logged only
}
```

Null-safe: if HTF has no OB, all its sub-fields are `None`. `depth_contained` counts TFs where relation is "contained" *and* bias matches. Bias-conflicted stacks count in `depth_overlapping` *only if* their body relation is overlapping — otherwise they go to `"none"` to avoid inflating overlap counts with bias-mismatched levels.

Handle the empty 4H-OB case explicitly: if there's no 4H OB, return zeros/Nones throughout so downstream `score_breakdown.get` calls never KeyError.

---

## Step 4 — `structure_info` enrichment

The `tf_stack` dict attaches to the `_detect_structure` return (Step 2d). `structure_info` in the Signal is populated from `structure_signal` (at `smc_engine.py:1362`). No Signal dataclass change needed — `structure_info: dict` accepts new keys.

Verify no downstream code does `structure_info = {"order_block": ..., "fvg": ..., "bos": ...}` with exhaustive key listing. Grep for `structure_info` consumers.

---

## Step 5 — Scorer integration (zero-weight pass-through)

**File**: `src/strategy/signal_scorer.py`

### 5a. `SCORER_WEIGHTS` additions

Add to both `phase_ad` and `structure_primary` profiles (lines 18–37):

```python
"stacking": 0.0,         # 0 in v1 — calibrate from replay before enabling
"stacking_max": 10.0,
```

### 5b. `SignalScore` field

```python
tf_stack_score: float = 0.0
```

### 5c. `_score_tf_stacking` method

Mirror `_score_level_freshness` pattern (disabled-symbols list, symbol param, returns float):

```python
def _score_tf_stacking(
    self,
    structures: Dict,
    max_points: float = 10.0,
    symbol: Optional[str] = None,
) -> float:
    """Phase 2A v1: always returns 0.0 regardless of structure content.

    Kept as a real method (not a constant) so weight calibration post-
    validation is a one-place edit, and per-symbol kill-switch wires in
    the same way freshness did.
    """
    if not getattr(self.config, "stacking_scoring_enabled", False):
        return 0.0
    disabled = set(getattr(self.config, "stacking_disabled_symbols", []) or [])
    if symbol and symbol in disabled:
        return 0.0
    # v1 scaffold only — calibration lives here after validation.
    return 0.0
```

### 5d. Wire into `score_signal`

Add after the freshness line (currently line ~165):

```python
tf_stack_score = self._score_tf_stacking(
    structures, max_points=w.get("stacking_max", 10.0), symbol=signal.symbol,
)
```

Add `w["stacking"] * tf_stack_score` to the `total = ...` sum.

Pass `tf_stack_score=tf_stack_score` to the `SignalScore` constructor.

---

## Step 6 — `score_breakdown` dict additions (3 sites)

**File**: `src/strategy/smc_engine.py`

At lines ~1206, ~1254, ~1345 (the three `score_breakdown` construction sites identified in recon), add these keys:

```python
"tf_stack_depth_contained": float(stack.get("tf_stack_depth_contained", 0)),
"tf_stack_depth_overlapping": float(stack.get("tf_stack_depth_overlapping", 0)),
"tf_stack_bias_conflict": float(stack.get("tf_stack_bias_conflict", 0)),
"tf_stack_score": float(score_obj.tf_stack_score),
```

Where `stack = structure_signal.get("tf_stack", {}) or {}`.

The string-valued fields (freshness grades, bias) cannot go in `score_breakdown` (typed `Dict[str, float]`). They live in `structure_info.tf_stack` and are extracted by the research capture path directly from there.

---

## Step 7 — Research capture path

**File**: `scripts/alpha_combination_analysis.py`

### 7a. Extract `tf_stack` in `_patched_generate` (around line 988)

After the existing freshness_info extraction:

```python
stack = si.get("tf_stack") or {}
tf_stack_info = {
    "tf_stack_depth_contained": stack.get("tf_stack_depth_contained"),
    "tf_stack_depth_overlapping": stack.get("tf_stack_depth_overlapping"),
    "tf_stack_bias_conflict": stack.get("tf_stack_bias_conflict"),
    "1d_ob_relation": (stack.get("tf_stack_relation") or {}).get("1d"),
    "1w_ob_relation": (stack.get("tf_stack_relation") or {}).get("1w"),
    "1d_ob_body_freshness": (stack.get("htf_ob_body_freshness") or {}).get("1d"),
    "1w_ob_body_freshness": (stack.get("htf_ob_body_freshness") or {}).get("1w"),
    "1d_ob_bias": (stack.get("htf_ob_bias") or {}).get("1d"),
    "1w_ob_bias": (stack.get("htf_ob_bias") or {}).get("1w"),
    "1d_ob_age_candles": (stack.get("htf_ob_age_candles") or {}).get("1d"),
    "1w_ob_age_candles": (stack.get("htf_ob_age_candles") or {}).get("1w"),
    "1d_fvg_freshness": (stack.get("htf_fvg_freshness") or {}).get("1d"),
    "1w_fvg_freshness": (stack.get("htf_fvg_freshness") or {}).get("1w"),
}
```

### 7b. Merge into row

Merge `tf_stack_info` into the row dict alongside `freshness_info`.

### 7c. Add to flatten loop

Extend the key list in the enrichment loop (around line 1106) so all `tf_stack` keys survive the JSONL roundtrip.

---

## Step 8 — Config additions

**File**: `src/config/config.py`, StrategyConfig, after `freshness_disabled_symbols`

```python
stacking_scoring_enabled: bool = Field(
    default=False,
    description="Enable multi-TF OB stacking bonus (Phase 2A). "
                "v1 ships with this False — calibrate weights from replay first.",
)
stacking_max_points: float = Field(
    default=10.0, ge=0.0, le=20.0,
    description="Maximum points awarded for multi-TF OB stacking.",
)
stacking_disabled_symbols: List[str] = Field(
    default_factory=list,
    description="Symbols where stacking scoring is forced to 0. "
                "Populate from per-symbol IC check after 2A-1 validation "
                "(analog of freshness_disabled_symbols).",
)
```

No `config.yaml` changes — defaults are production-safe (weight 0, enabled False).

---

## Step 9 — Verification before replay

Run in order:

1. `uv run ruff check` on all touched files.
2. `uv run ty check` on all touched files.
3. `uv run pytest tests/unit/test_smc_engine_multi_tf.py` — Step 0 tests must pass.
4. `uv run pytest tests/unit/test_signal_scorer.py` — existing scorer tests must pass (zero-weight pass-through is a no-op).
5. Quick smoke: run the existing replay for 30 days on 1 symbol, confirm `tf_stack_*` columns appear in decision_data.jsonl and have the expected distribution (most rows should have `tf_stack_depth_contained` in {0,1}; some 2s).
6. Confirm `total_score` is unchanged vs. pre-2A for a sample of signals (stacking weight is 0 so totals must not drift).

Only after all six pass: run full 400-day replay.

---

## Step 10 — 400-day validation replay

Reuse `scripts/run_freshness_validation.sh` unchanged except for the output dir (e.g. `reports/phase2a_validation/`). Same 5 symbols, 400 days, `REPLAY_OVERRIDE_FVG_MITIGATION_MODE=full`. Expected ~22 min wall-clock on 4 vCPU.

Output: combined `decision_data_with_returns.jsonl` with the new `tf_stack_*` columns.

---

## Step 11 — Analysis (`scripts/bucket_stacking_analysis.py`)

New script, modeled on `scripts/bucket_freshness_analysis.py` + `scripts/per_symbol_freshness_ic.py`. Produces:

1. Table: forward returns bucketed by `tf_stack_depth_contained` ∈ {0,1,2}. Aggregate + per-symbol.
2. Table: same, for `tf_stack_depth_overlapping` — should show weaker/no lift vs. contained.
3. Joint table: body_freshness × tf_stack_depth_contained — the "most important analysis" flagged in the brief. 3×3 grid of mean 5b return.
4. Bias-conflict bucket: forward returns for `tf_stack_bias_conflict=True` vs. lone 4H signals (depth 0, no conflict).
5. HTF freshness breakdown: within `tf_stack_depth_contained ≥ 1`, bucket by `1d_ob_body_freshness` — does stacking inside an *untouched* 1D OB outperform stacking inside a *tested* 1D OB?
6. Per-symbol IC on stacking depth (mandatory — parallels the SOL-spotting check from Phase 1).

---

## Step 12 — Calibration commit (post-analysis)

Based on the analysis, a single follow-up commit sets:

- `stacking_scoring_enabled: True` (if gate criteria from brief §6 are met)
- Score weights and formula inside `_score_tf_stacking`
- `stacking_disabled_symbols` populated from per-symbol failures
- `SCORER_WEIGHTS["structure_primary"]["stacking"] = 1.0`
- Updated tests for the calibrated scoring

If gate criteria fail, that commit is instead "document that stacking didn't replicate; capture findings; close 2A-1 without enabling."

---

## Out of scope for this plan

- 12H/8H timeframes (Phase 2A-2, gated on 2A-1 validation).
- Refinement cascade (Phase 2B, gated on 2A-1 validation).
- Any change to Phase 1 scoring (freshness stays untouched).
- SOL diagnosis (separate session).
- FVG scoring on any TF (logging only throughout).

---

## Risk register

| Risk | Mitigation |
|---|---|
| Lookahead bias in 1D/1W slicing | Step 0 unit tests + Step 9 smoke check (totals unchanged under zero-weight) |
| New fields break downstream `structure_info` consumers | Grep for exhaustive-key consumers before Step 4 |
| `tf_stack_*` fields not serialized through JSONL roundtrip | Step 9 smoke check |
| Stacking distribution is degenerate (e.g. 99% depth=0) | Step 9 smoke check — if distribution is degenerate, rethink detection before running 400-day replay |
| Replay takes longer than Phase 1 due to extra OB calls on 1D/1W | Acceptable — HTF candle counts are ~24× smaller than 4H counts; per-signal overhead is tiny |

---

## Commit sequence

1. `docs(phase2a): implementation plan` — this file
2. `test(smc): lookahead safety for multi-TF level detection` — Step 0
3. `feat(smc): zone-relation helper + multi-TF OB detection scaffolding` — Steps 1–2
4. `feat(smc): stacking metadata + structure_info enrichment` — Steps 3–4
5. `feat(scoring): stacking scaffold (weight 0 in v1)` — Steps 5–6
6. `feat(research): capture tf_stack fields` — Step 7
7. `feat(config): stacking config fields (disabled by default)` — Step 8
8. `feat(research): bucket_stacking_analysis script` — Step 11 (can land before replay)
9. *(after replay + analysis)* `feat(scoring): calibrate stacking weights from 400d validation` — Step 12

Each commit independently reviewable. First 8 land the scaffold; commit 9 is the decision.
