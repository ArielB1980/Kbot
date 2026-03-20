# Onboarding Audit: Strategy & Research Infrastructure

**Author**: Head of Research
**Date**: 2026-03-20
**Status**: Initial onboarding audit complete

---

## Executive Summary

Kbot runs a **Smart Money Concepts (SMC) strategy** on crypto perpetual futures (Kraken, USDT-margined). The system is architecturally sound — deterministic signal generation, multi-factor scoring, regime-aware parameters, and a mature research automation pipeline with promotion gates and multi-seed replay validation. However, several high-impact improvement opportunities exist in signal quality, regime filtering, and research infrastructure throughput.

---

## 1. Current Strategy: How It Works

### Signal Generation Pipeline

The strategy uses a **hierarchical timeframe decision model**:

| Timeframe | Role | Key Logic |
|-----------|------|-----------|
| **1D** | Regime filter | EMA200 bias (bullish/bearish/neutral) |
| **4H** | Decision authority | SMC structure detection (Order Blocks, FVGs, Break of Structure) |
| **1H** | Refinement | ADX trend strength filter, swing point precision |
| **15m** | Entry timing | Price proximity to entry zones, reconfirmation |

**Core flow**: Bias determination → Market structure change detection → Confirmation/reconfirmation → ADX filter → SMC structure detection → Fibonacci validation gate → Multi-factor scoring → Score gate → Stop/TP calculation → Deduplication → Signal output.

### Scoring System (0-100)

| Factor | Max Points | What It Measures |
|--------|-----------|-----------------|
| SMC Quality | 25 | OB (+10), FVG (+8), BOS (+7) presence |
| Fib Confluence | 20 | OTE zone (+15), key level proximity (+10) |
| HTF Alignment | 20 | Bias alignment with trade direction |
| ADX Strength | 15 | Trend conviction (40+ = full marks) |
| Cost Efficiency | 20 | Entry cost in bps (lower = better) |

Score gates are regime-aware: tight SMC (OB/FVG) requires 75-80; wide structure (BOS/TREND) requires 70-75.

### Risk Management

- ATR-based stop-loss: 0.15-0.30x ATR for tight SMC, 0.50-0.60x for wide structure
- Adaptive stop widening after consecutive stopouts (1.5x multiplier, up to 2.0x)
- Optional conviction-based entry gate with time-decaying thesis management
- Multi-TP ladder: 40% at 1R, 40% at 2.5R, 20% runner with trailing stop

---

## 2. Strategy Strengths

1. **Deterministic & reproducible** — same candles always produce the same signal (cached indicators, no randomness)
2. **Layered timeframe authority** — clear hierarchy prevents lower-TF noise from overriding 4H structure decisions
3. **Multi-factor scoring** — 5 independent quality dimensions reduce false signal rate
4. **Adaptive confirmation** — ATR-aware confirmation extends during volatility spikes (1→2 candles at high vol)
5. **Regime-aware parameters** — tight SMC vs wide structure have distinct stop/score thresholds
6. **Comprehensive configurability** — all parameters exposed for per-symbol overrides
7. **Cost awareness** — scoring penalizes high-cost entries; funding rate integration for futures
8. **Post-stopout adaptation** — tracks recent stopouts and widens stops automatically
9. **Signal deduplication** — structure-based fingerprinting with 45-min debounce prevents repeated signals
10. **Performance** — cached indicators + numpy vectorization for swing detection

---

## 3. Strategy Weaknesses

### High Impact

1. **No volume confirmation on BOS** — Break-of-structure signals lack volume profile validation. Choppy breakouts with low volume get the same treatment as high-conviction moves. This is likely a significant source of false signals.

2. **Fib validation gate too rigid** — Hard requirement for OTE zone (0.705-0.79) or 20 bps proximity to fib level. Fixed tolerance across all timeframes and regimes. Higher TFs need larger tolerance; illiquid alts need 50+ bps. Currently an all-or-nothing gate that may reject valid setups.

3. **EMA slope calculated but unused in scoring** — Slope (up/down/flat) is computed but only logged. An "up" slope should boost bullish signal scores by +5-10 points. Free alpha being left on the table.

4. **No mean-reversion detection** — System doesn't flag when price is at a previous TP or resistance level from prior signals. Could prevent repeated quick-fail entries at the same level.

### Medium Impact

5. **RSI divergence soft-only** — Detected and logged but doesn't hard-reject counter-trend divergences. Making this a hard gate for counter-trend setups could reduce whipsaw losses.

6. **Thesis conviction decay is linear** — 12-hour fixed window. Should be regime-aware: faster decay in choppy markets, slower in trending. Current linear model doesn't adapt to market conditions.

7. **Higher-TF context is soft gate only** — Weekly zone outside → -18 point penalty. Extremely extended moves (>5% outside zone) should probably be hard-rejected.

8. **Conviction stop widening may be too aggressive for tight SMC** — 1.5x multiplier at conviction 60 may widen stops excessively for OB/FVG setups. Consider regime-aware multiplier (1.2x tight, 1.5x wide).

### Lower Impact

9. **4H decision timeframe is hardcoded** — Legacy code references 1H fallback but production is fixed at 4H. Should formalize multi-decision-TF config.

10. **No gap risk handling** — Futures gaps at US market open/weekends not explicitly managed.

---

## 4. Research Infrastructure Assessment

### Pipeline Strengths

- **Multi-window evaluation** with train/holdout splits (70/30, 40%/60% weighting)
- **Hard promotion gates** — >3 trades, DD<35%, return>-10%, risk-adjusted checks
- **Multi-seed replay validation** — deterministic episode replay with fault injection before promotion
- **Counterfactual twin analysis** — quick secondary validation on historical decision opportunities
- **Campaign-level falsification gate** — auto-stops research after 6 cycles of no improvement
- **Parameter allowlisting** — only strategy params exposed; risk/execution paths locked
- **Human-in-the-loop** for risk tradeoffs via Telegram prompts

### Pipeline Weaknesses

1. **Promotion queue not integrated end-to-end** — `promotion_queue` exists in state but actual promotion to live config is a separate manual trigger. No automated canary/shadow mode before full application.

2. **Replay gate is binary** — all seeds must pass or entire replay fails. No gradation ("passed 1 of 2 seeds") or diagnostic output on why seeds diverged.

3. **Override tuning is greedy** — `research_autolearn.py` adjusts replay parameters based on one cycle's filter stats with no validation that new overrides actually improve the next cycle. Could oscillate.

4. **State file has no locking** — JSON read/write can race if multiple processes touch state. Relies on atomic operations but no explicit locks.

5. **Live universe scope mismatch** — daemon can research 80+ symbols but `LIVE_UNIVERSE_FOR_APPLY` caps at 12. Symbols researched beyond the cap are never applied.

6. **Strategy registry not integrated** — new `registry.py` has lifecycle stages (DRAFT→BACKTESTED→PAPER_TRADING→LIVE→RETIRED) but promotion pipeline bypasses it.

7. **Watchdog cannot distinguish CPU-bound stalls from network I/O** — checks log freshness but can't tell if daemon is legitimately waiting on Kraken API.

---

## 5. Recent Research Results

### What's Been Tried

Based on ablation experiments in `data/research/ablations/`:

- **Entry condition variants** (`entry_probe_20260315_*`) — 5 variants tested against BTC/USD baseline. No improvement found over baseline.
- **Deduplication threshold testing** (`dedupe_probe_20260315`) — tested alternative debounce windows.
- **Score decomposition analysis** (`score_decomp_20260315`) — breakdown of which scoring factors drive acceptance/rejection.

### What Stagnated

Recent continuous daemon runs show convergence patterns where symbol-by-symbol optimization hits stagnation after ~20 iterations. The parameter mutation approach (random walk ±0.25% per param) may be too conservative to escape local optima on BTC/USD, which appears to be the hardest symbol to improve.

### Key Observation

The current scoring formula weights are static: `return×1.0 - DD×0.8 + (Sharpe+Sortino)×0.35 + win_rate×0.1 + trades×0.01`. The heavy return weighting (1.0) with moderate DD penalty (0.8) may favor high-variance candidates that look good in-sample but degrade OOS.

---

## 6. Top 3 Research Directions

### Direction 1: Volume-Confirmed Break of Structure
**Expected improvement**: Sharpe +0.15 to +0.30 (medium confidence)
**Rationale**: BOS signals currently have no volume validation. Adding a volume profile confirmation filter (e.g., break candle volume > 1.5x 20-period average) would eliminate low-conviction breakouts that reverse quickly. This is the most common SMC failure mode in crypto.
**Experiment**: Add volume threshold parameter to BOS detection. Backtest with thresholds [1.0x, 1.5x, 2.0x] across BTC, ETH, SOL. Measure rejection rate vs win-rate improvement.
**Risk**: May reduce trade count significantly on illiquid alts. Needs per-symbol calibration.

### Direction 2: Adaptive Fib Tolerance by Regime and Volatility
**Expected improvement**: +10-15% more valid setups passing the gate (medium-high confidence)
**Rationale**: Fixed 20 bps fib proximity rejects valid setups during high-volatility periods and on higher timeframes. Making tolerance scale with ATR (already partially supported via `entry_zone_tolerance_adaptive`) and extending this to the fib gate itself would capture more opportunities without degrading quality.
**Experiment**: Replace fixed `fib_proximity_bps=20` with `fib_proximity_bps = base_bps × (1 + atr_ratio × scale_factor)`. Test base_bps=[15,20,25], scale_factor=[0.3,0.5,0.8]. Evaluate signal count and quality metrics.
**Risk**: Low — this is parameter tuning within existing infrastructure.

### Direction 3: EMA Slope Integration into Scoring
**Expected improvement**: Sharpe +0.05 to +0.15 (high confidence, low effort)
**Rationale**: EMA200 slope is already calculated but ignored for scoring. Adding +5 to +10 points for aligned slope (bullish signal + rising EMA, or bearish + falling EMA) would improve signal quality with minimal complexity. This is essentially free alpha from data already being computed.
**Experiment**: Add `ema_slope_bonus` parameter (default 7) to signal scorer. Test [0, 5, 7, 10] across all symbols. Compare Sharpe and win-rate before/after.
**Risk**: Very low — additive scoring change, easily reversed.

---

## 7. Recommended Next Experiment

**Start with Direction 3 (EMA Slope Integration)** because:
- Lowest risk, highest confidence of improvement
- Minimal code change (add one scoring component to `signal_scorer.py`)
- Uses data already being computed (zero additional latency)
- Can be validated in a single research cycle
- Success/failure provides signal about scoring system sensitivity

**Protocol**:
1. Add `ema_slope_bonus` to StrategyConfig (default 0, opt-in)
2. Add `_score_ema_slope()` method to SignalScorer
3. Add parameter to research allowlist
4. Run continuous daemon with EMA slope bonus in [5, 7, 10]
5. Evaluate across all 6 default symbols (BTC, ETH, SOL, XRP, ADA, LINK)
6. Compare composite scores: if Sharpe improves ≥0.05 OOS with no DD increase >2%, promote

---

## 8. Infrastructure Gaps Limiting Iteration Speed

1. **No gradual rollout path** — research results go directly to `live_research_overrides.yaml` with no canary/shadow period. Need a shadow-mode evaluation step where new params run alongside production for N cycles before full promotion.

2. **Binary replay gate** — should report per-seed metrics so we can diagnose partial failures instead of full rejection. A candidate that passes 4/5 seeds may be worth investigating.

3. **Stale autolearn loop** — override tuning fires after every cycle without validating improvements. Should track autolearn delta impact over 3-cycle rolling window and revert if no improvement.

4. **Research scoring formula is static** — the composite score weights (return×1.0, DD×0.8, etc.) are hardcoded. These should themselves be subject to meta-optimization or at least be configurable per research campaign.

5. **Strategy registry is disconnected** — the new registry with lifecycle stages is not wired into the promotion pipeline. Connecting it would give us proper audit trails and prevent regressions.

---

## 9. Immediate Action Items

| Priority | Action | Owner |
|----------|--------|-------|
| P0 | Run EMA slope integration experiment (Direction 3) | Head of Research |
| P1 | Design volume-confirmed BOS experiment (Direction 1) | Head of Research |
| P1 | Wire strategy registry into promotion pipeline | Head of Research + CEO approval |
| P2 | Add per-seed replay metrics reporting | Head of Research |
| P2 | Implement shadow-mode evaluation for promotions | Head of Research + CEO approval |

---

*This memo represents the initial onboarding audit. Findings will be refined as experiments produce data.*
