"""Alpha Combination Engine — Fundamental Law of Active Management analysis.

Implements the 11-step alpha combination procedure:
1.  Raw signal return series
2.  Remove systematic drift (expanding-window mean)
3.  Variance normalization (expanding-window std)
4.  Cross-sectional mean at each bar
5.  Cross-sectional demeaning
6.  Re-normalize post-demeaning
7.  Correlation matrix + heatmap
8.  Signal clustering (hierarchical)
9.  Independence extraction (regression residuals)
10. Optimal weighting (by independent IR)
11. Combined alpha construction

Usage:
    # Analysis only (requires existing JSONL):
    uv run scripts/alpha_combination_analysis.py \\
        --input data.jsonl --output-dir reports/alpha_combination/

    # Generate data from backtest then analyze:
    uv run scripts/alpha_combination_analysis.py \\
        --generate-data --symbols ETH/USD,BTC/USD,SOL/USD,LINK/USD \\
        --lookback-days 365 --output-dir reports/alpha_combination/
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import pickle
import sys
import warnings
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import dendrogram, fcluster, linkage
from scipy.spatial.distance import squareform
from sklearn.linear_model import LinearRegression

warnings.filterwarnings("ignore", category=FutureWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("alpha_combination")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RANDOM_SEED = 42

# Score breakdown features from smc_engine.py score_breakdown dicts
SCORE_FEATURES = [
    "smc",
    "fib",
    "htf",
    "cost",
    "volume",
    "structure",
    "rsi_div",
    "fib_1h",
    "adx_grad",
    "freshness",
    "tf_stack_depth_contained",
    "tf_stack_depth_overlapping",
    "tf_stack_bias_conflict",
    "tf_stack_score",
    "higher_tf_bonus",
    "higher_tf_penalty",
]

# Minimum observations for a signal to be included in regression
MIN_SIGNAL_OBS = 30
# Minimum expanding window before making predictions
DEFAULT_MIN_WINDOW = 200
# Correlation threshold for flagging pairs
CORR_FLAG_THRESHOLD = 0.6
# Cluster distance threshold (within-cluster avg correlation > 0.5)
CLUSTER_DISTANCE_THRESHOLD = 0.5


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class AlphaModel:
    """Serializable model for runtime scoring."""

    weights: dict[str, float]
    feature_means: dict[str, float]
    feature_stds: dict[str, float]
    cluster_assignments: dict[str, int]
    sparse_features: list[str]
    fitted_at: str = ""


# ---------------------------------------------------------------------------
# Step 0: Load and validate data
# ---------------------------------------------------------------------------


def load_data(path: Path, target_col: str = "forward_return_5bar") -> pd.DataFrame:
    """Load JSONL and flatten into a feature matrix."""
    log.info("Loading data from %s", path)
    rows: list[dict[str, Any]] = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))

    if not rows:
        raise ValueError(f"No rows loaded from {path}")

    df = pd.DataFrame(rows)
    log.info("Loaded %d rows with columns: %s", len(df), list(df.columns))

    # Flatten nested score_breakdown if present
    if "score_breakdown" in df.columns and isinstance(df["score_breakdown"].iloc[0], dict):
        breakdown_df = pd.json_normalize(df["score_breakdown"])
        for col in breakdown_df.columns:
            if col not in df.columns:
                df[col] = breakdown_df[col]

    # Validate required columns
    required = ["symbol", target_col]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Validate feature columns
    available_features = [f for f in SCORE_FEATURES if f in df.columns]
    missing_features = [f for f in SCORE_FEATURES if f not in df.columns]
    if missing_features:
        log.warning("Missing feature columns (will use 0.0): %s", missing_features)
        for f in missing_features:
            df[f] = 0.0

    # Ensure numeric types
    for f in SCORE_FEATURES:
        df[f] = pd.to_numeric(df[f], errors="coerce").fillna(0.0)
    df[target_col] = pd.to_numeric(df[target_col], errors="coerce")

    # Drop rows with NaN target
    n_before = len(df)
    df = df.dropna(subset=[target_col]).reset_index(drop=True)
    if len(df) < n_before:
        log.warning("Dropped %d rows with NaN target", n_before - len(df))

    # Sort by timestamp if available
    if "timestamp" in df.columns or "bar_timestamp" in df.columns:
        ts_col = "timestamp" if "timestamp" in df.columns else "bar_timestamp"
        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
        df = df.sort_values(ts_col).reset_index(drop=True)

    log.info(
        "Feature matrix: %d rows, %d features, %d symbols",
        len(df),
        len(available_features),
        df["symbol"].nunique(),
    )
    return df


# ---------------------------------------------------------------------------
# Steps 1-6: Normalization pipeline
# ---------------------------------------------------------------------------


def step_01_raw_signal_returns(
    df: pd.DataFrame, features: list[str], target_col: str
) -> pd.DataFrame:
    """Multiply each feature by forward return to get signal return series."""
    log.info("Step 1: Computing raw signal return series")
    result = pd.DataFrame(index=df.index)
    for f in features:
        result[f] = df[f] * df[target_col]
    return result


def step_02_remove_drift(signal_returns: pd.DataFrame, min_periods: int = 50) -> pd.DataFrame:
    """Remove systematic drift via expanding-window mean subtraction."""
    log.info("Step 2: Removing systematic drift (min_periods=%d)", min_periods)
    expanding_mean = signal_returns.expanding(min_periods=min_periods).mean()
    return signal_returns - expanding_mean


def step_03_variance_normalize(
    drift_adj: pd.DataFrame, min_periods: int = 50
) -> tuple[pd.DataFrame, list[str]]:
    """Normalize by expanding-window std. Returns (normalized, excluded_cols)."""
    log.info("Step 3: Variance normalization")
    expanding_std = drift_adj.expanding(min_periods=min_periods).std()
    excluded: list[str] = []

    normalized = drift_adj.copy()
    for col in drift_adj.columns:
        col_std = expanding_std[col]
        constant_mask = col_std < 1e-8
        if constant_mask.all():
            excluded.append(col)
            log.warning("Feature '%s' has zero variance — excluding", col)
            normalized[col] = 0.0
        else:
            normalized[col] = drift_adj[col] / col_std.replace(0, np.nan)
            normalized[col] = normalized[col].clip(-3, 3).fillna(0.0)

    return normalized, excluded


def step_04_cross_sectional_mean(
    normalized: pd.DataFrame, symbols: pd.Series
) -> pd.Series:
    """Compute cross-sectional mean at each bar (market factor)."""
    log.info("Step 4: Computing cross-sectional mean")
    # Group by the row's temporal index and compute mean across symbols
    # Since rows are ordered by time and may have multiple symbols per timestamp,
    # we compute the mean of normalized features across all features per row
    return normalized.mean(axis=1)


def step_05_cross_sectional_demean(
    normalized: pd.DataFrame,
    df: pd.DataFrame,
    features: list[str],
    target_col: str,
) -> pd.DataFrame:
    """Subtract cross-sectional mean per timestamp to isolate symbol-specific signal."""
    log.info("Step 5: Cross-sectional demeaning")
    ts_col = "timestamp" if "timestamp" in df.columns else "bar_timestamp"
    if ts_col not in df.columns:
        log.warning("No timestamp column; skipping cross-sectional demeaning")
        return normalized

    demeaned = normalized.copy()
    # Group by timestamp, compute mean, subtract
    for col in features:
        # Use the signal return version
        if col in demeaned.columns:
            signal_group_mean = demeaned.groupby(df[ts_col])[col].transform("mean")
            demeaned[col] = demeaned[col] - signal_group_mean

    return demeaned


def step_06_renormalize(
    demeaned: pd.DataFrame, min_periods: int = 50
) -> tuple[pd.DataFrame, list[str]]:
    """Re-apply variance normalization post-demeaning."""
    log.info("Step 6: Re-normalizing post-demeaning")
    return step_03_variance_normalize(demeaned, min_periods)


# ---------------------------------------------------------------------------
# Steps 7-8: Correlation and clustering
# ---------------------------------------------------------------------------


def step_07_correlation_matrix(
    normalized: pd.DataFrame,
    features: list[str],
    output_dir: Path,
) -> pd.DataFrame:
    """Compute pairwise correlation and save heatmap."""
    log.info("Step 7: Computing correlation matrix")
    active = [f for f in features if f in normalized.columns]
    corr = normalized[active].corr()

    # Flag highly correlated pairs
    for i, fi in enumerate(active):
        for j, fj in enumerate(active):
            if i < j and abs(corr.loc[fi, fj]) > CORR_FLAG_THRESHOLD:
                log.info(
                    "  High correlation: %s <-> %s = %.3f",
                    fi, fj, corr.loc[fi, fj],
                )

    # Save CSV
    corr.to_csv(output_dir / "correlation_matrix.csv")

    # Save heatmap
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import seaborn as sns

        fig, ax = plt.subplots(figsize=(10, 8))
        sns.heatmap(
            corr,
            annot=True,
            fmt=".2f",
            cmap="RdBu_r",
            center=0,
            vmin=-1,
            vmax=1,
            ax=ax,
        )
        ax.set_title("Signal Return Correlation Matrix")
        fig.tight_layout()
        fig.savefig(output_dir / "correlation_heatmap.png", dpi=150)
        plt.close(fig)
        log.info("  Saved correlation_heatmap.png")
    except Exception as e:
        log.warning("Could not generate heatmap: %s", e)

    return corr


def step_08_signal_clustering(
    corr: pd.DataFrame,
    output_dir: Path,
) -> dict[str, int]:
    """Hierarchical clustering on correlation matrix."""
    log.info("Step 8: Signal clustering")
    features = list(corr.columns)
    n = len(features)
    if n < 2:
        return {f: 0 for f in features}

    # Convert correlation to distance: d = 1 - |corr|
    dist_matrix = 1 - corr.abs().values
    np.fill_diagonal(dist_matrix, 0)

    # Replace NaN/Inf with max distance (1.0 = uncorrelated)
    dist_matrix = np.nan_to_num(dist_matrix, nan=1.0, posinf=1.0, neginf=1.0)

    # Ensure symmetry and valid condensed form
    dist_matrix = (dist_matrix + dist_matrix.T) / 2
    # Clip to valid range [0, 2]
    dist_matrix = np.clip(dist_matrix, 0, 2)
    condensed = squareform(dist_matrix, checks=False)

    linkage_matrix = linkage(condensed, method="average")
    clusters = fcluster(linkage_matrix, t=CLUSTER_DISTANCE_THRESHOLD, criterion="distance")
    assignments = {f: int(c) for f, c in zip(features, clusters)}

    log.info("  Cluster assignments: %s", assignments)

    # Save dendrogram
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(10, 6))
        dendrogram(
            linkage_matrix,
            labels=features,
            ax=ax,
            leaf_rotation=45,
            leaf_font_size=9,
        )
        ax.set_title("Signal Clustering Dendrogram")
        ax.set_ylabel("Distance (1 - |correlation|)")
        fig.tight_layout()
        fig.savefig(output_dir / "cluster_dendrogram.png", dpi=150)
        plt.close(fig)
        log.info("  Saved cluster_dendrogram.png")
    except Exception as e:
        log.warning("Could not generate dendrogram: %s", e)

    # Save assignments
    with open(output_dir / "cluster_assignments.json", "w") as f:
        json.dump(assignments, f, indent=2)

    return assignments


# ---------------------------------------------------------------------------
# Steps 9-10: Independence extraction and weighting
# ---------------------------------------------------------------------------


def step_09_independence_extraction(
    normalized: pd.DataFrame,
    df: pd.DataFrame,
    features: list[str],
    target_col: str,
) -> pd.DataFrame:
    """Regress each signal against all others to extract independent contribution."""
    log.info("Step 9: Independence extraction")
    target = df[target_col].values

    results: list[dict[str, Any]] = []

    for f in features:
        f_values = normalized[f].values
        valid_mask = ~np.isnan(f_values) & ~np.isnan(target)
        n_active = int(np.sum(np.abs(f_values[valid_mask]) > 1e-10))

        if n_active < MIN_SIGNAL_OBS:
            log.warning(
                "  Feature '%s' has only %d active obs (< %d) — zero IC assigned",
                f, n_active, MIN_SIGNAL_OBS,
            )
            results.append({
                "feature": f,
                "raw_ic": 0.0,
                "independent_ic": 0.0,
                "independent_ir": 0.0,
                "n_active": n_active,
                "sparse": True,
            })
            continue

        # Raw IC: correlation of feature signal returns with target
        raw_ic = float(np.corrcoef(f_values[valid_mask], target[valid_mask])[0, 1])
        if np.isnan(raw_ic):
            raw_ic = 0.0

        # Independent IC: regress f against all other features, take residual
        other_features = [o for o in features if o != f]
        x_all = normalized[other_features].values[valid_mask]
        y = f_values[valid_mask]

        # Handle case where x_all has zero variance columns
        valid_x_cols = np.std(x_all, axis=0) > 1e-10
        if valid_x_cols.sum() == 0:
            residual = y
        else:
            x_clean = x_all[:, valid_x_cols]
            reg = LinearRegression(fit_intercept=True)
            reg.fit(x_clean, y)
            predicted = reg.predict(x_clean)
            residual = y - predicted

        # Independent IC
        independent_ic = float(np.corrcoef(residual, target[valid_mask])[0, 1])
        if np.isnan(independent_ic):
            independent_ic = 0.0

        # Independent IR
        residual_std = float(np.std(residual))
        independent_ir = (
            float(np.mean(residual)) / residual_std if residual_std > 1e-10 else 0.0
        )

        results.append({
            "feature": f,
            "raw_ic": round(raw_ic, 6),
            "independent_ic": round(independent_ic, 6),
            "independent_ir": round(independent_ir, 6),
            "n_active": n_active,
            "sparse": False,
        })
        log.info(
            "  %s: raw_IC=%.4f, indep_IC=%.4f, indep_IR=%.4f (n=%d)",
            f, raw_ic, independent_ic, independent_ir, n_active,
        )

    return pd.DataFrame(results)


def step_10_optimal_weighting(
    ic_table: pd.DataFrame,
    cluster_assignments: dict[str, int],
) -> dict[str, float]:
    """Weight each signal by its independent information ratio."""
    log.info("Step 10: Optimal weighting")

    weights: dict[str, float] = {}
    total_abs_ir = 0.0

    for _, row in ic_table.iterrows():
        feat = row["feature"]
        if row["sparse"]:
            weights[feat] = 0.0
        else:
            weights[feat] = row["independent_ir"]
            total_abs_ir += abs(row["independent_ir"])

    # Normalize
    if total_abs_ir > 1e-10:
        weights = {k: v / total_abs_ir for k, v in weights.items()}
    else:
        log.warning("Total absolute IR is near zero — using equal weights")
        non_sparse = [f for f, w in weights.items() if not np.isclose(w, 0.0) or not ic_table[ic_table["feature"] == f]["sparse"].values[0]]
        if non_sparse:
            equal_w = 1.0 / len(non_sparse)
            weights = {f: equal_w if f in non_sparse else 0.0 for f in weights}

    log.info("  Optimal weights: %s", {k: round(v, 4) for k, v in weights.items()})
    return weights


# ---------------------------------------------------------------------------
# Step 11: Combined alpha
# ---------------------------------------------------------------------------


def step_11_combined_alpha(
    normalized: pd.DataFrame,
    weights: dict[str, float],
    features: list[str],
) -> pd.Series:
    """Compute combined alpha as weighted sum of normalized signal values."""
    log.info("Step 11: Constructing combined alpha")
    alpha = pd.Series(0.0, index=normalized.index)
    for f in features:
        if f in weights and f in normalized.columns:
            alpha += weights[f] * normalized[f]
    return alpha


# ---------------------------------------------------------------------------
# Expanding-window backtest (no lookahead)
# ---------------------------------------------------------------------------


def run_expanding_window_backtest(
    df: pd.DataFrame,
    features: list[str],
    target_col: str,
    min_window: int,
    thresholds: list[float],
    output_dir: Path,
) -> pd.DataFrame:
    """Run expanding-window backtest with no lookahead.

    At each bar t, fit steps 1-10 on data [0:t-1], compute combined alpha at t.
    """
    log.info(
        "Running expanding-window backtest (min_window=%d, thresholds=%s)",
        min_window,
        thresholds,
    )
    n = len(df)
    if n < min_window + 1:
        log.warning(
            "Not enough data for expanding window (%d rows, need %d)",
            n,
            min_window + 1,
        )
        return pd.DataFrame()

    decisions: list[dict[str, Any]] = []

    # Refit every N bars for efficiency
    refit_interval = max(50, min_window // 4)
    current_weights: dict[str, float] | None = None
    current_means: dict[str, float] = {}
    current_stds: dict[str, float] = {}
    last_fit_idx = -1

    for t in range(min_window, n):
        # Refit periodically
        if current_weights is None or (t - last_fit_idx) >= refit_interval:
            train = df.iloc[:t]
            train_features = train[features].copy()
            train_target = train[target_col].values

            # Compute training stats
            current_means = {f: float(train_features[f].mean()) for f in features}
            current_stds = {
                f: max(float(train_features[f].std()), 1e-10) for f in features
            }

            # Normalize training features
            train_norm = pd.DataFrame(index=train.index)
            for f in features:
                train_norm[f] = (train_features[f] - current_means[f]) / current_stds[f]
                train_norm[f] = train_norm[f].clip(-3, 3).fillna(0.0)

            # Compute signal returns on train
            signal_returns = pd.DataFrame(index=train.index)
            for f in features:
                signal_returns[f] = train_norm[f] * train_target

            # Compute ICs directly (simplified for efficiency)
            ics: dict[str, float] = {}
            for f in features:
                sr = signal_returns[f].values
                valid = ~np.isnan(sr) & ~np.isnan(train_target)
                n_active = int(np.sum(np.abs(sr[valid]) > 1e-10))
                if n_active < MIN_SIGNAL_OBS:
                    ics[f] = 0.0
                    continue
                corr_val = np.corrcoef(sr[valid], train_target[valid])[0, 1]
                ics[f] = 0.0 if np.isnan(corr_val) else float(corr_val)

            # Weight by IC magnitude (simplified from full 11-step for speed)
            total_abs = sum(abs(v) for v in ics.values())
            if total_abs > 1e-10:
                current_weights = {k: v / total_abs for k, v in ics.items()}
            else:
                equal_w = 1.0 / max(1, len(features))
                current_weights = {f: equal_w for f in features}

            last_fit_idx = t

        # Score current bar using trained weights
        row = df.iloc[t]
        norm_values: dict[str, float] = {}
        for f in features:
            val = float(row[f]) if not pd.isna(row[f]) else 0.0
            norm_values[f] = max(-3.0, min(3.0, (val - current_means[f]) / current_stds[f]))

        combined_alpha = sum(
            current_weights.get(f, 0.0) * norm_values[f] for f in features
        )

        hand_tuned_score = float(row.get("total_score", row.get("total", 0.0)))
        fwd_return = float(row[target_col]) if not pd.isna(row[target_col]) else 0.0

        decision_row: dict[str, Any] = {
            "bar_index": t,
            "symbol": row.get("symbol", ""),
            "combined_alpha": round(combined_alpha, 6),
            "hand_tuned_score": round(hand_tuned_score, 4),
            "forward_return": round(fwd_return, 6),
        }
        if "timestamp" in df.columns:
            ts = row["timestamp"]
            decision_row["timestamp"] = str(ts) if pd.notna(ts) else ""

        # Threshold decisions
        for thresh in thresholds:
            decision_row[f"enter_t{thresh:.2f}"] = combined_alpha > thresh

        decisions.append(decision_row)

    decisions_df = pd.DataFrame(decisions)

    # Save decisions JSONL
    with open(output_dir / "expanding_window_decisions.jsonl", "w") as f:
        for d in decisions:
            f.write(json.dumps(d, default=str) + "\n")
    log.info("  Saved %d expanding-window decisions", len(decisions))

    return decisions_df


# ---------------------------------------------------------------------------
# Evaluation and comparison
# ---------------------------------------------------------------------------


def evaluate_and_compare(
    decisions_df: pd.DataFrame,
    df: pd.DataFrame,
    features: list[str],
    target_col: str,
    thresholds: list[float],
    output_dir: Path,
) -> dict[str, Any]:
    """Compare combined alpha vs hand-tuned scoring."""
    log.info("Evaluating and comparing")

    summary: dict[str, Any] = {"thresholds": {}}

    if decisions_df.empty:
        log.warning("No decisions to evaluate")
        return summary

    for thresh in thresholds:
        col = f"enter_t{thresh:.2f}"
        if col not in decisions_df.columns:
            continue
        entered = decisions_df[decisions_df[col]]
        skipped = decisions_df[~decisions_df[col]]

        entered_return = float(entered["forward_return"].mean()) if len(entered) > 0 else 0.0
        skipped_return = float(skipped["forward_return"].mean()) if len(skipped) > 0 else 0.0
        win_rate = float((entered["forward_return"] > 0).mean()) if len(entered) > 0 else 0.0
        separation = entered_return - skipped_return

        summary["thresholds"][f"t{thresh:.2f}"] = {
            "threshold": thresh,
            "total_entered": len(entered),
            "total_skipped": len(skipped),
            "mean_fwd_return_entered": round(entered_return * 100, 4),
            "mean_fwd_return_skipped": round(skipped_return * 100, 4),
            "win_rate": round(win_rate * 100, 2),
            "separation_pp": round(separation * 100, 4),
        }

    # Quality gates
    best_threshold: str | None = None
    best_separation = -999.0
    for k, v in summary["thresholds"].items():
        if v["separation_pp"] > best_separation and v["total_entered"] >= 30:
            best_separation = v["separation_pp"]
            best_threshold = k

    summary["best_threshold"] = best_threshold
    summary["best_separation_pp"] = round(best_separation, 4) if best_threshold else None

    # Quality gate checks
    if best_threshold:
        bt = summary["thresholds"][best_threshold]
        summary["quality_gates"] = {
            "entered_gt_skipped": bt["mean_fwd_return_entered"] > bt["mean_fwd_return_skipped"],
            "win_rate_gt_55": bt["win_rate"] > 55.0,
            "total_entered_gt_30": bt["total_entered"] > 30,
            "positive_expectancy": bt["mean_fwd_return_entered"] > 0,
            "beats_empirical_0_392pp": bt["separation_pp"] > 0.392,
        }

    return summary


def compute_per_symbol_ic(
    df: pd.DataFrame,
    features: list[str],
    target_col: str,
    output_dir: Path,
) -> dict[str, dict[str, float]]:
    """Compute IC per component per symbol for the XRP diagnostic."""
    log.info("Computing per-symbol IC diagnostic")
    symbols = df["symbol"].unique()
    ic_by_symbol: dict[str, dict[str, float]] = {}

    for sym in symbols:
        sym_df = df[df["symbol"] == sym]
        if len(sym_df) < 10:
            log.warning("  %s: only %d rows, skipping IC calculation", sym, len(sym_df))
            ic_by_symbol[str(sym)] = {f: float("nan") for f in features}
            continue

        target = sym_df[target_col].values
        ics: dict[str, float] = {}
        for f in features:
            vals = sym_df[f].values.astype(float)
            valid = ~np.isnan(vals) & ~np.isnan(target)
            if valid.sum() < 10:
                ics[f] = float("nan")
            else:
                corr_val = np.corrcoef(vals[valid], target[valid])[0, 1]
                ics[f] = 0.0 if np.isnan(corr_val) else round(float(corr_val), 4)
        ic_by_symbol[str(sym)] = ics

    # Save JSON
    with open(output_dir / "per_symbol_ic.json", "w") as f:
        json.dump(ic_by_symbol, f, indent=2)

    # Save heatmap
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import seaborn as sns

        ic_df = pd.DataFrame(ic_by_symbol).T
        fig, ax = plt.subplots(figsize=(12, 6))
        sns.heatmap(
            ic_df,
            annot=True,
            fmt=".3f",
            cmap="RdYlGn",
            center=0,
            ax=ax,
        )
        ax.set_title("Per-Symbol Information Coefficient by Component")
        ax.set_ylabel("Symbol")
        ax.set_xlabel("Component")
        fig.tight_layout()
        fig.savefig(output_dir / "per_symbol_ic_heatmap.png", dpi=150)
        plt.close(fig)
        log.info("  Saved per_symbol_ic_heatmap.png")
    except Exception as e:
        log.warning("Could not generate per-symbol IC heatmap: %s", e)

    return ic_by_symbol


# ---------------------------------------------------------------------------
# Visualization helpers
# ---------------------------------------------------------------------------


def plot_ic_comparison(
    ic_table: pd.DataFrame,
    output_dir: Path,
) -> None:
    """Bar chart comparing raw IC vs independent IC."""
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(12, 6))
        x = range(len(ic_table))
        width = 0.35

        ax.bar(
            [i - width / 2 for i in x],
            ic_table["raw_ic"],
            width,
            label="Raw IC",
            color="steelblue",
        )
        ax.bar(
            [i + width / 2 for i in x],
            ic_table["independent_ic"],
            width,
            label="Independent IC",
            color="coral",
        )
        ax.set_xticks(list(x))
        ax.set_xticklabels(ic_table["feature"], rotation=45, ha="right")
        ax.set_ylabel("Information Coefficient")
        ax.set_title("Raw IC vs Independent IC (after decorrelation)")
        ax.legend()
        ax.axhline(y=0, color="black", linewidth=0.5)
        fig.tight_layout()
        fig.savefig(output_dir / "raw_vs_independent_ic.png", dpi=150)
        plt.close(fig)
        log.info("  Saved raw_vs_independent_ic.png")
    except Exception as e:
        log.warning("Could not generate IC comparison chart: %s", e)


def plot_threshold_sweep(
    summary: dict[str, Any],
    output_dir: Path,
) -> None:
    """Plot entered return and trade count by threshold."""
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        thresholds_data = summary.get("thresholds", {})
        if not thresholds_data:
            return

        thresh_vals = []
        returns = []
        counts = []
        for _, v in sorted(thresholds_data.items()):
            thresh_vals.append(v["threshold"])
            returns.append(v["mean_fwd_return_entered"])
            counts.append(v["total_entered"])

        fig, ax1 = plt.subplots(figsize=(10, 6))
        ax2 = ax1.twinx()

        ax1.plot(thresh_vals, returns, "b-o", label="Mean Fwd Return (%)")
        ax2.bar(thresh_vals, counts, width=0.08, alpha=0.3, color="gray", label="Trade Count")

        ax1.set_xlabel("Alpha Threshold (z-score)")
        ax1.set_ylabel("Mean Forward Return (%)", color="blue")
        ax2.set_ylabel("Trade Count", color="gray")
        ax1.set_title("Threshold Sweep: Return vs Selectivity")
        ax1.legend(loc="upper left")
        ax2.legend(loc="upper right")
        fig.tight_layout()
        fig.savefig(output_dir / "threshold_sweep.png", dpi=150)
        plt.close(fig)
        log.info("  Saved threshold_sweep.png")
    except Exception as e:
        log.warning("Could not generate threshold sweep plot: %s", e)


def plot_alpha_separation(
    decisions_df: pd.DataFrame,
    output_dir: Path,
) -> None:
    """Plot combined alpha deciles vs mean forward return."""
    if decisions_df.empty:
        return
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        df_plot = decisions_df[["combined_alpha", "forward_return"]].dropna()
        if len(df_plot) < 20:
            return

        df_plot["decile"] = pd.qcut(df_plot["combined_alpha"], 10, labels=False, duplicates="drop")
        decile_returns = df_plot.groupby("decile")["forward_return"].mean() * 100

        fig, ax = plt.subplots(figsize=(10, 6))
        decile_returns.plot(kind="bar", ax=ax, color="steelblue")
        ax.set_xlabel("Combined Alpha Decile (0=lowest, 9=highest)")
        ax.set_ylabel("Mean Forward Return (%)")
        ax.set_title("Alpha Separation: Forward Return by Alpha Decile")
        ax.axhline(y=0, color="black", linewidth=0.5)
        fig.tight_layout()
        fig.savefig(output_dir / "alpha_separation_curve.png", dpi=150)
        plt.close(fig)
        log.info("  Saved alpha_separation_curve.png")
    except Exception as e:
        log.warning("Could not generate alpha separation plot: %s", e)


# ---------------------------------------------------------------------------
# Data generation (requires backtest infrastructure)
# ---------------------------------------------------------------------------


async def generate_decision_data(
    symbols: list[str],
    lookback_days: int,
    output_path: Path,
) -> Path:
    """Run backtest with all score gates at 0 and capture decision data.

    This must be run on the production server with database access.
    It uses the BacktestEngine to run each symbol, intercepting
    structlog output to capture all signal events with score_breakdown.
    """
    # Import here to avoid issues when running analysis-only mode locally
    from src.backtest.backtest_engine import BacktestEngine
    from src.config.config import Config

    log.info(
        "Generating decision data: symbols=%s, lookback=%d days",
        symbols, lookback_days,
    )

    config = Config.from_yaml(Path("src/config/config.yaml"))  # type: ignore[arg-type]
    # Keep normal score gates — we want both accepted AND rejected signals.
    # Rejected signals log score_breakdown; accepted signals we enrich
    # from the Signal object via monkey-patching.

    end_date = datetime.now(UTC)
    start_date = end_date - timedelta(days=lookback_days)

    # Capture all signal events via structlog processor + signal factory patch
    captured_signals: list[dict[str, Any]] = []

    import structlog  # noqa: E402

    from src.strategy.smc_engine import SMCEngine  # noqa: E402

    original_processors = structlog.get_config().get("processors", [])

    # Capture score-rejected signals from structlog
    def signal_capture_processor(
        logger_obj: Any, method_name: str, event_dict: dict[str, Any]
    ) -> dict[str, Any]:
        """Intercept structlog events to capture rejected signal data."""
        event = event_dict.get("event", "")
        if event in (
            "Signal Rejected (Score)",
            "ENTRY_BLOCKED_LOW_CONVICTION",
        ):
            captured_signals.append({
                "timestamp": str(event_dict.get("timestamp", datetime.now(UTC).isoformat())),
                "symbol": event_dict.get("symbol", ""),
                "event": event,
                "score_breakdown": event_dict.get("score_breakdown", {}),
                "setup": event_dict.get("setup", event_dict.get("setup_type", "")),
                "signal_type": event_dict.get("signal_type", ""),
                "entry": event_dict.get("entry", 0),
                "stop": event_dict.get("stop", 0),
                "bias": event_dict.get("bias", ""),
                "adx": event_dict.get("adx", 0),
                "regime": event_dict.get("regime", ""),
                "decision": "REJECT",
            })
        # Capture accepted signals via "Score Passed" log
        elif "Score Passed" in str(event_dict.get("event", "")):
            # This doesn't fire — accepted signals go through Signal creation
            pass
        return event_dict

    # Monkey-patch SMCEngine to capture accepted signals with score_breakdown
    _original_generate = SMCEngine.generate_signal

    def _patched_generate(self_engine: Any, *args: Any, **kwargs: Any) -> Any:
        result = _original_generate(self_engine, *args, **kwargs)
        # result is a Signal — check if it has score_breakdown (accepted signal)
        if result is not None and hasattr(result, "score_breakdown"):
            bd = getattr(result, "score_breakdown", None)
            if bd and isinstance(bd, dict) and bd.get("smc") is not None:
                # Pull freshness grade strings + touch metadata from structure_info.
                # These live on the enriched OB/FVG dicts populated by smc_engine.
                si = getattr(result, "structure_info", {}) or {}
                ob = si.get("order_block") or {}
                fvg = si.get("fvg") or {}
                freshness_info = {
                    "ob_freshness": ob.get("freshness"),
                    "ob_touch_count": ob.get("touch_count"),
                    "ob_age_candles": ob.get("age_candles"),
                    "ob_body_freshness": ob.get("body_freshness"),
                    "ob_body_touch_count": ob.get("body_touch_count"),
                    "fvg_freshness": fvg.get("freshness"),
                    "fvg_touch_count": fvg.get("touch_count"),
                    "fvg_age_candles": fvg.get("age_candles"),
                    "fvg_mitigation_depth": fvg.get("mitigation_depth"),
                }
                # Phase 2A: multi-TF OB stacking metadata. The per-TF dicts inside
                # tf_stack carry raw zone_relation + bias for re-bucketing.
                tf_stack = si.get("tf_stack") or {}
                stacking_info = {
                    "tf_stack_depth_contained": tf_stack.get("tf_stack_depth_contained"),
                    "tf_stack_depth_overlapping": tf_stack.get("tf_stack_depth_overlapping"),
                    "tf_stack_bias_conflict": tf_stack.get("tf_stack_bias_conflict"),
                    "tf_stack_1d": tf_stack.get("tf_stack_1d"),
                    "tf_stack_1w": tf_stack.get("tf_stack_1w"),
                }
                captured_signals.append({
                    "timestamp": str(getattr(result, "timestamp", datetime.now(UTC).isoformat())),
                    "symbol": getattr(result, "symbol", ""),
                    "event": "Signal accepted",
                    "score_breakdown": {k: float(v) if v is not None else 0.0 for k, v in bd.items()},
                    "freshness_info": freshness_info,
                    "stacking_info": stacking_info,
                    "setup": str(getattr(result, "setup_type", "")),
                    "signal_type": str(getattr(result, "signal_type", "")),
                    "entry": float(getattr(result, "entry_price", 0)),
                    "stop": float(getattr(result, "stop_loss", 0)),
                    "bias": str(getattr(result, "higher_tf_bias", "")),
                    "adx": float(getattr(result, "adx", 0)),
                    "regime": str(getattr(result, "regime", "")),
                    "decision": "ENTER",
                })
        return result

    SMCEngine.generate_signal = _patched_generate  # type: ignore[assignment]

    # Patch BacktestEngine._fetch_historical to always use DB data
    # (avoids the 95% coverage check that falls back to the API, which
    # only returns ~720 recent candles and loses all DB history).
    from src.storage.repository import get_candles as _db_get_candles

    async def _db_only_fetch(
        self_engine: Any,
        symbol: str,
        timeframe: str,
        fetch_start: datetime,
        fetch_end: datetime,
    ) -> list:
        """Fetch candles from DB only, skip API fallback."""
        candles = _db_get_candles(symbol, timeframe, fetch_start, fetch_end)
        log.info(
            "  DB-only fetch: %s %s -> %d candles (%s to %s)",
            symbol, timeframe, len(candles),
            str(candles[0].timestamp)[:19] if candles else "N/A",
            str(candles[-1].timestamp)[:19] if candles else "N/A",
        )
        return self_engine._normalize_candles(candles, fetch_start, fetch_end)

    _original_fetch = BacktestEngine._fetch_historical
    BacktestEngine._fetch_historical = _db_only_fetch  # type: ignore[assignment]

    # Add our processor
    structlog.configure(
        processors=[signal_capture_processor] + list(original_processors),
    )

    async def _run_one(symbol: str) -> None:
        log.info("  Running backtest for %s (%s to %s)", symbol, start_date.date(), end_date.date())
        from decimal import Decimal
        engine = BacktestEngine(config, symbol=symbol, starting_equity=Decimal("10000"))
        try:
            await engine.run(start_date=start_date, end_date=end_date)
        except Exception as exc:
            log.warning("  Backtest failed for %s: %s", symbol, exc)
        finally:
            if getattr(engine, "client", None):
                try:
                    await engine.client.close()
                except Exception:
                    pass

    try:
        import asyncio
        await asyncio.gather(*[_run_one(sym) for sym in symbols])
    finally:
        # Restore original state
        structlog.configure(processors=list(original_processors))
        SMCEngine.generate_signal = _original_generate  # type: ignore[assignment]
        BacktestEngine._fetch_historical = _original_fetch  # type: ignore[assignment]

    log.info("Captured %d signal events", len(captured_signals))

    if not captured_signals:
        log.warning("No signals captured — check that backtest ran correctly")
        return output_path

    # Flatten and compute forward returns
    # Forward returns require candle data; for now we record
    # the captured signals and note that forward returns need
    # to be added from a candle data source
    rows: list[dict[str, Any]] = []
    for sig in captured_signals:
        row: dict[str, Any] = {
            "timestamp": sig["timestamp"],
            "symbol": sig["symbol"],
            "signal_type": sig["signal_type"],
            "setup_type": sig["setup"],
            "regime": sig["regime"],
            "higher_tf_bias": sig["bias"],
            "decision": sig["decision"],
            "entry_price": sig["entry"],
            "stop_loss": sig["stop"],
            "adx": sig["adx"],
        }
        # Flatten score_breakdown
        bd = sig.get("score_breakdown", {})
        for key in SCORE_FEATURES:
            row[key] = float(bd.get(key, 0.0))
        row["total_score"] = float(bd.get("total", sum(row[k] for k in SCORE_FEATURES)))
        row["threshold"] = float(bd.get("threshold", 0.0))
        # Flatten freshness_info (grade strings + touch metadata for bucketing)
        fi = sig.get("freshness_info", {}) or {}
        for key in (
            "ob_freshness", "ob_touch_count", "ob_age_candles",
            "ob_body_freshness", "ob_body_touch_count",
            "fvg_freshness", "fvg_touch_count", "fvg_age_candles",
            "fvg_mitigation_depth",
        ):
            row[key] = fi.get(key)
        # Flatten stacking_info (Phase 2A multi-TF OB metadata). Scalar fields
        # already surface via score_breakdown; the nested per-TF dicts carry
        # zone_relation + bias for post-hoc bucketing beyond the two-bucket
        # depth_contained / depth_overlapping cut.
        sti = sig.get("stacking_info", {}) or {}
        row["tf_stack_1d"] = sti.get("tf_stack_1d")
        row["tf_stack_1w"] = sti.get("tf_stack_1w")
        rows.append(row)

    # Write JSONL
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        for row in rows:
            f.write(json.dumps(row, default=str) + "\n")

    log.info("Wrote %d decision rows to %s", len(rows), output_path)
    log.warning(
        "NOTE: forward_return_5bar and forward_return_10bar columns are NOT "
        "yet populated. You must compute them from candle data before running "
        "the analysis. Use --compute-forward-returns to add them."
    )
    return output_path


async def compute_forward_returns(
    input_path: Path,
    output_path: Path,
    horizon_bars: int = 5,
    bar_timeframe_minutes: int = 240,
) -> Path:
    """Add forward returns to decision JSONL using database candle data.

    Must be run on production server with database access.
    Uses direct SQL queries to get 4H candle data for forward return calculation.
    """
    import os

    from sqlalchemy import create_engine, text

    log.info("Computing forward returns (horizon=%d bars, tf=%dm)", horizon_bars, bar_timeframe_minutes)

    rows: list[dict[str, Any]] = []
    with open(input_path) as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))

    if not rows:
        raise ValueError("No rows in input file")

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        raise ValueError("DATABASE_URL not set — run on production server")

    engine = create_engine(db_url)
    symbols = list({r["symbol"] for r in rows})

    # Load 4H candle data for each symbol from DB
    candle_cache: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        log.info("  Loading candle data for %s from database", symbol)
        try:
            query = text("""
                SELECT timestamp, close
                FROM candles
                WHERE symbol = :symbol AND timeframe = '4h'
                ORDER BY timestamp
            """)
            with engine.connect() as conn:
                result = conn.execute(query, {"symbol": symbol})
                candle_rows = result.fetchall()
            if candle_rows:
                cdf = pd.DataFrame(candle_rows, columns=["timestamp", "close"])
                cdf["timestamp"] = pd.to_datetime(cdf["timestamp"], utc=True)
                cdf["close"] = cdf["close"].astype(float)
                cdf = cdf.sort_values("timestamp").reset_index(drop=True)
                candle_cache[symbol] = cdf
                log.info("    Loaded %d 4H candles for %s", len(cdf), symbol)
            else:
                log.warning("    No candles found for %s", symbol)
        except Exception as e:
            log.warning("  Failed to load candles for %s: %s", symbol, e)

    engine.dispose()

    # Match signals to forward returns
    enriched = 0
    for row in rows:
        symbol = row["symbol"]
        ts = pd.Timestamp(row["timestamp"])
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        entry_price = float(row.get("entry_price", 0))
        sig_type = str(row.get("signal_type", "long")).lower()
        direction = 1.0 if "long" in sig_type else -1.0

        cdf = candle_cache.get(symbol)
        if cdf is None or entry_price == 0:
            row["forward_return_5bar"] = None
            row["forward_return_10bar"] = None
            continue

        # Find candles after signal timestamp
        future = cdf[cdf["timestamp"] > ts]

        if len(future) >= horizon_bars:
            close_5 = float(future.iloc[horizon_bars - 1]["close"])
            row["forward_return_5bar"] = direction * (close_5 - entry_price) / entry_price
            enriched += 1
        else:
            row["forward_return_5bar"] = None

        if len(future) >= 10:
            close_10 = float(future.iloc[9]["close"])
            row["forward_return_10bar"] = direction * (close_10 - entry_price) / entry_price
        else:
            row["forward_return_10bar"] = None

    # Write updated JSONL
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        for row in rows:
            f.write(json.dumps(row, default=str) + "\n")

    log.info(
        "Wrote %d rows with forward returns to %s (%d have 5-bar returns)",
        len(rows), output_path, enriched,
    )
    return output_path


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def run_analysis(
    input_path: Path,
    output_dir: Path,
    target_col: str,
    min_window: int,
    thresholds: list[float],
) -> dict[str, Any]:
    """Execute the full 11-step alpha combination analysis."""
    np.random.seed(RANDOM_SEED)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 0: Load data
    df = load_data(input_path, target_col)

    # Determine active features
    features = [f for f in SCORE_FEATURES if f in df.columns]
    log.info("Active features (%d): %s", len(features), features)

    # Save feature matrix
    try:
        df.to_parquet(output_dir / "feature_matrix.parquet", index=False)
        log.info("Saved feature_matrix.parquet")
    except Exception:
        df.to_csv(output_dir / "feature_matrix.csv", index=False)
        log.info("Saved feature_matrix.csv (parquet failed)")

    # Steps 1-3: Raw signal returns -> drift removal -> normalization
    signal_returns = step_01_raw_signal_returns(df, features, target_col)
    drift_adj = step_02_remove_drift(signal_returns)
    normalized, excluded_step3 = step_03_variance_normalize(drift_adj)

    # Steps 4-6: Cross-sectional demeaning
    _ = step_04_cross_sectional_mean(normalized, df["symbol"])
    demeaned = step_05_cross_sectional_demean(normalized, df, features, target_col)
    renormalized, excluded_step6 = step_06_renormalize(demeaned)

    all_excluded = list(set(excluded_step3 + excluded_step6))
    active_features = [f for f in features if f not in all_excluded]
    if all_excluded:
        log.info("Excluded features (zero variance): %s", all_excluded)

    # Step 7: Correlation matrix
    corr = step_07_correlation_matrix(renormalized, active_features, output_dir)

    # Step 8: Clustering
    cluster_assignments = step_08_signal_clustering(corr, output_dir)

    # Step 9: Independence extraction
    ic_table = step_09_independence_extraction(
        renormalized, df, active_features, target_col
    )

    # Save IC table
    ic_records = ic_table.to_dict("records")
    with open(output_dir / "independent_ic.json", "w") as f:
        json.dump(ic_records, f, indent=2)

    # Plot IC comparison
    plot_ic_comparison(ic_table, output_dir)

    # Step 10: Optimal weighting
    weights = step_10_optimal_weighting(ic_table, cluster_assignments)
    with open(output_dir / "optimal_weights.json", "w") as f:
        json.dump(weights, f, indent=2)

    # Step 11: Combined alpha
    combined_alpha = step_11_combined_alpha(renormalized, weights, active_features)
    df["combined_alpha"] = combined_alpha.values

    # Per-symbol IC diagnostic (Section 5c)
    per_symbol_ic = compute_per_symbol_ic(df, active_features, target_col, output_dir)

    # Expanding-window backtest
    decisions_df = run_expanding_window_backtest(
        df, active_features, target_col, min_window, thresholds, output_dir
    )

    # Evaluation
    summary = evaluate_and_compare(
        decisions_df, df, active_features, target_col, thresholds, output_dir
    )
    summary["features_used"] = active_features
    summary["features_excluded"] = all_excluded
    summary["total_rows"] = len(df)
    summary["symbols"] = sorted(df["symbol"].unique().tolist())
    summary["per_symbol_ic"] = per_symbol_ic
    summary["optimal_weights"] = weights

    # Save summary
    with open(output_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)
    log.info("Saved summary.json")

    # Plots
    plot_threshold_sweep(summary, output_dir)
    plot_alpha_separation(decisions_df, output_dir)

    # Save model
    model = AlphaModel(
        weights=weights,
        feature_means={f: float(df[f].mean()) for f in active_features},
        feature_stds={f: max(float(df[f].std()), 1e-10) for f in active_features},
        cluster_assignments=cluster_assignments,
        sparse_features=[
            r["feature"] for r in ic_records if r.get("sparse", False)
        ],
        fitted_at=datetime.now(UTC).isoformat(),
    )
    with open(output_dir / "model.pkl", "wb") as f:
        pickle.dump(asdict(model), f)
    log.info("Saved model.pkl")

    # Print summary
    log.info("=" * 60)
    log.info("ALPHA COMBINATION ANALYSIS COMPLETE")
    log.info("=" * 60)
    log.info("Total rows: %d", len(df))
    log.info("Active features: %d", len(active_features))
    log.info("Symbols: %s", summary["symbols"])
    log.info("Optimal weights:")
    for feat, w in sorted(weights.items(), key=lambda x: abs(x[1]), reverse=True):
        log.info("  %15s: %+.4f", feat, w)

    if summary.get("quality_gates"):
        log.info("Quality gates:")
        for gate, passed in summary["quality_gates"].items():
            status = "PASS" if passed else "FAIL"
            log.info("  %s: %s", gate, status)

    if summary.get("best_threshold"):
        bt = summary["thresholds"][summary["best_threshold"]]
        log.info(
            "Best threshold: %s (separation=%.4f pp, %d trades, %.1f%% WR)",
            summary["best_threshold"],
            bt["separation_pp"],
            bt["total_entered"],
            bt["win_rate"],
        )

    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Alpha Combination Engine — Fundamental Law of Active Management"
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Path to input JSONL file with decision data",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports/alpha_combination"),
        help="Output directory for artifacts",
    )
    parser.add_argument(
        "--target-column",
        default="forward_return_5bar",
        help="Target column for forward returns (default: forward_return_5bar)",
    )
    parser.add_argument(
        "--min-window",
        type=int,
        default=DEFAULT_MIN_WINDOW,
        help=f"Minimum expanding window before predictions (default: {DEFAULT_MIN_WINDOW})",
    )
    parser.add_argument(
        "--threshold-sweep",
        default="0.0,0.25,0.50,0.75,1.0",
        help="Comma-separated z-score thresholds to sweep",
    )
    parser.add_argument(
        "--generate-data",
        action="store_true",
        help="Generate decision data from backtest (requires DB access)",
    )
    parser.add_argument(
        "--compute-forward-returns",
        action="store_true",
        help="Compute forward returns from candle data (requires DB access)",
    )
    parser.add_argument(
        "--symbols",
        default="ETH/USD,BTC/USD,SOL/USD,LINK/USD,XRP/USD",
        help="Comma-separated symbols for data generation",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=365,
        help="Lookback period in days for data generation (default: 365)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    thresholds = [float(t) for t in args.threshold_sweep.split(",")]
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    input_path = args.input

    if args.generate_data:
        symbols = [s.strip() for s in args.symbols.split(",")]
        default_output = output_dir / "decision_data.jsonl"
        input_path = input_path or default_output
        asyncio.run(
            generate_decision_data(symbols, args.lookback_days, input_path)
        )

    if args.compute_forward_returns:
        if input_path is None:
            log.error("--input required with --compute-forward-returns")
            sys.exit(1)
        enriched = output_dir / "decision_data_with_returns.jsonl"
        asyncio.run(compute_forward_returns(input_path, enriched))
        input_path = enriched

    if input_path is None:
        log.error(
            "No input file specified. Use --input <path> or --generate-data"
        )
        sys.exit(1)

    if not input_path.exists():
        log.error("Input file not found: %s", input_path)
        sys.exit(1)

    run_analysis(
        input_path=input_path,
        output_dir=output_dir,
        target_col=args.target_column,
        min_window=args.min_window,
        thresholds=thresholds,
    )


if __name__ == "__main__":
    main()
