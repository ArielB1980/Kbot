from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple

from src.monitoring.logger import get_logger

logger = get_logger(__name__)


@dataclass
class EVResult:
    prior_win_prob: float
    posterior_win_prob: float
    ev_r: float
    ev_usd: float
    reason: str = ""


class EVEngine:
    """
    Phase C1 shadow-only EV/Bayesian estimator.

    Design goal: compute and log signals for observability with zero behavior change.
    """

    def __init__(self, config: Any):
        self.config = config
        # (symbol, regime, conviction_band) -> {"wins": int, "trials": int}
        self.buckets: Dict[Tuple[str, str, int], Dict[str, int]] = {}

    def compute(self, inputs: Dict[str, Any]) -> EVResult:
        symbol = str(inputs.get("symbol", "UNKNOWN"))
        conviction = float(inputs.get("conviction", 20.0))
        regime = str(inputs.get("regime", "unknown"))
        risk_r = float(inputs.get("risk_r", 1.0))

        conviction_band = int(conviction // 20)
        bucket_key = (symbol, regime, conviction_band)
        bucket = self.buckets.get(bucket_key, {"wins": 0, "trials": 0})

        # Beta(1,1) smoothed prior from sparse bucket stats.
        prior = (
            (float(bucket["wins"]) + 1.0) / (float(bucket["trials"]) + 2.0)
            if bucket["trials"] > 0
            else 0.5
        )

        # C1: neutral likelihood ratio (placeholder for future evidence model).
        lr = 1.0
        posterior = (prior * lr) / (prior * lr + (1.0 - prior))

        ev_r = (posterior * 1.0) - ((1.0 - posterior) * 1.0)
        ev_usd = ev_r * risk_r

        return EVResult(
            prior_win_prob=prior,
            posterior_win_prob=posterior,
            ev_r=ev_r,
            ev_usd=ev_usd,
            reason="shadow_v1_neutral_lr",
        )

    def log_ev_trace(self, result: EVResult, inputs: Dict[str, Any]) -> None:
        logger.info(
            "EV_SHADOW_TRACE",
            symbol=inputs.get("symbol"),
            regime=inputs.get("regime"),
            conviction=inputs.get("conviction"),
            prior=result.prior_win_prob,
            posterior=result.posterior_win_prob,
            ev_r=result.ev_r,
            ev_usd=result.ev_usd,
            reason=result.reason,
        )
