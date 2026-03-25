"""
Auction-based portfolio allocator.

Implements deterministic auction logic to select the best 50 positions each cycle,
with hysteresis and cost penalties to prevent churn.
"""
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Set, Tuple
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from enum import Enum

from src.domain.models import Position, Signal, Side
from src.data.symbol_utils import normalize_symbol_for_position_match as _normalize_symbol_for_matching
from src.monitoring.logger import get_logger

logger = get_logger(__name__)


class ContenderKind(str, Enum):
    """Type of contender in the auction."""
    OPEN = "open"
    NEW = "new"


@dataclass
class OpenPositionMetadata:
    """Metadata for an open position needed for auction evaluation."""
    position: Position
    entry_time: datetime
    entry_score: float
    current_pnl_R: Decimal  # PnL in R multiples
    margin_used: Decimal
    cluster: str  # e.g., "tight_smc_ob", "wide_structure_bos"
    direction: Side
    age_seconds: float
    is_protective_orders_live: bool
    locked: bool = False  # Cannot be kicked (within MIN_HOLD, protective orders not live, etc.)
    spot_symbol: Optional[str] = None  # Spot symbol for matching against candidate signals
    conviction: Optional[float] = None  # Optional thesis conviction for dynamic hold controls


@dataclass
class CandidateSignal:
    """Candidate signal for entry."""
    signal: Signal
    score: float
    direction: Side
    symbol: str
    cluster: str
    required_margin: Decimal
    risk_R: Decimal  # Stop distance in R
    position_notional: Decimal  # Pre-computed position notional from auction sizing


@dataclass
class PortfolioLimits:
    """Portfolio-level constraints."""
    max_positions: int = 50
    max_margin_util: float = 0.90  # 90% of available margin
    max_per_cluster: int = 12  # For 50 positions, ~12 per cluster
    max_per_symbol: int = 1  # One position per symbol
    max_net_long: Optional[Decimal] = None  # Optional net exposure cap
    max_net_short: Optional[Decimal] = None
    direction_concentration_penalty: float = 10.0  # Score penalty at maximum directional imbalance
    max_directional_imbalance_pct: float = 100.0


@dataclass
class Contender:
    """A single contender in the auction (open position or new candidate)."""
    kind: ContenderKind
    symbol: str
    cluster: str
    direction: Side
    required_margin: Decimal  # For NEW, margin_used for OPEN
    value: float  # Computed value score
    locked: bool = False  # OPEN only
    
    # Reference to original object
    open_metadata: Optional[OpenPositionMetadata] = None
    candidate: Optional[CandidateSignal] = None
    
    # For tie-breaking
    age_seconds: float = 0.0  # OPEN only


@dataclass
class AllocationPlan:
    """Result of the auction allocation."""
    opens: List[Signal]  # Signals to open
    closes: List[str]  # Position symbols to close
    reductions: List[Tuple[str, Decimal]] = field(default_factory=list)  # Optional size reductions
    reasons: Dict[str, any] = field(default_factory=dict)  # Logging metadata


class AuctionAllocator:
    """
    Deterministic auction allocator for portfolio management.
    
    Selects the best 50 positions each cycle using value scoring,
    with hysteresis and cost penalties to prevent churn.
    """
    
    def __init__(
        self,
        limits: PortfolioLimits,
        swap_threshold: float = 10.0,
        min_hold_minutes: int = 15,
        max_trades_per_cycle: int = 5,
        max_new_opens_per_cycle: int = 5,
        max_closes_per_cycle: int = 5,
        entry_cost: float = 2.0,
        exit_cost: float = 2.0,
        rebalancer_enabled: bool = False,
        rebalancer_trigger_pct_equity: float = 0.32,
        rebalancer_clear_pct_equity: float = 0.24,
        rebalancer_per_symbol_trim_cooldown_cycles: int = 2,
        rebalancer_max_reductions_per_cycle: int = 1,
        rebalancer_max_total_margin_reduced_per_cycle: float = 0.25,
        no_signal_persistence_enabled: bool = False,
        no_signal_close_persistence_cycles: int = 3,
        no_signal_persistence_canary_symbols: Optional[List[str]] = None,
    ):
        """
        Initialize auction allocator.
        
        Args:
            limits: Portfolio constraints
            swap_threshold: Minimum score advantage required to replace an open position
            min_hold_minutes: Minimum time before a position can be kicked
            max_trades_per_cycle: Maximum total trades per cycle (anti-churn)
            max_new_opens_per_cycle: Maximum new positions per cycle
            max_closes_per_cycle: Maximum closes per cycle
            entry_cost: Entry cost penalty in score points
            exit_cost: Exit cost penalty in score points
        """
        self.limits = limits
        self.swap_threshold = swap_threshold
        self.min_hold_seconds = min_hold_minutes * 60
        self.max_trades_per_cycle = max_trades_per_cycle
        self.max_new_opens_per_cycle = max_new_opens_per_cycle
        self.max_closes_per_cycle = max_closes_per_cycle
        self.entry_cost = entry_cost
        self.exit_cost = exit_cost
        self.rebalancer_enabled = rebalancer_enabled
        self.rebalancer_trigger_pct_equity = Decimal(str(rebalancer_trigger_pct_equity))
        self.rebalancer_clear_pct_equity = Decimal(str(rebalancer_clear_pct_equity))
        self.rebalancer_per_symbol_trim_cooldown_cycles = rebalancer_per_symbol_trim_cooldown_cycles
        self.rebalancer_max_reductions_per_cycle = rebalancer_max_reductions_per_cycle
        self.rebalancer_max_total_margin_reduced_per_cycle = Decimal(
            str(rebalancer_max_total_margin_reduced_per_cycle)
        )
        self.no_signal_persistence_enabled = bool(no_signal_persistence_enabled)
        self.no_signal_close_persistence_cycles = max(int(no_signal_close_persistence_cycles or 1), 1)
        self.no_signal_persistence_canary_symbols = {
            str(s).strip().upper() for s in (no_signal_persistence_canary_symbols or [])
        }
        
        logger.info(
            "AuctionAllocator initialized",
            max_positions=limits.max_positions,
            swap_threshold=swap_threshold,
            min_hold_minutes=min_hold_minutes,
            rebalancer_enabled=rebalancer_enabled,
            no_signal_persistence_enabled=self.no_signal_persistence_enabled,
            no_signal_close_persistence_cycles=self.no_signal_close_persistence_cycles,
        )

    def _symbol_in_persistence_canary(self, symbol: str) -> bool:
        if not self.no_signal_persistence_canary_symbols:
            return True
        normalized = str(symbol).strip().upper().split(":")[0]
        return normalized in self.no_signal_persistence_canary_symbols
    
    def allocate(
        self,
        open_positions: List[OpenPositionMetadata],
        candidate_signals: List[CandidateSignal],
        portfolio_state: Dict[str, any],
    ) -> AllocationPlan:
        """
        Run the auction allocation algorithm.
        
        Args:
            open_positions: All open positions with metadata (<=50)
            candidate_signals: All new signals this cycle
            portfolio_state: Current portfolio state (account_equity, available_margin, etc.)
        
        Returns:
            AllocationPlan with opens, closes, and reasons
        """
        # Step A: Build contender list
        contenders = self._build_contender_list(open_positions, candidate_signals, portfolio_state)
        
        if not contenders:
            logger.debug("No contenders in auction")
            return AllocationPlan(opens=[], closes=[], reasons={"message": "No contenders"})
        
        # Step B: Sort by value (descending)
        contenders.sort(key=lambda c: (
            -c.value,  # Higher value first
            0 if c.kind == ContenderKind.OPEN else 1,  # OPEN beats NEW (stability)
            -c.age_seconds,  # Older open wins (stability)
            c.required_margin,  # Lower margin (efficiency)
        ))
        
        # Step C: Select winners under constraints
        winners = self._select_winners(contenders, portfolio_state)
        
        # Step D: Translate into actions
        winner_symbols = {c.symbol for c in winners}
        open_symbols = {op.position.symbol for op in open_positions}
        
        to_keep = [op for op in open_positions if op.position.symbol in winner_symbols]
        to_close_candidates = [op for op in open_positions if op.position.symbol not in winner_symbols]
        to_open_candidates = [c for c in winners if c.kind == ContenderKind.NEW]
        
        # Step E: Apply hysteresis swap rule
        final_closes, final_opens, hysteresis_stats = self._apply_hysteresis(
            to_close_candidates,
            to_open_candidates,
            to_keep,
            portfolio_state,
        )
        
        # Apply per-cycle limits as paired swaps to preserve "best 50"
        # Limit swaps (paired closes+opens) to maintain portfolio quality
        max_new_opens_per_cycle = max(
            int(
                portfolio_state.get(
                    "auction_max_new_opens_per_cycle",
                    self.max_new_opens_per_cycle,
                )
                or self.max_new_opens_per_cycle
            ),
            1,
        )
        max_closes_per_cycle = max(
            int(
                portfolio_state.get(
                    "auction_max_closes_per_cycle",
                    self.max_closes_per_cycle,
                )
                or self.max_closes_per_cycle
            ),
            1,
        )
        max_trades_per_cycle = max(
            int(
                portfolio_state.get(
                    "auction_max_trades_per_cycle",
                    self.max_trades_per_cycle,
                )
                or self.max_trades_per_cycle
            ),
            1,
        )
        max_swaps = min(max_new_opens_per_cycle, max_closes_per_cycle, max_trades_per_cycle)
        
        # Match closes to opens for swaps
        swap_pairs = []
        remaining_closes = []
        remaining_opens = []
        
        # First, pair up closes with opens (swaps)
        close_by_cluster = {op.position.symbol: op for op in final_closes}
        for new_contender in final_opens[:max_swaps]:
            # Find best matching close in same cluster
            matching_close = None
            for close_op in final_closes:
                if close_op.cluster == new_contender.cluster and close_op.position.symbol not in [p[0] for p in swap_pairs]:
                    if matching_close is None or close_op.position.symbol not in [p[0] for p in swap_pairs]:
                        matching_close = close_op
                        break
            
            if matching_close:
                swap_pairs.append((matching_close.position.symbol, new_contender))
            else:
                remaining_opens.append(new_contender)
        
        # Remaining closes (not part of swaps) - limit independently
        paired_close_symbols = {p[0] for p in swap_pairs}
        remaining_closes = [op for op in final_closes if op.position.symbol not in paired_close_symbols]
        remaining_closes = remaining_closes[:max(max_closes_per_cycle - len(swap_pairs), 0)]
        
        # Remaining opens (not part of swaps) - limit independently
        remaining_opens = remaining_opens[:max(max_new_opens_per_cycle - len(swap_pairs), 0)]
        
        # Build result
        all_opens = [c.candidate.signal for _, c in swap_pairs if c.candidate] + [c.candidate.signal for c in remaining_opens if c.candidate]
        closes = [symbol for symbol, _ in swap_pairs] + [op.position.symbol for op in remaining_closes]
        
        # CRITICAL FIX: Enforce net_opens <= net_closes + free_slots to prevent exceeding max positions
        current_open_count = len(open_positions)
        free_slots = max(self.limits.max_positions - current_open_count, 0)
        closes_count = len(closes)
        allowed_opens = closes_count + free_slots
        opens = all_opens[:allowed_opens]  # Enforce net position limit
        
        reductions, reduction_reasons = self._plan_concentration_reductions(
            open_positions=open_positions,
            close_symbols=set(closes),
            portfolio_state=portfolio_state,
        )

        reasons = {
            "total_contenders": len(contenders),
            "winners_selected": len(winners),
            "opens_after_hysteresis": len(final_opens),
            "closes_after_hysteresis": len(final_closes),
            "opens_after_limits": len(opens),
            "closes_after_limits": len(closes),
            "reductions_planned": len(reductions),
            "reduction_reasons": reduction_reasons,
            "hysteresis_swap_rejected": int(hysteresis_stats.get("swap_rejected_hysteresis", 0)),
            "hysteresis_swap_rejected_chop": int(
                hysteresis_stats.get("swap_rejected_hysteresis_chop", 0)
            ),
            "hysteresis_close_suppressed_no_signal": int(
                hysteresis_stats.get("close_suppressed_no_signal_persistence", 0)
            ),
        }
        
        logger.info(
            "Auction allocation complete",
            **reasons
        )
        
        return AllocationPlan(
            opens=opens,
            closes=closes,
            reductions=reductions,
            reasons=reasons,
        )

    def _plan_concentration_reductions(
        self,
        open_positions: List[OpenPositionMetadata],
        close_symbols: Set[str],
        portfolio_state: Dict[str, any],
    ) -> Tuple[List[Tuple[str, Decimal]], Dict[str, int]]:
        """
        Plan partial reductions for oversized positions using the same metric as invariants:
        position size_notional / account_equity.
        """
        reasons: Dict[str, int] = {}
        reductions: List[Tuple[str, Decimal]] = []
        if not self.rebalancer_enabled:
            reasons["rebalancer_disabled"] = 1
            return reductions, reasons
        if self.rebalancer_max_reductions_per_cycle <= 0:
            reasons["max_reductions_zero"] = 1
            return reductions, reasons

        account_equity = Decimal(str(portfolio_state.get("account_equity", 0) or 0))
        if account_equity <= 0:
            reasons["invalid_equity"] = 1
            return reductions, reasons

        trigger_pct = self.rebalancer_trigger_pct_equity
        clear_pct = self.rebalancer_clear_pct_equity
        if clear_pct >= trigger_pct:
            reasons["invalid_hysteresis"] = 1
            return reductions, reasons

        current_cycle = int(portfolio_state.get("current_cycle", 0) or 0)
        cooldown_cycles = int(self.rebalancer_per_symbol_trim_cooldown_cycles or 0)
        last_trim_cycle_by_symbol = dict(
            portfolio_state.get("last_trim_cycle_by_symbol", {}) or {}
        )
        max_total_margin_reduction = (
            account_equity * self.rebalancer_max_total_margin_reduced_per_cycle
        )
        allow_locked_rebalancer_trims = bool(
            portfolio_state.get("allow_locked_rebalancer_trims", False)
        )

        # Prioritize largest concentration offenders first
        ranked = sorted(
            open_positions,
            key=lambda op: (
                -(
                    (getattr(op.position, "size_notional", Decimal("0")) or Decimal("0"))
                    / account_equity
                ) if account_equity > 0 else Decimal("0")
            ),
        )

        total_margin_reduction = Decimal("0")
        for op_meta in ranked:
            if len(reductions) >= self.rebalancer_max_reductions_per_cycle:
                reasons["max_reductions_reached"] = reasons.get("max_reductions_reached", 0) + 1
                break
            symbol = op_meta.position.symbol
            if symbol in close_symbols and not allow_locked_rebalancer_trims:
                reasons["skip_symbol_closing"] = reasons.get("skip_symbol_closing", 0) + 1
                continue
            if op_meta.locked and not allow_locked_rebalancer_trims:
                reasons["skip_locked"] = reasons.get("skip_locked", 0) + 1
                continue

            size_notional = getattr(op_meta.position, "size_notional", Decimal("0")) or Decimal("0")
            size_qty = getattr(op_meta.position, "size", Decimal("0")) or Decimal("0")
            margin_used = getattr(op_meta.position, "margin_used", Decimal("0")) or Decimal("0")
            if size_notional <= 0 or size_qty <= 0:
                reasons["skip_invalid_size"] = reasons.get("skip_invalid_size", 0) + 1
                continue

            concentration_pct = size_notional / account_equity
            if concentration_pct <= trigger_pct:
                reasons["below_trigger"] = reasons.get("below_trigger", 0) + 1
                continue

            last_trim_cycle = last_trim_cycle_by_symbol.get(symbol)
            if (
                cooldown_cycles > 0
                and last_trim_cycle is not None
                and current_cycle > 0
                and (current_cycle - int(last_trim_cycle)) < cooldown_cycles
            ):
                reasons["cooldown_active"] = reasons.get("cooldown_active", 0) + 1
                continue

            target_notional = account_equity * clear_pct
            trim_notional = max(Decimal("0"), size_notional - target_notional)
            if trim_notional <= 0:
                reasons["already_below_clear"] = reasons.get("already_below_clear", 0) + 1
                continue

            trim_fraction = trim_notional / size_notional
            est_margin_reduction = margin_used * trim_fraction if margin_used > 0 else Decimal("0")

            # Clamp by max total margin reduced per cycle
            remaining_margin_budget = max_total_margin_reduction - total_margin_reduction
            if remaining_margin_budget <= 0:
                reasons["margin_reduction_budget_reached"] = reasons.get(
                    "margin_reduction_budget_reached", 0
                ) + 1
                break
            if est_margin_reduction > remaining_margin_budget and est_margin_reduction > 0:
                scale = remaining_margin_budget / est_margin_reduction
                trim_fraction *= scale
                est_margin_reduction = remaining_margin_budget

            trim_qty = size_qty * trim_fraction
            if trim_qty <= 0:
                reasons["skip_zero_trim_qty"] = reasons.get("skip_zero_trim_qty", 0) + 1
                continue

            reductions.append((symbol, trim_qty))
            total_margin_reduction += est_margin_reduction
            reasons["planned"] = reasons.get("planned", 0) + 1

            logger.info(
                "Auction rebalancer reduction planned",
                symbol=symbol,
                concentration_pct=f"{concentration_pct:.2%}",
                trigger_pct=f"{trigger_pct:.2%}",
                clear_pct=f"{clear_pct:.2%}",
                trim_qty=str(trim_qty),
                trim_fraction=f"{trim_fraction:.4f}",
                est_margin_reduction=str(est_margin_reduction),
            )

        return reductions, reasons
    
    def _build_contender_list(
        self,
        open_positions: List[OpenPositionMetadata],
        candidate_signals: List[CandidateSignal],
        portfolio_state: Dict[str, any],
    ) -> List[Contender]:
        """Build and filter the contender list."""
        contenders = []
        chop_active_symbols = {
            _normalize_symbol_for_matching(str(s))
            for s in (portfolio_state.get("auction_chop_active_symbols", []) or [])
            if str(s).strip()
        }
        chop_min_hold_minutes = max(
            int(
                portfolio_state.get(
                    "auction_chop_min_hold_minutes",
                    int(self.min_hold_seconds / 60),
                )
                or int(self.min_hold_seconds / 60)
            ),
            0,
        )
        chop_min_hold_seconds = chop_min_hold_minutes * 60
        high_conviction_threshold = float(
            portfolio_state.get("auction_min_hold_high_conviction_threshold", 50.0) or 50.0
        )
        high_conviction_min_hold_minutes = max(
            int(
                portfolio_state.get(
                    "auction_min_hold_high_conviction_minutes",
                    int(self.min_hold_seconds / 60),
                )
                or int(self.min_hold_seconds / 60)
            ),
            0,
        )
        high_conviction_min_hold_seconds = high_conviction_min_hold_minutes * 60
        pnl_positive_lock_enabled = bool(
            portfolio_state.get("auction_pnl_positive_lock_enabled", False)
        )
        pnl_positive_lock_max_seconds = max(
            int(
                portfolio_state.get("auction_pnl_positive_lock_max_minutes", 60)
                or 60
            ),
            0,
        ) * 60

        # Add open positions
        now = datetime.now(timezone.utc)
        for op_meta in open_positions:
            # CRITICAL: Use spot symbol for matching if available, otherwise use futures symbol
            # This ensures proper symbol matching between open positions (futures) and candidates (spot)
            contender_symbol = op_meta.spot_symbol if op_meta.spot_symbol else op_meta.position.symbol
            contender_normalized = _normalize_symbol_for_matching(contender_symbol)
            min_hold_seconds_effective = (
                chop_min_hold_seconds
                if contender_normalized in chop_active_symbols
                else self.min_hold_seconds
            )
            if (
                op_meta.conviction is not None
                and float(op_meta.conviction) >= high_conviction_threshold
            ):
                min_hold_seconds_effective = max(
                    min_hold_seconds_effective,
                    high_conviction_min_hold_seconds,
                )
            # Mark as locked if within MIN_HOLD, protective orders not live, or UNPROTECTED
            is_unprotected = not getattr(op_meta.position, 'is_protected', True)
            # R4: PnL-positive lock — protect profitable positions from rotation
            # Cap: only applies for max_minutes after min_hold expires
            pnl_positive_lock = False
            if (
                pnl_positive_lock_enabled
                and op_meta.current_pnl_R > Decimal("0")
                and op_meta.age_seconds >= min_hold_seconds_effective
            ):
                time_since_unlock = op_meta.age_seconds - min_hold_seconds_effective
                if time_since_unlock < pnl_positive_lock_max_seconds:
                    pnl_positive_lock = True
            locked = (
                op_meta.age_seconds < min_hold_seconds_effective or
                not op_meta.is_protective_orders_live or
                is_unprotected or
                pnl_positive_lock
            )
            if is_unprotected:
                logger.warning(
                    "UNPROTECTED position marked as locked in auction",
                    symbol=op_meta.position.symbol,
                    reason=getattr(op_meta.position, 'protection_reason', 'UNKNOWN')
                )
            
            # Update locked state in metadata (for use in hysteresis)
            op_meta.locked = locked
            
            # Compute value for open position
            value = self._compute_open_value(op_meta, portfolio_state)
            
            contender = Contender(
                kind=ContenderKind.OPEN,
                symbol=contender_symbol,  # Use spot symbol for proper matching
                cluster=op_meta.cluster,
                direction=op_meta.direction,
                required_margin=op_meta.margin_used,
                value=value,
                locked=locked,
                open_metadata=op_meta,
                age_seconds=op_meta.age_seconds,
            )
            contenders.append(contender)
        
        # Capital reallocation rate limit: skip new opens when within partial-close cooldown
        last_partial = portfolio_state.get("last_partial_close_at")
        cooldown_sec = portfolio_state.get("partial_close_cooldown_seconds", 0) or 0
        in_cooldown = (
            cooldown_sec > 0
            and last_partial is not None
            and (now - last_partial).total_seconds() < cooldown_sec
        )
        if in_cooldown:
            logger.info(
                "Auction: Skipping new opens (partial-close cooldown)",
                cooldown_sec=cooldown_sec,
                seconds_since_partial=(now - last_partial).total_seconds() if last_partial else 0,
            )

        # Add candidate signals (after hard filters)
        for candidate in candidate_signals:
            if in_cooldown:
                continue
            # Hard constraint checks
            if not self._passes_hard_filters(candidate, portfolio_state):
                continue
            
            # Compute value for new candidate
            value = self._compute_new_value(candidate, portfolio_state)
            
            contender = Contender(
                kind=ContenderKind.NEW,
                symbol=candidate.symbol,
                cluster=candidate.cluster,
                direction=candidate.direction,
                required_margin=candidate.required_margin,
                value=value,
                candidate=candidate,
            )
            contenders.append(contender)
        
        return contenders
    
    def _passes_hard_filters(
        self,
        candidate: CandidateSignal,
        portfolio_state: Dict[str, any],
    ) -> bool:
        """Check if candidate passes hard constraints."""
        # Margin utilization check
        available_margin = portfolio_state.get("available_margin", Decimal("0"))
        if candidate.required_margin > available_margin * Decimal(str(self.limits.max_margin_util)):
            return False
        
        # Symbol cap (would be checked later, but early reject if already at max)
        # This is a soft check - actual enforcement happens in _select_winners
        
        return True
    
    def _compute_new_value(
        self,
        candidate: CandidateSignal,
        portfolio_state: Dict[str, any],
    ) -> float:
        """
        Compute value score for a new candidate.
        
        value_new = score - entry_cost_penalty - concentration_penalty - correlation_penalty
        """
        value = candidate.score
        
        # Entry cost penalty
        value -= self.entry_cost
        
        # Concentration penalty (computed later in context, but estimate here)
        # Will be refined in _select_winners
        
        return value
    
    def _compute_open_value(
        self,
        op_meta: OpenPositionMetadata,
        portfolio_state: Dict[str, any],
    ) -> float:
        """
        Compute value score for an open position.
        
        value_open = entry_score + pnl_bonus + trend_followthrough_bonus
                    - time_decay_penalty - concentration_penalty
                    - correlation_penalty - exit_cost_penalty
        """
        value = op_meta.entry_score
        
        # PnL bonus (convert R to score points, e.g., 1R = +5 points)
        pnl_bonus = float(op_meta.current_pnl_R) * 5.0
        value += pnl_bonus
        
        # Time decay (optional - small penalty for very old positions)
        # Skip for now
        
        # Exit cost penalty (because closing costs money)
        value -= self.exit_cost
        
        return value
    
    def _direction_penalty(self, direction: Side, long_count: int, short_count: int) -> float:
        """Compute directional concentration penalty for a candidate.
        
        Returns a score penalty that increases as the portfolio becomes more
        directionally imbalanced. Zero penalty at 50/50 balance, max penalty
        when 100% of positions are on the same side as this candidate.
        """
        total = long_count + short_count
        if total == 0:
            return 0.0
        same_side = long_count if direction == Side.LONG else short_count
        imbalance_ratio = same_side / total  # 1.0 = all same direction
        # Scale: 0 penalty at 50% or below, linearly up to max at 100%
        penalty = self.limits.direction_concentration_penalty * max(0.0, imbalance_ratio - 0.5) * 2
        return penalty

    def _select_winners(
        self,
        contenders: List[Contender],
        portfolio_state: Dict[str, any],
    ) -> List[Contender]:
        """
        Select winners under constraints with dynamic directional penalty.
        
        Iterate sorted list and add to winners if:
        - len(WINNERS) < MAX_POSITIONS
        - Adding keeps within margin cap, per-cluster/per-symbol caps, exposure caps
        
        Directional concentration penalty is applied dynamically: as the
        winner set becomes more imbalanced, same-direction candidates need
        increasingly higher base scores to be selected.
        """
        winners = []
        margin_used = Decimal("0")
        cluster_counts: Dict[str, int] = {}
        symbol_counts: Dict[str, int] = {}
        long_count = 0
        short_count = 0
        available_margin = Decimal(str(portfolio_state.get("available_margin", 0)))
        max_margin = available_margin * Decimal(str(self.limits.max_margin_util))
        
        for contender in contenders:
            if len(winners) >= self.limits.max_positions:
                break
            
            if margin_used + contender.required_margin > max_margin:
                continue
            
            contender_normalized = _normalize_symbol_for_matching(contender.symbol)
            
            existing_count = 0
            for winner in winners:
                winner_normalized = _normalize_symbol_for_matching(winner.symbol)
                if winner_normalized == contender_normalized:
                    existing_count += 1
            
            if existing_count >= self.limits.max_per_symbol:
                continue
            
            cluster_count = cluster_counts.get(contender.cluster, 0)
            if cluster_count >= self.limits.max_per_cluster:
                continue
            
            # Net exposure caps
            if self.limits.max_net_long is not None:
                if contender.direction == Side.LONG:
                    net_long_margin = sum(
                        w.required_margin for w in winners if w.direction == Side.LONG
                    )
                    if net_long_margin + contender.required_margin > self.limits.max_net_long:
                        continue
            if self.limits.max_net_short is not None:
                if contender.direction == Side.SHORT:
                    net_short_margin = sum(
                        w.required_margin for w in winners if w.direction == Side.SHORT
                    )
                    if net_short_margin + contender.required_margin > self.limits.max_net_short:
                        continue
            
            # Hard directional imbalance cap
            if self.limits.max_directional_imbalance_pct < 100.0:
                total_so_far = long_count + short_count
                if total_so_far > 0:
                    same_side = (long_count if contender.direction == Side.LONG else short_count) + 1
                    new_total = total_so_far + 1
                    if (same_side / new_total) * 100 > self.limits.max_directional_imbalance_pct:
                        if not contender.locked:
                            logger.debug(
                                "Contender rejected by directional imbalance cap",
                                symbol=contender.symbol,
                                direction=contender.direction.value,
                                same_side=same_side,
                                total=new_total,
                                cap_pct=self.limits.max_directional_imbalance_pct,
                            )
                            continue

            # Dynamic directional concentration penalty
            dir_penalty = self._direction_penalty(contender.direction, long_count, short_count)
            adjusted_value = contender.value - dir_penalty
            
            # Reject if penalty pushes value negative (not worth the concentration risk)
            if adjusted_value < 0 and not contender.locked:
                logger.debug(
                    "Contender rejected by directional penalty",
                    symbol=contender.symbol,
                    direction=contender.direction.value,
                    base_value=f"{contender.value:.1f}",
                    penalty=f"{dir_penalty:.1f}",
                    long_count=long_count,
                    short_count=short_count,
                )
                continue
            
            winners.append(contender)
            margin_used += contender.required_margin
            cluster_counts[contender.cluster] = cluster_count + 1
            symbol_counts[contender_normalized] = symbol_counts.get(contender_normalized, 0) + 1
            
            if contender.direction == Side.LONG:
                long_count += 1
            else:
                short_count += 1
        
        if long_count + short_count > 0:
            logger.info(
                "Auction directional balance",
                long_count=long_count,
                short_count=short_count,
                imbalance_ratio=f"{max(long_count, short_count) / (long_count + short_count):.1%}",
                max_penalty=f"{self.limits.direction_concentration_penalty:.1f}",
            )
        
        return winners
    
    def _apply_hysteresis(
        self,
        to_close: List[OpenPositionMetadata],
        to_open: List[Contender],
        to_keep: List[OpenPositionMetadata],
        portfolio_state: Dict[str, any],
    ) -> Tuple[List[OpenPositionMetadata], List[Contender], Dict[str, int]]:
        """
        Apply hysteresis swap rule to prevent churn.
        
        For each open in to_close, find the new that is "taking its slot".
        Only close if value_new >= value_open + SWAP_THRESHOLD.
        If not, cancel that swap: keep the open, drop that new.
        """
        final_closes = []
        final_opens = []
        stats = {
            "swap_rejected_hysteresis": 0,
            "swap_rejected_hysteresis_chop": 0,
            "close_suppressed_no_signal_persistence": 0,
        }
        swap_threshold_base = float(
            portfolio_state.get("auction_swap_threshold", self.swap_threshold) or self.swap_threshold
        )
        swap_threshold_chop = float(
            portfolio_state.get("auction_chop_swap_threshold", swap_threshold_base) or swap_threshold_base
        )
        chop_active_symbols = {
            _normalize_symbol_for_matching(str(s))
            for s in (portfolio_state.get("auction_chop_active_symbols", []) or [])
            if str(s).strip()
        }
        no_signal_cycles = int(portfolio_state.get("auction_no_signal_cycles", 0) or 0)
        no_signal_enabled = bool(
            portfolio_state.get("auction_no_signal_persistence_enabled", self.no_signal_persistence_enabled)
        )
        no_signal_threshold = int(
            portfolio_state.get(
                "auction_no_signal_close_persistence_cycles",
                self.no_signal_close_persistence_cycles,
            ) or self.no_signal_close_persistence_cycles
        )
        no_signal_canary = {
            str(s).strip().upper()
            for s in (
                portfolio_state.get(
                    "auction_no_signal_persistence_canary_symbols",
                    list(self.no_signal_persistence_canary_symbols),
                ) or []
            )
        }
        
        # Build lookup maps
        close_by_symbol = {op.position.symbol: op for op in to_close}
        open_by_symbol = {op.position.symbol: op for op in to_keep}
        
        # Match closes to opens by cluster (or globally if no cluster match)
        for close_op in to_close:
            # Skip if locked
            if close_op.locked:
                logger.debug(
                    "Skipping locked position",
                    symbol=close_op.position.symbol,
                    reason="locked"
                )
                continue
            
            # Find best matching new candidate in same cluster
            matching_new = None
            for new_contender in to_open:
                if new_contender.cluster == close_op.cluster:
                    if matching_new is None or new_contender.value > matching_new.value:
                        matching_new = new_contender
            
            # If no cluster match, find globally best
            if matching_new is None and to_open:
                matching_new = max(to_open, key=lambda c: c.value)
            
            # Check swap threshold
            if matching_new:
                # Recompute values for accurate comparison (use same portfolio_state)
                close_value = self._compute_open_value(close_op, portfolio_state)
                # For new, we already computed value, but ensure we're comparing apples to apples
                new_value = matching_new.value
                close_norm = _normalize_symbol_for_matching(close_op.position.symbol)
                new_norm = _normalize_symbol_for_matching(matching_new.symbol)
                is_chop_pair = (
                    close_norm in chop_active_symbols
                    or new_norm in chop_active_symbols
                )
                swap_threshold_effective = swap_threshold_chop if is_chop_pair else swap_threshold_base
                
                if new_value >= close_value + swap_threshold_effective:
                    # Swap approved
                    final_closes.append(close_op)
                    if matching_new not in final_opens:
                        final_opens.append(matching_new)
                    logger.debug(
                        "Swap approved",
                        close_symbol=close_op.position.symbol,
                        open_symbol=matching_new.symbol,
                        close_value=close_value,
                        new_value=new_value,
                        threshold=swap_threshold_effective,
                        threshold_mode="chop" if is_chop_pair else "base",
                    )
                else:
                    # Swap rejected - keep the open
                    stats["swap_rejected_hysteresis"] += 1
                    if is_chop_pair:
                        stats["swap_rejected_hysteresis_chop"] += 1
                    gap = new_value - close_value
                    logger.debug(
                        "Swap rejected (hysteresis)",
                        close_symbol=close_op.position.symbol,
                        open_symbol=matching_new.symbol if matching_new else None,
                        close_value=close_value,
                        new_value=new_value if matching_new else None,
                        threshold=swap_threshold_effective,
                        threshold_mode="chop" if is_chop_pair else "base",
                        gap=gap,
                    )
                    logger.info(
                        "Swap rejection diagnostics",
                        close_symbol=close_op.position.symbol,
                        open_symbol=matching_new.symbol,
                        close_value=close_value,
                        new_value=new_value,
                        gap=gap,
                        threshold=swap_threshold_effective,
                        threshold_mode="chop" if is_chop_pair else "base",
                    )
            else:
                # No replacement candidate: strategic close can be suppressed during no-signal persistence.
                canary_allows = True
                if no_signal_canary:
                    normalized = str(close_op.position.symbol).strip().upper().split(":")[0]
                    canary_allows = normalized in no_signal_canary
                should_suppress_close = (
                    no_signal_enabled
                    and canary_allows
                    and no_signal_cycles < no_signal_threshold
                )
                if should_suppress_close:
                    stats["close_suppressed_no_signal_persistence"] += 1
                    logger.info(
                        "Strategic close suppressed by no-signal persistence",
                        symbol=close_op.position.symbol,
                        no_signal_cycles=no_signal_cycles,
                        threshold=no_signal_threshold,
                    )
                else:
                    final_closes.append(close_op)
        
        # Add remaining opens that weren't matched to closes
        for new_contender in to_open:
            if new_contender not in final_opens:
                final_opens.append(new_contender)
        
        return final_closes, final_opens, stats


def derive_cluster(signal: Signal) -> str:
    """
    Derive cluster identifier from signal.
    
    Cluster = regime + setup_type (e.g., "tight_smc_ob", "wide_structure_bos")
    """
    regime = signal.regime  # "tight_smc" or "wide_structure"
    setup = signal.setup_type.value if hasattr(signal.setup_type, 'value') else str(signal.setup_type)
    return f"{regime}_{setup}"


def create_candidate_signal(
    signal: Signal,
    required_margin: Decimal,
    risk_R: Decimal,
    position_notional: Decimal,
) -> CandidateSignal:
    """Create a CandidateSignal from a Signal."""
    direction = Side.LONG if signal.signal_type.value == "long" else Side.SHORT
    cluster = derive_cluster(signal)
    
    return CandidateSignal(
        signal=signal,
        score=signal.score,
        direction=direction,
        symbol=signal.symbol,
        cluster=cluster,
        required_margin=required_margin,
        risk_R=risk_R,
        position_notional=position_notional,
    )


def position_to_open_metadata(
    position: Position,
    account_equity: Decimal,
    is_protective_orders_live: bool = True,
) -> OpenPositionMetadata:
    """
    Convert a Position to OpenPositionMetadata for auction evaluation.
    
    Args:
        position: The position object
        account_equity: Current account equity (for R calculation)
        is_protective_orders_live: Whether stop/tp orders are active
    
    Returns:
        OpenPositionMetadata with computed values
    """
    now = datetime.now(timezone.utc)
    age_seconds = (now - position.opened_at).total_seconds()
    
    # Calculate current PnL in R multiples
    # R = risk at entry = position_notional * initial_stop_distance_pct
    if position.initial_stop_distance_pct and position.initial_stop_distance_pct > 0:
        risk_R = position.size_notional * position.initial_stop_distance_pct
        if risk_R > 0:
            current_pnl_R = position.unrealized_pnl / risk_R
        else:
            current_pnl_R = Decimal("0")
    else:
        # Fallback: estimate R from current stop distance
        if position.initial_stop_price and position.entry_price:
            stop_distance_pct = abs(position.entry_price - position.initial_stop_price) / position.entry_price
            risk_R = position.size_notional * stop_distance_pct
            if risk_R > 0:
                current_pnl_R = position.unrealized_pnl / risk_R
            else:
                current_pnl_R = Decimal("0")
        else:
            current_pnl_R = Decimal("0")
    
    # Derive cluster if not stored
    cluster = position.cluster
    if not cluster and position.setup_type and position.regime:
        cluster = f"{position.regime}_{position.setup_type}"
    elif not cluster:
        cluster = "unknown"
    
    # Get entry score (default to 0 if not stored)
    entry_score = position.entry_score if position.entry_score is not None else 0.0
    
    return OpenPositionMetadata(
        position=position,
        entry_time=position.opened_at,
        entry_score=entry_score,
        current_pnl_R=current_pnl_R,
        margin_used=position.margin_used,
        cluster=cluster,
        direction=position.side,
        age_seconds=age_seconds,
        is_protective_orders_live=is_protective_orders_live,
    )
