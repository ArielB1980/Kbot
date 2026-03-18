"""
Risk management for position sizing, leverage control, and safety limits.
"""
from enum import Enum
from typing import List, Optional, TYPE_CHECKING
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
import os

from src.domain.models import Signal, RiskDecision, Position, Side
from src.config.config import LiquidityFilters, RiskConfig, TierConfig, resolve_risk_for_symbol
from src.monitoring.logger import get_logger
from src.domain.protocols import EventRecorder, _noop_event_recorder
from src.risk.basis_guard import BasisGuard

if TYPE_CHECKING:
    from src.config.config import Config

logger = get_logger(__name__)


class BindingConstraint(str, Enum):
    """Single winner: the final constraint that limited position size."""
    RISK_SIZING = "risk_sizing"
    NOTIONAL_OVERRIDE = "notional_override"
    MAX_USD = "max_usd"
    BUYING_POWER = "buying_power"
    SINGLE_MARGIN = "single_margin"
    AGGREGATE_MARGIN = "aggregate_margin"
    AVAILABLE_MARGIN = "available_margin"
    UTILISATION_BOOST = "utilisation_boost"
    MIN_NOTIONAL_REJECT = "min_notional_reject"


class RiskManager:
    """
    Risk management and position sizing.
    
    CRITICAL: Position sizing is independent of leverage.
    Leverage determines margin usage, not risk.
    
    Supports tier-based sizing when liquidity_filters is provided:
    - Tier A (high liquidity): Full leverage and size limits
    - Tier B (medium liquidity): Reduced leverage and size
    - Tier C (low liquidity): Most conservative limits
    """
    
    def __init__(
        self,
        config: RiskConfig,
        *,
        liquidity_filters: Optional[LiquidityFilters] = None,
        event_recorder: EventRecorder = _noop_event_recorder,
    ):
        """
        Initialize risk manager.
        
        Args:
            config: Risk configuration
            liquidity_filters: Optional liquidity filters with tier configs for tier-based sizing
            event_recorder: Callable for recording system events (injected; defaults to no-op)
        """
        self.config = config
        self.liquidity_filters = liquidity_filters
        self._record_event = event_recorder
        self._base_config = config
        
        # Portfolio state tracking
        self.current_positions: List[Position] = []
        self.daily_pnl = Decimal("0")
        self.daily_start_equity = Decimal("0")
        
        # Regime-specific streak tracking
        self.consecutive_losses_tight = 0
        self.consecutive_losses_wide = 0
        
        self.cooldown_until: Optional[datetime] = None  # Time-based cooldown
        
        tier_info = "enabled" if liquidity_filters else "disabled"
        logger.info("Risk Manager initialized", config=config.model_dump(), tier_based_sizing=tier_info)
    
    def get_tier_config(self, tier: str) -> Optional[TierConfig]:
        """Get tier-specific config if liquidity_filters is set."""
        if self.liquidity_filters:
            return self.liquidity_filters.get_tier_config(tier)
        return None
    
    def validate_trade(
        self,
        signal: Signal,
        account_equity: Decimal,
        spot_price: Decimal,
        perp_mark_price: Decimal,
        exchange_liquidation_price: Optional[Decimal] = None,
        futures_entry_price: Optional[Decimal] = None,
        futures_stop_loss: Optional[Decimal] = None,
        available_margin: Optional[Decimal] = None,
        notional_override: Optional[Decimal] = None,
        skip_margin_check: bool = False,
        symbol_tier: Optional[str] = None,
    ) -> RiskDecision:
        """
        Validate proposed trade against all risk limits.
        
        Args:
            signal: Trading signal from strategy
            account_equity: Current account equity
            spot_price: Current spot price
            perp_mark_price: Current perpetual mark price
            exchange_liquidation_price: Exchange-reported liquidation price (if position exists)
            futures_entry_price: Converted futures entry price (for accurate risk calc)
            futures_stop_loss: Converted futures stop price (for accurate risk calc)
            available_margin: Kraken available margin (equity minus margin in use). When set,
                position size is capped so we never exceed it, preventing insufficientAvailableFunds.
        
        Returns:
            RiskDecision with approval status and details
        """
        self.config = resolve_risk_for_symbol(self._base_config, signal.symbol)
        rejection_reasons = []
        # Calculate position size using FUTURES prices if available (more accurate)
        # Otherwise fall back to spot prices
        if futures_entry_price and futures_stop_loss:
            entry_for_risk = futures_entry_price
            stop_for_risk = futures_stop_loss
        else:
            entry_for_risk = signal.entry_price
            stop_for_risk = signal.stop_loss

        # Validate entry price to prevent division by zero
        if entry_for_risk <= 0:
            logger.error(
                "Invalid entry price for risk calculation",
                symbol=signal.symbol,
                entry_for_risk=str(entry_for_risk),
                futures_entry_price=str(futures_entry_price) if futures_entry_price else None,
                signal_entry_price=str(signal.entry_price)
            )
            return RiskDecision(
                approved=False,
                rejection_reasons=["Invalid entry price (zero or negative)"],
                position_notional=Decimal("0"),
                leverage=Decimal("1"),
                margin_required=Decimal("0"),
                liquidation_buffer_pct=Decimal("0"),
                basis_divergence_pct=Decimal("0"),
                estimated_fees_funding=Decimal("0")
            )

        stop_distance_pct = abs(entry_for_risk - stop_for_risk) / entry_for_risk

        # Validate stop distance to prevent division by zero
        if stop_distance_pct <= 0:
            logger.error(
                "Invalid stop distance for risk calculation",
                symbol=signal.symbol,
                entry_for_risk=str(entry_for_risk),
                stop_for_risk=str(stop_for_risk),
                stop_distance_pct=str(stop_distance_pct)
            )
            return RiskDecision(
                approved=False,
                rejection_reasons=["Invalid stop distance (stop equals entry)"],
                position_notional=Decimal("0"),
                leverage=Decimal("1"),
                margin_required=Decimal("0"),
                liquidation_buffer_pct=Decimal("0"),
                basis_divergence_pct=Decimal("0"),
                estimated_fees_funding=Decimal("0")
            )

        # Validate account equity to prevent division by zero
        if account_equity <= 0:
            logger.error(
                "Invalid account equity for risk calculation",
                symbol=signal.symbol,
                account_equity=str(account_equity)
            )
            return RiskDecision(
                approved=False,
                rejection_reasons=["Invalid account equity (zero or negative)"],
                position_notional=Decimal("0"),
                leverage=Decimal("1"),
                margin_required=Decimal("0"),
                liquidation_buffer_pct=Decimal("0"),
                basis_divergence_pct=Decimal("0"),
                estimated_fees_funding=Decimal("0")
            )

        # Calculate leverage setting (use target leverage for sizing)
        # target_leverage determines actual position leverage (e.g., 7x)
        # max_leverage is the absolute cap for safety checks (10x)
        requested_leverage = Decimal(str(getattr(self.config, 'target_leverage', self.config.max_leverage)))
        
        # Apply tier-specific leverage cap if symbol_tier is provided
        tier_config = self.get_tier_config(symbol_tier) if symbol_tier else None
        tier_max_leverage = Decimal(str(tier_config.max_leverage)) if tier_config else None
        tier_max_size = tier_config.max_position_size_usd if tier_config else None
        
        logger.info(
            "Trade tier classification",
            symbol=signal.symbol,
            tier=symbol_tier or "none",
            tier_max_leverage=str(tier_max_leverage) if tier_max_leverage else "global",
            tier_max_size=str(tier_max_size) if tier_max_size else "global",
        )
        
        if tier_max_leverage and requested_leverage > tier_max_leverage:
            logger.info(
                "Applying tier leverage cap",
                symbol=signal.symbol,
                tier=symbol_tier,
                original_leverage=str(requested_leverage),
                tier_max_leverage=str(tier_max_leverage),
            )
            requested_leverage = tier_max_leverage
        
        # Get sizing method (needed for later logic even if using override)
        sizing_method = getattr(self.config, 'sizing_method', 'fixed')
        
        # Calculate buying_power (needed for later checks even if using override)
        buying_power = account_equity * requested_leverage
        
        # Binding constraint and computed-from-risk for explainability log (one winner: final_binding_constraint)
        binding_constraint = BindingConstraint.NOTIONAL_OVERRIDE
        binding_constraints: List[BindingConstraint] = []
        computed_notional_from_risk: Optional[Decimal] = None
        per_position_ceiling: Optional[Decimal] = None
        aggregate_margin_remaining: Optional[Decimal] = None
        utilisation_boost_applied = False

        # If notional_override provided (auction execution), use it directly and skip sizing
        if notional_override is not None:
            position_notional = notional_override
            computed_notional_from_risk = position_notional
            binding_constraints = [BindingConstraint.NOTIONAL_OVERRIDE]
            logger.debug(
                "Using notional override from auction",
                symbol=signal.symbol,
                notional=str(position_notional),
            )
        else:
            binding_constraint = BindingConstraint.RISK_SIZING
            binding_constraints = [BindingConstraint.RISK_SIZING]
            # --- NEW SIZING LOGIC (V4: Adaptive) ---
            # Leverage-Based Sizing (Simple)
            # Position size = Equity × Leverage × Risk%
            if sizing_method == "leverage_based":
                position_notional = buying_power * Decimal(str(self.config.risk_per_trade_pct))
                logger.debug(
                    "Leverage-based sizing",
                    equity=str(account_equity),
                    leverage=str(requested_leverage),
                    risk_pct=str(self.config.risk_per_trade_pct),
                    position_notional=str(position_notional)
                )
            else:
                # Base Sizing (Fixed Risk)
                # Position size = (Equity * Risk%) / Stop_Dist%
                base_risk_amount = account_equity * Decimal(str(self.config.risk_per_trade_pct))
                position_notional = base_risk_amount / stop_distance_pct

            # Kelly Criterion Sizing (skip if leverage_based)
            if sizing_method in ["kelly", "kelly_volatility"]:
                win_prob = Decimal(str(self.config.kelly_win_prob))
                win_loss_ratio = Decimal(str(self.config.kelly_win_loss_ratio))
                
                # Kelly % = W - (1-W)/R
                kelly_pct = win_prob - ((Decimal("1") - win_prob) / win_loss_ratio)
                
                # Apply Cap (Quarter Kelly defaults to 0.25)
                max_kelly = Decimal(str(self.config.kelly_max_fraction))
                kelly_fraction = min(kelly_pct, max_kelly)
                
                if kelly_fraction > 0:
                    # Kelly suggests risking X% of bankroll per trade
                    # Risk Amount = Equity * Kelly%
                    kelly_risk_amount = account_equity * kelly_fraction
                    
                    # Check absolute max risk cap
                    max_risk_pct = Decimal(str(self.config.max_risk_per_trade_entry_pct))
                    abs_risk_cap = account_equity * max_risk_pct
                    
                    final_risk_amount = min(kelly_risk_amount, abs_risk_cap)
                    
                    kelly_notional = final_risk_amount / stop_distance_pct
                    position_notional = kelly_notional
                    logger.debug(f"Kelly Sizing: Frac={kelly_fraction:.2f}, Risk=${final_risk_amount:.2f}")
            
            # Volatility Scaling
            if sizing_method in ["volatility", "kelly_volatility"]:
                # Check availability of ATR Ratio from Signal
                if hasattr(signal, 'atr_ratio') and signal.atr_ratio is not None:
                    ratio = float(signal.atr_ratio)
                    scaler = 1.0
                    
                    high_threshold = float(getattr(self.config, 'vol_sizing_atr_threshold_high', 1.5))
                    low_threshold = float(getattr(self.config, 'vol_sizing_atr_threshold_low', 0.8))
                    
                    if ratio > high_threshold:
                        penalty = float(getattr(self.config, 'vol_sizing_high_vol_penalty', 0.6))
                        scaler = penalty
                        logger.debug(f"Volatility Sizing: High Vol (Ratio {ratio:.2f}) -> Penalty {penalty}x")
                        
                    elif ratio < low_threshold:
                        boost = float(getattr(self.config, 'vol_sizing_low_vol_boost', 1.2))
                        scaler = boost
                        logger.debug(f"Volatility Sizing: Low Vol (Ratio {ratio:.2f}) -> Boost {boost}x")
                        
                    position_notional *= Decimal(str(scaler))
                else:
                    logger.debug("Volatility Sizing skipped: atr_ratio missing in Signal")

            computed_notional_from_risk = position_notional
            
            # Hard Cap: Max Notional USD (use tier-specific cap if available)
            max_usd = Decimal(str(self.config.max_position_size_usd))
            if tier_max_size and tier_max_size < max_usd:
                effective_max_usd = tier_max_size
                logger.debug(
                    "Using tier max position size",
                    symbol=signal.symbol,
                    tier=symbol_tier,
                    tier_max=str(tier_max_size),
                    global_max=str(max_usd),
                )
            else:
                effective_max_usd = max_usd
            
            if position_notional > effective_max_usd:
                position_notional = effective_max_usd
                binding_constraint = BindingConstraint.MAX_USD
                binding_constraints.append(BindingConstraint.MAX_USD)

        # Hard Cap: Max Leverage Buying Power (only if not using override)
        if notional_override is None:
            buying_power = account_equity * requested_leverage
            if position_notional > buying_power:
                position_notional = buying_power
                binding_constraint = BindingConstraint.BUYING_POWER
                binding_constraints.append(BindingConstraint.BUYING_POWER)

        min_notional_viable = Decimal("10")
        # Margin caps (capital utilisation): limit margin vs equity, not notional
        max_single_margin_pct = Decimal(str(getattr(self.config, "max_single_position_margin_pct_equity", 0.25)))
        max_aggregate_margin_pct = Decimal(str(getattr(self.config, "max_aggregate_margin_pct_equity", 2.0)))
        max_single_margin = account_equity * max_single_margin_pct
        max_single_notional = max_single_margin * requested_leverage
        per_position_ceiling = max_single_notional
        if position_notional > max_single_notional:
            logger.info(
                "Capping position by single-position margin limit",
                symbol=signal.symbol,
                before=str(position_notional),
                after=str(max_single_notional),
                equity=str(account_equity),
                max_margin_pct=str(max_single_margin_pct),
                leverage=str(requested_leverage),
            )
            position_notional = max_single_notional
            binding_constraint = BindingConstraint.SINGLE_MARGIN
            binding_constraints.append(BindingConstraint.SINGLE_MARGIN)
        existing_notional = sum(
            abs(Decimal(str(p.size)) * Decimal(str(p.current_mark_price or p.entry_price or 0)))
            for p in self.current_positions
            if p.size and p.size != 0
        )
        existing_margin = existing_notional / requested_leverage if requested_leverage > 0 else Decimal("0")
        new_margin = position_notional / requested_leverage
        max_aggregate_margin = account_equity * max_aggregate_margin_pct
        aggregate_margin_remaining = max_aggregate_margin - existing_margin
        projected_margin = existing_margin + new_margin
        if projected_margin > max_aggregate_margin:
            margin_headroom = max(max_aggregate_margin - existing_margin, Decimal("0"))
            allowed_notional = margin_headroom * requested_leverage
            if allowed_notional < min_notional_viable:
                rejection_reasons.append(
                    f"Aggregate margin ${projected_margin:.2f} would exceed {max_aggregate_margin_pct:.0%} of equity "
                    f"(${max_aggregate_margin:.2f}). Existing margin=${existing_margin:.2f}, "
                    f"new margin=${new_margin:.2f}. Headroom=${allowed_notional:.2f} below min ${min_notional_viable}."
                )
            else:
                logger.info(
                    "Capping position notional by aggregate margin limit",
                    symbol=signal.symbol,
                    existing_margin=str(existing_margin),
                    before=str(position_notional),
                    after=str(allowed_notional),
                    equity=str(account_equity),
                    max_margin_pct=str(max_aggregate_margin_pct),
                )
                position_notional = allowed_notional
                binding_constraint = BindingConstraint.AGGREGATE_MARGIN
                binding_constraints.append(BindingConstraint.AGGREGATE_MARGIN)

        if position_notional < min_notional_viable:
            rejection_reasons.append(
                f"Position notional ${position_notional:.2f} below minimum ${min_notional_viable} "
                f"after equity cap (equity=${account_equity:.2f})"
            )
            logger.warning(
                "MIN_NOTIONAL_HARD_REJECT",
                symbol=signal.symbol,
                position_notional=str(position_notional),
                min_notional_viable=str(min_notional_viable),
                equity=str(account_equity),
            )
            return RiskDecision(
                approved=False,
                rejection_reasons=rejection_reasons,
                position_notional=Decimal("0"),
                leverage=requested_leverage,
                margin_required=Decimal("0"),
                liquidation_buffer_pct=Decimal("0"),
                basis_divergence_pct=Decimal("0"),
                estimated_fees_funding=Decimal("0"),
            )

        # Cap by available margin (prevents Kraken "insufficientAvailableFunds")
        # Skip margin check if skip_margin_check=True (auction already validated)
        # Note: min_notional lowered to $10 to support smaller accounts with tier C (2x leverage)
        min_notional = Decimal("10")
        if not skip_margin_check and available_margin is not None:
            if available_margin <= 0:
                rejection_reasons.append(
                    "Insufficient available margin (all margin in use); cannot open new position"
                )
                return RiskDecision(
                    approved=False,
                    rejection_reasons=rejection_reasons,
                    position_notional=Decimal("0"),
                    leverage=requested_leverage,
                    margin_required=Decimal("0"),
                    liquidation_buffer_pct=Decimal("0"),
                    basis_divergence_pct=Decimal("0"),
                    estimated_fees_funding=Decimal("0"),
                )
            # Use ~95% of available to leave buffer for fees/slippage
            max_margin_use = available_margin * Decimal("0.95")
            max_notional_from_avail = max_margin_use * requested_leverage
            if position_notional > max_notional_from_avail:
                logger.debug(
                    "Capping position notional by available margin",
                    symbol=signal.symbol,
                    available_margin=str(available_margin),
                    before=str(position_notional),
                    after=str(max_notional_from_avail),
                )
                position_notional = max_notional_from_avail
                binding_constraint = BindingConstraint.AVAILABLE_MARGIN
                binding_constraints.append(BindingConstraint.AVAILABLE_MARGIN)
            if position_notional < min_notional:
                rejection_reasons.append(
                    f"Insufficient available margin: would allow only ${position_notional:.0f} notional (min ${min_notional})"
                )
                return RiskDecision(
                    approved=False,
                    rejection_reasons=rejection_reasons,
                    position_notional=Decimal("0"),
                    leverage=requested_leverage,
                    margin_required=Decimal("0"),
                    liquidation_buffer_pct=Decimal("0"),
                    basis_divergence_pct=Decimal("0"),
                    estimated_fees_funding=Decimal("0"),
                )
        elif skip_margin_check:
            logger.debug(
                "Skipping margin check (auction execution)",
                symbol=signal.symbol,
                notional=str(position_notional),
            )

        # Post-sizing utilisation boost (auction mode only): if margin utilisation
        # below target, scale notional up (bounded by hard caps).
        # Gate: only fires when auction provides a notional_override (i.e. the
        # execution path was auction-selected). Non-auction single-signal trades
        # never get boosted.
        # Risk sanity: only boost when sizing is leverage_based. With stop-distance-
        # based sizing (fixed/kelly/volatility), notional = f(stop_distance); boosting
        # would increase dollar risk for the same stop and violate risk-per-trade.
        if (
            getattr(self.config, "auction_mode_enabled", False)
            and notional_override is not None
            and sizing_method == "leverage_based"
            and position_notional >= min_notional_viable
            and aggregate_margin_remaining is not None
            and per_position_ceiling is not None
            and requested_leverage > 0
        ):
            new_margin_here = position_notional / requested_leverage
            current_util = (existing_margin + new_margin_here) / account_equity if account_equity > 0 else Decimal("0")
            target_min = Decimal(str(getattr(self.config, "target_margin_util_min", 0.70)))
            if current_util < target_min:
                target_margin = target_min * account_equity - existing_margin
                target_notional = (target_margin * requested_leverage) if target_margin > 0 else position_notional
                max_factor = Decimal(str(getattr(self.config, "utilisation_boost_max_factor", 2.0)))

                # Compute max notional cap (tier-specific or global)
                _max_usd_global = Decimal(str(self.config.max_position_size_usd))
                _tier_max = tier_max_size if tier_max_size else _max_usd_global
                _effective_max_usd = min(_tier_max, _max_usd_global)

                # Hard caps: every one of these must hold
                caps = [
                    target_notional,                                     # what target util needs
                    position_notional * max_factor,                      # config max boost factor
                    per_position_ceiling,                                # single-position margin cap
                    aggregate_margin_remaining * requested_leverage,     # aggregate margin cap
                    _effective_max_usd,                                  # explicit notional cap
                ]
                # Also cap by exchange available margin (prevents insufficientAvailableFunds)
                if available_margin is not None and available_margin > 0:
                    caps.append(available_margin * Decimal("0.95") * requested_leverage)

                capped_boost = min(caps)

                if capped_boost > position_notional:
                    # Identify which cap was the binding one
                    boost_binding = "max_factor"
                    if capped_boost == per_position_ceiling:
                        boost_binding = "single_margin"
                    elif capped_boost == aggregate_margin_remaining * requested_leverage:
                        boost_binding = "aggregate_margin"
                    elif available_margin is not None and capped_boost == available_margin * Decimal("0.95") * requested_leverage:
                        boost_binding = "available_margin"
                    elif capped_boost == _effective_max_usd:
                        boost_binding = "max_usd"
                    elif capped_boost == target_notional:
                        boost_binding = "target_util"
                    elif capped_boost == position_notional * max_factor:
                        boost_binding = "max_factor"

                    boost_factor = capped_boost / position_notional if position_notional > 0 else Decimal("0")
                    logger.info(
                        "UTILISATION_BOOST_APPLIED",
                        symbol=signal.symbol,
                        old_notional=str(position_notional),
                        boosted_notional=str(capped_boost),
                        boost_factor=f"{boost_factor:.2f}",
                        current_util=f"{current_util:.3f}",
                        target_min_util=str(target_min),
                        binding_cap=boost_binding,
                        max_factor=str(max_factor),
                        per_position_ceiling=str(per_position_ceiling),
                        aggregate_remaining=str(aggregate_margin_remaining * requested_leverage),
                        available_margin_cap=str(available_margin * Decimal("0.95") * requested_leverage) if available_margin is not None else "n/a",
                    )
                    position_notional = capped_boost
                    utilisation_boost_applied = True
                    binding_constraint = BindingConstraint.UTILISATION_BOOST
                    binding_constraints.append(BindingConstraint.UTILISATION_BOOST)
        
        logger.debug(
            f"Validating trade: {len(self.current_positions)} active positions",
            symbol=signal.symbol,
            equity=str(account_equity),
            buying_power=str(buying_power),
            available_margin=str(available_margin) if available_margin is not None else "n/a",
        )

        # P0.3: Max dollar loss per trade — explicit cap on worst-case loss if stop hits.
        # Computed after all sizing adjustments (caps, boosts) so it reflects actual position size.
        max_loss_per_trade_usd = Decimal(str(getattr(self.config, "max_loss_per_trade_usd", 500.0)))
        if position_notional > 0 and stop_distance_pct > 0:
            # Dollar loss = notional * stop_distance_pct
            estimated_loss_at_stop = position_notional * stop_distance_pct
            if estimated_loss_at_stop > max_loss_per_trade_usd:
                # Try to cap position size first
                capped_notional = max_loss_per_trade_usd / stop_distance_pct
                if capped_notional >= min_notional_viable:
                    logger.info(
                        "Capping position by max_loss_per_trade_usd",
                        symbol=signal.symbol,
                        before=str(position_notional),
                        after=str(capped_notional),
                        estimated_loss=str(estimated_loss_at_stop),
                        max_loss=str(max_loss_per_trade_usd),
                        stop_distance_pct=str(stop_distance_pct),
                    )
                    position_notional = capped_notional
                    binding_constraint = BindingConstraint.MAX_USD
                    binding_constraints.append(BindingConstraint.MAX_USD)
                else:
                    rejection_reasons.append(
                        f"Max loss ${estimated_loss_at_stop:.0f} exceeds ${max_loss_per_trade_usd:.0f} per trade "
                        f"(stop distance {stop_distance_pct:.2%}, even min notional would lose too much)"
                    )

        # Determine Effective Leverage for monitoring ONLY
        effective_leverage = position_notional / account_equity
        if effective_leverage > requested_leverage:
             rejection_reasons.append(
                f"Effective leverage {effective_leverage:.2f}× exceeds max {requested_leverage}×"
            )

        margin_required = position_notional / requested_leverage
        leverage = requested_leverage
        
        logger.debug(
            "Position sizing calculated",
            position_notional=str(position_notional),
            leverage=str(leverage),
            margin_required=str(margin_required),
            stop_distance_pct=str(stop_distance_pct),
            using_futures_prices=bool(futures_entry_price),
        )
        
        # Calculate liquidation buffer (if we have exchange-reported liq price)
        liquidation_buffer_pct = Decimal("0")
        if exchange_liquidation_price:
            liquidation_buffer_pct = self._calculate_liquidation_distance(
                perp_mark_price,
                exchange_liquidation_price,
                signal.signal_type.value,
            )
            
            min_buffer = Decimal(str(self.config.min_liquidation_buffer_pct))
            
            if liquidation_buffer_pct < min_buffer:
                rejection_reasons.append(
                    f"Liquidation buffer {liquidation_buffer_pct:.1%} < minimum {min_buffer:.1%}"
                )
        else:
            # Enforce liquidation safety using proxy checks
            # 1. Effective leverage must not be too close to max
            max_effective_leverage = requested_leverage * Decimal("0.90")  # 90% of max
            if effective_leverage > max_effective_leverage:
                rejection_reasons.append(
                    f"Effective leverage {effective_leverage:.2f}× too close to max {requested_leverage}×"
                )
            
            # 2. Require minimum free margin buffer
            min_free_margin_pct = Decimal("0.15")  # 15% safety buffer
            free_margin_pct = (account_equity - margin_required) / account_equity
            if free_margin_pct < min_free_margin_pct:
                rejection_reasons.append(
                    f"Insufficient margin buffer: {free_margin_pct:.1%} < {min_free_margin_pct:.1%}"
                )
        
        # Calculate basis divergence
        if spot_price <= 0:
            logger.error(
                "Invalid spot price for basis calculation",
                symbol=signal.symbol,
                spot_price=str(spot_price)
            )
            rejection_reasons.append("Invalid spot price (zero or negative)")
            basis_divergence_pct = Decimal("0")
        else:
            basis_divergence_pct = abs(spot_price - perp_mark_price) / spot_price
        
        # BASIS GUARD ENFORCEMENT
        # This was missing a hard check
        basis_max = Decimal(str(getattr(self.config, 'basis_max_pct', '0.0075'))) 
        if basis_divergence_pct > basis_max:
             rejection_reasons.append(
                f"Basis divergence {basis_divergence_pct:.2%} > limit {basis_max:.2%}"
            )

        # Portfolio-level limits
        should_close_existing = False
        close_symbol = None
        
        # If auction mode is enabled, ignore max_concurrent_positions and use auction_max_positions instead
        position_limit = self.config.auction_max_positions if self.config.auction_mode_enabled else self.config.max_concurrent_positions
        
        if len(self.current_positions) >= position_limit:
            # Replacement is explicitly opt-in.
            # Default behavior (especially in prod): reject when at limit (no close-then-open race).
            if not bool(getattr(self.config, "replacement_enabled", False)):
                rejection_reasons.append(
                    f"Max concurrent positions ({position_limit}) reached"
                )
            else:
                # OPPORTUNITY COST LOGIC (LEGACY / DEPRECATED)
                # NOTE: This is intentionally disabled by default. If re-enabled, it should be
                # replaced by an explicit persisted entry_score + strict guards.
            
                # 1. Calculate New Trade R:R
                if signal.take_profit:
                    new_reward = abs(signal.take_profit - signal.entry_price)
                    new_risk = abs(signal.entry_price - signal.stop_loss)
                    new_rr = new_reward / new_risk if new_risk > 0 else Decimal("0")
                else:
                    new_rr = Decimal("0")
                
                # 2. Find Weakest Existing Position
                weakest_pos = None
                lowest_rr = Decimal("9999")
            
                for pos in self.current_positions:
                    # Calculate existing R:R based on initial params (if available)
                    if pos.final_target_price and pos.initial_stop_price and pos.entry_price:
                        curr_reward = abs(pos.final_target_price - pos.entry_price)
                        curr_risk = abs(pos.entry_price - pos.initial_stop_price)
                        curr_rr = curr_reward / curr_risk if curr_risk > 0 else Decimal("0")
                        
                        if curr_rr < lowest_rr:
                            lowest_rr = curr_rr
                            weakest_pos = pos
            
                # 3. Compare
                # Threshold: New potential must be > 2.0x existing potential
                if weakest_pos and new_rr > (lowest_rr * Decimal("2.0")):
                    should_close_existing = True
                    close_symbol = weakest_pos.symbol
                    logger.info(
                        "Opportunity Cost Override Triggered",
                        new_symbol=signal.symbol,
                        new_rr=float(new_rr),
                        weakest_symbol=weakest_pos.symbol,
                        weakest_rr=float(lowest_rr),
                        multiplier=float(new_rr/lowest_rr if lowest_rr > 0 else 0)
                    )
                else:
                    rejection_reasons.append(
                        f"Max concurrent positions ({position_limit}) reached"
                    )
        
        # Daily loss limit
        daily_loss_pct = abs(self.daily_pnl) / self.daily_start_equity if self.daily_start_equity > 0 else Decimal("0")
        if self.daily_pnl < 0 and daily_loss_pct > Decimal(str(self.config.daily_loss_limit_pct)):
            rejection_reasons.append(
                f"Daily loss limit exceeded: {daily_loss_pct:.1%} > {self.config.daily_loss_limit_pct:.1%}"
            )
        
        # Time-based loss streak cooldown (NEW - prevents deadlock)
        now = datetime.now(timezone.utc)
        
        if self.cooldown_until and now < self.cooldown_until:
            remaining_minutes = int((self.cooldown_until - now).total_seconds() / 60)
            rejection_reasons.append(
                f"Loss streak cooldown active: {remaining_minutes} minutes remaining until {self.cooldown_until.strftime('%H:%M UTC')}"
            )
        
        # **REGIME-SPECIFIC VALIDATION** (NEW)
        # Different cost models for tight-stop SMC vs wide-stop structure
        regime = signal.regime  # "tight_smc" or "wide_structure"
        
        if regime == "tight_smc":
            # Tight-stop SMC (OB/FVG): 0.4-1.0% stops
            # DISABLE R:R distortion filter
            # INSTEAD: Absolute cost cap + minimum R multiple
            
            # 1. Calculate expected costs with probabilistic funding
            estimated_fees_funding = self._estimate_costs_tight_smc(position_notional, stop_distance_pct)
            
            # 2. Absolute cost cap (e.g., 25 bps max)
            cost_cap_decimal = Decimal(str(self.config.tight_smc_cost_cap_bps / 10000))  # bps to decimal
            if estimated_fees_funding > position_notional * cost_cap_decimal:
                rejection_reasons.append(
                    f"Total cost ${estimated_fees_funding:.2f} exceeds {self.config.tight_smc_cost_cap_bps:.0f} bps cap on ${position_notional:.2f} notional"
                )
            
            # 3. Minimum R:R multiple (ensure TP is far enough)
            if signal.take_profit:
                tp_distance = abs(signal.take_profit - signal.entry_price)
                stop_distance = abs(signal.stop_loss - signal.entry_price)
                rr_multiple = tp_distance / stop_distance if stop_distance > 0 else Decimal("0")
                
                min_rr = Decimal(str(self.config.tight_smc_min_rr_multiple))
                replay_rr_override = os.getenv("REPLAY_OVERRIDE_TIGHT_SMC_MIN_RR")
                if replay_rr_override is not None and replay_rr_override.strip():
                    try:
                        min_rr = Decimal(str(replay_rr_override))
                    except Exception:
                        logger.warning(
                            "Invalid REPLAY_OVERRIDE_TIGHT_SMC_MIN_RR",
                            value=replay_rr_override,
                        )
                if rr_multiple < min_rr:
                    rejection_reasons.append(
                        f"R:R multiple {rr_multiple:.1f} < minimum {min_rr:.1f} for tight-stop SMC"
                    )
            
            # For logging
            risk_amount = position_notional * stop_distance_pct
            rr_distortion = estimated_fees_funding / risk_amount if risk_amount > 0 else Decimal("0")
            max_distortion = cost_cap_decimal  # For compatibility
        
        else:
            # Wide-stop structure (BOS/TREND): 1.5-3.0% stops
            # KEEP R:R distortion filter (existing logic)
            
            estimated_fees_funding = self._estimate_costs_wide_structure(position_notional, stop_distance_pct)
            
            # Cost-aware R:R distortion
            risk_amount = position_notional * stop_distance_pct
            rr_distortion = estimated_fees_funding / risk_amount if risk_amount > 0 else Decimal("0")
            
            # Use regime-specific distortion limit (e.g., 15%)
            max_distortion = Decimal(str(self.config.wide_structure_max_distortion_pct))
            
            if rr_distortion > max_distortion:
                rejection_reasons.append(
                    f"Fees+funding distort R:R by {rr_distortion:.1%} > max {max_distortion:.1%} (Stop: {stop_distance_pct:.2%})"
                )
            # Hard funding gate for prolonged contango burden in wide-structure regime.
            daily_funding_bps = Decimal(str(self.config.funding_rate_daily_bps))
            wide_hold_hours = Decimal(str(self.config.wide_structure_avg_hold_hours))
            projected_funding_bps = daily_funding_bps * (wide_hold_hours / Decimal("24"))
            funding_cap_bps = Decimal(str(getattr(self.config, "wide_structure_funding_hard_cap_bps", 0.0)))
            if funding_cap_bps > 0 and projected_funding_bps > funding_cap_bps:
                rejection_reasons.append("REJECT_WIDE_FUNDING_CONTANGO")
                logger.info(
                    "Risk funding hard gate rejected",
                    symbol=signal.symbol,
                    regime=regime,
                    projected_funding_bps=float(projected_funding_bps),
                    funding_cap_bps=float(funding_cap_bps),
                )

        fee_edge_metrics = None
        if bool(getattr(self.config, "fee_edge_guard_enabled", False)):
            fee_edge_metrics = self._compute_fee_edge_metrics(
                signal=signal,
                entry_price=entry_for_risk,
                regime=regime,
            )
            edge_bps = fee_edge_metrics["edge_bps"]
            required_bps = fee_edge_metrics["required_bps"]
            fee_bps_rt = fee_edge_metrics["fee_bps_rt"]

            if edge_bps is None:
                rejection_reasons.append("REJECT_EDGE_BELOW_FEES_PLUS_FUNDING")
                logger.info(
                    "Risk fee-edge gate rejected (missing TP1 proxy)",
                    symbol=signal.symbol,
                    regime=regime,
                    fee_bps_rt=float(fee_bps_rt),
                    required_bps=float(required_bps),
                )
            elif edge_bps < required_bps:
                reason_code = (
                    "REJECT_EDGE_BELOW_FEES_PLUS_FUNDING"
                    if fee_edge_metrics["funding_bps_est"] > 0
                    else "REJECT_EDGE_BELOW_FEES"
                )
                rejection_reasons.append(reason_code)
                logger.info(
                    "Risk fee-edge gate rejected",
                    symbol=signal.symbol,
                    regime=regime,
                    edge_bps=float(edge_bps),
                    required_bps=float(required_bps),
                    fee_bps_rt=float(fee_bps_rt),
                    fees_bps_rt=float(fee_edge_metrics["fees_bps_rt"]),
                    slippage_bps_rt=float(fee_edge_metrics["slippage_bps_rt"]),
                    funding_bps_est=float(fee_edge_metrics["funding_bps_est"]),
                    edge_multiple_k=float(fee_edge_metrics["edge_multiple_k"]),
                )
            else:
                logger.debug(
                    "Risk fee-edge gate passed",
                    symbol=signal.symbol,
                    regime=regime,
                    edge_bps=float(edge_bps),
                    required_bps=float(required_bps),
                    fee_bps_rt=float(fee_bps_rt),
                )
        
        # Approve or reject
        approved = len(rejection_reasons) == 0

        # One-line explainability: why is stake this size / what bound it (one winner: final_binding_constraint)
        final_binding_constraint = binding_constraint
        auction_max_margin_util = getattr(self.config, "auction_max_margin_util", None)
        min_notional_log = min_notional_viable  # same as min_notional used in checks
        logger.debug(
            "Risk sizing binding constraint",
            symbol=signal.symbol,
            equity=str(account_equity),
            target_leverage=str(requested_leverage),
            auction_max_margin_util=auction_max_margin_util,
            min_notional=str(min_notional_log),
            computed_notional_from_risk=str(computed_notional_from_risk) if computed_notional_from_risk is not None else "n/a",
            per_position_ceiling=str(per_position_ceiling) if per_position_ceiling is not None else "n/a",
            aggregate_margin_remaining=str(aggregate_margin_remaining) if aggregate_margin_remaining is not None else "n/a",
            final_notional=str(position_notional),
            binding_constraints=[c.value for c in binding_constraints],
            final_binding_constraint=final_binding_constraint.value,
        )
        
        decision = RiskDecision(
            approved=approved,
            position_notional=position_notional,
            leverage=leverage,
            margin_required=margin_required,
            liquidation_buffer_pct=liquidation_buffer_pct,
            basis_divergence_pct=basis_divergence_pct,
            estimated_fees_funding=estimated_fees_funding,
            rejection_reasons=rejection_reasons,
            should_close_existing=should_close_existing,
            close_symbol=close_symbol,
            utilisation_boost_applied=utilisation_boost_applied,
        )
        
        if approved:
            logger.info(
                "Trade approved",
                symbol=signal.symbol,
                notional=str(position_notional),
                leverage=str(leverage),
            )
        else:
            logger.warning(
                "Trade rejected",
                symbol=signal.symbol,
                reasons=rejection_reasons,
                signal_score=float(signal.score) if getattr(signal, "score", None) is not None else None,
                signal_score_breakdown=getattr(signal, "score_breakdown", None) or {},
            )
            
        # --- EXPLAINABILITY INSTRUMENTATION ---
        
        # Determine strictness tier for logging
        tight_threshold = Decimal(str(self.config.tight_stop_threshold_pct))
        strictness_tier = "TIGHT" if stop_distance_pct <= tight_threshold else "NORMAL"
        
        validation_data = {
            "approved": approved,
            "reasons": rejection_reasons,
            "metrics": {
                "position_notional": float(position_notional),
                "leverage": float(leverage),
                "margin_required": float(margin_required),
                "stop_distance_pct": float(stop_distance_pct),
                "rr_distortion": float(rr_distortion),
                "liquidation_buffer_pct": float(liquidation_buffer_pct),
                "basis_divergence_pct": float(basis_divergence_pct),
            },
            "limits": {
                "max_leverage": float(self.config.max_leverage),
                "max_distortion": float(max_distortion),
                "strictness_tier": strictness_tier
            }
        }
        if fee_edge_metrics:
            validation_data["metrics"]["fee_edge_bps_rt"] = float(fee_edge_metrics["fee_bps_rt"])
            validation_data["metrics"]["fee_edge_required_bps"] = float(fee_edge_metrics["required_bps"])
            validation_data["metrics"]["fee_edge_observed_bps"] = (
                float(fee_edge_metrics["edge_bps"]) if fee_edge_metrics["edge_bps"] is not None else None
            )
        
        self._record_event("RISK_VALIDATION", signal.symbol, validation_data)
        
        return decision
    
    def _calculate_liquidation_distance(
        self,
        mark_price: Decimal,
        liq_price: Decimal,
        side: str,
    ) -> Decimal:
        """
        Calculate directional liquidation distance.
        """
        if side == "long":
            distance = (mark_price - liq_price) / mark_price
        else:  # short
            distance = (liq_price - mark_price) / mark_price
        
        return distance
    
    def _estimate_costs_tight_smc(self, position_notional: Decimal, stop_distance_pct: Decimal) -> Decimal:
        """
        Estimate costs for TIGHT-STOP SMC trades (OB/FVG).
        """
        taker_fee_bps = Decimal(str(self.config.taker_fee_bps))
        
        entry_fee = position_notional * (taker_fee_bps / Decimal("10000"))
        exit_fee = position_notional * (taker_fee_bps / Decimal("10000"))
        
        # Probabilistic funding
        avg_hold_hours = Decimal(str(self.config.tight_smc_avg_hold_hours))  # e.g., 6 hours
        funding_interval_hours = Decimal("8")  # Funding every 8 hours
        
        # Probability of paying funding = min(avg_hold / 8, 1.0)
        funding_probability = min(avg_hold_hours / funding_interval_hours, Decimal("1.0"))
        
        daily_funding_bps = Decimal(str(self.config.funding_rate_daily_bps))
        one_interval_funding = position_notional * (daily_funding_bps / Decimal("10000")) / Decimal("3")  # Daily / 3 intervals
        
        expected_funding = one_interval_funding * funding_probability
        
        total = entry_fee + exit_fee + expected_funding
        
        return total

    def _resolve_tp1_proxy(self, signal: Signal) -> Optional[Decimal]:
        """Resolve first-profit edge proxy deterministically from signal."""
        if signal.take_profit and signal.take_profit > 0:
            return signal.take_profit
        for candidate in (signal.tp_candidates or []):
            if candidate and candidate > 0:
                return candidate
        return None

    def _compute_fee_edge_metrics(self, signal: Signal, entry_price: Decimal, regime: str) -> dict:
        """
        Compute deterministic edge-vs-cost gate metrics in bps.

        edge_bps uses TP1 proxy; fee_bps_rt uses conservative fee + slippage + funding
        with a configurable cost buffer multiplier.
        """
        tp1_proxy = self._resolve_tp1_proxy(signal)
        edge_bps: Optional[Decimal] = None
        if tp1_proxy and entry_price > 0:
            edge_bps = abs(tp1_proxy - entry_price) / entry_price * Decimal("10000")

        taker_fee_bps = Decimal(str(self.config.taker_fee_bps))
        maker_fee_bps = Decimal(str(self.config.maker_fee_bps))
        conservative_taker = bool(getattr(self.config, "fee_edge_use_conservative_taker", True))
        fees_bps_rt = (
            taker_fee_bps * Decimal("2")
            if conservative_taker
            else (maker_fee_bps + taker_fee_bps)
        )
        slippage_bps_rt = Decimal(str(getattr(self.config, "fee_edge_slippage_bps_est", 4.0)))
        avg_hold_hours = Decimal(
            str(
                self.config.tight_smc_avg_hold_hours
                if regime == "tight_smc"
                else self.config.wide_structure_avg_hold_hours
            )
        )
        funding_bps_from_hold = Decimal(str(self.config.funding_rate_daily_bps)) * (
            avg_hold_hours / Decimal("24")
        )
        funding_floor_bps = Decimal(str(getattr(self.config, "fee_edge_funding_floor_bps", 2.0)))
        funding_bps_est = max(funding_bps_from_hold, funding_floor_bps)

        buffer_mult = Decimal(str(getattr(self.config, "fee_edge_cost_buffer_multiplier", 1.2)))
        fee_bps_rt = (fees_bps_rt + slippage_bps_rt + funding_bps_est) * buffer_mult
        edge_multiple_k = Decimal(str(getattr(self.config, "fee_edge_multiple_k", 5.0)))
        required_bps = fee_bps_rt * edge_multiple_k

        return {
            "edge_bps": edge_bps,
            "fee_bps_rt": fee_bps_rt,
            "required_bps": required_bps,
            "fees_bps_rt": fees_bps_rt,
            "slippage_bps_rt": slippage_bps_rt,
            "funding_bps_est": funding_bps_est,
            "edge_multiple_k": edge_multiple_k,
        }
    
    def _estimate_costs_wide_structure(self, position_notional: Decimal, stop_distance_pct: Decimal) -> Decimal:
        """
        Estimate costs for WIDE-STOP structure trades (BOS/TREND).
        """
        taker_fee_bps = Decimal(str(self.config.taker_fee_bps))
        
        entry_fee = position_notional * (taker_fee_bps / Decimal("10000"))
        exit_fee = position_notional * (taker_fee_bps / Decimal("10000"))
        
        # Multi-interval funding
        avg_hold_hours = Decimal(str(self.config.wide_structure_avg_hold_hours))  # e.g., 36 hours
        funding_interval_hours = Decimal("8")
        
        funding_intervals = avg_hold_hours / funding_interval_hours
        
        daily_funding_bps = Decimal(str(self.config.funding_rate_daily_bps))
        one_interval_funding = position_notional * (daily_funding_bps / Decimal("10000")) / Decimal("3")
        
        expected_funding = one_interval_funding * funding_intervals
        
        total = entry_fee + exit_fee + expected_funding
        
        return total
    
    def update_position_list(self, positions: List[Position]):
        """Update current positions for portfolio tracking."""
        prev_count = len(self.current_positions)
        self.current_positions = positions
        if len(positions) != prev_count:
            logger.info(f"RiskManager position count updated: {prev_count} -> {len(positions)}")
    
    def record_trade_result(self, net_pnl: Decimal, account_equity: Decimal, setup_type: Optional[str] = None):
        """
        Record trade result for daily P&L and streak tracking.
        
        CRITICAL CHANGE: Regime-aware loss streaks.
        - tight_smc: shorter tolerance (3 losses), longer pause (120m)
        - wide_structure: longer tolerance (4-5 losses), shorter pause (90m)
        
        Args:
            net_pnl: Net P&L from the trade
            account_equity: Current account equity
            setup_type: str "ob"/"fvg" (tight) or "bos"/"trend" (wide)
        """
        from src.domain.models import SetupType
        
        self.daily_pnl += net_pnl
        
        # Determine regime
        is_tight = setup_type in [SetupType.OB, SetupType.FVG] if setup_type else False
        
        # Only count MEANINGFUL losses (> X bps of equity)
        loss_bps = abs(net_pnl) / account_equity * Decimal("10000") if account_equity > 0 else Decimal("0")
        min_loss_bps = Decimal(str(self.config.loss_streak_min_loss_bps))
        
        if net_pnl < 0 and loss_bps >= min_loss_bps:
            # Loss Logic
            if is_tight:
                self.consecutive_losses_tight += 1
                limit = self.config.loss_streak_cooldown_tight
                pause_min = self.config.loss_streak_pause_minutes_tight
                msg_prefix = "Tight SMC"
            else:
                self.consecutive_losses_wide += 1
                limit = self.config.loss_streak_cooldown_wide
                pause_min = self.config.loss_streak_pause_minutes_wide
                msg_prefix = "Wide Structure"
            
            # Check Threshold
            current_streak = self.consecutive_losses_tight if is_tight else self.consecutive_losses_wide
            
            if current_streak >= limit:
                # Activate Pause
                pause_duration = timedelta(minutes=pause_min)
                self.cooldown_until = datetime.now(timezone.utc) + pause_duration
                
                logger.warning(
                    f"{msg_prefix} Streak Limit Reached - COOLDOWN ACTIVATED",
                    streak=current_streak,
                    limit=limit,
                    pause_min=pause_min,
                    cooldown_until=self.cooldown_until.isoformat()
                )
                
                # Reset ALL streaks on cooldown activation to prevent immediate re-trigger
                self.consecutive_losses_tight = 0
                self.consecutive_losses_wide = 0
        
        elif net_pnl > 0:
            # Win Logic - Reset ALL streaks
            # A win restores confidence in the system overall
            self.consecutive_losses_tight = 0
            self.consecutive_losses_wide = 0
            
            if self.cooldown_until:
                logger.info("Win recorded - clearing active cooldown early")
                self.cooldown_until = None
        
        logger.info(
            "Trade result recorded",
            net_pnl=str(net_pnl),
            daily_pnl=str(self.daily_pnl),
            streaks={
                "tight": self.consecutive_losses_tight,
                "wide": self.consecutive_losses_wide
            },
            cooldown_active=bool(self.cooldown_until),
        )
    
    def reset_daily_metrics(self, starting_equity: Decimal):
        """Reset daily metrics at start of new trading day."""
        self.daily_pnl = Decimal("0")
        self.daily_start_equity = starting_equity
        self.consecutive_losses_tight = 0
        self.consecutive_losses_wide = 0
        logger.info("Daily metrics reset", starting_equity=str(starting_equity))
