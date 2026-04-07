"""
Execution Engine for converting SMC Spot Signals into Futures Orders.
Handles:
- Spot -> Futures Price Conversion
- Multi-Take Profit Generation (Structure + RR Fallback)
- Stop Loss Management (Reduce-only)
- Dynamic State Management (Break-even, Trailing Stops)
"""
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN
from typing import List, Optional, Tuple, Dict
from datetime import datetime, timezone

from src.config.config import Config
from src.domain.models import (
    Signal, SignalType, Order, OrderType, OrderStatus, 
    Side, Position
)
from src.monitoring.logger import get_logger

logger = get_logger(__name__)

class ExecutionEngine:
    def __init__(self, config: Config):
        self.config = config.execution
        self.strategy_config = config.strategy
        mtp = getattr(config, "multi_tp", None)

        self._multi_tp_config = mtp
        if mtp and getattr(mtp, "enabled", False):
            runner_has_fixed_tp = getattr(mtp, "runner_has_fixed_tp", False)
            if runner_has_fixed_tp:
                # Legacy 3-TP mode: runner gets a fixed TP order
                runner_r = getattr(mtp, "runner_tp_r_multiple", 3.0) or 3.0
                self._tp_splits = [mtp.tp1_close_pct, mtp.tp2_close_pct, mtp.runner_pct]
                self._rr_fallback_multiples = [mtp.tp1_r_multiple, mtp.tp2_r_multiple, runner_r]
            else:
                # Runner mode: only 2 TPs, runner has no TP order (trend-following)
                self._tp_splits = [mtp.tp1_close_pct, mtp.tp2_close_pct]
                self._rr_fallback_multiples = [mtp.tp1_r_multiple, mtp.tp2_r_multiple]
            self._runner_pct = mtp.runner_pct
            self._runner_has_fixed_tp = runner_has_fixed_tp
            self._regime_sizing_enabled = getattr(mtp, "regime_runner_sizing_enabled", False)
            self._regime_overrides = getattr(mtp, "regime_runner_overrides", {})
            self._hybrid_exit_mode_enabled = bool(getattr(mtp, "hybrid_exit_mode_enabled", False))
            self._hybrid_exit_canary_symbols = {
                str(s).strip().upper() for s in (getattr(mtp, "hybrid_exit_canary_symbols", []) or [])
            }
            self._hybrid_exit_regime_mode_overrides = {
                str(k).strip().lower(): str(v).strip().lower()
                for k, v in (getattr(mtp, "hybrid_exit_regime_mode_overrides", {}) or {}).items()
            }
            self._hybrid_unknown_regime_fallback = str(
                getattr(mtp, "hybrid_exit_unknown_regime_fallback_mode", "global_default")
            ).strip().lower()
            logger.info(
                "ExecutionEngine using multi_tp config",
                tp_splits=self._tp_splits,
                rr_multiples=self._rr_fallback_multiples,
                runner_pct=self._runner_pct,
                runner_has_fixed_tp=self._runner_has_fixed_tp,
                regime_sizing=self._regime_sizing_enabled,
                hybrid_exit_mode_enabled=self._hybrid_exit_mode_enabled,
            )
        else:
            self._tp_splits = list(self.config.tp_splits)
            self._rr_fallback_multiples = list(self.config.rr_fallback_multiples)
            self._runner_pct = 0.0
            self._runner_has_fixed_tp = True  # legacy: all TPs have orders
            self._regime_sizing_enabled = False
            self._regime_overrides = {}
            self._hybrid_exit_mode_enabled = False
            self._hybrid_exit_canary_symbols = set()
            self._hybrid_exit_regime_mode_overrides = {}
            self._hybrid_unknown_regime_fallback = "global_default"

        self.price_precision = Decimal("0.1")
        self.qty_precision = Decimal("0.001")

    @staticmethod
    def _normalize_regime_key(raw_regime: Optional[str]) -> str:
        if not raw_regime:
            return ""
        key = str(raw_regime).strip().lower()
        aliases = {
            "tight": "tight_smc",
            "tightsmc": "tight_smc",
            "wide": "wide_structure",
            "widestructure": "wide_structure",
            "range": "consolidation",
        }
        return aliases.get(key, key)

    @staticmethod
    def _normalize_symbol_key(raw_symbol: Optional[str]) -> str:
        if not raw_symbol:
            return ""
        return str(raw_symbol).strip().upper().split(":")[0]

    def _hybrid_canary_allows_symbol(self, symbol: Optional[str]) -> bool:
        if not self._hybrid_exit_canary_symbols:
            return True
        return self._normalize_symbol_key(symbol) in self._hybrid_exit_canary_symbols

    def _resolve_effective_runner_mode(
        self,
        signal: Signal,
    ) -> Tuple[bool, str, bool, str]:
        """
        Returns:
            (effective_runner_has_fixed_tp, effective_exit_mode, fallback_used, normalized_regime)
        """
        effective_runner_has_fixed_tp = self._runner_has_fixed_tp
        normalized_regime = self._normalize_regime_key(getattr(signal, "regime", ""))
        fallback_used = False

        if not self._hybrid_exit_mode_enabled:
            mode = "fixed_tp3" if effective_runner_has_fixed_tp else "runner"
            return effective_runner_has_fixed_tp, mode, fallback_used, normalized_regime
        if not self._hybrid_canary_allows_symbol(getattr(signal, "symbol", None)):
            mode = "fixed_tp3" if effective_runner_has_fixed_tp else "runner"
            return effective_runner_has_fixed_tp, mode, fallback_used, normalized_regime

        regime_mode = self._hybrid_exit_regime_mode_overrides.get(normalized_regime)
        if regime_mode == "fixed_tp3":
            effective_runner_has_fixed_tp = True
        elif regime_mode == "runner":
            effective_runner_has_fixed_tp = False
        else:
            fallback_used = True
            if self._hybrid_unknown_regime_fallback == "fixed_tp3":
                effective_runner_has_fixed_tp = True
            elif self._hybrid_unknown_regime_fallback == "runner":
                effective_runner_has_fixed_tp = False
            else:
                effective_runner_has_fixed_tp = self._runner_has_fixed_tp
            logger.warning(
                "Hybrid exit regime fallback used",
                symbol=getattr(signal, "symbol", None),
                regime=getattr(signal, "regime", None),
                normalized_regime=normalized_regime or None,
                fallback_mode=self._hybrid_unknown_regime_fallback,
            )

        mode = "fixed_tp3" if effective_runner_has_fixed_tp else "runner"
        return effective_runner_has_fixed_tp, mode, fallback_used, normalized_regime
        
    def generate_entry_plan(
        self, 
        signal: Signal, 
        size_notional: Decimal,
        spot_price: Decimal,
        mark_price: Decimal,
        leverage: Decimal,
        *,
        step_size: Optional[Decimal] = None,
    ) -> Dict[str, any]:
        """
        Generate complete order package: Entry + SL + TPs.
        Returns a dict of orders/intents.
        """
        # 1. Price Conversion (Spot -> Futures)
        # Ensure Decimal arithmetic — signal prices may arrive as float from backtests
        _entry = Decimal(str(signal.entry_price)) if not isinstance(signal.entry_price, Decimal) else signal.entry_price
        _stop = Decimal(str(signal.stop_loss)) if not isinstance(signal.stop_loss, Decimal) else signal.stop_loss
        # Calculate Spot percentages
        if signal.signal_type == SignalType.LONG:
            sl_pct = (_entry - _stop) / _entry
        else:
            sl_pct = (_stop - _entry) / _entry
            
        # Apply to Futures Mark Price ("Reference Price")
        # Entry assumed at current mark price for market orders
        fut_entry = mark_price
        
        if signal.signal_type == SignalType.LONG:
            fut_sl = fut_entry * (Decimal("1") - sl_pct)
        else:
            fut_sl = fut_entry * (Decimal("1") + sl_pct)
            
        (
            effective_runner_has_fixed_tp,
            effective_exit_mode,
            fallback_used,
            normalized_regime,
        ) = self._resolve_effective_runner_mode(signal)
        logger.info(
            "Execution exit mode resolved",
            symbol=signal.symbol,
            regime=getattr(signal, "regime", None),
            normalized_regime=normalized_regime or None,
            effective_exit_mode=effective_exit_mode,
            fallback_used=fallback_used,
        )

        # Build per-trade RR ladder from effective mode.
        if effective_runner_has_fixed_tp:
            if len(self._rr_fallback_multiples) >= 3:
                effective_rr_multiples = list(self._rr_fallback_multiples[:3])
            else:
                runner_r = getattr(self._multi_tp_config, "runner_tp_r_multiple", 3.0) if self._multi_tp_config else 3.0
                effective_rr_multiples = list(self._rr_fallback_multiples[:2]) + [float(runner_r or 3.0)]
        else:
            effective_rr_multiples = list(self._rr_fallback_multiples[:2])

        # 2. Regime-aware sizing: override tp_splits and runner_pct based on signal's regime
        effective_tp_splits = list(self._tp_splits)
        if effective_runner_has_fixed_tp and len(effective_tp_splits) > 3:
            effective_tp_splits = effective_tp_splits[:3]
        if not effective_runner_has_fixed_tp and len(effective_tp_splits) > 2:
            effective_tp_splits = effective_tp_splits[:2]
        effective_runner_pct = self._runner_pct
        regime_used = getattr(signal, 'regime', None)
        regime_override_key = normalized_regime or regime_used
        
        if (
            self._regime_sizing_enabled
            and not effective_runner_has_fixed_tp
            and regime_override_key
            and regime_override_key in self._regime_overrides
        ):
            override = self._regime_overrides[regime_override_key]
            effective_runner_pct = override.get("runner_pct", self._runner_pct)
            regime_tp1 = override.get("tp1_close_pct", effective_tp_splits[0] if effective_tp_splits else 0.4)
            regime_tp2 = override.get("tp2_close_pct", effective_tp_splits[1] if len(effective_tp_splits) > 1 else 0.4)
            effective_tp_splits = [regime_tp1, regime_tp2]
            logger.info(
                "Regime-aware sizing applied",
                regime=regime_used,
                tp1_pct=regime_tp1,
                tp2_pct=regime_tp2,
                runner_pct=effective_runner_pct,
            )
        
        # 2b. Generate TP Ladder (Futures prices)
        tps = self._generate_tp_ladder(
            signal,
            fut_entry,
            fut_sl,
            signal.signal_type,
            rr_multiples=effective_rr_multiples,
        )
        
        # 3. Calculate Quantities (using effective regime-adjusted splits)
        size_qty = size_notional / fut_entry
        qty_step = step_size if step_size is not None and step_size > 0 else self.qty_precision
        tp_quantities = self._split_quantities(size_qty, len(tps), effective_tp_splits, step_size=qty_step)

        # Guard: if minimum-step promotion caused TP qtys to exceed total_qty, scale down
        # proportionally so the sum never exceeds the position size.
        tp_qty_sum = sum(tp_quantities)
        if tp_qty_sum > size_qty and tp_quantities:
            scale = size_qty / tp_qty_sum
            scaled = [(q * scale).quantize(qty_step, rounding=ROUND_DOWN) for q in tp_quantities]
            # Ensure every TP still has at least one step after scaling; if not, keep original
            # (caller will need to handle the over-allocation, but at least we tried).
            if all(q >= qty_step for q in scaled):
                logger.warning(
                    "TP qty sum exceeded total_qty after minimum-step promotion; scaled down",
                    tp_qty_sum_before=str(tp_qty_sum),
                    total_qty=str(size_qty),
                    scale=str(scale),
                )
                tp_quantities = scaled

        # 4. Construct Orders (Intents only, ID generation happens at placement)
        # Entry
        entry_side = Side.LONG if signal.signal_type == SignalType.LONG else Side.SHORT
        entry_order = {
            "type": "entry",
            "side": entry_side,
            "order_type": self.config.default_order_type, # Market or Limit
            "qty": size_qty,
            "price": fut_entry if self.config.default_order_type == "limit" else None,
            "leverage": leverage
        }
        
        # Stop Loss
        sl_order = {
            "type": "stop_loss",
            "side": Side.SHORT if entry_side == Side.LONG else Side.LONG,
            "order_type": OrderType.STOP_LOSS,
            "qty": size_qty, # Full position size
            "price": fut_sl,
            "reduce_only": True,
            "trigger_price": fut_sl
        }
        
        # Take Profits
        tp_orders = []
        for i, (tp_price, tp_qty) in enumerate(zip(tps, tp_quantities)):
            tp_orders.append({
                "type": f"tp_{i+1}",
                "side": Side.SHORT if entry_side == Side.LONG else Side.LONG,
                "order_type": OrderType.TAKE_PROFIT,
                "qty": tp_qty,
                "price": tp_price,
                "reduce_only": True,
                "trigger_price": tp_price
            })
        
        # Compute final target price for runner mode trail-tightening.
        # In runner mode (no TP3 order), this is the RR level at 3.0R (or last TP if 3 TPs).
        # This price is used as a management signal, not an order.
        risk = abs(fut_entry - fut_sl)
        if not effective_runner_has_fixed_tp and len(tps) == 2:
            # Runner mode: compute final target at 3.0R as aspiration level
            final_target_r = Decimal("3.0")
            if signal.signal_type == SignalType.LONG:
                final_target_price = fut_entry + (risk * final_target_r)
            else:
                final_target_price = fut_entry - (risk * final_target_r)
        else:
            # Legacy mode: final target is the last TP price
            final_target_price = tps[-1] if tps else None
            
        return {
            "entry": entry_order,
            "stop_loss": sl_order,
            "take_profits": tp_orders,
            "metadata": {
                "fut_entry": fut_entry,
                "fut_sl": fut_sl,
                "sl_pct": sl_pct,
                "runner_pct": effective_runner_pct,
                "runner_has_fixed_tp": effective_runner_has_fixed_tp,
                "final_target_price": final_target_price,
                "regime": regime_used,
                "normalized_regime": normalized_regime,
                "effective_exit_mode": effective_exit_mode,
                "fallback_used": fallback_used,
                "effective_tp_splits": effective_tp_splits,
            }
        }
        
    def check_break_even(
        self, 
        position: Position, 
        current_mark_price: Decimal
    ) -> Optional[Decimal]:
        """
        Check if BE should be triggered.
        Returns NEW Stop Loss price if update needed, else None.
        Default trigger: TP1 Fill (checked by caller).
        """
        if position.break_even_active:
            return None # Already moved to BE
            
        # Calculate new SL
        # Offset to cover fees
        tick_size = self.price_precision 
        offset = tick_size * Decimal(str(self.config.break_even_offset_ticks))
        
        new_sl = None
        if position.side == Side.LONG:
            new_sl = position.entry_price + offset
            # Validate: must be higher than current SL (tightening)
             # We need current SL price, assumed caller has it or we look it up.
             # For now, we return candidate. Caller validates "tightening".
        else:
            new_sl = position.entry_price - offset
            
        return new_sl

    def check_trailing_stop(
        self,
        position: Position,
        current_mark_price: Decimal,
        current_atr_spot: Decimal,
        current_spot_price: Decimal,
        current_sl_price: Decimal
    ) -> Optional[Decimal]:
        """
        Calculate Trailing Stop update.
        Uses ATR from Spot applied to Futures Mark Price.
        """
        if not position.trailing_active:
            return None
            
        # 1. Convert Spot ATR to %
        atr_pct = current_atr_spot / current_spot_price
        
        # 2. Apply to Futures Mark
        atr_fut = current_mark_price * atr_pct
        trail_dist = atr_fut * Decimal(str(self.config.trailing_atr_mult))
        
        # 3. Calculate Candidate SL
        candidate_sl = None
        
        if position.side == Side.LONG:
            # Trailing from Peak High
            peak = position.peak_price if position.peak_price else current_mark_price
            candidate_sl = peak - trail_dist
            
            # Update peak if simplified logic here (caller manages state usually, 
            # but let's assume peak passed in is valid).
            
            # Validate Tightening
            # Must be > current_sl + min_step
            min_step = self.price_precision * Decimal(str(self.config.trailing_update_min_ticks))
            
            if candidate_sl > (current_sl_price + min_step):
                # Ensure we don't move SL above current price (safety, unlikely if peak is high)
                if candidate_sl < current_mark_price:
                    return candidate_sl
                    
        else: # SHORT
            # Trailing from Valley Low
            valley = position.peak_price if position.peak_price else current_mark_price
            candidate_sl = valley + trail_dist
            
            min_step = self.price_precision * Decimal(str(self.config.trailing_update_min_ticks))
            
            if candidate_sl < (current_sl_price - min_step):
                if candidate_sl > current_mark_price:
                     return candidate_sl
                     
        return None

    def _generate_tp_ladder(
        self, 
        signal: Signal, 
        fut_entry: Decimal, 
        fut_sl: Decimal, 
        side: SignalType,
        rr_multiples: Optional[List[float]] = None,
    ) -> List[Decimal]:
        """
        Generate TP levels (2 in runner mode, 3 in legacy/fixed-TP mode).
        Priority: Structure (Signal.tp_candidates) > RR Fallback.
        """
        tps = []
        candidates = signal.tp_candidates if hasattr(signal, "tp_candidates") else []
        
        # Calculate Risk (R)
        risk = abs(fut_entry - fut_sl)
        if risk == 0: risk = Decimal("1") # Edge case protection or config error
        
        multiples = rr_multiples if rr_multiples is not None else self._rr_fallback_multiples
        fallbacks = []
        for m in multiples:
            m = Decimal(str(m))
            if side == SignalType.LONG:
                fallbacks.append(fut_entry + (risk * m))
            else:
                fallbacks.append(fut_entry - (risk * m))
                
        # Merge: Use candidates where available, else fill with fallback
        # Logic: If we have 2 candidates, use them as TP1, TP2, use fallback for remainder.
        # But we need to ensure they are "progressive" (further away).
        
        final_tps = []
        
        # Dynamic slot count: 2 in runner mode (no fixed TP for runner), 3 in legacy
        num_slots = len(multiples)
        
        structure_idx = 0
        fallback_idx = 0
        
        # Sort candidates just in case (Nearest first)
        if side == SignalType.LONG:
            candidates = sorted([c for c in candidates if c > fut_entry])
        else:
            candidates = sorted([c for c in candidates if c < fut_entry], reverse=True)
            
        for i in range(num_slots):
            # Prefer structural candidate
            if structure_idx < len(candidates):
                # Candidates in Signal are SPOT prices - convert to futures
                spot_tp = candidates[structure_idx]
                # Calculate % dist from spot entry
                _ep = Decimal(str(signal.entry_price)) if not isinstance(signal.entry_price, Decimal) else signal.entry_price
                _sp = Decimal(str(spot_tp)) if not isinstance(spot_tp, Decimal) else spot_tp
                if side == SignalType.LONG:
                     dist_pct = (_sp - _ep) / _ep
                     fut_tp_candidate = fut_entry * (Decimal("1") + dist_pct)
                else:
                     dist_pct = (_ep - _sp) / _ep
                     fut_tp_candidate = fut_entry * (Decimal("1") - dist_pct)
                
                final_tps.append(fut_tp_candidate)
                structure_idx += 1
            else:
                # Use fallback - index i maps to fallback index i
                final_tps.append(fallbacks[i])
        
        # Final sort to ensure logical order (e.g. if fallback TP3 < structure TP2 somehow)
        if side == SignalType.LONG:
            final_tps.sort()
        else:
            final_tps.sort(reverse=True)
            
        return final_tps

    def _split_quantities(
        self,
        total_qty: Decimal,
        num_tps: int,
        splits_override: Optional[list] = None,
        *,
        step_size: Optional[Decimal] = None,
    ) -> List[Decimal]:
        """Split quantities according to config splits.
        
        In runner mode (2 TPs), each TP gets exactly its configured pct of total_qty.
        The implicit runner remainder is NOT included in the returned list.
        In legacy mode (3 TPs), the last TP still gets remainder to ensure exact sum.
        
        Uses Decimal.quantize with venue step_size to avoid float/ConversionSyntax issues.
        round(qty, 4) was removed - it caused decimal.ConversionSyntax in TP backfill.
        
        Args:
            splits_override: If provided, use these splits instead of self._tp_splits.
            step_size: Venue size step for quantize (from InstrumentSpec). Default: qty_precision.
        """
        splits = splits_override if splits_override is not None else self._tp_splits
        step = step_size if step_size is not None and step_size > 0 else self.qty_precision

        # Ensure splits match num_tps
        if len(splits) != num_tps:
            splits = [Decimal("1") / Decimal(str(num_tps))] * num_tps

        splits_sum = sum(Decimal(str(s)) for s in splits)
        runner_mode = splits_sum < Decimal("0.999")

        qtys = []
        if runner_mode:
            for i in range(num_tps):
                split_pct = Decimal(str(splits[i]))
                qty = (total_qty * split_pct).quantize(step, rounding=ROUND_DOWN)
                if qty <= 0:
                    qty = step
                    logger.warning(
                        "TP qty rounded to minimum step",
                        tp_index=i + 1,
                        split_pct=str(split_pct),
                        min_qty=str(step),
                    )
                qtys.append(qty)
        else:
            remaining = total_qty
            for i in range(num_tps - 1):
                split_pct = Decimal(str(splits[i]))
                qty = (total_qty * split_pct).quantize(step, rounding=ROUND_DOWN)
                qtys.append(qty)
                remaining -= qty
            qtys.append(remaining.quantize(step, rounding=ROUND_DOWN) if step > 0 else remaining)

        return qtys
