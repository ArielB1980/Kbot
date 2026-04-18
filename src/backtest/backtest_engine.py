"""
Backtesting engine for SMC strategy on historical spot data.

Simulates futures execution with realistic costs.
"""
from datetime import datetime, timezone, timedelta
import asyncio
from decimal import Decimal
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from src.config.config import Config
from src.data.kraken_client import KrakenClient
from src.strategy.smc_engine import SMCEngine
from src.risk.risk_manager import RiskManager
from src.risk.basis_guard import BasisGuard
from src.exceptions import OperationalError, DataError
from src.domain.models import Candle, Signal, SignalType, Position, Trade, Side
from src.storage.repository import save_candle, save_trade
from src.storage.db import init_db
from src.monitoring.logger import get_logger

logger = get_logger(__name__)


from src.execution.execution_engine import ExecutionEngine

@dataclass
class BacktestMetrics:
    """Performance metrics for backtest."""
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_pnl: Decimal = Decimal("0")
    total_fees: Decimal = Decimal("0")
    max_drawdown: Decimal = Decimal("0")
    win_rate: float = 0.0
    avg_win: Decimal = Decimal("0")
    avg_loss: Decimal = Decimal("0")
    profit_factor: float = 0.0
    sharpe_ratio: float = 0.0
    
    # Tracking
    equity_curve: List[Decimal] = field(default_factory=list)
    peak_equity: Decimal = Decimal("0")
    
    # NEW: Extended metrics for portfolio analysis
    max_concurrent_positions: int = 0  # Peak concurrent positions filled
    calmar_ratio: float = 0.0  # PnL / Max Drawdown (risk-adjusted return)
    trade_results: List[Decimal] = field(default_factory=list)  # Individual trade PnLs for correlation
    trade_timestamps: List[datetime] = field(default_factory=list)  # Trade close times
    trade_symbols: List[str] = field(default_factory=list)  # Symbols for each trade
    loss_correlation: float = 0.0  # Correlation coefficient of consecutive losses
    
    # Per-trade detail tracking
    trade_entry_times: List[datetime] = field(default_factory=list)
    trade_regimes: List[str] = field(default_factory=list)
    trade_sides: List[str] = field(default_factory=list)
    exit_reasons: List[str] = field(default_factory=list)
    
    # Computed aggregate metrics
    max_consecutive_losses: int = 0
    max_consecutive_wins: int = 0
    avg_holding_hours: float = 0.0
    median_holding_hours: float = 0.0
    
    # Runner-specific metrics
    tp1_fills: int = 0
    tp2_fills: int = 0
    tp1_pnl: Decimal = Decimal("0")
    tp2_pnl: Decimal = Decimal("0")
    runner_exits: int = 0
    runner_pnl: Decimal = Decimal("0")
    runner_r_multiples: List[float] = field(default_factory=list)
    runner_avg_r: float = 0.0
    runner_exits_beyond_3r: int = 0
    runner_max_r: float = 0.0
    
    def update(self):
        """Update calculated metrics."""
        if self.total_trades > 0:
            self.win_rate = (self.winning_trades / self.total_trades) * 100
        
        # Calmar ratio
        if self.max_drawdown > 0:
            self.calmar_ratio = float(self.total_pnl) / float(self.max_drawdown * Decimal("100"))
        elif float(self.total_pnl) > 0:
            self.calmar_ratio = float("inf")
        
        self._calculate_loss_correlation()
        
        # Avg win/loss and profit factor
        wins = [r for r in self.trade_results if r > 0]
        losses = [r for r in self.trade_results if r < 0]
        if wins:
            self.avg_win = sum(wins) / len(wins)
        if losses:
            self.avg_loss = sum(losses) / len(losses)
        if losses and self.avg_loss != 0 and wins:
            self.profit_factor = float(abs(sum(wins)) / abs(sum(losses)))
        
        # Sharpe ratio (annualized from daily equity curve returns)
        self._calculate_sharpe_ratio()
        
        # Consecutive wins/losses
        self._calculate_streaks()
        
        # Holding time
        self._calculate_holding_times()
        
        # Runner metrics
        if self.runner_r_multiples:
            self.runner_avg_r = sum(self.runner_r_multiples) / len(self.runner_r_multiples)
            self.runner_exits_beyond_3r = sum(1 for r in self.runner_r_multiples if r > 3.0)
            self.runner_max_r = max(self.runner_r_multiples)
    
    def _calculate_loss_correlation(self):
        """
        Calculate correlation of consecutive losses.
        High positive correlation = losses tend to cluster (bad)
        Near zero = losses are independent (neutral)
        Negative = losses tend to alternate with wins (good)
        """
        if len(self.trade_results) < 3:
            self.loss_correlation = 0.0
            return
        
        # Create binary sequence: 1 = loss, 0 = win/breakeven
        loss_sequence = [1 if r < 0 else 0 for r in self.trade_results]
        
        # Calculate autocorrelation at lag 1
        n = len(loss_sequence)
        mean = sum(loss_sequence) / n
        
        if mean == 0 or mean == 1:
            self.loss_correlation = 0.0  # All wins or all losses
            return
        
        # Covariance of sequence with itself shifted by 1
        variance = sum((x - mean) ** 2 for x in loss_sequence) / n
        if variance == 0:
            self.loss_correlation = 0.0
            return
        
        covariance = sum((loss_sequence[i] - mean) * (loss_sequence[i+1] - mean) for i in range(n-1)) / (n-1)
        self.loss_correlation = covariance / variance 

    def _calculate_sharpe_ratio(self):
        """Annualized Sharpe from daily equity curve snapshots."""
        if len(self.equity_curve) < 3:
            self.sharpe_ratio = 0.0
            return
        daily_returns = []
        for i in range(1, len(self.equity_curve)):
            prev = float(self.equity_curve[i - 1])
            if prev > 0:
                daily_returns.append((float(self.equity_curve[i]) - prev) / prev)
        if not daily_returns:
            self.sharpe_ratio = 0.0
            return
        mean_ret = sum(daily_returns) / len(daily_returns)
        variance = sum((r - mean_ret) ** 2 for r in daily_returns) / len(daily_returns)
        std_ret = variance ** 0.5
        if std_ret == 0:
            self.sharpe_ratio = 0.0
            return
        self.sharpe_ratio = (mean_ret / std_ret) * (365 ** 0.5)

    def _calculate_streaks(self):
        """Max consecutive wins and losses."""
        max_w = max_l = cur_w = cur_l = 0
        for r in self.trade_results:
            if r > 0:
                cur_w += 1
                cur_l = 0
            elif r < 0:
                cur_l += 1
                cur_w = 0
            else:
                cur_w = cur_l = 0
            max_w = max(max_w, cur_w)
            max_l = max(max_l, cur_l)
        self.max_consecutive_wins = max_w
        self.max_consecutive_losses = max_l

    def _calculate_holding_times(self):
        """Average and median holding time in hours."""
        if len(self.trade_timestamps) != len(self.trade_entry_times):
            return
        if not self.trade_timestamps:
            return
        hours = []
        for entry_t, exit_t in zip(self.trade_entry_times, self.trade_timestamps):
            h = (exit_t - entry_t).total_seconds() / 3600.0
            if h >= 0:
                hours.append(h)
        if hours:
            self.avg_holding_hours = sum(hours) / len(hours)
            sorted_h = sorted(hours)
            n = len(sorted_h)
            if n % 2 == 0:
                self.median_holding_hours = (sorted_h[n // 2 - 1] + sorted_h[n // 2]) / 2
            else:
                self.median_holding_hours = sorted_h[n // 2]


class BacktestEngine:
    """
    Backtest engine for SMC strategy.
    
    Workflow:
    1. Fetch historical spot data (multiple timeframes)
    2. Replay chronologically
    3. Generate signals on each new candle
    4. Simulate futures fills with realistic costs
    5. Track performance
    """
    
    def __init__(self, config: Config, symbol: Optional[str] = None, starting_equity: Optional[Decimal] = None):
        """
        Initialize backtest engine.
        
        Args:
            config: System configuration
            symbol: Trading symbol (e.g., "BTC/USD", "ETH/USD"). If None, uses first spot market.
            starting_equity: Starting capital. If None, uses config value.
        """
        self.config = config
        
        # V2: Multi-asset support - use specified symbol or default to first spot market
        self.symbol = symbol if symbol else config.exchange.spot_markets[0]
        
        # Starting capital — always Decimal for consistent arithmetic
        if starting_equity:
            self.starting_equity = Decimal(str(starting_equity)) if not isinstance(starting_equity, Decimal) else starting_equity
        else:
            self.starting_equity = Decimal(str(config.backtest.starting_equity))
        
        self.current_equity = self.starting_equity
        self.metrics = BacktestMetrics()
        self.metrics.equity_curve.append(self.starting_equity)
        self.metrics.peak_equity = self.starting_equity
        
        # Kraken client for fetching historical data
        self.client = KrakenClient(
            api_key=config.exchange.api_key,
            api_secret=config.exchange.api_secret,
            futures_api_key=config.exchange.futures_api_key,
            futures_api_secret=config.exchange.futures_api_secret,
            use_testnet=config.exchange.use_testnet
        )
        
        # Strategy and risk components
        self.smc_engine = SMCEngine(config.strategy)
        self.risk_manager = RiskManager(config.risk, liquidity_filters=config.liquidity_filters)
        self.basis_guard = BasisGuard(config.risk)
        self.execution = ExecutionEngine(config)
        
        # Backtest state
        self.position: Optional[Position] = None
        self.position_realized_pnl: Decimal = Decimal("0")
        
        # Cost assumptions
        self.taker_fee_bps = Decimal(str(config.backtest.taker_fee_bps))
        self.slippage_bps = Decimal(str(config.backtest.slippage_bps))
        
        logger.info(
            "BacktestEngine initialized",
            symbol=self.symbol,
            starting_equity=str(self.starting_equity)
        )
    
    def set_client(self, client):
        """
        Set Kraken client for data fetching.
        
        Args:
            client: KrakenClient instance
        """
        self.client = client
    
    async def run(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> BacktestMetrics:
        """Run backtest for given date range."""
        if start_date >= end_date:
            raise DataError(f"Invalid backtest window: start_date ({start_date}) must be < end_date ({end_date})")

        # Initialize Kraken client (lazy init for CCXT)
        await self.client.initialize()
        
        logger.info("Starting backtest", start=start_date, end=end_date, symbol=self.symbol)
        
        # Calculate warmup period (need ~200 days for daily EMA)
        data_start = start_date - timedelta(days=300)
        logger.info("Fetching historical data...", data_start=data_start.isoformat(), symbol=self.symbol)
        
        # Fetch data for the configured symbol
        candles_1d = await self._fetch_historical(self.symbol, "1d", data_start, end_date)
        candles_4h = await self._fetch_historical(self.symbol, "4h", data_start, end_date)
        candles_1h = await self._fetch_historical(self.symbol, "1h", data_start, end_date)
        candles_15m = await self._fetch_historical(self.symbol, "15m", data_start, end_date)

        # Enforce strict bounds on all fetched candles. If upstream provider
        # ignores `since`, we fail closed instead of leaking out-of-window data.
        candles_1d = self._normalize_candles(candles_1d, data_start, end_date)
        candles_4h = self._normalize_candles(candles_4h, data_start, end_date)
        candles_1h = self._normalize_candles(candles_1h, data_start, end_date)
        candles_15m = self._normalize_candles(candles_15m, data_start, end_date)
        timeline_1h = self._normalize_candles(candles_1h, start_date, end_date)

        logger.info(
            "Data fetched",
            symbol=self.symbol,
            count_1d=len(candles_1d),
            count_4h=len(candles_4h),
            count_1h=len(candles_1h),
            count_15m=len(candles_15m),
            timeline_1h=len(timeline_1h),
        )

        if not timeline_1h:
            raise DataError(
                f"No 1h candles in requested range for {self.symbol}: "
                f"{start_date.isoformat()} to {end_date.isoformat()}"
            )

        # Replay chronologically (use 1h as main timeline)
        for i, current_candle in enumerate(timeline_1h):
            cutoff_time = current_candle.timestamp
            
            # Historical candles for signal generation
            hist_1d = [c for c in candles_1d if c.timestamp <= cutoff_time]
            hist_4h = [c for c in candles_4h if c.timestamp <= cutoff_time]
            hist_1h = [c for c in candles_1h if c.timestamp <= cutoff_time]
            hist_15m = [c for c in candles_15m if c.timestamp <= cutoff_time]
            
            # Need enough history for indicators
            if len(hist_1d) < 200 or len(hist_1h) < 200:
                continue 
                
            # Check existing position
            if self.position:
                # Accrue funding costs (simulated every hour, applied 3x daily in reality)
                # Funding typically charged every 8 hours
                hours_since_open = (current_candle.timestamp - self.position.opened_at).total_seconds() / 3600
                funding_intervals = int(hours_since_open / 8)  # Every 8 hours
                
                if not hasattr(self.position, '_last_funding_interval'):
                    self.position._last_funding_interval = 0
                
                # Apply funding if we crossed a new 8-hour interval
                if funding_intervals > self.position._last_funding_interval:
                    intervals_to_charge = funding_intervals - self.position._last_funding_interval
                    
                    # Funding rate per 8h interval = daily_bps / 3
                    funding_rate_per_interval = Decimal(str(self.config.risk.funding_rate_daily_bps)) / Decimal("3") / Decimal("10000")
                    funding_cost = self.position.size_notional * funding_rate_per_interval * intervals_to_charge
                    
                    # Deduct from equity
                    self.current_equity -= funding_cost
                    self.metrics.total_fees += funding_cost  # Track as fee
                    
                    self.position._last_funding_interval = funding_intervals
                    
                    logger.debug(
                        "Funding cost applied",
                        intervals=intervals_to_charge,
                        cost=str(funding_cost),
                        total_fees=str(self.metrics.total_fees)
                    )
                
                # Simulate updates
                # Use current candle High/Low/Close to simulate price movement within the hour
                # Ideally we check High/Low for Exits, and Close for trailing updates?
                # Simplified: Check exits first based on High/Low.
                # If safe, update Trailing based on Close.
                
                filled = self._check_exit(self.position, current_candle)
                
                if not filled and self.position: # If still open
                     # Update State (Trailing/BE)
                     # Using candle close as 'current price' for state update
                     spot_price = current_candle.close
                     
                     # Update Peak
                     if self.position.side == Side.LONG:
                         if spot_price > (self.position.peak_price or self.position.entry_price):
                             self.position.peak_price = spot_price
                     else:
                         if spot_price < (self.position.peak_price or self.position.entry_price):
                             self.position.peak_price = spot_price
                     
                     # Check Trailing
                     if self.position.trailing_active:
                         # Calculate ATR using historical context
                         atr_val = self.smc_engine.indicators.calculate_atr(hist_1h, 14).iloc[-1]
                         current_sl = Decimal(self.position.stop_loss_order_id.split("-")[1])
                         
                         # Progressive trailing: compute R-multiple and apply tighter ATR mult if applicable
                         effective_atr_mult = None  # None = use default from config
                         risk_per_unit = getattr(self.position, '_initial_risk_per_unit', None)
                         mtp = getattr(self.config, 'multi_tp', None)
                         prog_enabled = mtp and getattr(mtp, 'progressive_trail_enabled', False)
                         
                         if prog_enabled and risk_per_unit and risk_per_unit > 0:
                             if self.position.side == Side.LONG:
                                 current_r = (spot_price - self.position.entry_price) / risk_per_unit
                             else:
                                 current_r = (self.position.entry_price - spot_price) / risk_per_unit
                             
                             prog_levels = getattr(mtp, 'progressive_trail_levels', [])
                             sorted_levels = sorted(prog_levels, key=lambda x: x.get("r_threshold", 0))
                             highest_level = getattr(self.position, '_prog_trail_level', -1)
                             
                             for idx, level in enumerate(sorted_levels):
                                 r_thresh = Decimal(str(level.get("r_threshold", 999)))
                                 if current_r >= r_thresh and idx > highest_level:
                                     self.position._prog_trail_level = idx
                                     effective_atr_mult = Decimal(str(level.get("atr_mult", 2.0)))
                             
                             # Use the highest applicable ATR mult
                             if hasattr(self.position, '_prog_trail_level') and self.position._prog_trail_level >= 0:
                                 best_level = sorted_levels[self.position._prog_trail_level]
                                 effective_atr_mult = Decimal(str(best_level.get("atr_mult", 2.0)))
                         
                         # Temporarily override execution engine's trailing ATR mult if progressive
                         original_mult = self.execution.config.trailing_atr_mult
                         if effective_atr_mult is not None:
                             self.execution.config.trailing_atr_mult = float(effective_atr_mult)
                         
                         new_sl = self.execution.check_trailing_stop(
                             self.position,
                             spot_price,
                             Decimal(str(atr_val)),
                             spot_price,
                             current_sl
                         )
                         
                         # Restore original mult
                         self.execution.config.trailing_atr_mult = original_mult
                         
                         if new_sl:
                             self.position.stop_loss_order_id = f"SL-{new_sl}"

            # Generate signal (only if no position) - 4H Decision Authority
            if not self.position:
                signal = self.smc_engine.generate_signal(
                    symbol=self.symbol,  # V2: Use configured symbol
                    regime_candles_1d=hist_1d,
                    decision_candles_4h=hist_4h,
                    refine_candles_1h=hist_1h,
                    refine_candles_15m=hist_15m,
                )
                
                # Process signal
                if signal.signal_type != SignalType.NO_SIGNAL:
                    await self._process_signal(signal, current_candle)
            
            # Update equity curve (Daily snapshot)
            if i % 24 == 0:
                self.metrics.equity_curve.append(self.current_equity)
                if self.current_equity > self.metrics.peak_equity:
                    self.metrics.peak_equity = self.current_equity
                else:
                    drawdown = (self.metrics.peak_equity - self.current_equity) / self.metrics.peak_equity
                    if drawdown > self.metrics.max_drawdown:
                        self.metrics.max_drawdown = drawdown
        
        # Close any remaining open position at end of backtest
        if self.position:
            final_candle = timeline_1h[-1] if timeline_1h else None
            if final_candle:
                logger.info(
                    "Closing open position at backtest end",
                    symbol=self.position.symbol,
                    side=self.position.side.value,
                    entry_price=str(self.position.entry_price),
                    exit_price=str(final_candle.close),
                )
                self._close_position(
                    final_candle.close,
                    "backtest_end",
                    final_candle.timestamp,
                    self.position.size
                )
        
        # Finalize metrics
        self.metrics.update()
        
        # Validation: Check for PnL/trade count consistency
        if self.metrics.total_pnl != 0 and self.metrics.total_trades == 0:
            logger.error(
                "BACKTEST CONSISTENCY ERROR: PnL recorded without trades",
                symbol=self.symbol,
                total_pnl=str(self.metrics.total_pnl),
                trades=self.metrics.total_trades,
                position_still_open=self.position is not None,
                unrealized_pnl=str(self.position_realized_pnl) if self.position_realized_pnl else "0",
            )
        
        logger.info(
            "Backtest complete",
            trades=self.metrics.total_trades,
            win_rate=f"{self.metrics.win_rate:.1f}%",
            total_pnl=str(self.metrics.total_pnl),
            max_dd=f"{self.metrics.max_drawdown:.1%}",
        )
        
        # Runner-specific summary
        if self.metrics.runner_exits > 0:
            logger.info(
                "Runner stats",
                runner_exits=self.metrics.runner_exits,
                runner_pnl=str(self.metrics.runner_pnl),
                runner_avg_r=f"{self.metrics.runner_avg_r:.2f}R",
                runner_max_r=f"{self.metrics.runner_max_r:.2f}R",
                runners_beyond_3r=self.metrics.runner_exits_beyond_3r,
                tp1_fills=self.metrics.tp1_fills,
                tp2_fills=self.metrics.tp2_fills,
                tp1_pnl=str(self.metrics.tp1_pnl),
                tp2_pnl=str(self.metrics.tp2_pnl),
                runner_pnl_pct=f"{float(self.metrics.runner_pnl) / max(float(self.metrics.total_pnl), 0.01) * 100:.1f}%",
            )
        
        return self.metrics
    
    async def _fetch_historical(
        self,
        symbol: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime,
    ) -> List[Candle]:
        """Fetch historical OHLCV data with database caching."""
        from src.storage.repository import get_candles, save_candles_bulk
        
        # 1. Attempt DB Load
        db_candles = get_candles(symbol, timeframe, start_date, end_date)
        
        total_seconds = (end_date - start_date).total_seconds()
        interval_seconds = self._timeframe_to_seconds(timeframe)
        expected_count = total_seconds / interval_seconds
        
        if len(db_candles) >= expected_count * 0.95:
            return self._normalize_candles(db_candles, start_date, end_date)
            
        logger.info("Cache miss - fetching from API", found=len(db_candles), timeframe=timeframe)

        # 2. Fetch from API (Throttled)
        candles = []
        since = int(start_date.timestamp() * 1000)
        end_ts = int(end_date.timestamp() * 1000)
        
        while since < end_ts:
            await asyncio.sleep(0.5) # Faster for backtest
            try:
                batch = await self.client.get_spot_ohlcv(symbol, timeframe, since=since, limit=720)
            except (OperationalError, DataError) as e:
                logger.warning("Rate limit hit", error=str(e), error_type=type(e).__name__)
                await asyncio.sleep(5.0)
                continue

            if not batch:
                break

            # Guard against providers returning out-of-window data.
            in_window_batch = [
                c for c in batch if start_date <= c.timestamp <= end_date
            ]
            candles.extend(in_window_batch)
            since = int(batch[-1].timestamp.timestamp() * 1000) + 1
            if in_window_batch:
                save_candles_bulk(in_window_batch)

        return self._normalize_candles(candles, start_date, end_date)

    def _normalize_candles(
        self,
        candles: List[Candle],
        start_date: datetime,
        end_date: datetime,
    ) -> List[Candle]:
        """Sort, de-dup, and strictly bound candles to requested window."""
        if not candles:
            return []

        filtered = [
            c for c in candles
            if c and c.timestamp and start_date <= c.timestamp <= end_date
        ]
        filtered.sort(key=lambda c: c.timestamp)

        deduped: List[Candle] = []
        seen_ts = set()
        for candle in filtered:
            ts = candle.timestamp
            if ts in seen_ts:
                continue
            seen_ts.add(ts)
            deduped.append(candle)
        return deduped

    def _timeframe_to_seconds(self, tf: str) -> int:
        """Helper to estimate candle count."""
        unit = tf[-1]
        value = int(tf[:-1])
        if unit == 'm': return value * 60
        if unit == 'h': return value * 3600
        if unit == 'd': return value * 86400
        return 60
    
    async def _process_signal(self, signal: Signal, current_candle: Candle):
        """Process trading signal and simulate entry."""
        futures_mark = current_candle.close
        spot_price = current_candle.close
        
        # Risk validation
        decision = self.risk_manager.validate_trade(
            signal, self.current_equity, spot_price, futures_mark
        )
        
        if not decision.approved:
            return
        
        # Execute using Engine
        plan = self.execution.generate_entry_plan(
            signal, 
            decision.position_notional, 
            spot_price, 
            futures_mark, 
            decision.leverage
        )
        
        # Simulate Entry Fill
        entry_intent = plan['entry']
        fill_price = entry_intent['price'] if entry_intent['price'] else futures_mark
        
        # Add costs
        total_cost_bps = self.taker_fee_bps + self.slippage_bps
        cost_mult = Decimal("1") + (total_cost_bps / Decimal("10000"))
        if signal.signal_type == SignalType.LONG:
            fill_price_w_cost = fill_price * cost_mult
        else:
            fill_price_w_cost = fill_price / cost_mult # Price is better for short but cost makes it worse? No, buying back higher.
            # Entry Short: Sell. Price received = Price * (1 - cost). 
            # Entry Long: Buy. Price paid = Price * (1 + cost).
            # Wait, `entry_price` in position usually tracks the "average entry price" for PnL.
            # If I sell at 100, fees deducted from margin usually, but effective price?
            # Let's track raw Fill Price and track fees separately in metrics.
            fill_price_w_cost = fill_price # Keep raw price for PnL base
        
        # Start Fee
        entry_fees = decision.position_notional * (self.taker_fee_bps / Decimal("10000"))
        self.metrics.total_fees += entry_fees
        
        # Calculate liquidation price
        maint_margin = Decimal("0.02")
        if signal.signal_type == SignalType.LONG:
             liq_price = fill_price * (Decimal("1") - (Decimal("1")/decision.leverage) + maint_margin)
        else:
             liq_price = fill_price * (Decimal("1") + (Decimal("1")/decision.leverage) - maint_margin)

        # Create TP IDs
        tp_ids = []
        for i, tp in enumerate(plan['take_profits']):
             tp_ids.append(f"TP-{i}-{tp['price']}-{tp['qty']}")
             
        sl_price = plan['stop_loss']['price']
        sl_id = f"SL-{sl_price}"

        # Calculate auction metadata
        stop_distance_pct = abs(fill_price - sl_price) / fill_price if sl_price else Decimal("0")
        from src.portfolio.auction_allocator import derive_cluster
        cluster = derive_cluster(signal)
        
        self.position = Position(
            symbol=signal.symbol,
            side=Side.LONG if signal.signal_type == SignalType.LONG else Side.SHORT,
            size=decision.position_notional / fill_price,
            size_notional=decision.position_notional,
            entry_price=fill_price,
            current_mark_price=fill_price,
            liquidation_price=liq_price,
            unrealized_pnl=Decimal("0"),
            leverage=decision.leverage,
            margin_used=decision.margin_required,
            stop_loss_order_id=sl_id,
            take_profit_order_id=None,
            tp_order_ids=tp_ids,
            trailing_active=False,
            break_even_active=False,
            peak_price=fill_price,
            opened_at=current_candle.timestamp,
            setup_type=signal.setup_type.value if hasattr(signal.setup_type, 'value') else signal.setup_type,
            regime=signal.regime,
            # Auction metadata
            entry_score=signal.score,
            cluster=cluster,
            initial_stop_distance_pct=stop_distance_pct,
            margin_used_at_entry=decision.margin_required
        )
        
        # Set runner tracking metadata
        self.position._tp_fills_count = 0
        self.position._initial_risk_per_unit = abs(fill_price - sl_price) if sl_price else None
        self.position._entry_time = current_candle.timestamp
    
    def _check_exit(self, position: Position, candle: Candle) -> bool:
        """Check if position hit stop-loss or take-profit."""
        # Check SL (High/Low)
        stop_loss = Decimal(position.stop_loss_order_id.split("-")[1])
        
        hit_sl = False
        if position.side == Side.LONG:
            if candle.low <= stop_loss:
                hit_sl = True
        else:
            if candle.high >= stop_loss:
                hit_sl = True
        
        if hit_sl:
            exit_reason = "trailing_stop" if position.trailing_active else "stop_loss"
            self._close_position(stop_loss, exit_reason, candle.timestamp, position.size)
            return True
            
        # Check TPs (Partial)
        remaining_tps = []
        hits = 0
        current_price_high = candle.high
        current_price_low = candle.low
        
        # Copy list to iterate safely
        tps = list(position.tp_order_ids)
        
        for tp_id in tps:
            parts = tp_id.split("-")
            price = Decimal(parts[2])
            qty = Decimal(parts[3])
            
            # Check if this TP is hit in this candle
            hit_tp = False
            if position.side == Side.LONG:
                if current_price_high >= price:
                    hit_tp = True
            else:
                if current_price_low <= price:
                    hit_tp = True
            
            if hit_tp:
                # Partial Close
                # Ensure we don't close more than current size
                close_qty = min(qty, position.size)
                self._close_partial(position, price, close_qty, f"tp_{parts[1]}", candle.timestamp)
                hits += 1
                position._tp_fills_count = getattr(position, '_tp_fills_count', 0) + 1
                
                 # Activate Trailing if TP1
                if parts[1] == "0":
                    if not position.trailing_active and self.config.execution.trailing_enabled:
                        position.trailing_active = True
            else:
                remaining_tps.append(tp_id)
        
        if hits > 0:
            position.tp_order_ids = remaining_tps
            # If size ~ 0, position is fully closed via TPs - record the trade
            if position.size <= Decimal("0.0001"):
                self.metrics.total_trades += 1
                if self.position_realized_pnl > 0:
                    self.metrics.winning_trades += 1
                else:
                    self.metrics.losing_trades += 1
                
                self.metrics.trade_results.append(self.position_realized_pnl)
                self.metrics.trade_timestamps.append(candle.timestamp)
                self.metrics.trade_symbols.append(position.symbol)
                self.metrics.exit_reasons.append("tp_full")
                self.metrics.trade_entry_times.append(getattr(position, '_entry_time', candle.timestamp))
                self.metrics.trade_regimes.append(getattr(position, 'regime', 'unknown') or 'unknown')
                self.metrics.trade_sides.append(position.side.value)
                
                self.risk_manager.record_trade_result(
                    self.position_realized_pnl,
                    self.current_equity,
                    setup_type=position.setup_type if hasattr(position, 'setup_type') else None
                )
                
                logger.info(
                    "Position fully closed via TPs",
                    symbol=position.symbol,
                    realized_pnl=str(self.position_realized_pnl),
                )
                
                self.position = None
                self.position_realized_pnl = Decimal("0")
                return True
                
        return False
        
    def _close_partial(self, position: Position, price: Decimal, qty: Decimal, reason: str, timestamp: datetime):
        """Close partial size."""
        if position.side == Side.LONG:
            pnl = (price - position.entry_price) * qty
        else:
            pnl = (position.entry_price - price) * qty
            
        exit_fees = (price * qty) * (self.taker_fee_bps / Decimal("10000"))
        net_pnl = pnl - exit_fees
        
        # Accumulate metrics
        self.metrics.total_fees += exit_fees
        self.metrics.total_pnl += net_pnl
        self.current_equity += net_pnl
        self.position_realized_pnl += net_pnl
        
        # Track TP-specific metrics
        if reason == "tp_0":
            self.metrics.tp1_fills += 1
            self.metrics.tp1_pnl += net_pnl
        elif reason == "tp_1":
            self.metrics.tp2_fills += 1
            self.metrics.tp2_pnl += net_pnl
        
        position.size -= qty
        position.size_notional -= (qty * position.entry_price)
        
        # Note: Do not increment trade counts for partials, only on full close
        
    def _close_position(self, exit_price: Decimal, reason: str, timestamp: datetime, size: Decimal):
        """Close remaining position."""
        if not self.position: return
        
        if self.position.side == Side.LONG:
            pnl = (exit_price - self.position.entry_price) * size
        else:
            pnl = (self.position.entry_price - exit_price) * size
            
        exit_fees = (exit_price * size) * (self.taker_fee_bps / Decimal("10000"))
        net_pnl = pnl - exit_fees
        
        # Accumulate metrics
        self.metrics.total_fees += exit_fees
        self.metrics.total_pnl += net_pnl
        self.current_equity += net_pnl
        self.position_realized_pnl += net_pnl
         
        # Final trade result based on total position PnL
        self.metrics.total_trades += 1
        if self.position_realized_pnl > 0: 
            self.metrics.winning_trades += 1
        else:
            self.metrics.losing_trades += 1
        
        # Record trade result and metadata
        self.metrics.trade_results.append(self.position_realized_pnl)
        self.metrics.trade_timestamps.append(timestamp)
        self.metrics.trade_symbols.append(self.position.symbol)
        self.metrics.exit_reasons.append(reason)
        self.metrics.trade_entry_times.append(getattr(self.position, '_entry_time', timestamp))
        self.metrics.trade_regimes.append(getattr(self.position, 'regime', 'unknown') or 'unknown')
        self.metrics.trade_sides.append(self.position.side.value)
        
        # Runner-specific metrics: if this is a runner exit (trailing stop / SL after TPs filled)
        # A runner exit = position closed after TP orders were already filled but position still had size
        is_runner_exit = (
            reason in ("trailing_stop", "stop_loss", "backtest_end")
            and hasattr(self.position, '_tp_fills_count')
            and self.position._tp_fills_count >= 2
        )
        if is_runner_exit:
            self.metrics.runner_exits += 1
            self.metrics.runner_pnl += net_pnl
            # Compute R-multiple for the runner portion
            stop_dist = getattr(self.position, '_initial_risk_per_unit', None)
            if stop_dist and stop_dist > 0:
                if self.position.side == Side.LONG:
                    r_mult = float((exit_price - self.position.entry_price) / stop_dist)
                else:
                    r_mult = float((self.position.entry_price - exit_price) / stop_dist)
                self.metrics.runner_r_multiples.append(r_mult)
            
        self.risk_manager.record_trade_result(
            self.position_realized_pnl, 
            self.current_equity,
            setup_type=self.position.setup_type if hasattr(self.position, 'setup_type') else None
        )
        self.position = None
        self.position_realized_pnl = Decimal("0")
