"""
Trade recording callbacks: post-trade risk updates, thesis alerts, daily loss checks.

Extracted from live_trading.py to reduce god-object size.
All functions receive a typed reference to the LiveTrading host for shared state access.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from src.exceptions import DataError, OperationalError
from src.monitoring.logger import get_logger

if TYPE_CHECKING:
    from src.live.live_trading import LiveTrading

logger = get_logger(__name__)


async def on_trade_recorded(lt: LiveTrading, position, trade) -> None:
    """
    Callback fired by ExecutionGateway after a trade is recorded.

    Updates risk manager daily PnL tracking and checks daily loss limits.
    This replaces the old save_trade_history() risk manager update path
    that was orphaned when V2 moved to trade_recorder.
    """
    try:
        from src.execution.equity import calculate_effective_equity

        net_pnl = trade.net_pnl
        setup_type = getattr(position, "setup_type", None)
        if lt.institutional_memory_manager:
            lt.institutional_memory_manager.on_trade_recorded(
                symbol=position.symbol,
                trade_id=trade.trade_id,
                net_pnl=trade.net_pnl,
                exited_at=trade.exited_at,
            )

        strategy_cfg = getattr(lt.config, "strategy", None)
        if bool(getattr(strategy_cfg, "thesis_alerts_enabled", False)):
            from src.monitoring.alert_dispatcher import fmt_price, fmt_size, send_alert

            thesis_outcome = "inconclusive"
            thesis_context = "Thesis: unavailable"
            threshold = float(
                getattr(strategy_cfg, "thesis_early_exit_threshold", 35.0)
                if strategy_cfg is not None
                else 35.0
            )
            if (
                lt.institutional_memory_manager
                and lt.institutional_memory_manager.is_enabled_for_symbol(position.symbol)
            ):
                thesis = lt.institutional_memory_manager.get_latest_thesis(position.symbol)
                if thesis:
                    conviction = float(thesis.current_conviction)
                    thesis_context = (
                        f"Thesis: {thesis.daily_bias} bias | "
                        f"Zone ${fmt_price(thesis.weekly_zone_low)}-${fmt_price(thesis.weekly_zone_high)} | "
                        f"Conviction {conviction:.1f}% ({thesis.status})"
                    )
                    if (
                        trade.net_pnl > 0
                        and conviction > threshold
                        and thesis.status in ("active", "decaying")
                    ):
                        thesis_outcome = "worked: thesis held and produced positive P&L"
                    elif conviction <= threshold or thesis.status == "invalidated":
                        thesis_outcome = "failed: conviction collapsed / thesis invalidated"
                    elif trade.net_pnl <= 0:
                        thesis_outcome = "mixed: thesis stayed live but trade closed red"
                    else:
                        thesis_outcome = "mixed: closed without clear thesis confirmation"
            else:
                if trade.net_pnl > 0:
                    thesis_outcome = "worked: positive close (no thesis snapshot)"
                elif trade.net_pnl < 0:
                    thesis_outcome = "failed: negative close (no thesis snapshot)"

            pnl_pct = Decimal("0")
            if getattr(trade, "size_notional", Decimal("0")):
                try:
                    pnl_pct = (trade.net_pnl / trade.size_notional) * Decimal("100")
                except Exception:
                    pnl_pct = Decimal("0")

            await send_alert(
                "THESIS_TRADE_CLOSED",
                f"[THESIS] Trade closed\n"
                f"Symbol: {position.symbol}\n"
                f"P&L: ${trade.net_pnl:.2f} ({pnl_pct:.2f}%)\n"
                f"Exit reason: {trade.exit_reason}\n"
                f"Entry: ${fmt_price(trade.entry_price)} | Exit: ${fmt_price(trade.exit_price)} | Size: {fmt_size(trade.size)}\n"
                f"{thesis_context}\n"
                f"Outcome: {thesis_outcome}",
                urgent=True,
            )

        # Get current equity for risk manager
        balance = await lt.client.get_futures_balance()
        base = getattr(lt.config.exchange, "base_currency", "USD")
        equity_now, _, _ = await calculate_effective_equity(
            balance, base_currency=base, kraken_client=lt.client
        )
        lt.risk_manager.record_trade_result(net_pnl, equity_now, setup_type)

        # Check if daily loss limit approached
        daily_loss_pct = (
            abs(lt.risk_manager.daily_pnl) / lt.risk_manager.daily_start_equity
            if lt.risk_manager.daily_start_equity > 0 and lt.risk_manager.daily_pnl < 0
            else Decimal("0")
        )
        if daily_loss_pct > Decimal(str(lt.config.risk.daily_loss_limit_pct * 0.7)):
            from src.monitoring.alert_dispatcher import send_alert

            limit_pct = lt.config.risk.daily_loss_limit_pct * 100
            await send_alert(
                "DAILY_LOSS_WARNING",
                f"Daily loss at {daily_loss_pct:.1%} of equity\n"
                f"Limit: {limit_pct:.0f}%\n"
                f"Daily P&L: ${lt.risk_manager.daily_pnl:.2f}",
                urgent=daily_loss_pct > Decimal(str(lt.config.risk.daily_loss_limit_pct)),
            )
    except (OperationalError, DataError, ImportError) as e:
        from src.monitoring.logger import get_logger

        logger = get_logger(__name__)
        logger.warning(
            "on_trade_recorded callback: failed to update risk manager (non-fatal)",
            error=str(e),
            error_type=type(e).__name__,
        )
