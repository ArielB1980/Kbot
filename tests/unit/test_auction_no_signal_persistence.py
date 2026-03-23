from datetime import datetime, timezone, timedelta
from decimal import Decimal

from src.domain.models import Position, Side, Signal, SignalType, SetupType
from src.portfolio.auction_allocator import (
    AuctionAllocator,
    PortfolioLimits,
    OpenPositionMetadata,
    CandidateSignal,
)


def _make_open(symbol: str = "PF_SOLUSD", score: float = 60.0) -> OpenPositionMetadata:
    now = datetime.now(timezone.utc)
    pos = Position(
        symbol=symbol,
        side=Side.LONG,
        size=Decimal("10"),
        size_notional=Decimal("1000"),
        entry_price=Decimal("100"),
        current_mark_price=Decimal("100"),
        leverage=Decimal("5"),
        margin_used=Decimal("200"),
        unrealized_pnl=Decimal("0"),
        liquidation_price=Decimal("70"),
        opened_at=now - timedelta(hours=2),
        is_protected=True,
        stop_loss_order_id="sl-1",
    )
    return OpenPositionMetadata(
        position=pos,
        entry_time=now - timedelta(hours=2),
        entry_score=score,
        current_pnl_R=Decimal("0"),
        margin_used=Decimal("200"),
        cluster="tight_smc_ob",
        direction=Side.LONG,
        age_seconds=7200,
        is_protective_orders_live=True,
        locked=False,
        spot_symbol="SOL/USD",
    )


def _make_candidate(symbol: str = "BTC/USD", score: float = 70.0, cluster: str = "tight_smc_ob") -> CandidateSignal:
    signal = Signal(
        timestamp=datetime.now(timezone.utc),
        symbol=symbol,
        signal_type=SignalType.LONG,
        entry_price=Decimal("100"),
        stop_loss=Decimal("98"),
        take_profit=Decimal("106"),
        reasoning="auction test",
        setup_type=SetupType.OB,
        regime="tight_smc",
        higher_tf_bias="bullish",
        adx=Decimal("25"),
        atr=Decimal("2"),
        ema200_slope="up",
        score=score,
    )
    return CandidateSignal(
        signal=signal,
        score=score,
        direction=Side.LONG,
        symbol=symbol,
        cluster=cluster,
        required_margin=Decimal("100"),
        risk_R=Decimal("2"),
        position_notional=Decimal("500"),
    )


def test_no_signal_persistence_suppresses_strategic_close_before_threshold():
    allocator = AuctionAllocator(
        limits=PortfolioLimits(max_positions=1, max_margin_util=0.9, max_per_cluster=1, max_per_symbol=1),
        swap_threshold=12.0,
        no_signal_persistence_enabled=True,
        no_signal_close_persistence_cycles=3,
    )
    open_position = _make_open()

    plan = allocator.allocate(
        open_positions=[open_position],
        candidate_signals=[],
        portfolio_state={
            "account_equity": Decimal("10000"),
            "available_margin": Decimal("10000"),
            "auction_no_signal_cycles": 1,
            "auction_no_signal_persistence_enabled": True,
            "auction_no_signal_close_persistence_cycles": 3,
        },
    )

    assert plan.closes == []


def test_no_signal_persistence_allows_close_at_threshold():
    allocator = AuctionAllocator(
        limits=PortfolioLimits(max_positions=1, max_margin_util=0.9, max_per_cluster=1, max_per_symbol=1),
        swap_threshold=12.0,
        no_signal_persistence_enabled=True,
        no_signal_close_persistence_cycles=3,
    )
    open_position = _make_open()

    plan = allocator.allocate(
        open_positions=[open_position],
        candidate_signals=[],
        portfolio_state={
            "account_equity": Decimal("10000"),
            "available_margin": Decimal("10000"),
            "auction_no_signal_cycles": 3,
            "auction_no_signal_persistence_enabled": True,
            "auction_no_signal_close_persistence_cycles": 3,
        },
    )

    assert plan.closes == [open_position.position.symbol]


def test_no_signal_persistence_canary_scope():
    allocator = AuctionAllocator(
        limits=PortfolioLimits(max_positions=1, max_margin_util=0.9, max_per_cluster=1, max_per_symbol=1),
        swap_threshold=12.0,
        no_signal_persistence_enabled=True,
        no_signal_close_persistence_cycles=3,
    )
    open_position = _make_open(symbol="PF_SOLUSD")

    plan = allocator.allocate(
        open_positions=[open_position],
        candidate_signals=[],
        portfolio_state={
            "account_equity": Decimal("10000"),
            "available_margin": Decimal("10000"),
            "auction_no_signal_cycles": 1,
            "auction_no_signal_persistence_enabled": True,
            "auction_no_signal_close_persistence_cycles": 3,
            "auction_no_signal_persistence_canary_symbols": ["PF_ETHUSD"],
        },
    )

    # Not in canary list => legacy close behavior remains.
    assert plan.closes == [open_position.position.symbol]


def test_rebalancer_reductions_still_planned_when_strategic_close_suppressed():
    allocator = AuctionAllocator(
        limits=PortfolioLimits(max_positions=2, max_margin_util=0.9, max_per_cluster=2, max_per_symbol=1),
        swap_threshold=12.0,
        no_signal_persistence_enabled=True,
        no_signal_close_persistence_cycles=3,
        rebalancer_enabled=True,
        rebalancer_trigger_pct_equity=0.32,
        rebalancer_clear_pct_equity=0.24,
        rebalancer_max_reductions_per_cycle=1,
    )
    open_position = _make_open()
    open_position.position.size_notional = Decimal("5000")  # 50% of 10k equity, above trigger
    open_position.position.size = Decimal("100")
    open_position.position.margin_used = Decimal("1000")

    plan = allocator.allocate(
        open_positions=[open_position],
        candidate_signals=[],
        portfolio_state={
            "account_equity": Decimal("10000"),
            "available_margin": Decimal("10000"),
            "auction_no_signal_cycles": 1,
            "auction_no_signal_persistence_enabled": True,
            "auction_no_signal_close_persistence_cycles": 3,
            "current_cycle": 10,
            "last_trim_cycle_by_symbol": {},
        },
    )

    # Strategic close is suppressed, but reduceOnly trim planning still happens.
    assert plan.closes == []
    assert len(plan.reductions) == 1
    assert plan.reductions[0][0] == open_position.position.symbol


def test_stricter_swap_threshold_rejects_marginal_replacement():
    allocator = AuctionAllocator(
        limits=PortfolioLimits(max_positions=1, max_margin_util=0.9, max_per_cluster=1, max_per_symbol=1),
        swap_threshold=12.0,
    )
    open_position = _make_open(score=60.0)
    candidate = _make_candidate(score=71.0)  # gap 11, below threshold 12

    plan = allocator.allocate(
        open_positions=[open_position],
        candidate_signals=[candidate],
        portfolio_state={
            "account_equity": Decimal("10000"),
            "available_margin": Decimal("10000"),
        },
    )

    assert plan.closes == []
    assert plan.opens == []


def test_portfolio_state_swap_threshold_override_rejects_marginal_replacement():
    allocator = AuctionAllocator(
        limits=PortfolioLimits(max_positions=1, max_margin_util=0.9, max_per_cluster=1, max_per_symbol=1),
        swap_threshold=10.0,
    )
    open_position = _make_open(score=60.0)
    candidate = _make_candidate(score=71.0)  # gap 11 (would pass at threshold 10)

    plan = allocator.allocate(
        open_positions=[open_position],
        candidate_signals=[candidate],
        portfolio_state={
            "account_equity": Decimal("10000"),
            "available_margin": Decimal("10000"),
            "auction_swap_threshold": 12.0,
        },
    )

    assert plan.closes == []
    assert plan.opens == []


def test_portfolio_state_min_hold_override_locks_recent_positions():
    allocator = AuctionAllocator(
        limits=PortfolioLimits(max_positions=1, max_margin_util=0.9, max_per_cluster=1, max_per_symbol=1),
        swap_threshold=10.0,
        min_hold_minutes=15,
    )
    open_position = _make_open(score=60.0)
    open_position.age_seconds = 50 * 60  # Older than base min hold, younger than override.
    candidate = _make_candidate(score=90.0)

    plan = allocator.allocate(
        open_positions=[open_position],
        candidate_signals=[candidate],
        portfolio_state={
            "account_equity": Decimal("10000"),
            "available_margin": Decimal("10000"),
            "auction_chop_active_symbols": ["SOL/USD"],
            "auction_chop_min_hold_minutes": 60,
        },
    )

    assert plan.closes == []
    assert plan.opens == []


def test_high_conviction_min_hold_override_locks_recent_positions():
    allocator = AuctionAllocator(
        limits=PortfolioLimits(max_positions=1, max_margin_util=0.9, max_per_cluster=1, max_per_symbol=1),
        swap_threshold=10.0,
        min_hold_minutes=45,
    )
    open_position = _make_open(score=60.0)
    open_position.age_seconds = 70 * 60  # Older than base hold, younger than high-conviction hold.
    open_position.conviction = 63.0
    candidate = _make_candidate(score=90.0)

    plan = allocator.allocate(
        open_positions=[open_position],
        candidate_signals=[candidate],
        portfolio_state={
            "account_equity": Decimal("10000"),
            "available_margin": Decimal("10000"),
            "auction_min_hold_high_conviction_minutes": 120,
            "auction_min_hold_high_conviction_threshold": 50.0,
        },
    )

    assert plan.closes == []
    assert plan.opens == []


def test_pnl_positive_lock_keeps_profitable_position_during_extra_window():
    allocator = AuctionAllocator(
        limits=PortfolioLimits(max_positions=1, max_margin_util=0.9, max_per_cluster=1, max_per_symbol=1),
        swap_threshold=10.0,
        min_hold_minutes=15,
    )
    open_position = _make_open(score=60.0)
    open_position.age_seconds = 20 * 60  # Past base hold, inside extra lock window.
    open_position.current_pnl_R = Decimal("0.8")
    candidate = _make_candidate(score=90.0)

    plan = allocator.allocate(
        open_positions=[open_position],
        candidate_signals=[candidate],
        portfolio_state={
            "account_equity": Decimal("10000"),
            "available_margin": Decimal("10000"),
            "auction_pnl_positive_lock_enabled": True,
            "auction_pnl_positive_lock_max_minutes": 60,
        },
    )

    assert plan.closes == []
    assert plan.opens == []


def test_pnl_positive_lock_expires_after_max_window():
    allocator = AuctionAllocator(
        limits=PortfolioLimits(max_positions=1, max_margin_util=0.9, max_per_cluster=1, max_per_symbol=1),
        swap_threshold=10.0,
        min_hold_minutes=15,
    )
    open_position = _make_open(score=60.0)
    open_position.age_seconds = 80 * 60  # Past base hold and extra lock cap.
    open_position.current_pnl_R = Decimal("0.8")
    candidate = _make_candidate(score=90.0)

    plan = allocator.allocate(
        open_positions=[open_position],
        candidate_signals=[candidate],
        portfolio_state={
            "account_equity": Decimal("10000"),
            "available_margin": Decimal("10000"),
            "auction_pnl_positive_lock_enabled": True,
            "auction_pnl_positive_lock_max_minutes": 60,
        },
    )

    assert plan.closes == [open_position.position.symbol]
    assert [signal.symbol for signal in plan.opens] == ["BTC/USD"]


def test_chop_min_hold_applies_only_to_active_symbols():
    allocator = AuctionAllocator(
        limits=PortfolioLimits(max_positions=1, max_margin_util=0.9, max_per_cluster=1, max_per_symbol=1),
        swap_threshold=10.0,
        min_hold_minutes=15,
    )
    open_position = _make_open(symbol="PF_ETHUSD", score=60.0)
    open_position.spot_symbol = "ETH/USD"
    open_position.age_seconds = 20 * 60  # Above base min hold, below chop override.
    candidate = _make_candidate(symbol="BTC/USD", score=90.0)

    plan = allocator.allocate(
        open_positions=[open_position],
        candidate_signals=[candidate],
        portfolio_state={
            "account_equity": Decimal("10000"),
            "available_margin": Decimal("10000"),
            "auction_chop_active_symbols": ["SOL/USD"],
            "auction_chop_min_hold_minutes": 60,
        },
    )

    # ETH is not chop-active, so base min hold applies and replacement can proceed.
    assert plan.closes == [open_position.position.symbol]
    assert [s.symbol for s in plan.opens] == ["BTC/USD"]


def test_chop_swap_threshold_applies_only_to_active_symbols():
    allocator = AuctionAllocator(
        limits=PortfolioLimits(max_positions=1, max_margin_util=0.9, max_per_cluster=1, max_per_symbol=1),
        swap_threshold=10.0,
    )
    open_position = _make_open(symbol="PF_SOLUSD", score=60.0)
    open_position.spot_symbol = "SOL/USD"
    candidate = _make_candidate(symbol="BTC/USD", score=71.0)  # Gap 11

    plan = allocator.allocate(
        open_positions=[open_position],
        candidate_signals=[candidate],
        portfolio_state={
            "account_equity": Decimal("10000"),
            "available_margin": Decimal("10000"),
            "auction_swap_threshold": 10.0,
            "auction_chop_swap_threshold": 12.0,
            "auction_chop_active_symbols": ["SOL/USD"],
        },
    )

    # SOL is chop-active, so stricter threshold (12) applies and rejects replacement.
    assert plan.closes == []
    assert plan.opens == []
