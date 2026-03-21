"""Debug script: find MagicMock leaks in LiveTrading after replay client injection."""

import os
os.environ["ENV"] = "local"
os.environ["DRY_RUN"] = "0"

from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from pathlib import Path

from src.config.config import load_config
from src.backtest.replay_harness.sim_clock import SimClock
from src.backtest.replay_harness.data_store import ReplayDataStore
from src.backtest.replay_harness.exchange_sim import ReplayKrakenClient, ExchangeSimConfig

config = load_config()
config.system.dry_run = False
config.exchange.spot_markets = ["BTC/USD:USD"]
config.exchange.futures_markets = ["BTC/USD:USD"]

clock = SimClock(start=datetime(2025, 1, 1, tzinfo=timezone.utc))
ds = ReplayDataStore(Path("data/replay"), symbols=["BTC/USD:USD"])
exchange = ReplayKrakenClient(clock=clock, data_store=ds)

def fake_ctor(*a, **kw):
    return exchange

from src.live.live_trading import LiveTrading

with patch("src.live.live_trading.KrakenClient", side_effect=fake_ctor):
    lt = LiveTrading(config)

# Check for MagicMock leaks at top level
print("\n=== MagicMock leaks on lt ===")
for attr_name in sorted(dir(lt)):
    if attr_name.startswith("_") and not attr_name.startswith("__"):
        continue
    try:
        val = getattr(lt, attr_name)
        if isinstance(val, MagicMock):
            print(f"  MOCK: lt.{attr_name}")
    except Exception:
        pass

# Check sub-components
for comp_name in ["client", "data_acq", "futures_adapter", "kill_switch",
                   "candle_manager", "execution_gateway", "executor",
                   "risk_manager", "instrument_spec_registry"]:
    comp = getattr(lt, comp_name, None)
    if comp is None:
        continue
    if isinstance(comp, MagicMock):
        print(f"  MOCK_COMPONENT: lt.{comp_name}")
        continue
    for attr_name in sorted(dir(comp)):
        if attr_name.startswith("__"):
            continue
        try:
            val = getattr(comp, attr_name)
            if isinstance(val, MagicMock):
                print(f"  MOCK: lt.{comp_name}.{attr_name}")
        except Exception:
            pass

print("\nDone.")
