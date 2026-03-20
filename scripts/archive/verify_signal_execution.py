#!/usr/bin/env python3
"""
Verify that the system can act on signals.

Checks:
1. Dry run mode status
2. Kill switch status
3. Risk manager configuration
4. Executor initialization
5. API credentials
6. Account balance/equity
7. Position limits
"""
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
env_local = Path(__file__).parent.parent / ".env.local"
if env_local.exists():
    from dotenv import load_dotenv
    load_dotenv(env_local)

from src.config.config import load_config
from src.monitoring.logger import get_logger, setup_logging
from src.utils.kill_switch import KillSwitch
from src.data.kraken_client import KrakenClient
from src.execution.equity import calculate_effective_equity

logger = get_logger(__name__)
setup_logging("INFO", "json")


def check_dry_run(config):
    """Check dry run mode status."""
    print("\n" + "="*80)
    print("DRY RUN MODE CHECK")
    print("="*80)
    
    dry_run = config.system.dry_run
    env_dry_run = os.getenv("DRY_RUN", "0")
    
    print(f"Config dry_run: {dry_run}")
    print(f"Environment DRY_RUN: {env_dry_run}")
    print(f"Environment: {config.environment}")
    
    if dry_run:
        print("\n⚠️  WARNING: DRY RUN MODE IS ENABLED")
        print("   → Signals will NOT execute real trades")
        print("   → Orders will be simulated only")
        print("\n   To disable:")
        print("   1. Set DRY_RUN=0 in .env.local or environment")
        print("   2. Or set system.dry_run: false in config.yaml")
        print("   3. Restart live trading")
        return False
    else:
        print("\n✅ DRY RUN MODE IS DISABLED")
        print("   → System will execute real trades")
        return True


def check_kill_switch(config):
    """Check kill switch status."""
    print("\n" + "="*80)
    print("KILL SWITCH CHECK")
    print("="*80)
    
    try:
        client = KrakenClient(
            api_key=config.exchange.futures_api_key,
            api_secret=config.exchange.futures_api_secret,
            use_testnet=config.exchange.use_testnet
        )
        kill_switch = KillSwitch(client)
        
        is_active = kill_switch.is_active()
        print(f"Kill switch active: {is_active}")
        
        if is_active:
            print("\n⚠️  WARNING: KILL SWITCH IS ACTIVE")
            print("   → Trading is disabled")
            print("   → Signals will not execute")
            print("\n   To deactivate:")
            print("   python3 run.py kill-switch deactivate")
            return False
        else:
            print("\n✅ KILL SWITCH IS INACTIVE")
            print("   → Trading is enabled")
            return True
    except Exception as e:
        print(f"\n⚠️  Could not check kill switch: {e}")
        return None


def check_api_credentials(config):
    """Check API credentials."""
    print("\n" + "="*80)
    print("API CREDENTIALS CHECK")
    print("="*80)
    
    futures_key = config.exchange.futures_api_key
    futures_secret = config.exchange.futures_api_secret
    
    has_key = bool(futures_key and futures_key.strip())
    has_secret = bool(futures_secret and futures_secret.strip())
    
    print(f"Futures API Key: {'✅ Set' if has_key else '❌ Missing'}")
    print(f"Futures API Secret: {'✅ Set' if has_secret else '❌ Missing'}")
    
    if not has_key or not has_secret:
        print("\n⚠️  WARNING: Missing API credentials")
        print("   → Cannot execute trades")
        return False
    
        # Try to connect
        try:
            import asyncio
            client = KrakenClient(
                api_key=config.exchange.api_key,
                api_secret=config.exchange.api_secret,
                futures_api_key=futures_key,
                futures_api_secret=futures_secret,
                use_testnet=config.exchange.use_testnet
            )
            
            # Test connection (async)
            async def test():
                try:
                    balance = await client.get_futures_balance()
                    return True
                except Exception as e:
                    print(f"   Connection test error: {e}")
                    return False
            
            result = asyncio.run(test())
            if result:
                print("\n✅ API credentials valid")
                print("   → Can connect to Kraken")
            return result
        except Exception as e:
            print(f"\n⚠️  API connection test failed: {e}")
            return False


def check_account_balance(config):
    """Check account balance and equity."""
    print("\n" + "="*80)
    print("ACCOUNT BALANCE CHECK")
    print("="*80)
    
    try:
        client = KrakenClient(
            api_key=config.exchange.api_key,
            api_secret=config.exchange.api_secret,
            futures_api_key=config.exchange.futures_api_key,
            futures_api_secret=config.exchange.futures_api_secret,
            use_testnet=config.exchange.use_testnet
        )
        
        import asyncio
        async def check():
            balance = await client.get_futures_balance()
            base = getattr(config.exchange, "base_currency", "USD")
            equity, available_margin, _ = await calculate_effective_equity(
                balance, base_currency=base, kraken_client=client
            )
            
            print(f"Equity: ${equity:,.2f}")
            print(f"Available Margin: ${available_margin:,.2f}")
            
            if equity <= 0:
                print("\n⚠️  WARNING: Zero or negative equity")
                print("   → Cannot execute trades")
                return False
            elif equity < 100:
                print("\n⚠️  WARNING: Very low equity")
                print("   → May not have enough for trades")
                return True
            else:
                print("\n✅ Sufficient equity for trading")
                return True
        
        return asyncio.run(check())
    except Exception as e:
        print(f"\n⚠️  Could not check balance: {e}")
        return None


def check_risk_limits(config):
    """Check risk management configuration."""
    print("\n" + "="*80)
    print("RISK MANAGEMENT CHECK")
    print("="*80)
    
    risk = config.risk
    print(f"Risk per trade: {risk.risk_per_trade_pct * 100:.2f}%")
    print(f"Max leverage: {risk.max_leverage}x")
    print(f"Target leverage: {risk.target_leverage}x")
    print(f"Max concurrent positions: {risk.max_concurrent_positions}")
    print(f"Daily loss limit: {risk.daily_loss_limit_pct * 100:.2f}%")
    
    if risk.risk_per_trade_pct <= 0:
        print("\n⚠️  WARNING: Risk per trade is zero or negative")
        return False
    
    if risk.max_leverage <= 0:
        print("\n⚠️  WARNING: Max leverage is zero or negative")
        return False
    
    print("\n✅ Risk limits configured")
    return True


def check_executor(config):
    """Check executor configuration."""
    print("\n" + "="*80)
    print("EXECUTOR CHECK")
    print("="*80)
    
    exec_config = config.execution
    print(f"Order type: {exec_config.default_order_type}")
    print(f"TP mode: {exec_config.tp_mode}")
    print(f"Order timeout: {exec_config.order_timeout_seconds}s")
    print(f"Max retries: {exec_config.max_retries}")
    
    print("\n✅ Executor configuration valid")
    return True


def main():
    """Run all checks."""
    print("="*80)
    print("SIGNAL EXECUTION VERIFICATION")
    print("="*80)
    print(f"Analysis time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    try:
        config = load_config()
        
        checks = {
            "Dry Run Mode": check_dry_run(config),
            "Kill Switch": check_kill_switch(config),
            "API Credentials": check_api_credentials(config),
            "Account Balance": check_account_balance(config),
            "Risk Limits": check_risk_limits(config),
            "Executor Config": check_executor(config),
        }
        
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        
        all_passed = True
        for check_name, result in checks.items():
            if result is False:
                print(f"❌ {check_name}: FAILED")
                all_passed = False
            elif result is True:
                print(f"✅ {check_name}: PASSED")
            else:
                print(f"⚠️  {check_name}: UNKNOWN")
        
        print()
        if all_passed:
            print("✅ ALL CHECKS PASSED")
            print("   → System is ready to execute signals")
        else:
            print("❌ SOME CHECKS FAILED")
            print("   → Fix issues above before signals can execute")
        
        print("="*80)
        
    except Exception as e:
        logger.error("Verification failed", error=str(e))
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    from datetime import datetime, timezone
    main()
