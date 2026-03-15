from common.models import StockTicker
from worker.alert_engine import monitor_prices
import pytest

@pytest.mark.asyncio
async def test_alert_logic_triggers():
    # Create a "fake" tick that should trigger an alert
    low_tick = StockTicker(ticker="QLD", price=55.0)
    
    # Run the function manually
    # In a real test, you'd use a 'mock' logger to verify the warning was called
    await monitor_prices(low_tick) 
    print("Checked low price - look for warning in console")

@pytest.mark.asyncio
async def test_alert_logic_stays_quiet():
    # Create a "fake" tick that is safe
    high_tick = StockTicker(ticker="QLD", price=150.0)
    
    await monitor_prices(high_tick)
    print("Checked high price - look for info log only")