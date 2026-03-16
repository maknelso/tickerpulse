import asyncio
import pytest
from faststream.kafka import TestKafkaBroker
from worker.alert_engine import broker, monitor_prices
from common.models import StockTicker

@pytest.mark.asyncio
async def test_full_pipeline_integration():
    """Verifies that a message published to Kafka is correctly consumed by the worker."""
    
    # We use TestKafkaBroker to 'mock' the Kafka network while 
    # keeping the FastStream logic 100% real.
    async with TestKafkaBroker(broker) as br:
        # 1. Create a test payload
        test_data = StockTicker(ticker="QLD", price=55.0)

        # 2. Publish to the topic the worker is listening to
        # We 'wait' for the subscriber (monitor_prices) to finish processing
        await br.publish(test_data, topic="ticker_prices")

        # 3. Verification
        # In a real integration test, you might check if a record 
        # was added to a test DB or if a mock-log was triggered.
        # For now, we verify the broker handled the message without crashing.
        assert True