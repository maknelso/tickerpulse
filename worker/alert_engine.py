import os
import asyncio
import logging
import structlog
from faststream import FastStream
from faststream.kafka import KafkaBroker

from common.models import StockTicker

"""
Only uses the Kafka Broker, which FastStreams knows how to close.

Typically stateless - processes one message at a time and moves on.
"""

# 1. Structlog Configuration (Consistent with Producer for centralized log parsing)
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# 2. Environment Configuration
# Use constants pulled from ENV so DevOps can tune the system without touching code
KAFKA_URL = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = os.environ.get("KAFKA_TOPIC_NAME", "ticker_prices")
# Default to 60.0 but allow overrides
ALERT_THRESHOLD = float(os.environ.get("ALERT_PRICE_THRESHOLD", 60.0))

# 3. Initialize Broker and App
broker = KafkaBroker(KAFKA_URL)
app = FastStream(broker)

"""
alert_engine.py (FastStream worker)
        ↓ connects via TCP
    Kafka broker
        ↓ reads messages from topic
    does something with them (alerts, notifications, etc.)
"""

# 4. The Subscriber
@broker.subscriber(TOPIC_NAME)
async def monitor_prices(data: StockTicker) -> None:
    """Monitors incoming stock prices and triggers alerts based on thresholds.

    This worker acts as a 'Consumer' in the Kafka ecosystem. It uses Pydantic
    to ensure the incoming message matches our 'StockTicker' contract.

    Args:
        data: The validated StockTicker object from the Kafka message.
    """

    if data.price <= 0:
        # We don't want to trigger alerts for 0 or negative values 
        # that often come from API 'Not Found' responses.
        return

    # Contextual Logging: Bind the ticker to all logs in this function call
    log = logger.bind(
        ticker=data.ticker, 
        current_price=data.price,
        threshold=ALERT_THRESHOLD
    )

    log.info("tick_received")

    # Business Logic: Check against threshold
    if data.price < ALERT_THRESHOLD:
        # In a real 'Big Tech' app, you might trigger a PagerDuty alert, 
        # send a Slack message, or call a specialized Notification Service here.
        log.warning(
            "price_alert_triggered", 
            message="Asset has breached the lower price bound"
        )

# 5. Lifecycle Hooks
@app.after_startup
async def announce_startup() -> None:
    """Logs worker configuration at startup for debugging."""
    logger.info(
        "worker_started", 
        topic=TOPIC_NAME, 
        threshold=ALERT_THRESHOLD,
        broker=KAFKA_URL
    )

if __name__ == "__main__":
    try:
        # app.run() handles the underlying asyncio loop for the broker
        asyncio.run(app.run())
    except KeyboardInterrupt:
        logger.info("worker_shutdown_requested")
    except Exception as e:
        logger.critical("worker_crashed", error=str(e), exc_info=True)