import os
import asyncio 
import structlog
import logging
import httpx 
from typing import Set, List
from datetime import datetime

from dotenv import load_dotenv
from faststream import FastStream
from faststream.kafka import KafkaBroker

from common.models import StockTicker

# Load env variables once
load_dotenv()

# --- Configuration & Environment ---
API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()

# Use a default ("AAPL") only if the .env variable is missing
raw_tickers = os.getenv("TICKERS", "AAPL")
TICKER_LIST = [t.strip() for t in raw_tickers.split(",") if t.strip()]

KAFKA_URL = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# 1. structlog - copied and pasted
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        # This line turns everything into clean JSON for Datadog/ELK
        structlog.processors.JSONRenderer() 
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)
logger = structlog.get_logger()

# --- Global Entities ---
broker = KafkaBroker(KAFKA_URL)
app = FastStream(broker)
background_tasks: Set[asyncio.Task] = set()

# Initialize the client globally for shared connection pooling
# We'll open/close it in the app lifecycle hooks
http_client: httpx.AsyncClient = None

# --- Business Logic ---
async def stream_ticker(ticker: str) -> None:
    """Fetches stock data using contextual logging."""
    
    # BIND the ticker to this logger instance. 
    # Now EVERY log inside this function automatically includes "ticker": "QLD"
    log = logger.bind(ticker=ticker)
    log.info("stream_started")

    # Exponential Backoff variables
    base_delay = 1
    max_delay = 60 
    factor = 2
    current_delay = base_delay

    while True:
        try:
            url = "https://finnhub.io/api/v1/quote"
            params = {"symbol": ticker, "token": API_KEY}
            
            # Use the GLOBAL http_client here to benefit from pooling
            response = await http_client.get(url, params=params)
            
            if response.status_code != 200:
                log.error("api_request_failed", status=response.status_code)
                raise Exception(f"API Error: {response.status_code}")

            res = response.json()
            current_price = res.get('c')

            if current_price is None or current_price == 0:
                log.warning("invalid_price_ignored", data=res)
                await asyncio.sleep(5)
                continue

            log.info("quote_received", 
                    price=current_price,
                    high=res.get('h'), 
                    low=res.get('l')
            )
            
            payload = StockTicker(
                ticker=ticker,
                price=current_price,
                timestamp=int(datetime.now().timestamp() * 1000)
            )

            await broker.publish(payload, topic="ticker_prices")
            log.info("tick_published", price=current_price)

            current_delay = base_delay
            await asyncio.sleep(12)

        except asyncio.CancelledError:
            log.info("task_cancelled_shutting_down")
            raise # This re-raises the CancelledError after our log
        except Exception as e:
            log.error("stream_error", error=str(e))
            await asyncio.sleep(current_delay)
            current_delay = min(current_delay * factor, max_delay)

# --- Lifecycle Hooks ---
@app.after_startup
async def startup_logic() -> None:
    global http_client
    # Open the connection pool once
    http_client = httpx.AsyncClient(timeout=10.0)
    
    for ticker in TICKER_LIST:
        task = asyncio.create_task(stream_ticker(ticker))
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)
        logger.info("task_initialized", ticker=ticker)

@app.after_shutdown
async def shutdown_logic() -> None:
    logger.info("shutdown_started", active_tasks=len(background_tasks))
    
    for task in background_tasks:
        task.cancel()
    
    # Gracefully close the shared connection pool
    if http_client:
        await http_client.aclose()
        
    logger.info("shutdown_complete")

if __name__ == "__main__":
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        logger.info("process_interrupted")