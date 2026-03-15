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

# Initialize Finnhub Client
load_dotenv()

# Assign the value to the variable
API_KEY = os.getenv("FINNHUB_API_KEY")
if API_KEY:
    # .strip() removes any accidental spaces or newlines from the ends
    API_KEY = API_KEY.strip()
else:
    # If this triggers, your .env isn't loading into Docker at all
    print("FINNHUB_API_KEY_NOT_FOUND_IN_ENV")
    API_KEY = ""

print(f"DEBUG: My API key is: {API_KEY}")

# Use it to initialize the client
finnhub_client = finnhub.Client(api_key=API_KEY)

# 1. Get the raw string from the environment (e.g., "AAPL,QLD")
raw_tickers = os.getenv("TICKERS", "AAPL") 

# 2. Turn it into a real Python list by splitting on the comma
TICKER_LIST = [t.strip() for t in raw_tickers.split(",") if t.strip()]

print(f"DEBUG: Ticker list initialized as: {TICKER_LIST}")

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

# 3. Initialize Clients
broker = KafkaBroker(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
app = FastStream(broker)
# We instantiate the client here but will manage its lifecycle in start_tasks
finnhub_client = finnhub.Client(api_key=API_KEY)

background_tasks: Set[asyncio.Task] = set()

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

    # We use an AsyncClient for connection pooling (better performance)
    async with httpx.AsyncClient() as client:
        while True:
            try:
                # 2. Replicate the successful 'curl' logic
                # Pass the token directly as a query parameter
                url = "https://finnhub.io/api/v1/quote"
                params = {
                    "symbol": ticker,
                    "token": API_KEY  # This is the stripped variable from earlier
                }
                
                response = await client.get(url, params=params)
                
                # Check for 401 or 429 errors explicitly
                if response.status_code != 200:
                    log.error("api_request_failed", 
                              status=response.status_code, 
                              response_text=response.text)
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

            except Exception as e:
                log.error("stream_error", error=str(e))
                await asyncio.sleep(current_delay)
                current_delay = min(current_delay * factor, max_delay)

@app.after_startup
async def start_tasks() -> None:
    """Starts a concurrent stream for every ticker defined in the environment."""
    for ticker in TICKER_LIST:
        t = ticker.strip()
        if not t: continue
        
        task = asyncio.create_task(stream_ticker(t))
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)
        logger.info("task_initialized", ticker=t)

@app.after_shutdown
async def shutdown_entities() -> None:
    """Ensures external connections are closed and tasks are cancelled."""
    logger.info("shutdown_started", active_tasks=len(background_tasks))
    
    # Stop the loop tasks immediately
    for task in background_tasks:
        # if we're in the middle of a sleep when deployment happens, container stops immediately 
        #   rather than waiting for timer to end
        task.cancel()
    
    if hasattr(client, 'close'):
        await client.close()
    
    logger.info("shutdown_complete")

if __name__ == "__main__":
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        logger.info("process_interrupted")