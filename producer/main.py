"""
Asynchronous Event Producer (Kafka vVersion)

Pattern:
- Fetch data from source (Massive) 
- Publish it to a bus (message bus) (Kafka) so other services can react to it 

Every 12s, it calls market API (massive client) to get latest price for a stock (e.g. QLD)
It packages the price into a small box (JSON msg) and hands it to delivery driver.

Kafka - middleman, it makes sure box gets stored in specific Topic, in our case `stock_ticks`
it holds onto the msg so even if Worker is busy or crashes, the messages are not lost.
"""

import os # interact with os - read environment var, file paths, etc.
import asyncio # built in module for async programming - run multiple tasks concurrently without blocking
from dotenv import load_dotenv # dotenv is a lib that reads a .env file and loads var into environment

# modern tools
# SDK/client lib so we odn't have to manually construct HTTP requests
#   instantiate client with credentials, call method to interact with service
from massive import RESTClient
# FastStream is a framework for building msg-driven apps. Instead of HTTP requests, services communicate by publishing and consuming
#   messages through message broker like Kafka
from faststream import FastStream
from faststream.kafka import KafkaBroker

from common.models import StockTick

# Load secrets
load_dotenv() # actually reads the .env file and loads var into environment so os.getenv() can access
API_KEY = os.getenv("MASSIVE_API_KEY")
# Inside Docker, we'll use the service name 'kafka' defined in docker-compose
KAFKA_URL = os.getenv("KAFKA_URL", "localhost:9092")

"""
Kafka is actually quite difficult to install and run manually. 
Docker allows us to download a "pre-packaged" version of Kafka that is guaranteed to work exactly the same way on your computer as it does on a server 
in the cloud.

When you run docker-compose up, you are telling your computer:

"Run a Kafka server."
"Run my Producer code in its own little bubble."
"Run my Worker code in its own little bubble."
"Connect them all to a private virtual network."
"""

# Initialize the Kafka Broker
broker = KafkaBroker(KAFKA_URL)

# Create the FastStream App (the orchestrator)
app = FastStream(broker)

# Initialize the Massive Client
client = RESTClient(API_KEY)

async def stream_ticker(ticker: str):
    """Fetches price from Massive and pushes it to Kafka."""
    print(f"Starting Kafka stream for {ticker} ...")

    while True:
        try:
            print("Fetching data...")

            # Get latest price
            trade = client.get_last_trade(ticker)

            # Create the validated object
            payload = StockTick(
                symbol=ticker,
                price=trade.price,
                timestamp=trade.timestamp
            )

            # Publish it to Kafka topic
            # FastStream automatically converts the Pydantic object to JSON for Kafka
            await broker.publish(payload, topic="stock_ticks")

            print(f"Kafka Broadcast: {ticker} @ ${trade.price}")

            # Wait 12 seconds (respects Massive's free tier 5 calls per min)
            await asyncio.sleep(12)

        except Exception as e:
            print(f"Validation or API Error: {e}")
            await asyncio.sleep(5) # wait a bit before retrying

# Create a set to keep a strong reference to your tasks
background_tasks = set()

@app.after_startup
async def start_tasks():
    # create the task
    # we use create_task so it runs in the background
    task = asyncio.create_task(stream_ticker("QLD"))

    # add it to the set so it isn't GCed
    background_tasks.add(task)

    # tell task to remove itself from set once it's done 
    # in our case, it runs forever
    task.add_done_callback(background_tasks.discard)

if __name__ == "__main__":
    # this fires up the whole engine
    asyncio.run(app.run())


