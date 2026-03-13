import os
import asyncio
from faststream import FastStream
from faststream.kafka import KafkaBroker
from common.models import StockTick
# Use 'massive' instead of 'massive_api'
from massive import RESTClient

# Perspective: Inside Docker, we use service name 'kafka'
# When running inside Docker, services talk to each other using services names (like kafka) instead of localhost
# running locally -> localhost:9092
# running inside Docker -> kafka:9092 (set via end var)
# we mapped the port with -p 9092:9092 when running Kafka container

"""
It sits and watches the `stock_ticks` bin
Every time a new box arrives ,it opens it, looks at the price and says if it's over $95, it prints an alert
"""

"""
your Python script (localhost:9092)
        ↓
Docker port mapping (-p 9092:9092)
        ↓
Kafka container (port 9092 inside Docker)

-p 9092:9092 tells Docker: anything hitting port 9092 on my Mac should be forwarded to container's port 9092.
    - left 9092 -> port on your Mac
    - right 9092 -> port inside the container
    - -p punches a hole through Docker's network isolation to make service accessible from outside the container
From Py's perspective, it talks to localhost:9092 like Kafka is running natively - Docker handles the forwarding.
-p (publish) flag is what bridges machine and container
"""
KAFKA_URL = os.getenv("KAFKA_URL", "localhost:9092")

# Create a Kafka broker connection object pointing at that URL
# FastStream's abstraction over KafkaJS - represents TCP connection to Kafka instance
broker = KafkaBroker(KAFKA_URL)

# Create FastStream app and wires the broker into it
# FastStream is Python's async messaging platform - FastAPI but for message queues instead of HTTP
# `app` object is what you use to define consumers (subscribers) and start the worker
app = FastStream(broker)

"""
alert_engine.py (FastStream worker)
        ↓ connects via TCP
    Kafka broker
        ↓ reads messages from topic
    does something with them (alerts, notifications, etc.)
"""

# This is a Consumer Group
# Kafka uses this to track what messages you read.
# FastStream will automatically validate the incoming Kafka message!
# Decorator by FastStream 
# Listen to Kafka topic named stock_ticks
# fx acts as a Callback - sits idle until a new msg arrives
# `group_id="alert-processors` - Consumer Group
# messages are sent to a "Group
# @subscriber decorator - producer and worker are decoupled. Producer fires and forgets into Kafka
@broker.subscriber("stock_ticks", group_id="alert-processors")
async def monitor_prices(data: StockTick):
    print(f"📥 Validated Tick: {data.symbol} is ${data.price}")
    
    # Use dots here too!
    if data.symbol == "QLD" and data.price >= 95.00:
        print(f"🎯 ALERT: Target reached for {data.symbol}!")

    ticker = data.symbol
    price = data.price
    timestamp = data.timestamp

    print(f"📥 [Worker] Received {ticker}: ${price} (at {timestamp})")

    # Example Alert Logic
    # Since you track QLD, let's set a hypothetical 'Sell' alert
    TARGET_PRICE = 60.00

    if ticker == "QLD" and price < TARGET_PRICE:
        print(f"🚨 ALERT! {ticker} is ${price}. Target ${TARGET_PRICE} reached!")
        await send_alert_to_slack(ticker, price)
        # Here you would eventually call a function to send an email or Slack ping

if __name__ == "__main__":
    asyncio.run(app.run())