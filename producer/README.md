Find information and send it to Kafka. Talks to the outside world via massive API.

Workflow:
1. Fetch
2. Validate
3. Ship -> put this in ticker_prices slot

Manages the Massive SDK (RESTClient) which creates its own network connections and clean handshake to close.

Exponential Backoff - waiting longer and longer between retries to give failing service room to breathe.
Strcutured Logging (JSON) - allows us to query logs like a DB
Graceful Resource cleanup - RESTClient and KafkaBroker are closed properly when container shuts down to avoid memory 
    leaks or hanging connections.