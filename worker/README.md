Sits quietly, wait for message to arrive and then react.

Workflow:
1. Listen - stays connected to Kafka, watching the ticker_prices slot
2. Consume
3. Act: Compares price to threshold, if it's too low or high, it triggers an alert

