FROM python:3.11-slim

WORKDIR /app

# Copy worker requirements
COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the worker code and shared models
COPY worker/alert_engine.py .
COPY common/ ./common/

# Run the alert engine
CMD ["python", "alert_engine.py"]