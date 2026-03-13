# Use a slim Python image to keep things fast
FROM python:3.11-slim

# Set the work directory inside the container
WORKDIR /app

# Copy the specific requirements for the producer
COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the producer code and the shared models
COPY producer/main.py .
COPY common/ ./common/

# Run the producer
CMD ["python", "main.py"]