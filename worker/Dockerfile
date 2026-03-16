# 1. Use a lightweight Python image
FROM python:3.11-slim

# The Fix: Update system packages to patch known vulnerabilities
# We do this first so it stays in a cached layer
RUN apt-get update && apt-get upgrade -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Upgrade pip to ensure the latest wheel support
RUN pip install --upgrade pip

# 4. Copy ONLY the dependency manifest first
# This allows Docker to cache the 'pip install' step
COPY pyproject.toml .

# 5. Pre-install dependencies from pyproject.toml
# We use --no-root because the source code isn't in the image yet.
# This creates a "Dependency Layer" that rarely changes.
RUN pip install . --no-root || pip install pydantic "faststream[kafka]" structlog python-dotenv httpx

# 6. NOW copy your actual source code folders
# Note: We copy 'common' because it contains your StockTicker models
COPY common/ ./common/
COPY worker/ ./worker/

# 7. Final install to link the local folders into the environment
# Use -e (editable mode) to link the folders quickly 
# and avoid the 'Multiple top-level packages' wheel build error.
RUN pip install -e .

# 8. Set the PYTHONPATH so the container can find the 'common' package
ENV PYTHONPATH=/app

# 9. Start the worker using the module path (-m)
# This allows 'from common.models import StockTicker' to work perfectly
CMD ["python", "-m", "worker.alert_engine"]