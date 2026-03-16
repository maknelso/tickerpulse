# Dockerfile tells DOcker how to build a single isoalted environment (image)
# Step by step list of instructions to create a "Virtual Computer" that contains exactly what your code needs to run

# Use a lighweight Python image
FROM python:3.11-slim

# The Fix: Update system packages to patch known vulnerabilities
# We do this first so it stays in a cached layer
RUN apt-get update && apt-get upgrade -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Set the working directory inside the container 
# Set /app as the curr directionry inside the container
# once you build this virtual computer, create a folder named /app and make that the starting pt for everything

# From Your Laptop (VS Code),   To Inside the Container
# common/models.py, /app/common/models.py

# by creating /app, you are giving your project its own clean, isolated room
# we almost always see WORKDIR /app or WORKDIR /src
WORKDIR /app

# 1. Install system-level build tools if needed (minimal)
RUN pip install --upgrade pip

# 2. Copy ONLY the dependency manifest first (this makes rebuilding faster)
# COPY <source> <destination> (source is my mac), destination is container
COPY pyproject.toml .

# 3. Install dependencies from pyproject.toml
# We use --no-root because our code isn't inside the container yet.
# This downloads all the heavy libraries (Pydantic, etc.) into a cached layer.
RUN pip install . --no-root || pip install pydantic "faststream[kafka]" structlog python-dotenv httpx

#! Most cirtical part of build recipe
# transfroms a pile of files on laptop into a fx microservice inside a container
# Copy the shared models and producer code
# Remember our context in docker-compose is the root folder

# Command tells Docker to grab `common` folder (where models.py) lives, mirror it inside the container's virtual /app folder
# main.py needs StockTicker data model 
COPY common/ ./common/
COPY producer/ ./producer/

# install all packages listed in requirements.txt, --no-cache-dir tells pip not to store dl packages caches
# requirements.txt is copied first to help DOcker build cachingbecause dependencies rarely change, source code changes often
# Docker can reuse layer when packages were installed instead of reinstalling them every build
#! Install project in "editable" or "standard" mode
# this reads the dependencies from pyproject.toml automatically
# Use -e (editable mode) to link the folders quickly 
# and avoid the 'Multiple top-level packages' wheel build error.
RUN pip install -e .

#! Arguably most important line for Py developer
# By default, Py only looks for imports in the folder it's currently in
# This adds /app folder to Python's "Search Path"
# when code says from common.models import StockTicker, Py now thinks is there a common folder in my curr folder? No, is there a common folder in PYTHONPATH /app? Yes!
# Tell Python to look in /app for imports
# THis allows 'from common.models import StockTicker' to work
ENV PYTHONPATH=/app

# Start the worker

CMD ["python", "-m", "producer.main"]