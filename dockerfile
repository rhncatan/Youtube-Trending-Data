# Use official Python 3.11 image
FROM python:3.11.5-slim

# Set working directory inside container
WORKDIR /app

# Install system dependencies (if needed for pandas, sqlite, etc.)
RUN apt-get update && apt-get install -y \
    build-essential \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (better for caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app
COPY . .

# Default command (can override in docker-compose or docker run)
CMD ["python","scripts/Watcher.py"]
