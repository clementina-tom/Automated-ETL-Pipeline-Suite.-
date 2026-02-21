# Use Python 3.12 slim image
FROM python:3.12-slim AS base

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Install system dependencies required for Playwright and built python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers (Chromium only to save space)
RUN playwright install --with-deps chromium

# Copy the rest of the application
COPY . .

# Ensure output and logs directories exist
RUN mkdir -p output logs

# Set the default command to run the pipeline
CMD ["python", "pipeline.py"]
