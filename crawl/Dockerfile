# Base image with Python
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Google Cloud Storage client
RUN pip install --no-cache-dir google-cloud-storage

# Copy application files
COPY *.py .

# Create output directory
RUN mkdir -p output

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Default command: run streaming crawler
CMD ["python", "crypto_crawler_streaming.py"]
