# Dockerfile for Kafka to GCS consumer
FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts
COPY kafka_to_gcs.py .
COPY kafka_to_hdfs_real.py .

# Set Python unbuffered mode
ENV PYTHONUNBUFFERED=1

# Default command - use GCS version
CMD ["python", "-u", "kafka_to_gcs.py"]
