# Dockerfile for Kafka to HDFS consumer
FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir kafka-python

# Copy scripts
COPY kafka_to_hdfs_raw.py .

# Create data directory
RUN mkdir -p /app/data/raw

# Set Python unbuffered mode
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "-u", "kafka_to_hdfs_raw.py"]
