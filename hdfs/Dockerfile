# Dockerfile for Kafka to HDFS consumer
FROM python:3.9-slim

WORKDIR /app

# Install system dependencies for HDFS client
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    kafka-python \
    hdfs

# Copy scripts
COPY kafka_to_hdfs_real.py .
COPY kafka_to_hdfs_raw.py .

# Set Python unbuffered mode
ENV PYTHONUNBUFFERED=1

# Default command - use HDFS real version
CMD ["python", "-u", "kafka_to_hdfs_real.py"]
