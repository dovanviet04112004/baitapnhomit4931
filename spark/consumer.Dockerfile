FROM python:3.9-slim

LABEL maintainer="BigData Team"
LABEL description="Kafka to Elasticsearch Consumer for Speed Layer"

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    requests==2.31.0

# Copy consumer script
COPY streaming_realtime_consumer.py .

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Run consumer
CMD ["python", "-u", "streaming_realtime_consumer.py"]
