FROM apache/spark:3.4.1

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.4.1 \
    kafka-python \
    py4j

# Create app directory
WORKDIR /app

# Copy Spark streaming script
COPY streaming_processing.py .

# Create checkpoint directory (will be overridden by PVC mount)
RUN mkdir -p /checkpoints && chmod 777 /checkpoints

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_HOME=/opt/spark
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC=crypto-raw

# Default command
CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[2]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1", \
     "--conf", "spark.sql.streaming.checkpointLocation=/checkpoints", \
     "--conf", "spark.metrics.namespace=crypto_streaming", \
     "/app/streaming_processing.py"]
