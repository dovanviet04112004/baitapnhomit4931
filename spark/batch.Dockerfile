FROM apache/spark:3.4.1

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.4.1 \
    py4j

# Create app directory
WORKDIR /app

# Copy Spark scripts
COPY data_cleaning.py .
COPY daily_aggregation.py .
COPY export_metrics.py .

# Download connectors
RUN curl -L -o /opt/spark/jars/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar && \
    curl -L -o /opt/spark/jars/elasticsearch-spark-30_2.12-8.11.0.jar \
    https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.11.0/elasticsearch-spark-30_2.12-8.11.0.jar

# Set environment variables
ENV HDFS_DATA_DIR=/app/data
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_HOME=/opt/spark

# Default command (can be overridden in docker-compose)
CMD ["python", "data_cleaning.py"]
