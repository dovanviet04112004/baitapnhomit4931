# Spark Batch Processing with GCS Support
FROM apache/spark:3.4.1

USER root

# Install Python dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Download GCS connector for Spark
# https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage
RUN curl -L https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
    -o /opt/spark/jars/gcs-connector-hadoop3-latest.jar

# Copy job files
WORKDIR /app
COPY data_cleaning.py .
COPY daily_aggregation.py .
COPY export_metrics.py .

# Default command (can be overridden in K8s)
CMD ["spark-submit", "--master", "local[*]", "data_cleaning.py"]
