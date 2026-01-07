# Base image with Spark
FROM apache/spark:3.5.0

USER root

# Install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

# Install Google Cloud Storage connector for Spark
RUN curl -L https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
  -o /opt/spark/jars/gcs-connector-hadoop3-latest.jar

# Copy Spark job files
COPY *.py /app/
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV SPARK_HOME=/opt/bitnami/spark

# Default command
CMD ["spark-submit", "--master", "local[*]", "batch_processing.py"]
