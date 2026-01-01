#!/bin/bash

# Kafka Topics Creation Script
# This script creates the required topics for the crypto price analytics pipeline

echo "=================================================="
echo "Creating Kafka Topics for Crypto Analytics Pipeline"
echo "=================================================="

KAFKA_BROKER="localhost:19092"
REPLICATION_FACTOR=2
PARTITIONS=3

# Wait for Kafka to be ready
echo "Waiting for Kafka brokers to be ready..."
sleep 10

# Function to create topic
create_topic() {
    local topic_name=$1
    local retention_ms=$2
    
    echo ""
    echo "Creating topic: $topic_name"
    
    docker exec kafka-broker-1 kafka-topics --create \
        --bootstrap-server kafka-broker-1:9092 \
        --topic $topic_name \
        --partitions $PARTITIONS \
        --replication-factor $REPLICATION_FACTOR \
        --config retention.ms=$retention_ms \
        --config compression.type=snappy \
        --if-not-exists
    
    if [ $? -eq 0 ]; then
        echo "✅ Topic '$topic_name' created successfully"
    else
        echo "❌ Failed to create topic '$topic_name'"
    fi
}

# Create topics
echo ""
echo "Creating topics with retention policies..."

# raw_crypto: Keep raw data for 7 days (604800000 ms)
create_topic "raw_crypto" 604800000

# clean_crypto: Keep cleaned data for 30 days (2592000000 ms)
create_topic "clean_crypto" 2592000000

# alerts: Keep alerts for 90 days (7776000000 ms)
create_topic "alerts" 7776000000

# List all topics
echo ""
echo "=================================================="
echo "Current Topics:"
echo "=================================================="
docker exec kafka-broker-1 kafka-topics --list --bootstrap-server kafka-broker-1:9092

# Describe topics
echo ""
echo "=================================================="
echo "Topic Details:"
echo "=================================================="

for topic in raw_crypto clean_crypto alerts; do
    echo ""
    echo "--- Topic: $topic ---"
    docker exec kafka-broker-1 kafka-topics --describe \
        --bootstrap-server kafka-broker-1:9092 \
        --topic $topic
done

echo ""
echo "=================================================="
echo "✅ Kafka topics setup complete!"
echo "=================================================="
echo ""
echo "Access Kafka UI at: http://localhost:8080"
echo ""
