@echo off
REM Windows batch script to create Kafka topics

echo ==================================================
echo Creating Kafka Topics for Price Analytics Pipeline
echo ==================================================

set KAFKA_BROKER=localhost:19092
set REPLICATION_FACTOR=2
set PARTITIONS=3

echo Waiting for Kafka brokers to be ready...
timeout /t 10 /nobreak >nul

echo.
echo Creating topics with retention policies...

REM raw_products: Keep raw data for 7 days (604800000 ms)
echo.
echo Creating topic: raw_products
docker exec kafka-broker-1 kafka-topics --create --bootstrap-server kafka-broker-1:9092 --topic raw_products --partitions %PARTITIONS% --replication-factor %REPLICATION_FACTOR% --config retention.ms=604800000 --config compression.type=snappy --if-not-exists

REM clean_products: Keep cleaned data for 30 days (2592000000 ms)
echo.
echo Creating topic: clean_products
docker exec kafka-broker-1 kafka-topics --create --bootstrap-server kafka-broker-1:9092 --topic clean_products --partitions %PARTITIONS% --replication-factor %REPLICATION_FACTOR% --config retention.ms=2592000000 --config compression.type=snappy --if-not-exists

REM alerts: Keep alerts for 90 days (7776000000 ms)
echo.
echo Creating topic: alerts
docker exec kafka-broker-1 kafka-topics --create --bootstrap-server kafka-broker-1:9092 --topic alerts --partitions %PARTITIONS% --replication-factor %REPLICATION_FACTOR% --config retention.ms=7776000000 --config compression.type=snappy --if-not-exists

echo.
echo ==================================================
echo Current Topics:
echo ==================================================
docker exec kafka-broker-1 kafka-topics --list --bootstrap-server kafka-broker-1:9092

echo.
echo ==================================================
echo Topic Details:
echo ==================================================

echo.
echo --- Topic: raw_products ---
docker exec kafka-broker-1 kafka-topics --describe --bootstrap-server kafka-broker-1:9092 --topic raw_products

echo.
echo --- Topic: clean_products ---
docker exec kafka-broker-1 kafka-topics --describe --bootstrap-server kafka-broker-1:9092 --topic clean_products

echo.
echo --- Topic: alerts ---
docker exec kafka-broker-1 kafka-topics --describe --bootstrap-server kafka-broker-1:9092 --topic alerts

echo.
echo ==================================================
echo ✅ Kafka topics setup complete!
echo ==================================================
echo.
echo Access Kafka UI at: http://localhost:8080
echo.

pause
