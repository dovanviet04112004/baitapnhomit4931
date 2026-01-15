"""
Spark Structured Streaming - Crypto Real-time Analytics
Đọc từ Kafka → Xử lý real-time → Gửi alerts

Features:
  - Giá mới nhất của từng coin
  - Phát hiện pump/dump (>5% trong 1h, >10% trong 24h)
  - Whale alert (volume đột biến >200%)
  - Market sentiment (% coin tăng vs giảm)
  - Gửi alerts vào Kafka topic `alerts`
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pathlib import Path
import json
import os

# Kafka config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:19093,localhost:19094")
INPUT_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-raw")
ALERTS_TOPIC = "alerts"
CLEAN_TOPIC = "clean_crypto"
MARKET_SENTIMENT_TOPIC = "market_sentiment"

# Checkpoint Directory
# Trong Kubernetes: sử dụng PVC mount tại /checkpoints
# Local development: sử dụng hdfs/checkpoints/streaming
if os.path.exists("/checkpoints"):
    # Running in Kubernetes with PVC
    CHECKPOINT_DIR = "/checkpoints"
else:
    # Running locally
    BASE_DIR = Path(__file__).resolve().parent.parent
    CHECKPOINT_DIR = str(BASE_DIR / "hdfs" / "checkpoints" / "streaming")

# Alert thresholds
PUMP_1H_THRESHOLD = 5.0      # >5% trong 1h = pump
DUMP_1H_THRESHOLD = -5.0     # <-5% trong 1h = dump
PUMP_24H_THRESHOLD = 10.0    # >10% trong 24h = pump
DUMP_24H_THRESHOLD = -10.0   # <-10% trong 24h = dump
WHALE_VOLUME_RATIO = 2.0     # Volume > 200% avg = whale


def create_spark_session():
    """Tạo Spark session cho streaming"""
    return SparkSession.builder \
        .appName("CryptoStreaming") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .master("local[2]") \
        .getOrCreate()


# Schema cho crypto data
CRYPTO_SCHEMA = StructType([
    StructField("crawl_time", StringType(), True),
    StructField("coin_id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("name", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("price_change_24h", DoubleType(), True),
    StructField("price_change_percentage_24h", DoubleType(), True),
    StructField("price_change_percentage_1h", DoubleType(), True),
    StructField("price_change_percentage_7d", DoubleType(), True),
    StructField("market_cap", LongType(), True),
    StructField("market_cap_rank", LongType(), True),
    StructField("total_volume", LongType(), True),
    StructField("circulating_supply", LongType(), True),
    StructField("high_24h", DoubleType(), True),
    StructField("low_24h", DoubleType(), True),
])


def read_kafka_stream(spark):
    """Đọc stream từ Kafka"""
    print(f"📡 Connecting to Kafka: {KAFKA_BOOTSTRAP}")
    print(f"📥 Reading from topic: {INPUT_TOPIC}")
    
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()


def parse_crypto_data(kafka_df):
    """Parse JSON từ Kafka thành structured data"""
    return kafka_df \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(F.from_json(F.col("json_str"), CRYPTO_SCHEMA).alias("data")) \
        .select("data.*") \
        .withColumn("crawl_ts", F.to_timestamp("crawl_time")) \
        .withColumn("processing_time", F.current_timestamp())


def detect_pump_dump_alerts(df):
    """
    Phát hiện pump/dump alerts:
    - Pump 1h: >5%
    - Dump 1h: <-5%
    - Pump 24h: >10%
    - Dump 24h: <-10%
    """
    return df.withColumn("alert_type",
        F.when(F.col("price_change_percentage_1h") >= PUMP_1H_THRESHOLD, "PUMP_1H")
        .when(F.col("price_change_percentage_1h") <= DUMP_1H_THRESHOLD, "DUMP_1H")
        .when(F.col("price_change_percentage_24h") >= PUMP_24H_THRESHOLD, "PUMP_24H")
        .when(F.col("price_change_percentage_24h") <= DUMP_24H_THRESHOLD, "DUMP_24H")
        .otherwise(None)
    ).filter(F.col("alert_type").isNotNull())


def create_alert_json(df):
    """Tạo JSON alert để gửi vào Kafka"""
    return df.select(
        F.col("coin_id").alias("key"),
        F.to_json(F.struct(
            F.lit("price_alert").alias("alert_category"),
            F.col("alert_type"),
            F.col("coin_id"),
            F.col("symbol"),
            F.col("name"),
            F.col("current_price"),
            F.col("price_change_percentage_1h").alias("change_1h"),
            F.col("price_change_percentage_24h").alias("change_24h"),
            F.col("total_volume"),
            F.col("crawl_time"),
            F.current_timestamp().alias("alert_time")
        )).alias("value")
    )


def write_to_kafka(df, topic, checkpoint_name, output_mode="append"):
    """Ghi stream vào Kafka topic"""
    checkpoint_path = f"{CHECKPOINT_DIR}/{checkpoint_name}"
    
    return df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode(output_mode) \
        .start()


def write_to_console(df, name):
    """Ghi ra console để debug"""
    return df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", False) \
        .option("numRows", 20) \
        .queryName(name) \
        .start()


def create_latest_prices_view(parsed_df):
    """
    View 1: Giá mới nhất của từng coin
    Sử dụng watermark để xử lý late data
    """
    return parsed_df \
        .withWatermark("crawl_ts", "1 minute") \
        .groupBy(
            F.window("crawl_ts", "1 minute"),
            "coin_id", "symbol", "name"
        ) \
        .agg(
            F.last("current_price").alias("latest_price"),
            F.last("price_change_percentage_24h").alias("change_24h"),
            F.last("market_cap_rank").alias("rank"),
            F.last("total_volume").alias("volume"),
            F.max("crawl_ts").alias("last_update")
        ) \
        .select(
            "coin_id", "symbol", "name", "latest_price", 
            "change_24h", "rank", "volume", "last_update"
        )


def create_market_sentiment_view(parsed_df):
    """
    View 4: Market sentiment - % coin tăng vs giảm
    """
    return parsed_df \
        .withWatermark("crawl_ts", "1 minute") \
        .groupBy(F.window("crawl_ts", "1 minute")) \
        .agg(
            F.count("*").alias("total_coins"),
            F.sum(F.when(F.col("price_change_percentage_24h") > 0, 1).otherwise(0)).alias("bullish_count"),
            F.sum(F.when(F.col("price_change_percentage_24h") < 0, 1).otherwise(0)).alias("bearish_count"),
            F.sum(F.when(F.col("price_change_percentage_24h") == 0, 1).otherwise(0)).alias("neutral_count"),
            F.avg("price_change_percentage_24h").alias("avg_change_24h")
        ) \
        .withColumn("bullish_pct", F.round(F.col("bullish_count") / F.col("total_coins") * 100, 2)) \
        .withColumn("bearish_pct", F.round(F.col("bearish_count") / F.col("total_coins") * 100, 2)) \
        .withColumn("sentiment", 
            F.when(F.col("bullish_pct") > 60, "BULLISH")
            .when(F.col("bearish_pct") > 60, "BEARISH")
            .otherwise("NEUTRAL")
        ) \
        .select(
            F.col("window.start").alias("window_start"),
            "total_coins", "bullish_count", "bearish_count",
            "bullish_pct", "bearish_pct", "sentiment", "avg_change_24h"
        )


def main():
    print("=" * 60)
    print("🚀 SPARK STRUCTURED STREAMING - CRYPTO ANALYTICS")
    print("=" * 60)
    print(f"📡 Kafka: {KAFKA_BOOTSTRAP}")
    print(f"📥 Input topic: {INPUT_TOPIC}")
    print(f"📤 Output topics: {CLEAN_TOPIC}, {ALERTS_TOPIC}, {MARKET_SENTIMENT_TOPIC}")
    print(f"💾 Checkpoint: {CHECKPOINT_DIR}")
    print("=" * 60)
    
    # Create checkpoint directory
    Path(CHECKPOINT_DIR).mkdir(parents=True, exist_ok=True)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Read from Kafka
        kafka_df = read_kafka_stream(spark)
        
        # Parse JSON data
        parsed_df = parse_crypto_data(kafka_df)
        
        # ===== STREAM 1: Clean Data → Kafka =====
        print("\n🧹 Starting Clean Data Stream...")
        clean_json = parsed_df.select(
            F.col("coin_id").alias("key"),
            F.to_json(F.struct("*")).alias("value")
        )
        clean_query = write_to_kafka(clean_json, CLEAN_TOPIC, "clean_data")
        print(f"   ✅ Clean data streaming to topic: {CLEAN_TOPIC}")
        
        # ===== STREAM 2: Pump/Dump Alerts → Kafka =====
        print("\n🚨 Starting Pump/Dump Alert Stream...")
        alerts_df = detect_pump_dump_alerts(parsed_df)
        alerts_json = create_alert_json(alerts_df)
        
        alerts_query = write_to_kafka(alerts_json, ALERTS_TOPIC, "pump_dump_alerts")
        print(f"   ✅ Alerts streaming to topic: {ALERTS_TOPIC}")
        
        # ===== STREAM 3: Market Sentiment → Kafka =====
        print("\n😊 Starting Market Sentiment Stream...")
        sentiment_df = create_market_sentiment_view(parsed_df)
        
        sentiment_json = sentiment_df.select(
            F.col("window_start").cast("string").alias("key"),
            F.to_json(F.struct("*")).alias("value")
        )
        sentiment_query = write_to_kafka(sentiment_json, MARKET_SENTIMENT_TOPIC, "market_sentiment", output_mode="update")
        print(f"   ✅ Market sentiment streaming to topic: {MARKET_SENTIMENT_TOPIC}")
        
        # ===== STREAM 4: Console Monitor (for debugging) =====
        print("\n📺 Starting Console Monitor...")
        
        # Show alerts on console
        alerts_console = alerts_df.select(
            "crawl_ts", "symbol", "alert_type", 
            "current_price", "price_change_percentage_1h", "price_change_percentage_24h"
        ).writeStream \
            .format("console") \
            .outputMode("append") \
            .option("truncate", False) \
            .queryName("alerts_monitor") \
            .start()
        
        print("\n" + "=" * 60)
        print("✅ ALL STREAMS STARTED!")
        print("=" * 60)
        print("\n📋 Active Streams:")
        print("   1. Clean Data → Kafka topic 'clean_crypto'")
        print("   2. Pump/Dump Alerts → Kafka topic 'alerts'")
        print("   3. Market Sentiment → Kafka topic 'market_sentiment'")
        print("   4. Alerts Monitor → Console (debug)")
        print("\n⏳ Waiting for data from Kafka...")
        print("   (Press Ctrl+C to stop)")
        print("=" * 60)
        
        # Wait for all streams
        spark.streams.awaitAnyTermination()
        
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping streams...")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise
    finally:
        spark.stop()
        print("✅ Spark session stopped")


if __name__ == "__main__":
    main()
