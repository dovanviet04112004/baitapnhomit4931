"""
Simplified Spark Batch Job
1. Clean Data (Raw -> Clean)
2. Simple Analytics (Clean -> Aggregated)
   - Top Gainers/Losers
   - BTC Trend
"""
import os
import sys
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, TimestampType, LongType

# Paths
HDFS_DATA_DIR = os.getenv("HDFS_DATA_DIR", "/app/data")
RAW_PATH = f"{HDFS_DATA_DIR}/raw"  # Đọc từ đây (Raw JSON)
CLEAN_PATH = f"{HDFS_DATA_DIR}/clean"  # Ghi vào đây (Clean Parquet)
AGG_PATH = f"{HDFS_DATA_DIR}/aggregated"  # Ghi kết quả tính toán

def create_spark_session():
    return SparkSession.builder \
        .appName("SimpleCryptoBatch") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

# --- STEP 1: CLEANING LOGIC (Giống batch_processing.py) ---
def clean_data(spark):
    print(f"🧹 Bắt đầu làm sạch dữ liệu từ: {RAW_PATH}")
    
    # Đọc raw data (JSON)
    try:
        raw_df = spark.read.json(f"{RAW_PATH}/*/*/*.jsonl")
    except Exception as e:
        print(f"⚠️ Không tìm thấy raw data hoặc lỗi đọc: {e}")
        return None

    # Quan trọng: Loại bỏ bản ghi trùng lặp (Deduplication)
    # Nếu trong 1 giây crawl lỡ có 2 bản ghi cho cùng 1 coin -> Lấy cái mới nhất
    window_spec = Window.partitionBy("coin_id", "crawl_time").orderBy(F.desc("crawl_time"))
    
    clean_df = raw_df.withColumn("rank", F.row_number().over(window_spec)) \
        .filter(F.col("rank") == 1) \
        .drop("rank")

    # Ép kiểu dữ liệu (Type Casting) cho chuẩn
    clean_df = clean_df \
        .withColumn("current_price", F.col("current_price").cast(DoubleType())) \
        .withColumn("market_cap", F.col("market_cap").cast(LongType())) \
        .withColumn("total_volume", F.col("total_volume").cast(LongType())) \
        .withColumn("price_change_percentage_24h", F.col("price_change_percentage_24h").cast(DoubleType())) \
        .withColumn("crawl_ts", F.to_timestamp(F.col("crawl_time"))) \
        .withColumn("date", F.to_date(F.col("crawl_ts"))) \
        .withColumn("hour", F.hour(F.col("crawl_ts")))

    # Loại bỏ dữ liệu lỗi (giá null hoặc < 0)
    clean_df = clean_df.filter((F.col("current_price").isNotNull()) & (F.col("current_price") > 0))

    # Ghi Clean Data ra HDFS (Parquet) để dùng lại sau này
    print(f"💾 Đang lưu clean data vào: {CLEAN_PATH}")
    clean_df.write.mode("overwrite").partitionBy("date").parquet(CLEAN_PATH)
    
    print("✅ Làm sạch dữ liệu hoàn tất!")
    return clean_df

# --- STEP 2: SIMPLE ANALYTICS ---
def run_analytics(spark, df):
    print("🚀 Bắt đầu tính toán chỉ số đơn giản...")
    
    # Lấy dữ liệu mới nhất của từng coin để xếp hạng
    latest_window = Window.partitionBy("symbol").orderBy(F.desc("crawl_time"))
    latest_df = df.withColumn("rn", F.row_number().over(latest_window)) \
                  .filter(F.col("rn") == 1).drop("rn")

    # A. TOP 10 TĂNG GIÁ (Gainers)
    top_gainers = latest_df.orderBy(F.desc("price_change_percentage_24h")).limit(10)
    # Lưu kết quả
    top_gainers.write.mode("overwrite").parquet(f"{AGG_PATH}/top_gainers")
    print("✅ Đã lưu Top Gainers")
    top_gainers.select("symbol", "name", "price_change_percentage_24h").show(5)

    # B. TOP 10 GIẢM GIÁ (Losers)
    top_losers = latest_df.orderBy(F.asc("price_change_percentage_24h")).limit(10)
    top_losers.write.mode("overwrite").parquet(f"{AGG_PATH}/top_losers")
    print("✅ Đã lưu Top Losers")

    # C. XU HƯỚNG BTC THEO GIỜ
    # Lấy BTC -> Group theo Date, Hour -> Avg Price
    btc_trend = df.filter(F.lower(F.col("symbol")) == "btc") \
        .groupBy("date", "hour") \
        .agg(F.round(F.avg("current_price"), 2).alias("avg_price")) \
        .orderBy("date", "hour")
    
    btc_trend.write.mode("overwrite").parquet(f"{AGG_PATH}/btc_trend")
    print("✅ Đã lưu BTC Trend (Hourly)")

    # D. XU HƯỚNG THEO PHÚT (REAL-TIME-ISH) - Cho Demo Kibana nhảy số
    # Lấy BTC & ETH -> Group theo Phút (substring ngày giờ đến phút)
    print("\n" + "="*50)
    print("⏱️ TÍNH TOÁN XU HƯỚNG THEO PHÚT (CHO DEMO)")
    print("="*50)

    minute_trend = df.filter(F.col("symbol").isin(["BTC", "ETH"])) \
        .withColumn("time_minute", F.substring("crawl_time", 1, 16)) \
        .groupBy("symbol", "time_minute") \
        .agg(
            F.round(F.avg("current_price"), 2).alias("avg_price"),
            F.max("crawl_time").alias("timestamp") # Lấy mốc thời gian chuẩn
        ) \
        .orderBy("symbol", F.desc("time_minute"))
    
    # Lưu vào HDFS (cần overwrite để update mới nhất)
    minute_trend.write.mode("overwrite").parquet(f"{AGG_PATH}/minute_trend")
    print("✅ Đã lưu Minute Trend (cho Kibana Realtime)")
    minute_trend.show(5)

def main():
    spark = create_spark_session()
    
    # 1. Clean Data
    clean_df = clean_data(spark)
    
    # 2. Analytics (chỉ chạy nếu có clean data)
    if clean_df:
        run_analytics(spark, clean_df)
    else:
        print("❌ Không có dữ liệu để phân tích")

    spark.stop()

if __name__ == "__main__":
    main()
