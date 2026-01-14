"""
Reset Checkpoints Tool
Use this script to reset the checkpoints for Data Cleaning and Daily Aggregation jobs.
This allows re-processing past data (e.g. after inserting fake historical data).

Usage:
  spark-submit reset_checkpoints.py [RESET_DATE]

Args:
  RESET_DATE: Date to reset to (YYYY-MM-DD). Default: 2025-10-01
"""
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession

# Config
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "crypto-pipeline-data")
GCS_DATA_DIR = os.getenv("GCS_DATA_DIR", "data")

# Checkpoint Paths
CLEANING_CHECKPOINT = f"gs://{GCS_BUCKET}/{GCS_DATA_DIR}/checkpoints/data_cleaning/last_processed_time.txt"
AGG_CHECKPOINT = f"gs://{GCS_BUCKET}/{GCS_DATA_DIR}/checkpoints/daily_aggregation_checkpoint.txt"

def create_spark_session():
    """Create Spark session with GCS support"""
    return SparkSession.builder \
        .appName("ResetCheckpoints") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .getOrCreate()

def reset_checkpoints(reset_date_str):
    spark = create_spark_session()
    
    try:
        print("=" * 60)
        print(f"🔄 RESETTING CHECKPOINTS TO: {reset_date_str}")
        print("=" * 60)
        
        # Validate date
        reset_date = datetime.strptime(reset_date_str, "%Y-%m-%d")
        
        # 1. Reset Data Cleaning Checkpoint
        # Format: ISO timestamp (e.g., 2025-10-01T00:00:00)
        timestamp_str = reset_date.isoformat()
        print(f"\n1️⃣  Resetting Data Cleaning checkpoint...")
        print(f"   📍 Path: {CLEANING_CHECKPOINT}")
        print(f"   📝 Value: {timestamp_str}")
        
        df_clean = spark.createDataFrame([(timestamp_str,)], ["timestamp"])
        df_clean.write.mode("overwrite").text(CLEANING_CHECKPOINT)
        print("   ✅ Done.")
        
        # 2. Reset Daily Aggregation Checkpoint
        # Format: YYYY-MM-DD (e.g., 2025-10-01)
        print(f"\n2️⃣  Resetting Daily Aggregation checkpoint...")
        print(f"   📍 Path: {AGG_CHECKPOINT}")
        print(f"   📝 Value: {reset_date_str}")
        
        df_agg = spark.createDataFrame([(reset_date_str,)], ["date"])
        df_agg.write.mode("overwrite").text(AGG_CHECKPOINT)
        print("   ✅ Done.")
        
        print("\n" + "=" * 60)
        print("🎉 SUCCESS! Checkpoints have been reset.")
        print("   Now run your cleaning and aggregation jobs to re-process data.")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error resetting checkpoints: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    # Default to 7 days before the fake start date (just to be safe) or 2025-10-01
    target_date = "2025-10-01"
    
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        
    reset_checkpoints(target_date)
