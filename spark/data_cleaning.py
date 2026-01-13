"""
Data Cleaning Job - Production Grade
Input: Raw JSON from HDFS (/data/raw)
Output: Clean Parquet to HDFS (/data/clean)

Features:
- Checkpoint-based incremental processing
- Dynamic partition overwrite
- Fault tolerance
- Idempotent operations
"""
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, TimestampType, LongType, IntegerType

# Paths
HDFS_DATA_DIR = os.getenv("HDFS_DATA_DIR", "/app/data")
RAW_PATH = f"{HDFS_DATA_DIR}/raw"
CLEAN_PATH = f"{HDFS_DATA_DIR}/clean"
CHECKPOINT_PATH = f"{HDFS_DATA_DIR}/checkpoints/data_cleaning"


class CheckpointManager:
    """Manage checkpoint for incremental data processing"""
    
    def __init__(self, spark, checkpoint_path):
        self.spark = spark
        self.checkpoint_path = checkpoint_path
        self.checkpoint_file = f"{checkpoint_path}/last_processed_time.txt"
    
    def get_last_processed_time(self):
        """Get last processed timestamp from checkpoint"""
        try:
            # Read from HDFS
            df = self.spark.read.text(self.checkpoint_file)
            last_time_str = df.first()[0]
            last_time = datetime.fromisoformat(last_time_str)
            print(f"   📌 Last checkpoint: {last_time}")
            return last_time
        except Exception as e:
            # First run - start from 7 days ago to catch all existing data
            default_time = datetime.now() - timedelta(days=7)
            print(f"   ⚠️  No checkpoint found: {e}")
            print(f"   📌 Starting from: {default_time}")
            return default_time
    
    def save_processed_time(self, timestamp):
        """Save new checkpoint timestamp"""
        try:
            # Create DataFrame with timestamp
            df = self.spark.createDataFrame([(timestamp.isoformat(),)], ["timestamp"])
            
            # Write to HDFS (overwrite old checkpoint)
            df.write.mode("overwrite").text(self.checkpoint_file)
            
            print(f"   ✅ Checkpoint saved: {timestamp}")
        except Exception as e:
            print(f"   ❌ Failed to save checkpoint: {e}")
            raise


def create_spark_session():
    """Create Spark session with optimized configs"""
    return SparkSession.builder \
        .appName("CryptoDataCleaning_Production") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()


def clean_raw_data(spark):
    """
    Clean raw data with checkpoint-based incremental processing
    
    Steps:
    1. Read checkpoint to get last processed time
    2. Read only new raw data since checkpoint
    3. Deduplicate records
    4. Cast data types
    5. Validate data quality
    6. Add derived columns
    7. Save with dynamic partition overwrite
    8. Update checkpoint
    """
    print("=" * 70)
    print("🧹 DATA CLEANING JOB STARTED (Production Mode)")
    print("=" * 70)
    
    # Initialize checkpoint manager
    checkpoint_mgr = CheckpointManager(spark, CHECKPOINT_PATH)
    
    # Get last processed time
    last_processed = checkpoint_mgr.get_last_processed_time()
    current_time = datetime.now()
    
    print(f"\n📅 Processing window:")
    print(f"   From: {last_processed}")
    print(f"   To:   {current_time}")
    duration_hours = (current_time - last_processed).total_seconds() / 3600
    print(f"   Duration: {duration_hours:.2f} hours")
    
    # Step 1: Read raw data from checkpoint to now
    print(f"\n📖 Reading raw data from: {RAW_PATH}")
    try:
        # Read all raw data
        raw_df = spark.read.json(f"{RAW_PATH}/*/*/*.jsonl")
        
        # Filter by crawl_time (only new data since checkpoint)
        filtered_df = raw_df.filter(
            (F.col("crawl_time") > last_processed.isoformat()) &
            (F.col("crawl_time") <= current_time.isoformat())
        )
        
        raw_count = filtered_df.count()
        
        if raw_count == 0:
            print("   ⚠️  No new data to process")
            print("   ✅ Job completed (no-op)")
            return None
        
        print(f"   ✅ Found {raw_count:,} new raw records")
    except Exception as e:
        print(f"   ❌ Error reading raw data: {e}")
        raise
    
    # Step 2: Deduplication
    print("\n🔄 Deduplicating records...")
    # Keep latest record for each (coin_id, crawl_time) combination
    window_dedup = Window.partitionBy("coin_id", "crawl_time").orderBy(F.desc("last_updated"))
    
    dedup_df = filtered_df \
        .withColumn("row_num", F.row_number().over(window_dedup)) \
        .filter(F.col("row_num") == 1) \
        .drop("row_num")
    
    dedup_count = dedup_df.count()
    duplicates_removed = raw_count - dedup_count
    print(f"   ✅ Removed {duplicates_removed:,} duplicates")
    print(f"   ✅ Remaining: {dedup_count:,} unique records")
    
    # Step 3: Type casting
    print("\n🔧 Casting data types...")
    clean_df = dedup_df \
        .withColumn("current_price", F.col("current_price").cast(DoubleType())) \
        .withColumn("market_cap", F.col("market_cap").cast(LongType())) \
        .withColumn("market_cap_rank", F.col("market_cap_rank").cast(IntegerType())) \
        .withColumn("total_volume", F.col("total_volume").cast(LongType())) \
        .withColumn("circulating_supply", F.col("circulating_supply").cast(DoubleType())) \
        .withColumn("total_supply", F.col("total_supply").cast(DoubleType())) \
        .withColumn("max_supply", F.col("max_supply").cast(DoubleType())) \
        .withColumn("price_change_24h", F.col("price_change_24h").cast(DoubleType())) \
        .withColumn("price_change_percentage_24h", F.col("price_change_percentage_24h").cast(DoubleType())) \
        .withColumn("price_change_percentage_1h", F.col("price_change_percentage_1h").cast(DoubleType())) \
        .withColumn("price_change_percentage_7d", F.col("price_change_percentage_7d").cast(DoubleType())) \
        .withColumn("high_24h", F.col("high_24h").cast(DoubleType())) \
        .withColumn("low_24h", F.col("low_24h").cast(DoubleType())) \
        .withColumn("ath", F.col("ath").cast(DoubleType())) \
        .withColumn("atl", F.col("atl").cast(DoubleType()))
    
    print("   ✅ Type casting completed")
    
    # Step 4: Add derived columns
    print("\n➕ Adding derived columns...")
    clean_df = clean_df \
        .withColumn("crawl_ts", F.to_timestamp(F.col("crawl_time"))) \
        .withColumn("date", F.to_date(F.col("crawl_ts"))) \
        .withColumn("hour", F.hour(F.col("crawl_ts"))) \
        .withColumn("day_of_week", F.dayofweek(F.col("crawl_ts"))) \
        .withColumn("week_of_year", F.weekofyear(F.col("crawl_ts"))) \
        .withColumn("month", F.month(F.col("crawl_ts"))) \
        .withColumn("year", F.year(F.col("crawl_ts")))
    
    print("   ✅ Derived columns added")
    
    # Step 5: Data validation
    print("\n✅ Validating data quality...")
    
    # Remove invalid records
    before_validation = clean_df.count()
    
    clean_df = clean_df.filter(
        # Price must be positive
        (F.col("current_price").isNotNull()) & (F.col("current_price") > 0) &
        # Market cap must be positive
        (F.col("market_cap").isNotNull()) & (F.col("market_cap") > 0) &
        # Volume must be non-negative
        (F.col("total_volume").isNotNull()) & (F.col("total_volume") >= 0) &
        # Coin ID and symbol must exist
        (F.col("coin_id").isNotNull()) & (F.col("symbol").isNotNull())
    )
    
    after_validation = clean_df.count()
    invalid_removed = before_validation - after_validation
    
    print(f"   ✅ Removed {invalid_removed:,} invalid records")
    print(f"   ✅ Valid records: {after_validation:,}")
    
    # Step 6: Select final columns
    print("\n📋 Selecting final columns...")
    final_df = clean_df.select(
        # Identifiers
        "coin_id",
        "symbol",
        "name",
        # Timestamps
        "crawl_ts",
        "date",
        "hour",
        "day_of_week",
        "week_of_year",
        "month",
        "year",
        # Price data
        "current_price",
        "high_24h",
        "low_24h",
        "ath",
        "atl",
        # Market data
        "market_cap",
        "market_cap_rank",
        "total_volume",
        # Supply data
        "circulating_supply",
        "total_supply",
        "max_supply",
        # Price changes
        "price_change_24h",
        "price_change_percentage_24h",
        "price_change_percentage_1h",
        "price_change_percentage_7d",
        # Metadata
        "source",
        "last_updated"
    )
    
    print("   ✅ Final schema prepared")
    
    # Step 7: Save with dynamic partition overwrite
    print(f"\n💾 Saving clean data to: {CLEAN_PATH}")
    print("   🔧 Mode: Dynamic Partition Overwrite")
    
    final_df.write \
        .mode("overwrite") \
        .partitionBy("date", "hour") \
        .parquet(CLEAN_PATH)
    
    # Show affected partitions
    partitions = final_df.select("date", "hour").distinct().collect()
    print(f"   ✅ Overwritten {len(partitions)} partition(s):")
    for p in partitions[:10]:  # Show first 10
        print(f"      - date={p.date}/hour={p.hour}")
    if len(partitions) > 10:
        print(f"      ... and {len(partitions) - 10} more")
    
    print("   ✅ Clean data saved successfully")
    
    # Step 8: Update checkpoint (ONLY if successful)
    print("\n📌 Updating checkpoint...")
    checkpoint_mgr.save_processed_time(current_time)
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 CLEANING SUMMARY")
    print("=" * 70)
    print(f"   Raw records:        {raw_count:,}")
    print(f"   Duplicates removed: {duplicates_removed:,}")
    print(f"   Invalid removed:    {invalid_removed:,}")
    print(f"   Clean records:      {after_validation:,}")
    print(f"   Data quality:       {(after_validation/raw_count*100):.2f}%")
    print(f"   Partitions updated: {len(partitions)}")
    print("=" * 70)
    print("✅ DATA CLEANING JOB COMPLETED")
    print("=" * 70)
    
    return final_df


def main():
    """Main execution"""
    spark = create_spark_session()
    
    try:
        clean_df = clean_raw_data(spark)
        
        if clean_df is not None:
            # Show sample
            print("\n📋 Sample clean data:")
            clean_df.show(5, truncate=False)
            
            # Show schema
            print("\n📋 Clean data schema:")
            clean_df.printSchema()
            
    except Exception as e:
        print(f"\n❌ Error in cleaning job: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
