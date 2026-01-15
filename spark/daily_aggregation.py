"""
Daily/Weekly/Monthly Aggregation Job - INCREMENTAL VERSION
Input: Clean Parquet from GCS (gs://bucket/data/clean)
Output: Aggregated metrics to GCS (gs://bucket/data/aggregated)

Features:
- Checkpoint-based incremental processing
- Re-process checkpoint date to ensure completeness
- Dynamic partition overwrite (only affected partitions)
- Efficient processing of new data only

Outputs:
1. daily_metrics - Daily summary per coin
2. weekly_metrics - Weekly rollup
3. monthly_metrics - Monthly rollup
"""
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# Paths - GCS
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "crypto-pipeline-data")
GCS_DATA_DIR = os.getenv("GCS_DATA_DIR", "data")
CLEAN_PATH = f"gs://{GCS_BUCKET}/{GCS_DATA_DIR}/clean"
AGG_PATH = f"gs://{GCS_BUCKET}/{GCS_DATA_DIR}/aggregated"
CHECKPOINT_PATH = f"gs://{GCS_BUCKET}/{GCS_DATA_DIR}/checkpoints/daily_aggregation_checkpoint.txt"


def create_spark_session():
    """Create Spark session with GCS support"""
    return SparkSession.builder \
        .appName("CryptoDailyAggregation-GCS") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .getOrCreate()


def get_last_processed_date(spark):
    """
    Get the last processed date from checkpoint
    Returns: date object or None if no checkpoint exists
    """
    try:
        checkpoint_df = spark.read.text(CHECKPOINT_PATH)
        last_date_str = checkpoint_df.first()[0]
        return datetime.strptime(last_date_str, "%Y-%m-%d").date()
    except Exception as e:
        print(f"   ℹ️  No checkpoint found: {e}")
        return None


def get_date_range_to_process(spark):
    """
    Determine the date range to process
    Returns: (start_date, end_date) or None if no new data
    
    Logic:
    - If no checkpoint: process all data (first run)
    - If checkpoint exists: re-process from checkpoint date (inclusive) to latest date
      This ensures incomplete data on checkpoint date is re-calculated
    """
    last_processed = get_last_processed_date(spark)
    
    # Get latest date in clean data
    try:
        clean_df = spark.read.parquet(CLEAN_PATH)
        max_date_in_clean = clean_df.agg(F.max("date")).first()[0]
        
        if max_date_in_clean is None:
            print("   ⚠️  No data in clean path")
            return None
            
    except Exception as e:
        print(f"   ❌ Error reading clean data: {e}")
        return None
    
    if last_processed is None:
        # First run: process all data
        min_date_in_clean = clean_df.agg(F.min("date")).first()[0]
        print(f"   🆕 First run - Processing all data")
        print(f"   📅 Date range: {min_date_in_clean} → {max_date_in_clean}")
        return (min_date_in_clean, max_date_in_clean)
    
    # IMPORTANT: Re-process from checkpoint date (inclusive)
    # Because there might be new data arrived for that date
    start_date = last_processed
    
    if max_date_in_clean < start_date:
        # No new data
        print(f"   ✅ No new data to process")
        print(f"   📅 Last processed: {last_processed}, Latest in clean: {max_date_in_clean}")
        return None
    
    print(f"   🔄 Incremental processing")
    print(f"   📅 Date range: {start_date} (re-process) → {max_date_in_clean}")
    return (start_date, max_date_in_clean)


def save_checkpoint(spark, date):
    """Save checkpoint with the latest processed date"""
    checkpoint_df = spark.createDataFrame([(str(date),)], ["date"])
    checkpoint_df.write.mode("overwrite").text(CHECKPOINT_PATH)
    print(f"   💾 Checkpoint saved: {date}")


def calculate_daily_metrics(spark):
    """
    Calculate daily metrics for each coin - INCREMENTAL VERSION
    
    Output schema:
    - date (date)
    - year, month, week_of_year, day_of_week
    - coin_id, symbol, name
    - open_price (first price of day)
    - close_price (last price of day)
    - high_price, low_price
    - return_pct_day ((close-open)/open * 100)
    - volume_sum_day
    - volatility_day ((high-low)/open * 100)
    - rank_open, rank_close, rank_change
    """
    print("=" * 70)
    print("📊 DAILY METRICS INCREMENTAL CALCULATION")
    print("=" * 70)
    
    # 1. Determine date range to process
    print("\n🔍 Checking for new data...")
    date_range = get_date_range_to_process(spark)
    if date_range is None:
        return None
    
    start_date, end_date = date_range
    
    # 2. Read ONLY data in the date range
    print(f"\n📖 Reading clean data from: {CLEAN_PATH}")
    try:
        clean_df = spark.read.parquet(CLEAN_PATH) \
            .filter((F.col("date") >= start_date) & (F.col("date") <= end_date))
        
        total_records = clean_df.count()
        if total_records == 0:
            print("   ⚠️  No records in date range")
            return None
        
        print(f"   ✅ Loaded {total_records:,} records for processing")
        
        # Show date distribution
        date_dist = clean_df.groupBy("date").count().orderBy("date")
        print(f"\n   📊 Records per date:")
        date_dist.show(10, truncate=False)
        
    except Exception as e:
        print(f"   ❌ Error reading clean data: {e}")
        return None
    
    # 3. Calculate daily metrics
    print("\n🔢 Calculating daily metrics...")
    
    # Window specs
    day_window = Window.partitionBy("coin_id", "date").orderBy("crawl_ts")
    rank_window_open = Window.partitionBy("date").orderBy(F.desc("open_price"))
    rank_window_close = Window.partitionBy("date").orderBy(F.desc("close_price"))
    
    # Add row numbers to identify first/last records of the day
    df_with_order = clean_df.withColumn("row_num", F.row_number().over(day_window))
    df_with_order = df_with_order.withColumn(
        "max_row", F.max("row_num").over(Window.partitionBy("coin_id", "date"))
    )
    
    # Extract open and close prices
    daily_agg = df_with_order.groupBy("coin_id", "symbol", "name", "date").agg(
        # Open price (first record of day)
        F.first("current_price", ignorenulls=True).alias("open_price"),
        # Close price (last record of day)
        F.last("current_price", ignorenulls=True).alias("close_price"),
        # High and low
        F.max("current_price").alias("high_price"),
        F.min("current_price").alias("low_price"),
        # Volume (Use last recorded 24h volume of the day, do NOT sum)
        F.last("total_volume", ignorenulls=True).alias("volume_sum_day"),
        # Market cap (use latest)
        F.last("market_cap", ignorenulls=True).alias("market_cap_close"),
        F.last("market_cap_rank", ignorenulls=True).alias("market_cap_rank_close"),
        # Count records
        F.count("*").alias("record_count")
    )
    
    # Calculate derived metrics
    daily_metrics = daily_agg \
        .withColumn(
            "return_pct_day",
            ((F.col("close_price") - F.col("open_price")) / F.col("open_price") * 100)
        ) \
        .withColumn(
            "volatility_day",
            ((F.col("high_price") - F.col("low_price")) / F.col("open_price") * 100)
        ) \
        .withColumn(
            "price_range_day",
            F.col("high_price") - F.col("low_price")
        )
    
    # Add rankings
    daily_metrics = daily_metrics \
        .withColumn("rank_open", F.row_number().over(rank_window_open)) \
        .withColumn("rank_close", F.row_number().over(rank_window_close)) \
        .withColumn("rank_change", F.col("rank_open") - F.col("rank_close"))
    
    # Add day metadata
    daily_metrics = daily_metrics \
        .withColumn("day_of_week", F.dayofweek(F.col("date"))) \
        .withColumn("week_of_year", F.weekofyear(F.col("date"))) \
        .withColumn("month", F.month(F.col("date"))) \
        .withColumn("year", F.year(F.col("date")))
    
    # Select final columns
    daily_metrics = daily_metrics.select(
        "date",
        "year",
        "month",
        "week_of_year",
        "day_of_week",
        "coin_id",
        "symbol",
        "name",
        "open_price",
        "close_price",
        "high_price",
        "low_price",
        "return_pct_day",
        "volatility_day",
        "price_range_day",
        "volume_sum_day",
        "market_cap_close",
        "market_cap_rank_close",
        "rank_open",
        "rank_close",
        "rank_change",
        "record_count"
    )
    
    record_count = daily_metrics.count()
    print(f"   ✅ Calculated metrics for {record_count:,} coin-days")
    
    # 4. Save with DYNAMIC PARTITION OVERWRITE
    # Partition by year/month/day to prevent overwriting other days in the same month
    output_path = f"{AGG_PATH}/daily_metrics"
    print(f"\n💾 Saving daily metrics to: {output_path}")
    print(f"   📦 Mode: Dynamic Partition Overwrite (only affected year/month/day)")
    
    # Add day column for partitioning
    daily_metrics = daily_metrics.withColumn("day", F.dayofmonth(F.col("date")))
    
    # Get affected partitions
    affected_partitions = daily_metrics.select("year", "month", "day").distinct().collect()
    print(f"   📂 Affected partitions:")
    for row in affected_partitions:
        print(f"      - year={row['year']}/month={row['month']}/day={row['day']}")
    
    daily_metrics.write \
        .mode("overwrite") \
        .option("partitionOverwriteMode", "dynamic") \
        .partitionBy("year", "month", "day") \
        .parquet(output_path)
    
    print("   ✅ Daily metrics saved")
    
    # 5. Update checkpoint with the LATEST date processed
    save_checkpoint(spark, end_date)
    
    # Cache for weekly/monthly calculations
    daily_metrics = daily_metrics.cache()
    
    return daily_metrics


def calculate_weekly_metrics(spark, daily_metrics_new):
    """
    Calculate weekly metrics from daily metrics - INCREMENTAL VERSION
    
    Strategy:
    - Identify which weeks need to be re-calculated (from daily_metrics_new)
    - Read ALL daily metrics
    - Filter to only the weeks that need update
    - Calculate and save with dynamic partition overwrite
    
    Output schema:
    - year, week_of_year
    - week_start_date, week_end_date
    - coin_id, symbol, name
    - open_price_week, close_price_week
    - high_price_week, low_price_week
    - return_pct_week, volatility_week
    - volume_sum_week
    - avg_rank_week
    """
    print("\n" + "=" * 70)
    print("📊 WEEKLY METRICS INCREMENTAL CALCULATION")
    print("=" * 70)
    
    # 1. Identify weeks to re-calculate
    weeks_to_process = daily_metrics_new.select("year", "week_of_year").distinct()
    week_count = weeks_to_process.count()
    print(f"\n🔍 Identified {week_count} week(s) to re-calculate:")
    weeks_to_process.orderBy("year", "week_of_year").show(10, truncate=False)
    
    # 2. Read ALL daily metrics
    print(f"\n📖 Reading all daily metrics from: {AGG_PATH}/daily_metrics")
    try:
        all_daily = spark.read.parquet(f"{AGG_PATH}/daily_metrics")
        print(f"   ✅ Loaded all daily metrics")
    except Exception as e:
        print(f"   ❌ Error reading daily metrics: {e}")
        return None
    
    # 3. Filter to only weeks that need update
    daily_for_weeks = all_daily.join(
        weeks_to_process,
        ["year", "week_of_year"],
        "inner"
    )
    
    records_to_process = daily_for_weeks.count()
    print(f"   📊 Processing {records_to_process:,} daily records for weekly aggregation")
    
    # 4. Calculate weekly metrics
    print("\n🔢 Calculating weekly metrics...")
    
    # Window for ordering within each week
    week_window = Window.partitionBy("coin_id", "year", "week_of_year").orderBy("date")
    
    # Add row numbers
    df_with_order = daily_for_weeks.withColumn("row_num", F.row_number().over(week_window))
    
    # Aggregate by week
    weekly_agg = df_with_order.groupBy("coin_id", "symbol", "name", "year", "week_of_year").agg(
        # Week start/end dates
        F.min("date").alias("week_start_date"),
        F.max("date").alias("week_end_date"),
        # Prices
        F.first("open_price", ignorenulls=True).alias("open_price_week"),
        F.last("close_price", ignorenulls=True).alias("close_price_week"),
        F.max("high_price").alias("high_price_week"),
        F.min("low_price").alias("low_price_week"),
        # Volume
        F.sum("volume_sum_day").alias("volume_sum_week"),
        # Volatility (average of daily volatilities)
        F.avg("volatility_day").alias("avg_volatility_week"),
        # Rankings
        F.avg("rank_close").alias("avg_rank_week"),
        # Days count
        F.count("*").alias("days_in_week")
    )
    
    # Calculate weekly return
    weekly_metrics = weekly_agg \
        .withColumn(
            "return_pct_week",
            ((F.col("close_price_week") - F.col("open_price_week")) / F.col("open_price_week") * 100)
        ) \
        .withColumn(
            "volatility_week",
            ((F.col("high_price_week") - F.col("low_price_week")) / F.col("open_price_week") * 100)
        )
    
    # Select final columns
    weekly_metrics = weekly_metrics.select(
        "year",
        "week_of_year",
        "week_start_date",
        "week_end_date",
        "coin_id",
        "symbol",
        "name",
        "open_price_week",
        "close_price_week",
        "high_price_week",
        "low_price_week",
        "return_pct_week",
        "volatility_week",
        "avg_volatility_week",
        "volume_sum_week",
        "avg_rank_week",
        "days_in_week"
    )
    
    record_count = weekly_metrics.count()
    print(f"   ✅ Calculated metrics for {record_count:,} coin-weeks")
    
    # 5. Save with dynamic partition overwrite
    # Partition by year/week_of_year to prevent overwriting other weeks in the same year
    output_path = f"{AGG_PATH}/weekly_metrics"
    print(f"\n💾 Saving weekly metrics to: {output_path}")
    print(f"   📦 Mode: Dynamic Partition Overwrite (only affected year/week)")
    
    # Get affected partitions
    affected_partitions = weekly_metrics.select("year", "week_of_year").distinct().collect()
    print(f"   📂 Affected partitions:")
    for row in affected_partitions:
        print(f"      - year={row['year']}/week_of_year={row['week_of_year']}")
    
    weekly_metrics.write \
        .mode("overwrite") \
        .option("partitionOverwriteMode", "dynamic") \
        .partitionBy("year", "week_of_year") \
        .parquet(output_path)
    
    print("   ✅ Weekly metrics saved")
    
    return weekly_metrics


def calculate_monthly_metrics(spark, daily_metrics_new):
    """
    Calculate monthly metrics from daily metrics - INCREMENTAL VERSION
    
    Strategy:
    - Identify which months need to be re-calculated (from daily_metrics_new)
    - Read ALL daily metrics
    - Filter to only the months that need update
    - Calculate and save with dynamic partition overwrite
    
    Output schema:
    - year, month
    - month_start_date, month_end_date
    - coin_id, symbol, name
    - open_price_month, close_price_month
    - high_price_month, low_price_month
    - return_pct_month, volatility_month
    - volume_sum_month
    - avg_rank_month
    """
    print("\n" + "=" * 70)
    print("📊 MONTHLY METRICS INCREMENTAL CALCULATION")
    print("=" * 70)
    
    # 1. Identify months to re-calculate
    months_to_process = daily_metrics_new.select("year", "month").distinct()
    month_count = months_to_process.count()
    print(f"\n🔍 Identified {month_count} month(s) to re-calculate:")
    months_to_process.orderBy("year", "month").show(12, truncate=False)
    
    # 2. Read ALL daily metrics
    print(f"\n📖 Reading all daily metrics from: {AGG_PATH}/daily_metrics")
    try:
        all_daily = spark.read.parquet(f"{AGG_PATH}/daily_metrics")
        print(f"   ✅ Loaded all daily metrics")
    except Exception as e:
        print(f"   ❌ Error reading daily metrics: {e}")
        return None
    
    # 3. Filter to only months that need update
    daily_for_months = all_daily.join(
        months_to_process,
        ["year", "month"],
        "inner"
    )
    
    records_to_process = daily_for_months.count()
    print(f"   📊 Processing {records_to_process:,} daily records for monthly aggregation")
    
    # 4. Calculate monthly metrics
    print("\n🔢 Calculating monthly metrics...")
    
    # Window for ordering within each month
    month_window = Window.partitionBy("coin_id", "year", "month").orderBy("date")
    
    # Add row numbers
    df_with_order = daily_for_months.withColumn("row_num", F.row_number().over(month_window))
    
    # Aggregate by month
    monthly_agg = df_with_order.groupBy("coin_id", "symbol", "name", "year", "month").agg(
        # Month start/end dates
        F.min("date").alias("month_start_date"),
        F.max("date").alias("month_end_date"),
        # Prices
        F.first("open_price", ignorenulls=True).alias("open_price_month"),
        F.last("close_price", ignorenulls=True).alias("close_price_month"),
        F.max("high_price").alias("high_price_month"),
        F.min("low_price").alias("low_price_month"),
        # Volume
        F.sum("volume_sum_day").alias("volume_sum_month"),
        # Volatility
        F.avg("volatility_day").alias("avg_volatility_month"),
        # Rankings
        F.avg("rank_close").alias("avg_rank_month"),
        # Days count
        F.count("*").alias("days_in_month")
    )
    
    # Calculate monthly return
    monthly_metrics = monthly_agg \
        .withColumn(
            "return_pct_month",
            ((F.col("close_price_month") - F.col("open_price_month")) / F.col("open_price_month") * 100)
        ) \
        .withColumn(
            "volatility_month",
            ((F.col("high_price_month") - F.col("low_price_month")) / F.col("open_price_month") * 100)
        )
    
    # Select final columns
    monthly_metrics = monthly_metrics.select(
        "year",
        "month",
        "month_start_date",
        "month_end_date",
        "coin_id",
        "symbol",
        "name",
        "open_price_month",
        "close_price_month",
        "high_price_month",
        "low_price_month",
        "return_pct_month",
        "volatility_month",
        "avg_volatility_month",
        "volume_sum_month",
        "avg_rank_month",
        "days_in_month"
    )
    
    record_count = monthly_metrics.count()
    print(f"   ✅ Calculated metrics for {record_count:,} coin-months")
    
    # 5. Save with dynamic partition overwrite
    # Partition by year/month to prevent overwriting other months in the same year
    output_path = f"{AGG_PATH}/monthly_metrics"
    print(f"\n💾 Saving monthly metrics to: {output_path}")
    print(f"   📦 Mode: Dynamic Partition Overwrite (only affected year/month)")
    
    # Get affected partitions
    affected_partitions = monthly_metrics.select("year", "month").distinct().collect()
    print(f"   📂 Affected partitions:")
    for row in affected_partitions:
        print(f"      - year={row['year']}/month={row['month']}")
    
    monthly_metrics.write \
        .mode("overwrite") \
        .option("partitionOverwriteMode", "dynamic") \
        .partitionBy("year", "month") \
        .parquet(output_path)
    
    print("   ✅ Monthly metrics saved")
    
    return monthly_metrics


def main():
    """Main execution"""
    spark = create_spark_session()
    
    try:
        # Calculate daily metrics (incremental)
        daily_metrics = calculate_daily_metrics(spark)
        
        if daily_metrics is not None:
            # Show sample
            print("\n📋 Sample daily metrics (top movers):")
            daily_metrics.orderBy(F.desc("return_pct_day")).show(10, truncate=False)
            
            # Calculate weekly metrics (incremental)
            weekly_metrics = calculate_weekly_metrics(spark, daily_metrics)
            if weekly_metrics is not None:
                print("\n📋 Sample weekly metrics:")
                weekly_metrics.orderBy(F.desc("year"), F.desc("week_of_year")).show(5, truncate=False)
            
            # Calculate monthly metrics (incremental)
            monthly_metrics = calculate_monthly_metrics(spark, daily_metrics)
            if monthly_metrics is not None:
                print("\n📋 Sample monthly metrics:")
                monthly_metrics.orderBy(F.desc("year"), F.desc("month")).show(5, truncate=False)
            
            # Summary
            print("\n" + "=" * 70)
            print("📊 AGGREGATION SUMMARY")
            print("=" * 70)
            print(f"   Daily metrics processed:   {daily_metrics.count():,} records")
            if weekly_metrics is not None:
                print(f"   Weekly metrics updated:    {weekly_metrics.count():,} records")
            if monthly_metrics is not None:
                print(f"   Monthly metrics updated:   {monthly_metrics.count():,} records")
            print("=" * 70)
            print("✅ INCREMENTAL AGGREGATION JOB COMPLETED")
            print("=" * 70)
        else:
            print("\n" + "=" * 70)
            print("ℹ️  NO NEW DATA TO PROCESS")
            print("=" * 70)
            
    except Exception as e:
        print(f"\n❌ Error in aggregation job: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
