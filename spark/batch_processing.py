"""
Spark Batch Processing - Crypto Analytics
Đọc raw data từ HDFS → Tính toán → Ghi clean/aggregated data

Modes:
  --full: Xử lý toàn bộ data (mặc định lần đầu)
  --incremental: Chỉ xử lý data mới chưa được xử lý
"""

import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pathlib import Path

# Paths - configurable via environment
BASE_DIR = Path(os.getenv("HDFS_DATA_DIR", "/app/data"))
RAW_PATH = str(BASE_DIR / "raw")
CLEAN_PATH = str(BASE_DIR / "clean")
AGG_PATH = str(BASE_DIR / "aggregated")
CHECKPOINT_FILE = BASE_DIR / "processed_hours.txt"


def create_spark_session():
    """Tạo Spark session - optimized for low memory"""
    return SparkSession.builder \
        .appName("CryptoBatchProcessing") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "4") \
        .config("spark.memory.fraction", "0.6") \
        .master("local[2]") \
        .getOrCreate()


def get_processed_hours():
    """Lấy danh sách các date+hour đã xử lý từ checkpoint file"""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, "r") as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def save_processed_hours(date_hours):
    """Lưu danh sách date+hour đã xử lý vào checkpoint file"""
    existing = get_processed_hours()
    all_hours = existing.union(set(date_hours))
    with open(CHECKPOINT_FILE, "w") as f:
        for dh in sorted(all_hours):
            f.write(f"{dh}\n")
    print(f"   📝 Saved {len(date_hours)} new date+hours to checkpoint")


def get_raw_date_hours():
    """Lấy danh sách các date+hour có trong raw data"""
    raw_path = Path(RAW_PATH)
    date_hours = []
    for dt_folder in raw_path.glob("dt=*"):
        date_str = dt_folder.name.replace("dt=", "")
        for hr_folder in dt_folder.glob("hr=*"):
            hour_str = hr_folder.name.replace("hr=", "").zfill(2)
            date_hours.append(f"{date_str}_{hour_str}")
    return sorted(date_hours)


def get_raw_last_modified(date_str):
    """Lấy timestamp file mới nhất trong raw data của 1 ngày"""
    raw_date_path = Path(RAW_PATH) / f"dt={date_str}"
    if not raw_date_path.exists():
        return 0
    
    latest_time = 0
    for f in raw_date_path.rglob("*.jsonl"):
        mtime = f.stat().st_mtime
        if mtime > latest_time:
            latest_time = mtime
    return latest_time


def get_clean_last_modified(date_str):
    """Lấy timestamp file mới nhất trong clean data của 1 ngày"""
    clean_date_path = Path(CLEAN_PATH) / f"date={date_str}"
    if not clean_date_path.exists():
        return 0
    
    latest_time = 0
    for f in clean_date_path.rglob("*.parquet"):
        mtime = f.stat().st_mtime
        if mtime > latest_time:
            latest_time = mtime
    return latest_time


def get_date_hours_need_update():
    """
    Smart detect: Tìm các date+hour cần update dựa trên checkpoint
    - Có trong raw nhưng chưa trong processed → cần update
    - Luôn reprocess toàn bộ giờ của ngày hiện tại (để clean/aggregated cập nhật liên tục)
    """
    raw_date_hours = get_raw_date_hours()
    processed = get_processed_hours()

    # Các giờ mới chưa xử lý
    update_set = {dh for dh in raw_date_hours if dh not in processed}

    # Xác định ngày hiện tại theo local time trong container
    today_str = datetime.now().date().isoformat()
    # Lấy toàn bộ giờ thuộc ngày hiện tại có trong raw
    today_hours = {dh for dh in raw_date_hours if dh.startswith(f"{today_str}_")}

    if today_hours:
        # Luôn đưa toàn bộ giờ của hôm nay vào danh sách reprocess
        update_set.update(today_hours)
        print(f"   🔁 Reprocessing current day: {today_str} ({len(today_hours)} hours)")

    # Trả về danh sách đã sắp xếp để ổn định log
    return sorted(update_set)


def read_raw_data(spark, date_hours_to_process=None):
    """Đọc raw data từ HDFS, có thể filter theo date+hour"""
    print(f"📂 Reading raw data from {RAW_PATH}")
    
    if date_hours_to_process:
        # Chỉ đọc các date+hour cần xử lý
        paths = []
        for dh in date_hours_to_process:
            date_str, hour_str = dh.split("_")
            paths.append(f"{RAW_PATH}/dt={date_str}/hr={hour_str}/*.jsonl")
        df = spark.read.json(paths)
        print(f"   📅 Processing {len(date_hours_to_process)} date+hours")
    else:
        df = spark.read.json(f"{RAW_PATH}/*/*/*.jsonl")
    
    # Parse crawl_time thành timestamp
    df = df.withColumn("crawl_ts", F.to_timestamp("crawl_time"))
    df = df.withColumn("date", F.to_date("crawl_ts"))
    df = df.withColumn("hour", F.hour("crawl_ts"))
    df = df.withColumn("minute", F.minute("crawl_ts"))
    
    # Tính price_change_percentage_24h nếu không có (từ current_price và low_24h)
    if "price_change_percentage_24h" not in df.columns:
        df = df.withColumn(
            "price_change_percentage_24h",
            F.when(F.col("low_24h") > 0, 
                   ((F.col("current_price") - F.col("low_24h")) / F.col("low_24h")) * 100
            ).otherwise(0.0)
        )
    
    record_count = df.count()
    print(f"✅ Loaded {record_count:,} records")
    return df


# Global mode flag
INCREMENTAL_MODE = False


def smart_write(df, output_path, partition_cols=None):
    """
    Write DataFrame với mode thông minh:
    - Full mode: overwrite toàn bộ
    - Incremental mode: chỉ xóa partition cũ của dates đang xử lý, rồi append
    """
    if INCREMENTAL_MODE:
        # Append mode cho incremental
        writer = df.write.mode("append")
    else:
        # Overwrite cho full mode
        writer = df.write.mode("append")
    
    if partition_cols:
        if isinstance(partition_cols, str):
            writer.partitionBy(partition_cols).parquet(output_path)
        else:
            writer.partitionBy(*partition_cols).parquet(output_path)
    else:
        writer.parquet(output_path)


def job_daily_price_stats(df):
    """
    Job 1: Giá trung bình theo ngày/coin
    Output: avg_price, min_price, max_price, open_price, close_price per day per coin
    """
    print("\n📊 Job 1: Daily Price Statistics...")
    
    daily_stats = df.groupBy("date", "coin_id", "symbol", "name") \
        .agg(
            F.avg("current_price").alias("avg_price"),
            F.min("current_price").alias("min_price"),
            F.max("current_price").alias("max_price"),
            F.first("current_price").alias("open_price"),
            F.last("current_price").alias("close_price"),
            F.avg("total_volume").alias("avg_volume"),
            F.avg("market_cap").alias("avg_market_cap"),
            F.count("*").alias("data_points")
        ) \
        .withColumn("price_range", F.col("max_price") - F.col("min_price")) \
        .withColumn("daily_change_pct", 
                    (F.col("close_price") - F.col("open_price")) / F.col("open_price") * 100) \
        .orderBy("date", "coin_id")
    
    output_path = f"{AGG_PATH}/daily_price_stats"
    daily_stats.write.mode("append").partitionBy("date").parquet(output_path)
    print(f"   ✅ Saved to {output_path}")
    
    return daily_stats


def job_top_pumps_dumps(df):
    """
    Job 2: Top 10 coins pump/dump lớn nhất mỗi ngày
    """
    print("\n📈 Job 2: Top Pumps & Dumps per Day...")
    
    # Lấy record cuối của mỗi ngày cho mỗi coin
    window = Window.partitionBy("date", "coin_id").orderBy(F.desc("crawl_ts"))
    
    daily_snapshot = df.withColumn("rn", F.row_number().over(window)) \
        .filter(F.col("rn") == 1) \
        .select(
            "date", "coin_id", "symbol", "name",
            "current_price", "price_change_percentage_24h",
            "market_cap", "market_cap_rank", "total_volume"
        )
    
    # Window để rank trong mỗi ngày
    pump_window = Window.partitionBy("date").orderBy(F.desc("price_change_percentage_24h"))
    dump_window = Window.partitionBy("date").orderBy(F.asc("price_change_percentage_24h"))
    
    # Top 10 pumps mỗi ngày
    top_pumps = daily_snapshot \
        .withColumn("rank", F.row_number().over(pump_window)) \
        .filter(F.col("rank") <= 10) \
        .withColumn("type", F.lit("pump"))
    
    # Top 10 dumps mỗi ngày
    top_dumps = daily_snapshot \
        .withColumn("rank", F.row_number().over(dump_window)) \
        .filter(F.col("rank") <= 10) \
        .withColumn("type", F.lit("dump"))
    
    combined = top_pumps.union(top_dumps)
    
    output_path = f"{AGG_PATH}/top_pumps_dumps"
    combined.write.mode("append").partitionBy("date", "type").parquet(output_path)
    print(f"   ✅ Saved to {output_path}")
    
    return combined


def job_market_cap_distribution(df):
    """
    Job 3: Phân bố market cap (Large/Mid/Small cap)
    Large cap: > $10B
    Mid cap: $1B - $10B  
    Small cap: < $1B
    """
    print("\n💰 Job 3: Market Cap Distribution...")
    
    # Lấy snapshot cuối ngày
    window = Window.partitionBy("date", "coin_id").orderBy(F.desc("crawl_ts"))
    
    distribution = df.withColumn("rn", F.row_number().over(window)) \
        .filter(F.col("rn") == 1) \
        .withColumn("cap_category", 
            F.when(F.col("market_cap") >= 10_000_000_000, "Large Cap")
            .when(F.col("market_cap") >= 1_000_000_000, "Mid Cap")
            .otherwise("Small Cap")
        ) \
        .groupBy("date", "cap_category") \
        .agg(
            F.count("*").alias("coin_count"),
            F.sum("market_cap").alias("total_market_cap"),
            F.avg("market_cap").alias("avg_market_cap")
        ) \
        .orderBy("date", "cap_category")
    
    output_path = f"{AGG_PATH}/market_cap_distribution"
    distribution.write.mode("append").partitionBy("date").parquet(output_path)
    print(f"   ✅ Saved to {output_path}")
    
    return distribution


def job_top_coin_trends(df):
    """
    Job 4: Xu hướng giá BTC, ETH, top altcoins theo giờ
    """
    print("\n📉 Job 4: Top Coin Hourly Trends...")
    
    top_coins = ["bitcoin", "ethereum", "binancecoin", "solana", "ripple", 
                 "cardano", "dogecoin", "tron", "avalanche-2", "chainlink"]
    
    trends = df.filter(F.col("coin_id").isin(top_coins)) \
        .groupBy("date", "hour", "coin_id", "symbol") \
        .agg(
            F.avg("current_price").alias("avg_price"),
            F.min("current_price").alias("min_price"),
            F.max("current_price").alias("max_price"),
            F.avg("total_volume").alias("avg_volume"),
            F.avg("market_cap").alias("avg_market_cap"),
            F.avg("price_change_percentage_24h").alias("avg_change_24h")
        ) \
        .orderBy("coin_id", "date", "hour")
    
    output_path = f"{AGG_PATH}/top_coin_trends"
    trends.write.mode("append").partitionBy("coin_id").parquet(output_path)
    print(f"   ✅ Saved to {output_path}")
    
    return trends


def job_hourly_volume(df):
    """
    Job 5: Volume analysis theo giờ
    """
    print("\n📊 Job 5: Hourly Volume Analysis...")
    
    hourly_volume = df.groupBy("date", "hour") \
        .agg(
            F.sum("total_volume").alias("total_volume"),
            F.avg("total_volume").alias("avg_volume_per_coin"),
            F.countDistinct("coin_id").alias("active_coins"),
            F.sum("market_cap").alias("total_market_cap")
        ) \
        .orderBy("date", "hour")
    
    output_path = f"{AGG_PATH}/hourly_volume"
    hourly_volume.write.mode("append").partitionBy("date").parquet(output_path)
    print(f"   ✅ Saved to {output_path}")
    
    return hourly_volume


def job_btc_correlation(df):
    """
    Job 6: Correlation giữa BTC và altcoins
    """
    print("\n🔗 Job 6: BTC Correlation Analysis...")
    
    # Tính giá trung bình mỗi giờ cho mỗi coin
    hourly_prices = df.groupBy("date", "hour", "coin_id") \
        .agg(F.avg("current_price").alias("price"))
    
    # Lấy BTC price riêng
    btc_prices = hourly_prices.filter(F.col("coin_id") == "bitcoin") \
        .select("date", "hour", F.col("price").alias("btc_price"))
    
    # Join với altcoins
    altcoin_prices = hourly_prices.filter(F.col("coin_id") != "bitcoin")
    with_btc = altcoin_prices.join(btc_prices, ["date", "hour"])
    
    # Tính correlation coefficient theo ngày
    correlation = with_btc.groupBy("date", "coin_id") \
        .agg(F.corr("price", "btc_price").alias("btc_correlation")) \
        .filter(F.col("btc_correlation").isNotNull()) \
        .orderBy("date", F.desc(F.abs("btc_correlation")))
    
    output_path = f"{AGG_PATH}/btc_correlation"
    correlation.write.mode("append").partitionBy("date").parquet(output_path)
    print(f"   ✅ Saved to {output_path}")
    
    return correlation


def job_coin_volume_ranking(df):
    """
    Job 7: Volume ranking - Top 20 coins theo volume mỗi ngày
    """
    print("\n📊 Job 7: Daily Volume Ranking (Top 20)...")
    
    # First aggregate, then apply window on aggregated column
    volume_agg = df.groupBy("date", "coin_id", "symbol", "name") \
        .agg(
            F.sum("total_volume").alias("daily_volume"),
            F.avg("current_price").alias("avg_price"),
            F.avg("market_cap").alias("avg_market_cap")
        )
    
    window = Window.partitionBy("date").orderBy(F.desc("daily_volume"))
    
    volume_ranking = volume_agg \
        .withColumn("volume_rank", F.row_number().over(window)) \
        .filter(F.col("volume_rank") <= 20) \
        .orderBy("date", "volume_rank")
    
    output_path = f"{AGG_PATH}/coin_volume_ranking"
    volume_ranking.write.mode("append").partitionBy("date").parquet(output_path)
    print(f"   ✅ Saved to {output_path}")
    
    return volume_ranking


def job_pump_dump_alerts(df):
    """
    Job 8: Phát hiện pump/dump alerts
    - Pump: >5% trong 1h hoặc >10% trong 24h
    - Dump: <-5% trong 1h hoặc <-10% trong 24h
    """
    print("\n🚨 Job 8: Pump/Dump Alerts Detection...")
    
    alerts = df.filter(
        (F.abs(F.col("price_change_percentage_24h")) >= 10) |
        (F.abs(F.col("price_change_percentage_1h")) >= 5)
    ).withColumn("alert_type",
        F.when(F.col("price_change_percentage_24h") >= 10, "PUMP_24H")
        .when(F.col("price_change_percentage_24h") <= -10, "DUMP_24H")
        .when(F.col("price_change_percentage_1h") >= 5, "PUMP_1H")
        .when(F.col("price_change_percentage_1h") <= -5, "DUMP_1H")
        .otherwise("UNKNOWN")
    ).select(
        "crawl_ts", "date", "hour", "coin_id", "symbol", "name",
        "current_price", "price_change_percentage_24h", 
        "price_change_percentage_1h", "alert_type", "total_volume"
    )
    
    # Đếm alerts theo giờ
    hourly_alerts = alerts.groupBy("date", "hour", "alert_type") \
        .agg(
            F.count("*").alias("alert_count"),
            F.collect_list("symbol").alias("coins")
        )
    
    output_path = f"{AGG_PATH}/pump_dump_alerts"
    alerts.write.mode("append").partitionBy("date", "alert_type").parquet(output_path)
    
    hourly_path = f"{AGG_PATH}/hourly_alert_counts"
    hourly_alerts.write.mode("append").partitionBy("date").parquet(hourly_path)
    print(f"   ✅ Saved to {output_path}")
    
    return alerts


def job_btc_dominance(df):
    """
    Job 9: BTC Dominance - % market cap của BTC so với tổng
    """
    print("\n👑 Job 9: BTC Dominance...")
    
    # Tổng market cap theo giờ
    total_mcap = df.groupBy("date", "hour") \
        .agg(F.sum("market_cap").alias("total_market_cap"))
    
    # BTC market cap theo giờ
    btc_mcap = df.filter(F.col("coin_id") == "bitcoin") \
        .groupBy("date", "hour") \
        .agg(F.avg("market_cap").alias("btc_market_cap"))
    
    dominance = total_mcap.join(btc_mcap, ["date", "hour"]) \
        .withColumn("btc_dominance_pct", 
                    F.col("btc_market_cap") / F.col("total_market_cap") * 100) \
        .orderBy("date", "hour")
    
    output_path = f"{AGG_PATH}/btc_dominance"
    dominance.write.mode("append").partitionBy("date").parquet(output_path)
    print(f"   ✅ Saved to {output_path}")
    
    return dominance


def job_price_heatmap(df):
    """
    Job 10: Price change heatmap - % thay đổi giá theo coin × giờ
    """
    print("\n🔥 Job 10: Price Change Heatmap...")
    
    # Top 20 coins by market cap
    top_coins = df.groupBy("coin_id") \
        .agg(F.avg("market_cap").alias("avg_mcap")) \
        .orderBy(F.desc("avg_mcap")) \
        .limit(20) \
        .select("coin_id").collect()
    top_coin_ids = [row.coin_id for row in top_coins]
    
    heatmap = df.filter(F.col("coin_id").isin(top_coin_ids)) \
        .groupBy("date", "hour", "coin_id", "symbol") \
        .agg(
            F.avg("price_change_percentage_24h").alias("avg_change_24h"),
            F.avg("current_price").alias("avg_price")
        ) \
        .orderBy("date", "hour", "coin_id")
    
    output_path = f"{AGG_PATH}/price_heatmap"
    heatmap.write.mode("append").partitionBy("date").parquet(output_path)
    print(f"   ✅ Saved to {output_path}")
    
    return heatmap


def job_market_sentiment(df):
    """
    Job 11: Market Sentiment - % coins tăng vs giảm theo giờ
    """
    print("\n😊 Job 11: Market Sentiment...")
    
    sentiment = df.groupBy("date", "hour") \
        .agg(
            F.count("*").alias("total_records"),
            F.sum(F.when(F.col("price_change_percentage_24h") > 0, 1).otherwise(0)).alias("coins_up"),
            F.sum(F.when(F.col("price_change_percentage_24h") < 0, 1).otherwise(0)).alias("coins_down"),
            F.sum(F.when(F.col("price_change_percentage_24h") == 0, 1).otherwise(0)).alias("coins_neutral"),
            F.avg("price_change_percentage_24h").alias("avg_market_change")
        ) \
        .withColumn("bullish_pct", F.col("coins_up") / F.col("total_records") * 100) \
        .withColumn("bearish_pct", F.col("coins_down") / F.col("total_records") * 100) \
        .withColumn("sentiment", 
            F.when(F.col("bullish_pct") >= 60, "BULLISH")
            .when(F.col("bearish_pct") >= 60, "BEARISH")
            .otherwise("NEUTRAL")
        ) \
        .orderBy("date", "hour")
    
    output_path = f"{AGG_PATH}/market_sentiment"
    sentiment.write.mode("append").partitionBy("date").parquet(output_path)
    print(f"   ✅ Saved to {output_path}")
    
    return sentiment


def job_whale_detection(df):
    """
    Job 12: Whale Detection - Volume đột biến >200% so với trung bình
    """
    print("\n🐋 Job 12: Whale Detection (Volume Spikes)...")
    
    # Tính avg volume 7 ngày cho mỗi coin
    window_7d = Window.partitionBy("coin_id").orderBy("date").rowsBetween(-7, -1)
    
    daily_volume = df.groupBy("date", "coin_id", "symbol", "name") \
        .agg(F.sum("total_volume").alias("daily_volume"))
    
    with_avg = daily_volume.withColumn("avg_volume_7d", F.avg("daily_volume").over(window_7d))
    
    whales = with_avg.filter(F.col("avg_volume_7d").isNotNull()) \
        .withColumn("volume_spike_pct", 
                    (F.col("daily_volume") - F.col("avg_volume_7d")) / F.col("avg_volume_7d") * 100) \
        .filter(F.col("volume_spike_pct") >= 200) \
        .withColumn("spike_level",
            F.when(F.col("volume_spike_pct") >= 500, "EXTREME")
            .when(F.col("volume_spike_pct") >= 300, "HIGH")
            .otherwise("MODERATE")
        ) \
        .orderBy("date", F.desc("volume_spike_pct"))
    
    output_path = f"{AGG_PATH}/whale_detection"
    whales.write.mode("append").partitionBy("date").parquet(output_path)
    print(f"   ✅ Saved to {output_path}")
    
    return whales


def job_rank_changes(df):
    """
    Job 13: Market Cap Rank Changes - Coins thay đổi rank nhiều nhất
    """
    print("\n🔄 Job 13: Rank Changes Tracking...")
    
    # Lấy rank đầu và cuối ngày
    window_first = Window.partitionBy("date", "coin_id").orderBy("crawl_ts")
    window_last = Window.partitionBy("date", "coin_id").orderBy(F.desc("crawl_ts"))
    
    rank_changes = df.withColumn("rn_first", F.row_number().over(window_first)) \
        .withColumn("rn_last", F.row_number().over(window_last)) \
        .groupBy("date", "coin_id", "symbol", "name") \
        .agg(
            F.first(F.when(F.col("rn_first") == 1, F.col("market_cap_rank"))).alias("rank_start"),
            F.first(F.when(F.col("rn_last") == 1, F.col("market_cap_rank"))).alias("rank_end")
        ) \
        .withColumn("rank_change", F.col("rank_start") - F.col("rank_end")) \
        .filter(F.abs(F.col("rank_change")) >= 1) \
        .withColumn("direction",
            F.when(F.col("rank_change") > 0, "UP")
            .when(F.col("rank_change") < 0, "DOWN")
            .otherwise("SAME")
        ) \
        .orderBy("date", F.desc(F.abs("rank_change")))
    
    output_path = f"{AGG_PATH}/rank_changes"
    rank_changes.write.mode("append").partitionBy("date").parquet(output_path)
    print(f"   ✅ Saved to {output_path}")
    
    return rank_changes


def create_clean_data(df):
    """
    Tạo clean data - deduplicated, validated
    """
    print("\n🧹 Creating Clean Data...")
    
    clean_df = df \
        .filter(F.col("current_price") > 0) \
        .filter(F.col("market_cap") > 0) \
        .select(
            "crawl_ts", "date", "hour", "minute",
            "coin_id", "symbol", "name",
            "current_price", "price_change_24h", "price_change_percentage_24h",
            "market_cap", "market_cap_rank", "total_volume",
            "high_24h", "low_24h", "circulating_supply"
        )
    
    output_path = CLEAN_PATH
    # Dùng append để giữ data cũ, partition cũ đã được xóa bởi delete_existing_partitions()
    clean_df.write.mode("append").partitionBy("date", "hour").parquet(output_path)
    print(f"   ✅ Saved clean data to {output_path}")
    
    return clean_df


def delete_existing_partitions(dates_to_process):
    """Xóa các partition cũ của ngày cần xử lý lại (cho incremental mode)"""
    import shutil
    
    for date_str in dates_to_process:
        # Xóa trong aggregated
        for job_folder in Path(AGG_PATH).glob("*"):
            if job_folder.is_dir():
                date_partition = job_folder / f"date={date_str}"
                if date_partition.exists():
                    shutil.rmtree(date_partition)
        
        # Xóa trong clean
        clean_date = Path(CLEAN_PATH) / f"date={date_str}"
        if clean_date.exists():
            shutil.rmtree(clean_date)
    
    print(f"   🗑️ Deleted old partitions for {len(dates_to_process)} dates")


def main():
    global INCREMENTAL_MODE
    
    # Parse arguments
    mode = "full"
    if len(sys.argv) > 1:
        if sys.argv[1] == "--incremental":
            mode = "incremental"
        elif sys.argv[1] == "--full":
            mode = "full"
    
    print("=" * 60)
    print("🚀 SPARK BATCH PROCESSING - CRYPTO ANALYTICS")
    print(f"   Mode: {mode.upper()}")
    print("=" * 60)
    
    # Determine date+hours to process
    all_raw_date_hours = get_raw_date_hours()
    
    if mode == "incremental":
        INCREMENTAL_MODE = True
        # Smart detect: tìm date+hour chưa được xử lý
        date_hours_to_process = get_date_hours_need_update()
        if not date_hours_to_process:
            print("\n✅ No new data to process. All data is up-to-date!")
            print(f"   Total date+hours in raw: {len(all_raw_date_hours)}")
            return
        print(f"\n📅 New data found: {len(date_hours_to_process)} date+hours")
        print(f"   First few: {', '.join(date_hours_to_process[:5])}")
        # Xóa partition cũ để ghi mới
        dates_to_delete = list(set([dh.split("_")[0] for dh in date_hours_to_process]))
        delete_existing_partitions(dates_to_delete)
    else:
        INCREMENTAL_MODE = False
        date_hours_to_process = all_raw_date_hours
        print(f"\n📅 Processing ALL {len(date_hours_to_process)} date+hours")
    
    # Create output directories
    Path(CLEAN_PATH).mkdir(parents=True, exist_ok=True)
    Path(AGG_PATH).mkdir(parents=True, exist_ok=True)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Read raw data (only date+hours to process)
        df = read_raw_data(spark, date_hours_to_process if INCREMENTAL_MODE else None)
        
        # ===== BASIC ANALYTICS JOBS (1-6) =====
        job_daily_price_stats(df)
        spark.catalog.clearCache()
        
        df = read_raw_data(spark, date_hours_to_process if INCREMENTAL_MODE else None)
        job_top_pumps_dumps(df)
        job_market_cap_distribution(df)
        spark.catalog.clearCache()
        
        df = read_raw_data(spark, date_hours_to_process if INCREMENTAL_MODE else None)
        job_top_coin_trends(df)
        job_hourly_volume(df)
        job_btc_correlation(df)
        spark.catalog.clearCache()
        
        # ===== ADVANCED ANALYTICS JOBS (7-13) =====
        df = read_raw_data(spark, date_hours_to_process if INCREMENTAL_MODE else None)
        job_coin_volume_ranking(df)
        job_pump_dump_alerts(df)
        job_btc_dominance(df)
        spark.catalog.clearCache()
        
        df = read_raw_data(spark, date_hours_to_process if INCREMENTAL_MODE else None)
        job_price_heatmap(df)
        job_market_sentiment(df)
        job_whale_detection(df)
        job_rank_changes(df)
        spark.catalog.clearCache()
        
        # Create clean data
        df = read_raw_data(spark, date_hours_to_process if INCREMENTAL_MODE else None)
        create_clean_data(df)
        
        # Save processed date+hours to checkpoint
        save_processed_hours(date_hours_to_process)
        
        print("\n" + "=" * 60)
        print(f"✅ ALL 13 BATCH JOBS COMPLETED! ({len(date_hours_to_process)} date+hours processed)")
        print("=" * 60)
        
        # Show sample results
        print("\n📋 Sample: Daily Price Stats (BTC)")
        spark.read.parquet(f"{AGG_PATH}/daily_price_stats") \
            .filter(F.col("coin_id") == "bitcoin") \
            .select("date", "avg_price", "min_price", "max_price", "daily_change_pct") \
            .orderBy("date") \
            .show(5)
        
        print("\n📋 Sample: Market Sentiment")
        spark.read.parquet(f"{AGG_PATH}/market_sentiment") \
            .select("date", "sentiment", "bullish_pct", "bearish_pct") \
            .orderBy("date") \
            .show(5)
        
        print("\n📋 Sample: BTC Dominance")
        spark.read.parquet(f"{AGG_PATH}/btc_dominance") \
            .select("date", "hour", "btc_dominance_pct") \
            .orderBy("date", "hour") \
            .show(5)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
