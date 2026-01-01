"""
Spark to Elasticsearch - Index data từ HDFS vào Elasticsearch

Indices:
  - crypto_latest: Giá coin mới nhất (upsert theo symbol)
  - crypto_history: Dữ liệu giá lịch sử theo giờ
  - alerts: Cảnh báo pump/dump từ streaming

Usage:
  python spark_to_elasticsearch.py --all          # Index tất cả
  python spark_to_elasticsearch.py --latest       # Chỉ index crypto_latest
  python spark_to_elasticsearch.py --history      # Chỉ index crypto_history
  python spark_to_elasticsearch.py --alerts       # Chỉ index alerts
"""

import sys
import json
import requests
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent / "hdfs" / "data"
CLEAN_PATH = str(BASE_DIR / "clean")
AGG_PATH = str(BASE_DIR / "aggregated")

# Elasticsearch config
ES_HOST = "http://localhost:9200"
ES_INDEX_LATEST = "crypto_latest"
ES_INDEX_HISTORY = "crypto_history"
ES_INDEX_ALERTS = "alerts"

# Batch size for bulk indexing
BULK_SIZE = 500


def create_spark_session():
    """Tạo Spark session"""
    return SparkSession.builder \
        .appName("SparkToElasticsearch") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "4") \
        .master("local[2]") \
        .getOrCreate()


def check_elasticsearch():
    """Kiểm tra Elasticsearch có đang chạy không"""
    try:
        response = requests.get(ES_HOST, timeout=5)
        if response.status_code == 200:
            info = response.json()
            print(f"✅ Elasticsearch đang chạy: {info['cluster_name']} (v{info['version']['number']})")
            return True
    except Exception as e:
        print(f"❌ Không thể kết nối Elasticsearch: {e}")
        return False


def _parse_timestamp(ts_value):
    """Parse timestamp từ string thành ISO format cho Elasticsearch
    
    Handles various timestamp formats và returns ISO 8601 string
    """
    if not ts_value:
        return None
    
    if isinstance(ts_value, str):
        # Nếu đã là ISO format, trả về
        if 'T' in ts_value:
            return ts_value
        # Nếu là dạng số (timestamp Unix), convert
        try:
            ts_float = float(ts_value)
            return datetime.utcfromtimestamp(ts_float).isoformat() + 'Z'
        except (ValueError, TypeError):
            # Nếu không parse được, trả về string gốc (let ES handle)
            return ts_value
    
    # Nếu là datetime object
    if hasattr(ts_value, 'isoformat'):
        return ts_value.isoformat() + 'Z'
    
    return None


def create_indices():
    """Tạo các indices nếu chưa tồn tại"""
    print("\n" + "="*60)
    print("🔧 CHECKING/CREATING INDICES")
    print("="*60)
    
    indices = {
        ES_INDEX_LATEST: {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "properties": {
                    "symbol": {"type": "keyword"},
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "price_usd": {"type": "double"},
                    "market_cap": {"type": "double"},
                    "volume_24h": {"type": "double"},
                    "percent_change_1h": {"type": "double"},
                    "percent_change_24h": {"type": "double"},
                    "percent_change_7d": {"type": "double"},
                    "circulating_supply": {"type": "double"},
                    "total_supply": {"type": "double"},
                    "rank": {"type": "integer"},
                    "last_updated": {"type": "date"}
                }
            }
        },
        ES_INDEX_HISTORY: {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "properties": {
                    "symbol": {"type": "keyword"},
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "date": {"type": "date", "format": "yyyy-MM-dd"},
                    "hour": {"type": "integer"},
                    "price_usd": {"type": "double"},
                    "price_open": {"type": "double"},
                    "price_high": {"type": "double"},
                    "price_low": {"type": "double"},
                    "price_close": {"type": "double"},
                    "market_cap": {"type": "double"},
                    "volume_24h": {"type": "double"},
                    "percent_change_24h": {"type": "double"},
                    "rank": {"type": "integer"},
                    "record_count": {"type": "integer"}
                }
            }
        },
        ES_INDEX_ALERTS: {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "properties": {
                    "alert_id": {"type": "keyword"},
                    "symbol": {"type": "keyword"},
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "alert_type": {"type": "keyword"},
                    "severity": {"type": "keyword"},
                    "price_change_percent": {"type": "double"},
                    "volume_change_percent": {"type": "double"},
                    "current_price": {"type": "double"},
                    "previous_price": {"type": "double"},
                    "current_volume": {"type": "double"},
                    "previous_volume": {"type": "double"},
                    "threshold_exceeded": {"type": "double"},
                    "message": {"type": "text"},
                    "detected_at": {"type": "date"},
                    "window_start": {"type": "date"},
                    "window_end": {"type": "date"}
                }
            }
        }
    }
    
    for index_name, settings in indices.items():
        # Check if index exists
        response = requests.head(f"{ES_HOST}/{index_name}")
        if response.status_code == 200:
            print(f"   ✅ Index '{index_name}' đã tồn tại")
        else:
            # Create index
            response = requests.put(
                f"{ES_HOST}/{index_name}",
                headers={"Content-Type": "application/json"},
                data=json.dumps(settings)
            )
            if response.status_code == 200:
                print(f"   ✅ Đã tạo index '{index_name}'")
            else:
                print(f"   ❌ Lỗi tạo index '{index_name}': {response.text[:100]}")


def bulk_index(index_name, documents):
    """
    Bulk index documents vào Elasticsearch
    documents: list of dict với _id (optional) và các fields
    """
    if not documents:
        return 0
    
    bulk_data = []
    for doc in documents:
        doc_copy = doc.copy()  # Don't modify original
        doc_id = doc_copy.pop("_id", None)
        if doc_id:
            bulk_data.append(json.dumps({"index": {"_index": index_name, "_id": doc_id}}))
        else:
            bulk_data.append(json.dumps({"index": {"_index": index_name}}))
        bulk_data.append(json.dumps(doc_copy, default=str))
    
    bulk_body = "\n".join(bulk_data) + "\n"
    
    response = requests.post(
        f"{ES_HOST}/_bulk",
        headers={"Content-Type": "application/x-ndjson"},
        data=bulk_body.encode('utf-8')
    )
    
    if response.status_code == 200:
        result = response.json()
        if result.get("errors"):
            # Print first error for debugging
            for item in result["items"]:
                if "error" in item.get("index", {}):
                    print(f"   ⚠️  Error: {item['index']['error']['reason'][:100]}")
                    break
            error_count = sum(1 for item in result["items"] if "error" in item.get("index", {}))
            return len(documents) - error_count
        return len(documents)
    else:
        print(f"   ❌ Bulk index error: {response.text[:200]}")
        return 0


def index_crypto_latest(spark):
    """
    Index giá coin mới nhất vào crypto_latest
    Lấy record mới nhất của mỗi symbol từ clean data
    """
    print("\n" + "="*60)
    print("📊 INDEXING CRYPTO_LATEST")
    print("="*60)
    
    # Đọc toàn bộ clean data
    clean_path = Path(CLEAN_PATH)
    if not clean_path.exists():
        print("❌ Clean data không tồn tại!")
        return
    
    df = spark.read.parquet(CLEAN_PATH)
    print(f"   📂 Đọc {df.count():,} records từ clean data")
    
    # Lấy record mới nhất của mỗi symbol (dùng window để lấy row_number)
    # Cột thời gian trong clean data là "crawl_ts"
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("symbol").orderBy(F.col("crawl_ts").desc())
    
    latest_df = df.withColumn("rn", F.row_number().over(window_spec)) \
        .filter(F.col("rn") == 1) \
        .drop("rn")
    
    # Convert to list of dicts
    rows = latest_df.collect()
    documents = []
    for row in rows:
        row_dict = row.asDict()
        doc = {
            "_id": row_dict.get("symbol"),  # Dùng symbol làm ID để upsert
            "symbol": row_dict.get("symbol"),
            "name": row_dict.get("name"),
            "price_usd": float(row_dict.get("current_price", 0) or 0),
            "market_cap": float(row_dict.get("market_cap", 0) or 0),
            "volume_24h": float(row_dict.get("total_volume", 0) or 0),
            "percent_change_1h": 0,  # Không có trong clean data
            "percent_change_24h": float(row_dict.get("price_change_percentage_24h", 0) or 0),
            "percent_change_7d": 0,  # Không có trong clean data
            "circulating_supply": float(row_dict.get("circulating_supply", 0) or 0),
            "total_supply": 0,
            "rank": int(row_dict.get("market_cap_rank", 0) or 0),
            "last_updated": _parse_timestamp(row_dict.get("crawl_ts"))
        }
        documents.append(doc)
    
    # Bulk index
    indexed = 0
    for i in range(0, len(documents), BULK_SIZE):
        batch = documents[i:i+BULK_SIZE]
        indexed += bulk_index(ES_INDEX_LATEST, batch)
        print(f"   📤 Indexed {indexed}/{len(documents)} documents...")
    
    print(f"   ✅ Hoàn thành! Indexed {indexed} coins vào {ES_INDEX_LATEST}")


def index_crypto_history(spark):
    """
    Index dữ liệu giá lịch sử vào crypto_history
    Dùng daily_price_stats (có OHLC data)
    """
    print("\n" + "="*60)
    print("📈 INDEXING CRYPTO_HISTORY")
    print("="*60)
    
    # Đọc daily stats từ aggregated
    daily_path = Path(AGG_PATH) / "daily_price_stats"
    if not daily_path.exists():
        print("❌ Daily stats không tồn tại!")
        return
    
    df = spark.read.parquet(str(daily_path))
    total_count = df.count()
    print(f"   📂 Đọc {total_count:,} records từ daily_price_stats")
    
    # Collect và index
    rows = df.collect()
    documents = []
    for row in rows:
        row_dict = row.asDict()
        # Tạo unique ID: symbol_date
        doc_id = f"{row_dict.get('symbol')}_{row_dict.get('date')}"
        doc = {
            "_id": doc_id,
            "symbol": row_dict.get("symbol"),
            "name": row_dict.get("name"),
            "date": str(row_dict.get("date")),
            "hour": 0,  # Daily data, không có hour
            "price_open": float(row_dict.get("price_open", 0) or 0),
            "price_high": float(row_dict.get("price_high", 0) or 0),
            "price_low": float(row_dict.get("price_low", 0) or 0),
            "price_close": float(row_dict.get("price_close", 0) or 0),
            "price_usd": float(row_dict.get("avg_price", 0) or 0),
            "volume_24h": float(row_dict.get("total_volume", 0) or 0),
            "market_cap": float(row_dict.get("avg_market_cap", 0) or 0),
            "record_count": int(row_dict.get("record_count", 0) or 0)
        }
        documents.append(doc)
    
    # Bulk index
    indexed = 0
    for i in range(0, len(documents), BULK_SIZE):
        batch = documents[i:i+BULK_SIZE]
        indexed += bulk_index(ES_INDEX_HISTORY, batch)
        if (i // BULK_SIZE) % 10 == 0:
            print(f"   📤 Indexed {indexed}/{len(documents)} documents...")
    
    print(f"   ✅ Hoàn thành! Indexed {indexed} records vào {ES_INDEX_HISTORY}")


def index_alerts(spark):
    """
    Index cảnh báo pump/dump vào alerts
    Đọc từ aggregated/pump_dump_alerts hoặc clean_alerts
    """
    print("\n" + "="*60)
    print("🚨 INDEXING ALERTS")
    print("="*60)
    
    # Đọc pump_dump_alerts từ aggregated
    alerts_path = Path(AGG_PATH) / "pump_dump_alerts"
    if not alerts_path.exists():
        print("⚠️  Chưa có alerts data. Sẽ tạo alerts từ extreme_movements.")
        
        # Đọc extreme_movements thay thế
        extreme_path = Path(AGG_PATH) / "extreme_movements"
        if not extreme_path.exists():
            print("❌ Không có extreme_movements data!")
            return
        
        df = spark.read.parquet(str(extreme_path))
        df = df.withColumn("alert_type", 
            F.when(F.col("percent_change_24h") > 0, "PUMP")
            .otherwise("DUMP")
        ).withColumn("severity",
            F.when(F.abs(F.col("percent_change_24h")) > 50, "CRITICAL")
            .when(F.abs(F.col("percent_change_24h")) > 20, "HIGH")
            .otherwise("MEDIUM")
        )
    else:
        df = spark.read.parquet(str(alerts_path))
    
    total_count = df.count()
    print(f"   📂 Đọc {total_count:,} alerts")
    
    if total_count == 0:
        print("   ⚠️  Không có alerts để index")
        return
    
    # Collect và index
    rows = df.collect()
    documents = []
    for i, row in enumerate(rows):
        row_dict = row.asDict()
        doc = {
            "_id": f"alert_{i}_{row_dict.get('symbol', 'unknown')}_{row_dict.get('date', '')}",
            "symbol": row_dict.get("symbol", ""),
            "name": row_dict.get("name", ""),
            "alert_type": row_dict.get("alert_type", "UNKNOWN"),
            "severity": row_dict.get("severity", "MEDIUM"),
            "price_change_percent": float(row_dict.get("percent_change_24h", 0)),
            "current_price": float(row_dict.get("price_usd", 0)),
            "detected_at": str(row_dict.get("date", datetime.now().date()))
        }
        documents.append(doc)
    
    # Bulk index
    indexed = 0
    for i in range(0, len(documents), BULK_SIZE):
        batch = documents[i:i+BULK_SIZE]
        indexed += bulk_index(ES_INDEX_ALERTS, batch)
    
    print(f"   ✅ Hoàn thành! Indexed {indexed} alerts vào {ES_INDEX_ALERTS}")


def show_stats():
    """Hiển thị thống kê các indices"""
    print("\n" + "="*60)
    print("📊 ELASTICSEARCH INDICES STATS")
    print("="*60)
    
    indices = [ES_INDEX_LATEST, ES_INDEX_HISTORY, ES_INDEX_ALERTS]
    for index in indices:
        try:
            response = requests.get(f"{ES_HOST}/{index}/_count")
            if response.status_code == 200:
                count = response.json()["count"]
                print(f"   {index}: {count:,} documents")
            else:
                print(f"   {index}: Error - {response.status_code}")
        except Exception as e:
            print(f"   {index}: Error - {e}")


def main():
    print("="*60)
    print("🚀 SPARK TO ELASTICSEARCH INDEXER")
    print("="*60)
    print(f"   📁 Clean Path: {CLEAN_PATH}")
    print(f"   📁 Aggregated Path: {AGG_PATH}")
    print(f"   🔗 Elasticsearch: {ES_HOST}")
    
    # Check ES
    if not check_elasticsearch():
        print("\n❌ Vui lòng khởi động Elasticsearch trước!")
        sys.exit(1)
    
    # Create indices if not exist
    create_indices()
    
    # Parse arguments
    args = sys.argv[1:] if len(sys.argv) > 1 else ["--all"]
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        if "--all" in args:
            index_crypto_latest(spark)
            index_crypto_history(spark)
            index_alerts(spark)
        else:
            if "--latest" in args:
                index_crypto_latest(spark)
            if "--history" in args:
                index_crypto_history(spark)
            if "--alerts" in args:
                index_alerts(spark)
        
        # Show final stats
        show_stats()
        
    finally:
        spark.stop()
    
    print("\n" + "="*60)
    print("✅ INDEXING HOÀN TẤT!")
    print("="*60)


if __name__ == "__main__":
    main()
