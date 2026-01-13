"""
Unified Export Job - Production Grade
Export aggregated metrics to both PostgreSQL and Elasticsearch

PostgreSQL: For API/UI queries
Elasticsearch: For Kibana analytics & dashboards

Strategy: Read HDFS once, write to both targets
"""
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Paths
HDFS_DATA_DIR = os.getenv("HDFS_DATA_DIR", "/app/data")
AGG_PATH = f"{HDFS_DATA_DIR}/aggregated"

# PostgreSQL connection
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_analytics")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Elasticsearch connection
ES_NODES = os.getenv("ES_NODES", "elasticsearch")
ES_PORT = os.getenv("ES_PORT", "9200")


def create_spark_session():
    """Create Spark session with PostgreSQL and Elasticsearch connectors"""
    return SparkSession.builder \
        .appName("UnifiedExport_Production") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar,/opt/spark/jars/elasticsearch-spark-30_2.12-8.11.0.jar") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()


def export_daily_metrics(spark):
    """Export daily metrics to PostgreSQL and Elasticsearch"""
    print("=" * 70)
    print("📊 EXPORTING DAILY METRICS (Unified)")
    print("=" * 70)
    
    current_year = datetime.now().year
    print(f"\n📅 Target year: {current_year}")
    
    # Read from HDFS (once!)
    print(f"\n📖 Reading from: {AGG_PATH}/daily_metrics/year={current_year}")
    try:
        df = spark.read.parquet(f"{AGG_PATH}/daily_metrics/year={current_year}")
        count = df.count()
        print(f"   ✅ Loaded {count:,} records")
    except Exception as e:
        print(f"   ❌ Error reading data: {e}")
        return
    
    # Prepare data for PostgreSQL (convert date to string)
    df_postgres = df.withColumn("date", F.col("date").cast("string"))
    
    # Prepare data for Elasticsearch
    # Create doc_id column for Elasticsearch document ID
    df_es = df.withColumn("@timestamp", F.col("date").cast("timestamp")) \
              .withColumn("doc_id", F.concat_ws("_", F.col("coin_id"), F.col("date").cast("string")))
    
    # Export to PostgreSQL
    print(f"\n💾 Exporting to PostgreSQL...")
    try:
        df_postgres.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "daily_metrics") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print(f"   ✅ PostgreSQL: {count:,} records")
    except Exception as e:
        print(f"   ❌ PostgreSQL error: {e}")
        raise
    
    # Export to Elasticsearch
    print(f"\n💾 Exporting to Elasticsearch...")
    try:
        df_es.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", ES_NODES) \
            .option("es.port", ES_PORT) \
            .option("es.resource", "daily_metrics") \
            .option("es.mapping.id", "doc_id") \
            .option("es.write.operation", "upsert") \
            .option("es.nodes.wan.only", "true") \
            .mode("append") \
            .save()
        print(f"   ✅ Elasticsearch: {count:,} records")
    except Exception as e:
        print(f"   ❌ Elasticsearch error: {e}")
        raise
    
    print("   ✅ Daily metrics export completed")


def export_weekly_metrics(spark):
    """Export weekly metrics to PostgreSQL and Elasticsearch"""
    print("\n" + "=" * 70)
    print("📊 EXPORTING WEEKLY METRICS (Unified)")
    print("=" * 70)
    
    current_year = datetime.now().year
    print(f"\n📅 Target year: {current_year}")
    
    # Read from HDFS
    print(f"\n📖 Reading from: {AGG_PATH}/weekly_metrics/year={current_year}")
    try:
        df = spark.read.parquet(f"{AGG_PATH}/weekly_metrics/year={current_year}")
        count = df.count()
        print(f"   ✅ Loaded {count:,} records")
    except Exception as e:
        print(f"   ⚠️  No weekly metrics found: {e}")
        return
    
    # Prepare for PostgreSQL
    df_postgres = df.withColumn("week_start_date", F.col("week_start_date").cast("string")) \
                    .withColumn("week_end_date", F.col("week_end_date").cast("string"))
    
    # Prepare for Elasticsearch
    # Create doc_id column for Elasticsearch document ID
    # Note: weekly_metrics doesn't have 'year' column
    df_es = df.withColumn("@timestamp", F.col("week_start_date").cast("timestamp")) \
              .withColumn("doc_id", F.concat_ws("_", F.col("coin_id"), F.col("week_of_year").cast("string")))
    
    # Export to PostgreSQL
    print(f"\n💾 Exporting to PostgreSQL...")
    try:
        df_postgres.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "weekly_metrics") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print(f"   ✅ PostgreSQL: {count:,} records")
    except Exception as e:
        print(f"   ❌ PostgreSQL error: {e}")
        raise
    
    # Export to Elasticsearch
    print(f"\n💾 Exporting to Elasticsearch...")
    try:
        df_es.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", ES_NODES) \
            .option("es.port", ES_PORT) \
            .option("es.resource", "weekly_metrics") \
            .option("es.mapping.id", "doc_id") \
            .option("es.write.operation", "upsert") \
            .option("es.nodes.wan.only", "true") \
            .mode("append") \
            .save()
        print(f"   ✅ Elasticsearch: {count:,} records")
    except Exception as e:
        print(f"   ❌ Elasticsearch error: {e}")
        raise
    
    print("   ✅ Weekly metrics export completed")


def export_monthly_metrics(spark):
    """Export monthly metrics to PostgreSQL and Elasticsearch"""
    print("\n" + "=" * 70)
    print("📊 EXPORTING MONTHLY METRICS (Unified)")
    print("=" * 70)
    
    current_year = datetime.now().year
    print(f"\n📅 Target year: {current_year}")
    
    # Read from HDFS
    print(f"\n📖 Reading from: {AGG_PATH}/monthly_metrics/year={current_year}")
    try:
        df = spark.read.parquet(f"{AGG_PATH}/monthly_metrics/year={current_year}")
        count = df.count()
        print(f"   ✅ Loaded {count:,} records")
    except Exception as e:
        print(f"   ⚠️  No monthly metrics found: {e}")
        return
    
    # Prepare for PostgreSQL
    df_postgres = df.withColumn("month_start_date", F.col("month_start_date").cast("string")) \
                    .withColumn("month_end_date", F.col("month_end_date").cast("string"))
    
    # Prepare for Elasticsearch
    # Create doc_id column for Elasticsearch document ID
    df_es = df.withColumn("@timestamp", F.col("month_start_date").cast("timestamp")) \
              .withColumn("doc_id", F.concat_ws("_", F.col("coin_id"), F.col("month_start_date").cast("string")))
    
    # Export to PostgreSQL
    print(f"\n💾 Exporting to PostgreSQL...")
    try:
        df_postgres.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "monthly_metrics") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print(f"   ✅ PostgreSQL: {count:,} records")
    except Exception as e:
        print(f"   ❌ PostgreSQL error: {e}")
        raise
    
    # Export to Elasticsearch
    print(f"\n💾 Exporting to Elasticsearch...")
    try:
        df_es.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", ES_NODES) \
            .option("es.port", ES_PORT) \
            .option("es.resource", "monthly_metrics") \
            .option("es.mapping.id", "doc_id") \
            .option("es.write.operation", "upsert") \
            .option("es.nodes.wan.only", "true") \
            .mode("append") \
            .save()
        print(f"   ✅ Elasticsearch: {count:,} records")
    except Exception as e:
        print(f"   ❌ Elasticsearch error: {e}")
        raise
    
    print("   ✅ Monthly metrics export completed")


def create_postgres_indexes(spark):
    """Create indexes on PostgreSQL tables"""
    print("\n" + "=" * 70)
    print("🔧 CREATING POSTGRESQL INDEXES")
    print("=" * 70)
    
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_metrics(date)",
        "CREATE INDEX IF NOT EXISTS idx_daily_coin ON daily_metrics(coin_id)",
        "CREATE INDEX IF NOT EXISTS idx_daily_symbol ON daily_metrics(symbol)",
        "CREATE INDEX IF NOT EXISTS idx_daily_return ON daily_metrics(return_pct_day DESC)",
    ]
    
    try:
        from py4j.java_gateway import java_import
        java_import(spark._jvm, "java.sql.DriverManager")
        
        conn = spark._jvm.DriverManager.getConnection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD)
        stmt = conn.createStatement()
        
        for idx_sql in indexes:
            print(f"   🔧 {idx_sql}")
            stmt.execute(idx_sql)
        
        stmt.close()
        conn.close()
        print("   ✅ PostgreSQL indexes created")
    except Exception as e:
        print(f"   ⚠️  Could not create indexes: {e}")


def main():
    """Main execution"""
    spark = create_spark_session()
    
    try:
        print("=" * 70)
        print("🚀 UNIFIED EXPORT JOB (PostgreSQL + Elasticsearch)")
        print("=" * 70)
        print(f"\n📊 Targets:")
        print(f"   - PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
        print(f"   - Elasticsearch: {ES_NODES}:{ES_PORT}")
        
        # Export all metrics (read once, write twice)
        export_daily_metrics(spark)
        export_weekly_metrics(spark)
        export_monthly_metrics(spark)
        
        # Create PostgreSQL indexes
        create_postgres_indexes(spark)
        
        # Summary
        print("\n" + "=" * 70)
        print("✅ UNIFIED EXPORT COMPLETED")
        print("=" * 70)
        print("\n📊 Data exported to:")
        print("   ✅ PostgreSQL: daily_metrics, weekly_metrics, monthly_metrics")
        print("   ✅ Elasticsearch: daily_metrics, weekly_metrics, monthly_metrics")
        print("\n💡 Next steps:")
        print("   - Query PostgreSQL for API/UI")
        print("   - Create Kibana dashboards for analytics")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ Error in unified export job: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
