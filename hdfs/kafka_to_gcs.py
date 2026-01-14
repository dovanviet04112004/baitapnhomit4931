"""
Consume data from Kafka topic and write to Google Cloud Storage (GCS)
Layout: gs://bucket-name/data/raw/dt=YYYY-MM-DD/hr=HH/*.jsonl

GCS replaces HDFS with:
- Better scalability
- Managed service (no maintenance)
- Cheaper storage costs
- Native Spark integration
"""

import json
import os
from datetime import datetime
from typing import Optional
import time

from kafka import KafkaConsumer
from google.cloud import storage
from google.api_core import retry

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092").split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-raw")

# GCS config
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "crypto-pipeline-data")
GCS_DATA_DIR = os.getenv("GCS_DATA_DIR", "data")  # Base path in bucket

# GCS Client
gcs_client = None
gcs_bucket = None


def get_gcs_client():
    """Get or create GCS client"""
    global gcs_client, gcs_bucket
    if gcs_client is None:
        print(f"🔗 Connecting to Google Cloud Storage...")
        # Client will auto-detect credentials from:
        # 1. GOOGLE_APPLICATION_CREDENTIALS env var
        # 2. GKE workload identity
        # 3. gcloud auth application-default login
        gcs_client = storage.Client()
        gcs_bucket = gcs_client.bucket(GCS_BUCKET_NAME)
        print(f"✅ GCS client connected to bucket: {GCS_BUCKET_NAME}")
    return gcs_client, gcs_bucket


def get_partition_path(dt: str, hour: str) -> str:
    """Return GCS partition path"""
    return f"{GCS_DATA_DIR}/raw/dt={dt}/hr={hour}"


def get_file_path(dt: str, hour: str) -> str:
    """Return GCS file path for partition with unique timestamp"""
    partition_path = get_partition_path(dt, hour)
    timestamp = int(time.time())
    return f"{partition_path}/data_{timestamp}.jsonl"


@retry.Retry(deadline=60)
def write_to_gcs(file_path: str, data: str):
    """
    Write data to GCS file
    Uses retry decorator for transient errors
    """
    _, bucket = get_gcs_client()
    
    # Create blob (file) in GCS
    blob = bucket.blob(file_path)
    
    # Upload data
    blob.upload_from_string(
        data,
        content_type='application/json',
        timeout=30
    )
    
    print(f"      ✅ Written to gs://{GCS_BUCKET_NAME}/{file_path}")


def main(max_messages: Optional[int] = None) -> None:
    """
    Main function to consume from Kafka and write to GCS.
    
    Args:
        max_messages: Maximum number of messages to process. 
                      None or -1 means infinite (continuous mode).
                      Defaults to reading from MAX_MESSAGES env var, or None.
    """
    if max_messages is None:
        max_messages_env = os.getenv("MAX_MESSAGES", "")
        if max_messages_env and max_messages_env != "-1":
            max_messages = int(max_messages_env)
        # If -1 or empty, keep as None (infinite)
    """Main consumer loop"""
    
    # Test GCS connection
    max_retries = 5
    for i in range(max_retries):
        try:
            client, bucket = get_gcs_client()
            # Note: bucket.exists() requires storage.buckets.get permission
            # We only need storage.objects.* for uploading, so skip the check
            print(f"✅ GCS client connected to bucket: {GCS_BUCKET_NAME}")
            break
        except Exception as e:
            if i < max_retries - 1:
                print(f"⏳ Waiting for GCS... ({i+1}/{max_retries}): {e}")
                time.sleep(5)
            else:
                print(f"❌ Failed to connect to GCS: {e}")
                print("   Make sure:")
                print("   1. Bucket exists: gsutil mb gs://crypto-pipeline-data")
                print("   2. Credentials are set up (GOOGLE_APPLICATION_CREDENTIALS or Workload Identity)")
                raise

    # Connect to Kafka
    print(f"🔗 Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    
    # Set consumer_timeout_ms based on mode
    consumer_config = {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "auto_offset_reset": "earliest",
        "enable_auto_commit": False,  # Manual commit after GCS write succeeds
        "value_deserializer": lambda v: json.loads(v.decode("utf-8")),
        "key_deserializer": lambda k: k.decode("utf-8") if k else None,
        "group_id": "raw_to_gcs",
    }
    
    # In continuous mode (max_messages=None), don't timeout
    if max_messages is not None:
        consumer_config["consumer_timeout_ms"] = 10000
    
    consumer = KafkaConsumer(KAFKA_TOPIC, **consumer_config)

    print(f"✅ Connected to Kafka, consuming from topic: {KAFKA_TOPIC}")
    print(f"📂 Writing to GCS: gs://{GCS_BUCKET_NAME}/{GCS_DATA_DIR}/raw/")
    
    if max_messages is None:
        print(f"🔢 Continuous mode: Processing infinite messages")
    else:
        print(f"🔢 Batch mode: Max {max_messages} messages per run")
    
    print("=" * 70)

    current_dt: Optional[str] = None
    current_hr: Optional[str] = None
    batch_buffer = []
    batch_offsets = []  # Track offsets for manual commit
    BATCH_SIZE = 50  # Write 50 records per file (GCS optimized)

    def flush_batch():
        """Flush batch buffer to GCS and commit offsets"""
        nonlocal batch_buffer, batch_offsets
        if not batch_buffer or current_dt is None or current_hr is None:
            return
        
        # Generate new unique file path for each flush
        file_path = get_file_path(current_dt, current_hr)
        
        # Write all buffered records at once (newline-delimited JSON)
        data_str = '\n'.join(batch_buffer) + '\n'
        write_to_gcs(file_path, data_str)
        
        # Commit offsets ONLY after successful GCS write
        if batch_offsets:
            consumer.commit()
            batch_offsets = []
        
        batch_buffer = []

    def rotate_if_needed(dt_str: str, hr_str: str):
        """Rotate to new partition if date/hour changed"""
        nonlocal current_dt, current_hr
        if current_dt == dt_str and current_hr == hr_str:
            return
        
        # Flush any pending data from old partition
        flush_batch()
        
        current_dt, current_hr = dt_str, hr_str
        print(f"   ➜ Now writing to partition: gs://{GCS_BUCKET_NAME}/{get_partition_path(dt_str, hr_str)}/")

    count = 0
    try:
        for msg in consumer:
            value = msg.value

            # Get date and hour from crawl_time or fake_date
            dt_src = value.get("fake_date")
            if not dt_src:
                crawl_time = value.get("crawl_time")
                if not crawl_time:
                    continue
                try:
                    dt_parsed = datetime.fromisoformat(crawl_time.replace("Z", "+00:00"))
                except Exception:
                    continue
                dt_src = dt_parsed.date().isoformat()
                hour = f"{dt_parsed.hour:02d}"
            else:
                crawl_time = value.get("crawl_time")
                if crawl_time:
                    try:
                        dt_parsed = datetime.fromisoformat(crawl_time.replace("Z", "+00:00"))
                        hour = f"{dt_parsed.hour:02d}"
                    except Exception:
                        hour = "00"
                else:
                    hour = "00"

            rotate_if_needed(dt_src, hour)
            
            # Add to batch buffer
            batch_buffer.append(json.dumps(value, ensure_ascii=False))
            batch_offsets.append(msg)  # Track message for commit
            count += 1

            # Flush batch if full
            if len(batch_buffer) >= BATCH_SIZE:
                flush_batch()

            if max_messages is not None and count >= max_messages:
                print(f"✅ Reached max_messages={max_messages}, stopping.")
                break

            if count % 50 == 0:
                print(f"   ... written {count} records so far ...")

    except KeyboardInterrupt:
        print("\n🛑 Stopping consumer...")
    except Exception as e:
        print(f"\n⚠️  Consumer stopped: {e}")
        import traceback
        traceback.print_exc()
    finally:
        flush_batch()  # Flush remaining data
        consumer.close()
        print("=" * 70)
        print(f"✅ BATCH COMPLETED - Total messages written: {count}")
        print(f"📊 GCS location: gs://{GCS_BUCKET_NAME}/{GCS_DATA_DIR}/raw/")
        print("=" * 70)


if __name__ == "__main__":
    main()
