"""
Consume data from Kafka topic and write to HDFS
Layout: hdfs://namenode:9000/data/raw/dt=YYYY-MM-DD/hr=HH/*.jsonl
"""

import json
import os
from datetime import datetime
from typing import Optional
import time

from kafka import KafkaConsumer
from hdfs import InsecureClient

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092").split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-raw")
HDFS_NAMENODE_URL = os.getenv("HDFS_NAMENODE_URL", "http://hadoop-namenode:9870")
HDFS_DATA_DIR = os.getenv("HDFS_DATA_DIR", "/data")

# HDFS Client
hdfs_client = None


def get_hdfs_client():
    """Get or create HDFS client"""
    global hdfs_client
    if hdfs_client is None:
        print(f"🔗 Connecting to HDFS: {HDFS_NAMENODE_URL}")
        hdfs_client = InsecureClient(HDFS_NAMENODE_URL, user='root')
        print("✅ HDFS client connected")
    return hdfs_client


def ensure_hdfs_dir(path: str):
    """Create HDFS directory if not exists"""
    client = get_hdfs_client()
    try:
        client.status(path)
    except Exception:
        # Directory doesn't exist, create it
        client.makedirs(path)
        print(f"   📁 Created HDFS directory: {path}")


def get_partition_path(dt: str, hour: str) -> str:
    """Return HDFS partition path"""
    return f"{HDFS_DATA_DIR}/raw/dt={dt}/hr={hour}"


def get_file_path(dt: str, hour: str) -> str:
    """Return HDFS file path for partition with unique timestamp"""
    partition_path = get_partition_path(dt, hour)
    timestamp = int(time.time())
    return f"{partition_path}/data_{timestamp}.jsonl"


def write_to_hdfs(file_path: str, data: str):
    """Write data to HDFS file (overwrite mode, no lease conflict)"""
    client = get_hdfs_client()
    
    # Extract directory from file path
    dir_path = '/'.join(file_path.split('/')[:-1])
    ensure_hdfs_dir(dir_path)
    
    # Write to NEW file (overwrite=True, no lease conflict!)
    with client.write(file_path, overwrite=True, encoding='utf-8') as writer:
        writer.write(data)
    print(f"      ✅ Written to {file_path}")



def main(max_messages: Optional[int] = 500) -> None:  # Default 500 messages per batch
    # Wait for HDFS to be ready
    max_retries = 10
    for i in range(max_retries):
        try:
            client = get_hdfs_client()
            client.list('/')
            print("✅ HDFS is ready!")
            break
        except Exception as e:
            if i < max_retries - 1:
                print(f"⏳ Waiting for HDFS... ({i+1}/{max_retries})")
                time.sleep(5)
            else:
                print(f"❌ Failed to connect to HDFS: {e}")
                raise

    # Create base directory
    ensure_hdfs_dir(f"{HDFS_DATA_DIR}/raw")

    # Connect to Kafka
    print(f"🔗 Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        group_id="raw_to_hdfs",
        consumer_timeout_ms=10000  # Stop after 10s of no messages
    )

    print(f"✅ Connected to Kafka, consuming from topic: {KAFKA_TOPIC}")
    print(f"📂 Writing to HDFS: {HDFS_NAMENODE_URL}{HDFS_DATA_DIR}/raw/")
    print(f"🔢 Batch mode: Max {max_messages} messages per run")
    print("=" * 70)

    current_dt: Optional[str] = None
    current_hr: Optional[str] = None
    current_path: Optional[str] = None
    batch_buffer = []
    BATCH_SIZE = 10  # Write in batches for efficiency

    def flush_batch():
        nonlocal batch_buffer
        if not batch_buffer or current_path is None:
            return
        
        # Write all buffered records at once
        data_str = '\n'.join(batch_buffer)
        write_to_hdfs(current_path, data_str)
        batch_buffer = []

    def rotate_if_needed(dt_str: str, hr_str: str):
        nonlocal current_dt, current_hr, current_path
        if current_dt == dt_str and current_hr == hr_str:
            return
        
        # Flush any pending data
        flush_batch()
        
        current_dt, current_hr = dt_str, hr_str
        current_path = get_file_path(dt_str, hr_str)
        print(f"   ➜ Now writing to HDFS: {current_path}")

    count = 0
    try:
        for msg in consumer:
            value = msg.value

            # Get date and hour from crawl_time
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
            count += 1

            # Flush batch if full
            if len(batch_buffer) >= BATCH_SIZE:
                flush_batch()

            if max_messages is not None and count >= max_messages:
                print(f"✅ Reached max_messages={max_messages}, stopping.")
                break

            if count % 10 == 0:
                print(f"   ... written {count} records so far ...")

    except KeyboardInterrupt:
        print("\n🛑 Stopping consumer...")
    except Exception as e:
        print(f"\n⚠️  Consumer stopped: {e}")
    finally:
        flush_batch()  # Flush remaining data
        consumer.close()
        print("=" * 70)
        print(f"✅ BATCH COMPLETED - Total messages written: {count}")
        print("=" * 70)


if __name__ == "__main__":
    main()
