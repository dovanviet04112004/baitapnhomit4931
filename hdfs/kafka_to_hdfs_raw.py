"""\
Consume data from Kafka topic raw_crypto and write to partitioned storage
simulating HDFS with layout: hdfs/data/raw/dt=YYYY-MM-DD/hr=HH/*.jsonl
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:19093,localhost:19094").split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-raw")

# Base path for "HDFS" on local fs (hdfs/data/raw/)
BASE_DATA_DIR = Path(__file__).resolve().parent / "data" / "raw"


def ensure_partition_dir(dt: str, hour: str) -> Path:
    """Return partition folder path data/raw/dt=YYYY-MM-DD/hr=HH and create if missing."""
    part_dir = BASE_DATA_DIR / f"dt={dt}" / f"hr={hour}"
    part_dir.mkdir(parents=True, exist_ok=True)
    return part_dir


def open_partition_file(dt: str, hour: str) -> Path:
    """Return a JSONL file path for given partition. One file per hour."""
    part_dir = ensure_partition_dir(dt, hour)
    return part_dir / "data.jsonl"


def main(max_messages: Optional[int] = None) -> None:
    BASE_DATA_DIR.mkdir(parents=True, exist_ok=True)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        group_id="raw_to_hdfs",
    )

    print(f"✅ Connected to Kafka, consuming from topic {KAFKA_TOPIC}")
    print(f"📂 Writing partitions under {BASE_DATA_DIR}")

    current_dt: Optional[str] = None
    current_hr: Optional[str] = None
    current_path: Optional[Path] = None
    current_file = None

    def rotate_if_needed(dt_str: str, hr_str: str):
        nonlocal current_dt, current_hr, current_path, current_file
        if current_dt == dt_str and current_hr == hr_str and current_file is not None:
            return
        if current_file is not None:
            current_file.close()
        current_dt, current_hr = dt_str, hr_str
        current_path = open_partition_file(dt_str, hr_str)
        current_file = open(current_path, "a", encoding="utf-8")
        print(f"   ➜ Now writing to {current_path}")

    count = 0
    try:
        for msg in consumer:
            value = msg.value

            # Prefer fake_date if present, else derive from crawl_time
            dt_src = value.get("fake_date")
            if not dt_src:
                crawl_time = value.get("crawl_time")
                if not crawl_time:
                    # Skip records without time
                    continue
                try:
                    dt_parsed = datetime.fromisoformat(crawl_time.replace("Z", "+00:00"))
                except Exception:
                    continue
                dt_src = dt_parsed.date().isoformat()
                hour = f"{dt_parsed.hour:02d}"
            else:
                # If fake_date available, still derive hour from crawl_time
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
            assert current_file is not None
            current_file.write(json.dumps(value, ensure_ascii=False) + "\n")
            current_file.flush()  # Flush immediately
            count += 1

            if max_messages is not None and count >= max_messages:
                print(f"✅ Reached max_messages={max_messages}, stopping.")
                break

            if count % 10 == 0:  # Print more frequently
                print(f"   ... written {count} records so far ...")

    finally:
        if current_file is not None:
            current_file.close()
        consumer.close()
        print(f"🎉 Done. Total messages written: {count}")


if __name__ == "__main__":
    # Run forever, consuming from Kafka continuously
    main()  # main() now runs forever with the infinite for loop
