"""
Kafka Consumer for Speed Layer - Real-time Data to Elasticsearch

Đọc real-time data từ Kafka topics và ghi vào Elasticsearch:
  - Topic: alerts → Index: crypto_alerts_realtime
  - Topic: market_sentiment → Index: crypto_sentiment_realtime  
  - Topic: clean_crypto → Index: crypto_clean_realtime

Kiến trúc Lambda - Speed Layer:
  Spark Streaming → Kafka → [Consumer này] → Elasticsearch → Kibana
"""

import json
import os
import time
from datetime import datetime
from typing import Dict, List
import requests
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Kafka config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:19093,localhost:19094").split(",")
ALERTS_TOPIC = "alerts"
SENTIMENT_TOPIC = "market_sentiment"

# Elasticsearch config
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_ALERTS_INDEX = "crypto_alerts_realtime"
ES_SENTIMENT_INDEX = "crypto_sentiment_realtime"

# Batch size for bulk indexing
BULK_SIZE = 50
FLUSH_INTERVAL_SECONDS = 5  # Flush mỗi 5 giây


def check_elasticsearch():
    """Kiểm tra Elasticsearch có đang chạy không"""
    try:
        response = requests.get(ES_HOST, timeout=5)
        if response.status_code == 200:
            info = response.json()
            print(f"✅ Elasticsearch: {info['cluster_name']} (v{info['version']['number']})")
            return True
    except Exception as e:
        print(f"❌ Không thể kết nối Elasticsearch: {e}")
        return False


def create_indices():
    """Tạo Elasticsearch indices nếu chưa tồn tại"""
    print("\n" + "="*70)
    print("🔧 CREATING ELASTICSEARCH INDICES")
    print("="*70)
    
    indices = {
        ES_ALERTS_INDEX: {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "1s"  # Real-time refresh
            },
            "mappings": {
                "properties": {
                    "alert_category": {"type": "keyword"},
                    "alert_type": {"type": "keyword"},  # PUMP_1H, DUMP_1H, PUMP_24H, DUMP_24H
                    "coin_id": {"type": "keyword"},
                    "symbol": {"type": "keyword"},
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "current_price": {"type": "double"},
                    "change_1h": {"type": "double"},
                    "change_24h": {"type": "double"},
                    "total_volume": {"type": "long"},
                    "crawl_time": {"type": "date"},
                    "alert_time": {"type": "date"}
                }
            }
        },
        ES_SENTIMENT_INDEX: {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "1s"
            },
            "mappings": {
                "properties": {
                    "window_start": {"type": "date"},
                    "total_coins": {"type": "integer"},
                    "bullish_count": {"type": "integer"},
                    "bearish_count": {"type": "integer"},
                    "neutral_count": {"type": "integer"},
                    "bullish_pct": {"type": "double"},
                    "bearish_pct": {"type": "double"},
                    "sentiment": {"type": "keyword"},  # BULLISH, BEARISH, NEUTRAL
                    "avg_change_24h": {"type": "double"}
                }
            }
        }
    }
    
    for index_name, settings in indices.items():
        response = requests.head(f"{ES_HOST}/{index_name}")
        if response.status_code == 200:
            print(f"   ✅ Index '{index_name}' đã tồn tại")
        else:
            response = requests.put(
                f"{ES_HOST}/{index_name}",
                headers={"Content-Type": "application/json"},
                data=json.dumps(settings)
            )
            if response.status_code == 200:
                print(f"   ✅ Đã tạo index '{index_name}'")
            else:
                print(f"   ❌ Lỗi tạo index '{index_name}': {response.text[:100]}")


def bulk_index_to_es(index_name: str, documents: List[Dict]) -> int:
    """
    Bulk index documents vào Elasticsearch
    Returns: số documents đã index thành công
    """
    if not documents:
        return 0
    
    bulk_data = []
    for doc in documents:
        # Tạo unique ID
        if "window_start" in doc:
            doc_id = f"{doc.get('window_start', '')}"
        elif "alert_time" in doc:
            doc_id = f"{doc.get('coin_id', 'unknown')}_{doc.get('alert_time', '')}"
        else:
            doc_id = f"{doc.get('coin_id', 'unknown')}_{doc.get('crawl_time', '')}"
        
        bulk_data.append(json.dumps({"index": {"_index": index_name, "_id": doc_id}}))
        bulk_data.append(json.dumps(doc, default=str))
    
    bulk_body = "\n".join(bulk_data) + "\n"
    
    try:
        response = requests.post(
            f"{ES_HOST}/_bulk",
            headers={"Content-Type": "application/x-ndjson"},
            data=bulk_body.encode('utf-8'),
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get("errors"):
                error_count = sum(1 for item in result["items"] if "error" in item.get("index", {}))
                return len(documents) - error_count
            return len(documents)
        else:
            print(f"   ❌ Bulk index error: {response.status_code}")
            return 0
    except Exception as e:
        print(f"   ❌ Exception during bulk index: {e}")
        return 0


class RealtimeConsumer:
    """Consumer cho Speed Layer - đọc từ Kafka và ghi vào Elasticsearch"""
    
    def __init__(self):
        self.alerts_buffer = []
        self.sentiment_buffer = []
        self.last_flush_time = time.time()
        self.total_alerts = 0
        self.total_sentiment = 0
        
    def create_consumer(self, topic: str, group_id: str):
        """Tạo Kafka consumer"""
        return KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="latest",  # Chỉ đọc data mới (real-time)
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            group_id=group_id,
            consumer_timeout_ms=1000  # Timeout để có thể flush định kỳ
        )
    
    def flush_buffers(self, force=False):
        """Flush buffers vào Elasticsearch"""
        current_time = time.time()
        should_flush = force or (current_time - self.last_flush_time >= FLUSH_INTERVAL_SECONDS)
        
        if not should_flush:
            return
        
        # Flush alerts
        if self.alerts_buffer:
            indexed = bulk_index_to_es(ES_ALERTS_INDEX, self.alerts_buffer)
            self.total_alerts += indexed
            print(f"   🚨 Alerts: Indexed {indexed} docs | Total: {self.total_alerts}")
            self.alerts_buffer = []
        
        # Flush sentiment
        if self.sentiment_buffer:
            indexed = bulk_index_to_es(ES_SENTIMENT_INDEX, self.sentiment_buffer)
            self.total_sentiment += indexed
            print(f"   😊 Sentiment: Indexed {indexed} docs | Total: {self.total_sentiment}")
            self.sentiment_buffer = []
        
        self.last_flush_time = current_time
    
    def process_alerts_message(self, message):
        """Xử lý message từ alerts topic"""
        data = message.value
        
        # Validate data
        if not data.get("coin_id") or not data.get("alert_type"):
            return
        
        self.alerts_buffer.append(data)
        
        # Flush nếu buffer đầy
        if len(self.alerts_buffer) >= BULK_SIZE:
            self.flush_buffers(force=True)
    
    def process_sentiment_message(self, message):
        """Xử lý message từ market_sentiment topic"""
        data = message.value
        
        # Validate data
        if not data.get("window_start") or not data.get("sentiment"):
            return
        
        self.sentiment_buffer.append(data)
        
        # Flush nếu buffer đầy
        if len(self.sentiment_buffer) >= BULK_SIZE:
            self.flush_buffers(force=True)
    
    def run(self):
        """Chạy consumer chính"""
        print("\n" + "="*70)
        print("🚀 STARTING REAL-TIME CONSUMER (SPEED LAYER)")
        print("="*70)
        print(f"📡 Kafka: {KAFKA_BOOTSTRAP}")
        print(f"📥 Topics: {ALERTS_TOPIC}, {SENTIMENT_TOPIC}")
        print(f"📤 Elasticsearch: {ES_HOST}")
        print(f"💾 Indices: {ES_ALERTS_INDEX}, {ES_SENTIMENT_INDEX}")
        print(f"⏱️  Flush interval: {FLUSH_INTERVAL_SECONDS}s")
        print("="*70)
        
        # Tạo 2 consumers (1 cho mỗi topic)
        alerts_consumer = self.create_consumer(ALERTS_TOPIC, "alerts_to_es")
        sentiment_consumer = self.create_consumer(SENTIMENT_TOPIC, "sentiment_to_es")
        
        print("\n✅ Consumers started! Waiting for messages...")
        print("   (Press Ctrl+C to stop)\n")
        
        try:
            while True:
                # Poll từ alerts topic
                alerts_messages = alerts_consumer.poll(timeout_ms=100, max_records=10)
                for topic_partition, messages in alerts_messages.items():
                    for message in messages:
                        self.process_alerts_message(message)
                
                # Poll từ sentiment topic
                sentiment_messages = sentiment_consumer.poll(timeout_ms=100, max_records=10)
                for topic_partition, messages in sentiment_messages.items():
                    for message in messages:
                        self.process_sentiment_message(message)
                
                # Flush định kỳ
                self.flush_buffers()
                
                # Sleep ngắn để tránh CPU 100%
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            print("\n\n🛑 Stopping consumer...")
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Flush remaining data
            print("\n📤 Flushing remaining data...")
            self.flush_buffers(force=True)
            
            # Close consumers
            alerts_consumer.close()
            sentiment_consumer.close()
            
            print("\n" + "="*70)
            print("📊 FINAL STATS")
            print("="*70)
            print(f"   Total Alerts indexed: {self.total_alerts}")
            print(f"   Total Sentiment indexed: {self.total_sentiment}")
            print("="*70)
            print("✅ Consumer stopped")


def main():
    """Main function"""
    # Check Elasticsearch
    if not check_elasticsearch():
        print("\n❌ Vui lòng khởi động Elasticsearch trước!")
        print("   Docker: docker-compose up -d elasticsearch")
        return
    
    # Create indices
    create_indices()
    
    # Run consumer
    consumer = RealtimeConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
