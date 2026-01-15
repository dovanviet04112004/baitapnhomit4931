# 🏗️ Kiến Trúc Hạ Tầng Deploy - Crypto Analytics Pipeline

**Dự án:** Hệ thống phân tích giá Cryptocurrency theo thời gian thực  
**Platform:** Google Kubernetes Engine (GKE)  
**Ngày cập nhật:** 15/01/2026

---

## 📋 Mục Lục

1. [Tổng Quan Kiến Trúc](#1-tổng-quan-kiến-trúc)
2. [Chi Tiết Các Thành Phần](#2-chi-tiết-các-thành-phần)
3. [Luồng Dữ Liệu](#3-luồng-dữ-liệu)
4. [Phân Tích Điểm Mạnh](#4-phân-tích-điểm-mạnh)
5. [Phân Tích Điểm Yếu](#5-phân-tích-điểm-yếu)
6. [Lý Do Thiết Kế](#6-lý-do-thiết-kế)
7. [So Sánh Với Production](#7-so-sánh-với-production)
8. [Kết Luận](#8-kết-luận)

---

## 1. Tổng Quan Kiến Trúc

### 1.1. Sơ Đồ Tổng Thể

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    GOOGLE KUBERNETES ENGINE (GKE)                            │
│                    Namespace: crypto-pipeline                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │  DATA INGESTION LAYER                                              │    │
│  ├────────────────────────────────────────────────────────────────────┤    │
│  │                                                                     │    │
│  │  ┌──────────────┐         ┌─────────────────────────────────┐     │    │
│  │  │  CoinGecko   │────────▶│  Crawler (Deployment)           │     │    │
│  │  │  API         │         │  - Replicas: 1                  │     │    │
│  │  │              │         │  - Schedule: Continuous         │     │    │
│  │  └──────────────┘         │  - Image: crawler:latest        │     │    │
│  │                           └─────────────┬───────────────────┘     │    │
│  └─────────────────────────────────────────┼─────────────────────────┘    │
│                                             ↓                               │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │  MESSAGE QUEUE LAYER                                               │    │
│  ├────────────────────────────────────────────────────────────────────┤    │
│  │                                                                     │    │
│  │  ┌──────────────────────────────────────────────────────────┐     │    │
│  │  │  Apache Kafka (StatefulSet)                              │     │    │
│  │  │  ┌─────────────────────────────────────────────────┐     │     │    │
│  │  │  │  Zookeeper (Deployment)                         │     │     │    │
│  │  │  │  - Replicas: 1                                  │     │     │    │
│  │  │  │  - Port: 2181                                   │     │     │    │
│  │  │  └─────────────────────────────────────────────────┘     │     │    │
│  │  │                                                           │     │    │
│  │  │  ┌─────────────────────────────────────────────────┐     │     │    │
│  │  │  │  Kafka Broker (StatefulSet)                     │     │     │    │
│  │  │  │  - Replicas: 1                                  │     │     │    │
│  │  │  │  - Port: 9092                                   │     │     │    │
│  │  │  │  - Topics: crypto-raw, clean_crypto, alerts,    │     │     │    │
│  │  │  │            market_sentiment                     │     │     │    │
│  │  │  └─────────────────────────────────────────────────┘     │     │    │
│  │  └──────────────────────────────────────────────────────────┘     │    │
│  └─────────────────────────────────────────────┬────────────────────┘    │
│                                                 ↓                           │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │  PROCESSING LAYER                                                  │    │
│  ├────────────────────────────────────────────────────────────────────┤    │
│  │                                                                     │    │
│  │  ┌──────────────────────────────────────────────────────────┐     │    │
│  │  │  Spark Streaming (Deployment)                            │     │    │
│  │  │  - Mode: local[2]                                        │     │    │
│  │  │  - Replicas: 1                                           │     │    │
│  │  │  - Resources: 2-4Gi RAM, 1-2 CPU                        │     │    │
│  │  │  - Checkpoint: PVC (5Gi)                                 │     │    │
│  │  │  - Jobs: Clean data, Alerts, Market sentiment           │     │    │
│  │  └──────────────────────────────────────────────────────────┘     │    │
│  │                                                                     │    │
│  │  ┌──────────────────────────────────────────────────────────┐     │    │
│  │  │  Spark Batch Jobs (CronJobs)                             │     │    │
│  │  │                                                           │     │    │
│  │  │  ┌─────────────────────────────────────────────────┐     │     │    │
│  │  │  │  spark-clean-gcs (Hourly: 5 * * * *)           │     │     │    │
│  │  │  │  - Clean raw data from GCS                      │     │     │    │
│  │  │  │  - Output: gs://bucket/data/clean/              │     │     │    │
│  │  │  └─────────────────────────────────────────────────┘     │     │    │
│  │  │                                                           │     │    │
│  │  │  ┌─────────────────────────────────────────────────┐     │     │    │
│  │  │  │  spark-agg-gcs (Daily: 0 1 * * *)               │     │     │    │
│  │  │  │  - Aggregate daily metrics                      │     │     │    │
│  │  │  │  - Output: gs://bucket/data/aggregated/         │     │     │    │
│  │  │  └─────────────────────────────────────────────────┘     │     │    │
│  │  │                                                           │     │    │
│  │  │  ┌─────────────────────────────────────────────────┐     │     │    │
│  │  │  │  spark-export-gcs (Daily: 0 2 * * *)            │     │     │    │
│  │  │  │  - Export to PostgreSQL & Elasticsearch         │     │     │    │
│  │  │  └─────────────────────────────────────────────────┘     │     │    │
│  │  └──────────────────────────────────────────────────────────┘     │    │
│  │                                                                     │    │
│  │  ┌──────────────────────────────────────────────────────────┐     │    │
│  │  │  Kafka-to-ES Consumer (Deployment)                       │     │    │
│  │  │  - Replicas: 1                                           │     │    │
│  │  │  - Read from Kafka → Write to Elasticsearch             │     │    │
│  │  └──────────────────────────────────────────────────────────┘     │    │
│  └─────────────────────────────────────────────┬────────────────────┘    │
│                                                 ↓                           │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │  STORAGE LAYER                                                     │    │
│  ├────────────────────────────────────────────────────────────────────┤    │
│  │                                                                     │    │
│  │  ┌──────────────────────────────────────────────────────────┐     │    │
│  │  │  Google Cloud Storage (GCS)                              │     │    │
│  │  │  - Bucket: crypto-pipeline-data                          │     │    │
│  │  │  - Structure:                                            │     │    │
│  │  │    ├─ data/raw/        (Raw Kafka data)                 │     │    │
│  │  │    ├─ data/clean/      (Cleaned data)                   │     │    │
│  │  │    └─ data/aggregated/ (Daily metrics)                  │     │    │
│  │  └──────────────────────────────────────────────────────────┘     │    │
│  │                                                                     │    │
│  │  ┌──────────────────────────────────────────────────────────┐     │    │
│  │  │  PostgreSQL (StatefulSet)                                │     │    │
│  │  │  - Replicas: 1                                           │     │    │
│  │  │  - Storage: PVC (10Gi)                                   │     │    │
│  │  │  - Database: crypto_analytics                            │     │    │
│  │  │  - Tables: daily_metrics, weekly_metrics, etc.          │     │    │
│  │  └──────────────────────────────────────────────────────────┘     │    │
│  │                                                                     │    │
│  │  ┌──────────────────────────────────────────────────────────┐     │    │
│  │  │  Elasticsearch (StatefulSet)                             │     │    │
│  │  │  - Replicas: 3                                           │     │    │
│  │  │  - Storage: PVC per node (50Gi each)                     │     │    │
│  │  │  - Indices: crypto_latest, crypto_history, alerts        │     │    │
│  │  └──────────────────────────────────────────────────────────┘     │    │
│  └─────────────────────────────────────────────┬────────────────────┘    │
│                                                 ↓                           │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │  VISUALIZATION & API LAYER                                         │    │
│  ├────────────────────────────────────────────────────────────────────┤    │
│  │                                                                     │    │
│  │  ┌──────────────────────────────────────────────────────────┐     │    │
│  │  │  Kibana (Deployment)                                     │     │    │
│  │  │  - Replicas: 1                                           │     │    │
│  │  │  - Service: LoadBalancer                                 │     │    │
│  │  │  - Port: 5601                                            │     │    │
│  │  └──────────────────────────────────────────────────────────┘     │    │
│  │                                                                     │    │
│  │  ┌──────────────────────────────────────────────────────────┐     │    │
│  │  │  Web Dashboard (Deployment)                              │     │    │
│  │  │  - FastAPI backend                                       │     │    │
│  │  │  - HTML/CSS/JS frontend                                  │     │    │
│  │  │  - Service: LoadBalancer                                 │     │    │
│  │  │  - Port: 8000                                            │     │    │
│  │  └──────────────────────────────────────────────────────────┘     │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2. Thống Kê Tài Nguyên

| Component | Type | Replicas | CPU | Memory | Storage |
|-----------|------|----------|-----|--------|---------|
| **Crawler** | Deployment | 1 | 0.5 | 512Mi | - |
| **Zookeeper** | Deployment | 1 | 0.5 | 1Gi | - |
| **Kafka** | StatefulSet | 1 | 1 | 2Gi | 10Gi PVC |
| **Spark Streaming** | Deployment | 1 | 1-2 | 2-4Gi | 5Gi PVC |
| **Spark Batch** | CronJob | 0-1 | 1-2 | 2-4Gi | - |
| **Kafka-to-ES** | Deployment | 1 | 0.5 | 1Gi | - |
| **PostgreSQL** | StatefulSet | 1 | 1 | 2Gi | 10Gi PVC |
| **Elasticsearch** | StatefulSet | 3 | 2 | 4Gi | 50Gi PVC × 3 |
| **Kibana** | Deployment | 1 | 0.5 | 1Gi | - |
| **Web Dashboard** | Deployment | 1 | 0.5 | 512Mi | - |
| **TOTAL** | - | **11-12** | **10-12** | **18-22Gi** | **175Gi** |

**Chi phí ước tính trên GKE:**
- **Compute:** 3× n1-standard-4 nodes = ~$300/month
- **Storage:** 175Gi SSD = ~$35/month
- **Load Balancers:** 2× = ~$36/month
- **Tổng:** ~**$371/month**

---

## 2. Chi Tiết Các Thành Phần

### 2.1. Data Ingestion Layer

#### **Crawler Service**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: crawler-streaming
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: crawler
        image: gcr.io/crypto-project-bigdata/crawler:latest
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka:9092"
        - name: CRAWL_INTERVAL
          value: "60"  # 1 phút
```

**Chức năng:**
- Thu thập dữ liệu từ CoinGecko API mỗi 1 phút
- Gửi raw data vào Kafka topic `crypto-raw`
- Xử lý 100 coins/request

**Throughput:**
- 100 coins × 1 request/minute = **100 messages/minute**
- ~144,000 messages/day
- ~4.3M messages/month

---

### 2.2. Message Queue Layer

#### **Apache Kafka**
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
spec:
  replicas: 1
  serviceName: kafka
  volumeClaimTemplates:
  - metadata:
      name: kafka-data
    spec:
      resources:
        requests:
          storage: 10Gi
```

**Topics:**
| Topic | Partitions | Retention | Producers | Consumers |
|-------|------------|-----------|-----------|-----------|
| `crypto-raw` | 3 | 7 days | Crawler | Spark Streaming, Kafka-to-GCS |
| `clean_crypto` | 3 | 7 days | Spark Streaming | Kafka-to-ES |
| `alerts` | 3 | 30 days | Spark Streaming | Kafka-to-ES |
| `market_sentiment` | 1 | 7 days | Spark Streaming | Kafka-to-ES |

**Lý do chọn 1 replica:**
- ✅ Đủ cho throughput hiện tại (100 msg/min)
- ✅ Tiết kiệm chi phí
- ⚠️ Không có replication (single point of failure)

---

### 2.3. Processing Layer

#### **Spark Streaming**

**Cấu hình:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-streaming
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: spark-streaming
        image: gcr.io/crypto-project-bigdata/spark-streaming:latest
        command: ["/opt/spark/bin/spark-submit"]
        args:
        - --master
        - local[2]  # ← Chạy local mode với 2 executor threads
        - --packages
        - org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1
        - --conf
        - spark.sql.streaming.checkpointLocation=/checkpoints
        - /app/streaming_processing.py
        
        volumeMounts:
        - name: checkpoint-dir
          mountPath: /checkpoints
      
      volumes:
      - name: checkpoint-dir
        persistentVolumeClaim:
          claimName: spark-streaming-checkpoint
```

**Streaming Jobs:**
1. **Clean Data Stream**
   - Input: Kafka `crypto-raw`
   - Processing: Parse JSON, validate, enrich
   - Output: Kafka `clean_crypto`

2. **Pump/Dump Alerts**
   - Input: Kafka `crypto-raw`
   - Processing: Detect price changes > 5% (1h) or > 10% (24h)
   - Output: Kafka `alerts`

3. **Market Sentiment**
   - Input: Kafka `crypto-raw`
   - Processing: Calculate % bullish/bearish coins
   - Output: Kafka `market_sentiment`

**Checkpoint Strategy:**
- **Storage:** PersistentVolumeClaim (5Gi)
- **Location:** `/checkpoints/`
- **Retention:** Persistent across pod restarts
- **Recovery:** Resume from last committed offset

**Lý do chọn local[2]:**
- ✅ Throughput thấp (100 msg/min) → local mode đủ
- ✅ Latency thấp (in-memory processing)
- ✅ Chi phí thấp (1 pod vs 4+ pods cho cluster)
- ✅ Đơn giản trong deployment và monitoring
- ⚠️ Không scale horizontally
- ⚠️ Single point of failure

---

#### **Spark Batch Jobs**

**1. spark-clean-gcs (Hourly)**
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: spark-clean-gcs
spec:
  schedule: "5 * * * *"  # Mỗi giờ, phút thứ 5
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: spark-clean
            image: gcr.io/crypto-project-bigdata/spark-batch-gcs:latest
            command: ["/opt/spark/bin/spark-submit"]
            args:
            - --master
            - local[2]
            - /app/data_cleaning.py
```

**Chức năng:**
- Đọc raw data từ GCS (`gs://bucket/data/raw/`)
- Clean, validate, deduplicate
- Ghi vào GCS (`gs://bucket/data/clean/`)

**2. spark-agg-gcs (Daily)**
```yaml
schedule: "0 1 * * *"  # Mỗi ngày lúc 1:00 AM
```

**Chức năng:**
- Đọc clean data từ GCS
- Tính toán 14 metrics:
  - Daily price stats (OHLC)
  - Hourly volume
  - Top pumps/dumps
  - Market cap distribution
  - BTC dominance
  - Correlation analysis
  - Whale detection
  - Rank changes
  - Market sentiment
  - Price heatmap
  - Alert counts
- Ghi vào GCS (`gs://bucket/data/aggregated/`)

**3. spark-export-gcs (Daily)**
```yaml
schedule: "0 2 * * *"  # Mỗi ngày lúc 2:00 AM
```

**Chức năng:**
- Đọc aggregated data từ GCS
- Export vào:
  - PostgreSQL (daily/weekly/monthly tables)
  - Elasticsearch (time-series indices)

---

### 2.4. Storage Layer

#### **Google Cloud Storage**
```
gs://crypto-pipeline-data/
├── data/
│   ├── raw/              # Raw Kafka data (7 days retention)
│   │   └── dt=2026-01-15/
│   │       └── hr=10/
│   │           └── *.jsonl
│   ├── clean/            # Cleaned data (30 days retention)
│   │   └── dt=2026-01-15/
│   │       └── *.parquet
│   └── aggregated/       # Daily metrics (365 days retention)
│       ├── daily_price_stats/
│       ├── hourly_volume/
│       └── ...
└── checkpoints/          # Spark streaming checkpoints
    └── ...
```

**Lợi ích:**
- ✅ Durable, highly available
- ✅ Cost-effective ($0.02/GB/month)
- ✅ Unlimited scalability
- ✅ Lifecycle management (auto-delete old data)

---

#### **PostgreSQL**
```sql
-- Schema
CREATE DATABASE crypto_analytics;

-- Tables
CREATE TABLE daily_metrics (
    date DATE PRIMARY KEY,
    total_market_cap BIGINT,
    total_volume BIGINT,
    btc_dominance DECIMAL(5,2),
    avg_change_24h DECIMAL(5,2),
    bullish_count INT,
    bearish_count INT
);

CREATE TABLE weekly_metrics (...);
CREATE TABLE monthly_metrics (...);
```

**Lý do sử dụng:**
- ✅ ACID transactions
- ✅ SQL queries cho dashboard
- ✅ Structured data storage
- ✅ Backup & restore

---

#### **Elasticsearch**
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: elasticsearch
spec:
  replicas: 3  # 3-node cluster
  volumeClaimTemplates:
  - metadata:
      name: es-data
    spec:
      resources:
        requests:
          storage: 50Gi
```

**Indices:**
| Index | Documents | Size | Retention |
|-------|-----------|------|-----------|
| `crypto_latest` | ~100 | <1MB | Latest only |
| `crypto_history` | ~4.3M/month | ~2GB/month | 90 days |
| `alerts` | ~500K/month | ~500MB/month | 30 days |

**Lý do sử dụng:**
- ✅ Full-text search
- ✅ Time-series data
- ✅ Fast aggregations
- ✅ Kibana integration

---

### 2.5. Visualization Layer

#### **Kibana**
- **Dashboards:** Market overview, alerts, trends
- **Visualizations:** Line charts, pie charts, heatmaps
- **Discover:** Ad-hoc queries

#### **Web Dashboard**
- **Backend:** FastAPI (Python)
- **Frontend:** HTML/CSS/JavaScript
- **Features:**
  - Real-time price updates
  - Historical charts
  - Alert notifications
  - Market sentiment indicators

---

## 3. Luồng Dữ Liệu

### 3.1. Real-time Flow (Speed Layer)

```
CoinGecko API
    ↓ (HTTP GET every 1 min)
Crawler
    ↓ (Kafka produce)
Kafka Topic: crypto-raw
    ↓ (Spark Streaming consume)
Spark Streaming (local[2])
    ├─ Clean Data → Kafka: clean_crypto
    ├─ Alerts → Kafka: alerts
    └─ Sentiment → Kafka: market_sentiment
    ↓ (Kafka-to-ES consume)
Elasticsearch
    ↓ (Query)
Kibana / Web Dashboard
```

**Latency:** < 2 minutes (end-to-end)

---

### 3.2. Batch Flow (Batch Layer)

```
Kafka: crypto-raw
    ↓ (Kafka-to-GCS consumer)
GCS: data/raw/
    ↓ (spark-clean-gcs, hourly)
GCS: data/clean/
    ↓ (spark-agg-gcs, daily 1 AM)
GCS: data/aggregated/
    ↓ (spark-export-gcs, daily 2 AM)
PostgreSQL + Elasticsearch
    ↓ (Query)
Web Dashboard
```

**Latency:** 1-2 hours (daily aggregation)

---

## 4. Phân Tích Điểm Mạnh

### 4.1. Kiến Trúc

#### ✅ **Lambda Architecture**
**Mô tả:** Kết hợp Speed Layer (Spark Streaming) và Batch Layer (Spark Batch)

**Lợi ích:**
- **Speed Layer** cung cấp real-time insights (< 2 min latency)
- **Batch Layer** đảm bảo accuracy với historical recomputation
- **Serving Layer** merge views từ cả 2 layers

**Ví dụ:**
- Real-time: Alert khi BTC giảm > 5% trong 1h
- Batch: Tính lại BTC dominance chính xác cho toàn bộ lịch sử

---

#### ✅ **Microservices Architecture**
**Mô tả:** Mỗi component là một service độc lập

**Lợi ích:**
- **Decoupling:** Crawler fail không ảnh hưởng Spark
- **Independent scaling:** Scale Elasticsearch mà không ảnh hưởng Kafka
- **Technology diversity:** Python (Crawler), Scala/Python (Spark), Java (Kafka)

---

#### ✅ **Event-Driven Architecture**
**Mô tả:** Sử dụng Kafka làm message bus

**Lợi ích:**
- **Asynchronous processing:** Producer không cần chờ consumer
- **Replay capability:** Có thể replay data từ Kafka
- **Multiple consumers:** Nhiều service đọc cùng 1 topic

---

### 4.2. Deployment

#### ✅ **Kubernetes Native**
**Lợi ích:**
- **Auto-restart:** Pod fail → K8s tự động restart
- **Resource management:** CPU/Memory limits & requests
- **Service discovery:** DNS-based (kafka:9092, postgres:5432)
- **Rolling updates:** Zero-downtime deployment

**Ví dụ:**
```bash
# Update Spark Streaming
kubectl set image deployment/spark-streaming \
  spark-streaming=gcr.io/project/spark-streaming:v2.0
# → K8s tự động rolling update
```

---

#### ✅ **Persistent Storage**
**Mô tả:** Sử dụng PVC cho stateful services

**Lợi ích:**
- **Data durability:** Pod restart không mất data
- **Checkpoint recovery:** Spark resume từ last offset
- **Database persistence:** PostgreSQL data không mất

**Ví dụ:**
```yaml
# Spark Streaming checkpoint
volumeMounts:
- name: checkpoint-dir
  mountPath: /checkpoints
volumes:
- name: checkpoint-dir
  persistentVolumeClaim:
    claimName: spark-streaming-checkpoint  # 5Gi
```

---

#### ✅ **Scheduled Jobs**
**Mô tả:** Sử dụng CronJob cho batch processing

**Lợi ích:**
- **Automated execution:** Không cần manual trigger
- **Resource efficiency:** Chỉ chạy khi cần (không idle)
- **Retry logic:** Auto-retry khi fail

**Ví dụ:**
```yaml
# Daily aggregation at 1 AM
schedule: "0 1 * * *"
backoffLimit: 3  # Retry 3 lần nếu fail
```

---

### 4.3. Cost Optimization

#### ✅ **Spark Local Mode**
**So sánh:**
| Mode | Pods | CPU | Memory | Cost/month |
|------|------|-----|--------|------------|
| **local[2]** | 1 | 2 | 4Gi | **$73** |
| Standalone | 4 | 5.5 | 11Gi | $328 |

**Tiết kiệm:** 78% ($255/month)

---

#### ✅ **Single Kafka Broker**
**So sánh:**
| Setup | Brokers | Storage | Cost/month |
|-------|---------|---------|------------|
| **Current** | 1 | 10Gi | **$15** |
| Production | 3 | 30Gi | $45 |

**Tiết kiệm:** 67% ($30/month)

---

#### ✅ **GCS for Cold Storage**
**So sánh:**
| Storage | Type | Cost/GB/month |
|---------|------|---------------|
| **GCS Standard** | Object | **$0.020** |
| Persistent Disk | Block | $0.170 |

**Tiết kiệm:** 88% cho large datasets

---

### 4.4. Scalability

#### ✅ **Horizontal Scaling Ready**
**Có thể scale:**
```bash
# Scale Elasticsearch
kubectl scale statefulset elasticsearch --replicas=5

# Scale Kafka consumers
kubectl scale deployment kafka-to-es --replicas=3

# Scale Web Dashboard
kubectl scale deployment webapp --replicas=5
```

---

#### ✅ **Vertical Scaling**
**Có thể tăng resources:**
```yaml
# Tăng Spark Streaming resources
resources:
  requests:
    memory: "4Gi"  # Tăng từ 2Gi
    cpu: "2000m"   # Tăng từ 1000m
  limits:
    memory: "8Gi"  # Tăng từ 4Gi
    cpu: "4000m"   # Tăng từ 2000m
```

---

### 4.5. Monitoring & Observability

#### ✅ **Kubernetes Native Monitoring**
```bash
# Check pod status
kubectl get pods -n crypto-pipeline

# View logs
kubectl logs -f spark-streaming-xxx

# Describe resources
kubectl describe pod spark-streaming-xxx
```

---

#### ✅ **Application Metrics**
**Spark Streaming:**
- Batch processing time
- Input rate (messages/sec)
- Processing rate
- Scheduling delay

**Kafka:**
- Consumer lag
- Throughput (MB/sec)
- Partition distribution

---

## 5. Phân Tích Điểm Yếu

### 5.1. High Availability

#### ❌ **Single Points of Failure**

**1. Kafka (1 replica)**
```
Vấn đề:
- Kafka pod fail → Toàn bộ pipeline dừng
- Không có replication → Data loss risk

Ảnh hưởng:
- Downtime: 2-5 phút (pod restart)
- Data loss: Messages trong memory chưa flush

Giải pháp:
- Tăng lên 3 replicas
- Enable replication factor = 3
- Cost: +$30/month
```

---

**2. Spark Streaming (1 replica)**
```
Vấn đề:
- Pod fail → Real-time processing dừng
- Alerts bị delay

Ảnh hưởng:
- Downtime: 1-2 phút (pod restart + checkpoint recovery)
- Alert delay: 1-5 phút

Giải pháp:
- Không thể scale horizontal (local mode)
- Cần migrate sang Spark Cluster mode
- Cost: +$200/month
```

---

**3. PostgreSQL (1 replica)**
```
Vấn đề:
- Pod fail → Dashboard không có data
- Backup manual

Ảnh hưởng:
- Downtime: 2-5 phút
- Data loss risk nếu PVC corrupt

Giải pháp:
- Deploy PostgreSQL HA (Patroni/Stolon)
- Automated backups to GCS
- Cost: +$50/month
```

---

### 5.2. Performance Bottlenecks

#### ⚠️ **Spark Local Mode Limitations**

**Vấn đề:**
```
Current capacity:
- 2 executor threads
- 4Gi memory shared
- Cannot scale horizontally

Bottleneck khi:
- Data volume > 10GB/day
- Processing time > 1 hour
- Multiple concurrent jobs
```

**Ví dụ:**
```
Scenario: Tăng từ 100 coins → 1000 coins
- Messages: 100/min → 1000/min (10×)
- Processing time: 1s → 10s (10×)
- Memory usage: 2Gi → 20Gi (10×) ← EXCEED LIMIT!

Result: OOM errors, pod restart loop
```

**Giải pháp:**
```
Option 1: Migrate to Spark Cluster
- 10 workers × 4 executors = 40 executors
- Linear scaling
- Cost: +$200/month

Option 2: Vertical scaling
- Increase to 16Gi memory, 8 CPU
- Limited by single node capacity
- Cost: +$100/month
```

---

#### ⚠️ **Network Latency**

**Vấn đề:**
```
Spark → Kafka → Elasticsearch
- 3 network hops
- Latency: ~50-100ms per hop
- Total: 150-300ms overhead
```

**Ảnh hưởng:**
```
Real-time alert delay:
- Ideal: < 1 second
- Actual: 1-2 minutes
  ├─ Kafka produce: 10ms
  ├─ Spark processing: 30-60s (batch interval)
  ├─ Kafka consume: 10ms
  └─ ES indexing: 100-500ms
```

---

### 5.3. Data Consistency

#### ⚠️ **Eventual Consistency**

**Vấn đề:**
```
Speed Layer vs Batch Layer có thể cho kết quả khác nhau:

Example:
- Speed Layer (Spark Streaming):
  BTC price at 10:00 AM = $87,856
  
- Batch Layer (Daily aggregation):
  BTC average price for 10:00-11:00 AM = $87,920
  
Difference: $64 (0.07%)
```

**Nguyên nhân:**
- Streaming: Approximate aggregation (watermark)
- Batch: Exact aggregation (full data scan)

**Giải pháp:**
- Accept eventual consistency
- Use batch layer as source of truth
- Speed layer chỉ cho real-time alerts

---

### 5.4. Security

#### ❌ **Không có Authentication/Authorization**

**Vấn đề:**
```
Current:
- Kafka: No authentication
- Elasticsearch: No authentication
- PostgreSQL: Hardcoded password trong YAML
- Kibana: Public access
```

**Rủi ro:**
```
1. Unauthorized access:
   - Bất kỳ pod nào cũng có thể read/write Kafka
   - Bất kỳ ai cũng có thể truy cập Kibana

2. Data breach:
   - Password trong Git repository
   - Secrets không encrypted

3. Compliance:
   - Không đáp ứng GDPR, SOC2
```

**Giải pháp:**
```yaml
# 1. Kafka SASL/SSL
apiVersion: v1
kind: Secret
metadata:
  name: kafka-credentials
type: Opaque
data:
  username: <base64>
  password: <base64>

# 2. Elasticsearch X-Pack Security
xpack.security.enabled: true
xpack.security.transport.ssl.enabled: true

# 3. External Secrets Operator
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: postgres-secret
spec:
  secretStoreRef:
    name: gcpsm-secret-store
  target:
    name: postgres-credentials
  data:
  - secretKey: password
    remoteRef:
      key: postgres-password

# 4. Network Policies
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: kafka-network-policy
spec:
  podSelector:
    matchLabels:
      app: kafka
  ingress:
  - from:
    - podSelector:
        matchLabels:
          app: spark-streaming  # Only Spark can access
```

**Cost:** +$0 (chỉ cần config)

---

#### ❌ **Không có Encryption**

**Vấn đề:**
```
Data in transit:
- Kafka: Plaintext
- Elasticsearch: HTTP (not HTTPS)
- PostgreSQL: No SSL

Data at rest:
- PVC: Not encrypted
- GCS: Default encryption (Google-managed keys)
```

**Giải pháp:**
```
1. Enable TLS:
   - Kafka: SSL/TLS
   - Elasticsearch: HTTPS
   - PostgreSQL: SSL mode=require

2. Encrypt PVC:
   - Use encrypted StorageClass
   - Customer-managed encryption keys (CMEK)
```

---

### 5.5. Monitoring & Alerting

#### ⚠️ **Limited Observability**

**Thiếu:**
```
1. Centralized Logging:
   - Logs scattered across pods
   - No log aggregation (Fluentd/Loki)
   - Hard to debug issues

2. Metrics:
   - No Prometheus metrics
   - No Grafana dashboards
   - No SLO/SLA tracking

3. Tracing:
   - No distributed tracing (Jaeger)
   - Cannot trace request flow

4. Alerting:
   - No PagerDuty/OpsGenie
   - No automated alerts
   - Manual monitoring required
```

**Giải pháp:**
```yaml
# 1. Deploy Prometheus + Grafana
kubectl apply -f https://raw.githubusercontent.com/prometheus-operator/kube-prometheus/main/manifests/setup/

# 2. Add ServiceMonitor for Spark
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: spark-streaming
spec:
  selector:
    matchLabels:
      app: spark-streaming
  endpoints:
  - port: metrics
    interval: 30s

# 3. Create Grafana dashboard
# 4. Setup alerts
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: spark-alerts
spec:
  groups:
  - name: spark
    rules:
    - alert: SparkStreamingDown
      expr: up{job="spark-streaming"} == 0
      for: 5m
      annotations:
        summary: "Spark Streaming is down"
```

**Cost:** +$50/month (monitoring infrastructure)

---

### 5.6. Disaster Recovery

#### ❌ **Không có Backup Strategy**

**Vấn đề:**
```
Current backup:
- PostgreSQL: Manual pg_dump
- Elasticsearch: No snapshots
- Kafka: 7 days retention only
- GCS: No versioning

Recovery Time Objective (RTO): Unknown
Recovery Point Objective (RPO): Unknown
```

**Rủi ro:**
```
Scenario: Accidental deletion
- kubectl delete namespace crypto-pipeline
- Result: Mất toàn bộ data!
- Recovery: Không thể (no backups)
```

**Giải pháp:**
```yaml
# 1. Automated PostgreSQL backups
apiVersion: batch/v1
kind: CronJob
metadata:
  name: postgres-backup
spec:
  schedule: "0 2 * * *"  # Daily 2 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: backup
            image: postgres:15
            command:
            - sh
            - -c
            - |
              pg_dump -h postgres -U postgres crypto_analytics | \
              gzip > /backup/backup-$(date +%Y%m%d).sql.gz
              gsutil cp /backup/*.sql.gz gs://bucket/backups/postgres/

# 2. Elasticsearch snapshots
PUT /_snapshot/gcs_repository
{
  "type": "gcs",
  "settings": {
    "bucket": "crypto-pipeline-backups",
    "base_path": "elasticsearch"
  }
}

PUT /_snapshot/gcs_repository/snapshot_$(date +%Y%m%d)

# 3. GCS versioning
gsutil versioning set on gs://crypto-pipeline-data

# 4. Velero for K8s cluster backup
velero install --provider gcp --bucket crypto-pipeline-velero
velero schedule create daily --schedule="0 3 * * *"
```

**Cost:** +$20/month (backup storage)

---

## 6. Lý Do Thiết Kế

### 6.1. Tại Sao Chọn Kubernetes?

#### **So sánh với Docker Compose:**

| Aspect | Docker Compose | Kubernetes |
|--------|----------------|------------|
| **Scalability** | Manual | Auto-scaling |
| **High Availability** | Single host | Multi-node cluster |
| **Service Discovery** | Links | DNS-based |
| **Load Balancing** | Manual | Built-in |
| **Rolling Updates** | Manual | Automated |
| **Self-healing** | No | Yes (auto-restart) |
| **Production-ready** | ❌ No | ✅ Yes |

**Kết luận:** Kubernetes phù hợp cho production deployment

---

### 6.2. Tại Sao Chọn Spark Local Mode?

#### **Phân tích quyết định:**

**Data volume hiện tại:**
```
100 coins × 1 message/minute × 60 minutes × 24 hours
= 144,000 messages/day
= ~144 MB/day (1KB/message)
= ~4.3 GB/month
```

**Processing requirements:**
```
Real-time:
- Latency target: < 2 minutes
- Throughput: 100 messages/minute = 1.67 msg/sec
- Complexity: Simple filters, aggregations

Batch:
- Daily aggregation: ~4 GB data
- Processing time: ~10 minutes
- Complexity: GroupBy, Join, Window functions
```

**So sánh modes:**

| Mode | Throughput | Latency | Cost | Complexity |
|------|------------|---------|------|------------|
| **local[2]** | **1.67 msg/sec** ✅ | **< 1s** ✅ | **$73/month** ✅ | **Low** ✅ |
| Standalone | 100+ msg/sec | 50-200ms | $328/month | Medium |
| YARN | 1000+ msg/sec | 100-500ms | $500+/month | High |
| Databricks | Unlimited | < 100ms | $2000+/month | Low |

**Kết luận:**
- ✅ Local[2] **ĐỦ** cho throughput hiện tại
- ✅ **Tiết kiệm** 78% chi phí
- ✅ **Đơn giản** hơn trong deployment
- ⚠️ Cần migrate khi scale > 10GB/day

---

### 6.3. Tại Sao Chọn Lambda Architecture?

#### **So sánh với Kappa Architecture:**

**Lambda:**
```
Pros:
✅ Batch layer đảm bảo accuracy
✅ Speed layer cho real-time
✅ Có thể recompute historical data
✅ Fault tolerance tốt hơn

Cons:
❌ Maintain 2 codebases (batch + streaming)
❌ Eventual consistency
❌ Phức tạp hơn
```

**Kappa:**
```
Pros:
✅ Single codebase (streaming only)
✅ Simpler architecture
✅ Strong consistency

Cons:
❌ Khó recompute historical data
❌ Streaming phải handle all logic
❌ Higher resource usage
```

**Lý do chọn Lambda:**
```
1. Crypto data cần accuracy:
   - Daily metrics phải chính xác 100%
   - Batch layer recompute toàn bộ data

2. Real-time alerts cần low latency:
   - Speed layer cho alerts < 2 min
   - Không cần 100% accurate

3. Cost optimization:
   - Batch jobs chỉ chạy 1-2 lần/ngày
   - Streaming chạy 24/7 nhưng simple logic
```

---

### 6.4. Tại Sao Chọn GCS Thay Vì HDFS?

**So sánh:**

| Feature | HDFS | GCS |
|---------|------|-----|
| **Durability** | 99.9% | 99.999999999% |
| **Availability** | 99% | 99.95% |
| **Cost** | $0.17/GB/month (PD) | $0.02/GB/month |
| **Scalability** | Limited by cluster | Unlimited |
| **Maintenance** | High (NameNode, DataNode) | Zero (managed) |
| **Backup** | Manual | Automated |
| **Lifecycle** | Manual | Automated |

**Kết luận:**
- ✅ GCS **rẻ hơn** 88%
- ✅ **Không cần** maintain HDFS cluster
- ✅ **Unlimited** scalability
- ✅ **Better** durability

---

### 6.5. Tại Sao Dùng Cả PostgreSQL VÀ Elasticsearch?

**Phân công nhiệm vụ:**

**PostgreSQL:**
```
Use cases:
✅ Structured data (daily/weekly/monthly metrics)
✅ ACID transactions
✅ Complex JOINs
✅ Dashboard queries với SQL

Example:
SELECT 
  date,
  total_market_cap,
  btc_dominance
FROM daily_metrics
WHERE date BETWEEN '2026-01-01' AND '2026-01-31'
ORDER BY date;
```

**Elasticsearch:**
```
Use cases:
✅ Time-series data (real-time prices, alerts)
✅ Full-text search
✅ Fast aggregations
✅ Kibana visualizations

Example:
GET /alerts/_search
{
  "query": {
    "bool": {
      "must": [
        {"match": {"alert_type": "PUMP_1H"}},
        {"range": {"alert_time": {"gte": "now-1h"}}}
      ]
    }
  },
  "aggs": {
    "by_coin": {
      "terms": {"field": "symbol"}
    }
  }
}
```

**Kết luận:** Mỗi database phục vụ use case riêng

---

## 7. So Sánh Với Production

### 7.1. Với Netflix

| Aspect | Our Project | Netflix |
|--------|-------------|---------|
| **Scale** | 100 coins, 144K msg/day | 100K+ executors, PB/day |
| **Spark Mode** | local[2] | YARN + Kubernetes |
| **Storage** | GCS (4GB/month) | S3 (PB) |
| **Monitoring** | Basic (kubectl logs) | Genie, Atlas, custom tools |
| **Team** | 1-2 người | 100+ engineers |
| **Cost** | $371/month | $10M+/month |

**Kết luận:** Kiến trúc của chúng ta phù hợp cho **small-medium scale**

---

### 7.2. Với Uber

| Aspect | Our Project | Uber |
|--------|-------------|------|
| **Data Volume** | 4GB/month | 100+ PB/day |
| **Spark Jobs** | 4 jobs (1 streaming, 3 batch) | 10,000+ jobs/day |
| **Architecture** | Lambda | Lambda + Kappa hybrid |
| **Custom Tools** | None | Marmaray, Hudi, Databook |
| **Latency** | 1-2 minutes | < 1 second |

**Kết luận:** Chúng ta dùng **proven patterns** nhưng ở **smaller scale**

---

### 7.3. Best Practices Đã Áp Dụng

✅ **1. Separation of Concerns**
- Crawler → Kafka → Spark → Storage → Visualization
- Mỗi layer độc lập

✅ **2. Idempotency**
- Spark batch jobs có thể chạy lại nhiều lần
- Kết quả không thay đổi

✅ **3. Checkpoint & Recovery**
- Spark Streaming checkpoint vào PVC
- Kafka offset tracking

✅ **4. Resource Limits**
- Mọi pod đều có CPU/Memory limits
- Tránh resource starvation

✅ **5. Scheduled Jobs**
- CronJob cho batch processing
- Automated execution

✅ **6. Persistent Storage**
- StatefulSet cho Kafka, PostgreSQL, Elasticsearch
- PVC cho data persistence

---

### 7.4. Best Practices Chưa Áp Dụng

❌ **1. High Availability**
- Single replicas cho Kafka, PostgreSQL
- No replication

❌ **2. Monitoring Stack**
- No Prometheus + Grafana
- No centralized logging

❌ **3. Security**
- No authentication
- No encryption
- Hardcoded secrets

❌ **4. Disaster Recovery**
- No automated backups
- No disaster recovery plan

❌ **5. CI/CD Pipeline**
- Manual deployment
- No automated testing

❌ **6. Auto-scaling**
- Fixed replicas
- No HPA (Horizontal Pod Autoscaler)

---

## 8. Kết Luận

### 8.1. Tóm Tắt

**Kiến trúc hiện tại:**
- ✅ **Phù hợp** cho small-medium scale (< 10GB/day)
- ✅ **Cost-effective** ($371/month)
- ✅ **Production-ready** (Kubernetes, Lambda architecture)
- ⚠️ **Có limitations** (HA, security, monitoring)

---

### 8.2. Roadmap Cải Tiến

#### **Phase 1: Stability (Tháng 1-2)**
```
Priority: HIGH
Cost: +$100/month

Tasks:
1. ✅ Implement monitoring (Prometheus + Grafana)
2. ✅ Setup automated backups
3. ✅ Add health checks & liveness probes
4. ✅ Implement retry logic
```

#### **Phase 2: Security (Tháng 3-4)**
```
Priority: HIGH
Cost: +$0 (config only)

Tasks:
1. ✅ Enable Kafka SASL/SSL
2. ✅ Enable Elasticsearch X-Pack
3. ✅ Use External Secrets Operator
4. ✅ Implement Network Policies
5. ✅ Enable TLS everywhere
```

#### **Phase 3: High Availability (Tháng 5-6)**
```
Priority: MEDIUM
Cost: +$200/month

Tasks:
1. ✅ Scale Kafka to 3 replicas
2. ✅ Deploy PostgreSQL HA (Patroni)
3. ✅ Migrate Spark to Cluster mode
4. ✅ Add load balancers
```

#### **Phase 4: Observability (Tháng 7-8)**
```
Priority: MEDIUM
Cost: +$50/month

Tasks:
1. ✅ Deploy Fluentd for log aggregation
2. ✅ Setup Jaeger for distributed tracing
3. ✅ Create Grafana dashboards
4. ✅ Setup PagerDuty alerts
```

#### **Phase 5: Optimization (Tháng 9-12)**
```
Priority: LOW
Cost: Variable

Tasks:
1. ✅ Implement auto-scaling (HPA)
2. ✅ Optimize Spark jobs
3. ✅ Add caching layer (Redis)
4. ✅ Implement data lifecycle policies
```

---

### 8.3. Khi Nào Cần Migrate?

**Signals để migrate sang production-grade:**

```
1. Data volume > 10GB/day
   → Migrate Spark to Cluster mode

2. Uptime requirement > 99.9%
   → Implement HA for all components

3. Security compliance (GDPR, SOC2)
   → Implement authentication, encryption, audit logs

4. Team size > 5 người
   → Implement CI/CD, monitoring, alerting

5. Revenue-generating
   → Invest in Databricks or managed services
```

---

### 8.4. Đánh Giá Tổng Thể

**Điểm mạnh:**
- ✅ Kiến trúc đúng đắn (Lambda, microservices, event-driven)
- ✅ Cost-effective cho scale hiện tại
- ✅ Sử dụng proven technologies (Kafka, Spark, K8s)
- ✅ Có khả năng scale khi cần

**Điểm yếu:**
- ⚠️ Thiếu HA (single points of failure)
- ⚠️ Thiếu security (no auth, no encryption)
- ⚠️ Thiếu monitoring (no observability stack)
- ⚠️ Thiếu disaster recovery (no backups)

**Kết luận:**
> Đây là một **proof-of-concept** tốt, thể hiện hiểu biết về Big Data architecture. Phù hợp cho **đồ án tốt nghiệp** hoặc **MVP**. Cần **bổ sung HA, security, monitoring** trước khi đưa vào **production thực tế**.

---

### 8.5. Câu Hỏi Thường Gặp (FAQ)

**Q1: Tại sao không dùng Spark Standalone Cluster?**
> A: Với throughput hiện tại (100 msg/min), local[2] đủ và tiết kiệm 78% chi phí. Sẽ migrate khi data volume > 10GB/day.

**Q2: Tại sao chỉ 1 Kafka broker?**
> A: Tiết kiệm chi phí cho POC. Production nên dùng 3 brokers với replication factor = 3.

**Q3: Làm sao đảm bảo data không mất khi pod restart?**
> A: Sử dụng PersistentVolumeClaim cho Spark checkpoint, Kafka data, PostgreSQL, Elasticsearch.

**Q4: Chi phí $371/month có đắt không?**
> A: Rất rẻ so với managed services (Databricks ~$2000/month). Phù hợp cho startup/POC.

**Q5: Có thể chạy trên AWS/Azure thay vì GCP không?**
> A: Có. Chỉ cần thay đổi:
> - GCS → S3/ADLS
> - GKE → EKS/AKS
> - Các service khác giữ nguyên

---

## 📚 Tài Liệu Tham Khảo

1. **Lambda Architecture**
   - Nathan Marz, "Big Data: Principles and best practices of scalable realtime data systems"
   - http://lambda-architecture.net/

2. **Spark on Kubernetes**
   - Apache Spark Documentation: https://spark.apache.org/docs/latest/running-on-kubernetes.html
   - Google Cloud Dataproc: https://cloud.google.com/dataproc

3. **Kafka Best Practices**
   - Confluent Documentation: https://docs.confluent.io/
   - "Kafka: The Definitive Guide" by Neha Narkhede

4. **Kubernetes Patterns**
   - "Kubernetes Patterns" by Bilgin Ibryam
   - CNCF Best Practices: https://www.cncf.io/

5. **Production Deployments**
   - Netflix Tech Blog: https://netflixtechblog.com/
   - Uber Engineering Blog: https://eng.uber.com/

---

**Tác giả:** Nhóm IT4931  
**Ngày:** 15/01/2026  
**Version:** 1.0
