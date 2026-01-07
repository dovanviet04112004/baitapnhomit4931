# 🚀 Pipeline Phân Tích Giá Crypto Theo Thời Gian Thực

Hệ thống Big Data phân tích giá cryptocurrency theo thời gian thực sử dụng các công nghệ: **Apache Kafka**, **Apache Spark**, **HDFS**, **Elasticsearch**, **Kibana**, **FastAPI**.

---

## 📐 Sơ Đồ Kiến Trúc Hệ Thống

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           CRYPTO ANALYTICS PIPELINE                                      │
└─────────────────────────────────────────────────────────────────────────────────────────┘

  ┌──────────────┐     ┌───────────────────────────────────────┐     ┌──────────────────┐
  │              │     │           APACHE KAFKA                 │     │                  │
  │  CoinGecko   │────▶│  ┌─────────┐ ┌─────────┐ ┌─────────┐  │────▶│   HDFS Storage   │
  │    API       │     │  │Broker 1 │ │Broker 2 │ │Broker 3 │  │     │                  │
  │              │     │  │ :19092  │ │ :19093  │ │ :19094  │  │     │  /data/raw/      │
  └──────────────┘     │  └─────────┘ └─────────┘ └─────────┘  │     │  (336 files)     │
         │             │                                        │     │                  │
         │             │  Topics:                               │     └────────┬─────────┘
         ▼             │  • raw_crypto                          │              │
  ┌──────────────┐     │  • clean_crypto                        │              │
  │   Crawler    │     │  • alerts                              │              ▼
  │              │     └───────────────────────────────────────┘     ┌──────────────────┐
  │ • Real-time  │                                                   │                  │
  │ • Fake Data  │                                                   │  APACHE SPARK    │
  └──────────────┘                                                   │                  │
                                                                     │  Batch Jobs:     │
                                                                     │  • Clean Data    │
                                                                     │  • 14 Analytics  │
                                                                     │                  │
                                                                     └────────┬─────────┘
                                                                              │
                       ┌──────────────────────────────────────────────────────┤
                       │                                                      │
                       ▼                                                      ▼
              ┌──────────────────┐                               ┌──────────────────────┐
              │                  │                               │                      │
              │  /data/clean/    │                               │  /data/aggregated/   │
              │  (170 parquet)   │                               │  (14 folders)        │
              │  1,010,980 rows  │                               │                      │
              │                  │                               │  • daily_price_stats │
              └────────┬─────────┘                               │  • pump_dump_alerts  │
                       │                                         │  • btc_dominance     │
                       │                                         │  • whale_detection   │
                       │                                         │  • ... (10 more)     │
                       │                                         └──────────┬───────────┘
                       │                                                    │
                       └────────────────────┬───────────────────────────────┘
                                            │
                                            ▼
                              ┌───────────────────────────┐
                              │                           │
                              │     ELASTICSEARCH         │
                              │     (localhost:9200)      │
                              │                           │
                              │  Indices:                 │
                              │  • crypto_latest (98)     │
                              │  • crypto_history (769)   │
                              │  • alerts (336,144)       │
                              │                           │
                              └─────────────┬─────────────┘
                                            │
                       ┌────────────────────┼────────────────────┐
                       │                    │                    │
                       ▼                    ▼                    ▼
              ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
              │              │     │              │     │              │
              │   KIBANA     │     │  FastAPI     │     │   Client     │
              │  Dashboard   │     │  REST API    │     │   Apps       │
              │              │     │              │     │              │
              │ :5601        │     │ :8000        │     │              │
              └──────────────┘     └──────────────┘     └──────────────┘
```

---

## 📋 Tổng Quan Hệ Thống

### Mô tả dự án
Xây dựng pipeline Big Data để thu thập, xử lý và phân tích dữ liệu giá cryptocurrency từ CoinGecko API. Hệ thống hỗ trợ:
- Thu thập dữ liệu real-time qua Kafka
- Lưu trữ phân tán trên HDFS
- Xử lý batch với Spark (13 analytics jobs)
- Tìm kiếm và query qua Elasticsearch
- Visualization qua Kibana Dashboard
- REST API cho ứng dụng client

### Công nghệ sử dụng

| Tầng | Công nghệ | Version | Mô tả |
|------|-----------|---------|-------|
| Data Source | CoinGecko API | - | Top 100 coins theo market cap |
| Message Queue | Apache Kafka | 3.x | Cluster 3 brokers, 3 topics |
| Storage | HDFS (Local) | - | Phân vùng theo ngày/giờ |
| Batch Processing | Apache Spark | 3.5.x | PySpark, 13 analytics jobs |
| Search Engine | Elasticsearch | 8.13.2 | 3 indices, full-text search |
| REST API | FastAPI | 0.100+ | Python async API |
| Visualization | Kibana | 8.13.2 | Dashboard tương tác |
| Runtime | Python | 3.11 | Ngôn ngữ chính |
| Runtime | Java JDK | 11 | Eclipse Adoptium |
| Container | Docker | - | Kafka cluster |
| Orchestration | Kubernetes | 1.28+ | Container orchestration |

---

## 📁 Cấu Trúc Thư Mục Chi Tiết

```
bigdata/
│
├── 📂 crawl/                              # TẦNG THU THẬP DỮ LIỆU
│   ├── crypto_crawler.py                  # Crawler gọi CoinGecko API 1 lần (test)
│   ├── crypto_crawler_streaming.py        # Crawler chạy liên tục 24/7, gửi Kafka
│   ├── send_fake_crypto_kafka.py          # Tạo fake data 2 tháng (Nov-Dec 2025)
│   ├── kafka_producer.py                  # Helper class gửi message lên Kafka
│   ├── requirements.txt                   # Python dependencies cho crawl
│   └── 📂 output/
│       └── crypto_raw.json                # Sample data 100 coins từ API
│
├── 📂 kafka/                              # TẦNG MESSAGE QUEUE
│   ├── docker-compose.yml                 # Kafka cluster config (3 brokers)
│   ├── create_topics.bat                  # Script tạo topics (Windows)
│   ├── create_topics.sh                   # Script tạo topics (Linux/Mac)
│   └── README.md                          # Hướng dẫn Kafka
│
├── 📂 hdfs/                               # TẦNG LƯU TRỮ PHÂN TÁN
│   ├── kafka_to_hdfs_raw.py               # Consumer: đọc Kafka → ghi HDFS
│   ├── 📂 checkpoints/                    # Kafka consumer offsets
│   └── 📂 data/
│       ├── 📂 raw/                        # Dữ liệu thô từ Kafka
│       │   └── dt=YYYY-MM-DD/hr=HH/       # Phân vùng theo ngày/giờ
│       │       └── *.jsonl                # 336 files, ~2M records
│       │
│       ├── 📂 clean/                      # Dữ liệu đã làm sạch
│       │   └── dt=YYYY-MM-DD/             # Phân vùng theo ngày
│       │       └── *.parquet              # 170 files, 1,010,980 records
│       │
│       ├── 📂 aggregated/                 # Dữ liệu đã tổng hợp (14 jobs)
│       │   ├── 📂 daily_price_stats/      # Giá OHLC theo ngày
│       │   ├── 📂 hourly_volume/          # Volume theo giờ
│       │   ├── 📂 top_pumps_dumps/        # Top 10 pump/dump
│       │   ├── 📂 market_cap_distribution/# Phân bố Large/Mid/Small cap
│       │   ├── 📂 btc_dominance/          # BTC % thị phần
│       │   ├── 📂 btc_correlation/        # Tương quan BTC-Altcoins
│       │   ├── 📂 coin_volume_ranking/    # Top 20 volume daily
│       │   ├── 📂 pump_dump_alerts/       # Cảnh báo bất thường
│       │   ├── 📂 price_heatmap/          # Heatmap giá theo giờ
│       │   ├── 📂 market_sentiment/       # % coins tăng/giảm
│       │   ├── 📂 whale_detection/        # Volume spike >200%
│       │   ├── 📂 rank_changes/           # Thay đổi ranking
│       │   ├── 📂 top_coin_trends/        # Xu hướng BTC/ETH/SOL
│       │   └── 📂 hourly_alert_counts/    # Số alerts theo giờ
│       │
│       └── processed_dates.txt            # Tracking ngày đã xử lý
│
├── 📂 spark/                              # TẦNG XỬ LÝ BATCH
│   ├── batch_processing.py                # 13 Spark analytics jobs
│   ├── spark_to_elasticsearch.py          # Index data từ HDFS → ES
│   ├── streaming_processing.py            # Spark Streaming (Phase 5)
│   ├── scheduler.py                       # Tự động chạy batch theo lịch
│   ├── run_clean_with_java11.cmd          # Script chạy batch jobs
│   ├── run_es_with_java11.cmd             # Script chạy ES indexer
│   ├── run_streaming.cmd                  # Script chạy streaming
│   ├── run_scheduler.cmd                  # Script chạy scheduler
│   └── 📂 logs/                           # Log files
│
├── 📂 elasticsearch/                      # TẦNG TÌM KIẾM & API
│   ├── elasticsearch_queries.py           # Query library (15+ functions)
│   ├── query_api.py                       # FastAPI REST server
│   ├── run_api_server.cmd                 # Script chạy API server
│   └── run_queries_test.cmd               # Script test queries
│
├── 📂 k8s/                                # TRIỂN KHAI KUBERNETES
│   ├── namespace.yaml                     # Namespace crypto-pipeline
│   ├── 📂 kafka/                          # Kafka cluster manifests
│   │   ├── zookeeper.yaml                 # Zookeeper deployment
│   │   ├── kafka-cluster.yaml             # StatefulSet 3 brokers
│   │   └── kafka-topics.yaml              # Job tạo topics
│   ├── 📂 spark/                          # Spark cluster manifests
│   │   ├── spark-master.yaml              # Master + Service
│   │   └── spark-worker.yaml              # Workers (2 replicas)
│   ├── 📂 elasticsearch/                  # ES cluster manifests
│   │   ├── elasticsearch-cluster.yaml     # StatefulSet 2 nodes
│   │   └── elasticsearch-setup.yaml       # Job tạo indices
│   ├── 📂 kibana/
│   │   └── kibana.yaml                    # Kibana deployment
│   ├── 📂 crawler/
│   │   └── crawler-cronjob.yaml           # CronJob mỗi 5 phút
│   ├── 📂 api/
│   │   └── query-api.yaml                 # FastAPI deployment
│   ├── 📂 storage/
│   │   ├── persistent-volumes.yaml        # PVCs
│   │   └── resource-quotas.yaml           # Resource limits
│   ├── deploy.sh                          # Deploy script Linux
│   ├── deploy.bat                         # Deploy script Windows
│   └── README.md                          # K8s documentation
│
├── 📂 docs/                               # TÀI LIỆU & BÁO CÁO
│   ├── README.md                          # Hướng dẫn tài liệu
│   ├── 📂 reports/                        # Báo cáo (.docx, .pdf)
│   ├── 📂 diagrams/                       # Sơ đồ kiến trúc
│   └── 📂 screenshots/                    # Ảnh demo
│
├── PLAN.md                                # Chi tiết 9 phases
└── README.md                              # File này
```

---

## 🔧 Yêu Cầu Cài Đặt

### 1. Phần mềm cần cài

| Phần mềm | Version | Download |
|----------|---------|----------|
| Python | 3.11+ | https://python.org |
| Java JDK | 11 | https://adoptium.net |
| Docker Desktop | Latest | https://docker.com |
| Elasticsearch | 8.13.2 | https://elastic.co |
| Kibana | 8.13.2 | https://elastic.co |

### 2. Cài đặt Python packages

```bash
# Cài tất cả packages cần thiết
pip install pyspark kafka-python requests fastapi uvicorn pandas
```

### 3. Cấu hình Java

```bash
# Windows - Set JAVA_HOME
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.29.7-hotspot
set PATH=%JAVA_HOME%\bin;%PATH%

# Kiểm tra
java -version
```

### 4. Cấu hình Elasticsearch

Mở file `config/elasticsearch.yml`:
```yaml
xpack.security.enabled: false
network.host: localhost
http.port: 9200
```

---

## 🚀 Hướng Dẫn Chạy Chi Tiết

### 📌 PHASE 1: Khởi động Kafka Cluster

```bash
# Bước 1: Vào thư mục kafka
cd bigdata/kafka

# Bước 2: Khởi động Docker containers
docker-compose up -d

# Bước 3: Kiểm tra containers đang chạy
docker ps

# Bước 4: Tạo Kafka topics
create_topics.bat
```

**Kết quả mong đợi:**
- 3 Kafka brokers chạy trên ports: 19092, 19093, 19094
- 3 Topics được tạo: `raw_crypto`, `clean_crypto`, `alerts`

---

### 📌 PHASE 2: Thu thập dữ liệu

#### Cách 1: Fake Data 2 tháng (khuyên dùng để test)

```bash
# Vào thư mục crawl
cd bigdata/crawl

# Chạy script fake data
python send_fake_crypto_kafka.py
```

**Output:**
- Gửi ~2 triệu records lên Kafka
- Data từ 01/11/2025 đến 31/12/2025
- 100 coins, mỗi giờ 1 record

#### Cách 2: Crawl real-time từ CoinGecko

```bash
cd bigdata/crawl
python crypto_crawler_streaming.py
```

---

### 📌 PHASE 3: Consumer Kafka → HDFS

```bash
# Mở terminal mới
cd bigdata/hdfs

# Chạy consumer
python kafka_to_hdfs_raw.py
```

**Kết quả:**
- Đọc messages từ Kafka topic `raw_crypto`
- Ghi vào `/hdfs/data/raw/dt=YYYY-MM-DD/hr=HH/*.jsonl`
- 336 files được tạo

---

### 📌 PHASE 4: Chạy Spark Batch Jobs

```bash
# Vào thư mục spark
cd bigdata/spark

# Chạy batch processing (dùng file .cmd để set Java 11)
run_clean_with_java11.cmd
```

**Hoặc chạy trực tiếp với Python:**

```bash
# Set Java trước
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.29.7-hotspot

# Chạy
python batch_processing.py --all
```

**Output (14 jobs):**

| Job | Output Path | Records |
|-----|-------------|---------|
| Clean Data | `/data/clean/` | 1,010,980 |
| daily_price_stats | `/data/aggregated/daily_price_stats/` | 801 |
| hourly_volume | `/data/aggregated/hourly_volume/` | ~19,000 |
| pump_dump_alerts | `/data/aggregated/pump_dump_alerts/` | 336,144 |
| ... | ... | ... |

---

### 📌 PHASE 5: Khởi động Elasticsearch & Kibana

```bash
# Terminal 1: Elasticsearch
cd C:\elasticsearch-8.13.2
bin\elasticsearch.bat

# Terminal 2: Kibana
cd C:\kibana-8.13.2
bin\kibana.bat
```

**Kiểm tra:**
- Elasticsearch: http://localhost:9200 → Hiện JSON info
- Kibana: http://localhost:5601 → Hiện giao diện

---

### 📌 PHASE 6: Index Data vào Elasticsearch

```bash
cd bigdata/spark

# Chạy indexer
run_es_with_java11.cmd
```

**Hoặc:**

```bash
python spark_to_elasticsearch.py --all
```

**Kết quả:**

| Index | Documents | Mô tả |
|-------|-----------|-------|
| `crypto_latest` | 98 | Giá mới nhất 98 coins |
| `crypto_history` | 769 | Lịch sử giá theo ngày |
| `alerts` | 336,144 | Cảnh báo pump/dump |

---

### 📌 PHASE 7: Chạy REST API Server

```bash
cd bigdata/elasticsearch

# Chạy API server
run_api_server.cmd
```

**Hoặc:**

```bash
python query_api.py
```

**Truy cập:**
- API: http://localhost:8000
- Swagger Docs: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

---

### 📌 PHASE 8: Tạo Kibana Dashboard

1. **Truy cập Kibana:** http://localhost:5601

2. **Tạo Data Views:**
   - Menu → Stack Management → Data Views → Create
   - Tạo 3 data views: `crypto_latest`, `crypto_history`, `alerts`

3. **Tạo Visualizations:**
   - Menu → Analytics → Visualize Library → Create
   - Tạo các charts: Pie, Bar, Line, Table...

4. **Tạo Dashboard:**
   - Menu → Analytics → Dashboards → Create
   - Add các visualizations vào dashboard

---

## 📊 Chi Tiết Dữ Liệu

### Schema Raw Data (từ CoinGecko)

```json
{
  "crawl_time": "2025-12-25T10:30:00Z",
  "source": "coingecko",
  "coin_id": "bitcoin",
  "symbol": "BTC",
  "name": "Bitcoin",
  "current_price": 87856.0,
  "price_change_24h": -803.12,
  "price_change_percentage_24h": -0.91,
  "market_cap": 1754575603847,
  "market_cap_rank": 1,
  "total_volume": 31654170811,
  "circulating_supply": 19970040,
  "high_24h": 88900.0,
  "low_24h": 87100.0
}
```

### Schema Clean Data (Parquet)

| Column | Type | Description |
|--------|------|-------------|
| `coin_id` | string | ID unique của coin |
| `symbol` | string | Mã coin (BTC, ETH) |
| `name` | string | Tên đầy đủ |
| `current_price` | double | Giá hiện tại USD |
| `market_cap` | double | Vốn hóa thị trường |
| `market_cap_rank` | int | Xếp hạng |
| `total_volume` | double | Volume 24h |
| `price_change_percentage_24h` | double | % thay đổi 24h |
| `circulating_supply` | double | Số coin lưu hành |
| `crawl_ts` | timestamp | Thời gian crawl |
| `dt` | string | Partition date (YYYY-MM-DD) |

### Aggregated Jobs Chi Tiết

| # | Job Name | Input | Output | Mô tả |
|---|----------|-------|--------|-------|
| 1 | `daily_price_stats` | clean | OHLC daily | Giá Open/High/Low/Close theo ngày |
| 2 | `hourly_volume` | clean | volume/hour | Volume giao dịch theo giờ |
| 3 | `top_pumps_dumps` | clean | top 10 | Coins biến động mạnh nhất 24h |
| 4 | `market_cap_distribution` | clean | 3 tiers | Large (>$10B), Mid ($1B-$10B), Small (<$1B) |
| 5 | `btc_dominance` | clean | % daily | % market cap của BTC |
| 6 | `btc_correlation` | clean | correlation | Hệ số tương quan BTC vs Altcoins |
| 7 | `coin_volume_ranking` | clean | top 20 daily | 20 coins volume cao nhất mỗi ngày |
| 8 | `pump_dump_alerts` | clean | alerts | Cảnh báo giá tăng/giảm bất thường |
| 9 | `price_heatmap` | clean | matrix | Ma trận giá theo coin × giờ |
| 10 | `market_sentiment` | clean | % bullish | % coins tăng vs giảm |
| 11 | `whale_detection` | clean | spikes | Phát hiện volume spike >200% |
| 12 | `rank_changes` | clean | tracking | Theo dõi thay đổi ranking |
| 13 | `top_coin_trends` | clean | trends | Xu hướng BTC/ETH/SOL/BNB |
| 14 | `hourly_alert_counts` | alerts | counts | Đếm alerts theo giờ |

---

## 🔌 REST API Endpoints Chi Tiết

### Market Endpoints

| Endpoint | Method | Params | Response |
|----------|--------|--------|----------|
| `/api/market/summary` | GET | - | Total market cap, volume, sentiment |
| `/api/market/distribution` | GET | - | Large/Mid/Small cap counts |

### Coins Endpoints

| Endpoint | Method | Params | Response |
|----------|--------|--------|----------|
| `/api/coins` | GET | `min_market_cap`, `max_price`, `min_change`, `limit` | Filtered coin list |
| `/api/coins/{symbol}` | GET | - | Chi tiết 1 coin |
| `/api/coins/{symbol}/history` | GET | `days` | Lịch sử giá |
| `/api/coins/{symbol}/trend` | GET | `days` | Xu hướng, % change |

### Rankings Endpoints

| Endpoint | Method | Params | Response |
|----------|--------|--------|----------|
| `/api/rankings/gainers` | GET | `limit` | Top coins tăng giá |
| `/api/rankings/losers` | GET | `limit` | Top coins giảm giá |
| `/api/rankings/market-cap` | GET | `limit` | Ranking theo market cap |

### Alerts Endpoints

| Endpoint | Method | Params | Response |
|----------|--------|--------|----------|
| `/api/alerts` | GET | `alert_type`, `severity`, `limit` | Danh sách alerts |
| `/api/alerts/summary` | GET | `days` | Tổng hợp theo type/severity |
| `/api/alerts/volume-spikes` | GET | - | Volume spike alerts |

### Search & Dashboard

| Endpoint | Method | Params | Response |
|----------|--------|--------|----------|
| `/api/search` | GET | `q` | Tìm kiếm coin theo tên/symbol |
| `/api/dashboard/overview` | GET | - | Tất cả data cho dashboard |

### Ví dụ Request/Response

```bash
# GET /api/market/summary
curl http://localhost:8000/api/market/summary

# Response:
{
  "total_market_cap": 3010000000000,
  "total_volume_24h": 119850000000,
  "coin_count": 98,
  "avg_change_24h": -0.08,
  "gainers_count": 33,
  "losers_count": 64,
  "market_sentiment": "BEARISH"
}
```

```bash
# GET /api/coins?min_market_cap=10000000000&limit=5
curl "http://localhost:8000/api/coins?min_market_cap=10000000000&limit=5"

# Response:
[
  {"symbol": "BTC", "name": "Bitcoin", "price_usd": 87856, "market_cap": 1754575603847},
  {"symbol": "ETH", "name": "Ethereum", "price_usd": 2980.63, "market_cap": 359722229523},
  ...
]
```

---

## 📊 Kết Quả Demo

### Market Summary
```
┌─────────────────────────────────────────┐
│         CRYPTO MARKET OVERVIEW          │
├─────────────────────────────────────────┤
│  Total Market Cap    │  $3.01 Trillion  │
│  Total Volume 24h    │  $119.85 Billion │
│  Tracked Coins       │  98              │
│  Avg Change 24h      │  -0.08%          │
│  Gainers             │  33 coins        │
│  Losers              │  64 coins        │
│  Market Sentiment    │  BEARISH         │
└─────────────────────────────────────────┘
```

### Top Gainers 24h
```
🚀 TOP GAINERS
1. CC     +18.50%  $0.17
2. PUMP   +5.03%   $0.002
3. M      +4.92%   $1.69
4. ICP    +3.48%   $2.91
5. ZEC    +3.20%   $527.33
```

### Top Losers 24h
```
📉 TOP LOSERS
1. NIGHT  -4.27%   $0.086
2. UNI    -4.04%   $5.67
3. HASH   -3.83%   $0.026
4. ONDO   -2.72%   $0.36
5. SUI    -2.65%   $1.41
```

### Market Cap Distribution
```
💰 MARKET CAP DISTRIBUTION
┌────────────────┬───────┬──────────────┐
│ Tier           │ Count │ Criteria     │
├────────────────┼───────┼──────────────┤
│ Large Cap      │ 16    │ > $10B       │
│ Mid Cap        │ 75    │ $1B - $10B   │
│ Small Cap      │ 7     │ < $1B        │
└────────────────┴───────┴──────────────┘

BTC Dominance: 58.22%
```

---

## 🛠 Troubleshooting

### 1. Lỗi Java version

```bash
# Lỗi: Unsupported class file major version
# Fix: Set Java 11

set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.29.7-hotspot
set PATH=%JAVA_HOME%\bin;%PATH%
java -version  # Phải hiện version 11
```

### 2. Elasticsearch connection refused

```bash
# Kiểm tra ES đang chạy
curl http://localhost:9200

# Nếu lỗi, kiểm tra:
# - ES đã start chưa
# - Port 9200 có bị block không
# - Firewall settings
```

### 3. Kibana không thấy data

```
Nguyên nhân: Time filter không đúng range

Fix:
1. Click "Last 15 minutes" (góc phải trên)
2. Đổi thành "Last 1 year" hoặc "Last 30 days"
3. Hoặc: Stack Management → Data Views → Edit → 
   Timestamp field: "I don't want to use time filter"
```

### 4. Kafka consumer không nhận data

```bash
# Kiểm tra Kafka đang chạy
docker ps

# Kiểm tra topics
docker exec -it kafka1 kafka-topics --list --bootstrap-server localhost:19092

# Kiểm tra consumer group
docker exec -it kafka1 kafka-consumer-groups --list --bootstrap-server localhost:19092
```

### 5. Spark job chạy chậm

```bash
# Tăng memory
set SPARK_DRIVER_MEMORY=4g

# Hoặc sửa trong code:
.config("spark.driver.memory", "4g")
.config("spark.executor.memory", "2g")
```

---

## ☸️ Phase 9: Triển Khai Kubernetes (Azure AKS)

### Tổng quan K8s Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   AZURE KUBERNETES SERVICE (AKS)                 │
│                   Namespace: crypto-pipeline                     │
├─────────────────────────────────────────────────────────────────┤
│  Node 1 (Standard_B2s)    Node 2 (Standard_B2s)    Node 3       │
│  ┌────────────────────┐   ┌────────────────────┐   ┌──────────┐ │
│  │ kafka-0            │   │ kafka-1            │   │ kafka-2  │ │
│  │ elasticsearch-0    │   │ elasticsearch-1    │   │ kibana   │ │
│  │ spark-worker-0     │   │ spark-worker-1     │   │ query-api│ │
│  └────────────────────┘   └────────────────────┘   └──────────┘ │
│                                                                  │
│  Services (NodePort):                                           │
│  • Kibana       : 30561 → http://localhost:5601                 │
│  • Query API    : 30800 → http://localhost:8000                 │
│  • Elasticsearch: 30920 → http://localhost:9200                 │
│  • Spark UI     : 30080 → http://localhost:8080                 │
└─────────────────────────────────────────────────────────────────┘
```

### Azure AKS Cluster Specs

| Resource | Chi tiết | Chi phí |
|----------|----------|---------|
| **AKS Control Plane** | Managed by Azure | FREE |
| **Node Pool** | 3x Standard_B2s (2 vCPU, 4GB RAM) | ~$90/tháng |
| **Storage** | Azure Managed Disks | ~$5/tháng |
| **Region** | Southeast Asia | - |
| **Total** | | **~$95/tháng** |

> 💡 Azure Free Account: $200 credit / 30 ngày
> 💡 Azure for Students: $100 credit / 12 tháng (không cần credit card)

### Cấu trúc thư mục K8s

```
k8s/
├── namespace.yaml                 # Namespace crypto-pipeline
├── kafka/
│   ├── zookeeper.yaml            # Zookeeper Deployment + Service
│   ├── kafka-cluster.yaml        # Kafka StatefulSet (3 brokers)
│   └── kafka-topics.yaml         # Job tạo topics
├── spark/
│   ├── spark-master.yaml         # Spark Master + Service
│   └── spark-worker.yaml         # Spark Worker (2 replicas)
├── elasticsearch/
│   ├── elasticsearch-cluster.yaml # ES StatefulSet (2 nodes)
│   └── elasticsearch-setup.yaml   # Job tạo indices
├── kibana/
│   └── kibana.yaml               # Kibana Deployment + Service
├── crawler/
│   └── crawler-cronjob.yaml      # CronJob chạy mỗi 5 phút
├── api/
│   └── query-api.yaml            # FastAPI Deployment + Service
├── storage/
│   ├── persistent-volumes.yaml   # PVCs cho data persistence
│   └── resource-quotas.yaml      # Resource limits
├── setup-aks.bat                 # ⭐ Tạo AKS cluster
├── deploy.bat                    # Deploy pipeline
├── status.bat                    # Kiểm tra trạng thái
├── port-forward.bat              # Truy cập services local
├── cleanup-aks.bat               # Xóa cluster
├── validate.bat                  # Validate YAML
└── README.md                     # Hướng dẫn K8s
```

### 🚀 Hướng Dẫn Deploy Azure AKS

#### Bước 1: Cài đặt Azure CLI

```powershell
# Cài Azure CLI
winget install Microsoft.AzureCLI

# Kiểm tra
az version
```

#### Bước 2: Đăng nhập Azure

```powershell
# Đăng nhập (mở browser)
az login

# Kiểm tra subscription
az account show
```

#### Bước 3: Tạo AKS Cluster

```powershell
cd bigdata/k8s

# Tạo cluster 3 nodes (5-10 phút)
setup-aks.bat
```

**Output mong đợi:**
```
NAME                                STATUS   ROLES   AGE   VERSION
aks-nodepool1-12345678-vmss000000   Ready    agent   5m    v1.28.3
aks-nodepool1-12345678-vmss000001   Ready    agent   5m    v1.28.3
aks-nodepool1-12345678-vmss000002   Ready    agent   5m    v1.28.3
```

#### Bước 4: Deploy Pipeline

```powershell
# Validate trước
validate.bat

# Deploy
deploy.bat

# Kiểm tra
status.bat
```

#### Bước 5: Truy cập Services

```powershell
# Mở port-forward
port-forward.bat

# Truy cập browser:
# http://localhost:5601  - Kibana Dashboard
# http://localhost:8000  - FastAPI Docs
# http://localhost:9200  - Elasticsearch
# http://localhost:8080  - Spark UI
```

#### Bước 6: XÓA KHI XONG (Quan trọng!)

```powershell
# Xóa để không mất tiền
cleanup-aks.bat

# Hoặc xóa thủ công
az group delete --name rg-crypto-pipeline --yes
```

### Thông số Resources

| Component | Replicas | CPU Request | Memory Request | Storage |
|-----------|----------|-------------|----------------|---------|
| Kafka | 3 | 250m | 512Mi | 5Gi/broker |
| Zookeeper | 1 | 250m | 256Mi | - |
| Spark Master | 1 | 500m | 1Gi | - |
| Spark Worker | 2 | 500m | 2Gi | - |
| Elasticsearch | 2 | 500m | 1Gi | 10Gi/node |
| Kibana | 1 | 250m | 512Mi | - |
| Query API | 2 | 100m | 256Mi | - |
| Crawler | CronJob | 100m | 128Mi | - |

### Kubernetes Services

| Service | Type | Internal Port | External Port |
|---------|------|---------------|---------------|
| kafka-headless | ClusterIP | 9092 | - |
| zookeeper | ClusterIP | 2181 | - |
| spark-master | ClusterIP | 7077, 8080 | 30080 |
| elasticsearch | NodePort | 9200 | 30920 |
| kibana | NodePort | 5601 | 30561 |
| query-api | NodePort | 8000 | 30800 |

### ⚠️ Lưu ý quan trọng

1. **Nhớ xóa cluster** sau khi demo để không mất credit
2. **Monitor chi phí** tại Azure Portal: https://portal.azure.com
3. **Set budget alert** để không vượt quá credit

---

## 📝 TODO - Các Phase Tiếp Theo

- [ ] **Phase 5**: Spark Structured Streaming (real-time processing)
- [x] **Phase 9**: Triển khai Kubernetes cluster ✅
- [ ] Thêm ML models dự đoán giá
- [ ] Alert qua Telegram/Discord
- [ ] Thêm nhiều nguồn data (Binance, CoinMarketCap)
- [ ] Tối ưu performance với partitioning

---

## 👥 Team Members

| STT | Thành viên | MSSV | Vai trò |
|-----|------------|------|---------|
| 1 | - | - | Data Engineering |
| 2 | - | - | Spark Processing |
| 3 | - | - | Elasticsearch & API |
| 4 | - | - | Kibana Dashboard |

---

## 📚 Tài Liệu Tham Khảo

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Elasticsearch Guide](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [Kibana Guide](https://www.elastic.co/guide/en/kibana/current/index.html)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [CoinGecko API](https://www.coingecko.com/en/api/documentation)

---

## 📄 License

MIT License - Free to use for educational purposes.

---

<p align="center">
  <b>🚀 Crypto Analytics Pipeline - Big Data Project 2025</b>
</p>

---

## ☁️ Deploy Lên Google Cloud Platform (GCP)

Project này hỗ trợ deploy lên GCP với Google Kubernetes Engine (GKE). 

### 🎓 Mới Bắt Đầu? Đọc Ngay!
- **[DEPLOY_FROM_ZERO.md](./DEPLOY_FROM_ZERO.md)** ⭐ - Hướng dẫn từ số 0, siêu chi tiết!
- **[QUICK_REFERENCE.md](./QUICK_REFERENCE.md)** - Commands cheat sheet

### 📚 Documentation Đầy Đủ
- **[GCP_STUDENT_MODE.md](./GCP_STUDENT_MODE.md)** ⭐ - Deploy tiết kiệm cho bài tập (93% rẻ hơn!)
- **[COST_COMPARISON.md](./COST_COMPARISON.md)** - So sánh chi phí chi tiết
- **[GCP_DEPLOYMENT_GUIDE.md](./GCP_DEPLOYMENT_GUIDE.md)** - Hướng dẫn đầy đủ
- **[QUICK_START_GCP.md](./QUICK_START_GCP.md)** - Quick start guide
- **[GCP_CHECKLIST.md](./GCP_CHECKLIST.md)** - Checklist và troubleshooting

### 🚀 Quick Deploy

**Standard Mode (Production):**
```bash
cd k8s
deploy-gcp.bat              # Windows
./deploy-gcp.sh             # Linux/Mac
```

**Student Mode (Bài Tập Lớn):** ⭐ RECOMMENDED
```bash
cd k8s
deploy-gcp-student.bat      # Windows
./deploy-gcp-student.sh     # Linux/Mac
```

### 💰 Cost Estimate
- **Standard**: ~$208/tháng (Production, 3 nodes)
- **Student Mode**: ~$15/tháng (93% rẻ hơn!) ⭐
- **Part-time**: ~$3-5/tháng (chỉ chạy khi cần)
- **Free Trial**: $300 credit → **FREE 20 tháng** với Student Mode! 🎉

### 🌐 Architecture on GCP
```
GKE Cluster (3 nodes)
  ├── Kafka (StatefulSet)
  ├── Spark (Master + Workers)
  ├── Elasticsearch (2 nodes)
  ├── Kibana (LoadBalancer)
  └── Query API (LoadBalancer)

Cloud Storage
  ├── /data/raw/
  ├── /data/clean/
  └── /data/aggregated/
```

Xem thêm chi tiết trong [GCP_DEPLOYMENT_GUIDE.md](./GCP_DEPLOYMENT_GUIDE.md)

---
