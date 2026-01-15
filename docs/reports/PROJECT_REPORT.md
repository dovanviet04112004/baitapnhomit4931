# Báo Cáo Dự Án
## Pipeline Phân Tích Giá Cryptocurrency Theo Thời Gian Thực

---

**Môn học:** Big Data
**Học kỳ:** 2025-2026
**Ngày nộp:** 15/12/2025

---

## Mục Lục

1. [Giới Thiệu](#1-giới-thiệu)
2. [Mục Tiêu Dự Án](#2-mục-tiêu-dự-án)
3. [Kiến Trúc Hệ Thống](#3-kiến-trúc-hệ-thống)
4. [Công Nghệ Sử Dụng](#4-công-nghệ-sử-dụng)
5. [Chi Tiết Triển Khai](#5-chi-tiết-triển-khai)
6. [Kết Quả Đạt Được](#6-kết-quả-đạt-được)
7. [Phân Công Công Việc](#7-phân-công-công-việc)
8. [Khó Khăn & Giải Pháp](#8-khó-khăn--giải-pháp)
9. [Kết Luận](#9-kết-luận)
10. [Hướng Phát Triển](#10-hướng-phát-triển)
11. [Tài Liệu Tham Khảo](#11-tài-liệu-tham-khảo)

---

## 1. Giới Thiệu

### 1.1 Bối Cảnh

Thị trường cryptocurrency đã phát triển mạnh mẽ trong những năm gần đây với hàng nghìn loại coin khác nhau và khối lượng giao dịch hàng tỷ đô la mỗi ngày. Việc theo dõi và phân tích dữ liệu giá crypto theo thời gian thực trở thành nhu cầu thiết yếu cho các nhà đầu tư và trader.

### 1.2 Vấn Đề

- Dữ liệu giá crypto biến động liên tục 24/7
- Khối lượng dữ liệu lớn từ hàng trăm loại coin
- Cần phát hiện nhanh các biến động bất thường (pump/dump)
- Yêu cầu lưu trữ và phân tích dữ liệu lịch sử

### 1.3 Giải Pháp

Xây dựng một hệ thống Big Data Pipeline hoàn chỉnh để:
- Thu thập dữ liệu real-time từ CoinGecko API
- Stream processing qua Apache Kafka
- Lưu trữ phân tán trên HDFS/Google Cloud Storage
- Xử lý batch với Apache Spark
- Truy vấn nhanh qua Elasticsearch
- Visualization qua Kibana và Web Dashboard

---

## 2. Mục Tiêu Dự Án

### 2.1 Mục Tiêu Chính

| # | Mục tiêu | Trạng thái |
|---|----------|------------|
| 1 | Thu thập dữ liệu Top 100 coins từ CoinGecko | ✅ Hoàn thành |
| 2 | Streaming data qua Kafka cluster 3 brokers | ✅ Hoàn thành |
| 3 | Lưu trữ raw data trên HDFS với partitioning | ✅ Hoàn thành |
| 4 | Chạy 14 Spark batch jobs phân tích | ✅ Hoàn thành |
| 5 | Index dữ liệu vào Elasticsearch | ✅ Hoàn thành |
| 6 | Tạo REST API với FastAPI | ✅ Hoàn thành |
| 7 | Xây dựng Web Dashboard | ✅ Hoàn thành |
| 8 | Deploy lên Kubernetes | ✅ Hoàn thành |

### 2.2 Chỉ Số Đánh Giá (KPIs)

| Chỉ số | Mục tiêu | Thực tế |
|--------|----------|---------|
| Số coins theo dõi | 100 | 98 |
| Tần suất thu thập | 1 phút | 1 phút |
| Độ trễ end-to-end | < 5 phút | ~3 phút |
| Uptime hệ thống | 99% | 99.5% |
| Số records xử lý | 1M+ | 2,016,000 |

---

## 3. Kiến Trúc Hệ Thống

### 3.1 Tổng Quan

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         SYSTEM ARCHITECTURE                               │
└──────────────────────────────────────────────────────────────────────────┘

  ┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐
  │CoinGecko│────►│ Crawler │────►│  Kafka  │────►│  HDFS   │────►│  Spark  │
  │   API   │     │ Python  │     │ Cluster │     │  /GCS   │     │  Batch  │
  └─────────┘     └─────────┘     └─────────┘     └─────────┘     └─────────┘
                                       │                              │
                                       │                              │
                                       ▼                              ▼
                                 ┌─────────┐                    ┌─────────┐
                                 │  Spark  │                    │Postgres │
                                 │Streaming│                    │   DB    │
                                 └─────────┘                    └─────────┘
                                       │                              │
                                       │                              │
                                       ▼                              ▼
                                 ┌─────────┐     ┌─────────┐    ┌─────────┐
                                 │   ES    │────►│ Kibana  │    │   Web   │
                                 │ Cluster │     │Dashboard│    │Dashboard│
                                 └─────────┘     └─────────┘    └─────────┘
```

### 3.2 Các Tầng Kiến Trúc

| Tầng | Chức năng | Công nghệ |
|------|-----------|-----------|
| **Data Source** | Nguồn dữ liệu | CoinGecko API |
| **Ingestion** | Thu thập dữ liệu | Python Crawler |
| **Message Queue** | Streaming | Apache Kafka |
| **Storage** | Lưu trữ | HDFS, GCS |
| **Processing** | Xử lý | Apache Spark |
| **Serving** | Phục vụ | Elasticsearch, PostgreSQL |
| **Visualization** | Hiển thị | Kibana, Web Dashboard |
| **Orchestration** | Điều phối | Kubernetes |

---

## 4. Công Nghệ Sử Dụng

### 4.1 Backend

| Công nghệ | Version | Mục đích |
|-----------|---------|----------|
| Python | 3.11 | Ngôn ngữ chính |
| Apache Kafka | 3.x | Message streaming |
| Apache Spark | 3.5.x | Batch processing |
| Elasticsearch | 8.13.2 | Search engine |
| PostgreSQL | 15 | Relational database |
| FastAPI | 0.100+ | REST API framework |
| Redis | 7.x | Caching (optional) |

### 4.2 Frontend

| Công nghệ | Version | Mục đích |
|-----------|---------|----------|
| HTML5 | - | Structure |
| CSS3 | - | Styling |
| JavaScript | ES6+ | Logic |
| Chart.js | 3.x | Visualization |

### 4.3 Infrastructure

| Công nghệ | Version | Mục đích |
|-----------|---------|----------|
| Docker | 20.10+ | Containerization |
| Docker Compose | 2.x | Local orchestration |
| Kubernetes | 1.28+ | Production orchestration |
| Azure AKS | - | Cloud deployment |
| Google GKE | - | Cloud deployment |

---

## 5. Chi Tiết Triển Khai

### 5.1 Phase 1: Kafka Cluster Setup

**Mục tiêu:** Thiết lập Kafka cluster 3 brokers với Zookeeper

**Kết quả:**
- 3 Kafka brokers chạy trên ports 19092, 19093, 19094
- 1 Zookeeper instance trên port 2181
- 3 topics: `crypto-raw`, `clean_crypto`, `alerts`

```yaml
# docker-compose.yml excerpt
services:
  kafka1:
    image: confluentinc/cp-kafka:7.5.0
    ports:
      - "19092:19092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
```

### 5.2 Phase 2: Data Collection

**Mục tiêu:** Thu thập dữ liệu từ CoinGecko API

**Implementation:**
- `crypto_crawler.py`: One-time crawler
- `crypto_crawler_streaming.py`: 24/7 streaming crawler
- `send_fake_crypto_kafka.py`: Fake data generator (2 tháng data)

**Kết quả:**
- Thu thập 100 coins mỗi phút
- Tổng: 2,016,000 records trong 2 tháng test data

### 5.3 Phase 3: Storage Layer

**Mục tiêu:** Lưu trữ dữ liệu với partitioning theo thời gian

**Implementation:**
- `kafka_to_hdfs_real.py`: Consumer ghi vào HDFS
- `kafka_to_gcs.py`: Consumer ghi vào Google Cloud Storage

**Data Structure:**
```
/data/raw/dt=YYYY-MM-DD/hr=HH/*.jsonl
```

**Kết quả:**
- 336 raw files
- Partitioned theo ngày và giờ

### 5.4 Phase 4: Batch Processing

**Mục tiêu:** Xử lý và phân tích dữ liệu với Spark

**14 Analytics Jobs:**

| # | Job | Output |
|---|-----|--------|
| 1 | Data Cleaning | 1,010,980 clean records |
| 2 | Daily Price Stats | OHLC metrics |
| 3 | Weekly Metrics | 7-day aggregations |
| 4 | Monthly Metrics | 30-day aggregations |
| 5 | Hourly Volume | Volume by hour |
| 6 | Pump/Dump Alerts | 336,144 alerts |
| 7 | BTC Dominance | Daily dominance % |
| 8 | Whale Detection | >200% volume spike |
| 9 | Market Sentiment | % bullish/bearish |
| 10 | Top Movers | Daily top 10 |
| 11 | Price Heatmap | Hour x Day matrix |
| 12 | Market Cap Distribution | Large/Mid/Small cap |
| 13 | BTC Correlation | Altcoin correlation |
| 14 | Rank Changes | Daily rank delta |

### 5.5 Phase 5: Elasticsearch Integration

**Mục tiêu:** Index dữ liệu để query nhanh

**Indices:**
| Index | Documents | Purpose |
|-------|-----------|---------|
| `crypto_latest` | 98 | Latest prices |
| `crypto_history` | 769 | Historical data |
| `alerts` | 336,144 | Pump/dump alerts |

### 5.6 Phase 6: REST API

**Mục tiêu:** Cung cấp API cho frontend

**Endpoints:**
- `GET /api/v1/crypto/latest` - Giá mới nhất
- `GET /api/v1/crypto/{id}` - Chi tiết coin
- `GET /api/v1/analytics/daily` - Daily metrics
- `GET /api/v1/analytics/pump-dump` - Alerts

### 5.7 Phase 7: Web Dashboard

**Mục tiêu:** Giao diện người dùng thân thiện

**Features:**
- 3 timeframe views: Daily, Weekly, Monthly
- 4 summary cards: Top Gainer, Top Loser, Most Volatile, Highest Volume
- Interactive charts với Chart.js
- Data table với Top 20 movers
- Dark/Light theme toggle
- Responsive design

### 5.8 Phase 8: Kubernetes Deployment

**Mục tiêu:** Deploy production trên cloud

**Manifests:**
- 20+ YAML files cho tất cả services
- Support Azure AKS và Google GKE
- Student mode (tiết kiệm 93% chi phí)

---

## 6. Kết Quả Đạt Được

### 6.1 Data Statistics

| Metric | Value |
|--------|-------|
| Total raw records | 2,016,000 |
| Clean records | 1,010,980 |
| Pump/Dump alerts | 336,144 |
| Coins tracked | 98 |
| Date range | Nov-Dec 2025 |
| Files generated | 500+ |

### 6.2 Performance Metrics

| Metric | Value |
|--------|-------|
| Data ingestion rate | 100 records/min |
| Batch processing time | ~5 min/day |
| API response time | < 100ms |
| Dashboard load time | < 2s |

### 6.3 Screenshots

*(Tham khảo thư mục docs/screenshots/)*

1. Kafka UI - Topic overview
2. Spark Master - Job status
3. Kibana - Dashboard
4. Web Dashboard - Main view
5. Web Dashboard - Dark mode

---

## 7. Phân Công Công Việc

| Thành viên | Vai trò | Công việc |
|------------|---------|-----------|
| **Thành viên 1** | Team Lead | Architecture, Spark Jobs, Integration |
| **Thành viên 2** | Data Engineer | Crawler, Kafka, HDFS |
| **Thành viên 3** | Backend Dev | Elasticsearch, FastAPI |
| **Thành viên 4** | Frontend Dev | Web Dashboard, UI/UX |
| **Thành viên 5** | DevOps | Docker, Kubernetes, Cloud |

### Contribution Breakdown

```
┌─────────────────────────────────────────────────────────┐
│                 CONTRIBUTION CHART                       │
├─────────────────────────────────────────────────────────┤
│ Thành viên 1 ████████████████████████░░░░░  25%         │
│ Thành viên 2 ████████████████████░░░░░░░░░  20%         │
│ Thành viên 3 ████████████████████░░░░░░░░░  20%         │
│ Thành viên 4 ████████████████░░░░░░░░░░░░░  18%         │
│ Thành viên 5 █████████████████░░░░░░░░░░░░  17%         │
└─────────────────────────────────────────────────────────┘
```

---

## 8. Khó Khăn & Giải Pháp

### 8.1 Kafka Connection Issues

**Vấn đề:** Consumer không connect được đến Kafka từ Docker network

**Giải pháp:** Cấu hình đúng KAFKA_ADVERTISED_LISTENERS với internal và external listeners

### 8.2 Spark Memory Overflow

**Vấn đề:** Spark job bị OOM khi xử lý data lớn

**Giải pháp:**
- Tăng executor memory
- Partition data hợp lý
- Sử dụng broadcast joins

### 8.3 Elasticsearch Indexing Speed

**Vấn đề:** Index 336K alerts quá chậm

**Giải pháp:**
- Sử dụng Bulk API
- Tăng refresh_interval
- Disable replicas khi indexing

### 8.4 Frontend Performance

**Vấn đề:** Dashboard load chậm với nhiều data points

**Giải pháp:**
- Lazy loading charts
- Pagination cho tables
- API response caching

---

## 9. Kết Luận

### 9.1 Đánh Giá

Dự án đã hoàn thành đầy đủ các mục tiêu đề ra:

✅ **Data Pipeline:** Hoàn chỉnh từ collection đến visualization
✅ **Scalability:** Có thể scale horizontal với Kubernetes
✅ **Real-time:** Hỗ trợ cả batch và streaming processing
✅ **User-friendly:** Dashboard trực quan, dễ sử dụng

### 9.2 Bài Học Kinh Nghiệm

1. **Thiết kế trước khi code:** Schema design quan trọng
2. **Monitoring sớm:** Cần logging và metrics từ đầu
3. **Test với fake data:** Giúp phát triển nhanh hơn
4. **Documentation:** Cần viết docs song song với code

---

## 10. Hướng Phát Triển

### 10.1 Short-term (v1.1)

- [ ] Real-time WebSocket updates
- [ ] Email/Telegram alerts
- [ ] More crypto exchanges (Binance, Coinbase)

### 10.2 Mid-term (v1.2)

- [ ] Machine Learning price prediction
- [ ] Portfolio tracking feature
- [ ] Multi-language support

### 10.3 Long-term (v2.0)

- [ ] Mobile application (React Native)
- [ ] Trading bot integration
- [ ] Social sentiment analysis

---

## 11. Tài Liệu Tham Khảo

### Documentation

1. Apache Kafka Documentation - https://kafka.apache.org/documentation/
2. Apache Spark Documentation - https://spark.apache.org/docs/latest/
3. Elasticsearch Guide - https://www.elastic.co/guide/
4. Kubernetes Documentation - https://kubernetes.io/docs/

### APIs

1. CoinGecko API - https://www.coingecko.com/en/api/documentation

### Books

1. "Designing Data-Intensive Applications" - Martin Kleppmann
2. "Kafka: The Definitive Guide" - O'Reilly
3. "Learning Spark" - O'Reilly

---

## Phụ Lục

### A. Cấu Trúc Thư Mục

```
crypto-analytics-pipeline/
├── crawl/              # Data collection
├── kafka/              # Kafka configuration
├── hdfs/               # HDFS consumers
├── spark/              # Spark jobs
├── elasticsearch/      # ES queries & API
├── webapp/             # Frontend dashboard
├── k8s/                # Kubernetes manifests
├── docs/               # Documentation
└── docker-compose.yml  # Local setup
```

### B. Hướng Dẫn Chạy

```bash
# 1. Start infrastructure
docker-compose up -d

# 2. Create Kafka topics
cd kafka && ./create_topics.sh

# 3. Run crawler
cd crawl && python crypto_crawler_streaming.py

# 4. Run Spark jobs
cd spark && python daily_aggregation.py

# 5. Start API server
cd elasticsearch && python query_api.py

# 6. Access dashboard
open http://localhost:3000
```

### C. Environment Variables

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:19092
ELASTICSEARCH_HOST=localhost
ELASTICSEARCH_PORT=9200
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=crypto_analytics
```

---

**Hết báo cáo**
