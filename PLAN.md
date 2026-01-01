# Pipeline Phân Tích Giá Crypto Theo Thời Gian Thực - Danh Sách Công Việc

## Phase 1: Nguồn Dữ Liệu & Thiết Kế Schema
- [x] Xác định nguồn dữ liệu: **CoinGecko API** (top 100 coins theo market cap)
- [x] Ghi chép API endpoints:
  - `/coins/markets` - lấy giá và thông tin thị trường
  - `/coins/list` - danh sách tất cả coins
- [x] Thiết kế schema toàn diện với các trường:
  - [x] crawl_time, source, coin_id, symbol, name
  - [x] current_price, price_change_24h, price_change_percentage_24h
  - [x] market_cap, market_cap_rank, total_volume
  - [x] circulating_supply, total_supply, max_supply
  - [x] high_24h, low_24h, ath, atl
  - [x] image_url, last_updated
- [x] Ghi chép các thách thức:
  - Rate limit: 10-30 calls/phút (free tier)
  - Coin ra/vào top 100 theo ngày
  - Stablecoins có volatility thấp

## Phase 2: Tầng Thu Thập Dữ Liệu Streaming
- [x] Triển khai crawler gọi API mỗi 60 giây
- [x] Thiết lập Kafka cluster (3 brokers: 19092, 19093, 19094)
- [x] Tạo các Kafka topics:
  - [x] `raw_crypto` - dữ liệu thô từ API
  - [x] `clean_crypto` - dữ liệu đã làm sạch
  - [x] `alerts` - cảnh báo pump/dump
- [x] Triển khai Kafka producer trong crawler
- [x] Thêm xử lý lỗi và logic thử lại
- [x] Fake data 2 tháng (11-12/2025) với:
  - [x] Biến động giá realistic
  - [x] Coin ra/vào top 100
  - [x] Rank thay đổi theo market cap

## Phase 3: Tầng Lưu Trữ Phân Tán
- [x] Cấu hình HDFS cho lưu trữ phân tán (local simulation)
- [x] Triển khai chiến lược phân vùng: `dt=YYYY-MM-DD/hr=HH`
- [x] Thiết lập định dạng lưu trữ JSONL
- [x] Tạo cấu trúc thư mục:
  - [x] `/data/raw/` - dữ liệu thô từ Kafka (2,016,000 records, 336 files)
  - [ ] `/data/clean/` - dữ liệu đã làm sạch (Spark Phase 4)
  - [ ] `/data/aggregated/` - dữ liệu tổng hợp batch (Spark Phase 4)

## Phase 4: Tầng Xử Lý Batch
- [x] Triển khai các Spark batch jobs (13 jobs):
  
  **Basic Analytics (Job 1-6):**
  - [x] Job 1: Giá trung bình theo ngày/coin (daily_price_stats)
  - [x] Job 2: Top 10 coins pump/dump lớn nhất 24h (top_pumps_dumps)
  - [x] Job 3: Phân bố market cap - Large/Mid/Small cap (market_cap_distribution)
  - [x] Job 4: Xu hướng giá BTC, ETH, top altcoins (top_coin_trends)
  - [x] Job 5: Volume analysis theo giờ (hourly_volume)
  - [x] Job 6: Correlation giữa BTC và altcoins (btc_correlation)
  
  **Advanced Analytics (Job 7-13):**
  - [x] Job 7: Top 20 coins theo volume mỗi ngày (coin_volume_ranking)
  - [x] Job 8: Phát hiện pump/dump alerts (pump_dump_alerts)
  - [x] Job 9: BTC Dominance theo thời gian (btc_dominance)
  - [x] Job 10: Price heatmap theo coin × giờ (price_heatmap)
  - [x] Job 11: Market sentiment - % coins tăng/giảm (market_sentiment)
  - [x] Job 12: Whale detection - volume spike >200% (whale_detection)
  - [x] Job 13: Rank changes tracking (rank_changes)

- [x] Tạo clean data (validated + selected columns)
- [ ] Thiết lập incremental mode (chỉ xử lý data mới)
- [ ] Thiết lập lịch trình job (cron/Airflow)

## Phase 5: Tầng Xử Lý Thời Gian Thực
- [ ] Triển khai Spark Structured Streaming consumer
- [ ] Các view thời gian thực:
  - [ ] Giá mới nhất của từng coin
  - [ ] Phát hiện pump/dump (>5% trong 1h, >10% trong 24h)
  - [ ] Whale alert (volume đột biến >200%)
  - [ ] Market sentiment (% coin tăng vs giảm)
- [ ] Tạo cảnh báo real-time gửi vào topic `alerts`
- [ ] Cấu hình checkpointing cho khả năng chịu lỗi

## Phase 6: Tầng Tìm Kiếm & Phục Vụ ✅
- [x] Thiết lập Elasticsearch cluster (v8.13.2)
- [x] Tạo các indices:
  - [x] `crypto_latest` - 98 coins mới nhất
  - [x] `crypto_history` - 769 records lịch sử giá
  - [x] `alerts` - 336,144 cảnh báo pump/dump
- [x] Triển khai pipeline đánh index từ Spark (`spark_to_elasticsearch.py`)
- [x] Cấu hình mappings cho tìm kiếm và aggregations

## Phase 7: Tầng Truy Vấn ✅
- [x] Triển khai các truy vấn thống kê (`elasticsearch_queries.py`):
  - [x] Giá BTC/ETH/SOL theo thời gian (`get_price_history`)
  - [x] Xu hướng/trung bình/% thay đổi (`get_price_trend`)
  - [x] Top gainers/losers 24h (`get_top_gainers`, `get_top_losers`)
  - [x] Market cap ranking (`get_market_cap_ranking`)
  - [x] Volume spike detection (`get_volume_spikes`)
- [x] Triển khai các truy vấn tìm kiếm:
  - [x] Tìm kiếm coin theo tên/symbol (`search_coin`)
  - [x] Lọc theo market cap, khoảng giá, % thay đổi (`filter_coins`)
  - [x] Aggregations cho dashboard (`get_market_summary`, `get_market_cap_distribution`)
- [x] REST API Server (`query_api.py` - FastAPI)

## Phase 8: Tầng Trực Quan Hóa (Dashboard Kibana)
- [ ] Thiết lập Kibana
- [ ] Tạo 6+ biểu đồ bắt buộc:
  - [ ] Market cap distribution theo coin (treemap/pie)
  - [ ] Xu hướng giá BTC/ETH/SOL theo ngày (line chart)
  - [ ] Top 10 coins pump/dump 24h (horizontal bar)
  - [ ] Phân bố volume theo coin (bar chart)
  - [ ] Số lượng pump/dump alerts theo giờ (time series)
  - [ ] Bảng coins real-time với giá, % change, volume
- [ ] Biểu đồ bổ sung:
  - [ ] BTC Dominance gauge
  - [ ] Heatmap giá theo giờ trong ngày
- [ ] Tạo dashboard tương tác với bộ lọc

## Phase 9: Triển Khai Kubernetes/Cloud ✅
- [x] Thiết lập Kubernetes manifests:
  - [x] Namespace: `crypto-pipeline`
  - [x] Kafka cluster (3 brokers StatefulSet)
  - [x] Zookeeper deployment
  - [x] Spark Master + 2 Workers
  - [x] Elasticsearch cluster (2 nodes StatefulSet)
  - [x] Kibana deployment
  - [x] Crypto Crawler CronJob
  - [x] Query API deployment
- [x] Cấu hình persistent volumes (PVCs)
- [x] Thiết lập resource quotas và limits
- [x] Scripts deploy: `deploy.bat` (Windows), `deploy.sh` (Linux)
- [x] Documentation: `k8s/README.md`

---

## 📁 Cấu Trúc Thư Mục Project

```
bigdata/
├── crawl/
│   ├── crypto_crawler.py          # Batch crawler (1 lần)
│   ├── crypto_crawler_streaming.py # Streaming → Kafka (24/7)
│   ├── send_fake_crypto_kafka.py  # Fake 2 tháng data → Kafka
│   ├── kafka_producer.py          # Kafka helper
│   └── output/
│       └── crypto_raw.json        # 100 coins mẫu từ CoinGecko
├── hdfs/
│   ├── kafka_to_hdfs_raw.py       # Consumer: Kafka → HDFS
│   └── data/
│       ├── raw/                   # Dữ liệu thô từ Kafka
│       ├── clean/                 # Dữ liệu đã làm sạch
│       └── aggregated/            # Dữ liệu tổng hợp batch
├── spark/
│   ├── batch_processing.py        # Spark batch jobs
│   └── streaming_processing.py    # Spark streaming
├── elasticsearch/
│   └── mappings/                  # Index mappings
├── kibana/
│   └── dashboards/                # Dashboard exports
├── k8s/
│   └── manifests/                 # Kubernetes YAML files
├── docker-compose.yml             # Kafka cluster
└── PLAN.md                        # File này
```

---

## 📊 Schema Dữ Liệu Crypto

| Trường | Kiểu | Mô tả | Thay đổi? |
|--------|------|-------|-----------|
| crawl_time | timestamp | Thời gian crawl | Mỗi lần |
| source | string | "coingecko" | Cố định |
| coin_id | string | "bitcoin", "ethereum"... | Cố định |
| symbol | string | "BTC", "ETH"... | Cố định |
| name | string | "Bitcoin", "Ethereum"... | Cố định |
| current_price | float | Giá hiện tại (USD) | ✅ Real-time |
| price_change_24h | float | Thay đổi giá 24h | ✅ Real-time |
| price_change_percentage_24h | float | % thay đổi 24h | ✅ Real-time |
| market_cap | long | Vốn hóa thị trường | ✅ Real-time |
| market_cap_rank | int | Xếp hạng market cap | ✅ Thay đổi |
| total_volume | long | Volume giao dịch 24h | ✅ Real-time |
| circulating_supply | float | Số coin lưu hành | ✅ Tăng dần |
| total_supply | float | Tổng supply | Cố định |
| max_supply | float | Supply tối đa | Cố định |
| high_24h | float | Giá cao nhất 24h | ✅ Real-time |
| low_24h | float | Giá thấp nhất 24h | ✅ Real-time |
| ath | float | All-time high | Hiếm thay đổi |
| atl | float | All-time low | Hiếm thay đổi |

---

## 🚀 Các Lệnh Chạy

```bash
# 1. Crawl 1 lần (test)
cd crawl
py crypto_crawler.py

# 2. Fake 2 tháng data → Kafka
py send_fake_crypto_kafka.py

# 3. Streaming real-time → Kafka
py crypto_crawler_streaming.py

# 4. Consumer Kafka → HDFS
cd ../hdfs
py kafka_to_hdfs_raw.py
```

---

## 👥 Phân Công (Gợi ý)

| Phase | Người | Deadline |
|-------|-------|----------|
| Phase 1-2 | ✅ Done | - |
| Phase 3 (HDFS) | ? | ? |
| Phase 4 (Spark Batch) | ? | ? |
| Phase 5 (Spark Streaming) | ? | ? |
| Phase 6-7 (Elasticsearch) | ? | ? |
| Phase 8 (Kibana) | ? | ? |
| Phase 9 (K8s) | ? | ? |
