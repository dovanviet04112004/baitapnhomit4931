# Dự Án Big Data: Pipeline Phân Tích Giá E-commerce Theo Thời Gian Thực

Hệ thống thu thập và phân tích biến động giá, xu hướng sản phẩm từ dữ liệu web theo thời gian thực + batch, phục vụ dashboard và truy vấn nhanh.

## 📋 Tổng Quan Dự Án

Dự án này xây dựng một **pipeline Big Data hoàn chỉnh** để:
- 📊 Thu thập dữ liệu sản phẩm liên tục theo thời gian
- 💾 Lưu trữ dữ liệu lịch sử lớn (batch) và dữ liệu mới đến (stream)
- 🔍 Phân tích xu hướng, phát hiện biến động bất thường
- 📈 Cung cấp dashboard tổng quan + truy vấn nhanh

## 🏗️ Kiến Trúc Hệ Thống

```
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│   Crawler    │─────▶│    Kafka     │─────▶│    Spark     │
│  (Producer)  │      │   Cluster    │      │  Streaming   │
└──────────────┘      └──────────────┘      └──────────────┘
                             │                      │
                             │                      ▼
                             │               ┌──────────────┐
                             │               │     HDFS     │
                             │               │   Storage    │
                             │               └──────────────┘
                             ▼                      │
                      ┌──────────────┐             │
                      │ Elasticsearch│◀────────────┘
                      └──────────────┘
                             │
                             ▼
                      ┌──────────────┐
                      │    Kibana    │
                      │  Dashboard   │
                      └──────────────┘
```

## ✅ Tiến Độ Hiện Tại

### Phase 1: Nguồn Dữ Liệu & Schema ✅ HOÀN THÀNH
- ✅ Thiết kế schema toàn diện (raw_products, clean_products, alerts)
- ✅ Crawler nâng cao với time-series, quality scoring
- ✅ Tài liệu 5 data challenges + giải pháp
- ✅ HTML snapshots cho audit

### Phase 2: Hạ Tầng Streaming ✅ HOÀN THÀNH
- ✅ Kafka cluster 3 brokers (distributed)
- ✅ 3 topics với retention policies
- ✅ Kafka producer với error handling
- ✅ Streaming crawler tích hợp Kafka
- ✅ Continuous crawl mode (5 phút/lần)

### Phase 3-12: 🚧 ĐANG TRIỂN KHAI
- ⏳ Spark Structured Streaming
- ⏳ Batch processing
- ⏳ Elasticsearch indexing
- ⏳ Kibana dashboard
- ⏳ Kubernetes deployment
- ⏳ Scalability & fault tolerance testing

## 📁 Cấu Trúc Dự Án

```
baitapnhomit4931/
├── crawl/                              # Crawler & Producer
│   ├── ecommerce_crawler.py           # Crawler nâng cao
│   ├── ecommerce_crawler_streaming.py # Streaming crawler
│   ├── kafka_producer.py              # Kafka producer
│   ├── requirements.txt               # Python dependencies
│   └── output/                        # Dữ liệu đã crawl
│
├── kafka/                              # Kafka Streaming
│   ├── docker-compose.yml             # Kafka cluster (3 brokers)
│   ├── create_topics.bat              # Script tạo topics (Windows)
│   ├── create_topics.sh               # Script tạo topics (Linux)
│   └── README.md                      # Hướng dẫn Kafka
│
├── spark/                              # Spark Processing
│   ├── clean_books.py                 # Batch cleaning (cũ)
│   ├── spark_to_es.py                 # Elasticsearch indexing (cũ)
│   └── (sẽ thêm streaming jobs)
│
├── docs/                               # Tài Liệu
│   ├── task.md                        # Danh sách công việc
│   ├── implementation_plan.md         # Kế hoạch triển khai
│   ├── walkthrough_phase1_2.md        # Hướng dẫn Phase 1 & 2
│   └── data_sources.md                # Tài liệu nguồn dữ liệu
│
└── README.md                           # File này
```

## 🚀 Hướng Dẫn Nhanh

### 1. Khởi Động Kafka Cluster

```bash
cd kafka
docker-compose up -d
```

### 2. Tạo Kafka Topics

**Windows:**
```bash
cd kafka
create_topics.bat
```

**Linux/Mac:**
```bash
cd kafka
chmod +x create_topics.sh
./create_topics.sh
```

### 3. Cài Đặt Dependencies

```bash
cd crawl
pip install -r requirements.txt
```

### 4. Chạy Streaming Crawler

```bash
cd crawl
python ecommerce_crawler_streaming.py
```

### 5. Giám Sát Kafka

Mở trình duyệt: http://localhost:8080

## 📊 Dữ Liệu & Schema

### Nguồn Dữ Liệu
- **Hiện tại**: webscraper.io/test-sites/e-commerce (demo site)
- **Tương lai**: Tiki.vn, Shopee.vn, Sendo.vn

### Schema Chính

**Raw Products** (Kafka topic: `raw_products`)
```json
{
  "crawl_time": "2025-12-30T17:10:00+07:00",
  "product_id": "abc123",
  "product_name": "Laptop Dell Inspiron 15",
  "category": "Computers",
  "price": 799.99,
  "discount_price": 699.99,
  "rating": 4.5,
  "data_quality_score": 0.95
}
```

## 🛠️ Công Nghệ Sử Dụng

| Thành Phần | Công Nghệ | Version |
|------------|-----------|---------|
| **Crawler** | Python, BeautifulSoup, Requests | 3.11 |
| **Streaming** | Apache Kafka | 7.5.0 |
| **Processing** | Apache Spark (PySpark) | 3.3.3 |
| **Storage** | HDFS | 3.3.6 |
| **Search** | Elasticsearch | 8.13.2 |
| **Visualization** | Kibana | 8.13.2 |
| **Orchestration** | Docker, Kubernetes | - |

## 📚 Tài Liệu Chi Tiết

- **[task.md](docs/task.md)** - Danh sách công việc chi tiết (12 phases)
- **[implementation_plan.md](docs/implementation_plan.md)** - Kế hoạch triển khai toàn diện
- **[walkthrough_phase1_2.md](docs/walkthrough_phase1_2.md)** - Hướng dẫn Phase 1 & 2
- **[data_sources.md](docs/data_sources.md)** - Nguồn dữ liệu và challenges
- **[kafka/README.md](kafka/README.md)** - Hướng dẫn Kafka Streaming

## 🎯 Mục Tiêu Dự Án

### Yêu Cầu Bắt Buộc
1. ✅ **Data Collection Layer**: Kafka streaming (1-5 phút/lần)
2. ✅ **Storage Layer**: HDFS với phân vùng theo thời gian
3. ⏳ **Processing Layer**: Batch + Real-time (Lambda architecture)
4. ⏳ **Serving Layer**: Elasticsearch với 3 indices
5. ⏳ **Query Layer**: Statistical + Search queries
6. ⏳ **Visualization**: Kibana dashboard (6+ charts)
7. ⏳ **Deployment**: Kubernetes multi-node
8. ⏳ **Testing**: Scalability + Fault tolerance

### Deliverables
- [ ] Sơ đồ kiến trúc end-to-end
- [ ] Source code đầy đủ
- [ ] Deployment manifests (K8s)
- [ ] Dashboard Kibana
- [ ] Báo cáo toàn diện với test results

## 🧪 Testing

### Scalability Tests
- **Test A**: Scale data 10k → 100k records
- **Test B**: Scale workers 1 → 2 → 3

### Fault Tolerance Tests
- **FT1**: Kill Spark worker → verify recovery
- **FT2**: Restart Kafka/ES → verify pipeline recovery

## 👥 Đóng Góp

Dự án này được phát triển cho môn Big Data, tập trung vào:
- Kiến trúc phân tán
- Pipeline xử lý dữ liệu
- Khả năng mở rộng & chịu lỗi

## 📝 Ghi Chú

- **Môi trường**: Windows với Git Bash
- **Python**: 3.11
- **JDK**: OpenJDK 11 (cho Spark)
- **Docker**: Bắt buộc cho Kafka, Elasticsearch

## 🔗 Liên Kết Hữu Ích

- Kafka UI: http://localhost:8080
- Elasticsearch: http://localhost:9200
- Kibana: http://localhost:5601
- HDFS NameNode: http://localhost:9870

---

**Trạng thái**: 🚧 Đang phát triển - Phase 2/12 hoàn thành  
**Cập nhật**: 2025-12-30