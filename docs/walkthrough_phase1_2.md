# Hướng Dẫn: Phase 1 & 2 - Nguồn Dữ Liệu và Hạ Tầng Streaming

## Tổng Quan

Tài liệu này ghi chép việc triển khai **Phase 1** (Nguồn Dữ Liệu & Thiết Kế Schema) và **Phase 2** (Thiết Lập Hạ Tầng Streaming) cho Pipeline Phân Tích Giá E-commerce Theo Thời Gian Thực.

---

## Phase 1: Nguồn Dữ Liệu & Thiết Kế Schema ✅

### Những Gì Đã Xây Dựng

#### 1. Tài Liệu Nguồn Dữ Liệu

Tạo tài liệu toàn diện trong [data_sources.md](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/docs/data_sources.md) bao gồm:

**Websites Đã Chọn:**
- **Chính**: webscraper.io/test-sites/e-commerce (trang demo, thân thiện với scraping)
- **Tương lai**: Các nền tảng thương mại điện tử Việt Nam (Tiki.vn, Shopee.vn, Sendo.vn)

**Tại sao chọn webscraper.io?**
- ✅ Được thiết kế cho thực hành web scraping
- ✅ Không có giới hạn tốc độ hoặc phát hiện bot
- ✅ Cấu trúc HTML nhất quán
- ✅ Nhiều danh mục (Computers, Phones)
- ✅ Có dữ liệu giá và rating

#### 2. Thiết Kế Schema Toàn Diện

Ba schemas được thiết kế cho các giai đoạn khác nhau của pipeline:

**A. Schema Raw Products** (Kafka topic: `raw_products`)
```json
{
  "crawl_time": "2025-12-30T17:10:00+07:00",
  "source": "webscraper.io",
  "product_id": "abc123def456",
  "product_url": "https://...",
  "product_name": "Laptop Dell Inspiron 15",
  "category": "Computers",
  "price": 799.99,
  "currency": "USD",
  "discount_price": 699.99,
  "availability": "In Stock",
  "in_stock": true,
  "rating": 4.5,
  "num_reviews": 128,
  "raw_html_snapshot_path": "hdfs://...",
  "data_quality_score": 0.95
}
```

**B. Schema Clean Products** (Kafka topic: `clean_products`)
- Dữ liệu đã validate và chuẩn hóa
- Định dạng chuẩn
- Điểm chất lượng > 0.8

**C. Schema Alert** (Kafka topic: `alerts`)
```json
{
  "alert_time": "2025-12-30T17:10:00+07:00",
  "alert_type": "price_spike",
  "severity": "high",
  "product_id": "abc123",
  "price_change_percent": 28.57,
  "z_score": 3.5,
  "message": "Giá tăng 28.57%"
}
```

#### 3. Các Thách Thức Dữ Liệu Đã Ghi Chép

Năm thách thức chính với giải pháp:

| Thách Thức | Giải Pháp |
|------------|-----------|
| **Thiếu Trường** | Trường nullable, tính điểm chất lượng |
| **Dữ Liệu Không Nhất Quán** | Hàm chuẩn hóa, regex parsing |
| **Thay Đổi Schema** | Nhiều selector dự phòng, versioning |
| **Sản Phẩm Trùng Lặp** | MD5 fingerprinting, deduplication |
| **Giới Hạn Tốc Độ** | Exponential backoff, xoay User-Agent |

#### 4. Crawler E-commerce Nâng Cao

Tạo [ecommerce_crawler.py](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/crawl/ecommerce_crawler.py) với các tính năng tiên tiến:

**Tính Năng Chính:**
- ✅ **Hỗ trợ time-series**: Theo dõi `crawl_time` cho phân tích lịch sử
- ✅ **Tạo Product ID**: ID ổn định dựa trên MD5 để deduplication
- ✅ **Tính điểm chất lượng**: Điểm 0.0-1.0 dựa trên độ đầy đủ của trường
- ✅ **HTML snapshots**: Lưu HTML thô để audit
- ✅ **Xử lý lỗi**: Logic retry với exponential backoff
- ✅ **Giới hạn tốc độ**: Delay ngẫu nhiên (0.5-2s) để tránh phát hiện
- ✅ **Xoay User-Agent**: Ngăn phát hiện bot
- ✅ **Mô phỏng giá**: Thêm biến động ±5% để mô phỏng thay đổi thời gian thực

**Đoạn Code Nổi Bật:**

```python
def _generate_product_id(self, product_name: str, category: str) -> str:
    """Tạo product ID ổn định để deduplication"""
    key = f"{product_name}|{category}"
    return hashlib.md5(key.encode()).hexdigest()[:16]

def _calculate_quality_score(self, product: Dict) -> float:
    """Tính điểm chất lượng dữ liệu"""
    required_fields = ['product_id', 'product_name', 'price', 'category']
    optional_fields = ['rating', 'num_reviews', 'discount_price']
    
    required_score = sum(1 for f in required_fields if product.get(f)) / len(required_fields)
    optional_score = sum(1 for f in optional_fields if product.get(f)) / len(optional_fields)
    
    return round(0.7 * required_score + 0.3 * optional_score, 2)
```

---

## Phase 2: Thiết Lập Hạ Tầng Streaming ✅

### Kiến Trúc

```
┌─────────────────────┐
│  Crawler (Producer) │
│  - Crawls products  │
│  - Gửi đến Kafka    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────────────────────┐
│         Kafka Cluster               │
│  ┌──────────┐ ┌──────────┐ ┌──────┐│
│  │ Broker 1 │ │ Broker 2 │ │Broker││
│  │ :19092   │ │ :19093   │ │:19094││
│  └──────────┘ └──────────┘ └──────┘│
│                                     │
│  Topics:                            │
│  - raw_products (7 ngày retention)  │
│  - clean_products (30 ngày)         │
│  - alerts (90 ngày)                 │
└─────────────────────────────────────┘
           │
           ▼
    ┌──────────────┐
    │  Kafka UI    │
    │  :8080       │
    └──────────────┘
```

### Những Gì Đã Xây Dựng

#### 1. Kafka Cluster (3 Brokers)

Tạo [docker-compose.yml](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/kafka/docker-compose.yml) với:

**Các Thành Phần:**
- **Zookeeper**: Dịch vụ điều phối (port 2181)
- **Kafka Broker 1**: Port 19092 (localhost)
- **Kafka Broker 2**: Port 19093 (localhost)
- **Kafka Broker 3**: Port 19094 (localhost)
- **Kafka UI**: Dashboard giám sát (port 8080)

**Cấu Hình:**
- **Replication Factor**: 2 (dữ liệu được nhân bản trên 2 brokers)
- **Partitions**: 3 mỗi topic (xử lý song song)
- **Compression**: Snappy (nén hiệu quả)
- **Retention**: Theo topic (7-90 ngày)

**Tại sao 3 Brokers?**
- ✅ Thiết lập phân tán (đáp ứng yêu cầu dự án)
- ✅ Khả năng chịu lỗi (có thể chịu được 1 broker lỗi)
- ✅ Cân bằng tải trên các partitions
- ✅ Môi trường giống production thực tế

#### 2. Scripts Tạo Topic

Tạo cả scripts Windows và Linux:

**Windows**: [create_topics.bat](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/kafka/create_topics.bat)  
**Linux**: [create_topics.sh](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/kafka/create_topics.sh)

**Topics Đã Tạo:**

| Topic | Partitions | Replication | Retention | Mục Đích |
|-------|-----------|-------------|-----------|----------|
| `raw_products` | 3 | 2 | 7 ngày | Dữ liệu thô đã crawl |
| `clean_products` | 3 | 2 | 30 ngày | Dữ liệu đã validate |
| `alerts` | 3 | 2 | 90 ngày | Bất thường về giá |

#### 3. Kafka Producer

Tạo [kafka_producer.py](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/crawl/kafka_producer.py) với:

**Tính Năng:**
- ✅ **Gửi tin cậy**: `acks='all'` (đợi tất cả replicas)
- ✅ **Logic retry**: 3 lần retry với exponential backoff
- ✅ **Đảm bảo thứ tự**: `max_in_flight_requests=1`
- ✅ **Nén**: Nén Snappy
- ✅ **Batching**: 10ms linger time để hiệu quả
- ✅ **Xử lý lỗi**: Bắt exceptions KafkaError
- ✅ **Theo dõi thống kê**: Đếm messages đã gửi/thất bại

#### 4. Tích Hợp Streaming Crawler

Tạo [ecommerce_crawler_streaming.py](file:///d:/2025.1/BigData/BTL/baitapnhomit4931/crawl/ecommerce_crawler_streaming.py):

**Khả Năng Mới:**
- ✅ **Streaming thời gian thực**: Gửi mỗi sản phẩm đến Kafka ngay lập tức
- ✅ **Chế độ liên tục**: Có thể chạy vô thời hạn với khoảng thời gian cấu hình được
- ✅ **Đầu ra kép**: Lưu vào cả file (backup) và Kafka (streaming)
- ✅ **Graceful degradation**: Chuyển về chỉ file nếu Kafka không khả dụng
- ✅ **Biến động giá**: Mô phỏng thay đổi giá ±5% cho demo time-series

**Cấu Hình:**

```python
KAFKA_BOOTSTRAP_SERVERS = 'localhost:19092'
KAFKA_TOPIC_RAW = 'raw_products'
CRAWL_INTERVAL_SECONDS = 300  # 5 phút
CONTINUOUS_MODE = False  # Đặt True để crawl liên tục
```

---

## Kiểm Tra & Xác Minh

### 1. Thiết Lập Kafka Cluster

**Khởi động Kafka:**
```bash
cd kafka
docker-compose up -d
```

**Xác minh containers:**
```bash
docker ps
```

### 2. Tạo Topics

**Chạy script:**
```bash
cd kafka
create_topics.bat  # Windows
# hoặc
./create_topics.sh  # Linux
```

### 3. Test Producer

**Chạy test:**
```bash
cd crawl
pip install -r requirements.txt
python kafka_producer.py
```

Kết quả mong đợi:
```
Testing Kafka Producer
✅ Successfully connected to Kafka
✅ Test message sent successfully!
✅ Test alert sent successfully!
```

### 4. Test Streaming Crawler

**Chạy crawler:**
```bash
cd crawl
python ecommerce_crawler_streaming.py
```

---

## Cấu Trúc File

```
baitapnhomit4931/
├── crawl/
│   ├── book.py                          # Crawler gốc
│   ├── ecommerce_crawler.py             # Crawler nâng cao (Phase 1)
│   ├── ecommerce_crawler_streaming.py   # Streaming crawler (Phase 2)
│   ├── kafka_producer.py                # Kafka producer
│   ├── requirements.txt                 # Python dependencies
│   └── output/
│       ├── ecommerce_raw.json          # Dữ liệu đã crawl
│       └── snapshots/                  # HTML snapshots
├── kafka/
│   ├── docker-compose.yml              # Cấu hình Kafka cluster
│   ├── create_topics.sh                # Tạo topic (Linux)
│   ├── create_topics.bat               # Tạo topic (Windows)
│   └── README.md                       # Hướng dẫn thiết lập
├── docs/
│   ├── data_sources.md                 # Tài liệu nguồn dữ liệu
│   ├── task.md                         # Danh sách công việc
│   ├── implementation_plan.md          # Kế hoạch triển khai
│   └── walkthrough_phase1_2.md         # Hướng dẫn này
└── README.md                           # Tổng quan dự án
```

---

## Thành Tựu Chính

### Phase 1 ✅
- [x] Xác định nguồn dữ liệu (webscraper.io + các trang VN tương lai)
- [x] Thiết kế schemas toàn diện (raw, clean, alerts)
- [x] Ghi chép 5 thách thức dữ liệu với giải pháp
- [x] Xây dựng crawler nâng cao với hỗ trợ time-series
- [x] Triển khai tính điểm chất lượng dữ liệu
- [x] Thêm lưu trữ HTML snapshot

### Phase 2 ✅
- [x] Thiết lập Kafka cluster với 3 brokers
- [x] Tạo 3 topics với retention phù hợp
- [x] Triển khai Kafka producer tin cậy
- [x] Tích hợp streaming vào crawler
- [x] Thêm chế độ crawl liên tục
- [x] Tạo hướng dẫn thiết lập toàn diện
- [x] Test pipeline end-to-end

---

## Điểm Nổi Bật Kỹ Thuật

### 1. Tính Điểm Chất Lượng Dữ Liệu

Mỗi sản phẩm nhận điểm chất lượng (0.0-1.0):
- **Trường bắt buộc** (70% trọng số): product_id, name, price, category
- **Trường tùy chọn** (30% trọng số): rating, reviews, discount, description

Sản phẩm có điểm < 0.8 được đánh dấu để review.

### 2. Product IDs Ổn Định

MD5 fingerprinting đảm bảo cùng sản phẩm nhận cùng ID qua các lần crawl:
```python
key = f"{product_name}|{category}"
product_id = hashlib.md5(key.encode()).hexdigest()[:16]
```

### 3. Độ Tin Cậy Kafka

Cấu hình producer đảm bảo không mất dữ liệu:
- `acks='all'`: Đợi tất cả replicas xác nhận
- `retries=3`: Retry các lần gửi thất bại
- `max_in_flight_requests=1`: Duy trì thứ tự message

### 4. Mô Phỏng Biến Động Giá

Vì trang demo có giá tĩnh, chúng ta thêm biến động ngẫu nhiên:
```python
price_variation = random.uniform(-0.05, 0.05)  # ±5%
price = round(price * (1 + price_variation), 2)
```

Điều này tạo dữ liệu time-series thực tế cho phân tích xu hướng.

---

## Bước Tiếp Theo

### Phase 3: Xử Lý Thời Gian Thực (Tiếp theo)
- [ ] Triển khai Spark Structured Streaming consumer
- [ ] Đọc từ topic `raw_products`
- [ ] Làm sạch và validate dữ liệu
- [ ] Phát hiện bất thường về giá (thuật toán z-score)
- [ ] Ghi vào topics `clean_products` và `alerts`
- [ ] Ghi vào HDFS với phân vùng thời gian

---

## Tóm Tắt

**Phase 1 & 2 Hoàn Thành! 🎉**

Chúng ta đã thành công:
1. ✅ Thiết kế schemas dữ liệu toàn diện
2. ✅ Xây dựng crawler nâng cao với hỗ trợ time-series
3. ✅ Thiết lập Kafka cluster phân tán (3 brokers)
4. ✅ Tạo pipeline streaming (crawler → Kafka)
5. ✅ Triển khai producer tin cậy với xử lý lỗi
6. ✅ Test luồng dữ liệu end-to-end

**Sẵn sàng cho Phase 3**: Spark Structured Streaming để xử lý thời gian thực!
