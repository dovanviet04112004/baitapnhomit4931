# Kịch Bản Test Phase 1 & 2

## 📋 Mục Đích
Tài liệu này cung cấp kịch bản test chi tiết từng bước để kiểm tra tất cả chức năng của Phase 1 (Nguồn Dữ Liệu & Schema) và Phase 2 (Hạ Tầng Streaming).

---

## ✅ Phase 1: Nguồn Dữ Liệu & Schema Design

### Test 1.1: Crawler Cơ Bản - Thu Thập Dữ Liệu

**Mục tiêu**: Kiểm tra crawler có thể thu thập dữ liệu từ website

**Các bước thực hiện**:

```bash
# Bước 1: Di chuyển vào thư mục crawler
cd d:\2025.1\BigData\BTL\baitapnhomit4931\crawl

# Bước 2: Cài đặt dependencies (nếu chưa)
pip install -r requirements.txt

# Bước 3: Chạy crawler
python ecommerce_crawler.py
```

**Kết quả mong đợi**:
```
============================================================
🚀 Starting E-commerce Crawler v2.0
============================================================
⏰ Crawl Time: 2025-12-30T...
🌐 Source: webscraper.io
📁 Output: ...\output\ecommerce_raw.json

📂 Crawling category: Computers
   URL: https://webscraper.io/test-sites/e-commerce/allinone/computers
   ✅ Loaded in XXXms
   📦 Found XX products
   ✅ Completed Computers: XX products

📂 Crawling category: Phones
   URL: https://webscraper.io/test-sites/e-commerce/allinone/phones
   ✅ Loaded in XXXms
   📦 Found XX products
   ✅ Completed Phones: XX products

💾 Saved XX products to ...\ecommerce_raw.json

============================================================
📊 Crawl Summary
============================================================
Total Attempts:    XX
✅ Successful:     XX
❌ Failed:         0
⚠️  Missing Fields: X
📈 Success Rate:   100.0%
⭐ Avg Quality:    0.XX
============================================================
```

**Kiểm tra**:
- [ ] Crawler chạy không lỗi
- [ ] Có thông báo crawl từng category
- [ ] Success rate = 100%
- [ ] File `output/ecommerce_raw.json` được tạo

---

### Test 1.2: Kiểm Tra Schema Dữ Liệu

**Mục tiêu**: Xác minh dữ liệu có đúng schema đã thiết kế

**Các bước thực hiện**:

```bash
# Bước 1: Mở file JSON đã crawl
cd output
notepad ecommerce_raw.json
# Hoặc
code ecommerce_raw.json
```

**Kiểm tra từng trường**:

Chọn 1 sản phẩm bất kỳ và kiểm tra:

```json
{
  "crawl_time": "...",           // ✓ Có timestamp ISO 8601
  "source": "webscraper.io",     // ✓ Có tên nguồn
  "product_id": "...",           // ✓ Có ID (16 ký tự hex)
  "product_url": "https://...",  // ✓ Có URL đầy đủ
  "product_name": "...",         // ✓ Có tên sản phẩm
  "category": "Computers",       // ✓ Có category
  "price": 799.99,               // ✓ Có giá (số)
  "currency": "USD",             // ✓ Có đơn vị tiền
  "discount_price": 699.99,      // ✓ Có hoặc null
  "discount_percentage": 12.5,   // ✓ Có hoặc null
  "availability": "In Stock",    // ✓ Có trạng thái
  "in_stock": true,              // ✓ Có boolean
  "stock_quantity": 25,          // ✓ Có số lượng
  "rating": 4.5,                 // ✓ Có hoặc null
  "num_reviews": 128,            // ✓ Có số reviews
  "image_url": "https://...",    // ✓ Có URL ảnh
  "description": "...",          // ✓ Có mô tả
  "raw_html_snapshot_path": "...", // ✓ Có đường dẫn
  "data_quality_score": 0.95,    // ✓ Có điểm (0-1)
  "metadata": {                  // ✓ Có metadata
    "crawler_version": "2.0",
    "crawl_duration_ms": 1250,
    "http_status": 200
  }
}
```

**Checklist**:
- [ ] Tất cả trường bắt buộc đều có giá trị
- [ ] `crawl_time` đúng định dạng ISO 8601
- [ ] `product_id` là chuỗi 16 ký tự
- [ ] `price` là số, không phải string
- [ ] `data_quality_score` từ 0.0 đến 1.0
- [ ] `metadata` có đầy đủ 3 trường

---

### Test 1.3: Kiểm Tra Data Quality Score

**Mục tiêu**: Xác minh thuật toán tính điểm chất lượng hoạt động đúng

**Các bước thực hiện**:

```python
# Tạo file test_quality_score.py
import json

# Đọc dữ liệu
with open('output/ecommerce_raw.json', 'r', encoding='utf-8') as f:
    products = json.load(f)

# Phân tích điểm chất lượng
scores = [p.get('data_quality_score', 0) for p in products]
avg_score = sum(scores) / len(scores)

print(f"📊 Phân Tích Data Quality Score")
print(f"=" * 50)
print(f"Tổng số sản phẩm: {len(products)}")
print(f"Điểm trung bình: {avg_score:.2f}")
print(f"Điểm cao nhất: {max(scores):.2f}")
print(f"Điểm thấp nhất: {min(scores):.2f}")

# Đếm theo mức điểm
high_quality = sum(1 for s in scores if s >= 0.9)
medium_quality = sum(1 for s in scores if 0.7 <= s < 0.9)
low_quality = sum(1 for s in scores if s < 0.7)

print(f"\nPhân bố chất lượng:")
print(f"  Cao (≥0.9):     {high_quality} ({high_quality/len(products)*100:.1f}%)")
print(f"  Trung bình:     {medium_quality} ({medium_quality/len(products)*100:.1f}%)")
print(f"  Thấp (<0.7):    {low_quality} ({low_quality/len(products)*100:.1f}%)")

# Hiển thị sản phẩm chất lượng thấp
if low_quality > 0:
    print(f"\n⚠️  Sản phẩm chất lượng thấp:")
    for p in products:
        if p.get('data_quality_score', 0) < 0.7:
            print(f"  - {p['product_name']}: {p['data_quality_score']:.2f}")
            missing = []
            if not p.get('rating'): missing.append('rating')
            if not p.get('num_reviews'): missing.append('num_reviews')
            if not p.get('discount_price'): missing.append('discount_price')
            if not p.get('description'): missing.append('description')
            print(f"    Thiếu: {', '.join(missing)}")
```

**Chạy test**:
```bash
python test_quality_score.py
```

**Kết quả mong đợi**:
```
📊 Phân Tích Data Quality Score
==================================================
Tổng số sản phẩm: 45
Điểm trung bình: 0.85
Điểm cao nhất: 1.00
Điểm thấp nhất: 0.70

Phân bố chất lượng:
  Cao (≥0.9):     30 (66.7%)
  Trung bình:     12 (26.7%)
  Thấp (<0.7):    3 (6.7%)
```

**Checklist**:
- [ ] Điểm trung bình > 0.8
- [ ] Có ít nhất 60% sản phẩm điểm cao (≥0.9)
- [ ] Sản phẩm chất lượng thấp có lý do rõ ràng (thiếu trường)

---

### Test 1.4: Kiểm Tra HTML Snapshots

**Mục tiêu**: Xác minh HTML snapshots được lưu đúng

**Các bước thực hiện**:

```bash
# Bước 1: Kiểm tra thư mục snapshots
cd output/snapshots
dir  # Windows
# hoặc
ls -la  # Linux/Mac

# Bước 2: Kiểm tra có thư mục theo ngày
# Ví dụ: 20251230/
cd 20251230

# Bước 3: Đếm số file HTML
dir *.html | find /c ".html"  # Windows
# hoặc
ls -1 *.html | wc -l  # Linux/Mac
```

**Kết quả mong đợi**:
```
output/snapshots/
└── 20251230/
    ├── abc123def456.html
    ├── def456ghi789.html
    ├── ...
    └── xyz789abc123.html

Tổng: 45 files
```

**Kiểm tra**:
- [ ] Có thư mục theo ngày (YYYYMMDD)
- [ ] Số file HTML = số sản phẩm đã crawl
- [ ] Tên file là product_id (16 ký tự hex)
- [ ] Mở 1 file HTML, có nội dung trang sản phẩm

---

### Test 1.5: Kiểm Tra Deduplication (Product ID)

**Mục tiêu**: Xác minh cùng sản phẩm luôn có cùng ID

**Các bước thực hiện**:

```python
# Tạo file test_deduplication.py
import json
import hashlib

# Đọc dữ liệu
with open('output/ecommerce_raw.json', 'r', encoding='utf-8') as f:
    products = json.load(f)

print(f"📊 Kiểm Tra Deduplication")
print(f"=" * 50)

# Kiểm tra ID trùng lặp
ids = [p['product_id'] for p in products]
unique_ids = set(ids)

print(f"Tổng sản phẩm: {len(products)}")
print(f"Unique IDs: {len(unique_ids)}")

if len(ids) != len(unique_ids):
    print(f"⚠️  Có {len(ids) - len(unique_ids)} IDs trùng lặp!")
    duplicates = [id for id in ids if ids.count(id) > 1]
    print(f"IDs trùng: {set(duplicates)}")
else:
    print(f"✅ Không có ID trùng lặp")

# Kiểm tra thuật toán tạo ID
print(f"\n🔍 Kiểm Tra Thuật Toán:")
sample = products[0]
name = sample['product_name']
category = sample['category']
expected_id = hashlib.md5(f"{name}|{category}".encode()).hexdigest()[:16]
actual_id = sample['product_id']

print(f"Product: {name}")
print(f"Category: {category}")
print(f"Expected ID: {expected_id}")
print(f"Actual ID:   {actual_id}")
print(f"Match: {'✅' if expected_id == actual_id else '❌'}")
```

**Chạy test**:
```bash
python test_deduplication.py
```

**Kết quả mong đợi**:
```
📊 Kiểm Tra Deduplication
==================================================
Tổng sản phẩm: 45
Unique IDs: 45
✅ Không có ID trùng lặp

🔍 Kiểm Tra Thuật Toán:
Product: Laptop Dell Inspiron 15
Category: Computers
Expected ID: abc123def456789a
Actual ID:   abc123def456789a
Match: ✅
```

**Checklist**:
- [ ] Không có product_id trùng lặp
- [ ] Thuật toán MD5 hoạt động đúng
- [ ] Cùng tên + category = cùng ID

---

## ✅ Phase 2: Hạ Tầng Streaming

### Test 2.1: Khởi Động Kafka Cluster

**Mục tiêu**: Kiểm tra Kafka cluster khởi động thành công

**Các bước thực hiện**:

```bash
# Bước 1: Di chuyển vào thư mục kafka
cd d:\2025.1\BigData\BTL\baitapnhomit4931\kafka

# Bước 2: Khởi động cluster
docker-compose up -d

# Bước 3: Đợi 30 giây để Kafka khởi động hoàn toàn
timeout /t 30  # Windows
# hoặc
sleep 30  # Linux/Mac

# Bước 4: Kiểm tra containers
docker ps
```

**Kết quả mong đợi**:
```
CONTAINER ID   IMAGE                              STATUS         PORTS
abc123         confluentinc/cp-kafka:7.5.0       Up 30 seconds  0.0.0.0:19092->19092/tcp
def456         confluentinc/cp-kafka:7.5.0       Up 30 seconds  0.0.0.0:19093->19093/tcp
ghi789         confluentinc/cp-kafka:7.5.0       Up 30 seconds  0.0.0.0:19094->19094/tcp
jkl012         confluentinc/cp-zookeeper:7.5.0   Up 30 seconds  0.0.0.0:2181->2181/tcp
mno345         provectuslabs/kafka-ui:latest     Up 30 seconds  0.0.0.0:8080->8080/tcp
```

**Checklist**:
- [ ] 5 containers đang chạy (3 brokers + zookeeper + kafka-ui)
- [ ] Tất cả STATUS = "Up"
- [ ] Ports được map đúng (19092, 19093, 19094, 2181, 8080)

---

### Test 2.2: Tạo Kafka Topics

**Mục tiêu**: Kiểm tra topics được tạo với cấu hình đúng

**Các bước thực hiện**:

```bash
# Bước 1: Chạy script tạo topics
create_topics.bat  # Windows
# hoặc
./create_topics.sh  # Linux/Mac

# Bước 2: Liệt kê topics
docker exec kafka-broker-1 kafka-topics --list --bootstrap-server kafka-broker-1:9092
```

**Kết quả mong đợi**:
```
==================================================
Creating Kafka Topics for Price Analytics Pipeline
==================================================

Creating topic: raw_products
✅ Topic 'raw_products' created successfully

Creating topic: clean_products
✅ Topic 'clean_products' created successfully

Creating topic: alerts
✅ Topic 'alerts' created successfully

==================================================
Current Topics:
==================================================
alerts
clean_products
raw_products

==================================================
Topic Details:
==================================================

--- Topic: raw_products ---
Topic: raw_products     TopicId: ...    PartitionCount: 3       ReplicationFactor: 2
        Topic: raw_products     Partition: 0    Leader: 1       Replicas: 1,2   Isr: 1,2
        Topic: raw_products     Partition: 1    Leader: 2       Replicas: 2,3   Isr: 2,3
        Topic: raw_products     Partition: 2    Leader: 3       Replicas: 3,1   Isr: 3,1
```

**Checklist**:
- [ ] 3 topics được tạo: raw_products, clean_products, alerts
- [ ] Mỗi topic có 3 partitions
- [ ] Replication factor = 2
- [ ] Mỗi partition có 2 replicas
- [ ] ISR (In-Sync Replicas) = 2 cho mỗi partition

---

### Test 2.3: Kiểm Tra Kafka UI

**Mục tiêu**: Xác minh Kafka UI hoạt động và hiển thị đúng

**Các bước thực hiện**:

```bash
# Bước 1: Mở trình duyệt
start http://localhost:8080  # Windows
# hoặc
open http://localhost:8080  # Mac
# hoặc
xdg-open http://localhost:8080  # Linux
```

**Kiểm tra trên UI**:

1. **Trang chủ**:
   - [ ] Thấy cluster "bigdata-cluster"
   - [ ] 3 brokers online
   - [ ] 3 topics

2. **Tab "Topics"**:
   - [ ] Click vào "Topics"
   - [ ] Thấy 3 topics: raw_products, clean_products, alerts
   - [ ] Mỗi topic hiển thị số partitions = 3

3. **Chi tiết topic "raw_products"**:
   - [ ] Click vào "raw_products"
   - [ ] Tab "Overview": Partitions = 3, Replication = 2
   - [ ] Tab "Messages": (chưa có messages)
   - [ ] Tab "Consumers": (chưa có consumers)

---

### Test 2.4: Test Kafka Producer

**Mục tiêu**: Kiểm tra producer gửi messages thành công

**Các bước thực hiện**:

```bash
# Bước 1: Di chuyển vào thư mục crawler
cd d:\2025.1\BigData\BTL\baitapnhomit4931\crawl

# Bước 2: Chạy test producer
python kafka_producer.py
```

**Kết quả mong đợi**:
```
============================================================
Testing Kafka Producer
============================================================
Connecting to Kafka brokers: localhost:19092
✅ Successfully connected to Kafka

Sending test product...
✅ Test message sent successfully!

Sending test alert...
✅ Test alert sent successfully!

Producer Stats: {'sent': 2, 'failed': 0, 'retries': 0}

============================================================
Test Complete
============================================================
```

**Checklist**:
- [ ] Kết nối Kafka thành công
- [ ] 2 messages gửi thành công (product + alert)
- [ ] Không có lỗi
- [ ] Stats: sent=2, failed=0

---

### Test 2.5: Xác Minh Messages trong Kafka

**Mục tiêu**: Kiểm tra messages đã được lưu trong Kafka

**Phương pháp 1: Dùng Kafka UI**

```
1. Mở http://localhost:8080
2. Click "Topics" → "raw_products"
3. Click tab "Messages"
4. Thấy 1 message (test product)
5. Click vào message để xem nội dung JSON
6. Lặp lại cho topic "alerts"
```

**Phương pháp 2: Dùng Command Line**

```bash
# Xem messages trong raw_products
docker exec kafka-broker-1 kafka-console-consumer \
  --bootstrap-server kafka-broker-1:9092 \
  --topic raw_products \
  --from-beginning \
  --max-messages 5

# Xem messages trong alerts
docker exec kafka-broker-1 kafka-console-consumer \
  --bootstrap-server kafka-broker-1:9092 \
  --topic alerts \
  --from-beginning \
  --max-messages 5
```

**Kết quả mong đợi**:
```json
{
  "crawl_time": "2025-12-30T...",
  "source": "test",
  "product_id": "test-001",
  "product_name": "Test Product",
  "category": "Test Category",
  "price": 99.99,
  "currency": "USD",
  "in_stock": true
}
```

**Checklist**:
- [ ] Topic raw_products có 1 message
- [ ] Topic alerts có 1 message
- [ ] Messages có định dạng JSON đúng
- [ ] Có đầy đủ các trường cần thiết

---

### Test 2.6: Test Streaming Crawler

**Mục tiêu**: Kiểm tra crawler tích hợp Kafka hoạt động

**Các bước thực hiện**:

```bash
# Bước 1: Chạy streaming crawler
cd d:\2025.1\BigData\BTL\baitapnhomit4931\crawl
python ecommerce_crawler_streaming.py
```

**Kết quả mong đợi**:
```
============================================================
🚀 Crawl Run Started
============================================================
⏰ Time: 2025-12-30T22:25:00+07:00
🌐 Source: webscraper.io
📡 Kafka: Enabled
✅ Kafka producer initialized

📂 Category: Computers
   Found 21 products
   [21/21] Crawling...
   ✅ Completed: 21 products

📂 Category: Phones
   Found 24 products
   [24/24] Crawling...
   ✅ Completed: 24 products

============================================================
📊 Run Summary
============================================================
Products crawled: 45
Kafka sent: 45
Total products: 45
============================================================

============================================================
📊 Final Statistics
============================================================
Total Attempts:    45
✅ Successful:     45
❌ Failed:         0
⚠️  Missing Fields: 5
📡 Kafka Sent:     45
📡 Kafka Failed:   0
📈 Success Rate:   100.0%
⭐ Avg Quality:    0.85
💾 Output File:    ...\ecommerce_raw.json
============================================================
```

**Checklist**:
- [ ] Crawler chạy thành công
- [ ] Kafka Enabled = true
- [ ] Kafka Sent = số sản phẩm crawled
- [ ] Kafka Failed = 0
- [ ] Success Rate = 100%

---

### Test 2.7: Xác Minh Messages Từ Crawler

**Mục tiêu**: Kiểm tra dữ liệu thực từ crawler trong Kafka

**Các bước thực hiện**:

```bash
# Xem 10 messages mới nhất
docker exec kafka-broker-1 kafka-console-consumer \
  --bootstrap-server kafka-broker-1:9092 \
  --topic raw_products \
  --from-beginning \
  --max-messages 10
```

**Hoặc dùng Kafka UI**:
```
1. Mở http://localhost:8080
2. Topics → raw_products → Messages
3. Thấy 45+ messages (1 test + 45 từ crawler)
4. Click vào message bất kỳ
5. Kiểm tra JSON structure
```

**Kiểm tra message**:
```json
{
  "crawl_time": "2025-12-30T22:25:15+07:00",
  "source": "webscraper.io",
  "product_id": "a1b2c3d4e5f6g7h8",
  "product_name": "Asus VivoBook...",
  "category": "Computers",
  "price": 295.99,
  "currency": "USD",
  "discount_price": null,
  "in_stock": true,
  "rating": 4.0,
  "num_reviews": 7,
  "data_quality_score": 0.85,
  "metadata": {
    "crawler_version": "2.0-streaming",
    "crawl_duration_ms": 1234,
    "http_status": 200
  }
}
```

**Checklist**:
- [ ] Có 45+ messages trong topic
- [ ] Messages có cấu trúc JSON đúng
- [ ] Có đầy đủ trường bắt buộc
- [ ] `crawler_version` = "2.0-streaming"
- [ ] Giá có biến động nhẹ (±5%)

---

### Test 2.8: Test Continuous Mode (Tùy Chọn)

**Mục tiêu**: Kiểm tra chế độ crawl liên tục

**Các bước thực hiện**:

```python
# Bước 1: Sửa file ecommerce_crawler_streaming.py
# Dòng 30: CONTINUOUS_MODE = True
# Dòng 29: CRAWL_INTERVAL_SECONDS = 60  # 1 phút cho test

# Bước 2: Chạy crawler
python ecommerce_crawler_streaming.py
```

**Kết quả mong đợi**:
```
============================================================
🔄 Starting Continuous Crawling Mode
============================================================
Interval: 60 seconds
Press Ctrl+C to stop
============================================================

🔄 Run #1
...
Products crawled: 45
Kafka sent: 45

⏳ Waiting 60 seconds until next run...

🔄 Run #2
...
Products crawled: 45
Kafka sent: 45

⏳ Waiting 60 seconds until next run...
```

**Kiểm tra**:
- [ ] Crawler chạy lặp lại tự động
- [ ] Mỗi lần chạy gửi messages vào Kafka
- [ ] Có thể dừng bằng Ctrl+C
- [ ] Kafka UI hiển thị số messages tăng dần

**Dừng crawler**: Nhấn `Ctrl+C`

---

### Test 2.9: Test Fault Tolerance - Kafka Down

**Mục tiêu**: Kiểm tra crawler hoạt động khi Kafka không khả dụng

**Các bước thực hiện**:

```bash
# Bước 1: Dừng Kafka
cd d:\2025.1\BigData\BTL\baitapnhomit4931\kafka
docker-compose stop

# Bước 2: Chạy crawler
cd ..\crawl
python ecommerce_crawler_streaming.py
```

**Kết quả mong đợi**:
```
⚠️  Failed to initialize Kafka producer: ...
   Continuing in file-only mode...

============================================================
🚀 Crawl Run Started
============================================================
📡 Kafka: Disabled

...

============================================================
📊 Run Summary
============================================================
Products crawled: 45
Kafka sent: 0
Total products: 45
============================================================
```

**Checklist**:
- [ ] Crawler vẫn chạy được
- [ ] Hiển thị warning về Kafka
- [ ] Kafka: Disabled
- [ ] Kafka sent = 0
- [ ] Dữ liệu vẫn được lưu vào file JSON

**Khôi phục Kafka**:
```bash
cd ..\kafka
docker-compose start
```

---

### Test 2.10: Test Performance - Throughput

**Mục tiêu**: Đo throughput của Kafka producer

**Các bước thực hiện**:

```python
# Tạo file test_throughput.py
import time
from kafka_producer import EcommerceKafkaProducer

producer = EcommerceKafkaProducer()

# Tạo test data
test_product = {
    "crawl_time": "2025-12-30T22:25:00+07:00",
    "product_id": "test",
    "product_name": "Test Product",
    "category": "Test",
    "price": 99.99,
    "currency": "USD",
    "in_stock": True
}

# Test throughput
num_messages = 1000
print(f"Sending {num_messages} messages...")

start_time = time.time()
for i in range(num_messages):
    producer.send_product(test_product, topic='raw_products')
end_time = time.time()

duration = end_time - start_time
throughput = num_messages / duration

print(f"\n📊 Throughput Test Results:")
print(f"Messages sent: {num_messages}")
print(f"Duration: {duration:.2f} seconds")
print(f"Throughput: {throughput:.0f} messages/second")

producer.close()
```

**Chạy test**:
```bash
python test_throughput.py
```

**Kết quả mong đợi**:
```
Sending 1000 messages...

📊 Throughput Test Results:
Messages sent: 1000
Duration: 2.50 seconds
Throughput: 400 messages/second
```

**Checklist**:
- [ ] Throughput > 100 messages/second
- [ ] Không có lỗi trong quá trình gửi
- [ ] Kafka UI hiển thị 1000+ messages mới

---

## 📊 Tổng Kết Test Phase 1 & 2

### Checklist Tổng Hợp

**Phase 1: Nguồn Dữ Liệu & Schema**
- [ ] Test 1.1: Crawler thu thập dữ liệu thành công
- [ ] Test 1.2: Schema đúng với thiết kế
- [ ] Test 1.3: Data quality score hoạt động
- [ ] Test 1.4: HTML snapshots được lưu
- [ ] Test 1.5: Deduplication hoạt động đúng

**Phase 2: Hạ Tầng Streaming**
- [ ] Test 2.1: Kafka cluster khởi động thành công
- [ ] Test 2.2: Topics được tạo đúng cấu hình
- [ ] Test 2.3: Kafka UI hoạt động
- [ ] Test 2.4: Producer gửi messages thành công
- [ ] Test 2.5: Messages được lưu trong Kafka
- [ ] Test 2.6: Streaming crawler hoạt động
- [ ] Test 2.7: Dữ liệu thực trong Kafka đúng
- [ ] Test 2.8: Continuous mode hoạt động (tùy chọn)
- [ ] Test 2.9: Graceful degradation khi Kafka down
- [ ] Test 2.10: Throughput đạt yêu cầu

### Metrics Mục Tiêu

| Metric | Target | Actual |
|--------|--------|--------|
| Crawler success rate | 100% | ___ |
| Avg data quality score | > 0.8 | ___ |
| Kafka topics created | 3 | ___ |
| Kafka brokers online | 3 | ___ |
| Producer throughput | > 100 msg/s | ___ |
| Kafka sent success rate | 100% | ___ |

---

## 🐛 Troubleshooting

### Vấn đề thường gặp

**1. Crawler không chạy được**
```bash
# Kiểm tra Python version
python --version  # Cần >= 3.8

# Cài lại dependencies
pip install -r requirements.txt --force-reinstall
```

**2. Kafka không khởi động**
```bash
# Xem logs
docker logs kafka-broker-1

# Restart
docker-compose restart

# Xóa volumes và khởi động lại
docker-compose down -v
docker-compose up -d
```

**3. Producer không kết nối được Kafka**
```bash
# Kiểm tra Kafka có chạy không
docker ps | grep kafka

# Test kết nối
telnet localhost 19092

# Kiểm tra firewall
# Windows: Tắt firewall tạm thời để test
```

**4. Messages không xuất hiện trong Kafka UI**
```bash
# Refresh trang
# Đợi 5-10 giây
# Kiểm tra bằng command line
docker exec kafka-broker-1 kafka-console-consumer \
  --bootstrap-server kafka-broker-1:9092 \
  --topic raw_products \
  --from-beginning \
  --max-messages 1
```

---

## 📝 Ghi Chú

- **Thời gian test**: Khoảng 30-45 phút cho tất cả tests
- **Thứ tự test**: Nên làm theo thứ tự từ 1.1 → 2.10
- **Lưu kết quả**: Chụp screenshots các kết quả quan trọng
- **Báo cáo**: Ghi lại metrics thực tế vào bảng tổng kết

---

**Cập nhật**: 2025-12-30  
**Version**: 1.0
