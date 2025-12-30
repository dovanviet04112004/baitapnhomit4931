# Hướng Dẫn Kafka Streaming

## Tổng Quan
Hướng dẫn này giải thích cách thiết lập và chạy hạ tầng Kafka streaming cho pipeline phân tích giá e-commerce.

## Kiến Trúc
```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│   Crawler   │─────▶│    Kafka     │─────▶│    Spark    │
│  (Producer) │      │   Cluster    │      │  Streaming  │
└─────────────┘      └──────────────┘      └─────────────┘
                            │
                            ▼
                     ┌──────────────┐
                     │  Kafka UI    │
                     │ (Giám Sát)   │
                     └──────────────┘
```

## Các Thành Phần

### 1. Kafka Cluster (3 Brokers)
- **Broker 1**: Port 19092 (localhost)
- **Broker 2**: Port 19093 (localhost)
- **Broker 3**: Port 19094 (localhost)
- **Replication Factor**: 2
- **Partitions mỗi Topic**: 3

### 2. Topics
| Topic | Retention | Mục Đích |
|-------|-----------|----------|
| `raw_products` | 7 ngày | Dữ liệu sản phẩm thô đã crawl |
| `clean_products` | 30 ngày | Dữ liệu đã làm sạch và validate |
| `alerts` | 90 ngày | Cảnh báo bất thường về giá |

### 3. Kafka UI
- **URL**: http://localhost:8080
- **Mục đích**: Giám sát topics, messages, consumer groups

## Hướng Dẫn Thiết Lập

### Bước 1: Khởi Động Kafka Cluster

**Windows:**
```bash
cd kafka
docker-compose up -d
```

**Linux/Mac:**
```bash
cd kafka
docker-compose up -d
```

### Bước 2: Xác Minh Kafka Đang Chạy

Kiểm tra trạng thái container:
```bash
docker ps
```

Bạn sẽ thấy:
- `zookeeper`
- `kafka-broker-1`
- `kafka-broker-2`
- `kafka-broker-3`
- `kafka-ui`

### Bước 3: Tạo Topics

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

### Bước 4: Xác Minh Topics

Truy cập Kafka UI: http://localhost:8080

Hoặc dùng command line:
```bash
docker exec kafka-broker-1 kafka-topics --list --bootstrap-server kafka-broker-1:9092
```

Kết quả mong đợi:
```
alerts
clean_products
raw_products
```

### Bước 5: Cài Đặt Python Dependencies

```bash
cd crawl
pip install -r requirements.txt
```

### Bước 6: Test Kafka Producer

```bash
cd crawl
python kafka_producer.py
```

Kết quả mong đợi:
```
Testing Kafka Producer
Sending test product...
✅ Test message sent successfully!
Sending test alert...
✅ Test alert sent successfully!
```

### Bước 7: Chạy Streaming Crawler

**Chạy một lần:**
```bash
cd crawl
python ecommerce_crawler_streaming.py
```

**Chế độ liên tục (sửa file trước):**
Đặt `CONTINUOUS_MODE = True` trong `ecommerce_crawler_streaming.py`, sau đó:
```bash
python ecommerce_crawler_streaming.py
```

## Giám Sát

### Kafka UI Dashboard
1. Mở http://localhost:8080
2. Chọn cluster: `bigdata-cluster`
3. Xem topics, messages, và metrics

### Xem Messages trong Topic

**Raw products:**
```bash
docker exec kafka-broker-1 kafka-console-consumer \
  --bootstrap-server kafka-broker-1:9092 \
  --topic raw_products \
  --from-beginning \
  --max-messages 10
```

**Alerts:**
```bash
docker exec kafka-broker-1 kafka-console-consumer \
  --bootstrap-server kafka-broker-1:9092 \
  --topic alerts \
  --from-beginning
```

### Kiểm Tra Consumer Lag

```bash
docker exec kafka-broker-1 kafka-consumer-groups \
  --bootstrap-server kafka-broker-1:9092 \
  --list
```

## Xử Lý Sự Cố

### Vấn đề: Không kết nối được Kafka

**Giải pháp:**
1. Kiểm tra containers đang chạy: `docker ps`
2. Xem logs: `docker logs kafka-broker-1`
3. Restart cluster: `docker-compose restart`

### Vấn đề: Topics không được tạo

**Giải pháp:**
1. Đợi 10 giây sau khi start cluster
2. Chạy lại script create_topics
3. Xác minh: `docker exec kafka-broker-1 kafka-topics --list --bootstrap-server kafka-broker-1:9092`

### Vấn đề: Producer connection timeout

**Giải pháp:**
1. Xác minh Kafka có thể truy cập: `telnet localhost 19092`
2. Kiểm tra firewall settings
3. Dùng đúng bootstrap server: `localhost:19092`

### Vấn đề: Messages không xuất hiện trong topic

**Giải pháp:**
1. Kiểm tra producer logs để tìm lỗi
2. Xác minh topic tồn tại
3. Kiểm tra Kafka UI để xem số lượng message
4. Đảm bảo producer dùng đúng tên topic

## Tối Ưu Hiệu Suất

### Cài Đặt Producer
- `acks='all'`: Đợi tất cả replicas (độ bền cao)
- `compression_type='snappy'`: Nén messages
- `linger_ms=10`: Batch messages để hiệu quả hơn
- `batch_size=16384`: Kích thước batch tính bằng bytes

### Cài Đặt Topic
- `retention.ms`: Thời gian giữ messages
- `compression.type='snappy'`: Nén ở mức topic
- `min.insync.replicas=2`: Số replicas tối thiểu để ack

## Dọn Dẹp

### Dừng Kafka Cluster
```bash
cd kafka
docker-compose down
```

### Xóa Tất Cả Dữ Liệu (bao gồm volumes)
```bash
cd kafka
docker-compose down -v
```

## Bước Tiếp Theo

Sau khi Kafka chạy:
1. ✅ Kafka cluster hoạt động
2. ✅ Topics đã tạo
3. ✅ Producer đã test
4. ⏭️ Tiếp theo: Triển khai Spark Structured Streaming consumer (Phase 3)

## Tài Liệu Tham Khảo

- Kafka Documentation: https://kafka.apache.org/documentation/
- kafka-python: https://kafka-python.readthedocs.io/
- Kafka UI: https://github.com/provectus/kafka-ui
