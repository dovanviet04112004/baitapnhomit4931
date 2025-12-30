# Pipeline Phân Tích Giá & Xu Hướng E-commerce Theo Thời Gian Thực - Danh Sách Công Việc

## Phase 1: Nguồn Dữ Liệu & Thiết Kế Schema
- [x] Xác định 1-3 website thương mại điện tử Việt Nam để crawl (dữ liệu công khai, thân thiện với crawling)
- [x] Ghi chép URLs (trang danh mục, trang chi tiết sản phẩm)
- [x] Thiết kế schema toàn diện với các trường bắt buộc:
  - [x] crawl_time, source, product_id/url, product_name, category
  - [x] price, currency, discount_price, availability
  - [x] rating, num_reviews, location
  - [x] raw_html_snapshot_path hoặc raw_json
- [x] Ghi chép các thách thức về dữ liệu (thiếu trường, không nhất quán, thay đổi schema, trùng lặp, giới hạn tốc độ)

## Phase 2: Tầng Thu Thập Dữ Liệu Streaming
- [x] Triển khai crawler gần thời gian thực (1-5 phút/lần)
- [x] Thiết lập Kafka cluster (phân tán)
- [x] Tạo các Kafka topics:
  - [x] `raw_products` - dữ liệu thô đã crawl
  - [x] `clean_products` - dữ liệu đã làm sạch
  - [x] `alerts` - cảnh báo bất thường về giá
- [x] Triển khai Kafka producer trong crawler
- [x] Thêm xử lý lỗi và logic thử lại

## Phase 3: Tầng Lưu Trữ Phân Tán
- [ ] Cấu hình HDFS cho lưu trữ phân tán (hoặc S3/GCS tương đương)
- [ ] Triển khai chiến lược phân vùng: `dt=YYYY-MM-DD/hr=HH`
- [ ] Thiết lập định dạng lưu trữ Parquet/JSONL
- [ ] Tạo cấu trúc thư mục:
  - [ ] `/data/raw/` - dữ liệu thô đã nhập
  - [ ] `/data/clean/` - dữ liệu đã làm sạch
  - [ ] `/data/aggregated/` - dữ liệu tổng hợp batch

## Phase 4: Tầng Xử Lý Batch
- [ ] Triển khai các Spark batch jobs (chạy theo lịch mỗi 6-24 giờ):
  - [ ] Giá trung bình theo ngày/danh mục
  - [ ] Top sản phẩm có biến động giá cao nhất
  - [ ] Phân bố giá theo danh mục
  - [ ] Phân tích xu hướng lịch sử
- [ ] Thiết lập lịch trình job (cron/Airflow)

## Phase 5: Tầng Xử Lý Thời Gian Thực
- [ ] Triển khai Spark Structured Streaming consumer
- [ ] Các view thời gian thực:
  - [ ] Giá mới nhất của từng sản phẩm
  - [ ] Phát hiện tăng/giảm giá đột ngột (z-score/phần trăm thay đổi)
  - [ ] Tạo cảnh báo thời gian thực
- [ ] Cấu hình checkpointing cho khả năng chịu lỗi

## Phase 6: Tầng Tìm Kiếm & Phục Vụ
- [ ] Thiết lập Elasticsearch cluster (phân tán)
- [ ] Tạo các indices:
  - [ ] `products_latest` - trạng thái sản phẩm hiện tại
  - [ ] `products_history` - dữ liệu giá lịch sử
  - [ ] `alerts` - cảnh báo bất thường
- [ ] Triển khai pipeline đánh index từ Spark
- [ ] Cấu hình mappings cho tìm kiếm full-text và aggregations

## Phase 7: Tầng Truy Vấn
- [ ] Triển khai các truy vấn thống kê:
  - [ ] Xu hướng/trung bình/phần trăm thay đổi theo thời gian
  - [ ] Phân tích biến động giá
- [ ] Triển khai các truy vấn tìm kiếm:
  - [ ] Tìm kiếm full-text theo tên sản phẩm
  - [ ] Lọc theo danh mục, khoảng giá, thời gian
  - [ ] Aggregations cho dashboard

## Phase 8: Tầng Trực Quan Hóa (Dashboard)
- [ ] Thiết lập Kibana
- [ ] Tạo 6+ biểu đồ bắt buộc:
  - [ ] Tổng số sản phẩm theo danh mục (bar/pie)
  - [ ] Xu hướng giá trung bình theo ngày (line chart)
  - [ ] Top 10 sản phẩm có thay đổi giá lớn nhất trong 24h (bar)
  - [ ] Heatmap/Histogram phân bố giá theo danh mục
  - [ ] Số lượng cảnh báo theo ngày/giờ (time series)
  - [ ] Bảng sản phẩm mới nhất với tìm kiếm/lọc
- [ ] Tạo dashboard tương tác với bộ lọc

## Phase 9: Triển Khai Kubernetes/Cloud
- [ ] Thiết lập Kubernetes cluster (minikube multi-node hoặc cloud K8s)
- [ ] Tạo deployment manifests:
  - [ ] Kafka cluster (tối thiểu 3 brokers)
  - [ ] HDFS/Storage layer (2+ nodes)
  - [ ] Spark workers (2+ nodes)
  - [ ] Elasticsearch cluster (2+ nodes)
  - [ ] Kibana
  - [ ] Crawler service
- [ ] Ghi chép topology (service nào chạy trên node nào)
- [ ] Cấu hình persistent volumes
- [ ] Thiết lập networking và service discovery

## Phase 10: Kiểm Tra Khả Năng Mở Rộng
- [ ] Test A - Mở Rộng Khối Lượng Dữ Liệu:
  - [ ] Tạo 10k → 100k records
  - [ ] Đo thời gian thực thi batch job
  - [ ] Đo throughput stream (records/giây)
  - [ ] Đo độ trễ end-to-end (ingest → dashboard)
  - [ ] Tạo biểu đồ hiệu suất
- [ ] Test B - Mở Rộng Tính Toán:
  - [ ] Test với 1, 2, 3 workers
  - [ ] So sánh thời gian xử lý batch
  - [ ] So sánh độ trễ stream
  - [ ] Ghi chép kết quả với bảng và biểu đồ

## Phase 11: Kiểm Tra Khả Năng Chịu Lỗi
- [ ] FT1 - Lỗi Spark Worker:
  - [ ] Kill 1 executor/worker trong khi streaming
  - [ ] Xác minh tự phục hồi
  - [ ] Kiểm tra mất dữ liệu (at-least-once semantics)
  - [ ] Ghi chép với logs và screenshots
- [ ] FT2 - Khởi Động Lại Service:
  - [ ] Restart Elasticsearch hoặc Kafka broker
  - [ ] Xác minh pipeline phục hồi
  - [ ] Xác minh dashboard phục hồi
  - [ ] Ghi chép hành vi mong đợi vs thực tế

## Phase 12: Tài Liệu & Sản Phẩm Bàn Giao
- [ ] Tạo sơ đồ kiến trúc (pipeline end-to-end)
- [ ] Ghi chép tất cả source code với comments
- [ ] Tạo hướng dẫn triển khai
- [ ] Export Kibana dashboards
- [ ] Viết báo cáo toàn diện:
  - [ ] Nguồn dữ liệu và thách thức
  - [ ] Thiết kế schema
  - [ ] Luồng pipeline (batch vs realtime)
  - [ ] Ví dụ truy vấn
  - [ ] Kết quả test khả năng mở rộng
  - [ ] Kết quả test khả năng chịu lỗi
  - [ ] Insights và kết luận
