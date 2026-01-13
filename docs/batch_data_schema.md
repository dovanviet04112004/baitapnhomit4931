# Batch Data Schema - PostgreSQL Tables

## Tổng quan
Hệ thống batch processing tạo ra 3 bảng metrics chính từ dữ liệu crypto:
- **daily_metrics**: Metrics theo ngày cho từng coin
- **weekly_metrics**: Metrics theo tuần (tổng hợp từ daily)
- **monthly_metrics**: Metrics theo tháng (tổng hợp từ daily)

---

## 1. daily_metrics

**Mô tả**: Dữ liệu tổng hợp theo ngày cho mỗi coin

### Các trường chính:

#### Thời gian
- `date` (date): Ngày giao dịch
- `year`, `month`: Năm, tháng
- `week_of_year`: Tuần thứ mấy trong năm (1-52)
- `day_of_week`: Thứ trong tuần (1=CN, 2=T2, ...)

#### Thông tin coin
- `coin_id`: ID của coin (ví dụ: bitcoin, ethereum)
- `symbol`: Ký hiệu (BTC, ETH)
- `name`: Tên đầy đủ (Bitcoin, Ethereum)

#### Giá (Price)
- `open_price`: Giá mở cửa (giá đầu tiên trong ngày)
- `close_price`: Giá đóng cửa (giá cuối cùng trong ngày)
- `high_price`: Giá cao nhất trong ngày
- `low_price`: Giá thấp nhất trong ngày

#### Metrics tính toán
- `return_pct_day`: % thay đổi giá trong ngày = (close - open) / open * 100
- `volatility_day`: Độ biến động = (high - low) / open * 100
- `price_range_day`: Khoảng giá = high - low

#### Khối lượng & Vốn hóa
- `volume_sum_day`: Tổng khối lượng giao dịch trong ngày
- `market_cap_close`: Vốn hóa thị trường (cuối ngày)
- `market_cap_rank_close`: Xếp hạng vốn hóa

#### Xếp hạng
- `rank_open`: Xếp hạng theo giá mở cửa (trong tất cả coins cùng ngày)
- `rank_close`: Xếp hạng theo giá đóng cửa
- `rank_change`: Thay đổi xếp hạng = rank_open - rank_close

#### Metadata
- `record_count`: Số lượng records raw được tổng hợp

**Partition**: Theo `year` và `month`

---

## 2. weekly_metrics

**Mô tả**: Dữ liệu tổng hợp theo tuần cho mỗi coin

### Các trường chính:

#### Thời gian
- `year`: Năm
- `week_of_year`: Tuần thứ mấy trong năm
- `week_start_date`: Ngày bắt đầu tuần
- `week_end_date`: Ngày kết thúc tuần

#### Thông tin coin
- `coin_id`, `symbol`, `name`: Giống daily_metrics

#### Giá (Price)
- `open_price_week`: Giá mở cửa tuần (từ ngày đầu tuần)
- `close_price_week`: Giá đóng cửa tuần (từ ngày cuối tuần)
- `high_price_week`: Giá cao nhất trong tuần
- `low_price_week`: Giá thấp nhất trong tuần

#### Metrics tính toán
- `return_pct_week`: % thay đổi giá trong tuần
- `volatility_week`: Độ biến động tuần = (high - low) / open * 100
- `avg_volatility_week`: Trung bình volatility hàng ngày

#### Khối lượng & Xếp hạng
- `volume_sum_week`: Tổng khối lượng giao dịch trong tuần
- `avg_rank_week`: Xếp hạng trung bình trong tuần

#### Metadata
- `days_in_week`: Số ngày có dữ liệu trong tuần

**Partition**: Theo `year`

---

## 3. monthly_metrics

**Mô tả**: Dữ liệu tổng hợp theo tháng cho mỗi coin

### Các trường chính:

#### Thời gian
- `year`: Năm
- `month`: Tháng (1-12)
- `month_start_date`: Ngày bắt đầu tháng
- `month_end_date`: Ngày kết thúc tháng

#### Thông tin coin
- `coin_id`, `symbol`, `name`: Giống daily_metrics

#### Giá (Price)
- `open_price_month`: Giá mở cửa tháng
- `close_price_month`: Giá đóng cửa tháng
- `high_price_month`: Giá cao nhất trong tháng
- `low_price_month`: Giá thấp nhất trong tháng

#### Metrics tính toán
- `return_pct_month`: % thay đổi giá trong tháng
- `volatility_month`: Độ biến động tháng
- `avg_volatility_month`: Trung bình volatility hàng ngày

#### Khối lượng & Xếp hạng
- `volume_sum_month`: Tổng khối lượng giao dịch trong tháng
- `avg_rank_month`: Xếp hạng trung bình trong tháng

#### Metadata
- `days_in_month`: Số ngày có dữ liệu trong tháng

**Partition**: Theo `year`

---

## Ghi chú quan trọng

### Cách phân biệt dữ liệu giữa các ngày
- Dữ liệu được phân tách theo trường `date` (kiểu date)
- Mỗi coin có 1 record cho mỗi ngày
- Partition theo `year/month` giúp truy vấn nhanh

### Cách tính toán
- **Daily**: Tổng hợp từ raw data theo `coin_id` và `date`
- **Weekly**: Tổng hợp từ daily_metrics theo `coin_id`, `year`, `week_of_year`
- **Monthly**: Tổng hợp từ daily_metrics theo `coin_id`, `year`, `month`

### Incremental Processing
- Hệ thống sử dụng checkpoint để xử lý incremental
- Chỉ re-calculate các partition bị ảnh hưởng
- Dynamic partition overwrite đảm bảo hiệu suất

---

## Ví dụ Use Cases cho Web App

### 1. Dashboard Overview
- Top movers hôm nay: `ORDER BY return_pct_day DESC LIMIT 10`
- Coins biến động nhất: `ORDER BY volatility_day DESC`
- Market cap ranking: `ORDER BY market_cap_rank_close`

### 2. Price Charts
- Candlestick chart: Sử dụng `open_price`, `close_price`, `high_price`, `low_price`
- Line chart: Theo dõi `close_price` theo thời gian

### 3. Performance Analysis
- So sánh performance: `return_pct_day` vs `return_pct_week` vs `return_pct_month`
- Volatility trends: Theo dõi `volatility_day` theo thời gian

### 4. Volume Analysis
- Volume trends: `volume_sum_day` theo thời gian
- Volume vs Price correlation

### 5. Ranking Changes
- Track rank changes: `rank_change` để xem coins tăng/giảm hạng
- Historical ranking: Theo dõi `rank_close` theo thời gian
