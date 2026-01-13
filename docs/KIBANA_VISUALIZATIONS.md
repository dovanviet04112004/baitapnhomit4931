# Kibana Visualizations Guide - Crypto Real-time Analytics

## 📊 4 Biểu đồ chính để visualize dữ liệu streaming

### Setup ban đầu

**1. Tạo Data Views:**
- Vào **Stack Management** → **Data Views**
- Tạo 2 data views:
  - `crypto_alerts_realtime` với time field: `alert_time`
  - `crypto_sentiment_realtime` với time field: `window_start`

---

## 🚨 ALERTS VISUALIZATIONS (2 biểu đồ)

### 1️⃣ Alert Type Distribution - Phân bố loại alerts

**Mục đích:** Xem có bao nhiêu PUMP vs DUMP alerts, phân loại 1h vs 24h

**Loại biểu đồ:** Vertical Bar Chart

**Cách tạo:**

1. **Vào Visualize Library** → Create visualization → **Lens**
2. **Chọn data view:** `crypto_alerts_realtime`
3. **Time range:** Last 15 minutes (hoặc Last 1 hour)
4. **Configuration:**

```
Chart type: Vertical Bar
Horizontal axis: alert_type (Terms)
  - Field: alert_type
  - Order by: Count (descending)
  - Size: 10

Vertical axis: Count
  - Function: Count of records

Break down by: không cần
```

5. **Panel settings:**
   - Title: "Alert Type Distribution (Real-time)"
   - Color: Mỗi alert type 1 màu khác nhau

**Kết quả mong đợi:**
```
PUMP_24H  |████████ 15
DUMP_1H   |██████ 12
PUMP_1H   |█████ 10
DUMP_24H  |███ 5
```

---

### 2️⃣ Top Pumping/Dumping Coins - Top coins có alerts

**Mục đích:** Xem coins nào đang pump/dump mạnh nhất với giá và % thay đổi

**Loại biểu đồ:** Data Table

**Cách tạo:**

1. **Create visualization** → **Lens**
2. **Chọn data view:** `crypto_alerts_realtime`
3. **Configuration:**

```
Chart type: Table

Rows (theo thứ tự):
1. symbol.keyword (Terms)
   - Size: 15
   - Order by: alert_time (descending)

2. alert_type (Terms)
   - Size: 10

Metrics (cột):
1. Alert Count
   - Function: Count

2. Latest Price
   - Function: Last value
   - Field: current_price

3. Change 1H (%)
   - Function: Last value
   - Field: change_1h
   - Format: Percent (2 decimals)

4. Change 24H (%)
   - Function: Last value
   - Field: change_24h
   - Format: Percent (2 decimals)

5. Latest Alert Time
   - Function: Last value
   - Field: alert_time
```

4. **Sorting:** Alert Time descending (mới nhất lên đầu)

**Kết quả mong đợi:**
```
Symbol | Alert Type | Count | Latest Price | Change 1H | Change 24H | Alert Time
-------|------------|-------|--------------|-----------|------------|------------
BTC    | PUMP_24H   | 3     | $45,234      | 2.5%      | 12.3%      | 14:30:15
ETH    | DUMP_1H    | 2     | $2,845       | -5.2%     | 3.4%       | 14:29:45
XRP    | PUMP_1H    | 1     | $0.65        | 6.1%      | -2.1%      | 14:28:30
```

---

## 😊 MARKET SENTIMENT VISUALIZATIONS (2 biểu đồ)

### 3️⃣ Market Sentiment Over Time - Xu hướng thị trường

**Mục đích:** Xem % bullish vs bearish thay đổi theo thời gian

**Loại biểu đồ:** Line Chart (Multi-line) - KHUYÊN DÙNG

**Cách tạo (Option A - Line Chart - Rõ ràng nhất):**

1. **Create visualization** → **Lens**
2. **Chọn data view:** `crypto_sentiment_realtime`
3. **Configuration:**

```
Chart type: Line

Horizontal axis: window_start (Date histogram)
  - Interval: 1 minute

Vertical axis (Multiple lines):
1. Bullish %
   - Function: Average
   - Field: bullish_pct
   - Label: "Bullish %"
   - Color: #00CC66 (Green)
   - Line width: 2px

2. Bearish %
   - Function: Average
   - Field: bearish_pct
   - Label: "Bearish %"
   - Color: #DC3545 (Red)
   - Line width: 2px

Panel settings:
  - Show legend: Yes (Right side)
  - Y-axis label: "Percentage (%)"
  - Y-axis range: 0-100
  - Grid lines: Show
```

**Kết quả:** 2 đường riêng biệt, dễ so sánh

---

**Cách tạo (Option B - Area Stacked - Nếu muốn fill):**

```
Chart type: Area

⚠️ LƯU Ý QUAN TRỌNG:
Trong Lens, khi add layers, PHẢI:
1. Click "Add layer" cho mỗi series
2. Chọn màu CỤ THỂ cho từng layer:
   - Layer 1 (bullish_pct): 
     → Click color picker → Chọn Green (#00CC66)
   - Layer 2 (bearish_pct): 
     → Click color picker → Chọn Red (#DC3545)
3. Đảm bảo trong Legend hiển thị đúng màu

Panel settings:
  - Stack: None (hoặc "Stacked" nếu muốn chồng)
  - Fill opacity: 30-50%
  - Legend position: Right
```

**Kết quả mong đợi:**

**Option A (Line Chart):**
```
100% |                    
     |  ---- GREEN LINE (Bullish 70-80%)
75%  |                    
     |  
50%  |  
     |  
25%  |  ---- RED LINE (Bearish 20-30%)
0%   |_____________________
     14:20  14:25  14:30 (time)
```
→ Dễ đọc, thấy rõ trend của từng metric

**Option B (Area Stacked):**
```
100% |  [Vùng đỏ - Bearish]
     |  
75%  |  [Ranh giới]
     |  [Vùng xanh lá - Bullish]
0%   |_____________________
```
→ Thấy tỷ lệ tương đối

**💡 Giải thích biểu đồ của bạn:**
- Vùng xanh lớn (dưới) = Thị trường đang BULLISH
- Vùng hồng nhỏ (trên) = Chỉ ít coins đang giảm
- Điều này là BÌNH THƯỜNG khi thị trường tốt!

---

### 4️⃣ Current Market Sentiment - Sentiment hiện tại

**Mục đích:** Hiển thị sentiment hiện tại và các metrics chính

**Chọn 1 trong các options sau:**

---

#### **Option A: Gauge Chart** ⭐ RECOMMENDED - Trực quan nhất

**Loại:** Gauge (đồng hồ đo)

**Cách tạo:**

1. **Create visualization** → **Lens**
2. **Chọn data view:** `crypto_sentiment_realtime`
3. **Time range:** Last 5 minutes
4. **Configuration:**

```
Chart type: Gauge

Primary metric:
  - Function: Last value
  - Field: bullish_pct
  - Label: "Market Sentiment"

Goal/Target: 50 (neutral point)

Color ranges:
  - 0-30: #DC3545 (Red) - "Strong Bearish"
  - 30-45: #FF6B6B (Light Red) - "Bearish"
  - 45-55: #FFC107 (Yellow) - "Neutral"
  - 55-70: #90EE90 (Light Green) - "Bullish"
  - 70-100: #00CC66 (Green) - "Strong Bullish"

Panel settings:
  - Show percentage: Yes
  - Size: Large (200px height)
```

**Kết quả:**
```
┌─────────────────────┐
│  Market Sentiment   │
│        ___          │
│      /     \        │
│     |   65% |  🟢  │ (Gauge pointer ở vùng xanh)
│      \_____/        │
│   BULLISH           │
└─────────────────────┘
```

---

#### **Option B: Horizontal Bar (Progress Bar Style)**

**Loại:** Horizontal Bar - Nhìn như thanh tiến trình

**Cách tạo:**

1. **Create visualization** → **Lens**
2. **Configuration:**

```
Chart type: Bar horizontal

Y-axis (Categories): Static value "Sentiment"
X-axis (Values): 
  - Last value of bullish_pct (Green bar)
  - Last value of bearish_pct (Red bar)

Color mapping:
  - Bullish → Green
  - Bearish → Red

Panel settings:
  - Show data labels: Yes
  - Axis range: 0-100
  - Orientation: Horizontal
```

**Kết quả:**
```
Bullish  |████████████████░░░░| 75%
Bearish  |████░░░░░░░░░░░░░░░░| 20%
```

---

#### **Option C: Vertical Bar (Side by side)**

**Loại:** Vertical Bar - So sánh trực quan

**Cách tạo:**

1. **Create visualization** → **Lens**
2. **Configuration:**

```
Chart type: Bar vertical

X-axis: Terms (Manual categories)
  - "Bullish"
  - "Bearish"
  - "Neutral"

Y-axis: 3 bars với giá trị riêng biệt

⚠️ CÁCH TẠO 3 BARS:

**Cách 1: Dùng 3 visualizations riêng (Đơn giản nhất)**
- Tạo 3 Metric panels riêng biệt cho Bullish, Bearish, Neutral
- Arrange chúng ngang nhau trong dashboard

**Cách 2: Dùng Formula trong Lens**

Bar 1 - Bullish:
  - Function: Formula
  - Formula: `last_value(bullish_pct)`
  - Label: "Bullish %"
  - Color: Green

Bar 2 - Bearish:
  - Function: Formula
  - Formula: `last_value(bearish_pct)`
  - Label: "Bearish %"
  - Color: Red

Bar 3 - Neutral:
  - Function: Formula
  - Formula: `100 - last_value(bullish_pct) - last_value(bearish_pct)`
  - Label: "Neutral %"
  - Color: Gray

**Cách viết Formula:**
1. Add layer → Click "Formula"
2. Trong formula box, gõ:
   ```
   100 - last_value(bullish_pct) - last_value(bearish_pct)
   ```
3. Hoặc đơn giản hơn, dùng field có sẵn:
   ```
   last_value(neutral_count) / last_value(total_coins) * 100
   ```

Colors:
  - Bullish bar: Green (#00CC66)
  - Bearish bar: Red (#DC3545)
  - Neutral bar: Gray (#6C757D)

Panel settings:
  - Y-axis: 0-100%
  - Show values on top: Yes
```

**Kết quả:**
```
100%|     
    |     ████
75% |     ████  75%
    |     ████
50% |     ████  
    |  ████████ 
25% |  ████████  20%  5%
    |  ████████ ████ ████
  0%|──────────────────
     Bullish  Bearish Neutral
```

---

#### **Option D: Simple Metric Grid** (Giống Option gốc)

**Loại:** Metric

```
Chart type: Metric

4 metrics hiển thị:
1. sentiment (BULLISH/BEARISH/NEUTRAL)
2. bullish_pct
3. bearish_pct  
4. avg_change_24h

Kết quả:
┌─────────────────────┐
│   BULLISH  🟢      │
│                     │
│   Bullish: 65.5%   │
│   Bearish: 28.3%   │
│   Avg: +3.2%       │
└─────────────────────┘
```

---

#### **Option E: Single Number (Minimal)**

**Loại:** Metric với 1 số duy nhất

**Cách tạo:**

```
Chart type: Metric

Single metric:
  - Function: Last value
  - Field: bullish_pct
  - Format: Percentage
  - Font size: 72px

Color rules:
  - Value < 40: Red
  - Value 40-60: Yellow
  - Value > 60: Green

Secondary text: "Market Bullish %"

Kết quả:
┌─────────────────────┐
│                     │
│       75%           │ (Huge green number)
│                     │
│  Market Bullish %   │
└─────────────────────┘
```

---

### 💡 Khuyến nghị:

| Option | Ưu điểm | Nhược điểm | Dùng khi |
|--------|---------|-----------|----------|
| **A. Gauge** | Trực quan, dễ hiểu ngay | Chỉ hiện 1 metric | Muốn focus vào bullish % |
| **B. Horizontal Bar** | So sánh 2 metrics rõ | Ít info | Dashboard đơn giản |
| **C. Vertical Bar** | Thấy cả 3 metrics | Hơi phức tạp | Cần thấy neutral % |
| **D. Metric Grid** | Nhiều info nhất | Mất nhiều space | Dashboard chi tiết |
| **E. Single Number** | Minimal, đẹp | Ít context | Dashboard tổng quan |

**👉 Gợi ý:** Dùng **Option A (Gauge)** nếu muốn đẹp và dễ hiểu, hoặc **Option C (Vertical Bar)** nếu muốn thấy đầy đủ 3 metrics!

---

## 🎨 Tạo Dashboard tổng hợp

**Tạo Dashboard mới:**

1. Vào **Dashboard** → Create dashboard
2. Add cả 4 visualizations vừa tạo
3. **Layout đề xuất:**

```
┌─────────────────────────────────────────────┐
│  🚨 Alert Type Distribution │ 😊 Market    │
│  (Bar Chart)                 │ Sentiment    │
│                              │ (Metric)     │
├─────────────────────────────┼──────────────┤
│  📊 Market Sentiment Over Time              │
│  (Area Chart - Full width)                  │
├─────────────────────────────────────────────┤
│  📋 Top Pumping/Dumping Coins               │
│  (Table - Full width)                       │
└─────────────────────────────────────────────┘
```

4. **Dashboard Settings:**
   - Title: "Crypto Real-time Analytics - Speed Layer"
   - Auto-refresh: 10 seconds
   - Time range: Last 15 minutes

---

## ⚙️ Settings nâng cao

### Alerts Visualization - Conditional Formatting

**Trong Top Coins Table, add color rules:**

```javascript
// Change 1H column
if (value > 5) → Green background
if (value < -5) → Red background

// Change 24H column  
if (value > 10) → Dark green background
if (value < -10) → Dark red background
```

### Market Sentiment - Threshold Lines

**Trong Area Chart, add reference lines:**

```
Bullish threshold: 60% (Green dashed line)
Bearish threshold: 40% (Red dashed line)
```

---

## 🔍 Filters hữu ích

**Add filters vào Dashboard:**

1. **Alert Type filter:**
   - Field: `alert_type`
   - Type: Multi-select
   - Options: PUMP_1H, DUMP_1H, PUMP_24H, DUMP_24H

2. **Coin Symbol filter:**
   - Field: `symbol.keyword`
   - Type: Search/Select
   - Top 20 coins

3. **Sentiment filter:**
   - Field: `sentiment`
   - Type: Buttons
   - Options: BULLISH, BEARISH, NEUTRAL

---

## 📸 Export & Share

**Sau khi tạo xong:**

1. Save dashboard với tên: `crypto_realtime_analytics`
2. Copy shareable link
3. Hoặc export PDF: Dashboard → Share → PDF Reports

---

## 🐛 Troubleshooting

**Không có data?**

1. Check indices có data không:
```bash
curl -X GET "http://localhost:5601/api/console/proxy?path=crypto_alerts_realtime/_count&method=GET"
curl -X GET "http://localhost:5601/api/console/proxy?path=crypto_sentiment_realtime/_count&method=GET"
```

2. Check Spark Streaming có chạy không:
```bash
kubectl logs -f -n crypto-pipeline -l app=spark-streaming
```

3. Check Consumer có chạy không:
```bash
kubectl logs -f -n crypto-pipeline -l app=kafka-to-es-consumer
```

4. Refresh data views: Stack Management → Data Views → Refresh field list

---

## 📚 Tham khảo thêm

- Kibana Lens documentation: https://www.elastic.co/guide/en/kibana/current/lens.html
- Time series visualization: https://www.elastic.co/guide/en/kibana/current/tsvb.html
- Dashboard best practices: https://www.elastic.co/guide/en/kibana/current/dashboard.html
