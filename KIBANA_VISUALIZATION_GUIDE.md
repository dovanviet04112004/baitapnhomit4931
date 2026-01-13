# 📊 Kibana Visualization Guide - Top 4 Charts

## 🎯 **Overview**

Hướng dẫn tạo **4 biểu đồ đẹp và trực quan nhất** trong Kibana để phân tích dữ liệu crypto từ batch processing pipeline.

**Phân bổ:**
- ✅ **Daily Metrics**: 2 biểu đồ
- ✅ **Weekly Metrics**: 1 biểu đồ  
- ✅ **Monthly Metrics**: 1 biểu đồ

---

## 🚀 **STEP 1: Setup Index Patterns**

### **1.1. Access Kibana**

```
URL: http://localhost:5601
```

### **1.2. Create Index Patterns**

**Navigation:** Stack Management → Data Views → Create data view

#### **Index Pattern 1: Daily Crypto Metrics**

```
Name: Daily Crypto Metrics
Index pattern: daily_metrics
Timestamp field: @timestamp
```

**Key Fields:**
- `coin_id`, `symbol`, `name` (keyword/text)
- `date`, `@timestamp` (date)
- `open_price`, `close_price`, `high_price`, `low_price` (double)
- `return_pct_day`, `volatility_day` (double)
- `volume_sum_day` (long)
- `market_cap_close` (long)

#### **Index Pattern 2: Weekly Crypto Metrics**

```
Name: Weekly Crypto Metrics
Index pattern: weekly_metrics
Timestamp field: @timestamp
```

**Key Fields:**
- `coin_id`, `symbol`, `name`
- `week_of_year` (integer)
- `week_start_date`, `week_end_date` (date)
- `return_pct_week`, `volatility_week` (double)
- `volume_sum_week` (long)

#### **Index Pattern 3: Monthly Crypto Metrics**

```
Name: Monthly Crypto Metrics
Index pattern: monthly_metrics
Timestamp field: @timestamp
```

**Key Fields:**
- `coin_id`, `symbol`, `name`
- `month_start_date`, `month_end_date` (date)
- `close_price_month` (double)
- `return_pct_month`, `volatility_month` (double)

---

## 📈 **STEP 2: Create 4 Beautiful Visualizations**

---

### **1️⃣ Multi-Coin Price Trend (Line Chart)** 📈

> **Category:** Daily Metrics  
> **Why:** Trực quan nhất, hiển thị xu hướng giá của nhiều coin theo thời gian

#### **Configuration**

```yaml
Type: Line Chart
Data View: Daily Crypto Metrics

Y-axis:
  - Aggregation: Average
  - Field: close_price
  - Custom label: "Price (USD)"

X-axis:
  - Aggregation: Date Histogram
  - Field: @timestamp
  - Minimum interval: 1 day

Breakdown:
  - Field: symbol.keyword
  - Size: 5
  - Include: BTC, ETH, BNB, SOL, ADA

Time range: Last 30 days

Panel settings:
  - Legend position: Right
  - Show dots: false
  - Line width: 2
  - Smooth lines: true
```

#### **Step-by-Step Instructions**

1. **Create Visualization**
   - Navigate to: `Analytics → Visualize Library → Create visualization`
   - Select: `Line`
   - Choose data view: `Daily Crypto Metrics`

2. **Configure Y-axis**
   - Click `Add field` under Y-axis
   - Select `close_price`
   - Change aggregation to `Average`
   - Custom label: `Price (USD)`

3. **Configure X-axis**
   - Click `Add field` under X-axis
   - Select `@timestamp`
   - Aggregation: `Date Histogram`
   - Interval: `1 day`

4. **Add Breakdown (Multiple Lines)**
   - Click `Add` under `Breakdown`
   - Field: `symbol.keyword`
   - Size: `5`
   - Advanced → Include patterns: `BTC|ETH|BNB|SOL|ADA` (regex)

5. **Set Time Range**
   - Top right corner: Select `Last 30 days`

6. **Panel Settings**
   - Click `Settings` icon
   - Legend position: `Right`
   - Show dots: `Off`
   - Line width: `2`

7. **Save**
   - Click `Save`
   - Title: `Multi-Coin Price Trend`
   - Description: `Price trends for top 5 cryptocurrencies over 30 days`
   - Tags: `daily`, `price`, `trend`

#### **Expected Result**

```
Price (USD)
    │     ╱╲    ╱╲
60K │   ╱    ╲╱    ╲  ← BTC (Blue)
    │  ╱              ╲
40K │ ╱                ╲
    │╱                  ╲  ← ETH (Green)
20K │────────────────────────
    └─────────────────────────→ Time
    Jan 1    Jan 15    Jan 30
```

**Visual Features:**
- ✨ Smooth, colorful lines for each coin
- 📊 Clear price comparison across coins
- 🎨 Auto-assigned colors for each symbol
- 📈 Easy to spot trends and patterns

---

### **2️⃣ Volume Heatmap** 🔥

> **Category:** Daily Metrics  
> **Why:** Cực kỳ đẹp mắt với gradient colors, dễ phát hiện patterns

#### **Configuration**

```yaml
Type: Heat Map
Data View: Daily Crypto Metrics

Value:
  - Aggregation: Sum
  - Field: volume_sum_day
  - Custom label: "Trading Volume"

X-axis (Horizontal):
  - Aggregation: Date Histogram
  - Field: @timestamp
  - Interval: Daily

Y-axis (Vertical):
  - Aggregation: Terms
  - Field: symbol.keyword
  - Size: 10
  - Order by: Metric (Sum volume_sum_day)
  - Order: Descending

Color scale:
  - Palette: Green to Red
  - Steps: 5
  - Reverse: false

Time range: Last 30 days
```

#### **Step-by-Step Instructions**

1. **Create Visualization**
   - Navigate to: `Analytics → Visualize Library → Create visualization`
   - Select: `Heat map`
   - Choose data view: `Daily Crypto Metrics`

2. **Configure Value (Color Intensity)**
   - Click `Add field` under Value
   - Select `volume_sum_day`
   - Aggregation: `Sum`
   - Custom label: `Trading Volume`

3. **Configure X-axis (Time)**
   - Click `Add field` under X-axis
   - Select `@timestamp`
   - Aggregation: `Date Histogram`
   - Interval: `1 day`

4. **Configure Y-axis (Coins)**
   - Click `Add field` under Y-axis
   - Select `symbol.keyword`
   - Aggregation: `Terms`
   - Size: `10`
   - Order by: `Metric: Sum volume_sum_day`
   - Order: `Descending`

5. **Configure Colors**
   - Click `Settings` icon
   - Color palette: `Green to Red`
   - Number of steps: `5`
   - Reverse colors: `Off`

6. **Set Time Range**
   - Top right: `Last 30 days`

7. **Save**
   - Title: `Volume Heatmap`
   - Description: `Daily trading volume heatmap for top 10 coins`
   - Tags: `daily`, `volume`, `heatmap`

#### **Expected Result**

```
Symbol
BTC  │🟢🟡🟡🔴🔴🟡🟢🟢🟡🔴│
ETH  │🟡🟡🔴🔴🟢🟢🟡🟡🔴🔴│
BNB  │🟢🟢🟢🟡🟡🟡🟢🟢🟢🟡│
SOL  │🟡🔴🔴🟢🟢🟢🟡🟡🔴🔴│
ADA  │🟢🟢🟡🟡🟡🟢🟢🟢🟡🟡│
     └────────────────────→
      1  5  10 15 20 25 30 (Days)

🟢 Low Volume  🟡 Medium  🔴 High Volume
```

**Visual Features:**
- 🌈 Beautiful gradient from green (low) to red (high)
- 🔍 Easy to spot high-volume days
- 📊 Compare volume across coins and time
- 💎 Professional and modern look

---

### **3️⃣ Weekly Performance Bar Chart** 📊

> **Category:** Weekly Metrics  
> **Why:** Rõ ràng, màu sắc thông minh (xanh/đỏ), dễ so sánh

#### **Configuration**

```yaml
Type: Vertical Bar Chart
Data View: Weekly Crypto Metrics

Y-axis:
  - Aggregation: Average
  - Field: return_pct_week
  - Custom label: "Weekly Return (%)"

X-axis:
  - Aggregation: Terms
  - Field: symbol.keyword
  - Size: 10
  - Order by: Metric (Avg return_pct_week)
  - Order: Descending

Colors:
  - Color by value: Enabled
  - Positive values: Green (#00CC66)
  - Negative values: Red (#FF4444)

Time range: Last 7 days

Panel settings:
  - Show values on bars: true
  - Bar width: 0.7
```

#### **Step-by-Step Instructions**

1. **Create Visualization**
   - Navigate to: `Analytics → Visualize Library → Create visualization`
   - Select: `Vertical bar`
   - Choose data view: `Weekly Crypto Metrics`

2. **Configure Y-axis (Returns)**
   - Click `Add field` under Y-axis
   - Select `return_pct_week`
   - Aggregation: `Average`
   - Custom label: `Weekly Return (%)`

3. **Configure X-axis (Coins)**
   - Click `Add field` under X-axis
   - Select `symbol.keyword`
   - Aggregation: `Terms`
   - Size: `10`
   - Order by: `Metric: Average return_pct_week`
   - Order: `Descending`

4. **Configure Colors (Green/Red)**
   - Click `Settings` icon
   - Enable `Color by value`
   - Add rule: `value >= 0` → Color: `#00CC66` (Green)
   - Add rule: `value < 0` → Color: `#FF4444` (Red)

5. **Panel Settings**
   - Show values on bars: `On`
   - Bar width: `0.7`

6. **Set Time Range**
   - Top right: `Last 7 days`

7. **Save**
   - Title: `Weekly Performance`
   - Description: `Weekly return percentage for top 10 coins`
   - Tags: `weekly`, `performance`, `returns`

#### **Expected Result**

```
Return %
   15│     ██
   10│     ██  ██
    5│ ██  ██  ██  ██
    0├─────────────────────
   -5│             ██  ██
  -10│                 ██
     └─────────────────────
      BTC ETH BNB SOL ADA XRP DOT MATIC LINK UNI
      🟢  🟢  🟢  🟢  🔴  🟢  🔴  🔴   🟢   🔴

Green = Gainers | Red = Losers
```

**Visual Features:**
- 🎯 Clear visual distinction between gainers and losers
- 📊 Easy comparison across coins
- 💚❤️ Intuitive color coding (green=good, red=bad)
- 📈 Shows exact percentage values on bars

---

### **4️⃣ Monthly Trend Area Chart** 🌊

> **Category:** Monthly Metrics  
> **Why:** Cực kỳ đẹp với area fill, hiển thị xu hướng dài hạn

#### **Configuration**

```yaml
Type: Area Chart
Data View: Monthly Crypto Metrics

Y-axis:
  - Aggregation: Average
  - Field: close_price_month
  - Custom label: "Price (USD)"

X-axis:
  - Aggregation: Date Histogram
  - Field: @timestamp
  - Interval: Monthly (1M)

Breakdown:
  - Field: symbol.keyword
  - Size: 3
  - Include: BTC, ETH, BNB

Time range: Last 12 months

Panel settings:
  - Stacked: false
  - Fill opacity: 0.3
  - Line width: 2
  - Show points: false
  - Legend position: Right
```

#### **Step-by-Step Instructions**

1. **Create Visualization**
   - Navigate to: `Analytics → Visualize Library → Create visualization`
   - Select: `Area`
   - Choose data view: `Monthly Crypto Metrics`

2. **Configure Y-axis (Price)**
   - Click `Add field` under Y-axis
   - Select `close_price_month`
   - Aggregation: `Average`
   - Custom label: `Price (USD)`

3. **Configure X-axis (Time)**
   - Click `Add field` under X-axis
   - Select `@timestamp`
   - Aggregation: `Date Histogram`
   - Interval: `1 month` (1M)

4. **Add Breakdown (Multiple Areas)**
   - Click `Add` under `Breakdown`
   - Field: `symbol.keyword`
   - Size: `3`
   - Advanced → Include patterns: `BTC|ETH|BNB` (regex)

5. **Configure Area Settings**
   - Click `Settings` icon
   - Stacked: `Off` (overlapping areas)
   - Fill opacity: `0.3` (30% transparent)
   - Line width: `2`
   - Show points: `Off`

6. **Set Time Range**
   - Top right: `Last 12 months`

7. **Save**
   - Title: `Monthly Trend`
   - Description: `Monthly price trends for top 3 cryptocurrencies`
   - Tags: `monthly`, `trend`, `long-term`

#### **Expected Result**

```
Price (USD)
    │        ╱╲╱╲
60K │      ╱▓▓▓▓▓▓╲     ← BTC (Blue area)
    │    ╱▓▓▓▓▓▓▓▓▓▓╲
40K │  ╱▓▓▓▓▓▓▓▓▓▓▓▓▓▓╲
    │╱▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒╲ ← ETH (Green area)
20K │░░░░░░░░░░░░░░░░░░░░ ← BNB (Orange area)
    └────────────────────────→
    Jan  Mar  May  Jul  Sep  Nov

▓ = BTC  ▒ = ETH  ░ = BNB
```

**Visual Features:**
- 🌊 Smooth, flowing area charts
- 🎨 Semi-transparent fills create beautiful overlays
- 📊 Long-term trend visualization
- 💎 Premium, professional appearance

---

## 🎨 **STEP 3: Create Dashboard**

### **3.1. Create New Dashboard**

**Navigation:** Analytics → Dashboard → Create dashboard

**Dashboard Name:** `Crypto Analytics - Top 4 Charts`

### **3.2. Dashboard Layout**

```
┌─────────────────────────────────────────────────────────────┐
│  📊 CRYPTO ANALYTICS DASHBOARD                              │
│  [Time Range: Last 30 days] [Auto-refresh: 5 min]          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  📈 MULTI-COIN PRICE TREND (Last 30 Days)                  │
│  (Line Chart - Full Width, Height: 300px)                  │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  🔥 VOLUME HEATMAP                                         │
│  (Heat Map - Full Width, Height: 400px)                    │
│                                                             │
├──────────────────────────┬──────────────────────────────────┤
│  📊 WEEKLY PERFORMANCE   │  🌊 MONTHLY TREND                │
│  (Bar Chart - 1/2 width) │  (Area Chart - 1/2 width)        │
│  Height: 350px           │  Height: 350px                   │
│                          │                                  │
└──────────────────────────┴──────────────────────────────────┘
```

### **3.3. Add Visualizations to Dashboard**

1. **Add First Panel (Line Chart)**
   - Click `Add panel` → `Add from library`
   - Select `Multi-Coin Price Trend`
   - Resize: Full width, Height ~300px
   - Position: Top

2. **Add Second Panel (Heatmap)**
   - Click `Add panel` → `Add from library`
   - Select `Volume Heatmap`
   - Resize: Full width, Height ~400px
   - Position: Below line chart

3. **Add Third Panel (Bar Chart)**
   - Click `Add panel` → `Add from library`
   - Select `Weekly Performance`
   - Resize: 50% width, Height ~350px
   - Position: Bottom left

4. **Add Fourth Panel (Area Chart)**
   - Click `Add panel` → `Add from library`
   - Select `Monthly Trend`
   - Resize: 50% width, Height ~350px
   - Position: Bottom right

### **3.4. Dashboard Settings**

```yaml
Time range: Last 30 days
Auto-refresh: 5 minutes
Description: Top 4 most beautiful and informative crypto visualizations
Tags: crypto, analytics, dashboard, top-charts

Options:
  - Use margins between panels: true
  - Show panel titles: true
  - Sync color palettes: true
  - Hide filter bar: false
```

### **3.5. Save Dashboard**

1. Click `Save` in top right
2. Title: `Crypto Analytics - Top 4 Charts`
3. Description: `Daily, Weekly, and Monthly crypto market analytics`
4. Store time with dashboard: `Yes`
5. Click `Save`

---

## 🎯 **STEP 4: Quick Start Guide**

### **Complete Setup Checklist**

#### **Setup (5 minutes)**
- [ ] Access Kibana at `http://localhost:5601`
- [ ] Create 3 index patterns (daily, weekly, monthly)
- [ ] Verify data exists in Elasticsearch
- [ ] Refresh field lists

#### **Visualizations (20 minutes)**
- [ ] Create Line Chart: Multi-Coin Price Trend (5 min)
- [ ] Create Heatmap: Volume Heatmap (5 min)
- [ ] Create Bar Chart: Weekly Performance (5 min)
- [ ] Create Area Chart: Monthly Trend (5 min)

#### **Dashboard (5 minutes)**
- [ ] Create new dashboard
- [ ] Add all 4 visualizations
- [ ] Arrange layout as shown above
- [ ] Set time range to Last 30 days
- [ ] Enable auto-refresh (5 min)
- [ ] Save dashboard

#### **Testing (5 minutes)**
- [ ] Verify all charts display data
- [ ] Test time range filters
- [ ] Test auto-refresh
- [ ] Check colors and formatting

**Total Time: ~35 minutes**

---

## 🔧 **STEP 5: Troubleshooting**

### **Issue 1: No data in visualizations**

**Symptoms:**
- Charts show "No results found"
- Empty visualizations

**Solutions:**
```bash
# 1. Check if indices exist
curl http://localhost:9200/_cat/indices?v | grep metrics

# 2. Verify data count
curl http://localhost:9200/daily_metrics/_count
curl http://localhost:9200/weekly_metrics/_count
curl http://localhost:9200/monthly_metrics/_count

# 3. Check sample document
curl http://localhost:9200/daily_metrics/_search?size=1&pretty

# 4. In Kibana:
#    - Check time range (expand to "Last 90 days")
#    - Refresh index pattern field list
#    - Verify @timestamp field exists
```

---

### **Issue 2: Wrong colors in heatmap**

**Symptoms:**
- Colors don't match volume levels
- All cells same color

**Solutions:**
1. Check color palette settings:
   - Settings → Color palette → `Green to Red`
   - Reverse colors: `Off`
   - Number of steps: `5`

2. Verify value aggregation:
   - Should be `Sum` not `Average`
   - Field: `volume_sum_day`

3. Check data range:
   - If all values similar, colors will look same
   - Try different time range

---

### **Issue 3: Bar chart not showing colors**

**Symptoms:**
- All bars are same color
- No green/red distinction

**Solutions:**
1. Enable "Color by value":
   - Settings → Color by value: `On`

2. Add color rules:
   ```
   Rule 1: value >= 0 → #00CC66 (Green)
   Rule 2: value < 0 → #FF4444 (Red)
   ```

3. Verify field has positive and negative values:
   - Check `return_pct_week` data
   - Should have both gains and losses

---

### **Issue 4: Area chart looks messy**

**Symptoms:**
- Too many overlapping areas
- Can't distinguish coins

**Solutions:**
1. Reduce number of coins:
   - Breakdown size: `3` (not 5 or 10)
   - Include only: `BTC|ETH|BNB`

2. Adjust transparency:
   - Fill opacity: `0.3` (30%)
   - Too high = can't see overlap
   - Too low = hard to see areas

3. Disable stacking:
   - Stacked: `Off`
   - This allows areas to overlap

---

### **Issue 5: Slow dashboard loading**

**Symptoms:**
- Dashboard takes >10 seconds to load
- Browser becomes unresponsive

**Solutions:**
1. Reduce time ranges:
   - Daily charts: Last 30 days (not 90)
   - Weekly chart: Last 7 days
   - Monthly chart: Last 12 months

2. Limit data points:
   - Heatmap: Max 10 coins
   - Line chart: Max 5 coins
   - Bar chart: Max 10 coins

3. Optimize Elasticsearch:
   ```bash
   # Increase heap size (in docker-compose.yml)
   ES_JAVA_OPTS: "-Xms2g -Xmx2g"
   ```

4. Use filters:
   - Add filter for top market cap coins only
   - Exclude low-volume coins

---

## 💡 **STEP 6: Tips & Best Practices**

### **Color Guidelines**

```yaml
Price Trends (Line/Area):
  - BTC: Blue (#0066CC)
  - ETH: Green (#00CC66)
  - BNB: Orange (#FF9900)
  - SOL: Purple (#9966FF)
  - ADA: Cyan (#00CCCC)

Performance (Bar):
  - Positive: Green (#00CC66)
  - Negative: Red (#FF4444)
  - Neutral: Gray (#999999)

Heatmap:
  - Low: Green (#00CC66)
  - Medium: Yellow (#FFCC00)
  - High: Red (#FF4444)
```

### **Time Range Recommendations**

| Chart Type | Recommended Range | Why |
|------------|------------------|-----|
| Line Chart (Daily) | Last 30 days | Shows monthly trends clearly |
| Heatmap (Daily) | Last 30 days | 30 columns fit well on screen |
| Bar Chart (Weekly) | Last 7 days | Current week performance |
| Area Chart (Monthly) | Last 12 months | Full year trend |

### **Performance Optimization**

1. **Use appropriate intervals:**
   - Daily data: 1 day interval
   - Weekly data: 1 week interval
   - Monthly data: 1 month interval

2. **Limit breakdown size:**
   - Line chart: 5 coins max
   - Heatmap: 10 coins max
   - Bar chart: 10 coins max
   - Area chart: 3 coins max

3. **Enable auto-refresh wisely:**
   - 5 minutes for production
   - 1 minute for development
   - Disable for historical analysis

### **Visual Design Tips**

1. **Consistent spacing:**
   - Use margins between panels
   - Align panel edges
   - Keep consistent heights for side-by-side panels

2. **Clear titles:**
   - Use descriptive names
   - Include time range in title if relevant
   - Add emoji for visual appeal 📊📈🔥

3. **Legend placement:**
   - Right side for line/area charts
   - Bottom for bar charts
   - Auto for heatmaps

---

## 📚 **STEP 7: Sample Queries & Filters**

### **Useful KQL Queries**

#### **Filter for large-cap coins only**
```
market_cap_close > 10000000000
```

#### **Filter for high-volume coins**
```
volume_sum_day > 1000000000
```

#### **Filter for volatile coins**
```
volatility_day > 15
```

#### **Filter for specific coins**
```
symbol.keyword: (BTC OR ETH OR BNB)
```

#### **Filter for gainers only**
```
return_pct_day > 0
```

#### **Filter for losers only**
```
return_pct_day < 0
```

### **Dashboard-Level Filters**

Add these as global filters to affect all visualizations:

1. **Top 20 coins by market cap:**
   ```
   Add filter → market_cap_close → is between → 1000000000 and 999999999999
   ```

2. **Exclude stablecoins:**
   ```
   Add filter → symbol.keyword → is not one of → USDT, USDC, BUSD, DAI
   ```

3. **Last 30 days only:**
   ```
   Add filter → @timestamp → is between → now-30d and now
   ```

---

## 🎓 **STEP 8: Next Steps**

### **Immediate Actions**
1. ✅ Complete the 35-minute setup
2. ✅ Verify all 4 charts display correctly
3. ✅ Save dashboard and share URL with team
4. ✅ Set up auto-refresh for live monitoring

### **Advanced Enhancements**
1. 🔔 Set up alerts for extreme volatility
2. 📧 Configure email reports (daily/weekly)
3. 🔗 Create dashboard links for specific coins
4. 📱 Enable mobile-friendly view

### **Learning Resources**
- Kibana Lens Documentation: https://www.elastic.co/guide/en/kibana/current/lens.html
- Kibana Dashboard Best Practices: https://www.elastic.co/guide/en/kibana/current/dashboard.html
- Elasticsearch Aggregations: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html

---

## 🎉 **Summary**

### **What You've Built**

✅ **4 Beautiful Visualizations:**
1. 📈 Multi-Coin Price Trend (Line) - Daily price movements
2. 🔥 Volume Heatmap (Heat Map) - Trading volume patterns
3. 📊 Weekly Performance (Bar) - Week-over-week returns
4. 🌊 Monthly Trend (Area) - Long-term price trends

✅ **1 Professional Dashboard:**
- Clean layout with 4 panels
- Auto-refresh every 5 minutes
- Time range controls
- Beautiful color schemes

✅ **Total Setup Time:** ~35 minutes

### **Key Takeaways**

💡 **Elasticsearch + Kibana** = Powerful analytics platform  
💡 **4 chart types** cover all analysis needs  
💡 **Color coding** makes data instantly understandable  
💡 **Proper time ranges** ensure fast performance  

---

## 🙏 **Credits**

**Created for:** Crypto Analytics Pipeline  
**Data Source:** Batch processing (Spark → Elasticsearch)  
**Visualization Tool:** Kibana 8.x  
**Last Updated:** 2026-01-13  

---

**Happy Visualizing!** 📊✨🚀
