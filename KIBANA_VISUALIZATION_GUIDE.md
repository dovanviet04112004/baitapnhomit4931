# Kibana Visualization Guide for Batch Processing Data

## 📊 **Overview**

Hướng dẫn tạo dashboards và visualizations trong Kibana để phân tích dữ liệu crypto từ batch processing pipeline.

**Data Sources:**
- `daily_metrics`: Daily aggregated metrics (365 records/year/coin)
- `weekly_metrics`: Weekly aggregated metrics (52 records/year/coin)
- `monthly_metrics`: Monthly aggregated metrics (12 records/year/coin)

---

## 🚀 **STEP 1: Setup Index Patterns**

### **1.1. Access Kibana**

```
URL: http://localhost:5601
```

### **1.2. Create Index Patterns**

**Navigation:** Stack Management → Data Views → Create data view

#### **Index Pattern 1: Daily Metrics**

```
Name: Daily Crypto Metrics
Index pattern: daily_metrics
Timestamp field: @timestamp
```

**Fields available:**
- `coin_id` (keyword)
- `symbol` (keyword)
- `name` (text)
- `date` (date)
- `open_price`, `close_price`, `high_price`, `low_price` (double)
- `return_pct_day`, `volatility_day` (double)
- `volume_sum_day` (long)
- `market_cap_close` (long)
- `@timestamp` (date)

#### **Index Pattern 2: Weekly Metrics**

```
Name: Weekly Crypto Metrics
Index pattern: weekly_metrics
Timestamp field: @timestamp
```

**Fields available:**
- `coin_id`, `symbol`, `name`
- `week_of_year` (integer)
- `week_start_date`, `week_end_date` (date)
- `open_price_week`, `close_price_week`, `high_price_week`, `low_price_week`
- `return_pct_week`, `volatility_week`
- `volume_sum_week`

#### **Index Pattern 3: Monthly Metrics**

```
Name: Monthly Crypto Metrics
Index pattern: monthly_metrics
Timestamp field: @timestamp
```

---

## 📈 **STEP 2: Create Visualizations**

### **Dashboard 1: Market Overview** 🌍

#### **Visualization 1: Total Market Cap (Metric)**

**Type:** Metric

**Configuration:**
```
Data View: Daily Crypto Metrics
Metric:
  - Aggregation: Max
  - Field: market_cap_close
  - Custom label: "Total Market Cap"
  
Filters:
  - @timestamp: Last 24 hours
  
Format:
  - Number format: $0,0.00
  - Font size: 48px
```

**Steps:**
1. Analytics → Visualize Library → Create visualization
2. Select "Metric"
3. Choose "Daily Crypto Metrics" data view
4. Click "Add field" → Select "market_cap_close"
5. Change aggregation to "Max"
6. Add filter: `@timestamp >= now-1d`
7. Save as "Total Market Cap"

---

#### **Visualization 2: Top 10 Gainers (Table)**

**Type:** Table

**Configuration:**
```
Data View: Daily Crypto Metrics

Rows:
  - Field: symbol.keyword
  - Size: 10
  - Order by: Custom metric (return_pct_day)
  - Order: Descending

Metrics:
  1. Max return_pct_day (Custom label: "Return %")
  2. Max close_price (Custom label: "Price")
  3. Max volume_sum_day (Custom label: "Volume")
  4. Max market_cap_close (Custom label: "Market Cap")

Filters:
  - return_pct_day > 0
  - @timestamp: Last 24 hours
```

**Steps:**
1. Create visualization → Table
2. Add "Rows" bucket:
   - Aggregation: Terms
   - Field: symbol.keyword
   - Size: 10
   - Order by: Metric (return_pct_day)
   - Descending
3. Add metrics:
   - Click "Add metric"
   - Aggregation: Max
   - Field: return_pct_day
   - Repeat for other fields
4. Add filters in top bar
5. Save as "Top 10 Gainers"

---

#### **Visualization 3: Top 10 Losers (Table)**

**Same as Top 10 Gainers but:**
```
Filters:
  - return_pct_day < 0
  
Order: Ascending (most negative first)
```

---

### **Dashboard 2: Price Trends** 📉

#### **Visualization 4: Multi-Coin Price Trend (Line Chart)**

**Type:** Line

**Configuration:**
```
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
```

**Steps:**
1. Create visualization → Line
2. Y-axis: Average close_price
3. X-axis: Date Histogram (@timestamp, daily)
4. Add "Breakdown":
   - Field: symbol.keyword
   - Size: 5
   - Advanced → Include: "BTC|ETH|BNB|SOL|ADA" (regex)
5. Set time range to "Last 30 days"
6. Save as "Multi-Coin Price Trend"

---

#### **Visualization 5: Volume Heatmap**

**Type:** Heat map

**Configuration:**
```
Data View: Daily Crypto Metrics

Value:
  - Aggregation: Sum
  - Field: volume_sum_day

X-axis:
  - Aggregation: Date Histogram
  - Field: @timestamp
  - Interval: Daily

Y-axis:
  - Aggregation: Terms
  - Field: symbol.keyword
  - Size: 10
  - Order by: Metric (Sum volume_sum_day)

Color scale:
  - Palette: Green to Red
  - Steps: 5
```

**Steps:**
1. Create visualization → Heat map
2. Configure axes as above
3. Panel settings → Color palette: "Green to Red"
4. Save as "Volume Heatmap"

---

### **Dashboard 3: Volatility Analysis** 📊

#### **Visualization 6: Volatility Gauge**

**Type:** Gauge

**Configuration:**
```
Data View: Daily Crypto Metrics

Metric:
  - Aggregation: Average
  - Field: volatility_day

Ranges:
  - 0-5: Low (Green)
  - 5-15: Medium (Yellow)
  - 15-100: High (Red)

Filters:
  - symbol.keyword: "BTC"
  - @timestamp: Last 24 hours
```

**Steps:**
1. Create visualization → Gauge
2. Metric: Average volatility_day
3. Panel settings → Ranges:
   - Add range: 0 to 5 (Green)
   - Add range: 5 to 15 (Yellow)
   - Add range: 15 to 100 (Red)
4. Add filter: symbol.keyword = "BTC"
5. Save as "BTC Volatility Gauge"

---

#### **Visualization 7: Return vs Volatility Scatter**

**Type:** Lens XY Chart**

**Configuration:**
```
Data View: Daily Crypto Metrics

X-axis:
  - Field: volatility_day
  - Aggregation: Average

Y-axis:
  - Field: return_pct_day
  - Aggregation: Average

Breakdown:
  - Field: symbol.keyword
  - Size: 20

Time range: Last 7 days
```

---

### **Dashboard 4: Market Distribution** 🥧

#### **Visualization 8: Market Cap Pie Chart**

**Type:** Pie

**Configuration:**
```
Data View: Daily Crypto Metrics

Slice by:
  - Aggregation: Terms
  - Field: symbol.keyword
  - Size: 10
  - Order by: Metric (Avg market_cap_close)

Metric:
  - Aggregation: Average
  - Field: market_cap_close

Filters:
  - @timestamp: Last 24 hours

Panel settings:
  - Donut: true
  - Show labels: true
  - Show values: true
```

**Steps:**
1. Create visualization → Pie
2. Slice by: Terms symbol.keyword (size 10)
3. Metric: Average market_cap_close
4. Panel settings → Donut
5. Save as "Market Cap Distribution"

---

### **Dashboard 5: Weekly/Monthly Trends** 📅

#### **Visualization 9: Weekly Performance Bar Chart**

**Type:** Vertical bar

**Configuration:**
```
Data View: Weekly Crypto Metrics

Y-axis:
  - Aggregation: Average
  - Field: return_pct_week
  - Custom label: "Weekly Return %"

X-axis:
  - Aggregation: Terms
  - Field: symbol.keyword
  - Size: 10
  - Order by: Metric (Avg return_pct_week)

Filters:
  - @timestamp: Last 7 days

Colors:
  - Positive: Green
  - Negative: Red
```

---

#### **Visualization 10: Monthly Trend Area Chart**

**Type:** Area

**Configuration:**
```
Data View: Monthly Crypto Metrics

Y-axis:
  - Aggregation: Average
  - Field: close_price_month

X-axis:
  - Aggregation: Date Histogram
  - Field: @timestamp
  - Interval: Monthly

Breakdown:
  - Field: symbol.keyword
  - Size: 5
  - Include: BTC, ETH, BNB

Time range: Last 12 months

Panel settings:
  - Stacked: false
  - Fill opacity: 0.3
```

---

## 🎨 **STEP 3: Create Dashboard**

### **3.1. Create New Dashboard**

**Navigation:** Analytics → Dashboard → Create dashboard

**Dashboard Name:** Crypto Market Analytics

### **3.2. Dashboard Layout**

```
┌─────────────────────────────────────────────────────────────┐
│  📊 CRYPTO MARKET ANALYTICS                                 │
│  [Time Range: Last 30 days] [Auto-refresh: 5 min]          │
├─────────────────────────────────────────────────────────────┤
│  [Total Market Cap]  [Avg Volatility]  [Total Volume]      │
│  (Metrics - 1/3 width each)                                 │
├──────────────────────────┬──────────────────────────────────┤
│  🏆 Top 10 Gainers       │  📉 Top 10 Losers                │
│  (Table - 1/2 width)     │  (Table - 1/2 width)             │
├──────────────────────────┴──────────────────────────────────┤
│  📈 Multi-Coin Price Trend (Last 30 Days)                   │
│  (Line Chart - Full width)                                  │
├──────────────────────────┬──────────────────────────────────┤
│  🔥 Volume Heatmap       │  🥧 Market Cap Distribution      │
│  (Heat Map - 1/2 width)  │  (Pie Chart - 1/2 width)         │
├──────────────────────────┴──────────────────────────────────┤
│  📊 Return vs Volatility Scatter                            │
│  (XY Chart - Full width)                                    │
└─────────────────────────────────────────────────────────────┘
```

### **3.3. Add Visualizations**

1. Click "Add panel"
2. Select "Add from library"
3. Choose visualizations created above
4. Resize and arrange panels
5. Save dashboard

### **3.4. Dashboard Settings**

```
Time range: Last 30 days
Auto-refresh: 5 minutes
Description: Real-time crypto market analytics from batch processing
Tags: crypto, batch, analytics
```

---

## 🔍 **STEP 4: Advanced Features**

### **4.1. Saved Searches**

#### **Search 1: High Volatility Coins**

```
Index: daily_metrics
Query: volatility_day > 15 AND @timestamp >= now-7d
Sort: volatility_day DESC
Columns: symbol, name, volatility_day, return_pct_day, close_price
```

**Steps:**
1. Discover → Select "Daily Crypto Metrics"
2. Add filter: `volatility_day > 15`
3. Add time filter: Last 7 days
4. Select columns to display
5. Save search as "High Volatility Coins"

---

#### **Search 2: Today's Big Movers**

```
Index: daily_metrics
Query: ABS(return_pct_day) > 5 AND @timestamp >= now-1d
Sort: ABS(return_pct_day) DESC
```

---

### **4.2. Alerts (Watcher)**

#### **Alert 1: Extreme Volatility**

```
Trigger: volatility_day > 20
Condition: Any coin
Frequency: Every 1 hour
Action: Send notification
```

**Configuration:**
```json
{
  "trigger": {
    "schedule": {
      "interval": "1h"
    }
  },
  "input": {
    "search": {
      "request": {
        "indices": ["daily_metrics"],
        "body": {
          "query": {
            "bool": {
              "must": [
                {
                  "range": {
                    "volatility_day": {
                      "gt": 20
                    }
                  }
                },
                {
                  "range": {
                    "@timestamp": {
                      "gte": "now-1h"
                    }
                  }
                }
              ]
            }
          }
        }
      }
    }
  },
  "condition": {
    "compare": {
      "ctx.payload.hits.total": {
        "gt": 0
      }
    }
  },
  "actions": {
    "log_alert": {
      "logging": {
        "text": "High volatility detected: {{ctx.payload.hits.total}} coins"
      }
    }
  }
}
```

---

#### **Alert 2: Price Spike**

```
Trigger: ABS(return_pct_day) > 10
Condition: Any coin
Frequency: Real-time
Action: Send email
```

---

### **4.3. Custom Filters**

#### **Filter 1: Top Market Cap Coins**

```
Query: market_cap_close > 10000000000
Name: "Large Cap Coins"
```

#### **Filter 2: High Volume**

```
Query: volume_sum_day > 1000000000
Name: "High Volume Coins"
```

---

## 📊 **STEP 5: Sample Queries**

### **Discover Tab Queries**

#### **1. Find coins with consistent growth**

```
KQL: return_pct_day > 0 AND volatility_day < 10 AND @timestamp >= now-7d
```

#### **2. Identify pump and dump patterns**

```
KQL: return_pct_day > 20 OR return_pct_day < -20
```

#### **3. Bitcoin performance analysis**

```
KQL: symbol.keyword: "BTC" AND @timestamp >= now-30d
```

#### **4. Low volatility stable coins**

```
KQL: volatility_day < 5 AND @timestamp >= now-7d
```

---

## 🎯 **STEP 6: Best Practices**

### **6.1. Performance Optimization**

1. **Use appropriate time ranges**
   - Daily analysis: Last 7 days
   - Weekly analysis: Last 3 months
   - Monthly analysis: Last 12 months

2. **Limit data points**
   - Tables: Max 20 rows
   - Charts: Max 10 series
   - Heatmaps: Max 50 cells

3. **Use filters effectively**
   - Filter at dashboard level for global filters
   - Filter at visualization level for specific needs

### **6.2. Visual Design**

1. **Color Consistency**
   - Green: Positive returns
   - Red: Negative returns
   - Blue: Neutral metrics
   - Yellow: Warnings

2. **Chart Selection**
   - Time series: Line charts
   - Comparisons: Bar charts
   - Distributions: Pie charts
   - Correlations: Scatter plots

3. **Layout**
   - Most important metrics at top
   - Related visualizations grouped together
   - Consistent sizing and spacing

---

## ✅ **Quick Start Checklist**

### **Setup**
- [ ] Create 3 index patterns (daily, weekly, monthly)
- [ ] Verify data is indexed in Elasticsearch
- [ ] Check field mappings are correct

### **Visualizations**
- [ ] Create Total Market Cap metric
- [ ] Create Top Gainers/Losers tables
- [ ] Create Price Trend line chart
- [ ] Create Volume heatmap
- [ ] Create Market Cap pie chart
- [ ] Create Volatility gauge
- [ ] Create Return vs Volatility scatter
- [ ] Create Weekly/Monthly trend charts

### **Dashboard**
- [ ] Create main dashboard
- [ ] Add all visualizations
- [ ] Arrange layout
- [ ] Set time range (Last 30 days)
- [ ] Enable auto-refresh (5 min)
- [ ] Add filters
- [ ] Save dashboard

### **Advanced**
- [ ] Create saved searches
- [ ] Set up alerts
- [ ] Create custom filters
- [ ] Test queries

---

## 🔧 **Troubleshooting**

### **Issue 1: No data in visualizations**

**Solution:**
```
1. Check Elasticsearch indices:
   curl http://localhost:9200/_cat/indices?v

2. Verify data exists:
   curl http://localhost:9200/daily_metrics/_count

3. Check time range in Kibana
4. Refresh index pattern fields
```

### **Issue 2: Slow dashboard loading**

**Solution:**
```
1. Reduce time range
2. Limit number of visualizations
3. Use filters to reduce data
4. Increase Elasticsearch heap size
```

### **Issue 3: Field not found**

**Solution:**
```
1. Refresh index pattern:
   Stack Management → Index Patterns → Refresh field list

2. Check field mapping:
   curl http://localhost:9200/daily_metrics/_mapping
```

---

## 📚 **Additional Resources**

### **Kibana Documentation**
- Visualizations: https://www.elastic.co/guide/en/kibana/current/dashboard.html
- Lens: https://www.elastic.co/guide/en/kibana/current/lens.html
- Alerts: https://www.elastic.co/guide/en/kibana/current/alerting-getting-started.html

### **Sample Dashboards**
- Financial Analytics: https://www.elastic.co/kibana/kibana-dashboard-gallery
- Time Series Analysis: https://www.elastic.co/blog/

---

## 🎓 **Next Steps**

1. ✅ Complete setup checklist
2. ✅ Create all visualizations
3. ✅ Build main dashboard
4. ✅ Set up alerts
5. ✅ Share dashboard with team
6. ✅ Schedule regular reviews

**Happy Visualizing!** 📊✨
