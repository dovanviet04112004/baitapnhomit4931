# Plan: Web App Visualization cho Batch Data

## 🎯 Mục tiêu
Xây dựng web application để visualize dữ liệu batch từ PostgreSQL với giao diện đẹp, hiện đại và interactive.

---

## 📋 Phase 1: Setup & Infrastructure (Ngày 1)

### 1.1 Backend API Setup
- [ ] Tạo REST API với Node.js/Express hoặc Python/FastAPI
- [ ] Kết nối PostgreSQL database
- [ ] Tạo các endpoints cơ bản:
  - `GET /api/daily-metrics` - Lấy dữ liệu daily
  - `GET /api/weekly-metrics` - Lấy dữ liệu weekly
  - `GET /api/monthly-metrics` - Lấy dữ liệu monthly
  - `GET /api/coins` - Danh sách coins
  - `GET /api/top-movers` - Top movers theo timeframe
  - `GET /api/coin/:id/history` - Lịch sử giá của 1 coin

### 1.2 Frontend Setup
- [ ] Setup Vite + React (hoặc Next.js nếu cần SSR)
- [ ] Install dependencies:
  - Chart.js hoặc Recharts (cho charts)
  - TailwindCSS (styling)
  - Axios (API calls)
  - React Query (data fetching & caching)
  - Date-fns (date manipulation)

### 1.3 Database Query Optimization
- [ ] Tạo indexes cho các trường thường query:
  - `date`, `coin_id`, `symbol`
  - `return_pct_day`, `volatility_day`
- [ ] Tạo materialized views nếu cần (cho top movers, etc.)

---

## 🎨 Phase 2: Core Features (Ngày 2-3)

### 2.1 Dashboard Overview
**Components cần tạo:**
- [ ] **Market Overview Card**
  - Tổng số coins đang track
  - Tổng market cap
  - Average return 24h
  - Số coins tăng/giảm

- [ ] **Top Movers Table**
  - Top 10 gainers (return_pct_day DESC)
  - Top 10 losers (return_pct_day ASC)
  - Hiển thị: symbol, name, price, return%, volume
  - Color coding: xanh (tăng), đỏ (giảm)

- [ ] **Most Volatile Coins**
  - Top 10 coins theo volatility_day
  - Sparkline chart cho mỗi coin

### 2.2 Price Charts
**Components cần tạo:**
- [ ] **Candlestick Chart**
  - Input: coin_id, timeframe (daily/weekly/monthly)
  - Hiển thị: open, close, high, low
  - Interactive: zoom, pan, tooltip

- [ ] **Line Chart - Price History**
  - Multiple coins comparison
  - Toggle giữa close_price, volume
  - Date range selector

- [ ] **Area Chart - Market Cap**
  - Market cap evolution over time
  - Stacked area cho top coins

### 2.3 Coin Detail Page
**URL**: `/coin/:symbol`

**Components:**
- [ ] **Coin Header**
  - Name, symbol, current price
  - 24h change, 7d change, 30d change
  - Market cap, rank

- [ ] **Price Chart**
  - Candlestick + Volume
  - Timeframe selector: 7D, 1M, 3M, 6M, 1Y, ALL

- [ ] **Statistics Cards**
  - High/Low (24h, 7d, 30d)
  - Average volatility
  - Total volume
  - Rank changes

- [ ] **Performance Table**
  - Daily returns table
  - Sortable columns
  - Pagination

---

## 📊 Phase 3: Advanced Visualizations (Ngày 4-5)

### 3.1 Heatmap
- [ ] **Daily Returns Heatmap**
  - Rows: Coins
  - Columns: Dates
  - Color: return_pct_day (red to green gradient)
  - Interactive tooltip

### 3.2 Scatter Plot
- [ ] **Risk vs Return**
  - X-axis: volatility_day (risk)
  - Y-axis: return_pct_day (return)
  - Size: volume_sum_day
  - Color: coin category
  - Quadrant lines (high return/low risk, etc.)

### 3.3 Ranking Evolution
- [ ] **Rank Change Chart**
  - Line chart showing rank_close over time
  - Multiple coins comparison
  - Inverted Y-axis (rank 1 at top)

### 3.4 Volume Analysis
- [ ] **Volume Bar Chart**
  - Daily volume comparison
  - Overlay with price line
  - Highlight volume spikes

---

## 🎯 Phase 4: Filters & Interactions (Ngày 6)

### 4.1 Global Filters
- [ ] **Date Range Picker**
  - Presets: Today, 7D, 1M, 3M, 6M, 1Y, Custom
  - Apply to all charts

- [ ] **Coin Selector**
  - Multi-select dropdown
  - Search functionality
  - Select all/clear all

- [ ] **Timeframe Toggle**
  - Daily / Weekly / Monthly
  - Switch between aggregation levels

### 4.2 Interactive Features
- [ ] **Chart Interactions**
  - Hover tooltips
  - Click to drill down
  - Zoom & pan
  - Export chart as image

- [ ] **Table Features**
  - Sort by any column
  - Search/filter
  - Pagination
  - Export to CSV

---

## 🎨 Phase 5: UI/UX Polish (Ngày 7)

### 5.1 Design System
- [ ] **Color Palette**
  - Primary: Deep blue (#1e3a8a)
  - Success: Green (#10b981)
  - Danger: Red (#ef4444)
  - Background: Dark mode (#0f172a, #1e293b)
  - Text: Light gray (#e2e8f0)

- [ ] **Typography**
  - Font: Inter or Roboto
  - Headings: Bold, larger sizes
  - Body: Regular, readable

- [ ] **Components**
  - Cards with glassmorphism
  - Smooth transitions
  - Hover effects
  - Loading skeletons

### 5.2 Responsive Design
- [ ] Mobile layout (< 768px)
- [ ] Tablet layout (768px - 1024px)
- [ ] Desktop layout (> 1024px)

### 5.3 Performance
- [ ] Lazy loading for charts
- [ ] Virtual scrolling for large tables
- [ ] Data caching with React Query
- [ ] Debounce search inputs

---

## 🚀 Phase 6: Deployment & Testing (Ngày 8)

### 6.1 Testing
- [ ] API endpoint testing
- [ ] Chart rendering tests
- [ ] Responsive design testing
- [ ] Performance testing (load time, query speed)

### 6.2 Deployment
- [ ] Backend: Deploy API (Docker, Railway, etc.)
- [ ] Frontend: Deploy to Vercel/Netlify
- [ ] Database: Ensure PostgreSQL is accessible
- [ ] Environment variables setup

### 6.3 Documentation
- [ ] API documentation
- [ ] User guide
- [ ] Developer setup guide

---

## 🛠️ Tech Stack Đề xuất

### Backend
- **Framework**: FastAPI (Python) hoặc Express (Node.js)
- **Database**: PostgreSQL
- **ORM**: SQLAlchemy (Python) hoặc Prisma (Node.js)
- **API Docs**: Swagger/OpenAPI

### Frontend
- **Framework**: Vite + React
- **Styling**: TailwindCSS + Custom CSS
- **Charts**: Recharts hoặc Chart.js
- **State Management**: React Query + Context API
- **Routing**: React Router
- **Date Handling**: date-fns

### DevOps
- **Containerization**: Docker
- **CI/CD**: GitHub Actions
- **Hosting**: 
  - Frontend: Vercel/Netlify
  - Backend: Railway/Render/DigitalOcean

---

## 📁 Cấu trúc Project

```
baitapnhomit4931/
├── backend/
│   ├── app/
│   │   ├── api/
│   │   │   ├── routes/
│   │   │   │   ├── daily.py
│   │   │   │   ├── weekly.py
│   │   │   │   ├── monthly.py
│   │   │   │   └── coins.py
│   │   │   └── deps.py
│   │   ├── models/
│   │   ├── schemas/
│   │   └── main.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   │   ├── charts/
│   │   │   │   ├── CandlestickChart.jsx
│   │   │   │   ├── LineChart.jsx
│   │   │   │   ├── Heatmap.jsx
│   │   │   │   └── ScatterPlot.jsx
│   │   │   ├── tables/
│   │   │   │   ├── TopMoversTable.jsx
│   │   │   │   └── PerformanceTable.jsx
│   │   │   ├── cards/
│   │   │   │   ├── MarketOverview.jsx
│   │   │   │   └── StatCard.jsx
│   │   │   └── filters/
│   │   │       ├── DateRangePicker.jsx
│   │   │       └── CoinSelector.jsx
│   │   ├── pages/
│   │   │   ├── Dashboard.jsx
│   │   │   ├── CoinDetail.jsx
│   │   │   └── Analytics.jsx
│   │   ├── hooks/
│   │   │   ├── useMetrics.js
│   │   │   └── useCoins.js
│   │   ├── services/
│   │   │   └── api.js
│   │   ├── styles/
│   │   │   └── index.css
│   │   ├── App.jsx
│   │   └── main.jsx
│   ├── package.json
│   └── vite.config.js
│
└── docs/
    ├── batch_data_schema.md
    └── web_app_plan.md (this file)
```

---

## 🎯 Priorities

### Must Have (MVP)
1. ✅ Dashboard với top movers
2. ✅ Price charts (candlestick + line)
3. ✅ Coin detail page
4. ✅ Date range filter
5. ✅ Responsive design

### Nice to Have
1. Heatmap visualization
2. Scatter plot (risk vs return)
3. Export functionality
4. Dark/Light mode toggle
5. Real-time updates (WebSocket)

### Future Enhancements
1. User authentication
2. Watchlist/favorites
3. Alerts & notifications
4. Custom dashboards
5. AI-powered insights

---

## 📝 Next Steps

1. **Bắt đầu với Backend API** - Tạo endpoints cơ bản để fetch data
2. **Setup Frontend** - Initialize Vite project với dependencies
3. **Implement Dashboard** - Tạo trang chủ với overview
4. **Add Charts** - Implement các loại charts cơ bản
5. **Polish UI** - Làm đẹp giao diện, thêm animations
6. **Test & Deploy** - Testing và deploy lên production

---

**Bạn muốn bắt đầu từ đâu? Backend API hay Frontend setup?**
