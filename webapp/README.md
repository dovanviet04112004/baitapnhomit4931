# Crypto Analytics Dashboard

Dashboard đơn giản để visualize batch data từ PostgreSQL với 3 chế độ xem: Daily, Weekly, Monthly.

## 🎯 Features

- ✅ **3 Timeframe Views**: Daily, Weekly, Monthly metrics
- ✅ **Summary Cards**: Top Gainer, Top Loser, Most Volatile, Highest Volume
- ✅ **Interactive Charts**: Price performance & Volume analysis
- ✅ **Data Table**: Top 20 movers với filters (All/Gainers/Losers)
- ✅ **Modern UI**: Dark theme, glassmorphism, smooth animations
- ✅ **Responsive**: Hoạt động tốt trên mobile, tablet, desktop

## 📁 Cấu trúc

```
webapp/
├── index.html          # Main HTML file
├── styles.css          # Styling (dark theme)
├── app.js             # JavaScript logic
├── api.py             # Backend API (Python/FastAPI)
└── README.md          # This file
```

## 🚀 Quick Start

### Option 1: Chạy với Mock Data (Nhanh nhất)

1. Mở file `index.html` trực tiếp trong browser:
   ```bash
   # Windows
   start webapp/index.html
   
   # hoặc double-click vào file index.html
   ```

2. Dashboard sẽ hiển thị với mock data ngay lập tức!

### Option 2: Kết nối PostgreSQL (Production)

#### Bước 1: Setup Backend API

1. Install dependencies:
   ```bash
   cd webapp
   pip install fastapi uvicorn psycopg2-binary python-dotenv
   ```

2. Tạo file `.env`:
   ```env
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=crypto_db
   DB_USER=your_user
   DB_PASSWORD=your_password
   ```

3. Chạy API server:
   ```bash
   python api.py
   ```
   
   API sẽ chạy tại: `http://localhost:8000`

#### Bước 2: Update Frontend

1. Mở file `app.js`
2. Thay đổi dòng 3:
   ```javascript
   const USE_MOCK_DATA = false; // Đổi từ true sang false
   ```

3. Mở `index.html` trong browser hoặc dùng live server:
   ```bash
   # Nếu có Python
   python -m http.server 3000
   
   # Hoặc dùng VS Code Live Server extension
   ```

4. Truy cập: `http://localhost:3000`

## 📊 API Endpoints

Backend API cung cấp các endpoints sau:

- `GET /api/daily-metrics` - Lấy daily metrics
- `GET /api/weekly-metrics` - Lấy weekly metrics  
- `GET /api/monthly-metrics` - Lấy monthly metrics
- `GET /health` - Health check

### Response Format

```json
[
  {
    "coin_id": "bitcoin",
    "symbol": "BTC",
    "name": "Bitcoin",
    "open_price": 45000.50,
    "close_price": 46500.25,
    "high_price": 47000.00,
    "low_price": 44800.00,
    "return_pct_day": 3.33,
    "volatility_day": 4.89,
    "volume_sum_day": 25000000000
  },
  ...
]
```

## 🎨 Customization

### Thay đổi màu sắc

Mở `styles.css` và chỉnh sửa CSS variables:

```css
:root {
    --bg-primary: #0f172a;      /* Background chính */
    --color-green: #10b981;     /* Màu tăng */
    --color-red: #ef4444;       /* Màu giảm */
    --color-purple: #8b5cf6;    /* Accent color */
}
```

### Thay đổi số lượng coins hiển thị

Mở `app.js` và tìm dòng:

```javascript
// Line ~250
tbody.innerHTML = filteredData.slice(0, 20).map(...)
                                      // ↑ Thay đổi số này
```

### Thêm coins mới vào mock data

Mở `app.js` và thêm vào array `coins`:

```javascript
// Line ~320
const coins = [
    { id: 'bitcoin', symbol: 'BTC', name: 'Bitcoin' },
    // Thêm coin mới ở đây
    { id: 'your-coin', symbol: 'YC', name: 'Your Coin' },
];
```

## 🔧 Troubleshooting

### CORS Error khi kết nối API

Nếu gặp lỗi CORS, đảm bảo backend API có CORS middleware:

```python
# Đã có sẵn trong api.py
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### Charts không hiển thị

1. Kiểm tra console (F12) xem có lỗi không
2. Đảm bảo Chart.js đã load (kiểm tra network tab)
3. Thử refresh trang (Ctrl + F5)

### Data không load

1. Kiểm tra `USE_MOCK_DATA` trong `app.js`
2. Nếu dùng API, kiểm tra API server đang chạy
3. Kiểm tra database connection trong `.env`

## 📱 Screenshots

### Desktop View
- Dashboard với 4 summary cards
- 2 charts (Price Performance & Volume)
- Table với top 20 movers

### Mobile View
- Responsive layout
- Stacked cards
- Scrollable table

## 🚀 Next Steps

### Tính năng có thể thêm:

1. **Date Range Picker** - Chọn khoảng thời gian
2. **Coin Search** - Tìm kiếm coin cụ thể
3. **Export CSV** - Export data ra file
4. **Real-time Updates** - WebSocket cho live data
5. **Coin Detail Page** - Click vào coin để xem chi tiết
6. **Comparison Tool** - So sánh nhiều coins
7. **Alerts** - Cảnh báo khi giá thay đổi
8. **Dark/Light Mode Toggle** - Chuyển đổi theme

### Performance Optimization:

1. **Pagination** - Phân trang cho table
2. **Virtual Scrolling** - Cho danh sách dài
3. **Data Caching** - Cache API responses
4. **Lazy Loading** - Load charts khi cần

## 📝 Notes

- Mock data được generate random mỗi lần load
- Charts sử dụng Chart.js v4.4.1
- Responsive breakpoint: 768px
- Font: Inter (Google Fonts)

## 🤝 Contributing

Để thêm tính năng mới:

1. Tạo branch mới
2. Implement feature
3. Test trên cả mock data và real API
4. Submit pull request

## 📄 License

MIT License - Free to use and modify
