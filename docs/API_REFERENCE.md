# API Reference

Tài liệu chi tiết về các REST API endpoints của hệ thống Crypto Analytics Pipeline.

---

## Base URL

```
Development: http://localhost:8000
Production:  https://api.crypto-analytics.com
```

---

## Authentication

Hiện tại API không yêu cầu authentication. Các phiên bản sau sẽ hỗ trợ API Key authentication.

```bash
# Future implementation
curl -H "X-API-Key: your-api-key" http://localhost:8000/api/v1/...
```

---

## Endpoints

### 1. Health Check

#### GET /health

Kiểm tra trạng thái của API server.

**Response**
```json
{
  "status": "healthy",
  "timestamp": "2025-12-15T10:30:00Z",
  "version": "1.0.0"
}
```

---

### 2. Crypto Data Endpoints

#### GET /api/v1/crypto/latest

Lấy giá mới nhất của tất cả cryptocurrencies.

**Query Parameters**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| limit | int | 100 | Số lượng coins trả về |
| offset | int | 0 | Vị trí bắt đầu |

**Response**
```json
{
  "data": [
    {
      "id": "bitcoin",
      "symbol": "BTC",
      "name": "Bitcoin",
      "current_price": 98500.00,
      "market_cap": 1950000000000,
      "price_change_24h": 2.5,
      "volume_24h": 45000000000,
      "last_updated": "2025-12-15T10:30:00Z"
    }
  ],
  "total": 98,
  "limit": 100,
  "offset": 0
}
```

---

#### GET /api/v1/crypto/{coin_id}

Lấy thông tin chi tiết của một coin.

**Path Parameters**
| Parameter | Type | Description |
|-----------|------|-------------|
| coin_id | string | ID của coin (vd: bitcoin, ethereum) |

**Response**
```json
{
  "id": "bitcoin",
  "symbol": "BTC",
  "name": "Bitcoin",
  "current_price": 98500.00,
  "market_cap": 1950000000000,
  "market_cap_rank": 1,
  "price_change_24h": 2.5,
  "price_change_7d": 5.2,
  "price_change_30d": 15.8,
  "volume_24h": 45000000000,
  "circulating_supply": 19500000,
  "total_supply": 21000000,
  "ath": 108000.00,
  "ath_date": "2025-11-20T00:00:00Z",
  "last_updated": "2025-12-15T10:30:00Z"
}
```

**Error Response (404)**
```json
{
  "error": "Coin not found",
  "coin_id": "invalid-coin"
}
```

---

#### GET /api/v1/crypto/{coin_id}/history

Lấy lịch sử giá của một coin.

**Path Parameters**
| Parameter | Type | Description |
|-----------|------|-------------|
| coin_id | string | ID của coin |

**Query Parameters**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| days | int | 7 | Số ngày lịch sử (1, 7, 30, 90, 365) |
| interval | string | "daily" | Khoảng thời gian (hourly, daily) |

**Response**
```json
{
  "coin_id": "bitcoin",
  "prices": [
    {
      "timestamp": "2025-12-08T00:00:00Z",
      "price": 95000.00,
      "volume": 42000000000,
      "market_cap": 1850000000000
    }
  ],
  "days": 7,
  "interval": "daily"
}
```

---

### 3. Analytics Endpoints

#### GET /api/v1/analytics/daily

Lấy thống kê daily metrics.

**Query Parameters**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| date | string | today | Ngày cần lấy (YYYY-MM-DD) |
| limit | int | 20 | Số coins trả về |

**Response**
```json
{
  "date": "2025-12-15",
  "metrics": [
    {
      "coin_id": "bitcoin",
      "symbol": "BTC",
      "open": 97000.00,
      "high": 99500.00,
      "low": 96500.00,
      "close": 98500.00,
      "volume": 45000000000,
      "price_change_pct": 1.55
    }
  ],
  "total_market_cap": 3500000000000,
  "btc_dominance": 55.7
}
```

---

#### GET /api/v1/analytics/weekly

Lấy thống kê weekly metrics.

**Response**
```json
{
  "week_start": "2025-12-09",
  "week_end": "2025-12-15",
  "metrics": [
    {
      "coin_id": "bitcoin",
      "avg_price": 97500.00,
      "max_price": 99500.00,
      "min_price": 95000.00,
      "total_volume": 315000000000,
      "volatility": 2.3
    }
  ]
}
```

---

#### GET /api/v1/analytics/monthly

Lấy thống kê monthly metrics.

**Response**
```json
{
  "month": "2025-12",
  "metrics": [
    {
      "coin_id": "bitcoin",
      "avg_price": 96000.00,
      "max_price": 99500.00,
      "min_price": 91000.00,
      "price_change_pct": 8.5,
      "avg_daily_volume": 44000000000
    }
  ]
}
```

---

#### GET /api/v1/analytics/top-movers

Lấy top coins có biến động giá lớn nhất.

**Query Parameters**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| type | string | "gainers" | gainers hoặc losers |
| limit | int | 10 | Số coins trả về |
| timeframe | string | "24h" | 24h, 7d, 30d |

**Response**
```json
{
  "type": "gainers",
  "timeframe": "24h",
  "data": [
    {
      "coin_id": "solana",
      "symbol": "SOL",
      "price": 220.00,
      "price_change_pct": 15.5,
      "volume": 8000000000
    }
  ]
}
```

---

#### GET /api/v1/analytics/pump-dump

Lấy danh sách các pump/dump alerts.

**Query Parameters**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| type | string | "all" | pump, dump, hoặc all |
| date | string | today | Ngày cần lấy |
| limit | int | 50 | Số alerts trả về |

**Response**
```json
{
  "alerts": [
    {
      "coin_id": "dogecoin",
      "symbol": "DOGE",
      "type": "pump",
      "price_change_pct": 25.5,
      "volume_change_pct": 350.0,
      "detected_at": "2025-12-15T08:30:00Z",
      "severity": "high"
    }
  ],
  "total_pumps": 15,
  "total_dumps": 8
}
```

---

#### GET /api/v1/analytics/whale-detection

Lấy các hoạt động whale được phát hiện.

**Response**
```json
{
  "whale_activities": [
    {
      "coin_id": "ethereum",
      "symbol": "ETH",
      "volume_spike_pct": 280.0,
      "normal_volume": 15000000000,
      "current_volume": 42000000000,
      "detected_at": "2025-12-15T06:00:00Z"
    }
  ]
}
```

---

### 4. Market Overview

#### GET /api/v1/market/overview

Lấy tổng quan thị trường.

**Response**
```json
{
  "total_market_cap": 3500000000000,
  "total_volume_24h": 150000000000,
  "btc_dominance": 55.7,
  "eth_dominance": 12.3,
  "active_coins": 98,
  "market_sentiment": "bullish",
  "fear_greed_index": 72,
  "last_updated": "2025-12-15T10:30:00Z"
}
```

---

#### GET /api/v1/market/btc-dominance

Lấy lịch sử BTC dominance.

**Response**
```json
{
  "current": 55.7,
  "history": [
    {
      "date": "2025-12-15",
      "dominance": 55.7
    },
    {
      "date": "2025-12-14",
      "dominance": 55.2
    }
  ]
}
```

---

## Error Handling

### Error Response Format

```json
{
  "error": "Error message",
  "code": "ERROR_CODE",
  "details": {}
}
```

### HTTP Status Codes

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Bad Request - Invalid parameters |
| 404 | Not Found - Resource không tồn tại |
| 429 | Too Many Requests - Rate limit exceeded |
| 500 | Internal Server Error |
| 503 | Service Unavailable - Backend service down |

---

## Rate Limiting

| Tier | Requests/minute | Requests/day |
|------|-----------------|--------------|
| Free | 60 | 1,000 |
| Basic | 300 | 10,000 |
| Pro | 1,000 | Unlimited |

**Rate Limit Headers**
```
X-RateLimit-Limit: 60
X-RateLimit-Remaining: 45
X-RateLimit-Reset: 1702634400
```

---

## Examples

### cURL

```bash
# Get latest prices
curl http://localhost:8000/api/v1/crypto/latest

# Get Bitcoin details
curl http://localhost:8000/api/v1/crypto/bitcoin

# Get daily analytics
curl "http://localhost:8000/api/v1/analytics/daily?date=2025-12-15"

# Get top gainers
curl "http://localhost:8000/api/v1/analytics/top-movers?type=gainers&limit=10"
```

### Python

```python
import requests

BASE_URL = "http://localhost:8000"

# Get latest prices
response = requests.get(f"{BASE_URL}/api/v1/crypto/latest")
data = response.json()

# Get specific coin
response = requests.get(f"{BASE_URL}/api/v1/crypto/bitcoin")
bitcoin = response.json()

# Get analytics
response = requests.get(
    f"{BASE_URL}/api/v1/analytics/daily",
    params={"date": "2025-12-15", "limit": 20}
)
daily_stats = response.json()
```

### JavaScript

```javascript
const BASE_URL = 'http://localhost:8000';

// Get latest prices
const response = await fetch(`${BASE_URL}/api/v1/crypto/latest`);
const data = await response.json();

// Get specific coin
const bitcoin = await fetch(`${BASE_URL}/api/v1/crypto/bitcoin`)
  .then(res => res.json());

// Get top movers
const topGainers = await fetch(
  `${BASE_URL}/api/v1/analytics/top-movers?type=gainers&limit=10`
).then(res => res.json());
```

---

## Changelog

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2025-12-15 | Initial release |

---

## Support

Nếu gặp vấn đề với API, vui lòng:
- Tạo Issue trên GitHub
- Email: api-support@crypto-analytics.com
