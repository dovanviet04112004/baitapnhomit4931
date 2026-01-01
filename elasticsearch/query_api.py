"""
Crypto Query REST API - FastAPI Server

Cung cấp REST endpoints để query dữ liệu crypto từ Elasticsearch

Endpoints:
  GET /api/market/summary          - Tổng quan thị trường
  GET /api/coins                   - Danh sách coins (có filter)
  GET /api/coins/{symbol}          - Chi tiết 1 coin
  GET /api/coins/{symbol}/history  - Lịch sử giá coin
  GET /api/coins/{symbol}/trend    - Xu hướng giá
  GET /api/rankings/gainers        - Top gainers 24h
  GET /api/rankings/losers         - Top losers 24h  
  GET /api/rankings/market-cap     - Ranking theo market cap
  GET /api/alerts                  - Danh sách alerts
  GET /api/alerts/summary          - Tổng hợp alerts
  GET /api/search                  - Tìm kiếm coin

Usage:
  pip install fastapi uvicorn
  python query_api.py
  
  Then open: http://localhost:8000/docs
"""

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
import uvicorn

from elasticsearch_queries import CryptoQueries, format_number

# Initialize
app = FastAPI(
    title="Crypto Analytics API",
    description="REST API để query dữ liệu cryptocurrency từ Elasticsearch",
    version="1.0.0"
)

# CORS - cho phép frontend gọi
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Query client
queries = CryptoQueries()


# =============================================================================
# 📊 MARKET ENDPOINTS
# =============================================================================

@app.get("/api/market/summary", tags=["Market"])
def get_market_summary():
    """
    Tổng quan thị trường crypto
    
    Returns:
        - total_market_cap: Tổng vốn hóa
        - total_volume_24h: Tổng volume 24h
        - coin_count: Số coin tracked
        - gainers/losers count
        - market_sentiment: BULLISH/BEARISH
    """
    return queries.get_market_summary()


@app.get("/api/market/distribution", tags=["Market"])
def get_market_distribution():
    """
    Phân bố market cap theo tiers
    
    Returns:
        - large_cap (>$10B)
        - mid_cap ($1B-$10B)
        - small_cap (<$1B)
    """
    return queries.get_market_cap_distribution()


# =============================================================================
# 🪙 COINS ENDPOINTS
# =============================================================================

@app.get("/api/coins", tags=["Coins"])
def get_coins(
    min_market_cap: Optional[float] = Query(None, description="Market cap tối thiểu"),
    max_market_cap: Optional[float] = Query(None, description="Market cap tối đa"),
    min_price: Optional[float] = Query(None, description="Giá tối thiểu USD"),
    max_price: Optional[float] = Query(None, description="Giá tối đa USD"),
    min_change: Optional[float] = Query(None, description="% change 24h tối thiểu"),
    max_change: Optional[float] = Query(None, description="% change 24h tối đa"),
    limit: int = Query(50, ge=1, le=100, description="Số kết quả tối đa")
):
    """
    Lấy danh sách coins với bộ lọc
    
    Examples:
        - /api/coins?min_market_cap=1000000000  (coins > $1B market cap)
        - /api/coins?min_change=5  (coins tăng > 5% trong 24h)
        - /api/coins?max_price=1  (coins giá < $1)
    """
    return queries.filter_coins(
        min_market_cap=min_market_cap,
        max_market_cap=max_market_cap,
        min_price=min_price,
        max_price=max_price,
        min_change_24h=min_change,
        max_change_24h=max_change,
        limit=limit
    )


@app.get("/api/coins/{symbol}", tags=["Coins"])
def get_coin_detail(symbol: str):
    """
    Lấy thông tin chi tiết 1 coin
    
    Args:
        symbol: BTC, ETH, SOL...
    """
    coin = queries.get_coin_detail(symbol)
    if not coin:
        raise HTTPException(status_code=404, detail=f"Coin '{symbol}' không tìm thấy")
    return coin


@app.get("/api/coins/{symbol}/history", tags=["Coins"])
def get_coin_history(
    symbol: str,
    days: int = Query(7, ge=1, le=365, description="Số ngày lịch sử")
):
    """
    Lấy lịch sử giá coin theo ngày
    
    Args:
        symbol: BTC, ETH, SOL...
        days: Số ngày (default: 7)
    
    Returns:
        List[{date, price_usd, price_high, price_low, volume_24h}]
    """
    history = queries.get_price_history(symbol, days)
    if not history:
        raise HTTPException(status_code=404, detail=f"Không có dữ liệu lịch sử cho '{symbol}'")
    return {"symbol": symbol, "days": days, "data": history}


@app.get("/api/coins/{symbol}/trend", tags=["Coins"])
def get_coin_trend(
    symbol: str,
    days: int = Query(7, ge=1, le=365, description="Số ngày phân tích")
):
    """
    Phân tích xu hướng giá coin
    
    Returns:
        - price_change_percent
        - trend: UP/DOWN
        - avg/min/max price
    """
    return queries.get_price_trend(symbol, days)


# =============================================================================
# 🏆 RANKINGS ENDPOINTS
# =============================================================================

@app.get("/api/rankings/gainers", tags=["Rankings"])
def get_top_gainers(
    limit: int = Query(10, ge=1, le=50, description="Số kết quả")
):
    """
    Top coins tăng giá mạnh nhất 24h
    """
    return queries.get_top_gainers(limit)


@app.get("/api/rankings/losers", tags=["Rankings"])
def get_top_losers(
    limit: int = Query(10, ge=1, le=50, description="Số kết quả")
):
    """
    Top coins giảm giá mạnh nhất 24h
    """
    return queries.get_top_losers(limit)


@app.get("/api/rankings/market-cap", tags=["Rankings"])
def get_market_cap_ranking(
    limit: int = Query(20, ge=1, le=100, description="Số kết quả")
):
    """
    Xếp hạng coins theo market cap
    """
    return queries.get_market_cap_ranking(limit)


# =============================================================================
# 🚨 ALERTS ENDPOINTS
# =============================================================================

@app.get("/api/alerts", tags=["Alerts"])
def get_recent_alerts(
    alert_type: Optional[str] = Query(None, description="Loại alert: PUMP, DUMP, WHALE_ALERT..."),
    severity: Optional[str] = Query(None, description="Mức độ: CRITICAL, HIGH, MEDIUM"),
    limit: int = Query(50, ge=1, le=200)
):
    """
    Lấy danh sách alerts gần đây
    """
    # Build query based on filters
    import requests
    import json
    
    filters = []
    if alert_type:
        filters.append({"term": {"alert_type": alert_type.upper()}})
    if severity:
        filters.append({"term": {"severity": severity.upper()}})
    
    query = {
        "size": limit,
        "query": {
            "bool": {"must": filters} if filters else {"match_all": {}}
        },
        "sort": [{"detected_at": "desc"}]
    }
    
    response = requests.post(
        f"{queries.es_host}/{queries.index_alerts}/_search",
        headers={"Content-Type": "application/json"},
        data=json.dumps(query)
    )
    
    result = response.json()
    return [hit["_source"] for hit in result.get("hits", {}).get("hits", [])]


@app.get("/api/alerts/summary", tags=["Alerts"])
def get_alerts_summary(
    days: int = Query(7, ge=1, le=30, description="Số ngày")
):
    """
    Tổng hợp alerts theo loại và mức độ
    """
    return queries.get_alerts_summary(days)


@app.get("/api/alerts/volume-spikes", tags=["Alerts"])
def get_volume_spikes():
    """
    Phát hiện volume spike bất thường
    """
    return queries.get_volume_spikes()


# =============================================================================
# 🔍 SEARCH ENDPOINT
# =============================================================================

@app.get("/api/search", tags=["Search"])
def search_coins(
    q: str = Query(..., min_length=1, description="Từ khóa tìm kiếm")
):
    """
    Tìm kiếm coin theo tên hoặc symbol
    
    Examples:
        - /api/search?q=bitcoin
        - /api/search?q=eth
    """
    return queries.search_coin(q)


# =============================================================================
# 📈 DASHBOARD ENDPOINTS (Pre-built aggregations)
# =============================================================================

@app.get("/api/dashboard/overview", tags=["Dashboard"])
def get_dashboard_overview():
    """
    Dữ liệu tổng hợp cho dashboard overview
    
    Returns tất cả data cần cho dashboard trong 1 call
    """
    return {
        "market_summary": queries.get_market_summary(),
        "market_distribution": queries.get_market_cap_distribution(),
        "top_gainers": queries.get_top_gainers(5),
        "top_losers": queries.get_top_losers(5),
        "btc_trend": queries.get_price_trend("BTC", 7),
        "eth_trend": queries.get_price_trend("ETH", 7),
        "alerts_summary": queries.get_alerts_summary(7)
    }


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    print("="*60)
    print("🚀 CRYPTO QUERY API SERVER")
    print("="*60)
    print("   📍 API: http://localhost:8000")
    print("   📚 Docs: http://localhost:8000/docs")
    print("   🔄 ReDoc: http://localhost:8000/redoc")
    print("="*60)
    
    uvicorn.run(app, host="0.0.0.0", port=8000)
