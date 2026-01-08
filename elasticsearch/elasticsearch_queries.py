"""
Elasticsearch Query Library - Các truy vấn cho Crypto Analytics

Cung cấp các hàm truy vấn:
  - Thống kê: giá theo thời gian, xu hướng, top gainers/losers
  - Tìm kiếm: search coin, filter, aggregations

Usage:
  from elasticsearch_queries import CryptoQueries
  q = CryptoQueries()
  
  # Lấy giá BTC theo thời gian
  prices = q.get_price_history("BTC", days=7)
  
  # Top gainers 24h
  gainers = q.get_top_gainers(limit=10)
"""

import requests
import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any


class CryptoQueries:
    """Query library cho Crypto Elasticsearch indices"""
    
    def __init__(self, es_host: str = None):
        self.es_host = es_host or os.getenv("ES_HOST", "http://localhost:9200")
        self.index_latest = "crypto_latest"
        self.index_history = "crypto_history"
        self.index_alerts = "alerts"
    
    def _query(self, index: str, body: dict) -> dict:
        """Execute Elasticsearch query"""
        response = requests.post(
            f"{self.es_host}/{index}/_search",
            headers={"Content-Type": "application/json"},
            data=json.dumps(body)
        )
        return response.json()
    
    # =========================================================================
    # 📊 TRUY VẤN THỐNG KÊ
    # =========================================================================
    
    def get_price_history(self, symbol: str, days: int = 7) -> List[Dict]:
        """
        Lấy giá coin theo thời gian
        
        Args:
            symbol: BTC, ETH, SOL...
            days: Số ngày lịch sử
        
        Returns:
            List[{date, price_usd, price_high, price_low, volume}]
        """
        from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        query = {
            "size": 1000,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"symbol": symbol.upper()}},
                        {"range": {"date": {"gte": from_date}}}
                    ]
                }
            },
            "sort": [{"date": "asc"}]
        }
        
        result = self._query(self.index_history, query)
        return [hit["_source"] for hit in result.get("hits", {}).get("hits", [])]
    
    def get_price_trend(self, symbol: str, days: int = 7) -> Dict:
        """
        Tính xu hướng giá: trung bình, % thay đổi, min/max
        
        Returns:
            {symbol, avg_price, min_price, max_price, price_change_percent, trend}
        """
        history = self.get_price_history(symbol, days)
        
        if len(history) < 2:
            return {"symbol": symbol, "error": "Không đủ dữ liệu"}
        
        prices = [h.get("price_usd", 0) for h in history]
        first_price = prices[0] if prices[0] else 1
        last_price = prices[-1] if prices[-1] else 1
        
        change_percent = ((last_price - first_price) / first_price) * 100
        
        return {
            "symbol": symbol,
            "period_days": days,
            "first_price": first_price,
            "last_price": last_price,
            "avg_price": sum(prices) / len(prices),
            "min_price": min(prices),
            "max_price": max(prices),
            "price_change_percent": round(change_percent, 2),
            "trend": "UP" if change_percent > 0 else "DOWN"
        }
    
    def get_top_gainers(self, limit: int = 10) -> List[Dict]:
        """
        Top coins tăng giá mạnh nhất 24h
        
        Returns:
            List[{symbol, name, price_usd, percent_change_24h}]
        """
        query = {
            "size": limit,
            "query": {"range": {"percent_change_24h": {"gt": 0}}},
            "sort": [{"percent_change_24h": "desc"}],
            "_source": ["symbol", "name", "price_usd", "percent_change_24h", "volume_24h"]
        }
        
        result = self._query(self.index_latest, query)
        return [hit["_source"] for hit in result.get("hits", {}).get("hits", [])]
    
    def get_top_losers(self, limit: int = 10) -> List[Dict]:
        """
        Top coins giảm giá mạnh nhất 24h
        
        Returns:
            List[{symbol, name, price_usd, percent_change_24h}]
        """
        query = {
            "size": limit,
            "query": {"range": {"percent_change_24h": {"lt": 0}}},
            "sort": [{"percent_change_24h": "asc"}],
            "_source": ["symbol", "name", "price_usd", "percent_change_24h", "volume_24h"]
        }
        
        result = self._query(self.index_latest, query)
        return [hit["_source"] for hit in result.get("hits", {}).get("hits", [])]
    
    def get_market_cap_ranking(self, limit: int = 20) -> List[Dict]:
        """
        Xếp hạng coins theo market cap
        
        Returns:
            List[{rank, symbol, name, market_cap, price_usd}]
        """
        query = {
            "size": limit,
            "query": {"match_all": {}},
            "sort": [{"market_cap": "desc"}],
            "_source": ["symbol", "name", "market_cap", "price_usd", "rank", "volume_24h"]
        }
        
        result = self._query(self.index_latest, query)
        coins = [hit["_source"] for hit in result.get("hits", {}).get("hits", [])]
        
        # Add ranking
        for i, coin in enumerate(coins):
            coin["current_rank"] = i + 1
        
        return coins
    
    def get_volume_spikes(self, threshold_percent: float = 100) -> List[Dict]:
        """
        Phát hiện volume spike (volume cao bất thường)
        
        Args:
            threshold_percent: % volume tăng so với trung bình để coi là spike
        
        Returns:
            List of coins với volume spike
        """
        # Lấy tất cả alerts loại volume/whale
        query = {
            "size": 100,
            "query": {
                "bool": {
                    "should": [
                        {"term": {"alert_type": "whale_alert"}},
                        {"term": {"alert_type": "WHALE_ALERT"}},
                        {"wildcard": {"alert_type": "*volume*"}}
                    ],
                    "minimum_should_match": 1
                }
            },
            "sort": [{"detected_at": "desc"}]
        }
        
        result = self._query(self.index_alerts, query)
        return [hit["_source"] for hit in result.get("hits", {}).get("hits", [])]
    
    # =========================================================================
    # 🔍 TRUY VẤN TÌM KIẾM
    # =========================================================================
    
    def search_coin(self, keyword: str) -> List[Dict]:
        """
        Tìm kiếm coin theo tên hoặc symbol
        
        Args:
            keyword: "bitcoin", "btc", "eth"...
        
        Returns:
            List of matching coins
        """
        query = {
            "size": 20,
            "query": {
                "bool": {
                    "should": [
                        {"match": {"name": {"query": keyword, "fuzziness": "AUTO"}}},
                        {"match": {"symbol": {"query": keyword.upper()}}},
                        {"wildcard": {"name": f"*{keyword.lower()}*"}},
                        {"wildcard": {"symbol": f"*{keyword.upper()}*"}}
                    ],
                    "minimum_should_match": 1
                }
            }
        }
        
        result = self._query(self.index_latest, query)
        return [hit["_source"] for hit in result.get("hits", {}).get("hits", [])]
    
    def filter_coins(
        self,
        min_market_cap: Optional[float] = None,
        max_market_cap: Optional[float] = None,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
        min_change_24h: Optional[float] = None,
        max_change_24h: Optional[float] = None,
        limit: int = 50
    ) -> List[Dict]:
        """
        Lọc coins theo nhiều tiêu chí
        
        Args:
            min_market_cap: Market cap tối thiểu
            max_market_cap: Market cap tối đa  
            min_price: Giá tối thiểu
            max_price: Giá tối đa
            min_change_24h: % thay đổi 24h tối thiểu
            max_change_24h: % thay đổi 24h tối đa
        
        Returns:
            List of filtered coins
        """
        filters = []
        
        if min_market_cap is not None or max_market_cap is not None:
            market_cap_range = {}
            if min_market_cap: market_cap_range["gte"] = min_market_cap
            if max_market_cap: market_cap_range["lte"] = max_market_cap
            filters.append({"range": {"market_cap": market_cap_range}})
        
        if min_price is not None or max_price is not None:
            price_range = {}
            if min_price: price_range["gte"] = min_price
            if max_price: price_range["lte"] = max_price
            filters.append({"range": {"price_usd": price_range}})
        
        if min_change_24h is not None or max_change_24h is not None:
            change_range = {}
            if min_change_24h: change_range["gte"] = min_change_24h
            if max_change_24h: change_range["lte"] = max_change_24h
            filters.append({"range": {"percent_change_24h": change_range}})
        
        query = {
            "size": limit,
            "query": {
                "bool": {
                    "must": filters if filters else [{"match_all": {}}]
                }
            },
            "sort": [{"market_cap": "desc"}]
        }
        
        result = self._query(self.index_latest, query)
        return [hit["_source"] for hit in result.get("hits", {}).get("hits", [])]
    
    # =========================================================================
    # 📈 AGGREGATIONS CHO DASHBOARD
    # =========================================================================
    
    def get_market_summary(self) -> Dict:
        """
        Tổng quan thị trường: total market cap, volume, số coins
        
        Returns:
            {total_market_cap, total_volume, coin_count, avg_change_24h}
        """
        query = {
            "size": 0,
            "aggs": {
                "total_market_cap": {"sum": {"field": "market_cap"}},
                "total_volume": {"sum": {"field": "volume_24h"}},
                "avg_change_24h": {"avg": {"field": "percent_change_24h"}},
                "coin_count": {"value_count": {"field": "symbol"}},
                "gainers_count": {
                    "filter": {"range": {"percent_change_24h": {"gt": 0}}}
                },
                "losers_count": {
                    "filter": {"range": {"percent_change_24h": {"lt": 0}}}
                }
            }
        }
        
        result = self._query(self.index_latest, query)
        aggs = result.get("aggregations", {})
        
        gainers = aggs.get("gainers_count", {}).get("doc_count", 0)
        losers = aggs.get("losers_count", {}).get("doc_count", 0)
        
        return {
            "total_market_cap": aggs.get("total_market_cap", {}).get("value", 0),
            "total_volume_24h": aggs.get("total_volume", {}).get("value", 0),
            "coin_count": aggs.get("coin_count", {}).get("value", 0),
            "avg_change_24h": round(aggs.get("avg_change_24h", {}).get("value", 0), 2),
            "gainers_count": gainers,
            "losers_count": losers,
            "market_sentiment": "BULLISH" if gainers > losers else "BEARISH"
        }
    
    def get_market_cap_distribution(self) -> Dict:
        """
        Phân bố market cap theo tiers: Large/Mid/Small cap
        
        Returns:
            {large_cap, mid_cap, small_cap} với count và total_value
        """
        query = {
            "size": 0,
            "aggs": {
                "large_cap": {
                    "filter": {"range": {"market_cap": {"gte": 10_000_000_000}}},
                    "aggs": {
                        "total_value": {"sum": {"field": "market_cap"}}
                    }
                },
                "mid_cap": {
                    "filter": {
                        "bool": {
                            "must": [
                                {"range": {"market_cap": {"gte": 1_000_000_000}}},
                                {"range": {"market_cap": {"lt": 10_000_000_000}}}
                            ]
                        }
                    },
                    "aggs": {
                        "total_value": {"sum": {"field": "market_cap"}}
                    }
                },
                "small_cap": {
                    "filter": {"range": {"market_cap": {"lt": 1_000_000_000}}},
                    "aggs": {
                        "total_value": {"sum": {"field": "market_cap"}}
                    }
                }
            }
        }
        
        result = self._query(self.index_latest, query)
        aggs = result.get("aggregations", {})
        
        return {
            "large_cap": {
                "count": aggs.get("large_cap", {}).get("doc_count", 0),
                "total_market_cap": aggs.get("large_cap", {}).get("total_value", {}).get("value", 0),
                "criteria": ">$10B"
            },
            "mid_cap": {
                "count": aggs.get("mid_cap", {}).get("doc_count", 0),
                "total_market_cap": aggs.get("mid_cap", {}).get("total_value", {}).get("value", 0),
                "criteria": "$1B-$10B"
            },
            "small_cap": {
                "count": aggs.get("small_cap", {}).get("doc_count", 0),
                "total_market_cap": aggs.get("small_cap", {}).get("total_value", {}).get("value", 0),
                "criteria": "<$1B"
            }
        }
    
    def get_alerts_summary(self, days: int = 7) -> Dict:
        """
        Tổng hợp alerts theo loại và mức độ nghiêm trọng
        
        Returns:
            {by_type: {...}, by_severity: {...}, total_count}
        """
        from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        query = {
            "size": 0,
            "query": {
                "range": {"detected_at": {"gte": from_date}}
            },
            "aggs": {
                "by_type": {
                    "terms": {"field": "alert_type", "size": 20}
                },
                "by_severity": {
                    "terms": {"field": "severity", "size": 10}
                },
                "by_date": {
                    "date_histogram": {
                        "field": "detected_at",
                        "calendar_interval": "day"
                    }
                }
            }
        }
        
        result = self._query(self.index_alerts, query)
        aggs = result.get("aggregations", {})
        
        return {
            "total_count": result.get("hits", {}).get("total", {}).get("value", 0),
            "by_type": {
                bucket["key"]: bucket["doc_count"] 
                for bucket in aggs.get("by_type", {}).get("buckets", [])
            },
            "by_severity": {
                bucket["key"]: bucket["doc_count"]
                for bucket in aggs.get("by_severity", {}).get("buckets", [])
            },
            "by_date": [
                {"date": bucket["key_as_string"], "count": bucket["doc_count"]}
                for bucket in aggs.get("by_date", {}).get("buckets", [])
            ]
        }
    
    def get_coin_detail(self, symbol: str) -> Optional[Dict]:
        """
        Lấy thông tin chi tiết 1 coin
        
        Returns:
            Full coin data from crypto_latest
        """
        query = {
            "size": 1,
            "query": {"term": {"symbol": symbol.upper()}}
        }
        
        result = self._query(self.index_latest, query)
        hits = result.get("hits", {}).get("hits", [])
        
        if hits:
            return hits[0]["_source"]
        return None


# =============================================================================
# CLI TEST
# =============================================================================

def format_number(num: float) -> str:
    """Format số lớn cho dễ đọc"""
    if num >= 1_000_000_000_000:
        return f"${num/1_000_000_000_000:.2f}T"
    elif num >= 1_000_000_000:
        return f"${num/1_000_000_000:.2f}B"
    elif num >= 1_000_000:
        return f"${num/1_000_000:.2f}M"
    elif num >= 1_000:
        return f"${num/1_000:.2f}K"
    else:
        return f"${num:.2f}"


def main():
    """Test các queries"""
    print("="*60)
    print("🔍 ELASTICSEARCH CRYPTO QUERIES - TEST")
    print("="*60)
    
    q = CryptoQueries()
    
    # 1. Market Summary
    print("\n📊 MARKET SUMMARY")
    print("-"*40)
    summary = q.get_market_summary()
    print(f"   Total Market Cap: {format_number(summary['total_market_cap'])}")
    print(f"   Total Volume 24h: {format_number(summary['total_volume_24h'])}")
    print(f"   Tracked Coins: {summary['coin_count']}")
    print(f"   Avg Change 24h: {summary['avg_change_24h']}%")
    print(f"   Gainers: {summary['gainers_count']} | Losers: {summary['losers_count']}")
    print(f"   Sentiment: {summary['market_sentiment']}")
    
    # 2. Top Gainers
    print("\n🚀 TOP 5 GAINERS 24H")
    print("-"*40)
    gainers = q.get_top_gainers(5)
    for i, coin in enumerate(gainers, 1):
        print(f"   {i}. {coin['symbol']}: +{coin['percent_change_24h']:.2f}% (${coin['price_usd']:.4f})")
    
    # 3. Top Losers
    print("\n📉 TOP 5 LOSERS 24H")
    print("-"*40)
    losers = q.get_top_losers(5)
    for i, coin in enumerate(losers, 1):
        print(f"   {i}. {coin['symbol']}: {coin['percent_change_24h']:.2f}% (${coin['price_usd']:.4f})")
    
    # 4. BTC Price Trend
    print("\n📈 BTC PRICE TREND (7 DAYS)")
    print("-"*40)
    trend = q.get_price_trend("BTC", 7)
    if "error" not in trend:
        print(f"   First Price: ${trend['first_price']:,.2f}")
        print(f"   Last Price: ${trend['last_price']:,.2f}")
        print(f"   Change: {trend['price_change_percent']}% ({trend['trend']})")
        print(f"   Range: ${trend['min_price']:,.2f} - ${trend['max_price']:,.2f}")
    else:
        print(f"   {trend['error']}")
    
    # 5. Market Cap Distribution
    print("\n💰 MARKET CAP DISTRIBUTION")
    print("-"*40)
    dist = q.get_market_cap_distribution()
    for tier, data in dist.items():
        print(f"   {tier.replace('_', ' ').title()} ({data['criteria']}): {data['count']} coins")
    
    # 6. Search Example
    print("\n🔍 SEARCH 'bitcoin'")
    print("-"*40)
    results = q.search_coin("bitcoin")
    for coin in results[:3]:
        print(f"   {coin['symbol']}: {coin['name']} - ${coin['price_usd']:,.2f}")
    
    # 7. Alerts Summary
    print("\n🚨 ALERTS SUMMARY (7 DAYS)")
    print("-"*40)
    alerts = q.get_alerts_summary(7)
    print(f"   Total Alerts: {alerts['total_count']:,}")
    print(f"   By Type: {alerts['by_type']}")
    print(f"   By Severity: {alerts['by_severity']}")
    
    print("\n" + "="*60)
    print("✅ QUERIES TEST COMPLETE!")
    print("="*60)


if __name__ == "__main__":
    main()
