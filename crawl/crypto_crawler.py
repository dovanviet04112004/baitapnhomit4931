"""
CoinGecko Cryptocurrency Crawler - Batch Mode
Crawl giá crypto từ CoinGecko API và lưu vào file JSON.
"""

import json
import requests
from datetime import datetime, timezone
from typing import List, Dict

# CoinGecko API config
COINGECKO_API_URL = "https://api.coingecko.com/api/v3"

# Lấy top 100 coins theo market cap (không cần list ID cố định)
COIN_IDS = None  # None = lấy top 100 theo market cap
TOP_N_COINS = 100

OUTPUT_FILE = "output/crypto_raw.json"


def fetch_crypto_data() -> List[Dict]:
    """Fetch cryptocurrency data from CoinGecko API."""
    
    url = f"{COINGECKO_API_URL}/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": TOP_N_COINS,
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "1h,24h,7d"
    }
    
    # Nếu có list ID cụ thể thì dùng, không thì lấy top theo market cap
    if COIN_IDS:
        params["ids"] = ",".join(COIN_IDS)
    
    print(f"🔄 Đang gọi CoinGecko API (top {TOP_N_COINS} coins)...")
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    print(f"✅ Nhận được {len(data)} coins từ API")
    
    return data


def transform_coin_data(raw_data: List[Dict]) -> List[Dict]:
    """Transform raw API data to our schema."""
    
    crawl_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    
    results = []
    for coin in raw_data:
        transformed = {
            "crawl_time": crawl_time,
            "source": "coingecko",
            
            # Coin identification
            "coin_id": coin.get("id"),
            "symbol": coin.get("symbol", "").upper(),
            "name": coin.get("name"),
            
            # Price data (changes every second)
            "current_price": coin.get("current_price"),
            "price_change_24h": coin.get("price_change_24h"),
            "price_change_percentage_24h": coin.get("price_change_percentage_24h"),
            "price_change_percentage_1h": coin.get("price_change_percentage_1h_in_currency"),
            "price_change_percentage_7d": coin.get("price_change_percentage_7d_in_currency"),
            
            # Market data (changes frequently)
            "market_cap": coin.get("market_cap"),
            "market_cap_rank": coin.get("market_cap_rank"),
            "total_volume": coin.get("total_volume"),
            
            # Supply data
            "circulating_supply": coin.get("circulating_supply"),
            "total_supply": coin.get("total_supply"),
            "max_supply": coin.get("max_supply"),
            
            # Historical highs/lows
            "ath": coin.get("ath"),
            "ath_change_percentage": coin.get("ath_change_percentage"),
            "ath_date": coin.get("ath_date"),
            "atl": coin.get("atl"),
            "atl_change_percentage": coin.get("atl_change_percentage"),
            "atl_date": coin.get("atl_date"),
            
            # 24h range
            "high_24h": coin.get("high_24h"),
            "low_24h": coin.get("low_24h"),
            
            # Metadata
            "image_url": coin.get("image"),
            "last_updated": coin.get("last_updated"),
        }
        results.append(transformed)
    
    return results


def save_to_json(data: List[Dict], filepath: str):
    """Save data to JSON file."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"💾 Đã lưu {len(data)} coins vào {filepath}")


def main():
    """Main crawl function."""
    print("=" * 50)
    print("🚀 CoinGecko Cryptocurrency Crawler")
    print("=" * 50)
    
    try:
        # Fetch from API
        raw_data = fetch_crypto_data()
        
        # Transform data
        coins = transform_coin_data(raw_data)
        
        # Save to file
        save_to_json(coins, OUTPUT_FILE)
        
        # Print sample
        print("\n📊 Sample data (Bitcoin):")
        btc = next((c for c in coins if c["coin_id"] == "bitcoin"), None)
        if btc:
            print(f"   Price: ${btc['current_price']:,.2f}")
            print(f"   24h Change: {btc['price_change_percentage_24h']:.2f}%")
            print(f"   Market Cap: ${btc['market_cap']:,.0f}")
            print(f"   Volume 24h: ${btc['total_volume']:,.0f}")
        
        print("\n✅ Crawl hoàn tất!")
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Lỗi khi gọi API: {e}")
    except Exception as e:
        print(f"❌ Lỗi: {e}")


if __name__ == "__main__":
    main()
