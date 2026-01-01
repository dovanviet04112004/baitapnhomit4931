"""
CoinGecko Cryptocurrency Crawler - Streaming Mode
Crawl giá crypto từ CoinGecko API và gửi trực tiếp vào Kafka.
"""

import json
import time
import requests
from datetime import datetime, timezone
from typing import List, Dict

from kafka import KafkaProducer
from kafka.errors import KafkaError

# CoinGecko API config
COINGECKO_API_URL = "https://api.coingecko.com/api/v3"

# Lấy top 100 coins theo market cap
COIN_IDS = None  # None = lấy top 100 theo market cap
TOP_N_COINS = 100

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = ["localhost:19092", "localhost:19093", "localhost:19094"]
KAFKA_TOPIC = "raw_crypto"

# Crawl interval (CoinGecko free tier: 10-30 calls/minute)
CRAWL_INTERVAL_SECONDS = 60


def create_kafka_producer() -> KafkaProducer:
    """Create Kafka producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
        )
        print("✅ Kết nối Kafka thành công!")
        return producer
    except KafkaError as e:
        raise RuntimeError(f"❌ Không thể kết nối Kafka: {e}")


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
    
    if COIN_IDS:
        params["ids"] = ",".join(COIN_IDS)
    
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


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
            
            # Price data
            "current_price": coin.get("current_price"),
            "price_change_24h": coin.get("price_change_24h"),
            "price_change_percentage_24h": coin.get("price_change_percentage_24h"),
            "price_change_percentage_1h": coin.get("price_change_percentage_1h_in_currency"),
            "price_change_percentage_7d": coin.get("price_change_percentage_7d_in_currency"),
            
            # Market data
            "market_cap": coin.get("market_cap"),
            "market_cap_rank": coin.get("market_cap_rank"),
            "total_volume": coin.get("total_volume"),
            
            # Supply data
            "circulating_supply": coin.get("circulating_supply"),
            "total_supply": coin.get("total_supply"),
            "max_supply": coin.get("max_supply"),
            
            # 24h range
            "high_24h": coin.get("high_24h"),
            "low_24h": coin.get("low_24h"),
            
            # ATH/ATL
            "ath": coin.get("ath"),
            "ath_change_percentage": coin.get("ath_change_percentage"),
            "atl": coin.get("atl"),
            
            # Metadata
            "image_url": coin.get("image"),
            "last_updated": coin.get("last_updated"),
        }
        results.append(transformed)
    
    return results


def send_to_kafka(producer: KafkaProducer, coins: List[Dict]) -> int:
    """Send coin data to Kafka."""
    sent = 0
    for coin in coins:
        key = f"crypto_{coin['coin_id']}"
        producer.send(KAFKA_TOPIC, key=key, value=coin)
        sent += 1
    producer.flush()
    return sent


def main():
    """Main streaming function."""
    print("=" * 50)
    print("🚀 CoinGecko Streaming Crawler → Kafka")
    print(f"📡 Topic: {KAFKA_TOPIC}")
    print(f"⏱️  Interval: {CRAWL_INTERVAL_SECONDS}s")
    print("=" * 50)
    
    producer = create_kafka_producer()
    total_sent = 0
    crawl_count = 0
    
    try:
        while True:
            crawl_count += 1
            print(f"\n🔄 Crawl #{crawl_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            try:
                # Fetch from API
                raw_data = fetch_crypto_data()
                
                # Transform
                coins = transform_coin_data(raw_data)
                
                # Send to Kafka
                sent = send_to_kafka(producer, coins)
                total_sent += sent
                
                # Print status
                btc = next((c for c in coins if c["coin_id"] == "bitcoin"), None)
                if btc:
                    print(f"   BTC: ${btc['current_price']:,.2f} ({btc['price_change_percentage_24h']:+.2f}%)")
                
                print(f"   ✅ Đã gửi {sent} coins | Tổng: {total_sent}")
                
            except requests.exceptions.RequestException as e:
                print(f"   ⚠️ API error: {e}")
            
            # Wait for next crawl
            print(f"   ⏳ Đợi {CRAWL_INTERVAL_SECONDS}s...")
            time.sleep(CRAWL_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Dừng crawler. Tổng đã gửi: {total_sent} records")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
