"""
Fake dữ liệu crypto theo từng ngày trong 2 tháng 11-12/2025 và gửi vào Kafka.
Mô phỏng biến động giá crypto realistic.
"""

import json
import random
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = ["kafka:9092"]  # Docker network
KAFKA_TOPIC = "crypto-raw"  # Match with consumer

# Source data file
DATA_FILE = "output/crypto_raw.json"

# Date range to fake (1 ngày - test nhanh)
START_DATE = "2026-01-07"
END_DATE = "2026-01-07"

# Số lần crawl mỗi ngày (24 lần = mỗi giờ 1 lần)
CRAWLS_PER_DAY = 24

# Fake coins để thêm vào top 100 (mô phỏng coin mới vào top)
FAKE_NEW_COINS = [
    {"coin_id": "fake-moon-1", "symbol": "MOON1", "name": "MoonShot Token", "current_price": 0.05, "market_cap": 500000000, "total_volume": 50000000, "circulating_supply": 10000000000},
    {"coin_id": "fake-rocket-2", "symbol": "ROCK2", "name": "Rocket Finance", "current_price": 1.25, "market_cap": 450000000, "total_volume": 30000000, "circulating_supply": 360000000},
    {"coin_id": "fake-degen-3", "symbol": "DEGEN", "name": "Degen Protocol", "current_price": 0.008, "market_cap": 400000000, "total_volume": 80000000, "circulating_supply": 50000000000},
    {"coin_id": "fake-ai-4", "symbol": "AIBOT", "name": "AI Bot Network", "current_price": 2.50, "market_cap": 380000000, "total_volume": 25000000, "circulating_supply": 152000000},
    {"coin_id": "fake-meme-5", "symbol": "MEME5", "name": "Super Meme Coin", "current_price": 0.0001, "market_cap": 350000000, "total_volume": 100000000, "circulating_supply": 3500000000000},
]


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


def load_base_data(path: str) -> List[Dict]:
    """Load base crypto data from JSON file."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def generate_crawl_time(day_dt: datetime, crawl_index: int) -> str:
    """Generate crawl timestamp for specific minute of day.
    
    Args:
        day_dt: Ngày
        crawl_index: Lần crawl trong ngày (0-1439, tương ứng mỗi phút)
    """
    hour = crawl_index // 60
    minute = crawl_index % 60
    second = random.randint(0, 59)
    dt = datetime(
        day_dt.year, day_dt.month, day_dt.day,
        hour, minute, second
    )
    # Format không có timezone để tránh Spark convert
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def fake_crypto_data(base_coin: Dict, day_dt: datetime, crawl_index: int, 
                     price_trend: float, volatility: float, current_rank: int) -> Dict:
    """
    Fake crypto data với biến động realistic.
    
    Args:
        base_coin: Dữ liệu gốc của coin
        day_dt: Ngày fake
        crawl_index: Lần crawl trong ngày (0-1439)
        price_trend: Xu hướng giá (-1 to 1)
        volatility: Độ biến động của coin
        current_rank: Thứ hạng hiện tại của coin
    """
    coin = dict(base_coin)
    
    # Crawl time
    coin["crawl_time"] = generate_crawl_time(day_dt, crawl_index)
    coin["fake_date"] = day_dt.date().isoformat()
    coin["fake_minute"] = crawl_index  # Phút trong ngày (0-1439)
    
    # === FAKE RANK ===
    # Top 10 giữ nguyên rank (BTC=#1, ETH=#2...)
    # Top 11-50 dao động ±2
    # Top 51-100 dao động ±5
    if current_rank <= 10:
        coin["market_cap_rank"] = current_rank  # Không đổi
    elif current_rank <= 50:
        coin["market_cap_rank"] = max(11, min(50, current_rank + random.randint(-2, 2)))
    else:
        coin["market_cap_rank"] = max(51, min(100, current_rank + random.randint(-5, 5)))
    
    # === FAKE PRICE ===
    base_price = float(coin.get("current_price", 0) or 0)
    
    # Price change: trend + random noise
    # Crypto biến động nhỏ mỗi phút (~0.01-0.1%)
    minute_change = (price_trend * volatility / 1440) + random.gauss(0, volatility * 0.01)
    
    price_multiplier = 1 + minute_change
    new_price = max(0.0001, base_price * price_multiplier)
    coin["current_price"] = round(new_price, 6 if new_price < 1 else 2)
    
    # === FAKE 24H CHANGE ===
    coin["price_change_24h"] = round(new_price - base_price, 4)
    coin["price_change_percentage_24h"] = round(random.uniform(-15, 15), 2)
    coin["price_change_percentage_1h"] = round(random.uniform(-3, 3), 2)
    coin["price_change_percentage_7d"] = round(random.uniform(-25, 30), 2)
    
    # === FAKE MARKET CAP ===
    base_mcap = float(coin.get("market_cap", 0) or 0)
    if base_mcap > 0:
        supply = coin.get("circulating_supply", 0) or 0
        if supply > 0:
            coin["market_cap"] = int(new_price * supply)
        else:
            coin["market_cap"] = int(base_mcap * price_multiplier)
    
    # === FAKE VOLUME ===
    base_volume = float(coin.get("total_volume", 0) or 0)
    # Volume biến động 50-200% tùy ngày
    volume_factor = random.uniform(0.5, 2.0)
    coin["total_volume"] = int(base_volume * volume_factor)
    
    # === FAKE 24H HIGH/LOW ===
    price_range = new_price * volatility * 0.5
    coin["high_24h"] = round(new_price + random.uniform(0, price_range), 2)
    coin["low_24h"] = round(new_price - random.uniform(0, price_range), 2)
    
    # === FAKE SUPPLY (slight increase over time) ===
    base_supply = float(coin.get("circulating_supply", 0) or 0)
    if base_supply > 0:
        # Supply tăng nhẹ 0-0.01% mỗi ngày
        supply_increase = random.uniform(0, 0.0001)
        coin["circulating_supply"] = int(base_supply * (1 + supply_increase))
    
    return coin


def get_coin_volatility(coin_id: str) -> float:
    """Get volatility factor for each coin (0.01 to 0.15)."""
    # Stablecoins có volatility thấp
    stablecoins = ["tether", "usd-coin", "dai", "busd"]
    if coin_id in stablecoins:
        return 0.001  # 0.1%
    
    # Major coins volatility trung bình
    major_coins = ["bitcoin", "ethereum", "binancecoin"]
    if coin_id in major_coins:
        return 0.05  # 5%
    
    # Altcoins volatility cao
    return 0.10  # 10%


def send_fake_data(delay_seconds: float = 0.002) -> None:
    """Generate and send fake crypto data for date range."""
    
    # Load base data
    all_coins = load_base_data(DATA_FILE)
    print(f"📦 Đã load {len(all_coins)} coins từ {DATA_FILE}")
    
    # Thêm fake coins vào pool
    for fake_coin in FAKE_NEW_COINS:
        fake_coin["source"] = "coingecko"
        fake_coin["price_change_24h"] = 0
        fake_coin["price_change_percentage_24h"] = 0
        fake_coin["high_24h"] = fake_coin["current_price"]
        fake_coin["low_24h"] = fake_coin["current_price"]
        all_coins.append(fake_coin)
    
    print(f"📦 Tổng pool: {len(all_coins)} coins (bao gồm {len(FAKE_NEW_COINS)} fake coins)")
    
    # Create producer
    producer = create_kafka_producer()
    
    # Parse dates
    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")
    
    total_days = (end_dt - start_dt).days + 1
    print(f"📅 Fake data từ {START_DATE} đến {END_DATE} ({total_days} ngày)")
    
    total_sent = 0
    
    # Generate random price trends for each coin over the period
    price_trends = {}
    for coin in all_coins:
        price_trends[coin["coin_id"]] = random.uniform(-0.3, 0.3)
    
    # Current active coins (start with first 100)
    active_coins = all_coins[:100]
    reserve_coins = all_coins[100:]  # Coins chờ vào top 100
    
    try:
        cur_day = start_dt
        day_count = 0
        
        while cur_day <= end_dt:
            day_count += 1
            print(f"\n📅 Ngày {day_count}/{total_days}: {cur_day.date().isoformat()}")
            
            # === COIN RA/VÀO TOP 100 ===
            # Mỗi ngày có 10% chance có coin ra/vào
            # CHỈ coin rank 80-100 mới có thể rớt (coin lớn như BTC, ETH không rớt)
            if random.random() < 0.10 and reserve_coins:
                # Số coin thay đổi: 1-2
                num_changes = random.randint(1, 2)
                
                for _ in range(num_changes):
                    if reserve_coins and len(active_coins) >= 100:
                        # Chỉ chọn coin rớt từ vị trí 80-99 (coin nhỏ cuối top 100)
                        # Coin top 80 (index 0-79) được bảo vệ, không rớt
                        drop_idx = random.randint(80, len(active_coins)-1)
                        dropped_coin = active_coins.pop(drop_idx)
                        reserve_coins.append(dropped_coin)
                        
                        # Chọn coin mới vào
                        new_coin = reserve_coins.pop(random.randint(0, len(reserve_coins)-1))
                        active_coins.append(new_coin)
                        
                        print(f"   🔄 {dropped_coin['symbol']} (rank ~{drop_idx+1}) rớt khỏi top 100, {new_coin['symbol']} vào top!")
            
            # Slight daily trend shift
            for coin_id in price_trends:
                shift = random.gauss(0, 0.05)
                price_trends[coin_id] = max(-0.5, min(0.5, price_trends[coin_id] + shift))
            
            for crawl_idx in range(CRAWLS_PER_DAY):
                for idx, coin in enumerate(active_coins):
                    coin_id = coin["coin_id"]
                    volatility = get_coin_volatility(coin_id)
                    trend = price_trends.get(coin_id, 0)
                    current_rank = idx + 1  # Rank = vị trí trong list + 1
                    
                    # Generate fake data
                    fake_coin = fake_crypto_data(coin, cur_day, crawl_idx, trend, volatility, current_rank)
                    
                    # Send to Kafka
                    key = f"crypto_{coin_id}"
                    producer.send(KAFKA_TOPIC, key=key, value=fake_coin)
                    
                    total_sent += 1
                    
                    if delay_seconds > 0:
                        time.sleep(delay_seconds)
                
                # Flush mỗi 60 lần crawl (mỗi giờ)
                if crawl_idx % 60 == 59:
                    producer.flush()
                    hour = crawl_idx // 60
                    print(f"   ⏰ Giờ {hour:02d}: đã gửi {total_sent:,} records...")
            
            # Progress
            progress = (day_count / total_days) * 100
            print(f"   ✅ Ngày {day_count} hoàn tất ({progress:.1f}%)")
            
            cur_day += timedelta(days=1)
        
        producer.flush()
        print(f"\n🎉 Hoàn tất! Đã gửi {total_sent:,} records vào topic '{KAFKA_TOPIC}'")
        
    finally:
        producer.close()


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 Fake Crypto Data Generator → Kafka")
    print("=" * 60)
    send_fake_data()
