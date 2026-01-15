# Data Flow Documentation

Chi tiết luồng dữ liệu trong hệ thống Crypto Analytics Pipeline.

---

## 1. End-to-End Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        END-TO-END DATA FLOW                                  │
└─────────────────────────────────────────────────────────────────────────────┘

    EXTERNAL                    INTERNAL PIPELINE                      OUTPUT
    ────────                    ─────────────────                      ──────

  ┌──────────┐
  │CoinGecko │
  │   API    │
  └────┬─────┘
       │
       │ HTTP GET /coins/markets
       │ Rate: 1 req/min
       │ Data: Top 100 coins
       │
       ▼
  ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
  │ CRAWLER  │────►│  KAFKA   │────►│   HDFS   │────►│  SPARK   │
  │          │     │          │     │          │     │          │
  │ Python   │     │ 3 Broker │     │ Parquet  │     │ PySpark  │
  │ Requests │     │ 3 Topics │     │ Partition│     │ 14 Jobs  │
  └──────────┘     └──────────┘     └──────────┘     └──────────┘
                        │                                 │
                        │                                 │
                        ▼                                 ▼
                  ┌──────────┐                      ┌──────────┐
                  │  SPARK   │                      │   ES     │
                  │STREAMING │                      │ Indexer  │
                  │          │                      │          │
                  │Real-time │                      │ 3 Index  │
                  │ Alerts   │                      │          │
                  └──────────┘                      └──────────┘
                        │                                 │
                        │                                 │
                        ▼                                 ▼
                  ┌──────────┐                      ┌──────────┐
                  │PostgreSQL│                      │  KIBANA  │
                  │          │◄─────────────────────│          │
                  │ Metrics  │                      │Dashboard │
                  │   DB     │                      │          │
                  └──────────┘                      └──────────┘
                        │
                        │
                        ▼
                  ┌──────────┐
                  │   WEB    │
                  │DASHBOARD │
                  │          │
                  │ Chart.js │
                  └──────────┘
```

---

## 2. Detailed Data Transformation

### Stage 1: Data Collection

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGE 1: DATA COLLECTION                                                    │
└─────────────────────────────────────────────────────────────────────────────┘

CoinGecko API Response:
┌─────────────────────────────────────────────────────────────────────────────┐
│ GET /coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100       │
│                                                                              │
│ [                                                                            │
│   {                                                                          │
│     "id": "bitcoin",                                                         │
│     "symbol": "btc",                                                         │
│     "name": "Bitcoin",                                                       │
│     "current_price": 98500,                                                  │
│     "market_cap": 1950000000000,                                             │
│     "market_cap_rank": 1,                                                    │
│     "total_volume": 45000000000,                                             │
│     "high_24h": 99500,                                                       │
│     "low_24h": 96500,                                                        │
│     "price_change_24h": 1500,                                                │
│     "price_change_percentage_24h": 1.55,                                     │
│     "circulating_supply": 19500000,                                          │
│     "total_supply": 21000000,                                                │
│     "ath": 108000,                                                           │
│     "ath_date": "2025-11-20T00:00:00.000Z",                                  │
│     "last_updated": "2025-12-15T10:30:00.000Z"                               │
│   },                                                                         │
│   ... (99 more coins)                                                        │
│ ]                                                                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ Crawler transforms
                                      ▼
Kafka Message Format:
┌─────────────────────────────────────────────────────────────────────────────┐
│ Topic: crypto-raw                                                            │
│ Key: bitcoin                                                                 │
│ Value:                                                                       │
│ {                                                                            │
│   "id": "bitcoin",                                                           │
│   "symbol": "BTC",                                                           │
│   "name": "Bitcoin",                                                         │
│   "current_price": 98500.0,                                                  │
│   "market_cap": 1950000000000,                                               │
│   "market_cap_rank": 1,                                                      │
│   "volume_24h": 45000000000,                                                 │
│   "high_24h": 99500.0,                                                       │
│   "low_24h": 96500.0,                                                        │
│   "price_change_pct_24h": 1.55,                                              │
│   "circulating_supply": 19500000,                                            │
│   "timestamp": "2025-12-15T10:30:00Z",                                       │
│   "crawl_timestamp": "2025-12-15T10:30:05Z"                                  │
│ }                                                                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Stage 2: Storage Layer

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGE 2: STORAGE (HDFS/GCS)                                                 │
└─────────────────────────────────────────────────────────────────────────────┘

Directory Structure:
┌─────────────────────────────────────────────────────────────────────────────┐
│ /data/                                                                       │
│ ├── raw/                                                                     │
│ │   ├── dt=2025-11-01/                                                       │
│ │   │   ├── hr=00/                                                           │
│ │   │   │   └── crypto_00_001.jsonl                                          │
│ │   │   ├── hr=01/                                                           │
│ │   │   │   └── crypto_01_001.jsonl                                          │
│ │   │   └── ... (24 hours)                                                   │
│ │   ├── dt=2025-11-02/                                                       │
│ │   └── ... (61 days: Nov-Dec 2025)                                          │
│ │                                                                            │
│ │   Total: 336 files, 2,016,000 records                                      │
│ │                                                                            │
│ ├── clean/                                                                   │
│ │   ├── dt=2025-11-01/                                                       │
│ │   │   └── part-00000.parquet                                               │
│ │   └── ... (61 days)                                                        │
│ │                                                                            │
│ │   Total: 170 files, 1,010,980 records                                      │
│ │                                                                            │
│ └── aggregated/                                                              │
│     ├── daily_price_stats/                                                   │
│     ├── weekly_metrics/                                                      │
│     ├── monthly_metrics/                                                     │
│     ├── hourly_volume/                                                       │
│     ├── pump_dump_alerts/                                                    │
│     ├── btc_dominance/                                                       │
│     ├── whale_detection/                                                     │
│     ├── market_sentiment/                                                    │
│     ├── top_movers/                                                          │
│     ├── price_heatmap/                                                       │
│     ├── market_cap_distribution/                                             │
│     ├── btc_correlation/                                                     │
│     ├── rank_changes/                                                        │
│     └── top_coin_trends/                                                     │
└─────────────────────────────────────────────────────────────────────────────┘

Raw File Format (JSONL):
┌─────────────────────────────────────────────────────────────────────────────┐
│ /data/raw/dt=2025-12-15/hr=10/crypto_10_001.jsonl                            │
│                                                                              │
│ {"id":"bitcoin","symbol":"BTC","current_price":98500.0,...}                  │
│ {"id":"ethereum","symbol":"ETH","current_price":3850.0,...}                  │
│ {"id":"solana","symbol":"SOL","current_price":220.0,...}                     │
│ ... (100 records per file)                                                   │
└─────────────────────────────────────────────────────────────────────────────┘

Clean File Format (Parquet):
┌─────────────────────────────────────────────────────────────────────────────┐
│ /data/clean/dt=2025-12-15/part-00000.parquet                                 │
│                                                                              │
│ Schema:                                                                      │
│ ├── id: string                                                               │
│ ├── symbol: string                                                           │
│ ├── name: string                                                             │
│ ├── current_price: double                                                    │
│ ├── market_cap: long                                                         │
│ ├── market_cap_rank: int                                                     │
│ ├── volume_24h: double                                                       │
│ ├── price_change_pct_24h: double                                             │
│ ├── timestamp: timestamp                                                     │
│ └── dt: string (partition column)                                            │
│                                                                              │
│ Compression: Snappy                                                          │
│ Row groups: ~10,000 rows each                                                │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Stage 3: Batch Processing

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGE 3: SPARK BATCH PROCESSING                                             │
└─────────────────────────────────────────────────────────────────────────────┘

Job: daily_price_stats
┌─────────────────────────────────────────────────────────────────────────────┐
│ Input: /data/clean/dt=2025-12-15/*.parquet                                   │
│                                                                              │
│ Transformations:                                                             │
│ 1. Group by coin_id, date                                                    │
│ 2. Calculate OHLC (Open, High, Low, Close)                                   │
│ 3. Sum volume                                                                │
│ 4. Calculate price change %                                                  │
│                                                                              │
│ SQL:                                                                         │
│ SELECT                                                                       │
│   coin_id,                                                                   │
│   symbol,                                                                    │
│   dt as date,                                                                │
│   FIRST(current_price) as open,                                              │
│   MAX(current_price) as high,                                                │
│   MIN(current_price) as low,                                                 │
│   LAST(current_price) as close,                                              │
│   SUM(volume_24h) as volume,                                                 │
│   ((LAST(price) - FIRST(price)) / FIRST(price) * 100) as change_pct         │
│ FROM clean_data                                                              │
│ GROUP BY coin_id, symbol, dt                                                 │
│                                                                              │
│ Output: /data/aggregated/daily_price_stats/dt=2025-12-15/                    │
└─────────────────────────────────────────────────────────────────────────────┘

Job: pump_dump_detection
┌─────────────────────────────────────────────────────────────────────────────┐
│ Input: /data/clean/dt=2025-12-15/*.parquet                                   │
│                                                                              │
│ Logic:                                                                       │
│ - PUMP: price_change > 10% AND volume_spike > 200%                           │
│ - DUMP: price_change < -10% AND volume_spike > 200%                          │
│                                                                              │
│ Output Schema:                                                               │
│ {                                                                            │
│   "coin_id": "dogecoin",                                                     │
│   "symbol": "DOGE",                                                          │
│   "alert_type": "PUMP",                                                      │
│   "price_change_pct": 25.5,                                                  │
│   "volume_change_pct": 350.0,                                                │
│   "severity": "HIGH",                                                        │
│   "detected_at": "2025-12-15T08:30:00Z"                                      │
│ }                                                                            │
│                                                                              │
│ Output: /data/aggregated/pump_dump_alerts/                                   │
│ Total alerts generated: 336,144                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Stage 4: Serving Layer

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGE 4: ELASTICSEARCH INDEXING                                             │
└─────────────────────────────────────────────────────────────────────────────┘

Index: crypto_latest
┌─────────────────────────────────────────────────────────────────────────────┐
│ Mapping:                                                                     │
│ {                                                                            │
│   "mappings": {                                                              │
│     "properties": {                                                          │
│       "id": { "type": "keyword" },                                           │
│       "symbol": { "type": "keyword" },                                       │
│       "name": { "type": "text" },                                            │
│       "current_price": { "type": "float" },                                  │
│       "market_cap": { "type": "long" },                                      │
│       "market_cap_rank": { "type": "integer" },                              │
│       "volume_24h": { "type": "float" },                                     │
│       "price_change_pct_24h": { "type": "float" },                           │
│       "timestamp": { "type": "date" }                                        │
│     }                                                                        │
│   }                                                                          │
│ }                                                                            │
│                                                                              │
│ Documents: 98 (one per coin)                                                 │
│ Update frequency: Every 5 minutes                                            │
└─────────────────────────────────────────────────────────────────────────────┘

Index: alerts
┌─────────────────────────────────────────────────────────────────────────────┐
│ Mapping:                                                                     │
│ {                                                                            │
│   "mappings": {                                                              │
│     "properties": {                                                          │
│       "coin_id": { "type": "keyword" },                                      │
│       "symbol": { "type": "keyword" },                                       │
│       "alert_type": { "type": "keyword" },                                   │
│       "severity": { "type": "keyword" },                                     │
│       "price_change_pct": { "type": "float" },                               │
│       "volume_change_pct": { "type": "float" },                              │
│       "detected_at": { "type": "date" }                                      │
│     }                                                                        │
│   }                                                                          │
│ }                                                                            │
│                                                                              │
│ Documents: 336,144                                                           │
│ Retention: 90 days                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Data Volume Summary

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DATA VOLUME SUMMARY                                   │
└─────────────────────────────────────────────────────────────────────────────┘

┌────────────────────┬─────────────────┬─────────────────┬────────────────────┐
│      Stage         │    Records      │     Files       │      Size          │
├────────────────────┼─────────────────┼─────────────────┼────────────────────┤
│ Raw (JSONL)        │   2,016,000     │      336        │     ~500 MB        │
│ Clean (Parquet)    │   1,010,980     │      170        │     ~150 MB        │
│ Aggregated         │     varies      │      14 dirs    │     ~100 MB        │
│ ES: crypto_latest  │          98     │        1        │       ~1 MB        │
│ ES: crypto_history │         769     │        1        │       ~5 MB        │
│ ES: alerts         │     336,144     │        1        │      ~50 MB        │
├────────────────────┼─────────────────┼─────────────────┼────────────────────┤
│ TOTAL              │   3,364,000+    │      523+       │     ~800 MB        │
└────────────────────┴─────────────────┴─────────────────┴────────────────────┘

Data Growth Rate:
• Raw data: ~33,000 records/day (100 coins × 24 hours × ~14 samples/hour)
• Clean data: ~16,500 records/day (after deduplication)
• Alerts: ~5,500 alerts/day (average)
```

---

## 4. Data Freshness

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DATA FRESHNESS                                        │
└─────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────┬─────────────────────┬──────────────────────────┐
│        Component           │    Update Freq      │        Latency           │
├────────────────────────────┼─────────────────────┼──────────────────────────┤
│ Crawler → Kafka            │    Every 1 min      │        < 5 sec           │
│ Kafka → HDFS               │    Real-time        │        < 10 sec          │
│ HDFS → Spark (Batch)       │    Hourly           │        < 5 min           │
│ Spark → Elasticsearch      │    Hourly           │        < 2 min           │
│ ES → Web Dashboard         │    On request       │        < 1 sec           │
│ Spark Streaming            │    Real-time        │        < 30 sec          │
└────────────────────────────┴─────────────────────┴──────────────────────────┘
```
