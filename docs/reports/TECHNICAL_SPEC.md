# Technical Specification Document
## Crypto Analytics Pipeline

---

**Version:** 1.0
**Last Updated:** 15/12/2025
**Status:** Approved

---

## 1. System Overview

### 1.1 Purpose

Tài liệu này mô tả chi tiết kỹ thuật của hệ thống Crypto Analytics Pipeline, bao gồm các thành phần, giao thức, API, và data schema.

### 1.2 Scope

- Data collection từ CoinGecko API
- Message streaming với Apache Kafka
- Distributed storage với HDFS/GCS
- Batch processing với Apache Spark
- Search indexing với Elasticsearch
- REST API với FastAPI
- Web Dashboard

---

## 2. Component Specifications

### 2.1 Crawler Service

#### 2.1.1 Configuration

```python
# crawl/config.py
COINGECKO_API_URL = "https://api.coingecko.com/api/v3"
COINS_ENDPOINT = "/coins/markets"

# Request parameters
PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 100,
    "page": 1,
    "sparkline": False
}

# Rate limiting
REQUEST_INTERVAL = 60  # seconds
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
```

#### 2.1.2 Data Schema (CoinGecko Response)

```json
{
  "id": "string",
  "symbol": "string",
  "name": "string",
  "image": "string (URL)",
  "current_price": "number",
  "market_cap": "number",
  "market_cap_rank": "integer",
  "fully_diluted_valuation": "number | null",
  "total_volume": "number",
  "high_24h": "number",
  "low_24h": "number",
  "price_change_24h": "number",
  "price_change_percentage_24h": "number",
  "market_cap_change_24h": "number",
  "market_cap_change_percentage_24h": "number",
  "circulating_supply": "number",
  "total_supply": "number | null",
  "max_supply": "number | null",
  "ath": "number",
  "ath_change_percentage": "number",
  "ath_date": "string (ISO 8601)",
  "atl": "number",
  "atl_change_percentage": "number",
  "atl_date": "string (ISO 8601)",
  "roi": "object | null",
  "last_updated": "string (ISO 8601)"
}
```

### 2.2 Kafka Cluster

#### 2.2.1 Cluster Configuration

```yaml
# Cluster specs
brokers: 3
replication_factor: 3
min_insync_replicas: 2

# Broker ports
broker_1: 19092
broker_2: 19093
broker_3: 19094

# Zookeeper
zookeeper_port: 2181
```

#### 2.2.2 Topic Specifications

| Topic | Partitions | Replication | Retention | Cleanup Policy |
|-------|------------|-------------|-----------|----------------|
| crypto-raw | 3 | 3 | 7 days | delete |
| clean_crypto | 3 | 3 | 30 days | delete |
| alerts | 1 | 3 | 90 days | delete |

#### 2.2.3 Message Schema (crypto-raw)

```json
{
  "id": "string",
  "symbol": "string",
  "name": "string",
  "current_price": "number",
  "market_cap": "number",
  "market_cap_rank": "integer",
  "volume_24h": "number",
  "high_24h": "number",
  "low_24h": "number",
  "price_change_pct_24h": "number",
  "circulating_supply": "number",
  "total_supply": "number | null",
  "timestamp": "string (ISO 8601)",
  "crawl_timestamp": "string (ISO 8601)"
}
```

### 2.3 HDFS Storage

#### 2.3.1 Directory Structure

```
hdfs://namenode:9000/
└── data/
    ├── raw/
    │   └── dt=YYYY-MM-DD/
    │       └── hr=HH/
    │           └── crypto_{hr}_{batch}.jsonl
    ├── clean/
    │   └── dt=YYYY-MM-DD/
    │       └── part-{id}.parquet
    └── aggregated/
        ├── daily_price_stats/
        ├── weekly_metrics/
        ├── monthly_metrics/
        ├── hourly_volume/
        ├── pump_dump_alerts/
        ├── btc_dominance/
        ├── whale_detection/
        ├── market_sentiment/
        ├── top_movers/
        ├── price_heatmap/
        ├── market_cap_distribution/
        ├── btc_correlation/
        ├── rank_changes/
        └── top_coin_trends/
```

#### 2.3.2 File Formats

| Layer | Format | Compression | Partitioning |
|-------|--------|-------------|--------------|
| Raw | JSONL | None | dt, hr |
| Clean | Parquet | Snappy | dt |
| Aggregated | Parquet | Snappy | varies |

### 2.4 Spark Processing

#### 2.4.1 Spark Configuration

```python
# spark/config.py
SPARK_CONFIG = {
    "spark.app.name": "CryptoAnalytics",
    "spark.master": "spark://spark-master:7077",
    "spark.executor.memory": "2g",
    "spark.executor.cores": "2",
    "spark.driver.memory": "1g",
    "spark.sql.shuffle.partitions": "10",
    "spark.sql.parquet.compression.codec": "snappy"
}
```

#### 2.4.2 Job Specifications

**Job: data_cleaning**
```python
Input: /data/raw/dt=*/*.jsonl
Output: /data/clean/dt=*/*.parquet

Transformations:
1. Remove duplicates (window: 1 hour, key: id+timestamp)
2. Handle null values (fill with previous value)
3. Validate data types
4. Normalize timestamps to UTC
5. Filter invalid prices (price <= 0)
```

**Job: daily_aggregation**
```python
Input: /data/clean/dt=*/*.parquet
Output: /data/aggregated/daily_price_stats/

Aggregations:
- open: first(current_price) ORDER BY timestamp
- high: max(current_price)
- low: min(current_price)
- close: last(current_price) ORDER BY timestamp
- volume: sum(volume_24h)
- price_change: (close - open) / open * 100
```

**Job: pump_dump_detection**
```python
Thresholds:
- PUMP: price_change > 10% AND volume_spike > 200%
- DUMP: price_change < -10% AND volume_spike > 200%

Severity levels:
- LOW: 10-20% change
- MEDIUM: 20-50% change
- HIGH: >50% change
```

### 2.5 Elasticsearch

#### 2.5.1 Cluster Configuration

```yaml
cluster.name: crypto-analytics
node.name: es-node-1
network.host: 0.0.0.0
http.port: 9200

# Memory
ES_JAVA_OPTS: "-Xms512m -Xmx512m"

# Index settings
index.number_of_shards: 1
index.number_of_replicas: 1
```

#### 2.5.2 Index Mappings

**Index: crypto_latest**
```json
{
  "mappings": {
    "properties": {
      "id": { "type": "keyword" },
      "symbol": { "type": "keyword" },
      "name": {
        "type": "text",
        "fields": {
          "keyword": { "type": "keyword" }
        }
      },
      "current_price": { "type": "float" },
      "market_cap": { "type": "long" },
      "market_cap_rank": { "type": "integer" },
      "volume_24h": { "type": "float" },
      "price_change_pct_24h": { "type": "float" },
      "high_24h": { "type": "float" },
      "low_24h": { "type": "float" },
      "circulating_supply": { "type": "float" },
      "timestamp": { "type": "date" }
    }
  },
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 1,
    "refresh_interval": "5s"
  }
}
```

**Index: alerts**
```json
{
  "mappings": {
    "properties": {
      "coin_id": { "type": "keyword" },
      "symbol": { "type": "keyword" },
      "alert_type": { "type": "keyword" },
      "severity": { "type": "keyword" },
      "price_change_pct": { "type": "float" },
      "volume_change_pct": { "type": "float" },
      "detected_at": { "type": "date" },
      "details": { "type": "object" }
    }
  }
}
```

### 2.6 REST API

#### 2.6.1 Endpoints Specification

| Method | Endpoint | Description | Response |
|--------|----------|-------------|----------|
| GET | /health | Health check | 200 OK |
| GET | /api/v1/crypto/latest | All coins latest | Array |
| GET | /api/v1/crypto/{id} | Single coin | Object |
| GET | /api/v1/crypto/{id}/history | Price history | Array |
| GET | /api/v1/analytics/daily | Daily metrics | Object |
| GET | /api/v1/analytics/weekly | Weekly metrics | Object |
| GET | /api/v1/analytics/monthly | Monthly metrics | Object |
| GET | /api/v1/analytics/top-movers | Top gainers/losers | Array |
| GET | /api/v1/analytics/pump-dump | Alerts | Array |
| GET | /api/v1/market/overview | Market summary | Object |

#### 2.6.2 Response Format

```json
{
  "success": true,
  "data": { ... },
  "meta": {
    "timestamp": "2025-12-15T10:30:00Z",
    "total": 100,
    "page": 1,
    "limit": 20
  }
}
```

#### 2.6.3 Error Response

```json
{
  "success": false,
  "error": {
    "code": "NOT_FOUND",
    "message": "Coin with id 'invalid' not found"
  }
}
```

### 2.7 PostgreSQL Schema

#### 2.7.1 Tables

```sql
-- Daily metrics table
CREATE TABLE daily_metrics (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open_price DECIMAL(20, 8),
    high_price DECIMAL(20, 8),
    low_price DECIMAL(20, 8),
    close_price DECIMAL(20, 8),
    volume DECIMAL(30, 2),
    market_cap DECIMAL(30, 2),
    price_change_pct DECIMAL(10, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uk_daily_coin_date UNIQUE (coin_id, date)
);

CREATE INDEX idx_daily_date ON daily_metrics(date);
CREATE INDEX idx_daily_symbol ON daily_metrics(symbol);

-- Weekly metrics table
CREATE TABLE weekly_metrics (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    week_start DATE NOT NULL,
    week_end DATE NOT NULL,
    avg_price DECIMAL(20, 8),
    max_price DECIMAL(20, 8),
    min_price DECIMAL(20, 8),
    total_volume DECIMAL(30, 2),
    volatility DECIMAL(10, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Monthly metrics table
CREATE TABLE monthly_metrics (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    month VARCHAR(7) NOT NULL,
    avg_price DECIMAL(20, 8),
    max_price DECIMAL(20, 8),
    min_price DECIMAL(20, 8),
    price_change_pct DECIMAL(10, 4),
    avg_daily_volume DECIMAL(30, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alerts table
CREATE TABLE alerts (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    alert_type VARCHAR(20) NOT NULL,
    severity VARCHAR(10) NOT NULL,
    price_change_pct DECIMAL(10, 4),
    volume_change_pct DECIMAL(10, 4),
    detected_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_alerts_type ON alerts(alert_type);
CREATE INDEX idx_alerts_detected ON alerts(detected_at);
CREATE INDEX idx_alerts_coin ON alerts(coin_id);
```

---

## 3. Infrastructure Specifications

### 3.1 Docker Containers

| Service | Image | Resources |
|---------|-------|-----------|
| zookeeper | confluentinc/cp-zookeeper:7.5.0 | 512MB RAM |
| kafka | confluentinc/cp-kafka:7.5.0 | 1GB RAM |
| spark-master | bitnami/spark:3.5 | 1GB RAM |
| elasticsearch | elasticsearch:8.13.2 | 2GB RAM |
| kibana | kibana:8.13.2 | 512MB RAM |
| postgres | postgres:15 | 512MB RAM |
| webapp | python:3.11-slim | 256MB RAM |

### 3.2 Kubernetes Resources

```yaml
# Resource quotas
apiVersion: v1
kind: ResourceQuota
metadata:
  name: crypto-analytics-quota
spec:
  hard:
    requests.cpu: "8"
    requests.memory: "16Gi"
    limits.cpu: "16"
    limits.memory: "32Gi"
    pods: "20"
```

### 3.3 Network Configuration

| Service | Internal Port | External Port |
|---------|---------------|---------------|
| Kafka 1 | 9092 | 19092 |
| Kafka 2 | 9092 | 19093 |
| Kafka 3 | 9092 | 19094 |
| Zookeeper | 2181 | 2181 |
| HDFS NameNode | 9870 | 9870 |
| Spark Master | 8080 | 8080 |
| Elasticsearch | 9200 | 9200 |
| Kibana | 5601 | 5601 |
| PostgreSQL | 5432 | 5432 |
| API Server | 8000 | 8000 |
| Web Dashboard | 3000 | 3000 |

---

## 4. Security Specifications

### 4.1 Authentication

| Component | Method | Status |
|-----------|--------|--------|
| Kafka | SASL/PLAIN | Disabled (dev) |
| Elasticsearch | Basic Auth | Disabled (dev) |
| PostgreSQL | Password | Enabled |
| API | API Key | Planned v1.1 |

### 4.2 Network Security

```yaml
# Kubernetes NetworkPolicy
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: crypto-analytics-policy
spec:
  podSelector:
    matchLabels:
      app: crypto-analytics
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: crypto-analytics
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              name: crypto-analytics
```

---

## 5. Monitoring & Logging

### 5.1 Metrics

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| kafka_messages_per_sec | Kafka | < 10 (warning) |
| spark_job_duration | Spark | > 10 min (warning) |
| es_query_latency_ms | ES | > 1000ms (warning) |
| api_response_time_ms | API | > 500ms (warning) |
| error_rate_percent | All | > 5% (critical) |

### 5.2 Logging Format

```json
{
  "timestamp": "2025-12-15T10:30:00.000Z",
  "level": "INFO",
  "service": "crawler",
  "message": "Successfully fetched 100 coins",
  "metadata": {
    "duration_ms": 1234,
    "record_count": 100
  }
}
```

---

## 6. Appendix

### A. Environment Variables

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092,kafka3:9092
KAFKA_TOPIC_RAW=crypto-raw
KAFKA_TOPIC_CLEAN=clean_crypto
KAFKA_TOPIC_ALERTS=alerts

# HDFS
HDFS_NAMENODE=hdfs://namenode:9000
HDFS_RAW_PATH=/data/raw
HDFS_CLEAN_PATH=/data/clean
HDFS_AGG_PATH=/data/aggregated

# Elasticsearch
ES_HOST=elasticsearch
ES_PORT=9200
ES_INDEX_LATEST=crypto_latest
ES_INDEX_HISTORY=crypto_history
ES_INDEX_ALERTS=alerts

# PostgreSQL
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=crypto_analytics
POSTGRES_USER=admin
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}

# API
API_HOST=0.0.0.0
API_PORT=8000
API_DEBUG=false
```

### B. Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-12-15 | Initial specification |

---

**End of Technical Specification**
