# Sơ đồ Kiến trúc Hệ thống Crypto Analytics

Dưới đây là sơ đồ luồng dữ liệu của hệ thống, bao gồm Streaming và Batch processing layers.

```mermaid
graph TD
    %% Define Styles
    classDef kafka fill:#ff9,stroke:#333,stroke-width:2px;
    classDef k8s fill:#326ce5,stroke:#333,stroke-width:2px,color:white;
    classDef storage fill:#f9f,stroke:#333,stroke-width:2px;
    classDef db fill:#eee,stroke:#333,stroke-width:2px;
    classDef ext fill:#fff,stroke:#333,stroke-width:4px;

    subgraph "Data Source"
        P1[Crawler API]
    end

    subgraph "Ingestion Layer"
        K1((Kafka: crypto-raw)):::kafka
        D1[Deployment: crawler-streaming]:::k8s
    end

    subgraph "Streaming / Speed Layer"
        S1[Spark Streaming Driver<br>streaming_processing.py]:::k8s
        K2((Kafka: alerts)):::kafka
        K3((Kafka: market_sentiment)):::kafka
        K4((Kafka: clean_crypto)):::kafka
        C1[Deployment: Realtime Consumer<br>streaming_realtime_consumer.py]:::k8s
        ES1[(Elasticsearch<br>Realtime Indices)]:::db
        KIB1[Kibana Dashboard]:::ext
    end

    subgraph "Batch / Storage Layer"
        GCS1[(Google Cloud Storage<br>Raw Data)]:::storage
        GCS2[(Google Cloud Storage<br>Clean Data)]:::storage
        GCS3[(Google Cloud Storage<br>Aggregated Metrics)]:::storage
        
        B1[Deployment: Kafka to GCS<br>kafka_to_gcs.py]:::k8s
        
        CJ1[CronJob 1h: Data Cleaning<br>data_cleaning.py]:::k8s
        CJ2[CronJob 1AM: Daily Aggregation<br>daily_aggregation.py]:::k8s
        CJ3[CronJob 2AM: Export Metrics<br>export_metrics.py]:::k8s
        
        PG[(PostgreSQL)]:::db
        ES2[(Elasticsearch<br>Batch Indices)]:::db
        API[Web API]:::ext
        WEB[Web App]:::ext
    end

    %% Flow Connections - Ingestion
    P1 --> D1
    D1 -->|Push Data| K1

    %% Flow Connections - Streaming
    K1 -->|Read Stream| S1
    S1 -->|Write Clean| K4
    S1 -->|Calc Alerts| K2
    S1 -->|Calc Sentiment| K3
    K2 --> C1
    K3 --> C1
    C1 -->|Index Docs| ES1
    ES1 --> KIB1

    %% Flow Connections - Batch
    K1 -->|Consumer Group| B1
    B1 -->|Write Raw JSONL| GCS1
    
    GCS1 -->|Read Raw| CJ1
    CJ1 -->|Write Parquet| GCS2
    
    GCS2 -->|Read Clean| CJ2
    CJ2 -->|Agg Stats| GCS3
    
    GCS3 -->|Read Daily/Weekly/Monthly| CJ3
    
    %% Export
    CJ3 -->|Write Metrics| PG
    CJ3 -->|Write Metrics| ES2
    
    PG --> API
    API --> WEB
    ES2 --> KIB1
```

## Chi tiết các thành phần

### 1. Ingestion (Đầu vào)
*   **Crawler**: Chạy dưới dạng Deployment (`crawler-streaming`), liên tục lấy dữ liệu coin và đẩy vào Kafka topic `crypto-raw`.

### 2. Streaming Flow (Xử lý thời gian thực)
*   **Spark Structured Streaming**: 
    *   Deployment: `spark-streaming` chạy file `streaming_processing.py`.
    *   Logic: Đọc `crypto-raw`, tính toán pump/dump (Alerts) và xu hướng thị trường (Sentiment).
    *   Output: Đẩy ra Kafka topics `alerts`, `market_sentiment`, và `clean_crypto`.
*   **Realtime Consumer**:
    *   Deployment: `kafka-to-es-consumer` chạy file `streaming_realtime_consumer.py`.
    *   Logic: Đọc `alerts` và `market_sentiment` từ Kafka và ghi ngay lập tức vào **Elasticsearch**.
*   **Kibana**: Visualize dữ liệu realtime từ Elasticsearch.

### 3. Batch Flow (Xử lý định kỳ - Lưu trữ lâu dài)
*   **Kafka to GCS**:
    *   Deployment: `kafka-to-gcs` chạy file `hdfs/kafka_to_gcs.py` (chạy liên tục, không phải cron).
    *   Nhiệm vụ: Sync dữ liệu từ Kafka `crypto-raw` lưu trữ dạng file `.jsonl` phân vùng theo ngày/giờ trên **Google Cloud Storage (GCS)**.
*   **Data Cleaning (1 tiếng/lần)**:
    *   CronJob: `spark-clean-gcs-cronjob` chạy file `data_cleaning.py`.
    *   Nhiệm vụ: Đọc Raw GCS -> Làm sạch -> Ghi Clean Parquet GCS.
*   **Daily Aggregation (1h Sáng)**:
    *   CronJob: `spark-agg-gcs-cronjob` chạy file `daily_aggregation.py`.
    *   Nhiệm vụ: Thống kê dữ liệu (Daily, Weekly, Monthly) từ Clean GCS -> Ghi lại vào GCS (folder `aggregated`).
*   **Export Metrics (2h Sáng)**:
    *   CronJob: `spark-export-gcs-cronjob` chạy file `export_metrics.py`.
    *   Nhiệm vụ 1: Ghi vào **PostgreSQL** (Phục vụ Web API hiển thị báo cáo).
    *   Nhiệm vụ 2: Ghi vào **Elasticsearch** (Topic DAILY, WEEKLY, MONTHLY) để vẽ biểu đồ lịch sử trên Kibana.
