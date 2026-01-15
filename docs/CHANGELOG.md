# Changelog

Tất cả các thay đổi quan trọng của dự án sẽ được ghi lại trong file này.

Định dạng dựa trên [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
và dự án tuân theo [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.0.0] - 2025-12-15

### Added
- **Crawler Module**
  - Thu thập dữ liệu real-time từ CoinGecko API
  - Hỗ trợ streaming 24/7 với `crypto_crawler_streaming.py`
  - Fake data generator cho testing (2 tháng: Nov-Dec 2025)

- **Kafka Cluster**
  - Setup 3-broker cluster với Zookeeper
  - 3 topics: `crypto-raw`, `clean_crypto`, `alerts`
  - Docker Compose configuration

- **HDFS Storage**
  - Kafka consumer ghi dữ liệu vào HDFS
  - Phân vùng theo ngày/giờ (`dt=YYYY-MM-DD/hr=HH`)
  - Hỗ trợ Google Cloud Storage cho cloud deployment

- **Spark Batch Processing**
  - Data cleaning job (1,010,980 rows processed)
  - 14 analytics jobs:
    - Daily/Weekly/Monthly price statistics
    - Pump/Dump detection và alerts
    - BTC dominance tracking
    - Whale detection
    - Market sentiment analysis
    - Top coin trends

- **Elasticsearch Integration**
  - 3 indices: `crypto_latest`, `crypto_history`, `alerts`
  - 15+ query functions trong `elasticsearch_queries.py`
  - FastAPI REST server

- **Kibana Dashboard**
  - Hướng dẫn tạo visualizations chi tiết
  - Pre-built dashboard templates

- **Web Dashboard**
  - Modern UI với Chart.js
  - Dark/Light theme support
  - Responsive design
  - 3 timeframe views: Daily, Weekly, Monthly

- **Kubernetes Deployment**
  - Manifests cho tất cả services
  - Hỗ trợ Azure AKS và Google GKE
  - Student mode deployment (tiết kiệm 93% chi phí)

### Infrastructure
- Docker Compose cho local development
- Multi-stage Dockerfiles cho production
- Environment-based configuration

---

## [0.9.0] - 2025-12-01

### Added
- Initial project structure
- Basic Kafka setup
- Crawler prototype

### Changed
- Refactor crawler để sử dụng streaming mode

---

## [0.8.0] - 2025-11-15

### Added
- Spark job templates
- HDFS integration
- Basic data cleaning

### Fixed
- Kafka connection timeout issues
- HDFS write permissions

---

## [0.7.0] - 2025-11-01

### Added
- Elasticsearch setup
- Query API skeleton
- Kibana configuration

---

## [0.6.0] - 2025-10-15

### Added
- Web dashboard prototype
- Chart.js integration
- API client service

### Changed
- Refactor frontend thành modular components

---

## [0.5.0] - 2025-10-01

### Added
- Kubernetes manifests
- Namespace configuration
- Service deployments

---

## Roadmap

### Planned for v1.1.0
- [ ] Real-time alerting system
- [ ] WebSocket support cho live updates
- [ ] Mobile responsive improvements
- [ ] Additional crypto exchanges support

### Planned for v1.2.0
- [ ] Machine Learning price prediction
- [ ] Portfolio tracking feature
- [ ] Email/Telegram notifications
- [ ] Multi-language support

---

## Contributors

- Team Lead - Architecture & Spark Jobs
- Developer 1 - Crawler & Kafka
- Developer 2 - Elasticsearch & API
- Developer 3 - Frontend & Dashboard
- Developer 4 - Kubernetes & DevOps

---

[1.0.0]: https://github.com/username/crypto-analytics-pipeline/releases/tag/v1.0.0
[0.9.0]: https://github.com/username/crypto-analytics-pipeline/releases/tag/v0.9.0
[0.8.0]: https://github.com/username/crypto-analytics-pipeline/releases/tag/v0.8.0
[0.7.0]: https://github.com/username/crypto-analytics-pipeline/releases/tag/v0.7.0
[0.6.0]: https://github.com/username/crypto-analytics-pipeline/releases/tag/v0.6.0
[0.5.0]: https://github.com/username/crypto-analytics-pipeline/releases/tag/v0.5.0
