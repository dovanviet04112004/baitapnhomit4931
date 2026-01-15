# 🚀 Crypto Analytics Pipeline

> Real-time cryptocurrency price analysis system using Big Data technologies on Kubernetes

[![Platform](https://img.shields.io/badge/Platform-Google%20Kubernetes%20Engine-blue)](https://cloud.google.com/kubernetes-engine)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.4.1-orange)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.x-black)](https://kafka.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**Đồ án tốt nghiệp** - Hệ thống phân tích giá cryptocurrency theo thời gian thực sử dụng Lambda Architecture trên Google Kubernetes Engine.

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Features](#-features)
- [Tech Stack](#-tech-stack)
- [Quick Start](#-quick-start)
- [Deployment](#-deployment)
- [Usage](#-usage)
- [Documentation](#-documentation)
- [Performance](#-performance)
- [Contributing](#-contributing)

---

## 🎯 Overview

### What is this?

A production-ready Big Data pipeline that:
- 📊 Collects real-time cryptocurrency prices from CoinGecko API
- ⚡ Processes data using Apache Spark (Streaming + Batch)
- 🔍 Indexes data into Elasticsearch for fast queries
- 📈 Visualizes metrics through Kibana dashboards
- 🌐 Exposes REST API for web applications
- ☸️ Runs on Kubernetes for scalability and reliability

### Use Cases

- **Real-time Alerts:** Detect pump/dump events (>5% price change in 1 hour)
- **Market Analysis:** Track BTC dominance, market sentiment, whale activities
- **Historical Insights:** Daily/weekly/monthly aggregations
- **Portfolio Tracking:** Monitor your favorite coins
- **Research:** Analyze crypto market trends and correlations

---

## 🏗️ Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    GOOGLE KUBERNETES ENGINE                          │
│                    Namespace: crypto-pipeline                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  CoinGecko API → Crawler → Kafka → Spark → Storage → Visualization │
│                                                                      │
│  ┌──────────────┐   ┌──────────┐   ┌──────────┐                    │
│  │   Crawler    │──▶│  Kafka   │──▶│  Spark   │                    │
│  │ (Deployment) │   │(StatefulSet)│ │(Streaming)│                   │
│  └──────────────┘   └──────────┘   │  +Batch  │                    │
│                                     └────┬─────┘                    │
│                                          │                          │
│                         ┌────────────────┴────────────────┐         │
│                         ▼                                 ▼         │
│                  ┌─────────────┐                  ┌──────────────┐  │
│                  │ PostgreSQL  │                  │Elasticsearch │  │
│                  │(StatefulSet)│                  │(StatefulSet) │  │
│                  └──────┬──────┘                  └──────┬───────┘  │
│                         │                                │          │
│                         │                                │          │
│                         ▼                                ▼          │
│                  ┌────────────┐                   ┌────────────┐    │
│                  │Web Dashboard│                  │   Kibana   │    │
│                  │(LoadBalancer)                  │(LoadBalancer)   │
│                  └────────────┘                   └────────────┘    │
│                                                                      │
│  Data Flow:                                                         │
│  • PostgreSQL ← Spark Export (Daily metrics)                        │
│  • Elasticsearch ← Kafka Consumer (Real-time data)                  │
│  • Web Dashboard ← PostgreSQL (Structured queries)                  │
│  • Kibana ← Elasticsearch (Time-series analysis)                    │
└─────────────────────────────────────────────────────────────────────┘
```

### Lambda Architecture

**Speed Layer (Real-time):**
- Spark Streaming processes data from Kafka
- Latency: < 2 minutes
- Output: Real-time alerts, market sentiment

**Batch Layer (Historical):**
- Spark Batch jobs run hourly/daily
- Recompute metrics for accuracy
- Output: Daily/weekly/monthly aggregations

**Serving Layer:**
- PostgreSQL: Structured metrics
- Elasticsearch: Time-series data, full-text search
- Kibana + Web Dashboard: Visualization

📖 **Detailed Architecture:** See [DEPLOYMENT_ARCHITECTURE.md](docs/DEPLOYMENT_ARCHITECTURE.md)

---

## ✨ Features

### Real-time Processing
- ⚡ **Streaming Analytics:** Process 100+ messages/minute
- 🚨 **Price Alerts:** Detect pump (>5%) and dump (<-5%) events
- 📊 **Market Sentiment:** Calculate % bullish/bearish coins
- 🐋 **Whale Detection:** Identify volume spikes >200%

### Batch Processing
- 📈 **Daily Metrics:** OHLC prices, volume, market cap
- 🔝 **Top Movers:** Top 10 gainers/losers
- 💰 **BTC Dominance:** Bitcoin market share tracking
- 🔗 **Correlation Analysis:** BTC vs Altcoins correlation
- 📊 **14 Analytics Jobs:** Comprehensive market analysis

### Data Storage
- ☁️ **Google Cloud Storage:** Durable, scalable object storage
- 🗄️ **PostgreSQL:** ACID-compliant relational database
- 🔍 **Elasticsearch:** Fast full-text search and aggregations

### Visualization
- 📊 **Kibana Dashboards:** Interactive charts and graphs
- 🌐 **Web Dashboard:** Custom FastAPI + HTML/CSS/JS frontend
- 📱 **REST API:** RESTful endpoints for integration

---

## 🛠️ Tech Stack

### Core Technologies

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Orchestration** | Kubernetes (GKE) | 1.28+ | Container orchestration |
| **Message Queue** | Apache Kafka | 3.x | Event streaming |
| **Stream Processing** | Apache Spark Streaming | 3.4.1 | Real-time analytics |
| **Batch Processing** | Apache Spark | 3.4.1 | Historical analytics |
| **Object Storage** | Google Cloud Storage | - | Data lake |
| **Database** | PostgreSQL | 15 | Structured data |
| **Search Engine** | Elasticsearch | 8.13.2 | Time-series data |
| **Visualization** | Kibana | 8.13.2 | Dashboards |
| **API Framework** | FastAPI | 0.100+ | REST API |
| **Language** | Python | 3.11 | Primary language |

### Infrastructure

- **Cloud Provider:** Google Cloud Platform (GCP)
- **Container Registry:** Google Container Registry (GCR)
- **Persistent Storage:** GCE Persistent Disks (PVC)
- **Load Balancing:** GCP Load Balancers
- **Networking:** Kubernetes Services (ClusterIP, LoadBalancer)

---

## 🚀 Quick Start

### Prerequisites

- **Docker Desktop** (for local development)
- **kubectl** (Kubernetes CLI)
- **gcloud** (Google Cloud SDK)
- **Python 3.11+**
- **Git**

### Local Development (Docker Compose)

```bash
# Clone repository
git clone https://github.com/your-username/crypto-analytics-pipeline.git
cd crypto-analytics-pipeline

# Start Kafka cluster
cd kafka
docker-compose up -d

# Create Kafka topics
./create_topics.sh

# Run crawler
cd ../crawl
pip install -r requirements.txt
python crypto_crawler_streaming.py

# Run Spark Streaming (in another terminal)
cd ../spark
python streaming_processing.py
```

### Production Deployment (GKE)

```bash
# 1. Setup GKE cluster
gcloud container clusters create crypto-pipeline \
  --zone=asia-southeast1-a \
  --num-nodes=3 \
  --machine-type=n1-standard-4

# 2. Get credentials
gcloud container clusters get-credentials crypto-pipeline \
  --zone=asia-southeast1-a

# 3. Deploy all components
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka/
kubectl apply -f k8s/spark/
kubectl apply -f k8s/elasticsearch/
kubectl apply -f k8s/kibana/
kubectl apply -f k8s/crawler/
kubectl apply -f k8s/webapp/

# 4. Check deployment status
kubectl get pods -n crypto-pipeline
```

📖 **Detailed Deployment Guide:** See [k8s/README.md](k8s/README.md)

---

## 📦 Deployment

### Kubernetes Resources

| Resource | Type | Replicas | CPU | Memory | Storage |
|----------|------|----------|-----|--------|---------|
| crawler-streaming | Deployment | 1 | 0.5 | 512Mi | - |
| zookeeper | Deployment | 1 | 0.5 | 1Gi | - |
| kafka | StatefulSet | 1 | 1 | 2Gi | 10Gi |
| spark-streaming | Deployment | 1 | 1-2 | 2-4Gi | 5Gi (PVC) |
| spark-clean-gcs | CronJob | 0-1 | 1-2 | 2-4Gi | - |
| spark-agg-gcs | CronJob | 0-1 | 1-2 | 2-4Gi | - |
| spark-export-gcs | CronJob | 0-1 | 1-2 | 2-4Gi | - |
| postgres | StatefulSet | 1 | 1 | 2Gi | 10Gi |
| elasticsearch | StatefulSet | 3 | 2 | 4Gi | 50Gi × 3 |
| kibana | Deployment | 1 | 0.5 | 1Gi | - |
| webapp | Deployment | 1 | 0.5 | 512Mi | - |

**Total:** 11-12 pods, 10-12 CPU, 18-22Gi RAM, 175Gi storage

### Cost Estimation (GKE)

- **Compute:** 3× n1-standard-4 nodes = ~$300/month
- **Storage:** 175Gi SSD = ~$35/month
- **Load Balancers:** 2× = ~$36/month
- **Total:** ~**$371/month**

💡 **Cost Optimization:** Use preemptible nodes to save 70% on compute costs.

---

## 💻 Usage

### Access Services

```bash
# Get external IPs
kubectl get services -n crypto-pipeline

# Kibana Dashboard
http://<KIBANA_EXTERNAL_IP>:5601

# Web Dashboard
http://<WEBAPP_EXTERNAL_IP>:8000

# API Documentation
http://<WEBAPP_EXTERNAL_IP>:8000/docs
```

### API Examples

```bash
# Get market summary
curl http://localhost:8000/api/market/summary

# Get top gainers
curl http://localhost:8000/api/rankings/gainers?limit=10

# Get coin details
curl http://localhost:8000/api/coins/BTC

# Search coins
curl http://localhost:8000/api/search?q=bitcoin
```

### Kibana Dashboards

1. **Market Overview:** Total market cap, volume, sentiment
2. **Price Alerts:** Recent pump/dump events
3. **Top Movers:** Gainers and losers
4. **BTC Dominance:** Bitcoin market share over time
5. **Whale Activities:** Large volume transactions

📖 **Kibana Guide:** See [KIBANA_VISUALIZATION_GUIDE.md](KIBANA_VISUALIZATION_GUIDE.md)

---

## 📚 Documentation

### Project Documentation

- 📖 [Deployment Architecture](docs/DEPLOYMENT_ARCHITECTURE.md) - Detailed infrastructure analysis
- 📖 [Kibana Visualization Guide](KIBANA_VISUALIZATION_GUIDE.md) - Dashboard creation guide
- 📖 [Kubernetes Deployment](k8s/README.md) - K8s deployment instructions
- 📖 [API Documentation](http://localhost:8000/docs) - Interactive API docs (Swagger)

### Code Structure

```
.
├── crawl/                  # Data ingestion
│   ├── crypto_crawler_streaming.py
│   └── kafka_producer.py
├── spark/                  # Spark jobs
│   ├── streaming_processing.py
│   ├── data_cleaning.py
│   ├── daily_aggregation.py
│   └── export_metrics.py
├── k8s/                    # Kubernetes manifests
│   ├── kafka/
│   ├── spark/
│   ├── elasticsearch/
│   └── ...
├── webapp/                 # Web dashboard
│   ├── backend/           # FastAPI
│   └── frontend/          # HTML/CSS/JS
└── docs/                   # Documentation
```

---

## 📊 Performance

### Throughput

- **Data Ingestion:** 100 coins/minute = 144,000 messages/day
- **Spark Streaming:** 1.67 messages/second (real-time)
- **Batch Processing:** 4GB data in ~10 minutes

### Latency

- **End-to-end (Real-time):** < 2 minutes
- **Alert Detection:** < 1 minute
- **Batch Aggregation:** 1-2 hours (daily)

### Scalability

**Current Capacity:**
- 100 coins
- 144,000 messages/day
- 4.3 GB/month

**Maximum Capacity (with scaling):**
- 1,000+ coins
- 1.4M+ messages/day
- 43+ GB/month

**Scaling Options:**
- Horizontal: Add more Spark workers
- Vertical: Increase pod resources
- Storage: Unlimited (GCS)

---

## 🔧 Monitoring

### Health Checks

```bash
# Check all pods
kubectl get pods -n crypto-pipeline

# Check specific service
kubectl logs -f deployment/spark-streaming -n crypto-pipeline

# Check resource usage
kubectl top pods -n crypto-pipeline
```

### Metrics

**Spark Streaming:**
- Batch processing time
- Input/output rate
- Scheduling delay
- Checkpoint status

**Kafka:**
- Consumer lag
- Throughput (MB/sec)
- Partition distribution

**Elasticsearch:**
- Index size
- Query latency
- Indexing rate

---

## 🐛 Troubleshooting

### Common Issues

**1. Pod CrashLoopBackOff**
```bash
# Check logs
kubectl logs <pod-name> -n crypto-pipeline

# Describe pod
kubectl describe pod <pod-name> -n crypto-pipeline
```

**2. Kafka Connection Refused**
```bash
# Check Kafka service
kubectl get svc kafka -n crypto-pipeline

# Test connection
kubectl exec -it <crawler-pod> -n crypto-pipeline -- \
  nc -zv kafka 9092
```

**3. Spark Streaming Checkpoint Issues**
```bash
# Check PVC
kubectl get pvc -n crypto-pipeline

# Clear checkpoint (if corrupted)
kubectl exec -it <spark-pod> -n crypto-pipeline -- \
  rm -rf /checkpoints/*
```

📖 **More Troubleshooting:** See [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)

---

## 🤝 Contributing

We welcome contributions! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 for Python code
- Add unit tests for new features
- Update documentation
- Test on local environment before PR

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👥 Team

**Nhóm IT4931** - Đồ án tốt nghiệp Big Data

- **Nguyễn Văn A** - Team Lead, Backend Developer
- **Trần Thị B** - Data Engineer
- **Lê Văn C** - DevOps Engineer
- **Phạm Thị D** - Frontend Developer

---

## 🙏 Acknowledgments

- [Apache Spark](https://spark.apache.org/) - Unified analytics engine
- [Apache Kafka](https://kafka.apache.org/) - Distributed streaming platform
- [Elasticsearch](https://www.elastic.co/) - Search and analytics engine
- [CoinGecko](https://www.coingecko.com/) - Cryptocurrency data API
- [Google Cloud Platform](https://cloud.google.com/) - Cloud infrastructure

---

## 📞 Contact

- **Email:** nhomit4931@example.com
- **GitHub:** [github.com/your-username/crypto-analytics-pipeline](https://github.com/your-username/crypto-analytics-pipeline)
- **Documentation:** [docs/](docs/)

---

## 🗺️ Roadmap

### Phase 1: Stability (Q1 2026) ✅
- [x] Implement persistent checkpoints
- [x] Add health checks
- [x] Improve error handling

### Phase 2: Security (Q2 2026)
- [ ] Enable Kafka SASL/SSL
- [ ] Add authentication to Elasticsearch
- [ ] Implement External Secrets Operator
- [ ] Enable TLS everywhere

### Phase 3: High Availability (Q3 2026)
- [ ] Scale Kafka to 3 replicas
- [ ] Deploy PostgreSQL HA
- [ ] Migrate Spark to Cluster mode
- [ ] Add load balancers

### Phase 4: Observability (Q4 2026)
- [ ] Deploy Prometheus + Grafana
- [ ] Setup centralized logging (Fluentd)
- [ ] Add distributed tracing (Jaeger)
- [ ] Create alerting rules

### Phase 5: Optimization (2027)
- [ ] Implement auto-scaling (HPA)
- [ ] Optimize Spark jobs
- [ ] Add caching layer (Redis)
- [ ] Implement data lifecycle policies

---

## 📈 Project Stats

![GitHub stars](https://img.shields.io/github/stars/your-username/crypto-analytics-pipeline?style=social)
![GitHub forks](https://img.shields.io/github/forks/your-username/crypto-analytics-pipeline?style=social)
![GitHub issues](https://img.shields.io/github/issues/your-username/crypto-analytics-pipeline)
![GitHub pull requests](https://img.shields.io/github/issues-pr/your-username/crypto-analytics-pipeline)

---

<p align="center">
  Made with ❤️ by Nhóm IT4931
</p>

<p align="center">
  <a href="#-overview">Back to top</a>
</p>
