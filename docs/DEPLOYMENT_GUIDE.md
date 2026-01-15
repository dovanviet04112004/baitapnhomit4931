# Deployment Guide

Hướng dẫn chi tiết triển khai hệ thống Crypto Analytics Pipeline trên các môi trường khác nhau.

---

## Mục Lục

- [Yêu Cầu Hệ Thống](#yêu-cầu-hệ-thống)
- [Local Development](#local-development)
- [Docker Compose](#docker-compose)
- [Kubernetes - Azure AKS](#kubernetes---azure-aks)
- [Kubernetes - Google GKE](#kubernetes---google-gke)
- [Troubleshooting](#troubleshooting)

---

## Yêu Cầu Hệ Thống

### Hardware Requirements

| Environment | CPU | RAM | Storage |
|-------------|-----|-----|---------|
| Development | 4 cores | 8GB | 20GB |
| Staging | 8 cores | 16GB | 50GB |
| Production | 16+ cores | 32GB+ | 100GB+ |

### Software Requirements

| Software | Version | Purpose |
|----------|---------|---------|
| Docker | 20.10+ | Container runtime |
| Docker Compose | 2.0+ | Local orchestration |
| kubectl | 1.28+ | Kubernetes CLI |
| Python | 3.11+ | Application runtime |
| Node.js | 18+ | Frontend build (optional) |

---

## Local Development

### 1. Clone Repository

```bash
git clone https://github.com/username/crypto-analytics-pipeline.git
cd crypto-analytics-pipeline
```

### 2. Setup Python Environment

```bash
# Tạo virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (Linux/Mac)
source venv/bin/activate

# Install dependencies
pip install -r crawl/requirements.txt
pip install -r spark/requirements.txt
pip install -r elasticsearch/requirements.txt
pip install -r webapp/requirements.txt
```

### 3. Start Services

```bash
# Start Kafka cluster
cd kafka
docker-compose up -d

# Create topics
./create_topics.bat  # Windows
./create_topics.sh   # Linux/Mac

# Start Elasticsearch
docker run -d --name elasticsearch \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  elasticsearch:8.13.2
```

### 4. Run Components

```bash
# Terminal 1: Crawler
cd crawl
python crypto_crawler_streaming.py

# Terminal 2: Spark jobs
cd spark
python data_cleaning.py
python daily_aggregation.py

# Terminal 3: API server
cd elasticsearch
python query_api.py

# Terminal 4: Web dashboard
cd webapp
python api.py
```

---

## Docker Compose

### Full Stack Deployment

```bash
# Start all services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f

# Stop all services
docker-compose down
```

### Services Overview

| Service | Port | Health Check |
|---------|------|--------------|
| Zookeeper | 2181 | `nc -z localhost 2181` |
| Kafka 1 | 19092 | `kafka-broker-api-versions --bootstrap-server localhost:19092` |
| Kafka 2 | 19093 | `kafka-broker-api-versions --bootstrap-server localhost:19093` |
| Kafka 3 | 19094 | `kafka-broker-api-versions --bootstrap-server localhost:19094` |
| HDFS NameNode | 9870 | `curl http://localhost:9870` |
| Spark Master | 8080 | `curl http://localhost:8080` |
| Elasticsearch | 9200 | `curl http://localhost:9200/_cluster/health` |
| Kibana | 5601 | `curl http://localhost:5601/api/status` |
| PostgreSQL | 5432 | `pg_isready -h localhost -p 5432` |
| Web Dashboard | 3000 | `curl http://localhost:3000/health` |
| API Server | 8000 | `curl http://localhost:8000/health` |

### Environment Variables

Tạo file `.env` từ template:

```bash
cp .env.example .env
```

Cấu hình các biến:

```env
# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092,kafka3:9092

# Elasticsearch
ELASTICSEARCH_HOST=elasticsearch
ELASTICSEARCH_PORT=9200

# PostgreSQL
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=crypto_analytics
POSTGRES_USER=admin
POSTGRES_PASSWORD=your_secure_password

# API
API_HOST=0.0.0.0
API_PORT=8000

# CoinGecko (optional)
COINGECKO_API_KEY=your_api_key
```

---

## Kubernetes - Azure AKS

### 1. Prerequisites

```bash
# Install Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Login to Azure
az login

# Install kubectl
az aks install-cli
```

### 2. Create AKS Cluster

```bash
# Create resource group
az group create --name crypto-analytics-rg --location southeastasia

# Create AKS cluster
az aks create \
  --resource-group crypto-analytics-rg \
  --name crypto-analytics-aks \
  --node-count 3 \
  --node-vm-size Standard_DS2_v2 \
  --enable-managed-identity \
  --generate-ssh-keys

# Get credentials
az aks get-credentials \
  --resource-group crypto-analytics-rg \
  --name crypto-analytics-aks
```

### 3. Deploy to AKS

```bash
# Create namespace
kubectl apply -f k8s/namespace.yaml

# Deploy storage
kubectl apply -f k8s/storage/

# Deploy Kafka cluster
kubectl apply -f k8s/kafka/

# Deploy Elasticsearch
kubectl apply -f k8s/elasticsearch/

# Deploy Spark
kubectl apply -f k8s/spark/

# Deploy Web services
kubectl apply -f k8s/webapp/
kubectl apply -f k8s/postgres/

# Setup ingress
kubectl apply -f k8s/ingress.yaml
```

### 4. Verify Deployment

```bash
# Check pods
kubectl get pods -n crypto-analytics

# Check services
kubectl get svc -n crypto-analytics

# Port forward for testing
kubectl port-forward svc/webapp 3000:3000 -n crypto-analytics
```

### Student Mode (Cost Optimization)

```bash
# Use smaller node size
az aks create \
  --resource-group crypto-analytics-rg \
  --name crypto-analytics-student \
  --node-count 2 \
  --node-vm-size Standard_B2s \
  --enable-managed-identity

# Apply resource quotas
kubectl apply -f k8s/storage/resource-quotas.yaml
```

---

## Kubernetes - Google GKE

### 1. Prerequisites

```bash
# Install gcloud CLI
curl https://sdk.cloud.google.com | bash

# Initialize gcloud
gcloud init

# Install kubectl
gcloud components install kubectl
```

### 2. Create GKE Cluster

```bash
# Set project
gcloud config set project your-project-id

# Create cluster
gcloud container clusters create crypto-analytics \
  --zone asia-southeast1-a \
  --num-nodes 3 \
  --machine-type e2-standard-2 \
  --enable-autoscaling \
  --min-nodes 2 \
  --max-nodes 5

# Get credentials
gcloud container clusters get-credentials crypto-analytics \
  --zone asia-southeast1-a
```

### 3. Setup Google Cloud Storage

```bash
# Create GCS bucket
gsutil mb -l asia-southeast1 gs://crypto-analytics-data

# Create service account
gcloud iam service-accounts create crypto-analytics-sa

# Grant permissions
gsutil iam ch serviceAccount:crypto-analytics-sa@your-project.iam.gserviceaccount.com:objectAdmin gs://crypto-analytics-data

# Create key file
gcloud iam service-accounts keys create gcs-key.json \
  --iam-account crypto-analytics-sa@your-project.iam.gserviceaccount.com

# Create Kubernetes secret
kubectl create secret generic gcs-credentials \
  --from-file=key.json=gcs-key.json \
  -n crypto-analytics
```

### 4. Deploy to GKE

```bash
# Apply GCS config
kubectl apply -f k8s/gcs-config.yaml

# Deploy all services
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka/
kubectl apply -f k8s/elasticsearch/
kubectl apply -f k8s/spark/
kubectl apply -f k8s/hdfs/kafka-to-gcs-deployment.yaml
kubectl apply -f k8s/webapp/
```

---

## Monitoring & Logging

### Prometheus + Grafana (Optional)

```bash
# Deploy monitoring stack
kubectl apply -f k8s/monitoring/prometheus.yaml
kubectl apply -f k8s/monitoring/grafana.yaml

# Access Grafana
kubectl port-forward svc/grafana 3001:3000 -n monitoring
```

### Log Aggregation

```bash
# View logs của specific pod
kubectl logs -f deployment/crawler -n crypto-analytics

# View logs của all pods
kubectl logs -l app=spark -n crypto-analytics --all-containers
```

---

## Troubleshooting

### Common Issues

#### 1. Kafka Connection Failed

```bash
# Check Kafka pods
kubectl get pods -l app=kafka -n crypto-analytics

# Check Kafka logs
kubectl logs -l app=kafka -n crypto-analytics

# Restart Kafka
kubectl rollout restart deployment/kafka -n crypto-analytics
```

#### 2. Elasticsearch Out of Memory

```bash
# Increase memory limit
kubectl edit deployment elasticsearch -n crypto-analytics

# Update resources:
#   limits:
#     memory: "4Gi"
#   requests:
#     memory: "2Gi"
```

#### 3. Spark Job Failed

```bash
# Check Spark driver logs
kubectl logs spark-driver-pod -n crypto-analytics

# Check executor logs
kubectl logs spark-executor-pod -n crypto-analytics

# Restart Spark master
kubectl rollout restart deployment/spark-master -n crypto-analytics
```

#### 4. Web Dashboard Not Loading

```bash
# Check webapp pod
kubectl describe pod -l app=webapp -n crypto-analytics

# Check service
kubectl get svc webapp -n crypto-analytics

# Check ingress
kubectl describe ingress -n crypto-analytics
```

### Health Checks

```bash
# Check all pods
kubectl get pods -n crypto-analytics -o wide

# Check resource usage
kubectl top pods -n crypto-analytics

# Check events
kubectl get events -n crypto-analytics --sort-by='.lastTimestamp'
```

---

## Backup & Recovery

### Database Backup

```bash
# PostgreSQL backup
kubectl exec -it postgres-pod -n crypto-analytics -- \
  pg_dump -U admin crypto_analytics > backup.sql

# Elasticsearch snapshot
curl -X PUT "localhost:9200/_snapshot/backup" -H 'Content-Type: application/json' -d'
{
  "type": "fs",
  "settings": {
    "location": "/backup"
  }
}'
```

### Recovery

```bash
# Restore PostgreSQL
kubectl exec -i postgres-pod -n crypto-analytics -- \
  psql -U admin crypto_analytics < backup.sql

# Restore Elasticsearch
curl -X POST "localhost:9200/_snapshot/backup/snapshot_1/_restore"
```

---

## Security Checklist

- [ ] Change default passwords
- [ ] Enable TLS/SSL for all services
- [ ] Configure network policies
- [ ] Setup RBAC for Kubernetes
- [ ] Enable audit logging
- [ ] Regular security updates

---

## Support

Nếu gặp vấn đề khi deploy:
- Kiểm tra logs của từng service
- Tham khảo Troubleshooting section
- Tạo Issue trên GitHub với logs đầy đủ
