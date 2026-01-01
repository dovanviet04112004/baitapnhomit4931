# Kubernetes Manifests - Crypto Analytics Pipeline

Thư mục chứa các Kubernetes deployment manifests cho toàn bộ hệ thống.

## 📁 Cấu Trúc

```
k8s/
├── namespace.yaml              # Namespace: crypto-pipeline
├── kafka/
│   ├── zookeeper.yaml          # Zookeeper (Kafka dependency)
│   ├── kafka-cluster.yaml      # Kafka StatefulSet (3 brokers)
│   └── kafka-topics.yaml       # Job tạo topics
├── spark/
│   ├── spark-master.yaml       # Spark Master
│   └── spark-worker.yaml       # Spark Workers (2 replicas)
├── elasticsearch/
│   ├── elasticsearch-cluster.yaml  # ES StatefulSet (2 nodes)
│   └── elasticsearch-setup.yaml    # Job tạo indices
├── kibana/
│   └── kibana.yaml             # Kibana Deployment
├── crawler/
│   └── crawler-cronjob.yaml    # CronJob crawl mỗi phút
├── api/
│   └── query-api.yaml          # FastAPI service
├── storage/
│   ├── persistent-volumes.yaml # PVCs cho data
│   └── resource-quotas.yaml    # Resource limits
├── deploy.bat                  # Script deploy (Windows)
├── deploy.sh                   # Script deploy (Linux/Mac)
└── README.md                   # File này
```

## 🚀 Quick Deploy

### Windows:
```cmd
cd k8s
deploy.bat
```

### Linux/Mac:
```bash
cd k8s
chmod +x deploy.sh
./deploy.sh
```

## 📊 Resources Allocation

| Service | Replicas | Memory | CPU | Storage |
|---------|----------|--------|-----|---------|
| Zookeeper | 1 | 512Mi | 0.5 | - |
| Kafka | 3 | 1Gi | 0.5 | 5Gi each |
| Elasticsearch | 2 | 2Gi | 1.0 | 10Gi each |
| Kibana | 1 | 1Gi | 0.5 | - |
| Spark Master | 1 | 2Gi | 1.0 | - |
| Spark Worker | 2 | 4Gi | 2.0 | - |
| Query API | 2 | 512Mi | 0.5 | - |
| Crawler | CronJob | 256Mi | 0.2 | - |

**Total: ~20Gi RAM, 8 CPU cores**

## 🔗 Service URLs (NodePort)

| Service | Internal | External |
|---------|----------|----------|
| Kafka | kafka-headless:9092 | localhost:30092 |
| Elasticsearch | elasticsearch:9200 | localhost:30920 |
| Kibana | kibana:5601 | localhost:30561 |
| Spark UI | spark-master:8080 | localhost:30080 |
| Query API | query-api:8000 | localhost:30800 |

## 🔧 Useful Commands

```bash
# Check all resources
kubectl get all -n crypto-pipeline

# View logs
kubectl logs -f <pod-name> -n crypto-pipeline

# Scale workers
kubectl scale deployment spark-worker --replicas=4 -n crypto-pipeline

# Delete all
kubectl delete namespace crypto-pipeline
```

## 📋 Prerequisites

- Kubernetes cluster (Minikube/Docker Desktop/Cloud)
- kubectl CLI
- 8GB+ RAM, 4+ CPU cores

---

## 🎯 Workflow Sau Khi Deploy

### Bước 1: Verify Deployment

```bash
# Kiểm tra tất cả pods đang running
kubectl get pods -n crypto-pipeline

# Chờ tất cả pods ready (có thể mất 5-10 phút)
kubectl wait --for=condition=ready pod --all -n crypto-pipeline --timeout=600s
```

**Kết quả mong đợi:**
```
NAME                           READY   STATUS    RESTARTS   AGE
zookeeper-xxxxx                1/1     Running   0          5m
kafka-0                        1/1     Running   0          4m
kafka-1                        1/1     Running   0          4m
kafka-2                        1/1     Running   0          4m
elasticsearch-0                1/1     Running   0          3m
elasticsearch-1                1/1     Running   0          3m
kibana-xxxxx                   1/1     Running   0          2m
spark-master-xxxxx             1/1     Running   0          2m
spark-worker-xxxxx             1/1     Running   0          2m
query-api-xxxxx                1/1     Running   0          1m
```

---

### Bước 2: Kiểm tra Services

```bash
# List all services
kubectl get svc -n crypto-pipeline

# Test Kafka
kubectl exec -it kafka-0 -n crypto-pipeline -- kafka-topics --list --bootstrap-server localhost:9092

# Test Elasticsearch
kubectl exec -it elasticsearch-0 -n crypto-pipeline -- curl http://localhost:9200
```

---

### Bước 3: Chạy Pipeline

#### 3.1. CronJob Crawler đã tự động chạy
```bash
# Kiểm tra CronJob schedule (mỗi 5 phút)
kubectl get cronjobs -n crypto-pipeline

# Xem các jobs đã chạy
kubectl get jobs -n crypto-pipeline

# Xem logs crawler (job gần nhất)
kubectl logs -n crypto-pipeline -l job-name=<job-name>
```

#### 3.2. Consumer Kafka → HDFS (Chạy thủ công)
```bash
# Tạo pod consumer tạm thời
kubectl run kafka-consumer -n crypto-pipeline \
  --image=python:3.11-slim \
  --restart=Never \
  --command -- sleep infinity

# Copy code vào pod
kubectl cp ../hdfs/kafka_to_hdfs_raw.py crypto-pipeline/kafka-consumer:/app/

# Exec vào pod và chạy
kubectl exec -it kafka-consumer -n crypto-pipeline -- bash
cd /app
pip install kafka-python
python kafka_to_hdfs_raw.py
```

#### 3.3. Chạy Spark Batch Jobs
```bash
# Submit job lên Spark Master
kubectl exec -it spark-master-xxxxx -n crypto-pipeline -- \
  spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /app/batch_processing.py --all

# Hoặc chạy từng job
kubectl exec -it spark-master-xxxxx -n crypto-pipeline -- \
  spark-submit \
  --master spark://spark-master:7077 \
  /app/batch_processing.py --job daily_price_stats
```

#### 3.4. Index vào Elasticsearch
```bash
# Job ES setup đã tạo indices
kubectl logs -n crypto-pipeline -l job-name=elasticsearch-setup

# Chạy spark job index data
kubectl exec -it spark-master-xxxxx -n crypto-pipeline -- \
  spark-submit \
  --master spark://spark-master:7077 \
  /app/spark_to_elasticsearch.py --all
```

---

### Bước 4: Truy Cập Services (Port-forward hoặc NodePort)

#### Option A: Port-forward (Localhost)
```bash
# Kibana Dashboard
kubectl port-forward svc/kibana 5601:5601 -n crypto-pipeline

# FastAPI
kubectl port-forward svc/query-api 8000:8000 -n crypto-pipeline

# Elasticsearch
kubectl port-forward svc/elasticsearch 9200:9200 -n crypto-pipeline

# Spark UI
kubectl port-forward svc/spark-master 8080:8080 -n crypto-pipeline
```

Sau đó truy cập:
- Kibana: http://localhost:5601
- API Docs: http://localhost:8000/docs
- Elasticsearch: http://localhost:9200
- Spark UI: http://localhost:8080

#### Option B: NodePort (Cloud K8s)

Nếu dùng cloud K8s (AKS/EKS/GKE), services đã expose qua NodePort:

```bash
# Lấy External IP của cluster
kubectl get nodes -o wide

# Truy cập qua NodePort
# Kibana: http://<NODE_IP>:30561
# API: http://<NODE_IP>:30800
# Elasticsearch: http://<NODE_IP>:30920
```

---

### Bước 5: Monitor & Debug

```bash
# Xem logs realtime
kubectl logs -f <pod-name> -n crypto-pipeline

# Xem events
kubectl get events -n crypto-pipeline --sort-by='.lastTimestamp'

# Xem resource usage
kubectl top pods -n crypto-pipeline
kubectl top nodes

# Debug pod lỗi
kubectl describe pod <pod-name> -n crypto-pipeline

# Exec vào pod
kubectl exec -it <pod-name> -n crypto-pipeline -- bash
```

---

### Bước 6: Query Data

```bash
# Test API
curl http://localhost:8000/api/market/summary
curl http://localhost:8000/api/coins?limit=10
curl http://localhost:8000/api/rankings/gainers?limit=5

# Test Elasticsearch trực tiếp
curl http://localhost:9200/crypto_latest/_search?size=10

# Tạo Dashboard trong Kibana
# 1. Truy cập http://localhost:5601
# 2. Stack Management → Data Views
# 3. Create data view: crypto_latest, crypto_history, alerts
# 4. Analytics → Visualize Library → Create visualization
# 5. Analytics → Dashboards → Create dashboard
```

---

## 🔄 Pipeline Workflow Tổng Thể

```
1. CronJob Crawler (auto 5 phút)
   └─> Kafka Topic: raw_crypto
        
2. Kafka Consumer (chạy thủ công)
   └─> HDFS: /data/raw/

3. Spark Batch Jobs (chạy thủ công)
   └─> HDFS: /data/clean/ + /data/aggregated/

4. Spark to ES Job (chạy thủ công)
   └─> Elasticsearch indices

5. Query API (auto running)
   └─> REST endpoints: http://localhost:8000

6. Kibana Dashboard (auto running)
   └─> Visualization: http://localhost:5601
```

---

## 🗑️ Cleanup

```bash
# Xóa toàn bộ pipeline
kubectl delete namespace crypto-pipeline

# Xóa từng thành phần
kubectl delete -f kafka/ -n crypto-pipeline
kubectl delete -f spark/ -n crypto-pipeline
kubectl delete -f elasticsearch/ -n crypto-pipeline

# Xóa PVCs (data sẽ mất!)
kubectl delete pvc --all -n crypto-pipeline
```
