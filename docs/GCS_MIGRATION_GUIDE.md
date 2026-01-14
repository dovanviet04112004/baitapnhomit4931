# Migration Guide: HDFS → Google Cloud Storage (GCS)

## 📋 Overview

Migration từ HDFS sang GCS với những ưu điểm:
- ✅ **Fully managed** - Không cần maintain HDFS cluster
- ✅ **Scalable** - Unlimited storage
- ✅ **Cost-effective** - $0.02/GB/month
- ✅ **Native Spark support** - Spark đọc trực tiếp từ `gs://`
- ✅ **High durability** - 99.999999999% durability

---

## 🚀 Setup Steps

### Step 1: Create GCS Bucket

```bash
# Set project ID
export PROJECT_ID=crypto-project-bigdata

# Create bucket (region: asia-southeast1 for Vietnam)
gsutil mb -p $PROJECT_ID \
  -c STANDARD \
  -l asia-southeast1 \
  gs://crypto-pipeline-data

# Verify bucket
gsutil ls gs://crypto-pipeline-data
```

**Output:**
```
Creating gs://crypto-pipeline-data/...
```

---

### Step 2: Setup Workload Identity (For GKE)

**Workload Identity** cho phép pods truy cập GCS without service account keys.

```bash
# Enable Workload Identity on cluster (nếu chưa có)
gcloud container clusters update crypto-pipeline \
  --region=asia-southeast1 \
  --workload-pool=$PROJECT_ID.svc.id.goog

# Create Google Service Account
gcloud iam service-accounts create gcs-kafka-consumer \
  --display-name="GCS Kafka Consumer SA" \
  --project=$PROJECT_ID

# Grant GCS permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:gcs-kafka-consumer@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# Allow K8s SA to impersonate Google SA
gcloud iam service-accounts add-iam-policy-binding \
  gcs-kafka-consumer@$PROJECT_ID.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:$PROJECT_ID.svc.id.goog[crypto-pipeline/kafka-to-gcs]"

# Create K8s Service Account
kubectl create serviceaccount kafka-to-gcs -n crypto-pipeline

# Annotate K8s SA with Google SA
kubectl annotate serviceaccount kafka-to-gcs \
  -n crypto-pipeline \
  iam.gke.io/gcp-service-account=gcs-kafka-consumer@$PROJECT_ID.iam.gserviceaccount.com
```

---

### Step 3: Deploy Kafka to GCS Consumer

Tạo file deployment:

**File: `k8s/hdfs/kafka-to-gcs-deployment.yaml`**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-to-gcs
  namespace: crypto-pipeline
  labels:
    app: kafka-to-gcs
    component: data-ingestion
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-to-gcs
  template:
    metadata:
      labels:
        app: kafka-to-gcs
    spec:
      serviceAccountName: kafka-to-gcs  # Workload Identity
      
      containers:
      - name: consumer
        image: gcr.io/crypto-project-bigdata/kafka-to-gcs:latest
        imagePullPolicy: Always
        
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka:9092"
        - name: KAFKA_TOPIC
          value: "crypto-raw"
        - name: GCS_BUCKET_NAME
          value: "crypto-pipeline-data"
        - name: GCS_DATA_DIR
          value: "data"
        
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
      
      restartPolicy: Always
---
# CronJob version - Run every hour
apiVersion: batch/v1
kind: CronJob
metadata:
  name: kafka-to-gcs-hourly
  namespace: crypto-pipeline
spec:
  schedule: "0 * * * *"  # Every hour at :00
  jobTemplate:
    spec:
      template:
        spec:
          serviceAccountName: kafka-to-gcs
          containers:
          - name: consumer
            image: gcr.io/crypto-project-bigdata/kafka-to-gcs:latest
            env:
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: "kafka:9092"
            - name: KAFKA_TOPIC
              value: "crypto-raw"
            - name: GCS_BUCKET_NAME
              value: "crypto-pipeline-data"
          restartPolicy: OnFailure
```

Deploy:

```bash
kubectl apply -f k8s/hdfs/kafka-to-gcs-deployment.yaml
```

---

### Step 4: Build & Push Docker Image

```bash
cd hdfs

# Build image
docker build -t gcr.io/crypto-project-bigdata/kafka-to-gcs:latest .

# Push to GCR
docker push gcr.io/crypto-project-bigdata/kafka-to-gcs:latest
```

---

### Step 5: Verify GCS Data

```bash
# List files in GCS
gsutil ls -r gs://crypto-pipeline-data/data/raw/

# Expected output:
# gs://crypto-pipeline-data/data/raw/dt=2026-01-13/hr=00/data_1234567890.jsonl
# gs://crypto-pipeline-data/data/raw/dt=2026-01-13/hr=01/data_1234567891.jsonl

# Read sample file
gsutil cat gs://crypto-pipeline-data/data/raw/dt=2026-01-13/hr=00/data_*.jsonl | head -5
```

---

## 🔄 Update Spark Jobs to Use GCS

### Batch Processing

**Old (HDFS):**
```python
df = spark.read.json("hdfs://namenode:9000/data/raw/dt=2026-01-13/")
```

**New (GCS):**
```python
df = spark.read.json("gs://crypto-pipeline-data/data/raw/dt=2026-01-13/")
```

### Spark Configuration

GCS connector đã có sẵn trong image `apache/spark:3.4.1`. Chỉ cần:

```python
spark = SparkSession.builder \
    .appName("BatchProcessing") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .getOrCreate()
```

**Hoặc** dùng GCS connector JAR:

```bash
spark-submit \
  --packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11 \
  batch_processing.py
```

---

## 📊 Cost Estimate

### GCS Storage Costs (Standard Storage - asia-southeast1)

| Usage | Cost |
|-------|------|
| Storage (100GB) | $2.00/month |
| Class A operations (100k writes) | $0.50 |
| Class B operations (1M reads) | $0.40 |
| Network egress (10GB) | $1.20 |
| **Total** | **~$4-5/month** |

### vs HDFS on GKE

| Component | vCPU | RAM | Cost/month |
|-----------|------|-----|------------|
| Namenode | 2 | 4GB | $60 |
| Datanode x3 | 6 | 12GB | $180 |
| **Total** | | | **$240/month** |

**Savings: $235/month (98% cheaper!)** 💰

---

## 🔍 Monitoring

### Check Consumer Logs

```bash
kubectl logs -f -n crypto-pipeline -l app=kafka-to-gcs
```

### Monitor GCS Usage

```bash
# Object count
gsutil ls -r gs://crypto-pipeline-data/data/raw/ | wc -l

# Total size
gsutil du -s gs://crypto-pipeline-data/data/raw/
```

### GCP Console

https://console.cloud.google.com/storage/browser/crypto-pipeline-data

---

## 🐛 Troubleshooting

### Error: "Permission denied"

**Solution:** Check Workload Identity setup

```bash
# Test from pod
kubectl run -it --rm debug --image=google/cloud-sdk:alpine \
  --serviceaccount=kafka-to-gcs \
  -n crypto-pipeline \
  -- gsutil ls gs://crypto-pipeline-data
```

### Error: "Bucket does not exist"

**Solution:** Create bucket

```bash
gsutil mb gs://crypto-pipeline-data
```

### Error: "Quota exceeded"

**Solution:** Request quota increase in GCP Console

---

## ✅ Migration Checklist

- [ ] Create GCS bucket
- [ ] Setup Workload Identity
- [ ] Build & push Docker image
- [ ] Deploy consumer to K8s
- [ ] Verify data in GCS
- [ ] Update Spark jobs to read from GCS
- [ ] Test end-to-end pipeline
- [ ] Remove old HDFS deployment (optional)

---

## 📚 References

- [GCS Documentation](https://cloud.google.com/storage/docs)
- [Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)
- [GCS Connector for Spark](https://github.com/GoogleCloudDataproc/hadoop-connectors)
- [GCS Pricing](https://cloud.google.com/storage/pricing)
