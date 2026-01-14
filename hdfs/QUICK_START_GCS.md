# Quick Start: Deploy Kafka to GCS

## 🚀 5 Steps để chạy

### 1. Create GCS Bucket (1 lần duy nhất)

```bash
gsutil mb -l asia-southeast1 gs://crypto-pipeline-data
```

### 2. Setup Workload Identity (1 lần duy nhất)

```bash
# Run script này
./setup-gcs-workload-identity.sh
```

Hoặc manual:

```bash
PROJECT_ID=crypto-project-bigdata

# Create Google Service Account
gcloud iam service-accounts create gcs-kafka-consumer \
  --project=$PROJECT_ID

# Grant permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:gcs-kafka-consumer@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# Bind to K8s SA
gcloud iam service-accounts add-iam-policy-binding \
  gcs-kafka-consumer@$PROJECT_ID.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:$PROJECT_ID.svc.id.goog[crypto-pipeline/kafka-to-gcs]"
```

### 3. Build & Push Image

```bash
cd hdfs
./build-and-push-gcs.bat
```

### 4. Deploy to K8s

```bash
cd ..
kubectl apply -f k8s/hdfs/kafka-to-gcs-deployment.yaml
```

### 5. Verify

```bash
# Check pod
kubectl get pods -n crypto-pipeline -l app=kafka-to-gcs

# Check logs
kubectl logs -f -n crypto-pipeline -l app=kafka-to-gcs

# Check GCS
gsutil ls -r gs://crypto-pipeline-data/data/raw/
```

---

## ✅ Expected Output

**Pod logs:**
```
🔗 Connecting to Google Cloud Storage...
✅ GCS client connected to bucket: crypto-pipeline-data
✅ GCS bucket 'crypto-pipeline-data' is accessible!
🔗 Connecting to Kafka: ['kafka:9092']
✅ Connected to Kafka, consuming from topic: crypto-raw
📂 Writing to GCS: gs://crypto-pipeline-data/data/raw/
   ➜ Now writing to GCS: gs://crypto-pipeline-data/data/raw/dt=2026-01-13/hr=15/data_1736776800.jsonl
      ✅ Written to gs://crypto-pipeline-data/data/raw/dt=2026-01-13/hr=15/data_1736776800.jsonl
   ... written 50 records so far ...
```

**GCS structure:**
```
gs://crypto-pipeline-data/
└── data/
    └── raw/
        ├── dt=2026-01-13/
        │   ├── hr=00/
        │   │   ├── data_1736726400.jsonl
        │   │   └── data_1736726450.jsonl
        │   ├── hr=01/
        │   └── hr=02/
        └── dt=2026-01-14/
```

---

## 🔧 Update Spark Jobs

**Trong `batch_processing.py`:**

```python
# Old
raw_df = spark.read.json("hdfs://namenode:9000/data/raw/")

# New
raw_df = spark.read.json("gs://crypto-pipeline-data/data/raw/")
```

Không cần thay đổi gì khác! Spark tự động detect GCS.

---

## 💰 Cost

- Storage: ~$2/month cho 100GB
- Operations: ~$0.50/month
- **Total: $2-3/month**

vs HDFS cluster: $240/month → **Tiết kiệm 98%!**

---

## 📚 Full Documentation

Xem [GCS_MIGRATION_GUIDE.md](./GCS_MIGRATION_GUIDE.md) để biết thêm chi tiết.
