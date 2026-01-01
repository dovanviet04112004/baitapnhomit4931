# 🚀 HƯỚNG DẪN CÀI ĐẶT NHANH

## Yêu cầu hệ thống

| Phần mềm | Version | Link |
|----------|---------|------|
| Python | 3.11+ | https://python.org |
| Java JDK | 11 | https://adoptium.net |
| Docker Desktop | Latest | https://docker.com |
| Azure CLI | Latest | https://aka.ms/installazurecliwindows |

---

## Option 1: Chạy Local (Không cần K8s)

### Bước 1: Clone repo
```bash
git clone <repo-url>
cd bigdata
```

### Bước 2: Cài Python packages
```bash
pip install pyspark kafka-python requests fastapi uvicorn pandas elasticsearch
```

### Bước 3: Set Java 11
```bash
# Windows
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.29.7-hotspot
set PATH=%JAVA_HOME%\bin;%PATH%

# Verify
java -version
```

### Bước 4: Cài Elasticsearch + Kibana
- Download ES 8.13.2: https://www.elastic.co/downloads/elasticsearch
- Download Kibana 8.13.2: https://www.elastic.co/downloads/kibana
- Cấu hình ES: `config/elasticsearch.yml` → `xpack.security.enabled: false`

### Bước 5: Start Kafka (Docker)
```bash
cd kafka
docker-compose up -d
```

### Bước 6: Chạy Pipeline
```bash
# 1. Tạo fake data
cd crawl
python send_fake_crypto_kafka.py

# 2. Consumer Kafka → HDFS
cd ../hdfs
python kafka_to_hdfs_raw.py

# 3. Spark batch processing
cd ../spark
run_clean_with_java11.cmd

# 4. Index vào Elasticsearch
run_es_with_java11.cmd

# 5. Start API server
cd ../elasticsearch
python query_api.py
```

### Bước 7: Truy cập
- Kibana: http://localhost:5601
- API: http://localhost:8000/docs

---

## Option 2: Deploy lên Azure AKS (Kubernetes)

### Bước 1: Clone repo
```bash
git clone <repo-url>
cd bigdata/k8s
```

### Bước 2: Cài Azure CLI
```bash
winget install Microsoft.AzureCLI
```

### Bước 3: Đăng nhập Azure
```bash
az login
```

### Bước 4: Tạo AKS Cluster
```bash
setup-aks.bat
# Chờ 5-10 phút
```

### Bước 5: Deploy Pipeline
```bash
deploy.bat
```

### Bước 6: Truy cập Services
```bash
port-forward.bat

# Mở browser:
# http://localhost:5601  - Kibana
# http://localhost:8000  - API
```

### Bước 7: XÓA KHI XONG!
```bash
cleanup-aks.bat
```

---

## Troubleshooting

### Lỗi Java version
```bash
set JAVA_HOME=<đường dẫn JDK 11>
java -version  # Phải là 11
```

### Lỗi Kafka connection
```bash
docker ps  # Kiểm tra containers đang chạy
docker-compose up -d  # Restart nếu cần
```

### Lỗi Elasticsearch
```bash
curl http://localhost:9200  # Kiểm tra ES đang chạy
```

### Lỗi K8s pods
```bash
kubectl get pods -n crypto-pipeline
kubectl describe pod <pod-name> -n crypto-pipeline
kubectl logs <pod-name> -n crypto-pipeline
```

---

## Cấu trúc dự án

```
bigdata/
├── crawl/          # Thu thập dữ liệu
├── kafka/          # Kafka cluster (Docker)
├── hdfs/           # Consumer + Storage
├── spark/          # Batch processing
├── elasticsearch/  # Query API
├── k8s/            # Kubernetes manifests
├── docs/           # Tài liệu
├── README.md       # Tài liệu chính
└── PLAN.md         # Kế hoạch dự án
```

---

## Liên hệ

Nếu gặp vấn đề, tạo issue trên GitHub hoặc liên hệ team.
