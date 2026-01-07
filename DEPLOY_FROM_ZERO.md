# 🎓 Hướng Dẫn Deploy GCP Student Mode - Từ Số 0

## 📋 Mục Lục
1. [Prerequisites - Chuẩn Bị](#1-prerequisites---chuẩn-bị)
2. [Tạo Tài Khoản Google Cloud](#2-tạo-tài-khoản-google-cloud)
3. [Cài Đặt Công Cụ](#3-cài-đặt-công-cụ)
4. [Setup Project GCP](#4-setup-project-gcp)
5. [Deploy Lên GCP](#5-deploy-lên-gcp)
6. [Truy Cập Services](#6-truy-cập-services)
7. [Quản Lý & Monitoring](#7-quản-lý--monitoring)
8. [Cleanup](#8-cleanup)

**⏱️ Tổng thời gian: ~30-45 phút**

---

## 1. Prerequisites - Chuẩn Bị

### Những Gì Bạn Cần:

✅ **Máy tính** với ít nhất:
- 8GB RAM
- 20GB disk space trống
- Internet ổn định

✅ **Tài khoản Google** (Gmail)
- Nếu chưa có: https://accounts.google.com/signup

✅ **Thẻ tín dụng/ghi nợ** (để xác thực tài khoản GCP)
- ⚠️ **Google sẽ KHÔNG charge tiền**, chỉ để verify
- Bạn có $300 credit miễn phí!

✅ **Hệ điều hành**:
- Windows 10/11
- hoặc Linux (Ubuntu, Debian)
- hoặc macOS

---

## 2. Tạo Tài Khoản Google Cloud

### Bước 2.1: Đăng Ký GCP

1. **Truy cập**: https://cloud.google.com/free

2. **Click "Get started for free"** (Bắt đầu miễn phí)

3. **Đăng nhập** với tài khoản Google của bạn

4. **Chọn Country** = Vietnam

5. **Accept Terms of Service** ✅

6. **Nhập thông tin thanh toán**:
   ```
   Card Type: Visa/Mastercard
   Card Number: Số thẻ của bạn
   Expiry Date: MM/YY
   CVV: XXX
   
   ⚠️ Google sẽ charge $1-2 để verify, sau đó hoàn lại
   ```

7. **Click "Start my free trial"**

8. **🎉 Xong! Bạn có $300 credit, valid 90 ngày**

### Bước 2.2: Verify Email

1. Check email từ Google Cloud
2. Click link xác nhận
3. Done!

---

## 3. Cài Đặt Công Cụ

### 3.1. Cài Google Cloud SDK

#### **Windows:**

1. **Download installer**:
   - Truy cập: https://cloud.google.com/sdk/docs/install
   - Download: `GoogleCloudSDKInstaller.exe`

2. **Chạy installer**:
   - Double-click file vừa download
   - Next → Next → Install
   - ✅ Check: "Start Cloud SDK Shell"
   - Finish

3. **Verify**:
   ```cmd
   # Mở Command Prompt mới
   gcloud --version
   ```
   
   Kết quả mong đợi:
   ```
   Google Cloud SDK 460.0.0
   ```

#### **Linux/Mac:**

```bash
# Download và install
curl https://sdk.cloud.google.com | bash

# Restart shell
exec -l $SHELL

# Verify
gcloud --version
```

### 3.2. Cài Docker Desktop

#### **Windows:**

1. **Download**:
   - Truy cập: https://www.docker.com/products/docker-desktop
   - Download Docker Desktop for Windows

2. **Cài đặt**:
   - Run installer
   - Chọn: "Use WSL 2 instead of Hyper-V" (recommended)
   - Restart máy khi được yêu cầu

3. **Start Docker Desktop**:
   - Mở Docker Desktop từ Start Menu
   - Đợi Docker khởi động (icon Docker ở system tray)

4. **Verify**:
   ```cmd
   docker --version
   docker ps
   ```

#### **Linux:**

```bash
# Ubuntu/Debian
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# Logout và login lại
docker --version
```

#### **Mac:**

1. Download Docker Desktop for Mac
2. Install như app bình thường
3. Start Docker Desktop

### 3.3. Cài kubectl (Kubernetes CLI)

```bash
# Install qua gcloud (dễ nhất)
gcloud components install kubectl

# Verify
kubectl version --client
```

---

## 4. Setup Project GCP

### Bước 4.1: Đăng Nhập gcloud

```bash
# Login
gcloud auth login
```

1. Browser sẽ mở
2. Chọn tài khoản Google của bạn
3. Click "Allow"
4. Xong! Quay lại terminal

### Bước 4.2: Tạo Project Mới

```bash
# List projects hiện tại (nếu có)
gcloud projects list

# Tạo project mới
gcloud projects create crypto-project --name="Crypto Analytics"

# Set làm project mặc định
gcloud config set project crypto-project

# Verify
gcloud config list
```

**Output mong đợi:**
```
[core]
account = your-email@gmail.com
project = crypto-student-project
```

### Bước 4.3: Link Billing Account

1. **Truy cập**: https://console.cloud.google.com/billing

2. **My Projects** → Tìm `crypto-student-project`

3. **Click "Link a billing account"**

4. **Chọn billing account** (tài khoản có $300 credit)

5. **Click "Set account"**

### Bước 4.4: Enable APIs

```bash
# Enable các APIs cần thiết
gcloud services enable container.googleapis.com
gcloud services enable compute.googleapis.com  
gcloud services enable storage-api.googleapis.com
gcloud services enable containerregistry.googleapis.com

# Verify
gcloud services list --enabled
```

**Phải thấy:**
- Kubernetes Engine API
- Compute Engine API
- Cloud Storage API
- Container Registry API

---

## 5. Deploy Lên GCP

### Bước 5.1: Chuẩn Bị Project Code

```bash
# Di chuyển vào thư mục project
cd D:/2025.1/BigData/BTL/baitapnhomit4931

# Verify các file cần thiết
dir k8s\deploy-gcp-student.bat    # Windows
ls k8s/deploy-gcp-student.sh      # Linux/Mac

# Pull code mới nhất (nếu dùng Git)
git pull
```

### Bước 5.2: Update Configuration

**Mở file** `k8s/deploy-gcp-student.bat` (Windows) hoặc `k8s/deploy-gcp-student.sh` (Linux/Mac)

**Sửa dòng này:**
```bash
# TỪ:
PROJECT_ID="crypto-analytics-project"

# THÀNH:
PROJECT_ID="crypto-student-project"  # Tên project bạn vừa tạo
```

**Lưu file** (Ctrl+S)

### Bước 5.3: Chạy Deploy Script

#### **Windows:**

```cmd
cd k8s
deploy-gcp-student.bat
```

#### **Linux/Mac:**

```bash
cd k8s
chmod +x deploy-gcp-student.sh
./deploy-gcp-student.sh
```

### Bước 5.4: Theo Dõi Quá Trình Deploy

Script sẽ thực hiện các bước sau:

**[1/9] Check prerequisites** (10 giây)
- ✅ Kiểm tra gcloud, docker, kubectl

**[2/9] Set project** (5 giây)
- ✅ Set project crypto-student-project

**[3/9] Enable APIs** (30 giây)
- ✅ Enable các APIs cần thiết

**[4/9] Create GKE cluster** (5-8 phút) ⏰
- ⚠️ Bước này lâu nhất! Đi uống nước đi 😊
- ✅ Tạo cluster với 1 node e2-medium

**[5/9] Create storage bucket** (10 giây)
- ✅ Tạo Cloud Storage bucket

**[6/9] Build Docker images** (10-15 phút)
- ✅ Build crawler image
- ✅ Build spark image  
- ✅ Build API image
- ✅ Push lên Google Container Registry

**[7/9] Create namespace** (5 giây)
- ✅ Tạo namespace crypto-pipeline

**[8/9] Deploy services** (3-5 phút)
- ✅ Deploy Kafka
- ✅ Deploy Elasticsearch
- ✅ Deploy Kibana
- ✅ Deploy Spark
- ✅ Deploy API

**[9/9] Setup complete!**
- 🎉 Done!

**Tổng thời gian: ~25-35 phút**

### Bước 5.5: Verify Deployment

```bash
# Check pods
kubectl get pods -n crypto-pipeline

# Kết quả mong đợi (tất cả phải Running):
NAME                          READY   STATUS    RESTARTS   AGE
elasticsearch-0               1/1     Running   0          3m
kafka-0                       1/1     Running   0          4m
kibana-xxx                    1/1     Running   0          2m
query-api-xxx                 1/1     Running   0          1m
spark-master-xxx              1/1     Running   0          2m
spark-worker-xxx              1/1     Running   0          2m
zookeeper-xxx                 1/1     Running   0          5m
```

⚠️ **Nếu có pod status "Pending" hoặc "ContainerCreating"**:
- Đợi thêm 2-3 phút
- Chạy lại: `kubectl get pods -n crypto-pipeline`

⚠️ **Nếu có pod status "Error" hoặc "CrashLoopBackOff"**:
```bash
# Xem logs để debug
kubectl logs <pod-name> -n crypto-pipeline
kubectl describe pod <pod-name> -n crypto-pipeline
```

---

## 6. Truy Cập Services

### Bước 6.1: Qua Public IP - NodePort (Recommended cho Student Mode)

**Bước 1: Tạo Firewall Rule để mở NodePort:**
```bash
# Lấy node tag
NODE_TAG=$(gcloud compute instances list --filter="name~gke-crypto-pipeline" --format="value(tags.items[0])")

# Tạo firewall rule
gcloud compute firewall-rules create allow-nodeport-external \
  --allow tcp:30000-32767 \
  --source-ranges 0.0.0.0/0 \
  --target-tags $NODE_TAG

# Verify
gcloud compute firewall-rules describe allow-nodeport-external
```

⚠️ **Quan trọng**: Nếu không có bước này, bạn sẽ không truy cập được từ internet!

**Bước 2: Lấy External IP của node:**
```bash
kubectl get nodes -o wide
```

**Bước 3: Copy EXTERNAL-IP** (ví dụ: 34.143.219.50)

**Bước 4: Truy cập services:**
- 🔍 **Kibana**: http://EXTERNAL-IP:30561
- 📊 **Query API**: http://EXTERNAL-IP:30800/docs
- ⚡ **Spark Master UI**: http://EXTERNAL-IP:30080
- 🗄️ **Elasticsearch**: http://EXTERNAL-IP:30920

**Ví dụ:**
```
http://34.143.219.50:30561      # Kibana
http://34.143.219.50:30800/docs # API
```

✅ **Ưu điểm**: Truy cập từ bất kỳ đâu, không cần port-forward  
⚠️ **Lưu ý**: IP có thể thay đổi nếu node restart

### Bước 6.2: Port Forward (Method 2 - Secure)

**Mở Terminal 1 - Kibana:**
```bash
kubectl port-forward svc/kibana 5601:5601 -n crypto-pipeline
```

**Mở Terminal 2 - Query API:**
```bash
kubectl port-forward svc/query-api 8000:8000 -n crypto-pipeline
```

**Truy cập:**
- Kibana: http://localhost:5601
- API Docs: http://localhost:8000/docs

✅ **Ưu điểm**: Bảo mật hơn, không expose ra internet  
⚠️ **Giữ terminals chạy!** Đừng Ctrl+C

### Bước 6.3: Cloud Console (Method 3)

1. **Truy cập**: https://console.cloud.google.com/kubernetes/

2. **Chọn cluster**: crypto-pipeline

3. **Click "Connect"**

4. **Cloud Shell** sẽ mở

5. **Run port-forward** trong Cloud Shell:
   ```bash
   kubectl port-forward svc/kibana 5601:5601 -n crypto-pipeline
   ```

6. **Click "Web Preview" → Port 5601**

### Bước 6.4: Upgrade to LoadBalancer (Optional - Tốn thêm ~$20/tháng)

Nếu muốn IP tĩnh và port chuẩn:

```bash
# Convert Kibana to LoadBalancer
kubectl patch svc kibana -n crypto-pipeline -p '{"spec": {"type": "LoadBalancer"}}'

# Đợi 1-2 phút, check external IP
kubectl get svc kibana -n crypto-pipeline

# Truy cập: http://EXTERNAL-IP:5601
```

⚠️ **Chi phí**: Mỗi LoadBalancer tốn ~$20/tháng

### Bước 6.5: Test API

```bash
# Test qua public IP (thay EXTERNAL-IP)
curl http://EXTERNAL-IP:30800/health

# Hoặc test qua localhost (nếu dùng port-forward)
curl http://localhost:8000/health

# Mở browser để xem API docs:
# http://EXTERNAL-IP:30800/docs
```

**Test thành công khi thấy:**
```json
{"status": "ok"}
```

---

## 7. Quản Lý & Monitoring

### 7.1. Check Cluster Status

```bash
# Nodes
kubectl get nodes

# All resources
kubectl get all -n crypto-pipeline

# Pods với details
kubectl get pods -n crypto-pipeline -o wide

# Services
kubectl get svc -n crypto-pipeline
```

### 7.2. View Logs

```bash
# Logs của pod cụ thể
kubectl logs <pod-name> -n crypto-pipeline

# Follow logs (real-time)
kubectl logs -f <pod-name> -n crypto-pipeline

# Logs của tất cả pods của một service
kubectl logs -l app=kafka -n crypto-pipeline
```

### 7.3. Check Cost (Quan Trọng!)

**Console Web:**
1. Truy cập: https://console.cloud.google.com/billing/
2. Chọn billing account
3. Xem "Cost breakdown"

**CLI:**
```bash
# List billing accounts
gcloud billing accounts list

# Project billing info
gcloud billing projects describe crypto-student-project
```

**Set Budget Alert:**
1. https://console.cloud.google.com/billing/budgets
2. **Create Budget**
3. Name: "Student Budget"
4. Amount: $20
5. Alert thresholds: 50%, 90%, 100%
6. Email: your-email@gmail.com
7. **Create**

### 7.4. Resource Usage

```bash
# CPU/Memory usage
kubectl top nodes
kubectl top pods -n crypto-pipeline
```

---

## 8. Cleanup

### 8.1. Stop Cluster (Tạm Thời - Save Money)

```bash
# Stop cluster (không xóa, chỉ scale về 0)
gcloud container clusters resize crypto-pipeline-student \
  --num-nodes=0 \
  --zone=asia-southeast1-a

# Chi phí: ~$1/tháng (chỉ disk storage)
```

**Khi muốn dùng lại:**
```bash
# Start cluster
gcloud container clusters resize crypto-pipeline-student \
  --num-nodes=1 \
  --zone=asia-southeast1-a

# Đợi 2-3 phút
kubectl get pods -n crypto-pipeline
```

### 8.2. Delete Cluster (Hoàn Toàn)

```bash
# Delete cluster
gcloud container clusters delete crypto-pipeline-student \
  --zone=asia-southeast1-a

# Type "y" để confirm

# Delete storage bucket
gsutil rm -r gs://crypto-analytics-data-student

# Delete container images
gcloud container images delete gcr.io/crypto-student-project/crypto-crawler:student --quiet
gcloud container images delete gcr.io/crypto-student-project/spark-jobs:student --quiet
gcloud container images delete gcr.io/crypto-student-project/query-api:student --quiet
```

**Chi phí: $0 sau khi delete**

---

## 🐛 Troubleshooting - Giải Quyết Lỗi

### Lỗi 1: "gcloud: command not found"

**Nguyên nhân:** Chưa cài Google Cloud SDK

**Giải pháp:**
- Windows: Cài lại từ https://cloud.google.com/sdk/docs/install
- Linux/Mac: `curl https://sdk.cloud.google.com | bash`
- Restart terminal

### Lỗi 2: "Permission denied"

**Nguyên nhân:** Chưa login hoặc chưa enable billing

**Giải pháp:**
```bash
# Login lại
gcloud auth login

# Check project
gcloud config get-value project

# Enable billing tại:
# https://console.cloud.google.com/billing/
```

### Lỗi 3: "Quota exceeded"

**Nguyên nhân:** Vượt quota mặc định

**Giải pháp:**
1. Truy cập: https://console.cloud.google.com/iam-admin/quotas
2. Filter: "Compute Engine API"
3. Tìm quota bị exceed
4. Click "Edit quotas" → Request tăng
5. Hoặc chọn region khác ít người dùng hơn

### Lỗi 4: "Pod CrashLoopBackOff"

**Nguyên nhân:** Container lỗi khi start

**Giải pháp:**
```bash
# Xem logs
kubectl logs <pod-name> -n crypto-pipeline

# Xem events
kubectl describe pod <pod-name> -n crypto-pipeline

# Restart pod
kubectl delete pod <pod-name> -n crypto-pipeline
```

### Lỗi 5: "ImagePullBackOff"

**Nguyên nhân:** Không pull được Docker image

**Giải pháp:**
```bash
# Configure docker auth
gcloud auth configure-docker

# Rebuild và push lại image
docker build -t gcr.io/PROJECT_ID/IMAGE_NAME:tag .
docker push gcr.io/PROJECT_ID/IMAGE_NAME:tag
```

### Lỗi 6: "Cannot connect to cluster"

**Nguyên nhân:** kubectl chưa connect đến cluster

**Giải pháp:**
```bash
# Get credentials
gcloud container clusters get-credentials crypto-pipeline-student \
  --zone=asia-southeast1-a

# Verify
kubectl cluster-info
```

### Lỗi 7: "Out of memory"

**Nguyên nhân:** Node không đủ RAM

**Giải pháp:**
```bash
# Scale lên 2 nodes
gcloud container clusters resize crypto-pipeline-student \
  --num-nodes=2 \
  --zone=asia-southeast1-a

# Hoặc upgrade machine type
# (phải recreate cluster)
```

### Lỗi 8: "Cannot access via public IP / Connection timeout"

**Nguyên nhân:** GCP Firewall chưa mở NodePort range

**Giải pháp:**
```bash
# Check firewall rules
gcloud compute firewall-rules list --filter="name~nodeport"

# Nếu chưa có, tạo rule:
NODE_TAG=$(gcloud compute instances list --filter="name~gke-crypto-pipeline" --format="value(tags.items[0])")
gcloud compute firewall-rules create allow-nodeport-external \
  --allow tcp:30000-32767 \
  --source-ranges 0.0.0.0/0 \
  --target-tags $NODE_TAG

# Test lại sau 30 giây
curl http://EXTERNAL-IP:30800/docs
```

⚠️ **Bảo mật**: Rule này mở ports cho toàn internet. Nếu cần hạn chế, thay `0.0.0.0/0` bằng IP của bạn:
```bash
# Chỉ cho phép IP của bạn
gcloud compute firewall-rules create allow-nodeport-myip \
  --allow tcp:30000-32767 \
  --source-ranges YOUR_IP/32 \
  --target-tags $NODE_TAG
```

---

## 📊 Checklist Deploy Thành Công

Sau khi deploy, check list này:

- [ ] **GCP account** có $300 credit
- [ ] **gcloud** installed và login thành công
- [ ] **Docker Desktop** running
- [ ] **kubectl** installed
- [ ] **Project** created và billing enabled
- [ ] **Cluster** created và running (1 node)
- [ ] **Pods** tất cả đều "Running" (7-8 pods)
- [ ] **Port-forward** Kibana thành công
- [ ] **Port-forward** API thành công
- [ ] **Kibana** accessible tại localhost:5601
- [ ] **API docs** accessible tại localhost:8000/docs
- [ ] **Budget alert** đã setup ($20)
- [ ] **Cost** đang ở mức $0.02/giờ (~$15/tháng)

✅ **Nếu tất cả đều check → Deploy thành công!** 🎉

---

## 💡 Tips & Best Practices

### 1. Tiết Kiệm Chi Phí
```bash
# Stop cluster sau khi demo (quan trọng!)
gcloud container clusters resize crypto-pipeline-student --num-nodes=0

# Chỉ chạy khi cần
# → Tiết kiệm ~$12/tháng
```

### 2. Backup Data
```bash
# Export Elasticsearch data trước khi delete
kubectl exec elasticsearch-0 -n crypto-pipeline -- \
  curl -X PUT "localhost:9200/_snapshot/my_backup" \
  -H 'Content-Type: application/json' \
  -d '{"type": "fs", "settings": {"location": "/backup"}}'

# Copy ra local
kubectl cp crypto-pipeline/elasticsearch-0:/backup ./backup
```

### 3. Monitor Cost Daily
```bash
# Check hàng ngày
gcloud billing accounts list

# Hoặc: https://console.cloud.google.com/billing/
```

### 4. Use Screen Recording
- Record màn hình khi demo
- Không cần keep cluster running lâu
- Tiết kiệm tiền!

### 5. Document Everything
- Screenshot mỗi bước
- Note lại các vấn đề gặp phải
- Viết report chi tiết

---

## 📚 Tài Liệu Tham Khảo

### Official Docs:
- **GCP Free Tier**: https://cloud.google.com/free
- **GKE Docs**: https://cloud.google.com/kubernetes-engine/docs
- **gcloud CLI**: https://cloud.google.com/sdk/gcloud/reference

### Project Docs:
- **Deployment Guide**: [GCP_DEPLOYMENT_GUIDE.md](./GCP_DEPLOYMENT_GUIDE.md)
- **Student Mode**: [GCP_STUDENT_MODE.md](./GCP_STUDENT_MODE.md)
- **Cost Comparison**: [COST_COMPARISON.md](./COST_COMPARISON.md)

### Video Tutorials:
- **GCP Getting Started**: https://www.youtube.com/watch?v=IUU5xy4w-M8
- **Kubernetes Basics**: https://www.youtube.com/watch?v=X48VuDVv0do

---

## 🎯 Next Steps

### Sau Khi Deploy Thành Công:

1. **Test Các Chức Năng**
   - Check Kibana dashboard
   - Test API endpoints
   - Verify data flow

2. **Chuẩn Bị Demo**
   - Screenshot/record video
   - Prepare slides
   - Test scenarios

3. **Viết Report**
   - Architecture diagram
   - Cost analysis
   - Lessons learned

4. **Cleanup**
   - Stop/delete cluster
   - Delete unused resources
   - Final cost check

---

## 🆘 Cần Giúp Đỡ?

### Community Support:
- **Stack Overflow**: https://stackoverflow.com/questions/tagged/google-kubernetes-engine
- **GCP Community**: https://www.googlecloudcommunity.com/

### Official Support:
- **GCP Support**: https://cloud.google.com/support
- **Status Dashboard**: https://status.cloud.google.com/

### Project Issues:
- **GitHub Issues**: Tạo issue trong repo
- **Email**: Contact instructor/TA

---

## ✅ Summary

**Bạn đã học:**
1. ✅ Tạo tài khoản GCP và nhận $300 credit
2. ✅ Cài đặt gcloud, Docker, kubectl
3. ✅ Tạo project và enable APIs
4. ✅ Deploy cluster với student mode
5. ✅ Truy cập services qua port-forward
6. ✅ Monitor cost và resources
7. ✅ Cleanup để tiết kiệm tiền

**Chi phí:**
- Development + Demo: **< $5**
- Với $300 credit: **FREE for 20+ months!**

**Thời gian:**
- Initial setup: ~30 phút
- Deploy: ~25 phút
- Total: **< 1 giờ**

---

**🎉 Chúc mừng! Bạn đã deploy thành công project lên GCP với chi phí siêu tiết kiệm!**

**💪 Giờ bạn có thể tự tin demo bài tập lớn của mình!**
