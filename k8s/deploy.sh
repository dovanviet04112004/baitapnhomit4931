# Kubernetes Deployment Guide
# ============================

# 1. PREREQUISITES
# ----------------
# - Minikube hoặc Docker Desktop với Kubernetes enabled
# - kubectl CLI installed
# - Đủ resources: 8GB RAM, 4 CPU cores

# 2. START CLUSTER
# ----------------
# Minikube:
minikube start --cpus 4 --memory 8192 --driver=docker

# Docker Desktop:
# Enable Kubernetes trong Settings > Kubernetes

# 3. DEPLOY ORDER (QUAN TRỌNG!)
# -----------------------------
# Deploy theo thứ tự dependency

# Step 1: Create namespace
kubectl apply -f namespace.yaml

# Step 2: Storage
kubectl apply -f storage/

# Step 3: Zookeeper (Kafka dependency)
kubectl apply -f kafka/zookeeper.yaml
kubectl wait --for=condition=ready pod -l app=zookeeper -n crypto-pipeline --timeout=120s

# Step 4: Kafka cluster
kubectl apply -f kafka/kafka-cluster.yaml
kubectl wait --for=condition=ready pod -l app=kafka -n crypto-pipeline --timeout=180s

# Step 5: Kafka topics
kubectl apply -f kafka/kafka-topics.yaml

# Step 6: Elasticsearch
kubectl apply -f elasticsearch/elasticsearch-cluster.yaml
kubectl wait --for=condition=ready pod -l app=elasticsearch -n crypto-pipeline --timeout=300s

# Step 7: Elasticsearch setup (indices)
kubectl apply -f elasticsearch/elasticsearch-setup.yaml

# Step 8: Kibana
kubectl apply -f kibana/kibana.yaml

# Step 9: Spark cluster
kubectl apply -f spark/spark-master.yaml
kubectl apply -f spark/spark-worker.yaml

# Step 10: API service
kubectl apply -f api/query-api.yaml

# Step 11: Crawler (last, after all services ready)
kubectl apply -f crawler/crawler-cronjob.yaml

# 4. VERIFY DEPLOYMENT
# --------------------
kubectl get all -n crypto-pipeline

# 5. ACCESS SERVICES
# ------------------
# Minikube:
minikube service list -n crypto-pipeline

# URLs (NodePort):
# - Kafka: localhost:30092
# - Elasticsearch: localhost:30920
# - Kibana: localhost:30561
# - Spark UI: localhost:30080
# - Query API: localhost:30800

# 6. PORT FORWARDING (Alternative)
# --------------------------------
kubectl port-forward svc/elasticsearch 9200:9200 -n crypto-pipeline &
kubectl port-forward svc/kibana 5601:5601 -n crypto-pipeline &
kubectl port-forward svc/query-api 8000:8000 -n crypto-pipeline &
kubectl port-forward svc/spark-master 8080:8080 -n crypto-pipeline &

# 7. VIEW LOGS
# ------------
kubectl logs -f deployment/kibana -n crypto-pipeline
kubectl logs -f statefulset/kafka -n crypto-pipeline
kubectl logs -f statefulset/elasticsearch -n crypto-pipeline

# 8. TROUBLESHOOTING
# ------------------
# Check pod status:
kubectl describe pod <pod-name> -n crypto-pipeline

# Check events:
kubectl get events -n crypto-pipeline --sort-by='.lastTimestamp'

# Shell into pod:
kubectl exec -it <pod-name> -n crypto-pipeline -- /bin/bash

# 9. CLEANUP
# ----------
# Delete all resources:
kubectl delete namespace crypto-pipeline

# Or delete specific:
kubectl delete -f .

# 10. SCALING
# -----------
# Scale Kafka:
kubectl scale statefulset kafka --replicas=5 -n crypto-pipeline

# Scale Spark workers:
kubectl scale deployment spark-worker --replicas=4 -n crypto-pipeline

# Scale API:
kubectl scale deployment query-api --replicas=3 -n crypto-pipeline
