#!/bin/bash

# GCP Deployment Script - STUDENT/BUDGET MODE
# Cost: ~$15/month (or FREE with $300 credit for 20 months!)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration - STUDENT MODE
PROJECT_ID="crypto-analytics-project"
CLUSTER_NAME="crypto-pipeline-student"
ZONE="asia-southeast1-a"
BUCKET_NAME="crypto-analytics-data-student"

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}  GCP STUDENT MODE - Budget Deployment${NC}"
echo -e "${BLUE}  Cost: ~\$15/month (~93% cheaper!)${NC}"
echo -e "${BLUE}============================================${NC}\n"

echo -e "${YELLOW}⚠️  STUDENT MODE Configuration:${NC}"
echo "  • 1 node (instead of 3)"
echo "  • e2-medium (instead of e2-standard-4)"
echo "  • Preemptible (80% cheaper)"
echo "  • 1 Kafka broker (instead of 3)"
echo "  • 1 ES node (instead of 2)"
echo "  • No LoadBalancer (use port-forward)"
echo ""
read -p "Continue? (y/n): " confirm
if [ "$confirm" != "y" ]; then
    exit 0
fi

# Step 1: Check prerequisites
echo -e "\n${YELLOW}[1/9] Checking prerequisites...${NC}"
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}Error: gcloud CLI not found${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Prerequisites OK${NC}"

# Step 2: Set project
echo -e "\n${YELLOW}[2/9] Setting GCP project...${NC}"
gcloud config set project $PROJECT_ID
echo -e "${GREEN}✓ Project: $PROJECT_ID${NC}"

# Step 3: Enable APIs
echo -e "\n${YELLOW}[3/9] Enabling APIs...${NC}"
gcloud services enable container.googleapis.com compute.googleapis.com storage-api.googleapis.com containerregistry.googleapis.com
echo -e "${GREEN}✓ APIs enabled${NC}"

# Step 4: Create STUDENT cluster
echo -e "\n${YELLOW}[4/9] Creating STUDENT GKE cluster...${NC}"
if gcloud container clusters describe $CLUSTER_NAME --zone=$ZONE &> /dev/null; then
    echo "Cluster exists. Connecting..."
    gcloud container clusters get-credentials $CLUSTER_NAME --zone=$ZONE
else
    echo "Creating BUDGET cluster (this takes ~5 min)..."
    gcloud container clusters create $CLUSTER_NAME \
      --zone=$ZONE \
      --num-nodes=1 \
      --machine-type=e2-medium \
      --disk-size=30GB \
      --disk-type=pd-standard \
      --enable-autoscaling \
      --min-nodes=1 \
      --max-nodes=2 \
      --preemptible \
      --no-enable-cloud-logging \
      --no-enable-cloud-monitoring
    
    gcloud container clusters get-credentials $CLUSTER_NAME --zone=$ZONE
fi
echo -e "${GREEN}✓ Cluster ready${NC}"

# Step 5: Create bucket
echo -e "\n${YELLOW}[5/9] Creating storage bucket...${NC}"
if gsutil ls -b gs://$BUCKET_NAME &> /dev/null; then
    echo "Bucket exists"
else
    gsutil mb -c STANDARD -l asia-southeast1 gs://$BUCKET_NAME
fi
echo -e "${GREEN}✓ Bucket ready${NC}"

# Step 6: Build images
echo -e "\n${YELLOW}[6/9] Building Docker images...${NC}"
gcloud auth configure-docker

echo "Building crawler..."
docker build -t gcr.io/$PROJECT_ID/crypto-crawler:student -f ./crawl/Dockerfile ./crawl
docker push gcr.io/$PROJECT_ID/crypto-crawler:student

echo "Building spark..."
docker build -t gcr.io/$PROJECT_ID/spark-jobs:student -f ./spark/Dockerfile ./spark
docker push gcr.io/$PROJECT_ID/spark-jobs:student

echo "Building API..."
docker build -t gcr.io/$PROJECT_ID/query-api:student -f ./elasticsearch/Dockerfile ./elasticsearch
docker push gcr.io/$PROJECT_ID/query-api:student

echo -e "${GREEN}✓ Images pushed${NC}"

# Step 7: Create namespace
echo -e "\n${YELLOW}[7/9] Creating namespace...${NC}"
kubectl apply -f k8s/namespace.yaml
echo -e "${GREEN}✓ Namespace created${NC}"

# Step 8: Deploy services - MINIMAL CONFIG
echo -e "\n${YELLOW}[8/9] Deploying services (minimal)...${NC}"

# Deploy only 1 Kafka broker
echo "Deploying Kafka (1 broker)..."
kubectl apply -f k8s/kafka/zookeeper.yaml

# Create temporary single-broker Kafka config
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Service
metadata:
  name: kafka-headless
  namespace: crypto-pipeline
spec:
  clusterIP: None
  selector:
    app: kafka
  ports:
  - port: 9092
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
  namespace: crypto-pipeline
spec:
  serviceName: kafka-headless
  replicas: 1
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      containers:
      - name: kafka
        image: confluentinc/cp-kafka:7.5.0
        ports:
        - containerPort: 9092
        env:
        - name: KAFKA_ZOOKEEPER_CONNECT
          value: "zookeeper:2181"
        - name: KAFKA_ADVERTISED_LISTENERS
          value: "PLAINTEXT://kafka-0.kafka-headless:9092"
        - name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
          value: "1"
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
EOF

kubectl wait --for=condition=ready pod -l app=kafka -n crypto-pipeline --timeout=300s

# Deploy single ES node
echo "Deploying Elasticsearch (1 node)..."
cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: elasticsearch
  namespace: crypto-pipeline
spec:
  serviceName: elasticsearch
  replicas: 1
  selector:
    matchLabels:
      app: elasticsearch
  template:
    metadata:
      labels:
        app: elasticsearch
    spec:
      containers:
      - name: elasticsearch
        image: docker.elastic.co/elasticsearch/elasticsearch:8.13.2
        env:
        - name: discovery.type
          value: single-node
        - name: ES_JAVA_OPTS
          value: "-Xms512m -Xmx512m"
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "1.5Gi"
            cpu: "1"
        ports:
        - containerPort: 9200
---
apiVersion: v1
kind: Service
metadata:
  name: elasticsearch
  namespace: crypto-pipeline
spec:
  selector:
    app: elasticsearch
  ports:
  - port: 9200
EOF

kubectl wait --for=condition=ready pod -l app=elasticsearch -n crypto-pipeline --timeout=600s

# Deploy other minimal services
kubectl apply -f k8s/kibana/kibana.yaml
kubectl apply -f k8s/spark/spark-master.yaml

# Single worker
cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-worker
  namespace: crypto-pipeline
spec:
  replicas: 1
  selector:
    matchLabels:
      app: spark-worker
  template:
    metadata:
      labels:
        app: spark-worker
    spec:
      containers:
      - name: spark-worker
        image: bitnami/spark:3.5
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1"
EOF

kubectl apply -f k8s/api/query-api.yaml

echo -e "${GREEN}✓ Services deployed${NC}"

# Step 9: Instructions
echo -e "\n${YELLOW}[9/9] Setup complete!${NC}"
echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}  DEPLOYMENT COMPLETE - STUDENT MODE${NC}"
echo -e "${GREEN}============================================${NC}\n"

echo "Cluster: $CLUSTER_NAME"
echo "Cost: ~\$0.02/hour (~\$15/month if 24/7)"
echo ""
echo -e "${BLUE}💡 Access Services:${NC}"
echo ""
echo "Since we don't use LoadBalancer, use port-forward:"
echo ""
echo "  # Kibana"
echo "  kubectl port-forward svc/kibana 5601:5601 -n crypto-pipeline &"
echo "  # Then visit: http://localhost:5601"
echo ""
echo "  # Query API"
echo "  kubectl port-forward svc/query-api 8000:8000 -n crypto-pipeline &"
echo "  # Then visit: http://localhost:8000/docs"
echo ""
echo -e "${YELLOW}💰 To STOP cluster (save money):${NC}"
echo "  gcloud container clusters resize $CLUSTER_NAME --num-nodes=0 --zone=$ZONE"
echo ""
echo -e "${GREEN}🚀 To START cluster again:${NC}"
echo "  gcloud container clusters resize $CLUSTER_NAME --num-nodes=1 --zone=$ZONE"
echo ""
echo -e "${RED}🗑️  To DELETE everything:${NC}"
echo "  ./cleanup-gcp.sh"
echo ""
echo -e "${BLUE}📊 Check cost:${NC}"
echo "  https://console.cloud.google.com/billing/"
echo ""
echo -e "${YELLOW}Note: With \$300 free credit, this is FREE for ~20 months!${NC}"
