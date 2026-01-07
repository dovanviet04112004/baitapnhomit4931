#!/bin/bash

# GCP Cleanup Script

PROJECT_ID="crypto-analytics-project"
CLUSTER_NAME="crypto-pipeline"
ZONE="asia-southeast1-a"
BUCKET_NAME="crypto-analytics-data"

echo "⚠️  WARNING: This will delete ALL GCP resources!"
echo "   - GKE Cluster: $CLUSTER_NAME"
echo "   - Cloud Storage: gs://$BUCKET_NAME"
echo "   - Container Images in GCR"
echo ""
read -p "Are you sure? (type 'yes' to confirm): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Cleanup cancelled."
    exit 0
fi

echo ""
echo "Starting cleanup..."

# Delete Kubernetes resources
echo "[1/5] Deleting Kubernetes resources..."
kubectl delete namespace crypto-pipeline --ignore-not-found=true

# Delete LoadBalancers explicitly (to avoid lingering forwarding rules)
echo "[2/5] Deleting LoadBalancers..."
kubectl delete svc kibana-lb api-lb -n crypto-pipeline --ignore-not-found=true

# Wait a bit for LBs to be deleted
sleep 10

# Delete GKE cluster
echo "[3/5] Deleting GKE cluster..."
gcloud container clusters delete $CLUSTER_NAME --zone=$ZONE --quiet

# Delete Cloud Storage bucket
echo "[4/5] Deleting Cloud Storage bucket..."
gsutil -m rm -r gs://$BUCKET_NAME

# Delete container images
echo "[5/5] Deleting container images..."
gcloud container images delete gcr.io/$PROJECT_ID/crypto-crawler:latest --quiet
gcloud container images delete gcr.io/$PROJECT_ID/spark-jobs:latest --quiet
gcloud container images delete gcr.io/$PROJECT_ID/query-api:latest --quiet

echo ""
echo "✓ Cleanup complete!"
echo ""
echo "Note: Some resources (like disk snapshots) may persist."
echo "Check GCP Console to verify all resources are deleted:"
echo "https://console.cloud.google.com/home/dashboard?project=$PROJECT_ID"
