@echo off
REM Kubernetes Deployment Script for Windows
REM =========================================

echo ============================================================
echo   CRYPTO ANALYTICS PIPELINE - KUBERNETES DEPLOYMENT
echo ============================================================
echo.

REM Check kubectl
where kubectl >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] kubectl not found! Please install kubectl first.
    pause
    exit /b 1
)

cd /d "%~dp0"

echo [1/11] Creating namespace...
kubectl apply -f namespace.yaml

echo [2/11] Creating storage resources...
kubectl apply -f storage/

echo [3/11] Deploying Zookeeper...
kubectl apply -f kafka/zookeeper.yaml
echo Waiting for Zookeeper to be ready...
kubectl wait --for=condition=ready pod -l app=zookeeper -n crypto-pipeline --timeout=120s

echo [4/11] Deploying Kafka cluster (3 brokers)...
kubectl apply -f kafka/kafka-cluster.yaml
echo Waiting for Kafka to be ready...
timeout /t 30 /nobreak >nul

echo [5/11] Creating Kafka topics...
kubectl apply -f kafka/kafka-topics.yaml

echo [6/11] Deploying Elasticsearch cluster (2 nodes)...
kubectl apply -f elasticsearch/elasticsearch-cluster.yaml
echo Waiting for Elasticsearch to be ready...
timeout /t 60 /nobreak >nul

echo [7/11] Setting up Elasticsearch indices...
kubectl apply -f elasticsearch/elasticsearch-setup.yaml

echo [8/11] Deploying Kibana...
kubectl apply -f kibana/kibana.yaml

echo [9/11] Deploying Spark cluster...
kubectl apply -f spark/spark-master.yaml
kubectl apply -f spark/spark-worker.yaml

echo [10/11] Deploying Query API...
kubectl apply -f api/query-api.yaml

echo [11/11] Deploying Crypto Crawler CronJob...
kubectl apply -f crawler/crawler-cronjob.yaml

echo.
echo ============================================================
echo   DEPLOYMENT COMPLETE!
echo ============================================================
echo.
echo Waiting for pods to be ready (this may take a few minutes)...
echo.

echo Checking Zookeeper...
kubectl wait --for=condition=ready pod -l app=zookeeper -n crypto-pipeline --timeout=180s 2>nul || echo Zookeeper not ready yet

echo Checking Kafka...
kubectl wait --for=condition=ready pod -l app=kafka -n crypto-pipeline --timeout=180s 2>nul || echo Kafka not ready yet

echo Checking Elasticsearch...
kubectl wait --for=condition=ready pod -l app=elasticsearch -n crypto-pipeline --timeout=300s 2>nul || echo ES not ready yet

echo Checking Kibana...
kubectl wait --for=condition=ready pod -l app=kibana -n crypto-pipeline --timeout=180s 2>nul || echo Kibana not ready yet

echo Checking Spark...
kubectl wait --for=condition=ready pod -l app=spark -n crypto-pipeline --timeout=180s 2>nul || echo Spark not ready yet

echo Checking Query API...
kubectl wait --for=condition=ready pod -l app=query-api -n crypto-pipeline --timeout=180s 2>nul || echo API not ready yet

echo.
echo --- POD STATUS ---
kubectl get pods -n crypto-pipeline -o wide

echo.
echo ============================================================
echo   ACCESS SERVICES
echo ============================================================
echo.
echo Option 1: Port-forward (recommended for testing)
echo   kubectl port-forward svc/kibana 5601:5601 -n crypto-pipeline
echo   kubectl port-forward svc/query-api 8000:8000 -n crypto-pipeline
echo   kubectl port-forward svc/elasticsearch 9200:9200 -n crypto-pipeline
echo.
echo Option 2: NodePort URLs (if on cloud with external access)
echo   - Kibana        : http://^<NODE_IP^>:30561
echo   - Query API     : http://^<NODE_IP^>:30800
echo   - Elasticsearch : http://^<NODE_IP^>:30920
echo   - Spark UI      : http://^<NODE_IP^>:30080
echo.
echo Get node IPs: kubectl get nodes -o wide
echo.
echo ============================================================
echo   USEFUL COMMANDS
echo ============================================================
echo   Status    : status.bat
echo   Logs      : kubectl logs -f ^<pod-name^> -n crypto-pipeline
echo   Scale     : kubectl scale deployment spark-worker --replicas=4 -n crypto-pipeline
echo   Delete    : kubectl delete namespace crypto-pipeline
echo.

pause
