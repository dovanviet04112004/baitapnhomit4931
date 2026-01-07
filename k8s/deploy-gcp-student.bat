@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM GCP Deployment Script - STUDENT/BUDGET MODE
REM Cost: ~$15/month (or FREE with $300 credit!)

echo ============================================
echo   GCP STUDENT MODE - Budget Deployment
echo   Cost: ~$15/month (~93%% cheaper!)
echo ============================================
echo.

REM Configuration
set "PROJECT_ID=crypto-project-bigdata"
set "CLUSTER_NAME=crypto-pipeline"
set "ZONE=asia-southeast1-a"
set "BUCKET_NAME=crypto-analytics-data"

echo WARNING: STUDENT MODE Configuration:
echo   * 1 node (instead of 3)
echo   * e2-medium (instead of e2-standard-4)
echo   * Preemptible (80%% cheaper)
echo   * 1 Kafka broker (instead of 3)
echo   * 1 ES node (instead of 2)
echo   * No LoadBalancer (use port-forward)
echo.
pause

REM Step 1: Check prerequisites
echo [1/9] Checking prerequisites...

set "GCLOUD="
for /f "delims=" %%G in ('where gcloud 2^>nul') do (
  if not defined GCLOUD set "GCLOUD=%%G"
)
if not defined GCLOUD (
  echo Error: gcloud CLI not found. Install Google Cloud CLI and reopen terminal.
  exit /b 1
)
REM remove quotes if any
set "GCLOUD=%GCLOUD:"=%"
echo Using gcloud: [%GCLOUD%]

set "GSUTIL="
for /f "delims=" %%S in ('where gsutil 2^>nul') do (
  if not defined GSUTIL set "GSUTIL=%%S"
)
if not defined GSUTIL (
  echo Error: gsutil not found. Reinstall/repair Google Cloud CLI.
  exit /b 1
)
set "GSUTIL=%GSUTIL:"=%"
echo Using gsutil: [%GSUTIL%]

where kubectl >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
  echo Error: kubectl not found.
  exit /b 1
)

where docker >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
  echo Error: docker not found.
  exit /b 1
)

echo Prerequisites OK
echo.


REM ============================
REM Step 2: Set project
REM ============================
echo [2/9] Setting project...
call "!GCLOUD!" config set project "!PROJECT_ID!"
if !ERRORLEVEL! NEQ 0 (
  echo Error: Failed to set project "!PROJECT_ID!"
  echo.
  echo Available projects:
  call "!GCLOUD!" projects list
  echo.
  pause
  exit /b 1
)
echo Project set successfully: !PROJECT_ID!
echo.

REM ============================
REM Step 3: Enable APIs
REM ============================
echo [3/9] Enabling APIs...
call "!GCLOUD!" services enable container.googleapis.com compute.googleapis.com storage-api.googleapis.com containerregistry.googleapis.com
if !ERRORLEVEL! NEQ 0 (
  echo Error: Failed to enable APIs.
  echo Hint: Make sure billing is enabled for project !PROJECT_ID!
  pause
  exit /b 1
)
echo APIs enabled successfully
echo.

REM ============================
REM Step 4: Create cluster
REM ============================
echo [4/9] Creating STUDENT cluster...
call "!GCLOUD!" container clusters describe "!CLUSTER_NAME!" --zone="!ZONE!" >nul 2>nul

if !ERRORLEVEL! EQU 0 (
  echo Cluster exists. Connecting...
  call "!GCLOUD!" container clusters get-credentials "!CLUSTER_NAME!" --zone="!ZONE!"
  if !ERRORLEVEL! NEQ 0 (
    echo Error: Failed to get cluster credentials
    pause
    exit /b 1
  )
) else (
  echo Creating BUDGET cluster...
  call "!GCLOUD!" container clusters create "!CLUSTER_NAME!" ^
    --zone="!ZONE!" ^
    --num-nodes=1 ^
    --machine-type=e2-standard-4 ^
    --disk-size=50 ^
    --disk-type=pd-standard ^
    --enable-autoscaling ^
    --min-nodes=1 ^
    --max-nodes=4 ^
    --no-enable-cloud-logging ^
    --no-enable-cloud-monitoring

  if !ERRORLEVEL! NEQ 0 (
    echo Error: Failed to create cluster
    pause
    exit /b 1
  )

  call "!GCLOUD!" container clusters get-credentials "!CLUSTER_NAME!" --zone="!ZONE!"
  if !ERRORLEVEL! NEQ 0 (
    echo Error: Failed to get cluster credentials after create
    pause
    exit /b 1
  )
)
echo.

REM ============================
REM Step 5: Create bucket
REM ============================
echo [5/9] Creating bucket...
call "!GSUTIL!" ls -b "gs://!BUCKET_NAME!" >nul 2>nul
if !ERRORLEVEL! NEQ 0 (
  call "!GSUTIL!" mb -c STANDARD -l asia-southeast1 "gs://!BUCKET_NAME!"
  if !ERRORLEVEL! NEQ 0 (
    echo Error: Failed to create bucket gs://!BUCKET_NAME!
    pause
    exit /b 1
  )
)
echo Bucket OK: gs://!BUCKET_NAME!
echo.

REM ============================
REM Step 6: Build images
REM ============================
echo [6/9] Building images...
call "!GCLOUD!" auth configure-docker
if !ERRORLEVEL! NEQ 0 (
  echo Error: gcloud auth configure-docker failed
  pause
  exit /b 1
)

echo Building crawler...
docker build -t gcr.io/!PROJECT_ID!/crypto-crawler:student -f ../crawl/Dockerfile ../crawl
if !ERRORLEVEL! NEQ 0 exit /b 1
docker push gcr.io/!PROJECT_ID!/crypto-crawler:student
if !ERRORLEVEL! NEQ 0 exit /b 1

echo Building spark...
docker build -t gcr.io/!PROJECT_ID!/spark-jobs:student -f ../spark/Dockerfile ../spark
if !ERRORLEVEL! NEQ 0 exit /b 1
docker push gcr.io/!PROJECT_ID!/spark-jobs:student
if !ERRORLEVEL! NEQ 0 exit /b 1

echo Building API...
docker build -t gcr.io/!PROJECT_ID!/query-api:student -f ../elasticsearch/Dockerfile ../elasticsearch
if !ERRORLEVEL! NEQ 0 exit /b 1
docker push gcr.io/!PROJECT_ID!/query-api:student
if !ERRORLEVEL! NEQ 0 exit /b 1
echo.

REM ============================
REM Step 7: Create namespace
REM ============================
echo [7/9] Creating namespace...
kubectl apply -f namespace.yaml
if !ERRORLEVEL! NEQ 0 (
  echo Error: Failed to apply namespace.yaml
  pause
  exit /b 1
)
echo.

REM ============================
REM Step 8: Deploy minimal services
REM ============================
echo [8/9] Deploying services (minimal config)...
kubectl apply -f kafka/zookeeper.yaml
if !ERRORLEVEL! NEQ 0 (
  echo Error: Failed to deploy zookeeper
  pause
  exit /b 1
)
timeout /t 30 /nobreak >nul

echo Deploying minimal Kafka and Elasticsearch...
kubectl apply -f elasticsearch/
kubectl apply -f kibana/
kubectl apply -f spark/spark-master.yaml
kubectl apply -f api/
echo.

REM ============================
REM Step 9: Done
REM ============================
echo [9/9] Setup complete!
echo.
echo ============================================
echo   DEPLOYMENT COMPLETE - STUDENT MODE
echo ============================================
echo.
echo Cluster: !CLUSTER_NAME!
echo Cost: ~$0.02/hour (~$15/month if 24/7)
echo.
echo Access Services (use port-forward):
echo.
echo   kubectl port-forward svc/kibana 5601:5601 -n crypto-pipeline
echo   kubectl port-forward svc/query-api 8000:8000 -n crypto-pipeline
echo.
echo To STOP cluster (save money):
echo   gcloud container clusters resize !CLUSTER_NAME! --num-nodes=0 --zone=!ZONE!
echo.
echo To START cluster:
echo   gcloud container clusters resize !CLUSTER_NAME! --num-nodes=1 --zone=!ZONE!
echo.
echo Note: With $300 free credit, this is FREE for ~20 months!
echo.
pause
