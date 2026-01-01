@echo off
REM Kubernetes Manifest Validation Script
REM =====================================

echo ============================================================
echo   VALIDATING K8S MANIFESTS
echo ============================================================
echo.

cd /d "%~dp0"

REM Check kubectl
where kubectl >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] kubectl not found!
    pause
    exit /b 1
)

echo [1/10] Validating namespace.yaml...
kubectl apply -f namespace.yaml --dry-run=client -o yaml >nul
if %ERRORLEVEL% EQU 0 (echo    OK) else (echo    FAILED)

echo [2/10] Validating storage/...
kubectl apply -f storage/ --dry-run=client -o yaml >nul
if %ERRORLEVEL% EQU 0 (echo    OK) else (echo    FAILED)

echo [3/10] Validating kafka/zookeeper.yaml...
kubectl apply -f kafka/zookeeper.yaml --dry-run=client -o yaml >nul
if %ERRORLEVEL% EQU 0 (echo    OK) else (echo    FAILED)

echo [4/10] Validating kafka/kafka-cluster.yaml...
kubectl apply -f kafka/kafka-cluster.yaml --dry-run=client -o yaml >nul
if %ERRORLEVEL% EQU 0 (echo    OK) else (echo    FAILED)

echo [5/10] Validating kafka/kafka-topics.yaml...
kubectl apply -f kafka/kafka-topics.yaml --dry-run=client -o yaml >nul
if %ERRORLEVEL% EQU 0 (echo    OK) else (echo    FAILED)

echo [6/10] Validating elasticsearch/...
kubectl apply -f elasticsearch/ --dry-run=client -o yaml >nul
if %ERRORLEVEL% EQU 0 (echo    OK) else (echo    FAILED)

echo [7/10] Validating kibana/...
kubectl apply -f kibana/ --dry-run=client -o yaml >nul
if %ERRORLEVEL% EQU 0 (echo    OK) else (echo    FAILED)

echo [8/10] Validating spark/...
kubectl apply -f spark/ --dry-run=client -o yaml >nul
if %ERRORLEVEL% EQU 0 (echo    OK) else (echo    FAILED)

echo [9/10] Validating crawler/...
kubectl apply -f crawler/ --dry-run=client -o yaml >nul
if %ERRORLEVEL% EQU 0 (echo    OK) else (echo    FAILED)

echo [10/10] Validating api/...
kubectl apply -f api/ --dry-run=client -o yaml >nul
if %ERRORLEVEL% EQU 0 (echo    OK) else (echo    FAILED)

echo.
echo ============================================================
echo   VALIDATION COMPLETE
echo ============================================================
echo.
echo All manifests validated successfully (dry-run).
echo.
echo Next steps:
echo   1. Ensure you have a K8s cluster (AKS/GKE/EKS)
echo   2. Run: deploy.bat
echo.
pause
