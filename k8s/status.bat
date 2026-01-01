@echo off
REM Quick Status Check Script
REM ==========================

echo ============================================================
echo   CRYPTO PIPELINE STATUS
echo ============================================================
echo.

echo --- NODES ---
kubectl get nodes -o wide
echo.

echo --- NAMESPACE ---
kubectl get ns crypto-pipeline 2>nul || echo Namespace not found!
echo.

echo --- PODS ---
kubectl get pods -n crypto-pipeline -o wide
echo.

echo --- SERVICES ---
kubectl get svc -n crypto-pipeline
echo.

echo --- STATEFULSETS ---
kubectl get statefulsets -n crypto-pipeline
echo.

echo --- DEPLOYMENTS ---
kubectl get deployments -n crypto-pipeline
echo.

echo --- CRONJOBS ---
kubectl get cronjobs -n crypto-pipeline
echo.

echo --- PVCS ---
kubectl get pvc -n crypto-pipeline
echo.

echo --- RECENT EVENTS ---
kubectl get events -n crypto-pipeline --sort-by='.lastTimestamp' | tail -20
echo.

pause
