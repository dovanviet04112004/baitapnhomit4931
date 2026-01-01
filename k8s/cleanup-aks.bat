@echo off
REM ============================================================
REM   AZURE AKS CLEANUP - Delete cluster and resources
REM ============================================================

echo.
echo ============================================================
echo   CLEANUP AZURE AKS CLUSTER
echo ============================================================
echo.

set RESOURCE_GROUP=rg-crypto-pipeline
set CLUSTER_NAME=aks-crypto-cluster

echo WARNING: This will delete:
echo   - AKS Cluster: %CLUSTER_NAME%
echo   - Resource Group: %RESOURCE_GROUP%
echo   - ALL data and resources in the cluster
echo.

set /p CONFIRM="Are you sure you want to delete? (yes/no): "
if /i not "%CONFIRM%"=="yes" (
    echo Cancelled.
    pause
    exit /b 0
)

echo.
echo [Step 1/3] Deleting Kubernetes namespace...
kubectl delete namespace crypto-pipeline --ignore-not-found 2>nul

echo.
echo [Step 2/3] Deleting AKS Cluster...
az aks delete --name %CLUSTER_NAME% --resource-group %RESOURCE_GROUP% --yes --no-wait

echo.
echo [Step 3/3] Deleting Resource Group...
az group delete --name %RESOURCE_GROUP% --yes --no-wait

echo.
echo ============================================================
echo   CLEANUP INITIATED
echo ============================================================
echo.
echo Resources are being deleted in the background.
echo This may take a few minutes to complete.
echo.
echo Check Azure Portal for status:
echo   https://portal.azure.com
echo.
pause
