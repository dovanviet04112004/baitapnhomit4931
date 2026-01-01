@echo off
REM ============================================================
REM   AZURE AKS CLUSTER SETUP - Crypto Analytics Pipeline
REM ============================================================
REM Prerequisites: 
REM   1. Azure CLI installed (winget install Microsoft.AzureCLI)
REM   2. Azure account with subscription
REM ============================================================

echo.
echo ============================================================
echo   AZURE KUBERNETES SERVICE (AKS) SETUP
echo ============================================================
echo.

REM ============ CONFIGURATION - CHANGE IF NEEDED ==============
set RESOURCE_GROUP=rg-crypto-pipeline
set CLUSTER_NAME=aks-crypto-cluster
set LOCATION=southeastasia
set NODE_COUNT=3
set NODE_SIZE=Standard_B2s
REM ============================================================

REM Check Azure CLI
where az >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Azure CLI not found!
    echo.
    echo Please install Azure CLI first:
    echo   winget install Microsoft.AzureCLI
    echo.
    echo Or download from:
    echo   https://aka.ms/installazurecliwindows
    echo.
    pause
    exit /b 1
)

echo [Step 1/6] Checking Azure login status...
az account show >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo Not logged in. Opening browser for login...
    az login
    if %ERRORLEVEL% NEQ 0 (
        echo [ERROR] Login failed!
        pause
        exit /b 1
    )
)

echo.
echo Current Azure Account:
echo ----------------------
az account show --query "{Subscription:name, SubscriptionId:id, User:user.name}" -o table
echo.

set /p CONFIRM="Is this the correct subscription? (y/n): "
if /i not "%CONFIRM%"=="y" (
    echo.
    echo To change subscription, run:
    echo   az account list -o table
    echo   az account set --subscription "YOUR_SUBSCRIPTION_NAME"
    echo.
    pause
    exit /b 1
)

echo.
echo [Step 2/6] Creating Resource Group: %RESOURCE_GROUP%...
az group create --name %RESOURCE_GROUP% --location %LOCATION% -o table
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to create resource group!
    pause
    exit /b 1
)

echo.
echo [Step 3/6] Creating AKS Cluster...
echo    Cluster Name: %CLUSTER_NAME%
echo    Location: %LOCATION%
echo    Nodes: %NODE_COUNT% x %NODE_SIZE% (2 vCPU, 4GB RAM each)
echo.
echo    This will take 5-10 minutes...
echo.

az aks create ^
    --resource-group %RESOURCE_GROUP% ^
    --name %CLUSTER_NAME% ^
    --node-count %NODE_COUNT% ^
    --node-vm-size %NODE_SIZE% ^
    --enable-managed-identity ^
    --generate-ssh-keys ^
    --no-wait

echo.
echo Cluster creation started. Waiting for completion...
echo (This may take 5-10 minutes)
echo.

az aks wait --name %CLUSTER_NAME% --resource-group %RESOURCE_GROUP% --created --timeout 1800

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] AKS cluster creation failed!
    echo.
    echo Check Azure Portal for details:
    echo   https://portal.azure.com
    echo.
    pause
    exit /b 1
)

echo.
echo [Step 4/6] Getting cluster credentials...
az aks get-credentials --resource-group %RESOURCE_GROUP% --name %CLUSTER_NAME% --overwrite-existing
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to get credentials!
    pause
    exit /b 1
)

echo.
echo [Step 5/6] Verifying cluster nodes...
kubectl get nodes -o wide

echo.
echo [Step 6/6] Cluster info...
kubectl cluster-info

echo.
echo ============================================================
echo   AKS CLUSTER CREATED SUCCESSFULLY!
echo ============================================================
echo.
echo Cluster Details:
echo   Resource Group : %RESOURCE_GROUP%
echo   Cluster Name   : %CLUSTER_NAME%
echo   Location       : %LOCATION%
echo   Nodes          : %NODE_COUNT% x %NODE_SIZE%
echo.
echo Azure Portal:
echo   https://portal.azure.com/#resource/subscriptions/.../resourceGroups/%RESOURCE_GROUP%
echo.
echo ============================================================
echo   NEXT STEPS
echo ============================================================
echo.
echo   1. Validate manifests:
echo      validate.bat
echo.
echo   2. Deploy pipeline:
echo      deploy.bat
echo.
echo   3. Check status:
echo      status.bat
echo.
echo ============================================================
echo   IMPORTANT - DELETE WHEN DONE TO SAVE MONEY!
echo ============================================================
echo.
echo   cleanup-aks.bat
echo   OR
echo   az group delete --name %RESOURCE_GROUP% --yes --no-wait
echo.
pause
