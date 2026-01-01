@echo off
REM ============================================================
REM   PORT FORWARD - Access services locally
REM ============================================================

echo.
echo ============================================================
echo   PORT FORWARDING - Access K8s Services
echo ============================================================
echo.
echo Starting port-forwards in background...
echo.

REM Check namespace exists
kubectl get namespace crypto-pipeline >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Namespace crypto-pipeline not found!
    echo Run deploy.bat first.
    pause
    exit /b 1
)

echo Opening services:
echo.
echo   Kibana         : http://localhost:5601
echo   FastAPI        : http://localhost:8000
echo   Elasticsearch  : http://localhost:9200
echo   Spark UI       : http://localhost:8080
echo.

REM Start port-forwards
start /b kubectl port-forward svc/kibana 5601:5601 -n crypto-pipeline
start /b kubectl port-forward svc/query-api 8000:8000 -n crypto-pipeline
start /b kubectl port-forward svc/elasticsearch 9200:9200 -n crypto-pipeline
start /b kubectl port-forward svc/spark-master 8080:8080 -n crypto-pipeline

echo Port-forwards started!
echo.
echo Press any key to stop all port-forwards...
pause >nul

REM Kill all kubectl processes
taskkill /f /im kubectl.exe >nul 2>nul

echo.
echo Port-forwards stopped.
pause
