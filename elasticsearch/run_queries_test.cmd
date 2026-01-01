@echo off
chcp 65001 >nul
echo.
echo ============================================================
echo 🔍 TESTING ELASTICSEARCH QUERIES
echo ============================================================
echo.

cd /d "%~dp0"

REM Set paths
set PYTHON_PATH=C:\Users\ASUS\AppData\Local\Programs\Python\Python311\python.exe

REM Check Python
if not exist "%PYTHON_PATH%" (
    echo ❌ Python không tìm thấy tại: %PYTHON_PATH%
    pause
    exit /b 1
)

REM Run queries test
echo 📊 Running query tests...
echo.
"%PYTHON_PATH%" elasticsearch_queries.py

echo.
pause
