@echo off
chcp 65001 >nul
echo.
echo ============================================================
echo 🚀 CRYPTO QUERY API SERVER
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

REM Check if fastapi installed
"%PYTHON_PATH%" -c "import fastapi" 2>nul
if errorlevel 1 (
    echo 📦 Installing FastAPI and uvicorn...
    "%PYTHON_PATH%" -m pip install fastapi uvicorn --quiet
)

echo.
echo    📍 API: http://localhost:8000
echo    📚 Docs: http://localhost:8000/docs
echo    🔄 ReDoc: http://localhost:8000/redoc
echo.
echo    Press Ctrl+C to stop the server
echo.
echo ============================================================

"%PYTHON_PATH%" query_api.py

pause
