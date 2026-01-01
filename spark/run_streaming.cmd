@echo off
REM ========================================
REM Spark Streaming - Crypto Real-time Analytics
REM ========================================

set "JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.29.7-hotspot"
set "PATH=%JAVA_HOME%\bin;%PATH%"
set SPARK_HOME=

echo Using JAVA_HOME=%JAVA_HOME%
java -version

echo.
echo ============================================================
echo Starting Spark Streaming (Real-time Crypto Analytics)
echo ============================================================
echo.
echo Features:
echo   - Pump/Dump Alerts detection
echo   - Market Sentiment analysis  
echo   - Real-time monitoring
echo.
echo Press Ctrl+C to stop
echo.

"C:\Users\ASUS\AppData\Local\Programs\Python\Python311\python.exe" ^
  "C:\Users\ASUS\Python\bigdata\spark\streaming_processing.py"

pause
