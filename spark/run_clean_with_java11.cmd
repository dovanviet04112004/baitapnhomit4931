@echo off
REM ========================================
REM Spark Batch Analytics - E-commerce
REM ========================================

set "JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.29.7-hotspot"
set "PATH=%JAVA_HOME%\bin;%PATH%"
set SPARK_HOME=

echo Using JAVA_HOME=%JAVA_HOME%
java -version

echo.
echo Running Batch Processing Jobs (13 jobs)...
echo.

"C:\Users\ASUS\AppData\Local\Programs\Python\Python311\python.exe" ^
  "C:\Users\ASUS\Python\bigdata\spark\batch_processing.py" %1

pause