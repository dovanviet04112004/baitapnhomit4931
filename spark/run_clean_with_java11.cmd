@echo off
set "JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.29.7-hotspot"
set "PATH=%JAVA_HOME%\bin;%PATH%"
set SPARK_HOME=

echo Using JAVA_HOME=%JAVA_HOME%
java -version

"C:\Users\ASUS\AppData\Local\Programs\Python\Python311\python.exe" ^
  "C:\Users\ASUS\Python\book-bigdata\spark\clean_books.py"

pause