@echo off
echo ========================================
echo Running Lineage System Tests
echo ========================================

REM Read classpath from file
for /f "usebackq delims=" %%i in ("classpath.txt") do set CP=%%i
set CP=%CP%;target\classes;target\test-classes

echo Running LineageSystemTest...
java -cp %CP% com.bigdata.lineage.LineageSystemTest

echo.
echo All tests completed!
