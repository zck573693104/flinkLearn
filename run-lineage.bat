@echo off
rem Parse all .sql files under a directory, print aggregated input/output tables.
rem Usage:
rem   run-lineage.bat              -- parse .\sql in this project
rem   run-lineage.bat D:\dw\sql    -- parse any absolute directory
chcp 65001 >nul
set "MAVEN_OPTS=-Dfile.encoding=UTF-8"

set "SQL_DIR=%~1"
if "%SQL_DIR%"=="" set "SQL_DIR=sql"

if not exist "%SQL_DIR%" (
    echo [ERROR] directory not found: %SQL_DIR%
    exit /b 1
)

call mvn -q compile org.codehaus.mojo:exec-maven-plugin:3.1.1:java -Dexec.mainClass=com.bigdata.lineage.tools.SqlDirLineageTool -Dexec.args="%SQL_DIR%"
