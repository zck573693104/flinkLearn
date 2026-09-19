# Flink SQL Parser 快速设置脚本 (PowerShell)
# 使用 JDK 17 环境

$ErrorActionPreference = "Stop"

# 设置颜色
function Write-Success { param($msg) Write-Host "✓ $msg" -ForegroundColor Green }
function Write-Info { param($msg) Write-Host "ℹ $msg" -ForegroundColor Cyan }
function Write-Warn { param($msg) Write-Host "⚠ $msg" -ForegroundColor Yellow }
function Write-Error { param($msg) Write-Host "✗ $msg" -ForegroundColor Red }

# 1. 配置 JDK 17
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Flink SQL Parser Setup (JDK 17)" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

$jdkPath = "D:\Program Files\jdk-17.0.1"

if (-not (Test-Path $jdkPath)) {
    Write-Error "JDK 17 not found at: $jdkPath"
    Write-Host "Please install JDK 17 first." -ForegroundColor Yellow
    exit 1
}

$env:JAVA_HOME = $jdkPath
$env:PATH = "$jdkPath\bin;$env:PATH"

Write-Success "JDK 17 configured: $($jdkPath)"
Write-Host "Java version:"
java -version 2>&1

# 2. 进入项目目录
$projectDir = "d:\project\flinkLearn\superior-sql-parser-temp"

if (-not (Test-Path $projectDir)) {
    Write-Warn "Project directory not found. Cloning repository..."
    Write-Info "Cloning from https://github.com/melin/superior-sql-parser.git"
    
    $parentDir = Split-Path $projectDir -Parent
    & git clone https://github.com/melin/superior-sql-parser.git $projectDir
    
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to clone repository"
        exit 1
    }
    
    Write-Success "Repository cloned successfully"
} else {
    Set-Location $projectDir
    Write-Success "Project directory found"
}

# 3. 检查 Git 状态
Write-Host "`nGit Status:" -ForegroundColor Yellow
& git status --short

# 4. 创建新分支（如果不存在）
$branchName = "flink-only-jdk17-dev"
Write-Host "`nChecking branch: $branchName" -ForegroundColor Yellow

$branchExists = & git branch --list $branchName | Measure-Object | Select-Object -ExpandProperty Count

if ($branchExists -eq 0) {
    Write-Info "Creating new branch: $branchName"
    & git checkout -b $branchName
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Branch created successfully"
    } else {
        Write-Warn "Failed to create branch automatically"
        Write-Host "Please run manually: git checkout -b $branchName" -ForegroundColor Yellow
    }
} else {
    Write-Success "Branch already exists"
    & git checkout $branchName
}

# 5. 显示项目结构
Write-Host "`nProject Modules:" -ForegroundColor Yellow
Get-ChildItem -Directory | Where-Object { $_.Name -like "superior-*" } | ForEach-Object {
    Write-Host "  - $($_.Name)" -ForegroundColor Gray
}

# 6. 生成配置文件示例
$configFile = Join-Path $projectDir "pom-jdk17-changes.xml"
Write-Host "`nGenerating configuration template..." -ForegroundColor Yellow

$pomTemplate = @'
<!-- 
POM 修改指南 - JDK 17 升级和模块精简
  
需要修改的位置：
1. properties 部分 (第 33-34 行)
   <maven.compiler.source>17</maven.compiler.source>
   <maven.compiler.target>17</maven.compiler.target>

2. maven-compiler-plugin 配置 (第 184-188 行)
   <source>17</source>
   <target>17</target>
   <release>17</release>

3. modules 部分 (第 14-30 行)
   只保留以下模块：
   - superior-common-parser
   - superior-arithmetic-parser (可选)
   - superior-flink-parser

4. 删除以下目录：
   rm -rf superior-spark-parser
   rm -rf superior-mysql-parser
   rm -rf superior-postgres-parser
   rm -rf superior-presto-parser
   rm -rf superior-trino-parser
   rm -rf superior-sqlserver-parser
   rm -rf superior-oracle-parser
   rm -rf superior-dameng-parser
   rm -rf superior-starrocks-parser
   rm -rf superior-redshift-parser
   rm -rf superior-appjar-parser
-->
'@

Set-Content -Path $configFile -Value $pomTemplate
Write-Success "Configuration template saved to: $configFile"

# 7. 编译测试（可选）
Write-Host "`nCompile Test (Optional)" -ForegroundColor Yellow
Write-Host "Run 'mvn clean compile' to test compilation?" -ForegroundColor Gray
Write-Host "  [Y] Yes - Compile now" -ForegroundColor White
Write-Host "  [N] No - Skip compilation" -ForegroundColor White
Write-Host "  [C] Continue with manual steps" -ForegroundColor White

$choice = Read-Host "Enter your choice"

if ($choice -match "^y$") {
    Write-Info "Starting Maven compilation..."
    & mvn clean compile
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Compilation successful!"
    } else {
        Write-Warn "Compilation failed. Please check the errors above."
        Write-Host "You may need to modify pom.xml first." -ForegroundColor Yellow
    }
}

# 8. 显示下一步操作
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Next Steps" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

Write-Host "1. Edit pom.xml:" -ForegroundColor White
Write-Host "   - Update JDK version to 17" -ForegroundColor Gray
Write-Host "   - Remove non-Flink modules" -ForegroundColor Gray
Write-Host ""

Write-Host "2. Run tests:" -ForegroundColor White
Write-Host "   mvn test" -ForegroundColor Gray
Write-Host ""

Write-Host "3. Commit changes:" -ForegroundColor White
Write-Host "   git add ." -ForegroundColor Gray
Write-Host "   git commit -m 'refactor: upgrade to JDK 17 and keep Flink only'" -ForegroundColor Gray
Write-Host ""

Write-Host "4. Push to remote (optional):" -ForegroundColor White
Write-Host "   git push origin $branchName" -ForegroundColor Gray
Write-Host ""

Write-Host "See FLINK_PARSER_DEVELOPMENT_GUIDE.md for detailed instructions." -ForegroundColor Cyan

Write-Host "`nSetup complete!`n" -ForegroundColor Green
