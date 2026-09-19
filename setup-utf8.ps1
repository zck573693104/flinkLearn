# PowerShell UTF-8 配置脚本
# 运行此脚本后，PowerShell 将使用 UTF-8 编码处理文件

# 设置控制台输出编码为 UTF-8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 设置默认编码
$env:JAVA_TOOL_OPTIONS = "-Dfile.encoding=UTF-8"

Write-Host "✅ PowerShell UTF-8 配置完成！" -ForegroundColor Green
Write-Host ""
Write-Host "提示：现在可以重新编译项目：" -ForegroundColor Yellow
Write-Host "  mvn clean compile -DskipTests" -ForegroundColor Cyan
