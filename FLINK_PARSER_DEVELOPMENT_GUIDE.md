# Flink SQL Parser 开发指南

## 📋 任务概述

基于 [superior-sql-parser](https://github.com/melin/superior-sql-parser) 项目，使用 JDK 17 重新开发一个仅支持 Flink SQL 的解析器。

**当前状态**: 
- ✅ 已克隆仓库到 `d:\project\flinkLearn\superior-sql-parser-temp`
- ✅ 已完成项目分析（见 superior-sql-parser-analysis.md）
- ⚠️ Git 分支创建遇到权限问题，需要手动操作

---

## 🔧 环境配置

### JDK 设置

```bash
# JDK 17 路径
D:\Program Files\jdk-17.0.1

# 设置环境变量（PowerShell）
$env:JAVA_HOME = "D:\Program Files\jdk-17.0.1"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

# 验证
java -version
javac -version
```

### Maven 配置

确保 Maven 也使用 JDK 17：

```powershell
# 在 pom.xml 中配置
<maven.compiler.source>17</maven.compiler.source>
<maven.compiler.target>17</maven.compiler.target>
<release>17</release>
```

---

## 📝 手动操作步骤

### 步骤 1: 创建新分支

打开 PowerShell 或 CMD（建议以管理员身份运行），执行：

```bash
cd d:\project\flinkLearn\superior-sql-parser-temp

# 创建并切换到新分支
git checkout -b flink-only-jdk17-dev

# 如果失败，尝试先 fetch
git fetch origin
git checkout -b flink-only-jdk17-dev origin/branch-4.0
```

### 步骤 2: 精简模块结构

编辑根目录 `pom.xml`，修改 modules 部分：

```xml
<!-- 原配置 -->
<modules>
    <module>superior-common-parser</module>
    <module>superior-arithmetic-parser</module>
    <module>superior-appjar-parser</module>
    <module>superior-spark-parser</module>
    <module>superior-mysql-parser</module>
    <module>superior-postgres-parser</module>
    <module>superior-trino-parser</module>
    <module>superior-presto-parser</module>
    <module>superior-sqlserver-parser</module>
    <module>superior-flink-parser</module>
    <module>superior-oracle-parser</module>
    <module>superior-dameng-parser</module>
    <module>superior-starrocks-parser</module>
    <module>superior-redshift-parser</module>
</modules>

<!-- 修改为只保留 Flink 相关模块 -->
<modules>
    <module>superior-common-parser</module>
    <module>superior-arithmetic-parser</module>
    <module>superior-flink-parser</module>
</modules>
```

### 步骤 3: 升级 JDK 版本

#### 3.1 修改根 pom.xml

找到以下位置进行修改：

**位置 1**: properties 部分（第 33-34 行）
```xml
<properties>
    <maven.compiler.source>17</maven.compiler.source>
    <maven.compiler.target>17</maven.compiler.target>
    <!-- 保持其他属性不变 -->
</properties>
```

**位置 2**: maven-compiler-plugin 配置（第 184-188 行）
```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <configuration>
        <source>17</source>
        <target>17</target>
        <release>17</release>
        <encoding>UTF-8</encoding>
    </configuration>
</plugin>
```

#### 3.2 检查子模块 pom.xml

所有子模块继承父 POM，无需单独修改。

### 步骤 4: 删除不需要的模块

```bash
# 删除非 Flink 相关的模块目录
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
rm -rf superior-appjar-parser  # 可选，如果不需要 Jar 解析

# 提交更改
git add .
git commit -m "refactor: 移除非 Flink 相关模块，只保留核心功能"
```

### 步骤 5: 代码重构（JDK 17 新特性）

#### 5.1 使用 Record 替代 Data Class

在 `superior-common-parser` 中找到适合的数据类，例如 `TableId.kt`：

```kotlin
// 原始代码
data class TableId(
    val catalog: String?,
    val schema: String?,
    val tableName: String
)

// JDK 17 优化 - 使用 Record
record TableId(String? catalog, String? schema, String tableName)
```

**注意**: Kotlin 的 data class 已经很优秀，Record 迁移不是必须的，但可以作为学习实践。

#### 5.2 使用 Switch 表达式

查找现有的 when 表达式，可以优化为 switch 表达式：

```kotlin
// 原始代码
val result = when (type) {
    "CREATE" -> StatementType.CREATE
    "INSERT" -> StatementType.INSERT
    "ALTER" -> StatementType.ALTER
    else -> StatementType.UNKNOWN
}

// JDK 17 可以使用箭头语法（Kotlin 已有类似特性）
```

#### 5.3 使用 Text Blocks（多行字符串）

改进 SQL 示例和测试代码：

```kotlin
// 原始代码
val sql = "SELECT * FROM table " +
          "WHERE id = 1 " +
          "LIMIT 10"

// JDK 17 Text Blocks
val sql = """
    SELECT * FROM table
    WHERE id = 1
    LIMIT 10
""".trimIndent()
```

### 步骤 6: 更新依赖版本

检查并更新依赖到 JDK 17 兼容版本：

```xml
<properties>
    <!-- 保持 ANTLR4 版本不变 -->
    <antlr4.version>4.9.3</antlr4.version>
    
    <!-- 更新 JUnit 到 JUnit 5 -->
    <junit.version>5.10.0</junit.version>
    
    <!-- Guava 最新版本 -->
    <guava.version>33.4.0-jre</guava.version>
    
    <!-- Kotlin 版本（2.4.10 已支持 JDK 17） -->
    <kotlin.version>2.4.10</kotlin.version>
</properties>
```

### 步骤 7: 运行测试

```bash
# 编译项目
mvn clean compile

# 运行测试
mvn test

# 打包
mvn package
```

### 步骤 8: 集成到主项目

将开发好的模块集成到 `flinkLearn` 项目：

```bash
# 方式 1: 复制源代码
xcopy /E /I superior-common-parser\src\main\kotlin d:\project\flinkLearn\src\main\kotlin\io\github\melin\superior\common
xcopy /E /I superior-flink-parser\src\main\kotlin d:\project\flinkLearn\src\main\kotlin\io\github\melin\superior\parser\flink

# 方式 2: 添加为 Maven 依赖（推荐）
# 在项目 pom.xml 中添加依赖
<dependency>
    <groupId>io.github.melin.superior</groupId>
    <artifactId>superior-flink-parser</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 步骤 9: 提交代码

```bash
git add .
git commit -m "feat: 基于 superior-sql-parser 开发 Flink SQL 解析器 (JDK 17)"

# 推送到远程（如果需要）
git push origin flink-only-jdk17-dev
```

---

## 🎯 关键 API 使用

### 基本用法

```kotlin
import io.github.melin.superior.parser.flink.FlinkSqlHelper

// 1. 解析单个 SQL
val sql = """
    CREATE TABLE my_table (
        id INT,
        name STRING,
        WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'filesystem',
        'format' = 'json'
    )
""".trimIndent()

val statement = FlinkSqlHelper.parseStatement(sql)

when (statement) {
    is CreateTable -> {
        println("表名：${statement.tableId}")
        println("列定义：${statement.columnRels.size}")
        println("属性：${statement.properties}")
    }
}

// 2. 解析 DML
val insertSql = """
    INSERT INTO my_table
    SELECT id, name, timestamp FROM source_table
""".trimIndent()

val insertStmt = FlinkSqlHelper.parseStatement(insertSql) as InsertTable
println("输出表：${insertStmt.outputTables}")
println("查询语句：${insertStmt.queryStmt.sql}")

// 3. 批量解析
val multiSqls = """
    CREATE TABLE t1 (...);
    CREATE TABLE t2 (...);
    INSERT INTO t1 SELECT * FROM t2;
""".trimIndent()

val statements = FlinkSqlHelper.parseMultiStatement(multiSqls)
statements.forEach { stmt ->
    println("${stmt.statementType}: ${stmt.sql.take(50)}...")
}

// 4. 语法检查
try {
    FlinkSqlHelper.checkSqlSyntax("INVALID SQL STATEMENT")
} catch (e: ParseException) {
    println("语法错误：${e.message}")
}
```

### 获取血缘关系

```kotlin
val sql = """
    INSERT INTO output_table
    SELECT a.id, b.name
    FROM table_a a
    JOIN table_b b ON a.id = b.a_id
""".trimIndent()

val statement = FlinkSqlHelper.parseStatement(sql) as InsertTable
val queryStmt = statement.queryStmt as QueryStmt

// 输入表（源表）
val inputTables = queryStmt.inputTables
inputTables.forEach { table ->
    println("源表：${table.catalog}.${table.schema}.${table.tableName}")
}

// 输出表（目标表）
val outputTables = statement.outputTables
outputTables.forEach { table ->
    println("目标表：${table.catalog}.${table.schema}.${table.tableName}")
}
```

---

## 📚 参考资源

1. **项目文档**: [README.md](superior-sql-parser-temp/README.md)
2. **ANTLR4 官方文档**: https://www.antlr.org/
3. **Kotlin 官方文档**: https://kotlinlang.org/
4. **JDK 17 新特性**: https://openjdk.java.net/projects/jdk/17/
5. **Flink SQL 文档**: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/flow/

---

## ⚠️ 注意事项

1. **权限问题**: 如果遇到 Git 权限问题，尝试：
   - 以管理员身份运行 PowerShell
   - 关闭 IDE 后重试
   - 直接使用命令行操作

2. **Kotlin 兼容性**: 
   - Kotlin 2.4.10 已完全支持 JDK 17
   - 无需额外配置

3. **ANTLR4 缓存**: 
   - 解析大型 SQL 后建议调用 `AbstractSqlParser.refreshParserCaches()`
   - 可通过环境变量控制：`RELEASE_ANTLR_CACHE_AFTER_PARSING=true`

4. **测试覆盖**:
   - 确保所有现有测试用例通过
   - 补充边界情况测试

5. **向后兼容**:
   - 保持 API 接口不变
   - 逐步迁移代码，避免破坏性变更

---

## 🚀 快速开始脚本

创建 `setup-flink-parser.ps1`：

```powershell
# 设置 JDK 17
$env:JAVA_HOME = "D:\Program Files\jdk-17.0.1"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

# 进入项目目录
Set-Location "d:\project\flinkLearn\superior-sql-parser-temp"

# 创建分支
Write-Host "Creating branch..." -ForegroundColor Green
git checkout -b flink-only-jdk17-dev

# 备份原始配置
Write-Host "Backing up configuration..." -ForegroundColor Yellow
Copy-Item pom.xml pom.xml.backup

# 显示进度
Write-Host "Project setup ready for manual editing." -ForegroundColor Cyan
Write-Host "Please edit pom.xml to:" -ForegroundColor Cyan
Write-Host "  1. Update modules section" -ForegroundColor White
Write-Host "  2. Change JDK version to 17" -ForegroundColor White
Write-Host ""
Write-Host "Then run:" -ForegroundColor Cyan
Write-Host "  mvn clean compile" -ForegroundColor Gray
```

---

## 📊 时间估算

| 任务 | 预计时间 |
|------|---------|
| 环境配置 | 30 分钟 |
| 分支创建与模块精简 | 1 小时 |
| JDK 升级 | 30 分钟 |
| 代码重构 | 2-3 小时 |
| 测试与调试 | 1-2 小时 |
| 集成到主项目 | 1 小时 |
| **总计** | **约 8-10 小时** |

---

## ✅ 验收标准

- [ ] 成功创建 `flink-only-jdk17-dev` 分支
- [ ] 只保留 Flink 相关模块
- [ ] 项目能在 JDK 17 下编译通过
- [ ] 所有测试用例通过
- [ ] API 接口保持向后兼容
- [ ] 代码使用至少 2 个 JDK 17 新特性
- [ ] 集成到 flinkLearn 项目并正常运行

---

**最后更新**: 2026-09-19  
**版本**: v1.0  
**维护者**: AI Assistant
