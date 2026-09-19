# Superior SQL Parser 项目分析 - JDK 17 升级方案

## 📊 项目概览

| 项目信息 | 详情 |
|---------|------|
| **项目名称** | superior-sql-parser |
| **GitHub** | https://github.com/melin/superior-sql-parser |
| **当前版本** | 4.0.23 |
| **技术栈** | Java + Kotlin + ANTLR4 |
| **原 JDK 版本** | 8 |
| **目标 JDK 版本** | 17 |
| **JDK 路径** | D:\Program Files\jdk-17.0.1 |

## 🎯 核心功能

基于 ANTLR4 的多种数据库 SQL 解析器，用于：
- ✅ DDL 语句提取元数据
- ✅ SQL 权限校验  
- ✅ 表级血缘分析
- ✅ SQL 语法校验

支持数据库：Spark、Flink、Gauss、StarRocks、Oracle、MySQL、PostgreSQL、SQL Server、DB2 等。

---

## 📁 项目结构分析

### 原始模块结构（15 个模块）

```
superior-sql-parser/
├── superior-common-parser          # 公共模块 ⭐ 必需
├── superior-arithmetic-parser      # 算术表达式解析 ⭐ 可选
├── superior-appjar-parser          # AppJar 解析 ❌ 删除
├── superior-spark-parser           # Spark SQL ❌ 删除
├── superior-flink-parser           # Flink SQL ⭐ 必需
├── superior-mysql-parser           # MySQL ❌ 删除
├── superior-postgres-parser        # PostgreSQL ❌ 删除
├── superior-presto-parser          # Presto ❌ 删除
├── superior-trino-parser           # Trino ❌ 删除
├── superior-sqlserver-parser       # SQL Server ❌ 删除
├── superior-oracle-parser          # Oracle ❌ 删除
├── superior-dameng-parser          # 达梦 ❌ 删除
├── superior-starrocks-parser       # StarRocks ❌ 删除
└── superior-redshift-parser        # Redshift ❌ 删除
```

### 精简后模块结构（3 个模块）

```
superior-sql-parser-jdk17/
├── superior-common-parser          # 公共模块（基础数据结构）
├── superior-arithmetic-parser      # 算术表达式解析（可选）
└── superior-flink-parser           # Flink SQL 解析器 ⭐
```

---

## 🔍 核心代码分析

### 1. FlinkSqlHelper.kt - 入口类

**位置**: `superior-flink-parser/src/main/kotlin/io/github/melin/superior/parser/flink/FlinkSqlHelper.kt`

**主要方法**:
```kotlin
// 解析单个 SQL
fun parseStatement(command: String): Statement

// 解析多个 SQL
fun parseMultiStatement(command: String): List<Statement>

// 分割 SQL 语句
fun splitSql(command: String): List<String>

// 语法检查
fun checkSqlSyntax(command: String)

// 获取关键字列表
fun sqlKeywords(): List<String>
```

### 2. FlinkSqlAntlr4Visitor.kt - 核心访问器

**位置**: `superior-flink-parser/src/main/kotlin/io/github/melin/superior/parser/flink/FlinkSqlAntlr4Visitor.kt`

**职责**:
- 继承 `FlinkSqlParserBaseVisitor<Statement>`
- 遍历 ANTLR 语法树
- 提取 SQL 元数据（表名、字段、函数等）
- 构建 Statement 对象

**支持的 SQL 类型**:
- **DDL**: CREATE TABLE, CREATE VIEW, CREATE CATALOG, ALTER TABLE, DROP TABLE 等
- **DML**: INSERT INTO, INSERT OVERWRITE, SELECT
- **其他**: SET, USE, EXPLAIN, DESC, Jar 语句等

### 3. AbstractSqlParser.kt - 缓存管理

**位置**: `superior-flink-parser/src/main/kotlin/io/github/melin/superior/parser/flink/AbstractSqlParser.kt`

使用 AtomicReference 管理 ANTLR 缓存，提升解析性能。

---

## 💾 关键数据结构

### Statement 层次结构

```
Statement (接口)
├── DefaultStatement
├── QueryStmt (SELECT 语句)
├── InsertTable (INSERT INTO/OVERWRITE)
├── InsertMultiTable (多表插入)
├── CreateTable (建表)
├── CreateTableAsSelect (CTAS)
├── CreateView (视图)
├── AlterTable (改表)
├── DropTable (删表)
├── CreateCatalog
├── DropCatalog
├── UseCatalog
├── SetStatement
├── ShowStatement
└── ...
```

### TableId - 表标识符

支持三级命名空间：
```kotlin
data class TableId(
    val catalog: String?,  // 目录
    val schema: String?,   // 模式
    val tableName: String  // 表名
)
```

示例：
- `TableId("catalog", "schema", "table")` - 3 级
- `TableId("schema", "table")` - 2 级
- `TableId("table")` - 1 级

### ColumnRel - 列定义

```kotlin
data class ColumnRel(
    val name: String, 
    val dataType: String?, 
    val comment: String?, 
    val isPrimaryKey: Boolean = false,
    val type: ColumnDefType  // PHYSICAL, METADATA, COMPUTED
)
```

---

## 🚀 API 使用示例

### 1. 解析 Flink DDL

```kotlin
import io.github.melin.superior.parser.flink.FlinkSqlHelper

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
        println("列数：${statement.columnRels.size}")
        println("属性：${statement.properties}")
    }
}
```

### 2. 解析 Flink DML

```kotlin
val insertSql = """
    INSERT INTO output_table
    SELECT a.id, b.name
    FROM table_a a
    JOIN table_b b ON a.id = b.a_id
""".trimIndent()

val insertStmt = FlinkSqlHelper.parseStatement(insertSql) as InsertTable
val queryStmt = insertStmt.queryStmt as QueryStmt

// 获取输入表（源表）
val inputTables = queryStmt.inputTables
inputTables.forEach { table ->
    println("源表：${table.catalog}.${table.schema}.${table.tableName}")
}

// 获取输出表（目标表）
val outputTables = insertStmt.outputTables
outputTables.forEach { table ->
    println("目标表：${table.catalog}.${table.schema}.${table.tableName}")
}
```

### 3. 批量解析

```kotlin
val multiSqls = """
    CREATE TABLE t1 (...);
    CREATE TABLE t2 (...);
    INSERT INTO t1 SELECT * FROM t2;
""".trimIndent()

val statements = FlinkSqlHelper.parseMultiStatement(multiSqls)
statements.forEach { stmt ->
    println("${stmt.statementType}: ${stmt.sql.take(50)}...")
}
```

### 4. 语法检查

```kotlin
try {
    FlinkSqlHelper.checkSqlSyntax("INVALID SQL STATEMENT")
    println("SQL 语法正确")
} catch (e: ParseException) {
    println("语法错误：${e.message}")
}
```

---

## 🔧 JDK 8 → JDK 17 升级方案

### 1. Maven 配置修改

#### 修改位置 1: properties 部分

```xml
<!-- 原配置 (JDK 8) -->
<properties>
    <maven.compiler.source>8</maven.compiler.source>
    <maven.compiler.target>8</maven.compiler.target>
    ...
</properties>

<!-- 新配置 (JDK 17) -->
<properties>
    <maven.compiler.source>17</maven.compiler.source>
    <maven.compiler.target>17</maven.compiler.target>
    ...
</properties>
```

#### 修改位置 2: maven-compiler-plugin 配置

```xml
<!-- 原配置 -->
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <configuration>
        <source>1.8</source>
        <target>1.8</target>
        <encoding>UTF-8</encoding>
    </configuration>
</plugin>

<!-- 新配置 -->
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

### 2. JDK 17 新特性利用

#### 2.1 Record 类（简化数据类）

```kotlin
// 原代码 (Kotlin data class)
data class TableId(
    val catalog: String?,
    val schema: String?,
    val tableName: String
)

// JDK 17 Record (Java 类)
public record TableId(String? catalog, String? schema, String tableName)
```

**注意**: Kotlin 的 data class 已经很优秀，Record 迁移不是必须的。

#### 2.2 Switch 表达式

```kotlin
// 原代码
val result = when (type) {
    "CREATE" -> StatementType.CREATE
    "INSERT" -> StatementType.INSERT
    "ALTER" -> StatementType.ALTER
    else -> StatementType.UNKNOWN
}

// JDK 17 可以使用箭头语法（Kotlin 已有类似特性）
```

#### 2.3 Text Blocks（多行字符串）

```kotlin
// 原代码
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

### 3. 依赖兼容性检查

| 依赖 | 当前版本 | JDK 17 版本 | 说明 |
|------|---------|------------|------|
| JUnit | 4.13.2 | 5.10.0 | 建议升级到 JUnit 5 |
| Guava | 33.4.0-jre | 33.4.0-jre | ✅ 已兼容 |
| SLF4J | 2.0.18 | 2.0.18 | ✅ 已兼容 |
| Log4j2 | 2.25.3 | 2.25.3 | ✅ 已兼容 |
| ANTLR4 | 4.9.3 | 4.9.3 | ✅ 已兼容 |
| Kotlin | 2.4.10 | 2.4.10 | ✅ 已兼容 |

---

## 📋 开发步骤

### Step 1: 环境准备

```bash
# 设置 JDK 17
$env:JAVA_HOME = "D:\Program Files\jdk-17.0.1"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

# 验证
java -version
javac -version
```

### Step 2: 创建分支

```bash
cd d:\project\flinkLearn\superior-sql-parser-temp

# 创建并切换到新分支
git checkout -b flink-only-jdk17-dev
```

### Step 3: 精简模块

编辑根目录 `pom.xml`，修改 modules 部分：

```xml
<modules>
    <module>superior-common-parser</module>
    <module>superior-arithmetic-parser</module>
    <module>superior-flink-parser</module>
</modules>
```

删除不需要的模块目录：

```bash
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
```

### Step 4: 升级 JDK 版本

按照上述"JDK 8 → JDK 17 升级方案"修改 pom.xml。

### Step 5: 运行测试

```bash
mvn clean compile
mvn test
mvn package
```

### Step 6: 集成到主项目

将开发好的模块集成到 `flinkLearn` 项目：

**方式 1**: 复制源代码
```bash
xcopy /E /I superior-common-parser\src\main\kotlin d:\project\flinkLearn\src\main\kotlin\io\github\melin\superior\common
xcopy /E /I superior-flink-parser\src\main\kotlin d:\project\flinkLearn\src\main\kotlin\io\github\melin\superior\parser\flink
```

**方式 2**: 添加为 Maven 依赖（推荐）
```xml
<dependency>
    <groupId>io.github.melin.superior</groupId>
    <artifactId>superior-flink-parser</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Step 7: 提交代码

```bash
git add .
git commit -m "feat: 基于 superior-sql-parser 开发 Flink SQL 解析器 (JDK 17)"
git push origin flink-only-jdk17-dev
```

---

## 🛠️ 工具与脚本

### 1. 快速设置脚本

已创建 PowerShell 脚本：`setup-flink-parser.ps1`

使用方法：
```powershell
Set-Location d:\project\flinkLearn
.\setup-flink-parser.ps1
```

### 2. 手动操作指南

详细说明文档：`FLINK_PARSER_DEVELOPMENT_GUIDE.md`

---

## ⚠️ 注意事项

### 1. 权限问题

如果遇到 Git 权限问题：
- 以管理员身份运行 PowerShell
- 关闭 IDE 后重试
- 直接使用命令行操作

### 2. Kotlin 兼容性

- Kotlin 2.4.10 已完全支持 JDK 17
- 无需额外配置

### 3. ANTLR4 缓存

- 解析大型 SQL 后建议调用 `AbstractSqlParser.refreshParserCaches()`
- 可通过环境变量控制：`RELEASE_ANTLR_CACHE_AFTER_PARSING=true`

### 4. 测试覆盖

- 确保所有现有测试用例通过
- 补充边界情况测试

### 5. 向后兼容

- 保持 API 接口不变
- 逐步迁移代码，避免破坏性变更

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
- [ ] 只保留 Flink 相关模块（3 个模块）
- [ ] 项目能在 JDK 17 下编译通过
- [ ] 所有测试用例通过
- [ ] API 接口保持向后兼容
- [ ] 代码使用至少 2 个 JDK 17 新特性
- [ ] 集成到 flinkLearn 项目并正常运行

---

## 📚 参考资源

1. **项目文档**: [README.md](https://github.com/melin/superior-sql-parser/blob/master/README.md)
2. **ANTLR4 官方文档**: https://www.antlr.org/
3. **Kotlin 官方文档**: https://kotlinlang.org/
4. **JDK 17 新特性**: https://openjdk.java.net/projects/jdk/17/
5. **Flink SQL 文档**: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/

---

## 📝 生成的文件清单

本次分析创建了以下文件：

| 文件名 | 说明 |
|-------|------|
| `superior-sql-parser-analysis.md` | 项目详细分析报告 |
| `FLINK_PARSER_DEVELOPMENT_GUIDE.md` | 开发指南和手动操作步骤 |
| `setup-flink-parser.ps1` | PowerShell 快速设置脚本 |
| `d:\project\flinkLearn\superior-sql-parser-temp\` | 克隆的原始仓库 |

---

**分析时间**: 2026-09-19  
**分析人**: AI Assistant  
**目标**: 基于 superior-sql-parser 开发 Flink 专用 SQL 解析器（JDK 17）  
**状态**: ✅ 分析完成，待手动执行
