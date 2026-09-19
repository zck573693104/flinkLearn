# Superior SQL Parser 项目分析总结

## 项目概述

**项目名称**: superior-sql-parser  
**GitHub**: https://github.com/melin/superior-sql-parser  
**版本**: 4.0.23  
**技术栈**: Java 8 + Kotlin + ANTLR4  
**目标 JDK**: JDK 17 (需要升级)

## 核心功能

基于 ANTLR4 的多种数据库 SQL 解析器，获取 SQL 中元数据，可用于：
- DDL 语句提取元数据
- SQL 权限校验
- 表级血缘分析
- SQL 语法校验

支持数据库：Spark、Flink、Gauss、StarRocks、Oracle、MySQL、PostgreSQL、SQL Server、DB2 等。

## 项目结构

```
superior-sql-parser/
├── superior-common-parser          # 公共模块（基础数据结构）
├── superior-arithmetic-parser      # 算术表达式解析
├── superior-appjar-parser          # AppJar 解析
├── superior-spark-parser           # Spark SQL 解析器
├── superior-flink-parser           # Flink SQL 解析器 ⭐
├── superior-mysql-parser           # MySQL 解析器
├── superior-postgres-parser        # PostgreSQL 解析器
├── superior-presto-parser          # Presto 解析器
├── superior-trino-parser           # Trino 解析器
├── superior-sqlserver-parser       # SQL Server 解析器
├── superior-oracle-parser          # Oracle 解析器
├── superior-dameng-parser          # 达梦数据库解析器
├── superior-starrocks-parser       # StarRocks 解析器
└── superior-redshift-parser        # Redshift 解析器
```

## Flink 解析器核心实现

### 1. 入口类：FlinkSqlHelper.kt

主要方法：
- `parseStatement(sql)` - 解析单个 SQL
- `parseMultiStatement(sql)` - 解析多个 SQL
- `splitSql(sql)` - 分割 SQL 语句
- `checkSqlSyntax(sql)` - 语法检查
- `sqlKeywords()` - 获取关键字列表

### 2. 核心访问器：FlinkSqlAntlr4Visitor.kt

继承自 `FlinkSqlParserBaseVisitor<Statement>`

主要职责：
- 遍历 ANTLR 生成的语法树
- 提取 SQL 元数据（表名、字段、函数等）
- 构建 Statement 对象

支持的 SQL 类型：
- **DDL**: CREATE TABLE, CREATE VIEW, CREATE CATALOG, ALTER TABLE, DROP TABLE 等
- **DML**: INSERT INTO, INSERT OVERWRITE, SELECT
- **其他**: SET, USE, EXPLAIN, DESC, Jar 语句等

### 3. 缓存管理：AbstractSqlParser.kt

使用 AtomicReference 管理 ANTLR 缓存，提升解析性能。

## 关键数据结构

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

### TableId

表示表标识符，支持三级命名空间：
```kotlin
TableId(catalog, schema, tableName)  // 3 级
TableId(schema, tableName)            // 2 级
TableId(tableName)                    // 1 级
```

### ColumnRel

表示列定义：
```kotlin
ColumnRel(
    name: String, 
    dataType: String?, 
    comment: String?, 
    isPrimaryKey: Boolean = false,
    type: ColumnDefType  // PHYSICAL, METADATA, COMPUTED
)
```

## API 使用示例

### 1. 解析 Flink SQL

```kotlin
import io.github.melin.superior.parser.flink.FlinkSqlHelper

val sql = "CREATE TABLE my_table (id INT, name STRING) WITH (...)"
val statement = FlinkSqlHelper.parseStatement(sql)

when (statement) {
    is CreateTable -> {
        println("表名：${statement.tableId}")
        println("列数：${statement.columnRels.size}")
        println("属性：${statement.properties}")
    }
    is QueryStmt -> {
        println("输入表：${statement.inputTables}")
        println("限制：${statement.limit}")
    }
}
```

### 2. 批量解析

```kotlin
val sqls = """
    CREATE TABLE t1 (...);
    CREATE TABLE t2 (...);
    INSERT INTO t1 SELECT * FROM t2;
""".trimIndent()

val statements = FlinkSqlHelper.parseMultiStatement(sqls)
statements.forEach { stmt ->
    println("${stmt.statementType}: ${stmt.sql}")
}
```

## 升级计划（JDK 8 → JDK 17）

### 需要修改的配置

#### 1. pom.xml 根配置

```xml
<properties>
    <maven.compiler.source>17</maven.compiler.source>
    <maven.compiler.target>17</maven.compiler.target>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
</properties>
```

#### 2. maven-compiler-plugin 配置

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <configuration>
        <source>17</source>
        <target>17</target>
        <encoding>UTF-8</encoding>
        <release>17</release>
    </configuration>
</plugin>
```

#### 3. 依赖兼容性检查

- JUnit 5 (JUnit 4 → JUnit 5)
- Guava 最新版本（需检查 JDK 17 兼容性）
- SLF4J/Log4j2（已支持 JDK 17）

### JDK 17 新特性利用

1. **Switch 表达式** - 简化代码
2. **Record 类** - 用于不可变的数据类
3. **Text Blocks** - 改进字符串处理
4. **Pattern Matching** - 减少类型转换
5. **Collections.isEmpty()** - 替代 null 检查

## 开发建议

### 1. 只保留 Flink 相关模块

由于需求是"不需要 Spark 运行环境"，可以精简模块：
- ✅ 保留：superior-common-parser, superior-flink-parser
- ❌ 删除：superior-spark-parser 及其他无关数据库解析器
- ⚠️ 可选：superior-arithmetic-parser（如果 Flink 需要）

### 2. 代码重构方向

#### 使用 Record 简化数据结构

```kotlin
// 原代码
data class TableId(
    val catalog: String?,
    val schema: String?,
    val tableName: String
)

// JDK 17 优化
record TableId(String? catalog, String? schema, String tableName)
```

#### 使用 Switch 表达式

```kotlin
// 原代码
val result = when (type) {
    "A" -> value1
    "B" -> value2
    else -> defaultValue
}

// JDK 17 优化
val result = when (type) {
    "A" -> value1
    "B" -> value2
    in listOf("C", "D") -> complexCalculation()
    else -> defaultValue
}
```

### 3. 测试策略

保留现有测试用例，确保向后兼容：
- FlinkSqlParserDdlTest.kt - DDL 测试
- FlinkSqlParserDmlTest.kt - DML 测试
- FlinkCheckSql.kt - 语法检查测试

## 分支创建步骤

由于遇到权限问题，请手动执行以下命令：

```bash
# 1. 进入克隆的目录
cd d:\project\flinkLearn\superior-sql-parser-temp

# 2. 创建新分支（仅保留 Flink）
git checkout -b flink-only-jdk17-dev

# 3. 备份原始 pom.xml
cp pom.xml pom.xml.backup

# 4. 修改主 pom.xml，移除非 Flink 模块
# 编辑 modules 部分，只保留：
# - superior-common-parser
# - superior-arithmetic-parser (可选)
# - superior-flink-parser

# 5. 修改 JDK 版本为 17
# 在 properties 和 maven-compiler-plugin 中设置 source/target/release 为 17

# 6. 提交更改
git add .
git commit -m "refactor: 精简为 Flink 专用解析器，升级至 JDK 17"

# 7. 推送到远程（如果需要）
git push origin flink-only-jdk17-dev
```

## 下一步行动

1. ✅ 已完成：克隆仓库，分析项目结构
2. 🔄 进行中：手动创建分支并修改配置
3. ⏳ 待完成：
   - 修改 Maven 配置升级到 JDK 17
   - 移除非 Flink 模块
   - 代码重构（使用 JDK 17 新特性）
   - 运行测试确保兼容性
   - 集成到现有项目

## 注意事项

1. **权限问题**: 如果遇到 Git 权限问题，尝试：
   - 以管理员身份运行 PowerShell
   - 或直接在 superior-sql-parser-temp 目录操作
   
2. **Kotlin 兼容性**: 确认 Kotlin 版本与 JDK 17 兼容
   - 当前版本：2.4.10（已支持 JDK 17）

3. **ANTLR4 版本**: 4.9.3 支持 JDK 17，无需升级

4. **依赖冲突**: 升级 JDK 后检查所有依赖的兼容性

---

**分析时间**: 2026-09-19  
**分析人**: AI Assistant  
**目标**: 基于 superior-sql-parser 开发 Flink 专用 SQL 解析器（JDK 17）
