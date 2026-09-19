# SQL 血缘提取系统

基于 Flink/Spark/Presto 双模式架构的血缘提取解决方案，支持自动检测和优雅降级机制。

## 🎯 核心特性

### 双模式架构
- **方案 A（Route A）**: 基于 JSQLParserUtils 的文本解析 - 快速、无依赖、置信度 0.85
- **方案 B（Route B）**: 使用引擎原生 API 获取元数据信息 - 准确、需环境配置、置信度 0.95

### 优雅降级机制
系统默认采用**自动检测 + 优雅降级**：优先尝试原生 API，失败时自动回退到文本解析，确保在任何环境下都能正常工作。

### 支持的引擎
| 引擎 | 原生 API | 文本解析 | 说明 |
|------|---------|---------|------|
| Flink | ✅ 可用 | ✅ 备用 | 本地运行，无需集群 |
| Spark | ⚠️ 需环境 | ✅ 备用 | SparkSession 单例管理 |
| Presto | ❌ 需配置 | ✅ 备用 | 反射加载，无编译依赖 |

## 📦 快速开始

### 1. 添加依赖

项目已包含以下核心依赖（见 `pom.xml`）：

```xml
<!-- JSQLParser for SQL parsing -->
<dependency>
    <groupId>com.github.jsqlparser</groupId>
    <artifactId>jsqlparser</artifactId>
    <version>5.0</version>
</dependency>

<!-- Flink SQL Parser -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-sql-parser</artifactId>
    <version>1.20.3</version>
</dependency>

<!-- Spark SQL (optional for native mode) -->
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.12</artifactId>
    <version>3.5.0</version>
    <scope>provided</scope>
</dependency>

<!-- Presto Parser (optional for native mode) -->
<!-- 如需启用 Presto 原生 API，取消注释以下内容 -->
<!--
<dependency>
    <groupId>io.prestosql</groupId>
    <artifactId>presto-parser</artifactId>
    <version>372</version>
    <scope>provided</scope>
</dependency>
-->
```

### 2. 基本使用

#### 表级血缘提取

```java
String sql = "INSERT INTO t1 SELECT * FROM t2 JOIN t3 ON ...";

// Flink
FlinkSQLLineageExtractor flinkExtractor = new FlinkSQLLineageExtractor();
List<TableLineageResult> flinkResults = flinkExtractor.extractTableLineages(sql);

// Spark
SparkSQLLineageExtractor sparkExtractor = new SparkSQLLineageExtractor();
List<TableLineageResult> sparkResults = sparkExtractor.extractTableLineages(sql);

// Presto
PrestoSQLLineageExtractor prestoExtractor = new PrestoSQLLineageExtractor();
List<TableLineageResult> prestoResults = prestoExtractor.extractTableLineages(sql);
```

#### 统一分析（含置信度评分）

```java
UnifiedLineageExtractor extractor = new UnifiedLineageExtractor();
var result = extractor.analyze(sql);

System.out.println("源表：" + result.getSourceTables());
System.out.println("目标表：" + result.getTargetTables());
System.out.println("置信度：" + result.getConfidence());
System.out.println("等级：" + result.getConfidenceLevel().getZhName());
// HIGH(可信) | MEDIUM(基本可信) | LOW(需谨慎)
```

## 🔧 配置说明

### Flink SQL
✅ **开箱即用** - 本地运行，无需额外配置

```java
// 自动模式（推荐）
FlinkSQLLineageExtractor extractor = new FlinkSQLLineageExtractor();

// 强制使用原生 API
FlinkSQLLineageExtractor extractor = new FlinkSQLLineageExtractor(ExtractionMode.NATIVE_API);

// 强制使用文本解析
FlinkSQLLineageExtractor extractor = new FlinkSQLLineageExtractor(ExtractionMode.TEXT_PARSING);
```

### Spark SQL
⚠️ **需要 SparkSession** - 自动检测并降级

```java
// 自动检测 + 降级
SparkSQLLineageExtractor extractor = new SparkSQLLineageExtractor();
System.out.println(extractor.getExtractionMethod()); 
// NATIVE_CATALYST | TEXT_PARSING
```

### Presto SQL
❌ **需手动添加依赖** - 反射加载避免编译时依赖

在 `pom.xml` 中添加：

```xml
<dependency>
    <groupId>io.prestosql</groupId>
    <artifactId>presto-parser</artifactId>
    <version>372</version>
    <scope>provided</scope>
</dependency>
```

然后自动检测并使用：

```java
PrestoSQLLineageExtractor extractor = new PrestoSQLLineageExtractor();
System.out.println(extractor.getExtractionMethod());
// PRESTO_NATIVE_PARSER | TEXT_PARSING
```

## 📊 置信度评分

系统根据查询复杂度计算置信度分数（0-1）：

| 特征 | 影响 |
|------|------|
| 使用原生 API | +0.10 |
| 简单 INSERT INTO SELECT | +0.05 |
| 包含 CTE | -0.05 |
| 包含 Window Function | -0.03 |
| 包含 Temporal Join | -0.08 |
| Source 表数量 == 0 | -0.20 |
| Source 表数量 > 10 | -0.10 |

**等级划分：**
- **HIGH (≥0.9)**: 可信 - 通常使用原生 API
- **MEDIUM (≥0.7)**: 基本可信 - 复杂查询或 CTE
- **LOW (<0.7)**: 需谨慎 - 包含多种复杂特性

## 🧪 测试验证

运行完整测试套件：

```bash
mvn test -Dtest=LineageExtractorTest
```

预期输出：
```
=== All Tests Completed ===
Total Tests: 8
Passed: 8
Failed: 0
Success Rate: 100%
```

测试覆盖场景：
- ✅ Simple INSERT INTO SELECT
- ✅ Complex JOIN Query
- ✅ Window Function Query
- ✅ WITH CTE Query
- ✅ Multi-Table JOIN (4 tables)
- ✅ LEFT JOIN
- ✅ UNION Query
- ✅ Physical Mapping from CREATE TABLE SQL
- ✅ Confidence Scoring
- ✅ Fallback Parser
- ✅ Dual-Mode Tests (Flink/Spark/Presto)

## 📁 项目结构

```
flinkLearn/
├── src/main/java/com/bigdata/lineage/
│   ├── parser/
│   │   ├── FlinkSQLLineageExtractor.java      # Flink 双模式实现
│   │   ├── SparkSQLLineageExtractor.java      # Spark 双模式实现
│   │   ├── PrestoSQLLineageExtractor.java     # Presto 双模式实现
│   │   ├── SparkSessionManager.java           # SparkSession 单例管理
│   │   ├── PrestoParserManager.java           # Presto 反射加载
│   │   └── PhysicalMapping.java                 # 连接器物理映射
│   ├── LineageSystemTest.java                 # 集成测试
│   └── UnifiedLineageExtractor.java           # 统一分析器
├── outputs/
│   ├── SQL 血缘提取器配置指南.md               # 详细配置文档
│   └── SQL 血缘提取器 - 快速配置卡.md          # 快速参考卡片
├── pom.xml                                     # Maven 配置
└── README.md                                   # 本文件
```

## 🛠️ 故障排查

| 问题 | 解决方案 |
|------|---------|
| Presto 不可用 | 添加 `presto-parser` 依赖 |
| SparkSession 超时 | 检查 Spark 依赖是否在 classpath |
| 置信度过低 | 简化 SQL 或改用原生 API |
| 类找不到 | 运行 `mvn clean install` 重新构建 |

详细故障排查指南请查看 `outputs/SQL 血缘提取器配置指南.md`

## 📚 相关文档

- **详细配置指南**: `outputs/SQL 血缘提取器配置指南.md`
- **快速参考**: `outputs/SQL 血缘提取器 - 快速配置卡.md`
- **实现方案**: `Flink SQL 血缘实现方案.md`
- **实现说明**: `Flink 血缘实现说明.md`

## ✨ 核心优势

1. **双模式架构** - 每个引擎同时支持 NATIVE_API 和 TEXT_PARSING
2. **优雅降级** - 自动检测并回退，确保系统可用性
3. **置信度评分** - 0-1 分数 + 等级标识（HIGH/MEDIUM/LOW）
4. **物理映射** - 支持 17 种连接器自动识别
5. **零侵入** - 不影响现有代码，新增功能可选启用

## 📝 License

Licensed under the Apache License, Version 2.0.

## 🤝 Contributing

如有问题或建议，请提交 Issue 或 Pull Request。

---

**系统版本**: v1.2.0  
**最后更新**: 2026-09-19
