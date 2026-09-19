# Flink SQL 血缘提取完整实现方案

## 概述

本文档描述了基于生产环境最佳实践的 Flink SQL 血缘提取系统，采用**8 层架构设计**，支持表级和列级血缘提取。

---

## 一、架构设计（8 层）

```
┌─────────────────────────────────────────┐
│  8. Storage Layer    │ Graph Model      │
├─────────────────────────────────────────┤
│  7. Physical Mapping │ Connector Options│
├─────────────────────────────────────────┤
│  6. Lineage Extractor│ Table + Column   │
├─────────────────────────────────────────┤
│  5. Semantic Layer   │ Catalog Metadata │
├─────────────────────────────────────────┤
│  4. Parsing Layer    │ Flink Parser API │
├─────────────────────────────────────────┤
│  3. Version Router   │ Select Parser    │
├─────────────────────────────────────────┤
│  2. Preprocessor     │ Clean SQL        │
├─────────────────────────────────────────┤
│  1. Input            │ Multi-statement  │
└─────────────────────────────────────────┘
```

### 各层职责

1. **Preprocessing**: 多语句分割、注释去除
2. **Version Routing**: 根据 Flink 版本选择解析器
3. **Parsing**: `FlinkSqlParser.parse()` → `SqlNode` (AST)
4. **Semantic**: Catalog metadata → Validator
5. **Lineage Extraction**: 
   - 表级：Operation Tree 遍历
   - 列级：RelMetadataQuery.getColumnOrigins()
6. **Physical Mapping**: Connector options → 物理实体
7. **Fallback**: sqlglot/ANTLR 后备方案
8. **Storage**: 图模型存储节点和边

---

## 二、两种主要实现路线

### Route A: Flink Parser API（表级血缘）

**优点**: 正确性高、维护性好  
**缺点**: 仅支持表级血缘

```java
TableEnvironment tEnv = TableEnvironment.create(
    EnvironmentSettings.newInstance().inStreamingMode().build());

List<Operation> ops = tEnv.getParser().parse(sql);
for (Operation op : ops) {
    if (op instanceof SinkModifyOperation) {
        SinkModifyOperation sink = (SinkModifyOperation) op;
        String target = sink.getContextResolvedTable()
                           .getIdentifier().asSummaryString();
        Set<String> sources = new LinkedHashSet<>();
        collectSources(sink.getChild(), sources);
    }
}
```

**核心代码**: `FlinkSQLLineageExtractor.java`

### Route B: Calcite + Validator（列级血缘）

**优点**: 支持列级血缘、更详细  
**缺点**: 复杂度高、依赖 Calcite API

```java
RelNode relNode = convertToRelNode(operation);
RelMetadataQuery mq = RelMetadataQuery.standalone();

// Key API call
List<List<String>> columnOrigins = 
    mq.getColumnOrigins(relNode, ordinal);
```

**核心代码**: `ColumnLineageExtractor.java`

---

## 三、物理映射层

将逻辑表名映射到物理存储实体，支持不同 connector：

### Kafka Connector
```java
PhysicalEntity.builder()
    .logicalTableName("user_event")
    .connectorType(ConnectorType.KAFKA)
    .kafkaTopic("user-events-topic")
    .kafkaPartition("0")
    .build();
```

### JDBC Connector
```java
PhysicalEntity.builder()
    .logicalTableName("orders")
    .connectorType(ConnectorType.JDBC_MYSQL)
    .jdbcUrl("jdbc:mysql://localhost:3306/db")
    .jdbcTable("t_orders")
    .columnMappings(Map.of("id", "order_id"))
    .build();
```

### Hive Connector
```java
PhysicalEntity.builder()
    .logicalTableName("sales")
    .connectorType(ConnectorType.HIVE)
    .hiveDatabase("dw")
    .hiveTable("fact_sales")
    .hiveLocation("/user/hive/warehouse/sales")
    .build();
```

---

## 四、Fallback 机制

当 Flink 原生 Parser 无法处理时，使用替代方案：

```java
FallbackLineageExtractor fallback = new FallbackLineageExtractor();
List<TableLineageResult> results = fallback.extractWithFallbacks(sql);
```

支持的后备解析器：
- Superior MySQL Parser (ANTLR-based)
- Superior Presto Parser
- Superior Spark Parser

---

## 五、统一提取器

整合所有功能，提供简单 API：

```java
UnifiedLineageExtractor extractor = new UnifiedLineageExtractor();
LineageAnalysisResult result = extractor.analyze(flinkSql);

// 获取表级血缘
Set<String> sources = result.getSourceTables();
Set<String> targets = result.getTargetTables();

// 获取列级血缘
List<ColumnLineageResult> columns = result.getColumnLineages();

// 构建影响分析
ImpactAnalysis impact = extractor.buildImpactAnalysis("table_a", allResults);
```

---

## 六、使用示例

### 示例 1: 简单 INSERT INTO SELECT

```sql
INSERT INTO order_summary
SELECT o.order_id, u.user_name, o.amount 
FROM orders o 
JOIN users u ON o.user_id = u.user_id;
```

**血缘结果**:
- Source Tables: [orders, users]
- Target Table: order_summary

### 示例 2: 窗口函数

```sql
INSERT INTO window_result
SELECT user_id, amount, 
       SUM(amount) OVER (PARTITION BY user_id ORDER BY ts) as running_sum
FROM stream_table;
```

**血缘结果**:
- Source Tables: [stream_table]
- Target Table: window_result

### 示例 3: Kafka to JDBC

```sql
CREATE TABLE kafka_source (
    user_id STRING,
    event_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'user-events',
    'properties.bootstrap.servers' = 'localhost:9092'
);

CREATE TABLE jdbc_target (
    user_id STRING,
    event_time TIMESTAMP
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://localhost:3306/db',
    'table' = 't_events'
);

INSERT INTO jdbc_target
SELECT user_id, event_time FROM kafka_source;
```

**物理映射**:
- kafka_source → Topic: user-events
- jdbc_target → Table: t_events (MySQL)

---

## 七、依赖配置

```xml
<!-- Flink SQL Parser for lineage extraction -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-sql-parser</artifactId>
    <version>${flink.version}</version>
    <classifier>java11</classifier>
</dependency>

<!-- Flink Table Validator for column-level lineage -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-common</artifactId>
    <version>${flink.version}</version>
</dependency>

<!-- ANTLR-based fallback parsers -->
<dependency>
    <groupId>io.github.melin.superior</groupId>
    <artifactId>superior-spark-parser</artifactId>
    <version>4.0.16</version>
</dependency>
```

---

## 八、测试运行

```bash
mvn test -Dtest=LineageExtractorTest
```

测试覆盖：
1. 简单 INSERT INTO SELECT
2. 复杂 JOIN 查询
3. 窗口函数查询
4. Fallback 解析器测试

---

## 九、文件结构

```
D:\project\flinkLearn\
├── src\main\java\com\bigdata\lineage\
│   ├── parser\
│   │   ├── FlinkSQLLineageExtractor.java    # Route A: Table-level
│   │   ├── ColumnLineageExtractor.java      # Route B: Column-level
│   │   ├── UnifiedLineageExtractor.java     # Integration layer
│   │   └── FallbackLineageExtractor.java    # Fallback mechanism
│   └── model\
│       ├── PhysicalMapping.java             # Physical entity model
│       └── TableLineage.java                # Basic lineage model
└── src\test\java\com\bigdata\lineage\test\
    └── LineageExtractorTest.java            # Test cases
```

---

## 十、下一步优化

1. ✅ 表级血缘提取（Route A）
2. ✅ 列级血缘提取（Route B）
3. ✅ 物理映射层
4. ✅ Fallback 机制
5. ⏳ 集成到实际 Flink Job 中
6. ⏳ 添加数据库存储（已创建 schema）
7. ⏳ REST API 完善
8. ⏳ Web UI 可视化

---

## 十一、参考资源

- Flink Parser API: https://nightlies.apache.org/flink/flink-docs-master/api/java/
- Calcite RelMetadataQuery: https://calcite.apache.org/docs/api.html
- Superior Parser Library: https://github.com/melin0824/Superior-SQL-Parsers

---

## 十二、对比分析

| 方案 | 正确性 | 列级支持 | 复杂度 | 维护性 |
|------|--------|----------|--------|--------|
| Route A (Flink Parser) | ⭐⭐⭐⭐⭐ | ❌ | 低 | 高 |
| Route B (Calcite) | ⭐⭐⭐⭐ | ✅ | 高 | 中 |
| sqlglot | ⭐⭐⭐ | ✅ | 中 | 低 |
| ANTLR | ⭐⭐⭐⭐ | ✅ | 高 | 中 |

**推荐组合**: Route A + Route B + Fallback

---

**文档版本**: 1.0  
**最后更新**: 2026-09-19
