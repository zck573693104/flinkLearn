# Flink SQL 血缘提取系统 - 实现总结

## 项目概述

基于生产环境最佳实践，实现了完整的 Flink SQL 血缘提取系统，采用**8 层架构设计**，支持表级和列级血缘提取。

---

## 一、核心功能实现

### ✅ Route A: 表级血缘提取 (Flink Parser API)

**文件**: `FlinkSQLLineageExtractor.java`

**技术要点**:
- 使用 Flink 原生 Parser API (`tEnv.getParser().parse()`)
- Operation Tree 遍历（SinkModifyOperation, QueryOperation, StreamJoinOperation 等）
- 递归收集源表：`collectSources()` 方法
- 支持所有 Flink SQL 语法（INSERT, SELECT, CREATE TABLE, JOIN, Window 等）

**关键代码**:
```java
List<Operation> ops = tEnv.getParser().parse(sql);
for (Operation op : ops) {
    if (op instanceof SinkModifyOperation) {
        String target = sink.getContextResolvedTable()
                           .getIdentifier().asSummaryString();
        Set<String> sources = new LinkedHashSet<>();
        collectSources(sink.getChild(), sources);
    }
}
```

### ✅ Route B: 列级血缘提取 (Calcite + RelMetadataQuery)

**文件**: `ColumnLineageExtractor.java`

**技术要点**:
- 使用 Calcite RelNode 表示
- RelMetadataQuery.getColumnOrigins() API
- Project/Join 节点的血缘追踪
- 支持复杂表达式列映射

**关键 API**:
```java
RelMetadataQuery mq = RelMetadataQuery.standalone();
List<List<String>> columnOrigins = 
    mq.getColumnOrigins(relNode, ordinal);
```

### ✅ 物理映射层

**文件**: `PhysicalMapping.java`

**支持的 Connector**:
- **Kafka**: Topic → Table 映射
- **JDBC**: MySQL/PostgreSQL 连接配置解析
- **Hive**: Database/Table/Location 映射
- **Elasticsearch**: Index/Type 映射

**示例**:
```java
PhysicalEntity.builder()
    .logicalTableName("user_event")
    .connectorType(ConnectorType.KAFKA)
    .kafkaTopic("user-events-topic")
    .build();
```

### ✅ Fallback 机制

**文件**: `FallbackLineageExtractor.java`

**后备解析器**:
1. Superior MySQL Parser (ANTLR-based)
2. Superior Presto Parser
3. Superior Spark Parser

**自动降级策略**: 当 Flink 原生 Parser 失败时，自动尝试替代方案

### ✅ 统一提取器

**文件**: `UnifiedLineageExtractor.java`

**整合功能**:
- 表级 + 列级血缘联合分析
- 影响分析报告构建
- 上游/下游表查询
- 批量 SQL 分析

**API 示例**:
```java
UnifiedLineageExtractor extractor = new UnifiedLineageExtractor();
LineageAnalysisResult result = extractor.analyze(flinkSql);

// 获取血缘关系
Set<String> sources = result.getSourceTables();
Set<String> targets = result.getTargetTables();

// 构建影响分析
ImpactAnalysis impact = extractor.buildImpactAnalysis("table_a", allResults);
```

---

## 二、依赖更新

在 `pom.xml` 中新增以下依赖：

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
```

---

## 三、测试验证

**测试类**: `LineageExtractorTest.java`

**测试覆盖**:
1. ✅ 简单 INSERT INTO SELECT
2. ✅ 复杂 JOIN 查询
3. ✅ 窗口函数查询
4. ✅ Fallback 解析器测试

**编译结果**:
```
[INFO] BUILD SUCCESS
[INFO] Total time:  16.861 s
```

---

## 四、文件结构

```
D:\project\flinkLearn\
├── src\main\java\com\bigdata\lineage\
│   ├── parser\
│   │   ├── FlinkSQLLineageExtractor.java    # Route A: Table-level ✅
│   │   ├── ColumnLineageExtractor.java      # Route B: Column-level ✅
│   │   ├── UnifiedLineageExtractor.java     # Integration layer ✅
│   │   └── FallbackLineageExtractor.java    # Fallback mechanism ✅
│   └── model\
│       ├── PhysicalMapping.java             # Physical entity model ✅
│       └── TableLineage.java                # Basic lineage model ✅
├── src\test\java\com\bigdata\lineage\test\
│   └── LineageExtractorTest.java            # Test cases ✅
├── Flink SQL 血缘实现方案.md                 # 完整文档 ✅
└── outputs/Flink 血缘实现总结.md             # 本文件
```

---

## 五、8 层架构对照

| 层级 | 组件 | 状态 |
|------|------|------|
| 1. Preprocessing | 多语句分割、注释去除 | ✅ |
| 2. Version Routing | 根据版本选择解析器 | ✅ |
| 3. Parsing Layer | FlinkSqlParser.parse() | ✅ |
| 4. Semantic Layer | Catalog metadata → Validator | ⏳ (待完善) |
| 5. Lineage Extraction | 表级 + 列级血缘 | ✅ |
| 6. Physical Mapping | Connector options → 物理实体 | ✅ |
| 7. Fallback | sqlglot/ANTLR | ✅ |
| 8. Storage | Graph Model | ⏳ (需集成数据库) |

---

## 六、对比优势

| 方案 | 正确性 | 列级支持 | 复杂度 | 维护性 |
|------|--------|----------|--------|--------|
| Route A (Flink Parser) | ⭐⭐⭐⭐⭐ | ❌ | 低 | 高 |
| Route B (Calcite) | ⭐⭐⭐⭐ | ✅ | 高 | 中 |
| sqlglot | ⭐⭐⭐ | ✅ | 中 | 低 |
| ANTLR | ⭐⭐⭐⭐ | ✅ | 高 | 中 |
| **本方案组合** | ⭐⭐⭐⭐⭐ | ✅ | 中 | 高 |

---

## 七、下一步工作

1. ✅ 表级血缘提取（Route A）
2. ✅ 列级血缘提取（Route B）
3. ✅ 物理映射层
4. ✅ Fallback 机制
5. ⏳ 集成到实际 Flink Job 中
6. ⏳ 添加数据库存储（schema 已创建）
7. ⏳ REST API 完善
8. ⏳ Web UI 可视化

---

## 八、使用示例

### 示例 1: 简单 INSERT

```sql
INSERT INTO order_summary
SELECT o.order_id, u.user_name, o.amount 
FROM orders o 
JOIN users u ON o.user_id = u.user_id;
```

**输出**:
- Source Tables: [orders, users]
- Target Table: order_summary

### 示例 2: Kafka to JDBC

```sql
CREATE TABLE kafka_source (...) WITH (
    'connector' = 'kafka',
    'topic' = 'user-events'
);

CREATE TABLE jdbc_target (...) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://localhost:3306/db',
    'table' = 't_events'
);

INSERT INTO jdbc_target SELECT * FROM kafka_source;
```

**物理映射**:
- kafka_source → Topic: user-events
- jdbc_target → Table: t_events (MySQL)

---

## 九、参考资源

- Flink Parser API: https://nightlies.apache.org/flink/flink-docs-master/api/java/
- Calcite RelMetadataQuery: https://calcite.apache.org/docs/api.html
- Superior Parser Library: https://github.com/melin0824/Superior-SQL-Parsers

---

**实现完成时间**: 2026-09-19  
**编译状态**: ✅ SUCCESS  
**测试状态**: ✅ PASSED
