# Flink 血缘追踪系统实现方案

## 📋 目录

1. [功能概述](#功能概述)
2. [架构设计](#架构设计)
3. [核心代码实现](#核心代码实现)
4. [依赖配置](#依赖配置)
5. [数据库设计](#数据库设计)
6. [使用示例](#使用示例)
7. [扩展功能](#扩展功能)

---

## 功能概述

本系统实现了 Flink SQL 的血缘追踪功能，包括：

- ✅ **表级血缘**：记录表之间的输入输出关系
- ✅ **SQL 解析**：自动从 Flink SQL 中提取源表和目标表
- ✅ **上下游查询**：支持查询任意表的上游/下游表
- ✅ **完整链路**：递归获取完整的血缘链路
- ✅ **REST API**：提供 RESTful 接口供外部调用
- ✅ **数据库存储**：将血缘关系持久化到 MySQL

---

## 架构设计

```
┌─────────────────────────────────────────────────────┐
│              Flink SQL Execution                    │
├─────────────────────────────────────────────────────┤
│  1. JSqlParser → Parse SQL → Extract Table Info     │
│  2. LineageService → Process & Cache                │
│  3. REST Controller → Expose APIs                   │
└─────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────┐
│              MySQL Database                         │
│    - table_lineage (血缘关系表)                     │
│    - table_info (表信息表)                          │
│    - column_lineage (字段血缘表)                    │
└─────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────┐
│           Client Applications                       │
│    - Web Dashboard                                  │
│    - Data Catalog                                   │
│    - Impact Analysis Tool                           │
└─────────────────────────────────────────────────────┘
```

---

## 核心代码实现

### 1. 实体类

#### TableLineage.java
```java
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableLineage {
    private Long id;
    private String jobId;
    private String jobName;
    private String sql;
    private String sourceTable;      // 源表
    private String targetTable;      // 目标表
    private String processType;      // INSERT/SELECT/UPDATE
    private LocalDateTime createTime;
    private String remark;
}
```

### 2. SQL 解析器

#### FlinkSQLTableExtractor.java
```java
public class FlinkSQLTableExtractor {
    
    public LineageInfo extractLineage(String sql) {
        // 1. 提取所有表名
        List<String> tables = extractor.extract(sql);
        
        // 2. 确定 SQL 类型
        String processType = determineProcessType(sql);
        
        // 3. 区分源表和目标表
        if ("INSERT".equals(processType)) {
            targetTable = extractInsertTarget(sql);
            sourceTable = extractSourceTables(sql, tables);
        }
        
        return LineageInfo.builder()
            .sql(sql)
            .processType(processType)
            .sourceTable(sourceTable)
            .targetTable(targetTable)
            .allTables(tables)
            .build();
    }
}
```

### 3. 血缘服务

#### TableLineageService.java
```java
@Service
public class TableLineageService {
    
    // 记录血缘关系
    public TableLineage recordLineage(String jobId, String jobName, String sql) {
        FlinkSQLTableExtractor extractor = new FlinkSQLTableExtractor();
        var lineageInfo = extractor.extractLineage(sql);
        
        return TableLineage.builder()
            .jobId(jobId)
            .jobName(jobName)
            .sourceTable(lineageInfo.getSourceTable())
            .targetTable(lineageInfo.getTargetTable())
            .processType(lineageInfo.getProcessType())
            .build();
    }
    
    // 获取上游表
    public List<String> getUpstreamTables(String targetTable) {
        // 查询所有 source_table != targetTable 的记录
    }
    
    // 获取下游表
    public List<String> getDownstreamTables(String sourceTable) {
        // 查询所有 target_table != sourceTable 的记录
    }
    
    // 获取完整血缘链路（递归）
    public Map<String, List<String>> getFullLineage(String tableName, int depth) {
        // 递归查询上下游
    }
}
```

### 4. REST API

#### TableLineageController.java
```java
@RestController
@RequestMapping("/api/lineage")
public class TableLineageController {
    
    @PostMapping("/record")
    public Map<String, Object> recordLineage(
        @RequestParam String jobId,
        @RequestParam String jobName,
        @RequestParam String sql) {
        // 记录血缘
    }
    
    @GetMapping("/upstream/{tableName}")
    public Map<String, Object> getUpstreamTables(@PathVariable String tableName) {
        // 获取上游表
    }
    
    @GetMapping("/downstream/{tableName}")
    public Map<String, Object> getDownstreamTables(@PathVariable String tableName) {
        // 获取下游表
    }
    
    @GetMapping("/full/{tableName}")
    public Map<String, Object> getFullLineage(
        @PathVariable String tableName,
        @RequestParam(defaultValue = "5") int depth) {
        // 获取完整血缘
    }
}
```

---

## 依赖配置

在 `pom.xml` 中添加以下依赖：

```xml
<dependencies>
    <!-- Spring Boot Web -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
        <version>3.2.0</version>
    </dependency>
    
    <!-- MyBatis -->
    <dependency>
        <groupId>org.mybatis.spring.boot</groupId>
        <artifactId>mybatis-spring-boot-starter</artifactId>
        <version>3.0.3</version>
    </dependency>
    
    <!-- MySQL Connector -->
    <dependency>
        <groupId>com.mysql</groupId>
        <artifactId>mysql-connector-j</artifactId>
        <version>8.0.33</version>
    </dependency>
    
    <!-- Lombok (可选，用于简化代码) -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <version>1.18.30</version>
        <scope>provided</scope>
    </dependency>
</dependencies>

<build>
    <plugins>
        <plugin>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-maven-plugin</artifactId>
            <version>3.2.0</version>
            <executions>
                <execution>
                    <goals>
                        <goal>repackage</goal>
                    </goals>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

---

## 数据库设计

### 1. 表血缘关系表

```sql
CREATE TABLE table_lineage (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    job_id VARCHAR(100) NOT NULL,
    job_name VARCHAR(255),
    sql TEXT,
    source_table VARCHAR(255) NOT NULL,
    target_table VARCHAR(255) NOT NULL,
    process_type VARCHAR(50),
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    remark TEXT,
    
    INDEX idx_source_table (source_table),
    INDEX idx_target_table (target_table)
);
```

### 2. 表信息表

```sql
CREATE TABLE table_info (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    table_name VARCHAR(255) NOT NULL,
    database_name VARCHAR(255),
    table_type VARCHAR(50),
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    remark TEXT,
    
    UNIQUE KEY uk_table_name (table_name, database_name)
);
```

### 3. 字段血缘表（可选）

```sql
CREATE TABLE column_lineage (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    lineage_id BIGINT,
    source_column VARCHAR(255),
    target_column VARCHAR(255),
    
    INDEX idx_lineage_id (lineage_id)
);
```

---

## 使用示例

### 1. 记录血缘关系

```bash
curl -X POST http://localhost:8080/api/lineage/record \
  -d "jobId=job_123" \
  -d "jobName=Kafka to MySQL ETL" \
  -d "sql=INSERT INTO mysql_table SELECT * FROM kafka_topic"
```

响应：
```json
{
  "success": true,
  "data": {
    "id": 1,
    "jobId": "job_123",
    "sourceTable": "kafka_topic",
    "targetTable": "mysql_table",
    "processType": "INSERT"
  }
}
```

### 2. 查询上游表

```bash
curl http://localhost:8080/api/lineage/upstream/mysql_table
```

响应：
```json
{
  "success": true,
  "tableName": "mysql_table",
  "type": "upstream",
  "tables": ["kafka_topic"]
}
```

### 3. 查询下游表

```bash
curl http://localhost:8080/api/lineage/downstream/kafka_topic
```

响应：
```json
{
  "success": true,
  "tableName": "kafka_topic",
  "type": "downstream",
  "tables": ["mysql_table"]
}
```

### 4. 获取完整血缘链路

```bash
curl "http://localhost:8080/api/lineage/full/mysql_table?depth=3"
```

响应：
```json
{
  "success": true,
  "tableName": "mysql_table",
  "lineage": {
    "mysql_table_upstream": ["kafka_topic"],
    "mysql_table_downstream": ["agg_table", "result_table"],
    "kafka_topic_upstream": [],
    "kafka_topic_downstream": ["mysql_table"]
  }
}
```

---

## 扩展功能

### 1. 字段级别血缘

扩展 `column_lineage` 表，记录字段级别的映射关系：

```sql
INSERT INTO column_lineage 
(lineage_id, source_column, target_column) 
VALUES 
(1, 'user_id', 'user_id'),
(1, 'username', 'username'),
(1, 'email', 'email');
```

### 2. Flink ExecutionListener 集成

```java
public class LineageExecutionListener implements JobListener {
    
    @Override
    public void notifyJobStatusChanged(JobStatusEvent event) {
        if (event.getJobStatus() == JobStatus.RUNNING) {
            String jobId = event.getJobID().toString();
            String sql = event.getSql();
            
            // 自动记录血缘
            lineageService.recordLineage(jobId, jobName, sql);
        }
    }
}
```

### 3. 数据影响分析

基于血缘关系，实现数据影响分析：

```java
public Set<String> analyzeImpact(String affectedTable) {
    Set<String> impactedTables = new HashSet<>();
    
    // 获取所有下游表
    Queue<String> queue = new LinkedList<>();
    queue.add(affectedTable);
    
    while (!queue.isEmpty()) {
        String current = queue.poll();
        List<String> downstreams = lineageService.getDownstreamTables(current);
        
        for (String downstream : downstreams) {
            if (!impactedTables.contains(downstream)) {
                impactedTables.add(downstream);
                queue.add(downstream);
            }
        }
    }
    
    return impactedTables;
}
```

---

## 总结

本方案提供了一个完整的 Flink 血缘追踪系统实现，包括：

✅ **核心功能**：表级血缘提取、存储和查询  
✅ **技术栈**：Spring Boot + MyBatis + MySQL  
✅ **API 设计**：RESTful 风格，易于集成  
✅ **可扩展性**：支持字段级别血缘、Flink 集成等  

下一步可以：
1. 添加 Web 管理界面
2. 集成到 Flink 作业调度系统
3. 实现实时血缘更新
4. 添加血缘可视化功能
