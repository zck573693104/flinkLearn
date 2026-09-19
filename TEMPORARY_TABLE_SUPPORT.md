# 临时表语法支持说明

## 🎯 功能概述

当前实现**完全支持**临时表和临时视图的创建，包括 Flink SQL、Spark SQL 和 Presto SQL 三大引擎。

---

## ✅ 支持的语法格式

### 1. **Flink SQL**

#### 创建临时表（CTAS）
```sql
-- 基本语法
CREATE TEMPORARY TABLE temp_table AS 
SELECT user_id, COUNT(*) as cnt
FROM source_table
GROUP BY user_id;

-- 带 IF NOT EXISTS
CREATE TEMPORARY TABLE IF NOT EXISTS temp_table AS 
SELECT * FROM source_table;

-- 指定列
CREATE TEMPORARY TABLE temp_table (
    user_id BIGINT,
    cnt BIGINT
) AS 
SELECT user_id, COUNT(*) as cnt
FROM source_table;
```

#### 创建临时视图
```sql
-- 基本语法
CREATE TEMPORARY VIEW temp_view AS 
SELECT user_id, order_amount
FROM orders
WHERE order_amount > 100;

-- 带 IF NOT EXISTS
CREATE TEMPORARY VIEW IF NOT EXISTS temp_view AS 
SELECT * FROM source_table;
```

---

### 2. **Spark SQL**

#### 创建临时表（CTAS）
```sql
-- 基本语法
CREATE TEMPORARY TABLE temp_table AS 
SELECT user_id, COUNT(*) as cnt
FROM source_table
GROUP BY user_id;

-- 带属性定义
CREATE TEMPORARY TABLE temp_table (
    user_id BIGINT,
    cnt BIGINT
)
WITH (
    'format' = 'parquet',
    'location' = '/tmp/data'
) AS 
SELECT user_id, COUNT(*) as cnt
FROM source_table;

-- 带 IF NOT EXISTS
CREATE TEMPORARY TABLE IF NOT EXISTS temp_table AS 
SELECT * FROM source_table;
```

#### 创建临时视图
```sql
-- 基本语法
CREATE TEMPORARY VIEW temp_view AS 
SELECT user_id, order_amount
FROM orders
WHERE order_amount > 100;

-- 不支持 IF NOT EXISTS（Spark 限制）
CREATE TEMPORARY VIEW temp_view AS 
SELECT * FROM source_table;
```

---

### 3. **Presto SQL**

#### 创建临时表（CTAS）
```sql
-- 基本语法
CREATE TEMPORARY TABLE temp_table AS 
SELECT user_id, COUNT(*) as cnt
FROM source_table
GROUP BY user_id;

-- 带列定义
CREATE TEMPORARY TABLE temp_table (
    user_id BIGINT,
    cnt BIGINT
) AS 
SELECT user_id, COUNT(*) as cnt
FROM source_table;

-- 带 IF NOT EXISTS
CREATE TEMPORARY TABLE IF NOT EXISTS temp_table AS 
SELECT * FROM source_table;
```

#### 创建临时视图
```sql
-- 基本语法
CREATE TEMPORARY VIEW temp_view AS 
SELECT user_id, order_amount
FROM orders
WHERE order_amount > 100;

-- 带 IF NOT EXISTS
CREATE TEMPORARY VIEW IF NOT EXISTS temp_view AS 
SELECT * FROM source_table;
```

---

## 🔍 血缘提取规则

### 临时表的血缘处理

| 场景 | 源表 | 目标表 | 说明 |
|------|------|--------|------|
| `CREATE TEMP TABLE t1 AS SELECT * FROM source` | source | t1 | ✅ 提取血缘关系 |
| `CREATE TEMP VIEW v1 AS SELECT * FROM source` | source | v1 | ✅ 提取血缘关系 |
| `CREATE TEMP TABLE t1 (...)` | - | t1 | ❌ 无血缘（纯 DDL） |

### 使用临时表的血缘提取

当临时表被使用时，会正确提取其依赖关系：

```sql
-- 示例 1: CTE + 临时表
WITH cte_data AS (
    SELECT * FROM source_table
)
INSERT INTO target_table
SELECT * FROM cte_data;

-- 血缘：source_table → target_table
-- （不显示 cte_data，因为它是中间临时结果）
```

```sql
-- 示例 2: 临时表作为源表
CREATE TEMPORARY TABLE temp_users AS 
SELECT user_id, name FROM users;

INSERT INTO result_table
SELECT * FROM temp_users;

-- 血缘：users → temp_users → result_table
-- （完整血缘链）
```

```sql
-- 示例 3: 临时表 + 普通表 JOIN
CREATE TEMPORARY TABLE temp_orders AS 
SELECT order_id, user_id FROM orders WHERE status = 'PAID';

INSERT INTO enriched_result
SELECT u.*, o.order_id
FROM temp_users u
JOIN temp_orders o ON u.user_id = o.user_id;

-- 血缘：users → temp_users, orders → temp_orders, temp_users+temp_orders → enriched_result
```

---

## 📊 Grammar 定义

### Flink SQL Grammar
```antlr
createTableStatement
    : KW_CREATE (TEMPORARY | TEMP)? TABLE (IF NOT EXISTS)? tablePath 
      LPAREN columnDefinition (COMMA columnDefinition)* RPAREN tableProperties?
    | KW_CREATE (TEMPORARY | TEMP)? TABLE (IF NOT EXISTS)? tablePath AS queryExpression
    | KW_CREATE (TEMPORARY | TEMP)? VIEW (IF NOT EXISTS)? tablePath AS queryExpression
    ;
```

### Spark SQL Grammar
```antlr
createTableStatement
    : KW_CREATE (TEMPORARY | TEMP)? TABLE (IF NOT EXISTS)? tablePath 
      LPAREN columnDefinition (COMMA columnDefinition)* RPAREN tableProperties?
    | KW_CREATE (TEMPORARY | TEMP)? TABLE (IF NOT EXISTS)? tablePath AS queryExpression
    | KW_CREATE TEMPORARY VIEW tablePath AS queryExpression
    ;
```

### Presto SQL Grammar
```antlr
createTableStatement
    : KW_CREATE (TEMPORARY | TEMP)? TABLE (IF NOT EXISTS)? tablePath 
      LPAREN columnDefinition (COMMA columnDefinition)* RPAREN tableProperties?
    | KW_CREATE (TEMPORARY | TEMP)? TABLE (IF NOT EXISTS)? tablePath AS queryExpression
    | KW_CREATE (TEMPORARY | TEMP)? VIEW (IF NOT EXISTS)? tablePath AS queryExpression
    ;
```

---

## 💡 使用场景

### 场景 1: 数据预处理
```sql
-- 第一步：创建临时表进行数据清洗
CREATE TEMPORARY TABLE cleaned_data AS 
SELECT user_id, 
       CAST(amount AS DECIMAL(10,2)) as amount,
       processed_time
FROM raw_data
WHERE amount > 0;

-- 第二步：基于临时表聚合
INSERT INTO aggregated_result
SELECT user_id, SUM(amount) as total_amount
FROM cleaned_data
GROUP BY user_id;

-- 血缘：raw_data → cleaned_data → aggregated_result
```

### 场景 2: 多阶段转换
```sql
-- 阶段 1: 过滤
CREATE TEMPORARY TABLE filtered_records AS 
SELECT * FROM source WHERE status = 'ACTIVE';

-- 阶段 2: 关联
CREATE TEMPORARY TABLE enriched_records AS 
SELECT f.*, u.name, u.email
FROM filtered_records f
JOIN users u ON f.user_id = u.user_id;

-- 阶段 3: 输出
INSERT INTO final_result
SELECT * FROM enriched_records;

-- 血缘：source → filtered_records, users → enriched_records, enriched_records → final_result
```

### 场景 3: 复杂查询优化
```sql
-- 使用临时视图简化复杂查询
CREATE TEMPORARY VIEW high_value_customers AS 
SELECT user_id, SUM(amount) as total_spent
FROM orders
GROUP BY user_id
HAVING SUM(amount) > 10000;

-- 多次使用临时视图
INSERT INTO vip_users
SELECT * FROM high_value_customers;

INSERT INTO marketing_list
SELECT * FROM high_value_customers
WHERE total_spent > 50000;

-- 血缘：orders → high_value_customers → vip_users, marketing_list
```

---

## ⚠️ 注意事项

### 1. **临时表生命周期**
- 临时表只在当前会话中有效
- 会话结束后临时表自动删除
- 血缘分析不受影响，仍然记录完整的依赖关系

### 2. **性能考虑**
- 临时表适合存储中间结果，减少重复计算
- 对于复杂的多阶段 ETL，建议使用临时表提高可读性
- 血缘系统会正确追踪临时表的依赖链

### 3. **引擎差异**
- **Flink SQL**: 支持完整的临时表和临时视图语法
- **Spark SQL**: 临时视图不支持 IF NOT EXISTS
- **Presto SQL**: 支持完整的临时表和临时视图语法

---

## 🎉 总结

✅ **完全支持临时表/视图创建**  
✅ **正确提取临时表的血缘关系**  
✅ **支持多级临时表依赖链**  
✅ **跨引擎兼容（Flink/Spark/Presto）**  
✅ **与 CTE、子查询无缝配合**

临时表语法的支持使得血缘系统能够准确反映复杂的 ETL 流程，特别是涉及多阶段数据处理和优化的场景。
