package com.bigdata.lineage.parser;

import com.bigdata.lineage.parser.model.TableLineage;
import com.bigdata.lineage.parser.MultiEngineSQLLineageParser;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 多引擎 SQL 血缘解析器测试
 * 验证 CREATE TABLE、DROP TABLE、ALTER TABLE 等 DDL 语法支持
 */
public class MultiEngineSQLLineageParserTest {

    private final MultiEngineSQLLineageParser parser = new MultiEngineSQLLineageParser();

    // ============================================
    // Flink SQL 测试
    // ============================================

    @Test
    public void testFlinkCreateTableDDL() {
        // 测试：基本 CREATE TABLE DDL（无血缘）
        String sql = "CREATE TABLE my_table (\n" +
                     "    id BIGINT,\n" +
                     "    name STRING,\n" +
                     "    age INT\n" +
                     ") WITH ('connector' = 'filesystem')";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertEquals("my_table", lineages.get(0).getTargetTable());
        assertTrue(lineages.get(0).getSourceTables().isEmpty());
    }

    @Test
    public void testFlinkCreateTableCTAS() {
        // 测试：CREATE TABLE AS SELECT（有血缘）
        String sql = "CREATE TABLE result_table AS\n" +
                     "SELECT user_id, SUM(amount) as total\n" +
                     "FROM orders\n" +
                     "WHERE status = 'PAID'\n" +
                     "GROUP BY user_id";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertEquals("result_table", lineages.get(0).getTargetTable());
        assertEquals(1, lineages.get(0).getSourceTables().size());
        assertTrue(lineages.get(0).getSourceTables().contains("orders"));
    }

    @Test
    public void testFlinkCreateTemporaryTable() {
        // 测试：CREATE TEMPORARY TABLE
        String sql = "CREATE TEMPORARY TABLE temp_data AS\n" +
                     "SELECT * FROM source_table WHERE status = 'ACTIVE'";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertEquals("temp_data", lineages.get(0).getTargetTable());
        assertTrue(lineages.get(0).getSourceTables().contains("source_table"));
    }

    @Test
    public void testFlinkCreateView() {
        // 测试：CREATE VIEW
        String sql = "CREATE VIEW active_users AS\n" +
                     "SELECT user_id, name FROM users WHERE status = 'ACTIVE'";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertEquals("active_users", lineages.get(0).getTargetTable());
        assertTrue(lineages.get(0).getSourceTables().contains("users"));
    }

    @Test
    public void testFlinkDropTable() {
        // 测试：DROP TABLE（无血缘）
        String sql = "DROP TABLE IF EXISTS my_table";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertTrue(lineages.isEmpty(), "无血缘语句应被过滤");
    }

    @Test
    public void testFlinkAlterTableRename() {
        // 测试：ALTER TABLE RENAME（无血缘）
        String sql = "ALTER TABLE old_name RENAME TO new_name";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertTrue(lineages.isEmpty(), "无血缘语句应被过滤");
    }

    @Test
    public void testFlinkAlterTableAddColumn() {
        // 测试：ALTER TABLE ADD COLUMN（无血缘）
        String sql = "ALTER TABLE my_table ADD COLUMN age INT";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertTrue(lineages.isEmpty(), "无血缘语句应被过滤");
    }

    // ============================================
    // Spark SQL 测试
    // ============================================

    @Test
    public void testSparkCreateTableCTAS() {
        // 测试：Spark CREATE TABLE AS SELECT
        String sql = "CREATE TABLE spark_result AS\n" +
                     "SELECT user_id, COUNT(*) as cnt\n" +
                     "FROM click_log\n" +
                     "GROUP BY user_id";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertEquals("spark_result", lineages.get(0).getTargetTable());
        assertEquals(1, lineages.get(0).getSourceTables().size());
        assertTrue(lineages.get(0).getSourceTables().contains("click_log"));
    }

    @Test
    public void testSparkDropTable() {
        // 测试：Spark DROP TABLE
        String sql = "DROP TABLE IF EXISTS catalog.schema.my_table";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertTrue(lineages.isEmpty(), "无血缘语句应被过滤");
    }

    @Test
    public void testSparkUpdateStatement() {
        // 测试：Spark UPDATE（Spark 3.x+）
        String sql = "UPDATE customer_stats SET total_amount = 100 WHERE customer_id = 1";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertTrue(lineages.get(0).getSourceTables().isEmpty());
        assertNotNull(lineages.get(0).getTargetTable());
        assertTrue("customer_stats".equals(lineages.get(0).getTargetTable()));
    }

    @Test
    public void testSparkDeleteStatement() {
        // 测试：Spark DELETE（Spark 3.x+）
        String sql = "DELETE FROM logs WHERE created_date < '2023-01-01'";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertTrue(lineages.get(0).getSourceTables().isEmpty());
        assertNotNull(lineages.get(0).getTargetTable());
        assertTrue("logs".equals(lineages.get(0).getTargetTable()));
    }

    // ============================================
    // Presto SQL 测试
    // ============================================

    @Test
    public void testPrestoCreateTableCTAS() {
        // 测试：Presto CREATE TABLE AS SELECT
        String sql = "CREATE TABLE presto_result AS\n" +
                     "SELECT order_id, user_id, amount\n" +
                     "FROM orders WHERE amount > 100";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertEquals("presto_result", lineages.get(0).getTargetTable());
        assertEquals(1, lineages.get(0).getSourceTables().size());
        assertTrue(lineages.get(0).getSourceTables().contains("orders"));
    }

    @Test
    public void testPrestoDropTable() {
        // 测试：Presto DROP TABLE
        String sql = "DROP TABLE mysql_catalog.inventory.customers";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertTrue(lineages.isEmpty(), "无血缘语句应被过滤");
    }

    @Test
    public void testPrestoUpdateStatement() {
        // 测试：Presto UPDATE
        String sql = "UPDATE t1 SET status = 'PROCESSED' WHERE id IN (SELECT id FROM processed_t2)";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertEquals(1, lineages.get(0).getSourceTables().size());
        assertTrue(lineages.get(0).getSourceTables().contains("processed_t2"));
        assertNotNull(lineages.get(0).getTargetTable());
        assertTrue("t1".equals(lineages.get(0).getTargetTable()));
    }

    @Test
    public void testPrestoDeleteStatement() {
        // 测试：Presto DELETE
        String sql = "DELETE FROM active_users WHERE id IN (SELECT id FROM deleted_users)";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertEquals(1, lineages.get(0).getSourceTables().size());
        assertTrue(lineages.get(0).getSourceTables().contains("deleted_users"));
        assertNotNull(lineages.get(0).getTargetTable());
        assertTrue("active_users".equals(lineages.get(0).getTargetTable()));
    }

    // ============================================
    // 集合操作测试（UNION/INTERSECT/EXCEPT）
    // ============================================

    @Test
    public void testUnion() {
        // 测试：UNION 集合操作
        String sql = "SELECT user_id FROM table1\n" +
                     "UNION\n" +
                     "SELECT user_id FROM table2";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertEquals(2, lineages.get(0).getSourceTables().size());
        assertTrue(lineages.get(0).getSourceTables().contains("table1"));
        assertTrue(lineages.get(0).getSourceTables().contains("table2"));
    }

    @Test
    public void testUnionAll() {
        // 测试：UNION ALL
        String sql = "SELECT * FROM t1\n" +
                     "UNION ALL\n" +
                     "SELECT * FROM t2";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertEquals(2, lineages.get(0).getSourceTables().size());
    }

    @Test
    public void testIntersect() {
        // 测试：INTERSECT
        String sql = "SELECT user_id FROM active_users\n" +
                     "INTERSECT\n" +
                     "SELECT user_id FROM paid_users";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertEquals(2, lineages.get(0).getSourceTables().size());
        assertTrue(lineages.get(0).getSourceTables().contains("active_users"));
        assertTrue(lineages.get(0).getSourceTables().contains("paid_users"));
    }

    @Test
    public void testExcept() {
        // 测试：EXCEPT
        String sql = "SELECT user_id FROM orders_2023\n" +
                     "EXCEPT\n" +
                     "SELECT user_id FROM orders_2024";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertEquals(2, lineages.get(0).getSourceTables().size());
        assertTrue(lineages.get(0).getSourceTables().contains("orders_2023"));
        assertTrue(lineages.get(0).getSourceTables().contains("orders_2024"));
    }

    // ============================================
    // 复杂嵌套查询测试
    // ============================================

    @Test
    public void testComplexNestedQuery() {
        // 测试：多层 CTE + JOIN + 子查询
        String sql = "WITH filtered_orders AS (\n" +
                     "    SELECT order_id, user_id, amount\n" +
                     "    FROM orders WHERE status = 'PAID'\n" +
                     "),\n" +
                     "user_info AS (\n" +
                     "    SELECT fo.*, u.name\n" +
                     "    FROM filtered_orders fo\n" +
                     "    JOIN users u ON fo.user_id = u.user_id\n" +
                     ")\n" +
                     "INSERT INTO result_table\n" +
                     "SELECT * FROM user_info";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertEquals("result_table", lineages.get(0).getTargetTable());
        assertEquals(2, lineages.get(0).getSourceTables().size());
        assertTrue(lineages.get(0).getSourceTables().contains("orders"));
        assertTrue(lineages.get(0).getSourceTables().contains("users"));
    }

    @Test
    public void testSubQueryInFrom() {
        // 测试：FROM 子句中的子查询
        String sql = "INSERT INTO enriched_data\n" +
                     "SELECT a.*, b.category\n" +
                     "FROM table_a a\n" +
                     "JOIN (SELECT product_id, category FROM products) b\n" +
                     "ON a.product_id = b.product_id";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertEquals("enriched_data", lineages.get(0).getTargetTable());
        assertEquals(2, lineages.get(0).getSourceTables().size());
        assertTrue(lineages.get(0).getSourceTables().contains("table_a"));
        assertTrue(lineages.get(0).getSourceTables().contains("products"));
    }

    // ============================================
    // 三级命名空间测试（catalog.schema.table）
    // ============================================

    @Test
    public void testThreeLevelNamespace() {
        // 测试：Paimon 三级命名空间
        String sql = "INSERT INTO paimon_catalog.default.users\n" +
                     "SELECT * FROM paimon_catalog.default.raw_users";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertEquals("paimon_catalog.default.users", 
                     lineages.get(0).getTargetTable());
        assertEquals(1, lineages.get(0).getSourceTables().size());
        assertTrue(lineages.get(0).getSourceTables().contains("paimon_catalog.default.raw_users"));
    }

    @Test
    public void testDropThreeLevelNamespace() {
        // 测试：DROP TABLE with 三级命名空间
        String sql = "DROP TABLE hive_catalog.default.customer_order";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertTrue(lineages.isEmpty(), "无血缘语句应被过滤");
    }

    // ============================================
    // 批量处理测试
    // ============================================

    @Test
    public void testBatchProcessing() {
        // 测试：批量 SQL 语句
        String sql = "CREATE TABLE t1 AS SELECT * FROM source1;\n" +
                     "CREATE TABLE t2 AS SELECT * FROM source2;\n" +
                     "DROP TABLE IF EXISTS temp_table;";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(2, lineages.size());
        
        // 第一条：t1 <- source1
        assertEquals("t1", lineages.get(0).getTargetTable());
        assertTrue(lineages.get(0).getSourceTables().contains("source1"));
        
        // 第二条：t2 <- source2
        assertEquals("t2", lineages.get(1).getTargetTable());
        assertTrue(lineages.get(1).getSourceTables().contains("source2"));
        
    }

    // ============================================
    // 性能测试
    // ============================================

    @Test
    public void testPerformance() {
        // 测试：解析大量 SQL 的性能
        int batchSize = 100;
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < batchSize; i++) {
            String sql = "SELECT * FROM table_" + i + " WHERE id = " + i;
            parser.extractTableLineages(sql);
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        double qps = (batchSize * 1000.0) / duration;

        System.out.println("Processed " + batchSize + " SQLs in " + duration + "ms");
        System.out.println("QPS: " + String.format("%.2f", qps));

        // 性能要求：至少 500 SQL/秒
        assertTrue(qps >= 500, "Performance should be at least 500 SQL/s");
    }

    // ============================================
    // 错误处理测试
    // ============================================

    @Test
    public void testInvalidSQL() {
        // 测试：无效 SQL 的处理
        String invalidSql = "SELECT * FROM"; // 缺少表名

        assertDoesNotThrow(() -> {
            List<TableLineage> lineages = parser.extractTableLineages(invalidSql);
            // 即使 SQL 无效，也应该返回空结果而不是抛出异常
            assertTrue(lineages.isEmpty() || lineages.get(0).getSourceTables().isEmpty());
        });
    }

    // ============================================
    // 真实语料语法覆盖测试
    // ============================================

    @Test
    public void testIfFunctionWithEmptyStringAndNegativeNumber() {
        String sql = "INSERT INTO t_out SELECT IF(x IS NOT NULL OR x <> '', CAST(x AS BIGINT), -99999) AS y FROM t_in";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertFalse(lineages.isEmpty());
        TableLineage lineage = lineages.get(0);
        assertEquals("t_out", lineage.getTargetTable());
        assertTrue(lineage.getSourceTables().contains("t_in"));
    }

    @Test
    public void testUnnestInFromClause() {
        String sql = "INSERT INTO t_out SELECT a FROM kafka_src, UNNEST(eventBodyList) AS t (a, b)";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertFalse(lineages.isEmpty());
        TableLineage lineage = lineages.get(0);
        assertEquals("t_out", lineage.getTargetTable());
        assertTrue(lineage.getSourceTables().contains("kafka_src"));
        assertFalse(lineage.getSourceTables().contains("t"), "UNNEST 别名不应计入源表");
    }

    @Test
    public void testArraySubscriptFieldAccess() {
        String sql = "INSERT INTO t_out SELECT CAST(signals[1].value AS INT) AS v FROM t_in";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertFalse(lineages.isEmpty());
        TableLineage lineage = lineages.get(0);
        assertEquals("t_out", lineage.getTargetTable());
        assertTrue(lineage.getSourceTables().contains("t_in"));
    }

    @Test
    public void testUseAndDropStatementsIgnored() {
        String sql = "USE my_db; DROP TABLE IF EXISTS old_t; INSERT INTO t_out SELECT a FROM t_in";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        assertEquals("t_out", lineages.get(0).getTargetTable());
        assertTrue(lineages.get(0).getSourceTables().contains("t_in"));
    }

    // ============================================
    // 幻影表 bug 回归：子查询视图别名/字段曾被误识别为输入表
    // ============================================

    @Test
    public void testDerivedTableAliasNotPhantom() {
        String sql = "INSERT INTO dwd.t1 SELECT a.id, b.name "
                + "FROM (SELECT id FROM ods.s1) a JOIN (SELECT id, name FROM ods.s2) b ON a.id = b.id";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        TableLineage l = lineages.get(0);
        assertEquals("dwd.t1", l.getTargetTable());
        assertEquals(2, l.getSourceTables().size());
        assertTrue(l.getSourceTables().contains("ods.s1"));
        assertTrue(l.getSourceTables().contains("ods.s2"));
        assertFalse(l.getSourceTables().contains("a"));
        assertFalse(l.getSourceTables().contains("b"));
    }

    @Test
    public void testHiveStaticPartitionNotPhantom() {
        String sql = "INSERT OVERWRITE TABLE dwd.tp PARTITION(dt='2024-01-01') "
                + "SELECT u FROM (SELECT uid AS u FROM ods.sp) v";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        TableLineage l = lineages.get(0);
        assertEquals("dwd.tp", l.getTargetTable());
        assertTrue(l.getSourceTables().contains("ods.sp"));
        assertFalse(l.getSourceTables().contains("v"));
        assertFalse(l.isParseError());
    }

    @Test
    public void testHiveDynamicPartitionNotPhantom() {
        String sql = "INSERT OVERWRITE TABLE dwd.tdp PARTITION (dt) SELECT id, dt FROM ods.sdp";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        TableLineage l = lineages.get(0);
        assertEquals("dwd.tdp", l.getTargetTable());
        assertTrue(l.getSourceTables().contains("ods.sdp"));
    }

    @Test
    public void testNestedDerivedTablesNotPhantom() {
        String sql = "INSERT INTO dwd.t8 SELECT s.v1 "
                + "FROM (SELECT x.v1 FROM (SELECT v1 FROM ods.s11) x) s";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        TableLineage l = lineages.get(0);
        assertEquals("dwd.t8", l.getTargetTable());
        assertEquals(1, l.getSourceTables().size());
        assertTrue(l.getSourceTables().contains("ods.s11"));
    }

    @Test
    public void testFieldQualifiedNotPhantomSource() {
        // 限定字段 a.info.city 不应把 a 或 a.info 当成输入表
        String sql = "INSERT INTO dwd.tq SELECT a.info.city FROM (SELECT info FROM ods.sq) a";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        TableLineage l = lineages.get(0);
        assertEquals(1, l.getSourceTables().size());
        assertTrue(l.getSourceTables().contains("ods.sq"));
    }

    @Test
    public void testWhereInSubqueryAddsRealTable() {
        // WHERE IN (SELECT...) 里的表也要计为输入（此前被丢弃）
        String sql = "INSERT INTO dwd.t7 SELECT id FROM ods.s9 WHERE id IN (SELECT id FROM dim.s10)";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        TableLineage l = lineages.get(0);
        assertTrue(l.getSourceTables().contains("ods.s9"));
        assertTrue(l.getSourceTables().contains("dim.s10"));
    }

    @Test
    public void testScalarSubqueryInSelectListAddsRealTables() {
        // SELECT 列表中的标量子查询：主表和子查询表都要收集
        String sql = "INSERT INTO dwd.t17 SELECT (SELECT max(dt) FROM dim.s23) FROM ods.s24";

        List<TableLineage> lineages = parser.extractTableLineages(sql);

        assertEquals(1, lineages.size());
        TableLineage l = lineages.get(0);
        assertTrue(l.getSourceTables().contains("ods.s24"));
        assertTrue(l.getSourceTables().contains("dim.s23"));
    }
}
