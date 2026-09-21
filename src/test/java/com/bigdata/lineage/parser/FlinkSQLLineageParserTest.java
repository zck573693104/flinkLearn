package com.bigdata.lineage.parser;

import com.bigdata.lineage.parser.extractor.TableLineageExtractor;
import com.bigdata.lineage.parser.model.TableLineage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

/**
 * FlinkSQLLineageParser 单元测试 - 基于 ANTLR4 实现
 */
public class FlinkSQLLineageParserTest {
    
    private FlinkSQLLineageParser parser;
    
    @Before
    public void setUp() {
        // 禁用缓存以便测试
        parser = new FlinkSQLLineageParser(false);
    }
    
    /**
     * 测试简单 INSERT INTO 语句
     */
    @Test
    public void testSimpleInsertInto() {
        String sql = "INSERT INTO target_table SELECT * FROM source_table";
        
        List<TableLineage> lineages = parser.extractTableLineages(sql);
        
        Assert.assertNotNull("Lineages should not be null", lineages);
        Assert.assertTrue("Should have at least one lineage", lineages.size() > 0);
        
        TableLineage lineage = lineages.get(0);
        Assert.assertEquals("Target table should be target_table", "target_table", lineage.getTargetTable());
        Assert.assertNotNull("Source tables should not be null", lineage.getSourceTables());
        Assert.assertTrue("Should have source table", lineage.getSourceTables().contains("source_table"));
        Assert.assertEquals("Process type should be INSERT", "INSERT", lineage.getProcessType());
    }
    
    /**
     * 测试 INSERT OVERWRITE 语句
     */
    @Test
    public void testInsertOverwrite() {
        String sql = "INSERT OVERWRITE TABLE output_db.output_table SELECT a, b, c FROM input_db.input_table WHERE id > 100";
        
        List<TableLineage> lineages = parser.extractTableLineages(sql);
        
        Assert.assertNotNull("Lineages should not be null", lineages);
        Assert.assertTrue("Should have at least one lineage", lineages.size() > 0);
        
        TableLineage lineage = lineages.get(0);
        Assert.assertEquals("Target table should be output_db.output_table", "output_db.output_table", lineage.getTargetTable());
        Assert.assertNotNull("Source tables should not be null", lineage.getSourceTables());
        Assert.assertTrue("Should have source table", lineage.getSourceTables().contains("input_db.input_table"));
        Assert.assertEquals("Insert mode should be OVERWRITE", "OVERWRITE", lineage.getInsertMode());
    }
    
    /**
     * 测试多源表 JOIN 场景
     */
    @Test
    public void testMultiSourceJoin() {
        String sql = "INSERT INTO order_result SELECT o.id, o.amount, c.name " +
                     "FROM orders o JOIN customers c ON o.customer_id = c.id " +
                     "WHERE o.status = 'PAID'";
        
        List<TableLineage> lineages = parser.extractTableLineages(sql);
        
        Assert.assertNotNull("Lineages should not be null", lineages);
        Assert.assertTrue("Should have at least one lineage", lineages.size() > 0);
        
        TableLineage lineage = lineages.get(0);
        Assert.assertEquals("Target table should be order_result", "order_result", lineage.getTargetTable());
        
        Set<String> sources = lineage.getSourceTables();
        Assert.assertNotNull("Source tables should not be null", sources);
        Assert.assertTrue("Should have orders table", sources.contains("orders"));
        Assert.assertTrue("Should have customers table", sources.contains("customers"));
    }
    
    /**
     * 测试 CTE (WITH 子句)
     */
    @Test
    public void testWithClause() {
        String sql = "WITH monthly_sales AS (" +
                     "    SELECT product_id, SUM(amount) as total " +
                     "    FROM orders GROUP BY product_id" +
                     ") " +
                     "INSERT INTO product_summary SELECT product_id, total FROM monthly_sales";
        
        List<TableLineage> lineages = parser.extractTableLineages(sql);
        
        Assert.assertNotNull("Lineages should not be null", lineages);
        Assert.assertTrue("Should have at least one lineage", lineages.size() > 0);
        
        TableLineage lineage = lineages.get(0);
        Assert.assertTrue("Should have CTE", lineage.isHasCte());
        Assert.assertEquals("Target table should be product_summary", "product_summary", lineage.getTargetTable());
    }
    
    /**
     * 测试嵌套查询
     */
    @Test
    public void testNestedQuery() {
        String sql = "INSERT INTO final_result SELECT * FROM (" +
                     "    SELECT * FROM (" +
                     "        SELECT * FROM temp_table WHERE status = 'ACTIVE'" +
                     "    ) filtered WHERE amount > 1000" +
                     ")";
        
        List<TableLineage> lineages = parser.extractTableLineages(sql);
        
        Assert.assertNotNull("Lineages should not be null", lineages);
        Assert.assertTrue("Should have at least one lineage", lineages.size() > 0);
        
        TableLineage lineage = lineages.get(0);
        Assert.assertEquals("Target table should be final_result", "final_result", lineage.getTargetTable());
        Assert.assertNotNull("Source tables should not be null", lineage.getSourceTables());
        Assert.assertTrue("Should have temp_table", lineage.getSourceTables().contains("temp_table"));
    }
    
    /**
     * 测试 LEFT JOIN
     */
    @Test
    public void testLeftJoin() {
        String sql = "INSERT INTO left_join_result SELECT a.*, b.extra_info " +
                     "FROM table_a a LEFT JOIN table_b b ON a.id = b.a_id";
        
        List<TableLineage> lineages = parser.extractTableLineages(sql);
        
        Assert.assertNotNull("Lineages should not be null", lineages);
        Assert.assertTrue("Should have at least one lineage", lineages.size() > 0);
        
        TableLineage lineage = lineages.get(0);
        Assert.assertEquals("Target table should be left_join_result", "left_join_result", lineage.getTargetTable());
        
        Set<String> sources = lineage.getSourceTables();
        Assert.assertNotNull("Source tables should not be null", sources);
        Assert.assertTrue("Should have table_a", sources.contains("table_a"));
        Assert.assertTrue("Should have table_b", sources.contains("table_b"));
    }
    
    /**
     * 测试空 SQL
     */
    @Test
    public void testEmptySql() {
        List<TableLineage> lineages = parser.extractTableLineages("");
        Assert.assertNotNull("Lineages should not be null", lineages);
        Assert.assertTrue("Lineages should be empty", lineages.isEmpty());
    }
    
    /**
     * 测试 NULL SQL
     */
    @Test
    public void testNullSql() {
        List<TableLineage> lineages = parser.extractTableLineages(null);
        Assert.assertNotNull("Lineages should not be null", lineages);
        Assert.assertTrue("Lineages should be empty", lineages.isEmpty());
    }
    
    /**
     * 测试多条 SQL 语句
     */
    @Test
    public void testMultipleStatements() {
        String sql = "INSERT INTO table1 SELECT * FROM source1; " +
                     "INSERT INTO table2 SELECT * FROM source2";
        
        List<TableLineage> lineages = parser.extractTableLineages(sql);
        
        Assert.assertNotNull("Lineages should not be null", lineages);
        Assert.assertEquals("Should have two lineages", 2, lineages.size());
        
        Assert.assertEquals("First target should be table1", "table1", lineages.get(0).getTargetTable());
        Assert.assertEquals("Second target should be table2", "table2", lineages.get(1).getTargetTable());
    }
    
    /**
     * 测试缓存功能
     */
    @Test
    public void testCache() {
        String sql = "INSERT INTO cache_test SELECT * FROM source";
        
        // 第一次调用，应该未命中缓存
        List<TableLineage> result1 = parser.extractTableLineages(sql, true);
        Assert.assertNotNull("Result should not be null", result1);
        
        // 第二次调用，应该命中缓存
        List<TableLineage> result2 = parser.extractTableLineages(sql, true);
        Assert.assertNotNull("Result should not be null", result2);
        
        // 结果应该相同
        Assert.assertEquals("Results should be equal", result1.size(), result2.size());
    }

    /**
     * 语料暴露的语法点：窗口函数 OVER(PARTITION BY ... ORDER BY ...) 与 LIKE。
     * 直接走 extractor 断言 parseError，防止 grammar 回退后靠错误恢复蒙混过关
     */
    @Test
    public void testWindowFunctionAndLikeParseWithoutError() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql(
                "INSERT INTO dwd.t_win SELECT lag(amount) OVER (PARTITION BY user_id ORDER BY dt) AS prev, "
                        + "row_number() OVER (PARTITION BY id) AS rn "
                        + "FROM ods.src_win WHERE name LIKE '%外请%'");

        Assert.assertFalse("窗口函数/LIKE 不应产生解析错误", lineage.isParseError());
        Assert.assertEquals("dwd.t_win", lineage.getTargetTable());
        Assert.assertEquals(java.util.Set.of("ods.src_win"), lineage.getSourceTables());
        Assert.assertTrue(lineage.isHasWindowFunc());
    }

    /**
     * 多层 CTE + 窗口函数：CTE 名必须是临时名，不得泄漏成输入表
     */
    @Test
    public void testMultiCteChainNeverLeaksCteNames() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql(
                "WITH v_a AS (SELECT id, dt FROM ods.src_a), "
                        + "v_b AS (SELECT lag(id) OVER (PARTITION BY dt ORDER BY id) AS p FROM v_a) "
                        + "INSERT INTO dwd.t_multi SELECT p FROM v_b JOIN ods.src_d ON v_b.p = ods.src_d.id");

        Assert.assertFalse("CTE 链不应产生解析错误", lineage.isParseError());
        Assert.assertEquals("dwd.t_multi", lineage.getTargetTable());
        Assert.assertEquals(java.util.Set.of("ods.src_a", "ods.src_d"), lineage.getSourceTables());
    }

    /**
     * 词法层守护：name/level/type 这类常见列名必须是普通标识符，
     * 不能被登记成关键字，否则错误恢复会让列名泄漏成输入表
     */
    @Test
    public void testCommonColumnNamesArePlainIdentifiers() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql(
                "INSERT INTO dwd.t_id SELECT name, level, type, source, owner, state "
                        + "FROM ods.src_id WHERE name LIKE '%x%' AND level > 1");

        Assert.assertFalse("常见列名不得被词法保留", lineage.isParseError());
        Assert.assertEquals(java.util.Set.of("ods.src_id"), lineage.getSourceTables());
    }

    /**
     * USE 一类无血缘语句不应产生结果行（与 MultiEngine 入口保持一致）
     */
    @Test
    public void testUseStatementProducesNoLineageRow() {
        List<TableLineage> lineages = parser.extractTableLineages(
                "USE my_db; INSERT INTO dwd.t SELECT * FROM ods.s");

        Assert.assertEquals("只应保留有血缘的语句", 1, lineages.size());
        Assert.assertEquals("dwd.t", lineages.get(0).getTargetTable());
    }

    @Test
    public void testJoinOnSubqueryContributesSourceTables() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql(
                "INSERT INTO dwd.t SELECT x.id FROM ods.x x JOIN ods.y y ON x.id IN (SELECT id FROM ods.z)");

        Assert.assertFalse(lineage.isParseError());
        Assert.assertTrue("ON 子查询里的表也是上游: " + lineage.getSourceTables(),
                lineage.getSourceTables().contains("ods.z"));
    }

    @Test
    public void testCreateViewProcessType() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql(
                "CREATE TEMPORARY VIEW v AS SELECT * FROM ods.a");

        Assert.assertEquals("CREATE_VIEW", lineage.getProcessType());
        Assert.assertEquals("v", lineage.getTargetTable());
        Assert.assertTrue(lineage.getSourceTables().contains("ods.a"));
    }

    /**
     * 处理时间时态表 JOIN：维表必须进上游，且 hasTemporalJoin 要能被下游看到
     */
    @Test
    public void testProcessingTimeTemporalJoin() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql(
                "INSERT INTO dwd.enriched SELECT o.id, c.name FROM ods.orders AS o "
                        + "JOIN dim.currency FOR SYSTEM_TIME AS OF o.proctime AS c ON o.cur = c.id");

        Assert.assertFalse("时态表 JOIN 应可解析", lineage.isParseError());
        Assert.assertTrue(lineage.isHasTemporalJoin());
        Assert.assertEquals(Set.of("ods.orders", "dim.currency"), lineage.getSourceTables());
    }

    @Test
    public void testEventTimeLeftTemporalJoin() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql(
                "INSERT INTO dwd.enriched SELECT o.id, u.name FROM ods.orders o "
                        + "LEFT JOIN dim.users FOR SYSTEM_TIME AS OF o.order_time AS u ON o.uid = u.id");

        Assert.assertFalse("LEFT JOIN 时态表应可解析", lineage.isParseError());
        Assert.assertTrue(lineage.isHasTemporalJoin());
        Assert.assertEquals(Set.of("ods.orders", "dim.users"), lineage.getSourceTables());
    }

    /**
     * 旧式窗口 TVF：输入表写在 TUMBLE 的第一个实参位置
     */
    @Test
    public void testWindowTvfLegacySyntax() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql(
                "INSERT INTO dws.win_cnt SELECT window_start, COUNT(*) "
                        + "FROM TUMBLE(dwd.orders, ts, INTERVAL '5' MINUTE) GROUP BY window_start");

        Assert.assertFalse("窗口 TVF 应可解析", lineage.isParseError());
        Assert.assertEquals(Set.of("dwd.orders"), lineage.getSourceTables());
        Assert.assertTrue(lineage.isHasWindowFunc());
    }

    /**
     * Flink 1.14+ 新式 TVF：TABLE(HOP(TABLE t, DESCRIPTOR(col), INTERVAL ...))
     */
    @Test
    public void testWindowTvfNewSyntax() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql(
                "INSERT INTO dws.win_cnt SELECT * FROM TABLE("
                        + "HOP(TABLE dwd.clicks, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE))");

        Assert.assertFalse("新式 TVF 应可解析", lineage.isParseError());
        Assert.assertEquals(Set.of("dwd.clicks"), lineage.getSourceTables());
        Assert.assertTrue(lineage.isHasWindowFunc());
    }

    /**
     * 词法层守护：时间单位与 SYSTEM_TIME/OF 只是时态语境的软关键字，仍须可作列名
     */
    @Test
    public void testTimeUnitWordsArePlainIdentifiers() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql(
                "INSERT INTO dwd.t_cal SELECT year, month, day, hour, minute, second, quarter, system_time "
                        + "FROM ods.s_cal WHERE of = 1");

        Assert.assertFalse("时间单位词不得被词法保留", lineage.isParseError());
        Assert.assertEquals(Set.of("ods.s_cal"), lineage.getSourceTables());
    }

    /**
     * 词法层守护：ANTLR 会忽略词法规则内的空白，多词关键字必须拆成两个 token，
     * 否则 PRIMARY KEY / CHARACTER VARYING 这类写法永远匹配不上
     */
    @Test
    public void testMultiWordKeywordsAreSplitIntoTwoTokens() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql(
                "INSERT INTO dwd.t_varchar SELECT CAST(c AS CHARACTER VARYING(20)), CAST(d AS DOUBLE) "
                        + "FROM ods.s_varchar");

        Assert.assertFalse("CHARACTER VARYING/DOUBLE 必须是可用的类型名", lineage.isParseError());
        Assert.assertEquals(Set.of("ods.s_varchar"), lineage.getSourceTables());
    }

    /**
     * 计算列 + 表级 PRIMARY KEY ... NOT ENFORCED 是 Flink upsert 表的标准写法
     */
    @Test
    public void testComputedColumnAndTableLevelPrimaryKeyDdl() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql(
                "CREATE TABLE dwd.t_pk (id INT, name VARCHAR, proc_ts AS PROctime(), "
                        + "PRIMARY KEY (id) NOT ENFORCED) WITH ('connector' = 'jdbc')");

        Assert.assertFalse("计算列与表级主键应可解析", lineage.isParseError());
        Assert.assertEquals("CREATE_TABLE", lineage.getProcessType());
        Assert.assertEquals("dwd.t_pk", lineage.getTargetTable());
    }

    @Test
    public void testHivePartitionedDdl() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql(
                "CREATE TABLE dwd.t_hive (id INT, dt STRING) "
                        + "PARTITIONED BY (dt STRING) STORED AS PARQUET "
                        + "TBLPROPERTIES ('hive.partition-commit-policy' = 'none')");

        Assert.assertFalse("Hive 建表选项应可解析", lineage.isParseError());
        Assert.assertEquals("dwd.t_hive", lineage.getTargetTable());
    }

    @Test
    public void testInsertValuesHasTargetOnly() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql(
                "INSERT INTO dwd.t_val (id, name) VALUES (1, 'a'), (2, 'b')");

        Assert.assertFalse("INSERT VALUES 应可解析", lineage.isParseError());
        Assert.assertEquals("dwd.t_val", lineage.getTargetTable());
        Assert.assertTrue("VALUES 没有上游表", lineage.getSourceTables().isEmpty());
    }

    @Test
    public void testSetStatementVariantsProduceNoLineage() {
        TableLineageExtractor ex = new TableLineageExtractor();
        for (String sql : List.of("SET table.sql-dialect = hive", "SET 'pipeline.name' = 'my job'")) {
            TableLineage lineage = ex.extractFromSql(sql);
            Assert.assertFalse("SET 语句不应产生解析错误: " + sql, lineage.isParseError());
            Assert.assertTrue("SET 语句不应产生血缘: " + sql,
                    lineage.getTargetTable() == null || lineage.getTargetTable().isEmpty());
        }
    }

    @Test
    public void testArrayAndRowConstructorsKeepSourceTables() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql(
                "INSERT INTO dwd.t_ctor SELECT ARRAY[1, 2, 3], ROW(id, name), "
                        + "CAST(m AS MAP<STRING, ARRAY<INT>>) FROM ods.s_ctor");

        Assert.assertFalse("数组/行构造器应可解析", lineage.isParseError());
        Assert.assertEquals(Set.of("ods.s_ctor"), lineage.getSourceTables());
    }

    @Test
    public void testExistsSubqueryContributesSourceTables() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql(
                "INSERT INTO dwd.t_exists SELECT * FROM ods.a a WHERE EXISTS "
                        + "(SELECT 1 FROM ods.b WHERE b.id = a.id)");

        Assert.assertFalse("EXISTS 子查询应可解析", lineage.isParseError());
        Assert.assertEquals(Set.of("ods.a", "ods.b"), lineage.getSourceTables());
    }

    @Test
    public void testTruncateStatementHasNoLineage() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql("TRUNCATE TABLE dwd.t_trunc");

        Assert.assertFalse("TRUNCATE 应可解析", lineage.isParseError());
        Assert.assertTrue("TRUNCATE 不产生数据流",
                lineage.getTargetTable() == null || lineage.getTargetTable().isEmpty());
    }

    /** WITH 是 queryExpression 的前置子句：INSERT / CTAS 头部带 CTE 时 CTE 名不得成为输入表 */
    @Test
    public void testCteInsideInsertAndCtasBody() {
        TableLineage insert = new TableLineageExtractor().extractFromSql(
                "INSERT INTO dwd.t_wi WITH c AS (SELECT * FROM ods.s_wi) SELECT * FROM c");

        Assert.assertFalse("INSERT 头部带 WITH 应可解析", insert.isParseError());
        Assert.assertEquals("dwd.t_wi", insert.getTargetTable());
        Assert.assertEquals(Set.of("ods.s_wi"), insert.getSourceTables());

        TableLineage ctas = new TableLineageExtractor().extractFromSql(
                "CREATE TABLE dwd.t_wc AS WITH c AS (SELECT * FROM ods.s_wc) SELECT * FROM c");

        Assert.assertFalse("CTAS 头部带 WITH 应可解析", ctas.isParseError());
        Assert.assertEquals("dwd.t_wc", ctas.getTargetTable());
        Assert.assertEquals(Set.of("ods.s_wc"), ctas.getSourceTables());
    }

    /**
     * 限定列名位于子句末尾（ON 条件最后一个 token 就是 a.col）：
     * columnRef 若写成 tablePath DOT uid，tablePath 的内部可选分支会吞掉外层需要的 DOT
     */
    @Test
    public void testQualifiedColumnEndingAJoinCondition() {
        TableLineage lineage = new TableLineageExtractor().extractFromSql(
                "INSERT INTO dwd.t_qc SELECT t1.id FROM ods.s_qc_a t1 "
                        + "LEFT JOIN ods.s_qc_b t2 ON t1.m = t2.m AND t1.co = t2.employee_number "
                        + "WHERE t2.perf_month BETWEEN '1' AND '2'");

        Assert.assertFalse("ON 条件以限定列名结尾应可解析", lineage.isParseError());
        Assert.assertEquals(Set.of("ods.s_qc_a", "ods.s_qc_b"), lineage.getSourceTables());
    }
}
