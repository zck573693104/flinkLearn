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
}
