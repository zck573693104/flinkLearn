package com.bigdata.lineage.parser;

import com.bigdata.lineage.parser.extractor.PrestoTableLineageExtractor;
import com.bigdata.lineage.parser.extractor.SparkTableLineageExtractor;
import com.bigdata.lineage.parser.model.TableLineage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Spark/Presto 方言回归测试 —— 离线数仓语料暴露的语法点：
 * CASE WHEN、空字符串、一元负号、数组下标、IF()、USE、WITH+INSERT、UNNEST
 */
class SparkPrestoDialectLineageTest {

    private final SparkTableLineageExtractor spark = new SparkTableLineageExtractor();
    private final PrestoTableLineageExtractor presto = new PrestoTableLineageExtractor();

    private void assertSparkLineage(String sql, String target, String... sources) {
        TableLineage lineage = spark.extractFromSql(sql);
        assertNotNull(lineage, "Spark 解析结果不应为 null: " + sql);
        assertEquals(target, lineage.getTargetTable(), "目标表不匹配: " + sql);
        for (String s : sources) {
            assertTrue(lineage.getSourceTables().contains(s),
                    "缺少源表 " + s + ", 实际: " + lineage.getSourceTables());
        }
        assertEquals(sources.length, lineage.getSourceTables().size(),
                "源表数量不匹配: " + lineage.getSourceTables());
    }

    private void assertPrestoLineage(String sql, String target, String... sources) {
        TableLineage lineage = presto.extractFromSql(sql);
        assertNotNull(lineage, "Presto 解析结果不应为 null: " + sql);
        assertEquals(target, lineage.getTargetTable(), "目标表不匹配: " + sql);
        for (String s : sources) {
            assertTrue(lineage.getSourceTables().contains(s),
                    "缺少源表 " + s + ", 实际: " + lineage.getSourceTables());
        }
        assertEquals(sources.length, lineage.getSourceTables().size(),
                "源表数量不匹配: " + lineage.getSourceTables());
    }

    // ============================================
    // Spark 方言
    // ============================================

    @Test
    void sparkCaseWhen() {
        assertSparkLineage(
                "INSERT INTO dwd.t_case SELECT CASE WHEN a = 1 THEN 'x' WHEN a = 2 THEN 'y' ELSE '' END AS flag FROM ods.src_case",
                "dwd.t_case", "ods.src_case");
    }

    @Test
    void sparkIfFunctionWithEmptyStringAndNegativeNumber() {
        assertSparkLineage(
                "INSERT INTO dwd.t_if SELECT IF(b = '', -99999, b) FROM ods.src_if",
                "dwd.t_if", "ods.src_if");
    }

    @Test
    void sparkArraySubscriptAndFieldAccess() {
        assertSparkLineage(
                "INSERT INTO dwd.t_arr SELECT tags[1].name FROM ods.src_arr",
                "dwd.t_arr", "ods.src_arr");
    }

    @Test
    void sparkCteWithInsert() {
        assertSparkLineage(
                "WITH c AS (SELECT * FROM ods.src_cte) INSERT INTO dwd.t_cte SELECT * FROM c",
                "dwd.t_cte", "ods.src_cte");
    }

    @Test
    void sparkUseStatementHasNoLineage() {
        TableLineage lineage = spark.extractFromSql("USE my_db");
        assertTrue(lineage.getTargetTable() == null || lineage.getTargetTable().isEmpty());
        assertTrue(lineage.getSourceTables() == null || lineage.getSourceTables().isEmpty());
    }

    @Test
    void sparkUnnestInFromClause() {
        assertSparkLineage(
                "INSERT INTO dwd.t_unnest SELECT e FROM ods.src_unnest CROSS JOIN UNNEST(tags) AS e",
                "dwd.t_unnest", "ods.src_unnest");
    }

    // ============================================
    // Presto 方言
    // ============================================

    @Test
    void prestoCaseWhen() {
        assertPrestoLineage(
                "INSERT INTO dwd.t_case SELECT CASE WHEN a = 1 THEN 'x' ELSE '' END FROM ods.src_case",
                "dwd.t_case", "ods.src_case");
    }

    @Test
    void prestoIfFunctionWithEmptyString() {
        assertPrestoLineage(
                "INSERT INTO dwd.t_if SELECT IF(b = '', -1, b) FROM ods.src_if",
                "dwd.t_if", "ods.src_if");
    }

    @Test
    void prestoArraySubscript() {
        assertPrestoLineage(
                "INSERT INTO dwd.t_arr SELECT tags[1] FROM ods.src_arr",
                "dwd.t_arr", "ods.src_arr");
    }

    @Test
    void prestoCteWithInsert() {
        assertPrestoLineage(
                "WITH c AS (SELECT * FROM ods.src_cte) INSERT INTO dwd.t_cte SELECT * FROM c",
                "dwd.t_cte", "ods.src_cte");
    }

    @Test
    void prestoUnnestWithColumnAlias() {
        assertPrestoLineage(
                "INSERT INTO dwd.t_unnest SELECT x FROM ods.src_unnest CROSS JOIN UNNEST(arr) AS u(x)",
                "dwd.t_unnest", "ods.src_unnest");
    }

    @Test
    void prestoUseStatementHasNoLineage() {
        TableLineage lineage = presto.extractFromSql("USE my_catalog.my_schema");
        assertTrue(lineage.getTargetTable() == null || lineage.getTargetTable().isEmpty());
        assertTrue(lineage.getSourceTables() == null || lineage.getSourceTables().isEmpty());
    }

    // ============================================
    // 多引擎统一入口
    // ============================================

    @Test
    void multiEngineHandlesSparkDialectCorpus() {
        MultiEngineSQLLineageParser parser = new MultiEngineSQLLineageParser(false);
        var lineages = parser.extractTableLineages(
                "INSERT INTO dwd.t1 SELECT CASE WHEN a = 1 THEN '' ELSE -1 END FROM ods.s1");
        assertEquals(1, lineages.size());
        assertEquals("dwd.t1", lineages.get(0).getTargetTable());
        assertTrue(lineages.get(0).getSourceTables().contains("ods.s1"));
    }
}
