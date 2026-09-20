package com.bigdata.lineage.parser;

import com.bigdata.lineage.parser.extractor.PrestoTableLineageExtractor;
import com.bigdata.lineage.parser.extractor.SparkTableLineageExtractor;
import com.bigdata.lineage.parser.model.TableLineage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Spark/Presto 方言回归测试 —— 离线数仓语料暴露的语法点：
 * CASE WHEN、空字符串、一元负号、数组下标、IF()、USE、WITH+INSERT、
 * Spark 的 LATERAL VIEW explode / Presto 的 CROSS JOIN UNNEST（各引擎原生数组展开语法）
 */
class SparkPrestoDialectLineageTest {

    private final SparkTableLineageExtractor spark = new SparkTableLineageExtractor();
    private final PrestoTableLineageExtractor presto = new PrestoTableLineageExtractor();

    private void assertSparkLineage(String sql, String target, String... sources) {
        TableLineage lineage = spark.extractFromSql(sql);
        assertNotNull(lineage, "Spark 解析结果不应为 null: " + sql);
        assertFalse(lineage.isParseError(), "Spark 语法不应产生解析错误: " + sql);
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
        assertFalse(lineage.isParseError(), "Presto 语法不应产生解析错误: " + sql);
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
    void sparkLateralViewExplode() {
        assertSparkLineage(
                "INSERT INTO dwd.t_lat SELECT tag FROM ods.src_lat LATERAL VIEW explode(tags) lv AS tag",
                "dwd.t_lat", "ods.src_lat");
    }

    @Test
    void sparkLateralViewOuterPosexplodeTwoAliases() {
        assertSparkLineage(
                "INSERT INTO dwd.t_pos SELECT pos, tag FROM ods.src_pos LATERAL VIEW OUTER posexplode(tags) pv AS pos, tag",
                "dwd.t_pos", "ods.src_pos");
    }

    @Test
    void sparkWindowFunctionOverPartitionByOrderBy() {
        TableLineage lineage = spark.extractFromSql(
                "INSERT INTO dwd.t_win SELECT lag(amount) OVER (PARTITION BY user_id ORDER BY dt) AS prev "
                        + "FROM ods.src_win");
        assertFalse(lineage.isParseError(), "窗口函数语法不应产生解析错误");
        assertEquals("dwd.t_win", lineage.getTargetTable());
        assertEquals(java.util.Set.of("ods.src_win"), lineage.getSourceTables());
        assertTrue(lineage.isHasWindowFunc());
    }

    @Test
    void sparkLikeWithPercentWildcardAndChinese() {
        assertSparkLineage(
                "INSERT INTO dwd.t_like SELECT a FROM ods.src_like "
                        + "WHERE name LIKE '%外请%' AND code NOT LIKE '测试%'",
                "dwd.t_like", "ods.src_like");
    }

    @Test
    void sparkDistributeByAfterJoin() {
        assertSparkLineage(
                "INSERT OVERWRITE TABLE dwd.t_dist SELECT t1.id FROM ods.src_a t1 "
                        + "JOIN ods.src_b t2 ON t1.id = t2.id DISTRIBUTE BY t1.dt",
                "dwd.t_dist", "ods.src_a", "ods.src_b");
    }

    @Test
    void sparkBuiltinFunctionNamesArePlainIdentifiers() {
        TableLineage lineage = spark.extractFromSql(
                "INSERT INTO dwd.t_func SELECT substr(name, 1, 3), md5(id), concat_ws(',', a, b), "
                        + "get_json_object(j, '$.k'), to_json(named_struct('k', v)), "
                        + "regexp_extract(s, '(a)(b)', 1), count(1) OVER (PARTITION BY id) "
                        + "FROM ods.src_func");
        assertFalse(lineage.isParseError(), "内置函数名应可作为普通标识符解析");
        assertEquals(java.util.Set.of("ods.src_func"), lineage.getSourceTables());
    }

    /**
     * 语料暴露的核心缺陷：多层 CTE + 窗口函数 + LIKE 混排时，
     * 语法缺口会让 ANTLR 错误恢复吞掉 WITH 块，CTE 名泄漏成输入表
     */
    @Test
    void sparkMultiCteChainNeverLeaksCteNames() {
        TableLineage lineage = spark.extractFromSql(
                "WITH v_a AS (SELECT id, dt FROM ods.src_a), "
                        + "v_b AS (SELECT lag(id) OVER (PARTITION BY dt ORDER BY id) AS p, dt FROM v_a), "
                        + "v_c AS (SELECT p AS id, dt FROM v_b WHERE dt LIKE '%2024%') "
                        + "INSERT OVERWRITE TABLE dwd.t_multi SELECT id FROM v_c "
                        + "JOIN ods.src_d ON v_c.id = ods.src_d.id");
        assertFalse(lineage.isParseError(), "CTE 链不应产生解析错误");
        assertEquals("dwd.t_multi", lineage.getTargetTable());
        assertEquals(java.util.Set.of("ods.src_a", "ods.src_d"), lineage.getSourceTables());
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

    @Test
    void prestoWindowFunctionOverAndLike() {
        TableLineage lineage = presto.extractFromSql(
                "INSERT INTO dwd.t_win SELECT lag(amount) OVER (PARTITION BY user_id ORDER BY dt) AS prev "
                        + "FROM ods.src_win WHERE name LIKE '%外请%'");
        assertFalse(lineage.isParseError(), "Presto 窗口函数/LIKE 应可解析");
        assertEquals("dwd.t_win", lineage.getTargetTable());
        assertEquals(java.util.Set.of("ods.src_win"), lineage.getSourceTables());
    }

    @Test
    void prestoBuiltinFunctionNamesArePlainIdentifiers() {
        TableLineage lineage = presto.extractFromSql(
                "INSERT INTO dwd.t_func SELECT substr(name, 1, 3), concat_ws(',', a, b), "
                        + "regexp_extract(s, '(a)(b)', 1), cardinality(arr) FROM ods.src_func");        assertFalse(lineage.isParseError(), "内置函数名应可作为普通标识符解析");
        assertEquals(java.util.Set.of("ods.src_func"), lineage.getSourceTables());
    }

    /**
     * 词法层守护：只有解析器真正用到的词才允许登记为关键字。
     * name/level/type/source/owner/state 这类常见列名必须是普通标识符，
     * 否则 ANTLR 错误恢复会吞掉整个语句，CTE 名与列名泄漏成输入表
     */
    @Test
    void commonColumnNamesArePlainIdentifiers() {
        String sql = "INSERT INTO dwd.t_id SELECT name, level, type, source, owner, state "
                + "FROM ods.src_id WHERE name LIKE '%x%' AND level > 1";
        for (TableLineage lineage : new TableLineage[]{spark.extractFromSql(sql), presto.extractFromSql(sql)}) {
            assertFalse(lineage.isParseError(), "常见列名不得被词法保留: " + lineage.getOriginalSql());
            assertEquals(java.util.Set.of("ods.src_id"), lineage.getSourceTables());
        }
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

    /**
     * 注释里的单引号曾让切分器误判字符串状态，两条 INSERT 被并成一条：
     * 目标表只剩一个、两条血缘被揉成一条
     */
    @Test
    void multiEngineKeepsStatementsSeparateWhenCommentsContainQuotes() {
        MultiEngineSQLLineageParser parser = new MultiEngineSQLLineageParser(false);
        var lineages = parser.extractTableLineages(
                "-- step; it's fine\n"
                        + "INSERT INTO dwd.a SELECT * FROM ods.x;\n"
                        + "INSERT INTO dwd.b SELECT * FROM ods.y");
        assertEquals(2, lineages.size(), "两条 INSERT 应各自产出一条血缘: " + lineages);
        assertEquals("dwd.a", lineages.get(0).getTargetTable());
        assertEquals(java.util.Set.of("ods.x"), lineages.get(0).getSourceTables());
        assertEquals("dwd.b", lineages.get(1).getTargetTable());
        assertEquals(java.util.Set.of("ods.y"), lineages.get(1).getSourceTables());
    }

    @Test
    void cteNameExclusionIsCaseInsensitive() {
        TableLineage lineage = spark.extractFromSql(
                "WITH MyCte AS (SELECT id FROM ods.a) INSERT INTO dwd.t SELECT * FROM mycte");
        assertEquals(java.util.Set.of("ods.a"), lineage.getSourceTables(),
                "CTE 名大小写混用不得泄漏成输入表");
    }

    @Test
    void joinOnSubqueryContributesSourceTables() {
        assertSparkLineage(
                "INSERT INTO dwd.t SELECT x.id FROM ods.x x JOIN ods.y y ON x.id IN (SELECT id FROM ods.z)",
                "dwd.t", "ods.x", "ods.y", "ods.z");
        assertPrestoLineage(
                "INSERT INTO dwd.t SELECT x.id FROM ods.x x JOIN ods.y y ON x.id IN (SELECT id FROM ods.z)",
                "dwd.t", "ods.x", "ods.y", "ods.z");
    }

    @Test
    void createViewIsNotReportedAsCtas() {
        TableLineage sparkView = spark.extractFromSql("CREATE TEMPORARY VIEW v AS SELECT * FROM ods.a");
        assertEquals("CREATE_VIEW", sparkView.getProcessType());
        assertEquals("v", sparkView.getTargetTable());
        assertEquals(java.util.Set.of("ods.a"), sparkView.getSourceTables());

        TableLineage prestoView = presto.extractFromSql("CREATE VIEW v AS SELECT * FROM ods.a");
        assertEquals("CREATE_VIEW", prestoView.getProcessType());
    }

    @Test
    void parseErrorDowngradesConfidence() {
        TableLineage bad = spark.extractFromSql("INSERT INTO dwd.t SELECT FROM WHERE");
        assertTrue(bad.isParseError(), "非法 SQL 应标记 parseError");
        assertTrue(bad.getConfidence() < 0.95, "parseError 时置信度必须低于正常值: " + bad.getConfidence());

        TableLineage good = spark.extractFromSql("INSERT INTO dwd.t SELECT * FROM ods.s");
        assertFalse(good.isParseError());
        assertEquals(0.95, good.getConfidence(), 0.001);
    }

    // ============================================
    // 本轮语法体检暴露的缺口：逐条锁死，避免再次退化成"假通过"
    // ============================================

    @Test
    void sparkRecursiveCteDoesNotLeakCteName() {
        assertSparkLineage(
                "WITH RECURSIVE x AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM ods.src_rec WHERE n < 3) "
                        + "INSERT INTO dwd.t_rec SELECT * FROM x",
                "dwd.t_rec", "ods.src_rec");
    }

    @Test
    void sparkLeftSemiAndAntiJoin() {
        assertSparkLineage("INSERT INTO dwd.t_semi SELECT b.id FROM ods.a b LEFT SEMI JOIN ods.c ON b.id = c.id",
                "dwd.t_semi", "ods.a", "ods.c");
        assertSparkLineage("INSERT INTO dwd.t_anti SELECT b.id FROM ods.a b ANTI JOIN ods.c ON b.id = c.id",
                "dwd.t_anti", "ods.a", "ods.c");
    }

    @Test
    void sparkGroupingSetsCubeRollup() {
        assertSparkLineage("INSERT INTO dwd.t_gs SELECT a, count(*) FROM ods.s_gs GROUP BY GROUPING SETS ((a), (b), ())",
                "dwd.t_gs", "ods.s_gs");
        assertSparkLineage("INSERT INTO dwd.t_cube SELECT a, count(*) FROM ods.s_cube GROUP BY CUBE(a, b)",
                "dwd.t_cube", "ods.s_cube");
        assertSparkLineage("INSERT INTO dwd.t_rl SELECT a, count(*) FROM ods.s_rl GROUP BY ROLLUP(a, b)",
                "dwd.t_rl", "ods.s_rl");
    }

    @Test
    void sparkPivotAndTablesample() {
        assertSparkLineage("INSERT INTO dwd.t_pv SELECT * FROM ods.s_pv PIVOT (count(*) FOR k IN ('a' AS a, 'b' AS b))",
                "dwd.t_pv", "ods.s_pv");
        assertSparkLineage("INSERT INTO dwd.t_smp SELECT * FROM ods.s_smp TABLESAMPLE (10 PERCENT)",
                "dwd.t_smp", "ods.s_smp");
    }

    @Test
    void sparkIgnoreNullsWindowAndQualify() {
        assertSparkLineage("INSERT INTO dwd.t_null SELECT first_value(v) IGNORE NULLS OVER (PARTITION BY id ORDER BY ts) "
                + "FROM ods.s_null", "dwd.t_null", "ods.s_null");
        assertSparkLineage("INSERT INTO dwd.t_q SELECT * FROM ods.s_q "
                + "QUALIFY row_number() OVER (PARTITION BY id ORDER BY ts) = 1", "dwd.t_q", "ods.s_q");
    }

    @Test
    void sparkVarcharDoubleFloatIntegerCasts() {
        assertSparkLineage("INSERT INTO dwd.t_typ SELECT CAST(x AS VARCHAR(20)), CAST(y AS DOUBLE), "
                + "CAST(z AS FLOAT), CAST(w AS INTEGER), CAST(m AS DECIMAL(10, 2)) FROM ods.s_typ",
                "dwd.t_typ", "ods.s_typ");
    }

    @Test
    void sparkExistsAndNotInSubqueryContributeSourceTables() {
        assertSparkLineage("INSERT INTO dwd.t_ex SELECT * FROM ods.a WHERE EXISTS (SELECT 1 FROM ods.b WHERE b.id = a.id)",
                "dwd.t_ex", "ods.a", "ods.b");
        assertSparkLineage("INSERT INTO dwd.t_ni SELECT * FROM ods.a WHERE id NOT IN (SELECT id FROM ods.b)",
                "dwd.t_ni", "ods.a", "ods.b");
    }

    /** INSERT ... DIRECTORY 写的是文件路径，不是表：目标必须为空，否则会冒出幻影表 */
    @Test
    void sparkInsertDirectoryHasNoTargetTable() {
        TableLineage lineage = spark.extractFromSql(
                "INSERT OVERWRITE DIRECTORY '/tmp/out' STORED AS ORC SELECT * FROM ods.src_dir");
        assertFalse(lineage.isParseError(), "INSERT DIRECTORY 应可解析");
        assertTrue(lineage.getTargetTable() == null || lineage.getTargetTable().isEmpty(),
                "DIRECTORY 路径不应成为目标表: " + lineage.getTargetTable());
        assertEquals(java.util.Set.of("ods.src_dir"), lineage.getSourceTables());
    }

    @Test
    void sparkTruncateAndInsertValues() {
        TableLineage trunc = spark.extractFromSql("TRUNCATE TABLE dwd.t_trunc PARTITION (dt = '2026-09-20')");
        assertFalse(trunc.isParseError(), "TRUNCATE 应可解析");
        assertTrue(trunc.getTargetTable() == null || trunc.getTargetTable().isEmpty(),
                "TRUNCATE 不产生数据流");

        assertSparkLineage("INSERT INTO dwd.t_val VALUES (1, 'a'), (2, 'b')", "dwd.t_val");
    }

    @Test
    void sparkIntervalLiteral() {
        assertSparkLineage("INSERT INTO dwd.t_int SELECT ts + INTERVAL '1' DAY FROM ods.s_int",
                "dwd.t_int", "ods.s_int");
    }

    @Test
    void prestoRowConstructorAndGenericRowType() {
        assertPrestoLineage("INSERT INTO dwd.t_row SELECT CAST(ROW(1, 'a') AS ROW(a INTEGER, b VARCHAR)) FROM ods.s_row",
                "dwd.t_row", "ods.s_row");
        assertPrestoLineage("INSERT INTO dwd.t_arr SELECT ARRAY[1, 2, 3], m['k'] FROM ods.s_arr",
                "dwd.t_arr", "ods.s_arr");
    }

    @Test
    void prestoExtractAndIntervalAndCharacterVarying() {
        assertPrestoLineage("INSERT INTO dwd.t_ext SELECT EXTRACT(DAY FROM ts), ts + INTERVAL '1' HOUR FROM ods.s_ext",
                "dwd.t_ext", "ods.s_ext");
        assertPrestoLineage("INSERT INTO dwd.t_cv SELECT CAST(c AS CHARACTER VARYING(20)) FROM ods.s_cv",
                "dwd.t_cv", "ods.s_cv");
    }

    @Test
    void prestoUnnestWithOrdinalityAndLeftJoinOnTrue() {
        assertPrestoLineage("INSERT INTO dwd.t_uo SELECT x.item, x.n FROM ods.src_uo "
                + "CROSS JOIN UNNEST(src_uo.arr, src_uo.arr2) WITH ORDINALITY AS x(item, n)",
                "dwd.t_uo", "ods.src_uo");
        assertPrestoLineage("INSERT INTO dwd.t_ul SELECT * FROM ods.src_ul "
                + "LEFT JOIN UNNEST(src_ul.arr) AS x(item) ON TRUE",
                "dwd.t_ul", "ods.src_ul");
    }

    @Test
    void prestoTablesampleGroupingSetsAndExists() {
        assertPrestoLineage("INSERT INTO dwd.t_ts SELECT * FROM ods.s_ts TABLESAMPLE BERNOULLI (10)",
                "dwd.t_ts", "ods.s_ts");
        assertPrestoLineage("INSERT INTO dwd.t_pgs SELECT a, count(*) FROM ods.s_pgs GROUP BY GROUPING SETS ((a), ())",
                "dwd.t_pgs", "ods.s_pgs");
        assertPrestoLineage("INSERT INTO dwd.t_pex SELECT * FROM ods.a WHERE EXISTS (SELECT 1 FROM ods.b WHERE b.id = a.id)",
                "dwd.t_pex", "ods.a", "ods.b");
    }

    /** 词法层守护：LATERAL VIEW 这类多词关键字若写成单个词法规则，会退化成匹配 LATERALVIEW */
    @Test
    void sparkLateralViewSpelledAsTwoWordsStillParses() {
        assertSparkLineage("INSERT INTO dwd.t_lv2 SELECT tag FROM ods.src_lv2 LATERAL VIEW explode(tags) t AS tag",
                "dwd.t_lv2", "ods.src_lv2");
        TableLineage joined = spark.extractFromSql(
                "INSERT INTO dwd.t_lv3 SELECT a.id, t.tag FROM ods.src_a a "
                        + "JOIN ods.src_b b ON a.id = b.id LATERAL VIEW explode(b.tags) t AS tag");
        assertFalse(joined.isParseError(), "JOIN 之后接 LATERAL VIEW 应可解析");
        assertEquals(java.util.Set.of("ods.src_a", "ods.src_b"), joined.getSourceTables());
    }
}
