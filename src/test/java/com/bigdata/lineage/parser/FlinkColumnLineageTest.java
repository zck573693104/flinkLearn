package com.bigdata.lineage.parser;

import com.bigdata.lineage.parser.extractor.FlinkColumnLineageExtractor;
import com.bigdata.lineage.parser.extractor.TableLineageExtractor;
import com.bigdata.lineage.parser.model.ColumnDerivation;
import com.bigdata.lineage.parser.model.ColumnEdge;
import com.bigdata.lineage.parser.model.ColumnRef;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Flink 字段级血缘回归测试：表级测试已覆盖的通用语义这里只抽查，
 * 重点放在 Flink 独有构造——窗口 TVF、TABLE(TVM(...))、时态表 JOIN、UNNEST、CTAS。
 */
class FlinkColumnLineageTest {

    private final FlinkColumnLineageExtractor column = new FlinkColumnLineageExtractor();
    private final TableLineageExtractor table = new TableLineageExtractor();

    private ColumnAssert check(String sql) {
        return ColumnAssert.of(sql, table::extractFromSql, column::extractFromSql);
    }

    @Test
    void plainAndQualifiedColumns() {
        check("INSERT INTO dwd.tgt SELECT id, s.name FROM ods.src s")
                .count(2)
                .sources("dwd.tgt", "id", "ods.src.id")
                .sources("dwd.tgt", "name", "ods.src.name")
                .derivation("dwd.tgt", "id", ColumnDerivation.IDENTITY)
                .noGhostColumns();
    }

    @Test
    void temporalJoinKeepsBothSides() {
        check("INSERT INTO dwd.tgt SELECT p.amount, d.rate "
                + "FROM ods.probe AS p JOIN dim.cur FOR SYSTEM_TIME AS OF p.proctime AS d "
                + "ON p.cid = d.cid")
                .sources("dwd.tgt", "amount", "ods.probe.amount")
                .sources("dwd.tgt", "rate", "dim.cur.rate");
    }

    @Test
    void windowTvfReadsThroughItsInputTable() {
        check("INSERT INTO dwd.tgt SELECT word FROM TUMBLE(ods.src, ts, INTERVAL '5' MINUTE)")
                .sources("dwd.tgt", "word", "ods.src.word");
    }

    @Test
    void descriptorTvfKeepsTheOuterAlias() {
        check("INSERT INTO dwd.tgt SELECT w.word "
                + "FROM TABLE(TUMBLE(TABLE ods.src, DESCRIPTOR(ts), INTERVAL '5' MINUTE)) AS w")
                .sources("dwd.tgt", "word", "ods.src.word");
    }

    @Test
    void unnestColumnTracesBackToTheArrayColumn() {
        ColumnAssert result = check("INSERT INTO dwd.tgt SELECT u.word FROM ods.src AS s "
                + "CROSS JOIN UNNEST(s.arr) AS u (word)");
        String pseudo = result.pseudoTable("#unnest");
        result.sources(pseudo, "word", "ods.src.arr")
                .sources("dwd.tgt", "word", pseudo + ".word")
                .noGhostColumns();
    }

    @Test
    void cteChainAndColumnListAnchor() {
        check("INSERT INTO dwd.tgt WITH c(a, b) AS (SELECT id, name FROM ods.src), "
                + "d AS (SELECT a FROM c) SELECT a FROM d")
                .sources("c", "a", "ods.src.id")
                .sources("d", "a", "c.a")
                .sources("dwd.tgt", "a", "d.a");
    }

    @Test
    void insertInsideWithClause() {
        check("INSERT INTO dwd.tgt WITH c AS (SELECT id FROM ods.src) SELECT id FROM c")
                .sources("c", "id", "ods.src.id")
                .sources("dwd.tgt", "id", "c.id");
    }

    @Test
    void windowFunctionOverPartitionAndOrder() {
        check("INSERT INTO dwd.tgt SELECT id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts) AS rn "
                + "FROM ods.src")
                .sources("dwd.tgt", "id", "ods.src.id")
                .sources("dwd.tgt", "rn", "ods.src.k", "ods.src.ts")
                .derivation("dwd.tgt", "rn", ColumnDerivation.AGGREGATE);
    }

    @Test
    void caseWhenBranchesAllCount() {
        check("INSERT INTO dwd.tgt SELECT CASE WHEN a = 1 THEN b ELSE c END AS flag FROM ods.src")
                .sources("dwd.tgt", "flag", "ods.src.a", "ods.src.b", "ods.src.c")
                .derivation("dwd.tgt", "flag", ColumnDerivation.EXPRESSION);
    }

    @Test
    void starAndQualifiedStarStayUnexpanded() {
        check("INSERT INTO dwd.tgt SELECT * FROM ods.src")
                .count(1)
                .sources("dwd.tgt", "*", "ods.src.*")
                .derivation("dwd.tgt", "*", ColumnDerivation.STAR);

        ColumnAssert joined = check(
                "INSERT INTO dwd.tgt SELECT a.* FROM ods.a a JOIN ods.b b ON a.id = b.id");
        assertEquals(1, joined.toTargets("dwd.tgt").size(), joined.describe());
        joined.sources("dwd.tgt", "*", "ods.a.*");
    }

    @Test
    void ctasAndCreateViewProduceEdges() {
        check("CREATE TABLE dwd.tgt AS SELECT id FROM ods.src")
                .sources("dwd.tgt", "id", "ods.src.id");
        check("CREATE TEMPORARY VIEW v AS SELECT id AS vid FROM ods.src")
                .sources("v", "vid", "ods.src.id");
    }

    @Test
    void pureDdlProducesNoEdges() {
        assertTrue(check("CREATE TABLE t (id BIGINT, name STRING, "
                + "WATERMARK FOR rowtime AS rowtime - INTERVAL '5' SECOND) "
                + "WITH ('connector' = 'kafka')").edges().isEmpty(), "纯 DDL 没有来源列");
    }

    @Test
    void ctasColumnListNamesTheColumnsOfEachPosition() {
        check("CREATE TABLE dwd.tgt (id BIGINT, nm STRING) AS SELECT a, b FROM ods.src")
                .sources("dwd.tgt", "id", "ods.src.a")
                .sources("dwd.tgt", "nm", "ods.src.b");
    }

    @Test
    void subqueryAndUnionBranches() {
        ColumnAssert sub = check("INSERT INTO dwd.tgt SELECT x.id FROM (SELECT id FROM ods.src) x");
        String relation = sub.pseudoTable("#sub");
        sub.sources(relation, "id", "ods.src.id").sources("dwd.tgt", "id", relation + ".id");

        ColumnAssert union = check("INSERT INTO dwd.tgt SELECT a FROM ods.s1 "
                + "UNION ALL SELECT b AS a FROM ods.s2");
        List<String> sources = new ArrayList<>();
        for (ColumnEdge edge : union.toTargets("dwd.tgt")) {
            sources.add(edge.getSources().get(0).nodeId());
        }
        assertEquals(Arrays.asList("ods.s1.a", "ods.s2.b"), sources, union.describe());
    }

    @Test
    void unresolvedQualifierIsAdmittedNotGuessed() {
        check("INSERT INTO dwd.tgt SELECT id FROM ods.a JOIN ods.b ON a.k = b.k")
                .derivation("dwd.tgt", "id", ColumnDerivation.UNRESOLVED);
        // 限定符既不是作用域里的表，根列又落在两张未知表上：只能承认绑不上
        check("INSERT INTO dwd.tgt SELECT z.id FROM ods.a JOIN ods.b ON a.k = b.k")
                .derivation("dwd.tgt", "id", ColumnDerivation.UNRESOLVED);
    }

    /** 真实语料形态：ROW 列的字段访问路径不是"库.表"，取值来自根列 */
    @Test
    void structFieldAccessTracesToTheRootColumn() {
        ColumnAssert result = check("INSERT INTO dwd.tgt SELECT operation.info_str "
                + "FROM ods.src");
        result.count(1)
                .sources("dwd.tgt", "info_str", "ods.src.operation")
                .derivation("dwd.tgt", "info_str", ColumnDerivation.IDENTITY)
                .noGhostColumns();
        ColumnRef evidence = result.edges().get(0).getSources().get(0);
        assertEquals("operation.info_str", evidence.getRawText(), "字段路径要留给 UI 当证据");
    }

    /** 语料形态：逗号 + UNNEST 展开，展开列与主表列各归各的来源 */
    @Test
    void unnestOverCommaJoinKeepsBothRelations() {
        ColumnAssert result = check("INSERT INTO user_app_data SELECT t.app_name, k.day "
                + "FROM ods.kafka AS k, UNNEST(k.eventBodyList) AS t (app_name)");
        String pseudo = result.pseudoTable("#unnest");
        result.sources(pseudo, "app_name", "ods.kafka.eventbodylist")
                .sources("user_app_data", "app_name", pseudo + ".app_name")
                .sources("user_app_data", "day", "ods.kafka.day")
                .noGhostColumns();
    }

    /** 语料形态：字段路径的根列与 UNNEST 并列时，展开列报得出表头，根列只可能属于另一张 */
    @Test
    void structRootSurvivesCommaUnnestSibling() {
        check("INSERT INTO dwd.tgt SELECT operation.info_str FROM ods.kafka, "
                + "UNNEST(eventBodyList) AS t (app_name)")
                .sources("dwd.tgt", "info_str", "ods.kafka.operation");
    }

    @Test
    void insertColumnListNamesUnnamedExpressions() {
        check("INSERT INTO dwd.tgt (x, y) SELECT a, count(*) FROM ods.src GROUP BY a")
                .sources("dwd.tgt", "x", "ods.src.a")
                .sources("dwd.tgt", "y", "ods.src.*")
                .derivation("dwd.tgt", "y", ColumnDerivation.POSITIONAL);
    }
}
