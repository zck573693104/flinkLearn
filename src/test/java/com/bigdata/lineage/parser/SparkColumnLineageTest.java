package com.bigdata.lineage.parser;

import com.bigdata.lineage.parser.extractor.SparkColumnLineageExtractor;
import com.bigdata.lineage.parser.extractor.SparkTableLineageExtractor;
import com.bigdata.lineage.parser.model.ColumnDerivation;
import com.bigdata.lineage.parser.model.ColumnEdge;
import com.bigdata.lineage.parser.model.ColumnRef;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spark 字段级血缘回归测试：直通/改名/限定/表达式/聚合/常量/位置命名/CTE 链/
 * 子查询链/作用域隔离/LATERAL VIEW/集合操作/星号/标量子查询。
 */
class SparkColumnLineageTest {

    private final SparkColumnLineageExtractor column = new SparkColumnLineageExtractor();
    private final SparkTableLineageExtractor table = new SparkTableLineageExtractor();

    private ColumnAssert check(String sql) {
        return ColumnAssert.of(sql, table::extractFromSql, column::extractFromSql);
    }

    // ============================================
    // 直通、改名与限定
    // ============================================

    @Test
    void plainColumnsAreIdentityEdges() {
        ColumnAssert result = check("INSERT INTO dwd.tgt SELECT id, name FROM ods.src")
                .count(2)
                .sources("dwd.tgt", "id", "ods.src.id")
                .derivation("dwd.tgt", "id", ColumnDerivation.IDENTITY)
                .sources("dwd.tgt", "name", "ods.src.name")
                .noGhostColumns();

        assertEquals("dwd.tgt", result.edges().get(0).getTargetTable());
    }

    @Test
    void aliasRenamesTargetColumnOnly() {
        check("INSERT INTO dwd.tgt SELECT id AS user_id FROM ods.src")
                .sources("dwd.tgt", "user_id", "ods.src.id")
                .derivation("dwd.tgt", "user_id", ColumnDerivation.IDENTITY);
    }

    @Test
    void shortTableQualifierBindsToFullTableName() {
        check("INSERT INTO dwd.tgt SELECT s.id FROM ods.src s")
                .sources("dwd.tgt", "id", "ods.src.id")
                .qualifierOf("dwd.tgt", "id", "s")
                .noGhostColumns();
    }

    @Test
    void dbQualifiedReferenceBindsToPhysicalTable() {
        check("INSERT INTO dwd.tgt SELECT ods.src.id FROM ods.src")
                .sources("dwd.tgt", "id", "ods.src.id");
    }

    // ============================================
    // 表达式、聚合与常量
    // ============================================

    @Test
    void expressionCarriesEveryColumn() {
        check("INSERT INTO dwd.tgt SELECT concat(a, '-', b) AS c FROM ods.src")
                .sources("dwd.tgt", "c", "ods.src.a", "ods.src.b")
                .derivation("dwd.tgt", "c", ColumnDerivation.EXPRESSION)
                .transformContains("dwd.tgt", "c", "concat");
    }

    @Test
    void aggregateOverStarIsStillAggregate() {
        check("INSERT INTO dwd.tgt SELECT k, count(*) AS c FROM ods.src GROUP BY k")
                .sources("dwd.tgt", "k", "ods.src.k")
                .sources("dwd.tgt", "c", "ods.src.*")
                .derivation("dwd.tgt", "c", ColumnDerivation.AGGREGATE)
                .ordinal("dwd.tgt", "c", 1);
    }

    @Test
    void literalOnlyColumnIsConstant() {
        check("INSERT INTO dwd.tgt SELECT 'x' AS v, 1 AS n, id FROM ods.src")
                .derivation("dwd.tgt", "v", ColumnDerivation.CONSTANT)
                .sources("dwd.tgt", "v")
                .sources("dwd.tgt", "id", "ods.src.id");
    }

    @Test
    void valuesClauseNamesColumnsFromInsertList() {
        check("INSERT INTO dwd.tgt (a, b) VALUES (1, 'x')")
                .count(2)
                .derivation("dwd.tgt", "a", ColumnDerivation.CONSTANT)
                .sources("dwd.tgt", "b");
    }

    // ============================================
    // 目标列命名：INSERT 列名清单优先，其次别名，最后才承认猜不到
    // ============================================

    @Test
    void insertColumnListAlignsByPosition() {
        check("INSERT INTO dwd.tgt (x, y) SELECT a, b FROM ods.src")
                .sources("dwd.tgt", "x", "ods.src.a")
                .sources("dwd.tgt", "y", "ods.src.b")
                .derivation("dwd.tgt", "x", ColumnDerivation.IDENTITY);
    }

    @Test
    void unnamedExpressionWithInsertListIsPositional() {
        check("INSERT INTO dwd.tgt (x) SELECT a + b FROM ods.src")
                .derivation("dwd.tgt", "x", ColumnDerivation.POSITIONAL)
                .sources("dwd.tgt", "x", "ods.src.a", "ods.src.b");
    }

    @Test
    void unnamedExpressionWithoutInsertListIsUnnamed() {
        ColumnEdge edge = check("INSERT INTO dwd.tgt SELECT a + b FROM ods.src")
                .derivation("dwd.tgt", "expr_1", ColumnDerivation.UNNAMED)
                .edge("dwd.tgt", "expr_1");
        assertEquals(0.40, edge.getConfidence(), 1e-9);
    }

    // ============================================
    // CTE
    // ============================================

    @Test
    void cteColumnListIsTheNamingAnchor() {
        check("INSERT INTO dwd.tgt WITH c(a, b) AS (SELECT id, name FROM ods.src) SELECT a FROM c")
                .sources("c", "a", "ods.src.id")
                .derivation("c", "a", ColumnDerivation.IDENTITY)
                .sources("dwd.tgt", "a", "c.a")
                .noGhostColumns();
    }

    @Test
    void cteChainKeepsOneHopPerLevel() {
        check("INSERT INTO dwd.tgt WITH c1 AS (SELECT id FROM ods.a), "
                + "c2 AS (SELECT id FROM c1) SELECT id FROM c2")
                .sources("c1", "id", "ods.a.id")
                .sources("c2", "id", "c1.id")
                .sources("dwd.tgt", "id", "c2.id");
    }

    @Test
    void withBeforeInsertRegistersCtes() {
        check("WITH c AS (SELECT id FROM ods.a) INSERT INTO dwd.tgt SELECT id FROM c")
                .sources("c", "id", "ods.a.id")
                .sources("dwd.tgt", "id", "c.id");
    }

    // ============================================
    // 子查询与作用域隔离
    // ============================================

    @Test
    void aliasedSubqueryBecomesRelation() {
        check("INSERT INTO dwd.tgt SELECT x.xid FROM (SELECT id AS xid FROM ods.src) x")
                .sources("x", "xid", "ods.src.id")
                .sources("dwd.tgt", "xid", "x.xid")
                .noGhostColumns();
    }

    @Test
    void unaliasedSubqueryBecomesPseudoRelation() {
        ColumnAssert result = check("INSERT INTO dwd.tgt SELECT id FROM (SELECT id FROM ods.src)")
                .count(2);
        List<ColumnEdge> outer = result.toTargets("dwd.tgt");
        assertEquals(1, outer.size(), result.describe());
        String source = outer.get(0).getSources().get(0).nodeId();
        assertTrue(source.startsWith("#sub"), "无别名子查询应挂伪节点: " + source);
        assertTrue(source.endsWith(".id"), source);
    }

    @Test
    void siblingSubqueriesDoNotShareAliases() {
        check("INSERT INTO dwd.tgt SELECT u.id FROM (SELECT id FROM ods.a) u "
                + "JOIN (SELECT id FROM ods.b) v ON u.id = v.id")
                .sources("u", "id", "ods.a.id")
                .sources("v", "id", "ods.b.id")
                .sources("dwd.tgt", "id", "u.id");
    }

    @Test
    void unionBranchNamesComeFromTheFirstBranchEvenWithoutAlias() {
        ColumnAssert result = check(
                "INSERT INTO dwd.tgt SELECT a FROM ods.s1 UNION ALL SELECT b FROM ods.s2");
        List<String> columns = new ArrayList<>();
        List<String> sources = new ArrayList<>();
        for (ColumnEdge edge : result.toTargets("dwd.tgt")) {
            columns.add(edge.getTargetColumn());
            sources.add(edge.getSources().get(0).nodeId());
        }
        // 第二分支的 b 不是目标列名：集合操作对外只暴露第一分支的表头
        assertEquals(Arrays.asList("a", "a"), columns, result.describe());
        assertEquals(Arrays.asList("ods.s1.a", "ods.s2.b"), sources, result.describe());
    }

    @Test
    void everyBranchOfAChainedSetOperationEmitsEdges() {
        ColumnAssert result = check("INSERT INTO dwd.tgt SELECT a FROM ods.s1 "
                + "UNION ALL SELECT b FROM ods.s2 INTERSECT SELECT c FROM ods.s3");
        // A UNION B INTERSECT C 在语法里右递归成 A UNION (B INTERSECT C)，只看直接子节点会丢末分支
        assertEquals(3, result.edges().size(), result.describe());
        for (ColumnEdge edge : result.edges()) {
            assertEquals("a", edge.getTargetColumn(), result.describe());
        }
    }

    // ============================================
    // LATERAL VIEW
    // ============================================

    @Test
    void lateralViewOutputTracesBackToExplodeArgument() {
        ColumnAssert result = check("INSERT INTO dwd.tgt SELECT elem FROM ods.base "
                + "LATERAL VIEW explode(ods.base.arr) e AS elem");
        String pseudo = result.pseudoTable("#lat");
        result.sources(pseudo, "elem", "ods.base.arr")
                .sources("dwd.tgt", "elem", pseudo + ".elem")
                .noGhostColumns();
    }

    @Test
    void posexplodePositionColumnHasNoUpstreamField() {
        ColumnAssert result = check("INSERT INTO dwd.tgt SELECT pos, elem FROM ods.base "
                + "LATERAL VIEW posexplode(ods.base.arr) e AS pos, elem");
        String pseudo = result.pseudoTable("#lat");
        result.sources(pseudo, "pos", new String[0])
                .derivation(pseudo, "pos", ColumnDerivation.CONSTANT)
                .sources(pseudo, "elem", "ods.base.arr");
    }

    // ============================================
    // 星号（v1 不展开）
    // ============================================

    @Test
    void bareStarStaysUnexpanded() {
        ColumnAssert result = check("INSERT INTO dwd.tgt SELECT * FROM ods.src")
                .count(1)
                .sources("dwd.tgt", "*", "ods.src.*")
                .derivation("dwd.tgt", "*", ColumnDerivation.STAR);
        assertEquals(0.30, result.edge("dwd.tgt", "*").getConfidence(), 1e-9);
    }

    @Test
    void qualifiedStarOnlyPicksItsOwnTable() {
        ColumnAssert result = check(
                "INSERT INTO dwd.tgt SELECT a.* FROM ods.a a JOIN ods.b b ON a.id = b.id");
        assertEquals(1, result.toTargets("dwd.tgt").size(), result.describe());
        result.sources("dwd.tgt", "*", "ods.a.*");
    }

    // ============================================
    // 标量子查询
    // ============================================

    @Test
    void scalarSubqueryBecomesPseudoSource() {
        ColumnAssert result = check("INSERT INTO dwd.tgt SELECT id, "
                + "(SELECT max(v) AS mv FROM ods.dim WHERE dim.k = src.k) AS mv FROM ods.src");
        String pseudo = result.pseudoTable("#sub");
        result.sources("dwd.tgt", "id", "ods.src.id")
                .sources(pseudo, "mv", "ods.dim.v")
                .derivation(pseudo, "mv", ColumnDerivation.AGGREGATE)
                .sources("dwd.tgt", "mv", pseudo + ".mv");
    }

    // ============================================
    // 绑不上时承认猜不到
    // ============================================

    @Test
    void unknownQualifierOverTwoTablesIsUnresolved() {
        // z 既不是作用域里的表，根列又可能属于两张未知表：只能承认绑不上
        ColumnAssert result = check("INSERT INTO dwd.tgt SELECT z.id FROM ods.a JOIN ods.b ON a.k = b.k")
                .derivation("dwd.tgt", "id", ColumnDerivation.UNRESOLVED)
                .sources("dwd.tgt", "id", "z.id");
        assertNull(result.edge("dwd.tgt", "id").getSources().get(0).getBoundTable(),
                "限定符绑不上时不该塞给某张表");
    }

    /** 语料里的 ROW 列访问：限定符绑不上但只有一张表时，它是字段路径的根列 */
    @Test
    void structFieldAccessTracesToTheRootColumn() {
        check("INSERT INTO dwd.tgt SELECT operation.info_str FROM ods.src")
                .count(1)
                .sources("dwd.tgt", "info_str", "ods.src.operation")
                .derivation("dwd.tgt", "info_str", ColumnDerivation.IDENTITY)
                .noGhostColumns();
        ColumnRef evidence = check("INSERT INTO dwd.tgt SELECT operation.info_str FROM ods.src")
                .edges().get(0).getSources().get(0);
        assertEquals("operation.info_str", evidence.getRawText(), "字段路径要留给 UI 当证据");
    }

    @Test
    void ambiguousUnqualifiedColumnIsNotForcedOntoATable() {
        check("INSERT INTO dwd.tgt SELECT id FROM ods.a JOIN ods.b ON a.k = b.k")
                .derivation("dwd.tgt", "id", ColumnDerivation.UNRESOLVED);
    }

    /** 内层自己就有表却容不下这列，那是真歧义，不该回头去问外层作用域 */
    @Test
    void innerScopeDoesNotBorrowTheOuterTableForItsOwnColumns() {
        check("INSERT INTO dwd.tgt SELECT x.v FROM "
                + "(SELECT v FROM ods.p JOIN ods.q ON p.k = q.k) x JOIN ods.r ON x.v = r.id")
                .derivation("x", "v", ColumnDerivation.UNRESOLVED)
                .sources("x", "v", "v");
    }

    /** 限定符是库/表命名空间的一部分时，首段不是列名，不能当成字段路径的根 */
    @Test
    void namespaceQualifiedReferenceIsNotAStructPath() {
        check("INSERT INTO dwd.tgt SELECT cat.sch.col FROM cat.sch.tbl")
                .derivation("dwd.tgt", "col", ColumnDerivation.UNRESOLVED)
                .sources("dwd.tgt", "col", "cat.sch.col");
    }

    /** 派生关系报得出自己的列，未限定列就只剩一个可能的东家 */
    @Test
    void unqualifiedColumnFallsToTheOnlyTableWithoutDeclaredColumns() {
        check("INSERT INTO dwd.tgt SELECT payload FROM (SELECT id FROM ods.a) x "
                + "JOIN ods.b ON x.id = b.id")
                .sources("dwd.tgt", "payload", "ods.b.payload")
                .derivation("dwd.tgt", "payload", ColumnDerivation.IDENTITY)
                .noGhostColumns();
    }

    @Test
    void noTargetTableStatementProducesNoEdges() {
        assertTrue(check("SELECT id FROM ods.src").edges().isEmpty(),
                "裸 SELECT 没有写入目标，不该伪造血缘");
        assertTrue(check("DROP TABLE ods.src").edges().isEmpty());
        assertTrue(check("CREATE TABLE t (id BIGINT)").edges().isEmpty(), "纯 DDL 没有来源列");
    }

    @Test
    void updateStatementAssignmentsAreEdges() {
        check("UPDATE dwd.tgt SET b = a + 1 WHERE a > 0")
                .sources("dwd.tgt", "b", "dwd.tgt.a")
                .derivation("dwd.tgt", "b", ColumnDerivation.EXPRESSION);
    }

    // ============================================
    // 多语句
    // ============================================

    @Test
    void multipleStatementsInOneRun() {
        check("INSERT INTO dwd.a SELECT id FROM ods.s1; INSERT INTO dwd.b SELECT id FROM ods.s2;")
                .sources("dwd.a", "id", "ods.s1.id")
                .sources("dwd.b", "id", "ods.s2.id");
    }

    @Test
    void columnRefKeepsRawTextForEvidenceDisplay() {
        ColumnRef ref = check("INSERT INTO dwd.tgt SELECT s.id FROM ods.src s")
                .edge("dwd.tgt", "id").getSources().get(0);
        assertEquals("s.id", ref.getRawText());
    }
}
