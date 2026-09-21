package com.bigdata.lineage.parser;

import com.bigdata.lineage.parser.extractor.PrestoColumnLineageExtractor;
import com.bigdata.lineage.parser.extractor.PrestoTableLineageExtractor;
import com.bigdata.lineage.parser.model.ColumnDerivation;
import com.bigdata.lineage.parser.model.ColumnEdge;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Presto/Trino 字段级血缘回归测试：通用语义由 Spark 用例守着（同一份 ColumnLineageEngine），
 * 这里只压 Presto 独有构造——UNNEST 与 WITH ORDINALITY、EXTRACT、TRY_CAST、
 * ROW/MAP 构造器、UPDATE SET、CTAS 与 CREATE VIEW。
 */
class PrestoColumnLineageTest {

    private final PrestoColumnLineageExtractor column = new PrestoColumnLineageExtractor();
    private final PrestoTableLineageExtractor table = new PrestoTableLineageExtractor();

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
    void unnestColumnTracesBackToTheArrayColumn() {
        ColumnAssert result = check("INSERT INTO dwd.tgt SELECT u.word FROM ods.src AS s "
                + "CROSS JOIN UNNEST(s.arr) AS u (word)");
        String pseudo = result.pseudoTable("#unnest");
        result.sources(pseudo, "word", "ods.src.arr")
                .derivation(pseudo, "word", ColumnDerivation.EXPRESSION)
                .sources("dwd.tgt", "word", pseudo + ".word")
                .noGhostColumns();
    }

    @Test
    void multipleArraysMapToTheirOwnColumns() {
        ColumnAssert result = check("INSERT INTO dwd.tgt SELECT x.k, x.v FROM ods.src AS s "
                + "CROSS JOIN UNNEST(s.keys, s.vals) AS x (k, v)");
        String pseudo = result.pseudoTable("#unnest");
        // 位置一一对应：k 只来自 keys，不能把 vals 也拖进来
        result.sources(pseudo, "k", "ods.src.keys")
                .sources(pseudo, "v", "ods.src.vals")
                .sources("dwd.tgt", "k", pseudo + ".k")
                .noGhostColumns();
    }

    @Test
    void ordinalityCounterColumnIsNotAValueSource() {
        ColumnAssert result = check("INSERT INTO dwd.tgt SELECT u.word, u.ord "
                + "FROM ods.src AS s CROSS JOIN UNNEST(s.arr) WITH ORDINALITY AS u (word, ord)");
        String pseudo = result.pseudoTable("#unnest");
        result.sources(pseudo, "word", "ods.src.arr")
                .sources(pseudo, "ord")
                .derivation(pseudo, "ord", ColumnDerivation.CONSTANT)
                .sources("dwd.tgt", "ord", pseudo + ".ord")
                .noGhostColumns();
    }

    @Test
    void unnestWithRowAliasOnly() {
        ColumnAssert result = check("INSERT INTO dwd.tgt SELECT t FROM ods.src AS s "
                + "CROSS JOIN UNNEST(s.tags) AS t");
        String pseudo = result.pseudoTable("#unnest");
        result.sources(pseudo, "t", "ods.src.tags")
                .sources("dwd.tgt", "t", pseudo + ".t");
    }

    @Test
    void constantArrayUnnestHasNoUpstreamColumn() {
        ColumnAssert result = check("INSERT INTO dwd.tgt SELECT u.word FROM ods.src AS s "
                + "CROSS JOIN UNNEST(ARRAY['a', 'b']) AS u (word)");
        String pseudo = result.pseudoTable("#unnest");
        result.sources(pseudo, "word").derivation(pseudo, "word", ColumnDerivation.CONSTANT);
    }

    /** 乘号与星号在语法里是同一个 token，乘法不能被当成"全部列" */
    @Test
    void multiplicationIsNotAStar() {
        check("INSERT INTO dwd.tgt SELECT price * qty AS gmv FROM ods.src")
                .count(1)
                .sources("dwd.tgt", "gmv", "ods.src.price", "ods.src.qty")
                .derivation("dwd.tgt", "gmv", ColumnDerivation.EXPRESSION)
                .noGhostColumns();
    }

    @Test
    void extractCastAndTryCastKeepTheirOperand() {
        check("INSERT INTO dwd.tgt SELECT EXTRACT(DAY FROM ts) AS d, CAST(a AS BIGINT) AS b, "
                + "TRY_CAST(x AS TIMESTAMP) AS y FROM ods.src")
                .sources("dwd.tgt", "d", "ods.src.ts")
                .sources("dwd.tgt", "b", "ods.src.a")
                .sources("dwd.tgt", "y", "ods.src.x")
                .derivation("dwd.tgt", "d", ColumnDerivation.EXPRESSION)
                .noGhostColumns();
    }

    @Test
    void rowAndMapConstructorsTraceEveryElement() {
        check("INSERT INTO dwd.tgt SELECT ROW(a, b) AS r, MAP(ARRAY['k1', 'k2'], ARRAY[x, y]) AS m "
                + "FROM ods.src")
                .sources("dwd.tgt", "r", "ods.src.a", "ods.src.b")
                .sources("dwd.tgt", "m", "ods.src.x", "ods.src.y")
                .derivation("dwd.tgt", "r", ColumnDerivation.EXPRESSION)
                .noGhostColumns();
    }

    @Test
    void arraySubscriptKeepsTheBaseColumn() {
        check("INSERT INTO dwd.tgt SELECT tags[1] AS head, CARDINALITY(arr) AS n FROM ods.src")
                .sources("dwd.tgt", "head", "ods.src.tags")
                .sources("dwd.tgt", "n", "ods.src.arr")
                .noGhostColumns();
    }

    /** 高階函数写法是 Presto 的常规操作：{@code x -> ...} 里的 x 是形参，绑不到任何表 */
    @Test
    void lambdaParameterIsNotAColumn() {
        check("INSERT INTO dwd.tgt SELECT CARDINALITY(FILTER(SPLIT(tags, ','), x -> x <> 'a')) AS n "
                + "FROM ods.src")
                .sources("dwd.tgt", "n", "ods.src.tags")
                .derivation("dwd.tgt", "n", ColumnDerivation.EXPRESSION)
                .noGhostColumns();
    }

    /** 括号形式（{@code current_timestamp()}）本来就是函数，别把无参关键字那一套扩到它身上 */
    @Test
    void parenthesizedNiladicFunctionIsStillNotAColumn() {
        check("INSERT INTO dwd.tgt SELECT date_format(current_timestamp(), 'yyyyMMdd') AS d "
                + "FROM ods.src")
                .sources("dwd.tgt", "d")
                .derivation("dwd.tgt", "d", ColumnDerivation.CONSTANT);
    }

    @Test
    void windowFunctionIsAggregateAndCarriesItsKeys() {
        check("INSERT INTO dwd.tgt SELECT id, "
                + "sum(amount) OVER (PARTITION BY uid ORDER BY ts) AS running FROM ods.src")
                .sources("dwd.tgt", "running", "ods.src.amount", "ods.src.uid", "ods.src.ts")
                .derivation("dwd.tgt", "running", ColumnDerivation.AGGREGATE);
    }

    @Test
    void cteChainAndColumnListAnchor() {
        check("INSERT INTO dwd.tgt WITH c(a, b) AS (SELECT id, name FROM ods.src), "
                + "d AS (SELECT a FROM c) SELECT a FROM d")
                .sources("c", "a", "ods.src.id")
                .sources("c", "b", "ods.src.name")
                .sources("d", "a", "c.a")
                .sources("dwd.tgt", "a", "d.a")
                .noGhostColumns();
    }

    @Test
    void starAndQualifiedStarStayUnexpanded() {
        check("INSERT INTO dwd.tgt SELECT * FROM ods.src")
                .count(1)
                .sources("dwd.tgt", "*", "ods.src.*")
                .derivation("dwd.tgt", "*", ColumnDerivation.STAR);

        check("INSERT INTO dwd.tgt SELECT a.* FROM ods.a a JOIN ods.b b ON a.id = b.id")
                .sources("dwd.tgt", "*", "ods.a.*");
    }

    @Test
    void subqueryAndUnionBranches() {
        ColumnAssert sub = check("INSERT INTO dwd.tgt SELECT x.id FROM (SELECT id FROM ods.src) x");
        String relation = sub.pseudoTable("#sub");
        sub.sources(relation, "id", "ods.src.id").sources("dwd.tgt", "id", relation + ".id");

        ColumnAssert union = check("INSERT INTO dwd.tgt SELECT a FROM ods.s1 "
                + "UNION ALL SELECT b FROM ods.s2");
        union.derivation("dwd.tgt", "a", ColumnDerivation.IDENTITY);
        List<String> sources = new ArrayList<>();
        for (ColumnEdge edge : union.toTargets("dwd.tgt")) {
            sources.add(edge.getSources().get(0).nodeId());
        }
        assertEquals(Arrays.asList("ods.s1.a", "ods.s2.b"), sources, union.describe());
    }

    @Test
    void updateStatementBindsAgainstItsOwnTable() {
        check("UPDATE dwd.balance AS b SET amount = amount * rate WHERE id > 0")
                .count(1)
                .sources("dwd.balance", "amount", "dwd.balance.amount", "dwd.balance.rate")
                .derivation("dwd.balance", "amount", ColumnDerivation.EXPRESSION)
                .noGhostColumns();
    }

    @Test
    void multipleAssignmentsKeepPositions() {
        check("UPDATE dwd.tgt SET a = x, b = y WHERE k > 0")
                .ordinal("dwd.tgt", "a", 0)
                .ordinal("dwd.tgt", "b", 1);
    }

    @Test
    void ctasAndViewProduceEdges() {
        check("CREATE TABLE dwd.tgt AS SELECT id, name AS nm FROM ods.src")
                .sources("dwd.tgt", "id", "ods.src.id")
                .sources("dwd.tgt", "nm", "ods.src.name");
        check("CREATE VIEW v AS SELECT id AS vid FROM ods.src")
                .sources("v", "vid", "ods.src.id");
    }

    @Test
    void valuesOnlyProducesConstants() {
        check("INSERT INTO dwd.tgt (a, b) VALUES (1, 'x')")
                .count(2)
                .sources("dwd.tgt", "a")
                .derivation("dwd.tgt", "a", ColumnDerivation.CONSTANT)
                .ordinal("dwd.tgt", "b", 1);
    }

    @Test
    void pureDdlProducesNoEdges() {
        assertTrue(check("CREATE TABLE t (id BIGINT, name VARCHAR) WITH (format = 'ORC')")
                .edges().isEmpty(), "纯 DDL 没有来源列");
    }

    @Test
    void unresolvedQualifierIsAdmittedNotGuessed() {
        check("INSERT INTO dwd.tgt SELECT id FROM ods.a JOIN ods.b ON a.k = b.k")
                .derivation("dwd.tgt", "id", ColumnDerivation.UNRESOLVED);
        check("INSERT INTO dwd.tgt SELECT z.id FROM ods.a JOIN ods.b ON a.k = b.k")
                .derivation("dwd.tgt", "id", ColumnDerivation.UNRESOLVED);
    }

    /** ROW 列的字段访问路径：根列才是来源，末段只是往下钻 */
    @Test
    void structFieldAccessTracesToTheRootColumn() {
        check("INSERT INTO dwd.tgt SELECT operation.info_str FROM ods.src")
                .count(1)
                .sources("dwd.tgt", "info_str", "ods.src.operation")
                .derivation("dwd.tgt", "info_str", ColumnDerivation.IDENTITY)
                .noGhostColumns();
    }
}
