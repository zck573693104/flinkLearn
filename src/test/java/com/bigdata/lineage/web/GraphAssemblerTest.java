package com.bigdata.lineage.web;

import com.bigdata.lineage.graph.ColumnLink;
import com.bigdata.lineage.graph.LineageGraph;
import com.bigdata.lineage.graph.ColumnGraphBuilder;
import com.bigdata.lineage.parser.MultiEngineSQLLineageParser;
import com.bigdata.lineage.web.GraphAssembler.Direction;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 子图裁剪与视图序列化：WebUI 拿到的每个 JSON 字段都在这里钉住。
 *
 * <p>只测形状不测解析口径（那是 graph 包的事），关注点是"深度/方向裁掉了什么、
 * 裁完还剩什么证据、表名列名怎么寻址"。
 */
class GraphAssemblerTest {

    private static final MultiEngineSQLLineageParser PARSER = new MultiEngineSQLLineageParser();

    private static final String CHAIN =
            "INSERT INTO dwd.mid SELECT id FROM ods.src;\n"
                    + "INSERT INTO dws.fin SELECT id FROM dwd.mid;";

    private static LineageGraph graphOf(String sql) {
        return ColumnGraphBuilder.build(PARSER.extractTableLineages(sql, false));
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> rows(Map<String, Object> view, String key) {
        return (List<Map<String, Object>>) view.get(key);
    }

    private static Set<String> idsOf(Map<String, Object> view) {
        Set<String> ids = new LinkedHashSet<>();
        for (Map<String, Object> node : rows(view, "nodes")) {
            ids.add(String.valueOf(node.get("id")));
        }
        return ids;
    }

    /** 语句内的 CTE 关系在表级 DAG 里永远是噪音：它没有表级边，摆出来就是一堆孤岛 */
    @Test
    void wholeTableViewContainsOnlyPhysicalRelations() {
        LineageGraph graph = graphOf("WITH t AS (SELECT a AS a FROM ods.src) "
                + "INSERT INTO dws.x SELECT a FROM t");
        Map<String, Object> view = GraphAssembler.tableSubgraph(graph, null, Direction.BOTH, 2)
                .tableView();

        assertEquals(new LinkedHashSet<>(Arrays.asList("ods.src", "dws.x")), idsOf(view));
        for (String id : idsOf(view)) {
            assertTrue(!id.startsWith("["), "局部关系漏进表级视图：" + id);
        }
    }

    @Test
    void depthAndDirectionBoundTheTableNeighborhood() {
        LineageGraph graph = graphOf(CHAIN);

        assertEquals(new LinkedHashSet<>(Arrays.asList("dws.fin", "dwd.mid")),
                idsOf(GraphAssembler.tableSubgraph(graph, "dws.fin", Direction.UP, 1).tableView()));
        assertEquals(new LinkedHashSet<>(Arrays.asList("dws.fin", "dwd.mid", "ods.src")),
                idsOf(GraphAssembler.tableSubgraph(graph, "dws.fin", Direction.UP, 2).tableView()));
        assertEquals(new LinkedHashSet<>(Arrays.asList("ods.src", "dwd.mid")),
                idsOf(GraphAssembler.tableSubgraph(graph, "ods.src", Direction.DOWN, 1)
                        .tableView()));
        assertEquals(new LinkedHashSet<>(Arrays.asList("dwd.mid", "ods.src", "dws.fin")),
                idsOf(GraphAssembler.tableSubgraph(graph, "dwd.mid", Direction.BOTH, 1)
                        .tableView()));
    }

    /** 只画两端都在子图里的边，否则 Cytoscape 会因为悬空端点整图不渲染 */
    @Test
    void trimmedTableEdgesNeverDangle() {
        LineageGraph graph = graphOf(CHAIN);
        Map<String, Object> view = GraphAssembler.tableSubgraph(graph, "dwd.mid", Direction.UP, 1)
                .tableView();

        List<Map<String, Object>> edges = rows(view, "edges");
        assertEquals(1, edges.size());
        assertEquals("ods.src", edges.get(0).get("from"));
        assertEquals("dwd.mid", edges.get(0).get("to"));
        assertTrue(idsOf(view).contains("ods.src"));
    }

    /** 裁剪之后每条边仍要带着完整证据：折叠跳数与加工方式不能因为只取一层就丢掉 */
    @Test
    void columnViewKeepsTheFullEvidenceOfATrimmedEdge() {
        LineageGraph graph = graphOf("INSERT INTO dws.x SELECT s.v FROM "
                + "(SELECT a + 1 AS v FROM ods.src) s");
        Map<String, Object> view = GraphAssembler.columnSubgraph(graph, "dws.x", "v", 1)
                .columnView();

        List<Map<String, Object>> edges = rows(view, "edges");
        assertEquals(1, edges.size());
        assertEquals("ods.src.a", edges.get(0).get("from"));
        assertEquals("dws.x.v", edges.get(0).get("to"));
        assertEquals("EXPRESSION", edges.get(0).get("derivation"));
        assertEquals(3, ((Collection<?>) edges.get(0).get("hops")).size());

        Map<String, Object> sourceNode = nodesById(view).get("ods.src.a");
        assertEquals("ods.src", sourceNode.get("table"));
        assertEquals("a", sourceNode.get("column"));
        assertEquals(Boolean.FALSE, sourceNode.get("local"));
    }

    private static Map<String, Map<String, Object>> nodesById(Map<String, Object> view) {
        Map<String, Map<String, Object>> byId = new java.util.LinkedHashMap<>();
        for (Map<String, Object> node : rows(view, "nodes")) {
            byId.put(String.valueOf(node.get("id")), node);
        }
        return byId;
    }

    /** 整表展开时把该表所有字段都摆出来，一个都没有的表直接报错而不是画空图 */
    @Test
    void wholeTableColumnViewListsEveryColumn() {
        LineageGraph graph = graphOf("INSERT INTO dws.x SELECT a AS v1, b AS v2 FROM ods.src");
        Map<String, Object> view = GraphAssembler.columnSubgraph(graph, "dws.x", null, 1)
                .columnView();

        assertEquals(new LinkedHashSet<>(Arrays.asList("ods.src.a", "dws.x.v1", "ods.src.b",
                "dws.x.v2")), idsOf(view));
        assertEquals(2, rows(view, "edges").size());
    }

    @Test
    void shortAndMixedCaseNamesResolveToTheNormalizedId() {
        LineageGraph graph = graphOf("INSERT INTO dwd.t SELECT s.id FROM ods.src AS s");

        assertEquals("ods.src", GraphAssembler.requireTable(graph, "src"));
        assertEquals("ods.src", GraphAssembler.requireTable(graph, "ODS.SRC"));
        assertEquals("dwd.t", GraphAssembler.requireTable(graph, " dwd.T "));
    }

    @Test
    void ambiguousShortNameIsRejectedInsteadOfGuessed() {
        LineageGraph graph = graphOf("INSERT INTO d1.same SELECT id FROM ods.a;\n"
                + "INSERT INTO d2.same SELECT id FROM ods.b;");

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> GraphAssembler.requireTable(graph, "same"));
        assertTrue(error.getMessage().contains("不唯一"), error.getMessage());
    }

    @Test
    void unknownTableOrColumnTellTheUserWhatIsMissing() {
        LineageGraph graph = graphOf("INSERT INTO dws.x SELECT a AS v FROM ods.src");

        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> GraphAssembler.requireTable(graph, "nope")).getMessage()
                .contains("没有这张表"));
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> GraphAssembler.columnSubgraph(graph, "dws.x", "nope", 1)).getMessage()
                .contains("没有字段 nope"));
    }

    /** 列名大小写不敏感，但图内标识一律用登记过的那份，否则边接不上 */
    @Test
    void columnLookupReturnsTheGraphSpelling() {
        LineageGraph graph = graphOf("INSERT INTO dws.x SELECT a AS v FROM ods.src");
        Map<String, Object> view = GraphAssembler.columnSubgraph(graph, "dws.x", "V", 1)
                .columnView();

        assertTrue(idsOf(view).contains("dws.x.v"), idsOf(view).toString());
        assertEquals("ods.src.a", rows(view, "edges").get(0).get("from"));
    }

    @Test
    void depthInputIsValidatedAndCapped() {
        assertEquals(3, GraphAssembler.clampDepth(null, 3));
        assertEquals(2, GraphAssembler.clampDepth(" 2 ", 3));
        assertEquals(GraphAssembler.MAX_DEPTH, GraphAssembler.clampDepth("999", 2));
        assertThrows(IllegalArgumentException.class, () -> GraphAssembler.clampDepth("0", 2));
        assertThrows(IllegalArgumentException.class, () -> GraphAssembler.clampDepth("abc", 2));
    }

    @Test
    void directionDefaultsToBothAndRejectsTypos() {
        assertEquals(Direction.BOTH, GraphAssembler.Direction.of(null));
        assertEquals(Direction.BOTH, GraphAssembler.Direction.of("  "));
        assertEquals(Direction.UP, GraphAssembler.Direction.of("Up"));
        assertThrows(IllegalArgumentException.class, () -> GraphAssembler.Direction.of("upwards"));
    }

    /** 边 JSON 是 API 与前端唯一的字段约定，字段名在这里钉死 */
    @Test
    void edgeJsonCarriesTheAgreedFieldNames() {
        LineageGraph graph = graphOf("INSERT INTO dws.x SELECT s.v FROM "
                + "(SELECT a + 1 AS v FROM ods.src) s");
        List<ColumnLink> links = new ArrayList<>(graph.getColumnLinks());
        assertEquals(1, links.size());

        Map<String, Object> json = GraphAssembler.edgeJson(links.get(0));
        assertEquals("c:ods.src.a>dws.x.v#EXPRESSION", json.get("id"));
        assertEquals(links.get(0).getConfidence(), json.get("confidence"));
        assertEquals(links.get(0).getTransform(), json.get("transform"));
        assertEquals(links.get(0).getEngine(), json.get("engine"));
        assertEquals(links.get(0).getJobId(), json.get("jobId"));
    }
}
