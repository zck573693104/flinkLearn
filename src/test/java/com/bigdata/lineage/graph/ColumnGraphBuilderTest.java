package com.bigdata.lineage.graph;

import com.bigdata.lineage.parser.MultiEngineSQLLineageParser;
import com.bigdata.lineage.parser.model.ColumnDerivation;
import com.bigdata.lineage.parser.model.TableLineage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 图构建层的形状断言：从真解析器拿语句，检查伪节点折干净了、语句命名空间隔开了、分层算对了。
 *
 * <p>不在这里重复校验解析层的绑定口径（那是各方言 ColumnLineageTest 的事），
 * 这里只管"边接成图之后用户看得见什么"。
 */
class ColumnGraphBuilderTest {

    private static final MultiEngineSQLLineageParser PARSER = new MultiEngineSQLLineageParser();

    /** 每次建图都过一遍伪节点体检：图上出现 {@code #sub} 就说明有一跳没折干净 */
    private static LineageGraph graphOf(String sql) {
        LineageGraph graph = ColumnGraphBuilder.build(PARSER.extractTableLineages(sql));
        for (GraphNode node : graph.getNodes().values()) {
            assertFalse(node.getId().contains("#"),
                    "伪节点泄漏到图上：" + node.getId() + " <- " + sql);
        }
        return graph;
    }

    private static List<TableLineage> statementsOf(String sql) {
        return PARSER.extractTableLineages(sql, false);
    }

    private static ColumnLink onlySource(LineageGraph graph, String columnId) {
        List<ColumnLink> sources = graph.sourcesOf(columnId);
        assertEquals(1, sources.size(), columnId + " 应只有一条来源边，实际 " + sources);
        return sources.get(0);
    }

    @Test
    void subqueryNodeFoldsIntoOneHopButKeepsTheChainAsEvidence() {
        LineageGraph graph = graphOf("INSERT INTO dws.x SELECT s.v FROM "
                + "(SELECT a + 1 AS v FROM ods.src) s");
        assertEquals(new ArrayList<>(Arrays.asList("dws.x", "ods.src")),
                new ArrayList<>(graph.getNodes().keySet()));

        ColumnLink link = onlySource(graph, "dws.x.v");
        assertEquals("ods.src.a", link.getFrom());
        assertEquals(ColumnDerivation.EXPRESSION, link.getDerivation());
        assertEquals(3, link.getHops().size(), "证据里要留被折掉的中间列");
        // 带别名的子查询以别名登记，折叠后它只剩这一跳证据：图上没有 s 这个节点
        assertTrue(link.getHops().get(1).endsWith("]s.v"), link.getHops().toString());
    }

    /** 折过聚合之后的直通列不能再当 IDENTITY 看：置信度取整条链最低的那一档 */
    @Test
    void foldedHopInheritsTheWeakestDerivationOnTheChain() {
        LineageGraph graph = graphOf("INSERT INTO dws.x SELECT s.n FROM "
                + "(SELECT count(id) AS n FROM ods.src) s");
        ColumnLink link = onlySource(graph, "dws.x.n");
        assertEquals("ods.src.id", link.getFrom());
        assertEquals(ColumnDerivation.AGGREGATE, link.getDerivation(),
                "外层是 IDENTITY、内层是 AGGREGATE，整条链只能是 AGGREGATE");
    }

    @Test
    void aliasIsDereferencedToThePhysicalTable() {
        LineageGraph graph = graphOf("INSERT INTO dwd.t SELECT s.id FROM ods.src AS s");
        assertEquals("ods.src.id", onlySource(graph, "dwd.t.id").getFrom());
        assertEquals(2, graph.getNodes().size());
    }

    @Test
    void cteStaysANodeButOnlyInsideItsOwnStatement() {
        LineageGraph graph = graphOf("INSERT INTO dws.one "
                + "WITH c AS (SELECT id FROM ods.a) SELECT id FROM c; "
                + "INSERT INTO dws.two "
                + "WITH c AS (SELECT id FROM ods.b) SELECT id FROM c");

        List<GraphNode> locals = new ArrayList<>();
        for (GraphNode node : graph.getNodes().values()) {
            if (node.isLocal()) {
                locals.add(node);
            }
        }
        assertEquals(2, locals.size(), "两个同名 CTE 必须是两个节点：" + graph.getNodes().keySet());
        for (GraphNode cte : locals) {
            assertEquals("c", cte.getName());
            assertEquals(Arrays.asList("id"), cte.getColumns());
            assertNotNull(cte.getNamespace(), "语句标识要留给 UI 定位");
        }
        // 各自的上下游各归各：ods.a 只喂第一条，ods.b 只喂第二条
        assertEquals(1, graph.consumersOf("ods.a.id").size());
        assertEquals(1, graph.consumersOf("ods.b.id").size());
        assertTrue(graph.sourcesOf("dws.one.id").get(0).getFrom().startsWith("["));
        assertFalse(graph.sourcesOf("dws.one.id").get(0).getFrom().contains("ods.b"));
    }

    @Test
    void tableRanksComeOutAcrossStatements() {
        LineageGraph graph = graphOf("INSERT INTO dwd.a SELECT id FROM ods.src; "
                + "INSERT INTO dws.b SELECT id FROM dwd.a; "
                + "INSERT INTO ads.c SELECT id FROM dws.b");
        assertEquals(0, graph.rankOf("ods.src"));
        assertEquals(1, graph.rankOf("dwd.a"));
        assertEquals(2, graph.rankOf("dws.b"));
        assertEquals(3, graph.rankOf("ads.c"));
        assertTrue(graph.getCyclicTables().isEmpty());
        assertEquals(4, graph.tableCountWithColumns());
    }

    /** 增量表自依赖：环要标出来，两端同层，边仍然成环 */
    @Test
    void selfDependencyBecomesACyclicTableNotAMissingEdge() {
        LineageGraph graph = graphOf("INSERT INTO dwd.t SELECT id FROM dwd.t");
        assertTrue(graph.getCyclicTables().contains("dwd.t"));
        assertEquals(0, graph.rankOf("dwd.t"));
        assertEquals("dwd.t.id", onlySource(graph, "dwd.t.id").getFrom());
        assertEquals(1, graph.downstreamTables("dwd.t").size());
    }

    @Test
    void constantColumnsGetANodeButNoSourceEdge() {
        LineageGraph graph = graphOf("INSERT INTO dwd.t SELECT 1 AS flag FROM ods.src");
        assertTrue(graph.sourcesOf("dwd.t.flag").isEmpty(), "常量列不该被编出来源");
        assertEquals(0, graph.rankOf("ods.src"));
    }

    @Test
    void unqualifiedColumnResolvesThroughTheGraphToo() {
        LineageGraph graph = graphOf("INSERT INTO dwd.t SELECT id FROM ods.src");
        ColumnLink link = onlySource(graph, "dwd.t.id");
        assertEquals("ods.src.id", link.getFrom());
        assertEquals(ColumnDerivation.IDENTITY, link.getDerivation());
        assertEquals(Arrays.asList(link.getFrom(), "dwd.t.id"), link.getHops());
    }

    @Test
    void emptyOrNullInputYieldsEmptyGraph() {
        assertTrue(ColumnGraphBuilder.build(null).getNodes().isEmpty());
        assertTrue(ColumnGraphBuilder.build(new ArrayList<TableLineage>()).getNodes().isEmpty());
        assertTrue(LineageGraph.empty().sourcesOf("a.b").isEmpty());
    }

    @Test
    void graphIndexesAreReadableBothWays() {
        LineageGraph graph = graphOf("INSERT INTO dwd.t SELECT id FROM ods.src");
        List<ColumnLink> downstream = graph.consumersOf("ods.src.id");
        assertEquals(1, downstream.size());
        assertEquals("dwd.t.id", downstream.get(0).getTo());
        assertEquals(1, graph.upstreamTables("dwd.t").size());
        assertEquals("ods.src", graph.upstreamTables("dwd.t").get(0).getFrom());
        assertTrue(graph.downstreamTables("ods.src").size() > 0);
        assertEquals("t", graph.getNode("dwd.t").getName(), "getName 是末段短名，给 UI 当标题");
        assertEquals("dwd", graph.getNode("dwd.t").getNamespace());
        assertTrue(graph.getNode("dwd.t").isPhysical());
        assertFalse(graph.getNode("dwd.t").isLocal());
    }

    /** 每条边都带着语句标识与方言标记，UI 才能回答"这条线是哪条作业产出的" */
    @Test
    void linksCarryStatementAndEngineEvidence() {
        LineageGraph graph = graphOf("INSERT INTO dwd.t SELECT id FROM ods.src");
        ColumnLink link = onlySource(graph, "dwd.t.id");
        assertNotNull(link.getJobId());
        assertFalse(link.getJobId().isEmpty());
        assertNotNull(link.getEngine());
        assertEquals(0, link.getOrdinal());
    }
}
