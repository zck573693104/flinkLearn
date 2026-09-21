package com.bigdata.lineage.web.api;

import com.bigdata.lineage.graph.ColumnGraphBuilder;
import com.bigdata.lineage.graph.ColumnLink;
import com.bigdata.lineage.graph.GraphNode;
import com.bigdata.lineage.graph.LineageGraph;
import com.bigdata.lineage.graph.LineageStore;
import com.bigdata.lineage.graph.ScanReport;
import com.bigdata.lineage.parser.MultiEngineSQLLineageParser;
import com.bigdata.lineage.parser.model.ColumnDerivation;
import com.bigdata.lineage.parser.model.TableLineage;
import com.bigdata.lineage.web.CorpusScanner;
import com.bigdata.lineage.web.GraphAssembler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 血缘查询端点：全部只读，唯一的写操作是 {@code POST /api/scan} 换快照。
 *
 * <p>每个方法都在同一份 {@link LineageStore.Snapshot} 上取数（方法开头拿一次引用），
 * 这样一次请求内不会出现"图是新的、账本是旧的"。
 */
@RestController
@RequestMapping("/api")
public class LineageApiController {

    /** 试解析用独立实例：不能污染语料扫描的解析结果，也不带缓存 */
    private final MultiEngineSQLLineageParser probeParser = new MultiEngineSQLLineageParser(false);

    private final LineageStore store;
    private final CorpusScanner scanner;

    public LineageApiController(LineageStore store, CorpusScanner scanner) {
        this.store = store;
        this.scanner = scanner;
    }

    /** 概览：语料规模、图规模、质量问题数 */
    @GetMapping("/overview")
    public ApiResponse overview() {
        LineageStore.Snapshot snapshot = store.current();
        LineageGraph graph = snapshot.getGraph();
        ScanReport report = snapshot.getReport();
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("source", snapshot.getSource());
        data.put("durationMillis", snapshot.getDurationMillis());
        data.put("fileCount", report.getFileCount());
        data.put("statementCount", report.getStatementCount());
        data.put("parseErrorCount", report.getParseErrorCount());
        data.put("unresolvedCount", report.getUnresolvedCount());
        data.put("starCount", report.getStarCount());
        data.put("tableCount", countPhysical(graph));
        data.put("tableCountWithColumns", graph.tableCountWithColumns());
        data.put("localRelationCount", countLocal(graph));
        data.put("columnEdgeCount", graph.getColumnLinks().size());
        data.put("tableEdgeCount", graph.getTableLinks().size());
        data.put("maxLayer", maxRank(graph));
        data.put("cyclicTables", graph.getCyclicTables());
        return ApiResponse.ok(data);
    }

    /** 表清单：支持子串搜索、按层过滤、只看有质量问题的表 */
    @GetMapping("/tables")
    public ApiResponse tables(@RequestParam(required = false) String q,
                              @RequestParam(required = false) Integer layer,
                              @RequestParam(required = false) boolean onlyUnresolved) {
        LineageStore.Snapshot snapshot = store.current();
        LineageGraph graph = snapshot.getGraph();
        Set<String> dirty = dirtyTables(graph, snapshot.getReport());
        List<Map<String, Object>> rows = new ArrayList<>();
        String needle = q == null ? "" : q.trim().toLowerCase(Locale.ROOT);
        for (GraphNode node : graph.getNodes().values()) {
            if (node.isLocal() || !matches(node, needle) || !layerOf(layer, graph, node)) {
                continue;
            }
            if (onlyUnresolved && !dirty.contains(node.getId())) {
                continue;
            }
            rows.add(row(graph, node));
        }
        rows.sort((left, right) -> Integer.compare(
                (Integer) left.get("layer"), (Integer) right.get("layer")));
        return ApiResponse.ok(rows);
    }

    /** 表级分层 DAG；不传 root 就是全量 */
    @GetMapping("/graph/table")
    public ApiResponse tableGraph(@RequestParam(required = false) String root,
                                  @RequestParam(required = false) String direction,
                                  @RequestParam(required = false) String depth) {
        LineageGraph graph = store.current().getGraph();
        GraphAssembler.Subgraph subgraph = GraphAssembler.tableSubgraph(graph, root,
                GraphAssembler.Direction.of(direction),
                GraphAssembler.clampDepth(depth, 2));
        return ApiResponse.ok(subgraph.tableView());
    }

    /** 一张表的字段清单 + 每个字段的直接来源与去向 */
    @GetMapping("/table/{table:.+}/columns")
    public ApiResponse tableColumns(@PathVariable String table) {
        LineageStore.Snapshot snapshot = store.current();
        LineageGraph graph = snapshot.getGraph();
        String full = GraphAssembler.requireTable(graph, table);
        GraphNode node = graph.getNode(full);
        List<Map<String, Object>> columns = new ArrayList<>();
        for (String column : node.getColumns()) {
            String id = ColumnLink.columnId(full, column);
            List<ColumnLink> sources = graph.sourcesOf(id);
            List<ColumnLink> targets = graph.consumersOf(id);
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("column", column);
            json.put("id", id);
            json.put("sourceCount", sources.size());
            json.put("consumerCount", targets.size());
            json.put("unresolvedSource", snapshot.getReport().hasUnresolvedSource(id));
            json.put("sources", evidence(sources, snapshot));
            json.put("consumers", ids(targets, true));
            columns.add(json);
        }
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("table", full);
        data.put("name", node.getName());
        data.put("db", node.getNamespace());
        data.put("layer", graph.rankOf(full));
        data.put("local", node.isLocal());
        data.put("columns", columns);
        return ApiResponse.ok(data);
    }

    /** 字段级链路子图；不传 column 就是整张表的所有字段 */
    @GetMapping("/graph/column")
    public ApiResponse columnGraph(@RequestParam String table,
                                   @RequestParam(required = false) String column,
                                   @RequestParam(required = false) String depth) {
        LineageGraph graph = store.current().getGraph();
        GraphAssembler.Subgraph subgraph = GraphAssembler.columnSubgraph(graph, table, column,
                GraphAssembler.clampDepth(depth, 3));
        return ApiResponse.ok(subgraph.columnView());
    }

    /** 单条字段边的全部证据：加工方式、中间跳、哪份文件哪条语句、原文 */
    @GetMapping("/edge/column")
    public ApiResponse edgeEvidence(@RequestParam String from, @RequestParam String to) {
        LineageStore.Snapshot snapshot = store.current();
        List<ColumnLink> hits = new ArrayList<>();
        for (ColumnLink link : snapshot.getGraph().getColumnLinks()) {
            if (from.equals(link.getFrom()) && to.equals(link.getTo())) {
                hits.add(link);
            }
        }
        if (hits.isEmpty()) {
            return ApiResponse.fail("图里没有这条边：" + from + " -> " + to);
        }
        return ApiResponse.ok(evidence(hits, snapshot));
    }

    /** 质量视图：解析失败、绑定失败、星号、没出血缘的文件、没有任何边的孤儿表 */
    @GetMapping("/issues")
    public ApiResponse issues(@RequestParam(required = false) String type) {
        LineageStore.Snapshot snapshot = store.current();
        ScanReport report = snapshot.getReport();
        String kind = type == null ? "all" : type.trim().toLowerCase(Locale.ROOT);
        Map<String, Object> data = new LinkedHashMap<>();
        if (accepts(kind, "parseerror")) {
            data.put("parseErrors", report.getParseErrorSql());
        }
        if (accepts(kind, "unresolved")) {
            data.put("unresolvedEdges", report.getUnresolvedColumns());
        }
        if (accepts(kind, "star")) {
            data.put("starEdges", report.getStarColumns());
        }
        if (accepts(kind, "nolineage")) {
            data.put("filesWithoutLineage", report.getFilesWithoutLineage());
        }
        if (accepts(kind, "orphan")) {
            data.put("orphanTables", orphanTables(snapshot.getGraph()));
        }
        return ApiResponse.ok(data);
    }

    /** 贴 SQL 试解析：只读不落库，返回逐语句结果与折叠后的边 */
    @PostMapping("/parse")
    public ApiResponse parse(@RequestBody Map<String, String> body) {
        String sql = body == null ? null : body.get("sql");
        if (sql == null || sql.trim().isEmpty()) {
            return ApiResponse.fail("sql 不能为空");
        }
        List<TableLineage> statements = probeParser.extractTableLineages(sql, false);
        LineageGraph graph = ColumnGraphBuilder.build(statements);
        List<Map<String, Object>> edges = new ArrayList<>();
        for (ColumnLink link : graph.getColumnLinks()) {
            edges.add(GraphAssembler.edgeJson(link));
        }
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("statementCount", statements.size());
        data.put("statements", statements);
        data.put("graph", GraphAssembler.tableSubgraph(graph, null,
                GraphAssembler.Direction.BOTH, 1).tableView());
        data.put("columnEdges", edges);
        return ApiResponse.ok(data);
    }

    /** 重扫目录并原子换快照 */
    @PostMapping("/scan")
    public ApiResponse rescan(@RequestBody(required = false) Map<String, String> body)
            throws IOException {
        String dir = body == null ? null : body.get("dir");
        LineageStore.Snapshot snapshot = scanner.rescan(dir);
        ScanReport report = snapshot.getReport();
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("source", snapshot.getSource());
        data.put("durationMillis", snapshot.getDurationMillis());
        data.put("fileCount", report.getFileCount());
        data.put("statementCount", report.getStatementCount());
        data.put("parseErrorCount", report.getParseErrorCount());
        data.put("tableCount", countPhysical(snapshot.getGraph()));
        data.put("columnEdgeCount", snapshot.getGraph().getColumnLinks().size());
        return ApiResponse.ok(data);
    }

    private static boolean matches(GraphNode node, String needle) {
        return needle.isEmpty() || node.getId().toLowerCase(Locale.ROOT).contains(needle)
                || node.getName().toLowerCase(Locale.ROOT).contains(needle);
    }

    private static boolean layerOf(Integer layer, LineageGraph graph, GraphNode node) {
        return layer == null || graph.rankOf(node.getId()) == layer;
    }

    private static Map<String, Object> row(LineageGraph graph, GraphNode node) {
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("id", node.getId());
        json.put("name", node.getName());
        json.put("db", node.getNamespace());
        json.put("layer", graph.rankOf(node.getId()));
        json.put("colCount", node.getColumns().size());
        json.put("upstream", graph.upstreamTables(node.getId()).size());
        json.put("downstream", graph.downstreamTables(node.getId()).size());
        return json;
    }

    /** 边列表 → 证据 JSON；带上下文的入口（字段清单、单条边）才能补 SQL 原文与可信度提示 */
    private static List<Map<String, Object>> evidence(List<ColumnLink> links,
                                                     LineageStore.Snapshot snapshot) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (ColumnLink link : links) {
            Map<String, Object> json = GraphAssembler.edgeJson(link);
            json.put("sqlText", snapshot.sqlOf(link.getJobId()));
            json.put("parseError", snapshot.isParseError(link.getJobId()));
            rows.add(json);
        }
        return rows;
    }

    private static List<String> ids(List<ColumnLink> links, boolean fromSide) {
        Set<String> ids = new LinkedHashSet<>();
        for (ColumnLink link : links) {
            ids.add(fromSide ? link.getTo() : link.getFrom());
        }
        return new ArrayList<>(ids);
    }

    /** 有 UNRESOLVED / STAR 边的关系：质量视图的"哪些表需要人工确认" */
    private static Set<String> dirtyTables(LineageGraph graph, ScanReport report) {
        Set<String> dirty = new LinkedHashSet<>();
        for (ColumnLink link : graph.getColumnLinks()) {
            ColumnDerivation derivation = link.getDerivation();
            if (derivation == ColumnDerivation.UNRESOLVED || derivation == ColumnDerivation.STAR) {
                dirty.add(link.getToTable());
                dirty.add(link.getFromTable());
            }
        }
        // 来源没绑上的边根本进不了图，只在账本里留了个字段标识
        for (String columnId : report.getUnresolvedTargets()) {
            dirty.add(tableOf(columnId));
        }
        return dirty;
    }

    /** 字段标识 {@code table.column} 的表前缀：末段才是列名，所以只在最后一个点处切 */
    private static String tableOf(String columnId) {
        int dot = columnId.lastIndexOf('.');
        return dot < 0 ? columnId : columnId.substring(0, dot);
    }

    /** 既没有上游也没有下游的物理表：多半是漏解析或语料本身只提到一次 */
    private static List<String> orphanTables(LineageGraph graph) {
        List<String> orphans = new ArrayList<>();
        for (GraphNode node : graph.getNodes().values()) {
            if (node.isLocal()) {
                continue;
            }
            if (graph.upstreamTables(node.getId()).isEmpty()
                    && graph.downstreamTables(node.getId()).isEmpty()) {
                orphans.add(node.getId());
            }
        }
        return orphans;
    }

    private static int countPhysical(LineageGraph graph) {
        int count = 0;
        for (GraphNode node : graph.getNodes().values()) {
            if (!node.isLocal()) {
                count++;
            }
        }
        return count;
    }

    private static int countLocal(LineageGraph graph) {
        int count = 0;
        for (GraphNode node : graph.getNodes().values()) {
            if (node.isLocal()) {
                count++;
            }
        }
        return count;
    }

    private static int maxRank(LineageGraph graph) {
        int max = -1;
        for (Integer rank : graph.getRanks().values()) {
            max = Math.max(max, rank);
        }
        return max;
    }

    private static boolean accepts(String kind, String name) {
        return kind.isEmpty() || "all".equals(kind) || name.equals(kind);
    }
}
