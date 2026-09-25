package com.bigdata.lineage.web;

import com.bigdata.lineage.graph.ColumnLink;
import com.bigdata.lineage.graph.GraphNode;
import com.bigdata.lineage.graph.LineageGraph;
import com.bigdata.lineage.graph.TableLink;
import com.bigdata.lineage.parser.NameNormalizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 从整张图裁出前端能直接喂给 Cytoscape 的子图：按方向/深度裁剪，再序列化成节点+边 JSON。
 *
 * <p>为什么单开一层：{@link LineageGraph} 是为查询建的全量索引，一次语料扫描就有上千条字段边，
 * 全推给浏览器既画不开也没法看。裁剪只改"这次请求看见哪些端点"，不改边上的证据字段，
 * 所以证据（hops/derivation/jobId）必须在裁剪后仍然完整。
 *
 * <p>表级视图永远不含语句内关系（CTE、伪节点）：它们没有表级边，摆进 DAG 只会是一堆孤岛。
 */
public final class GraphAssembler {

    /** 深度上限：再深就不是"看链路"而是把整张图拉下来了 */
    public static final int MAX_DEPTH = 8;

    private GraphAssembler() {
    }

    /** 裁剪方向：数据流的上游（来源）、下游（去向）或两头都要 */
    public enum Direction {
        UP, DOWN, BOTH;

        public static Direction of(String raw) {
            if (raw == null || raw.trim().isEmpty()) {
                return BOTH;
            }
            String value = raw.trim().toLowerCase(Locale.ROOT);
            for (Direction direction : values()) {
                if (direction.name().toLowerCase(Locale.ROOT).equals(value)) {
                    return direction;
                }
            }
            throw new IllegalArgumentException("direction 只接受 up/down/both：" + raw);
        }
    }

    public static int clampDepth(String raw, int fallback) {
        if (raw == null || raw.trim().isEmpty()) {
            return fallback;
        }
        int depth;
        try {
            depth = Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("depth 必须是整数：" + raw);
        }
        if (depth < 1) {
            throw new IllegalArgumentException("depth 至少为 1");
        }
        return Math.min(depth, MAX_DEPTH);
    }

    /**
     * 表级子图。
     *
     * @param root 中心表，null 或空 = 全量物理表 DAG（此时 direction/depth 无意义）
     */
    public static Subgraph tableSubgraph(LineageGraph graph, String root, Direction direction,
                                        int depth) {
        Set<String> keep = new LinkedHashSet<>();
        if (root == null || root.trim().isEmpty()) {
            for (GraphNode node : graph.getNodes().values()) {
                if (!node.isLocal()) {
                    keep.add(node.getId());
                }
            }
        } else {
            String start = requireTable(graph, root);
            keep.add(start);
            bfsTables(graph, start, direction, depth, keep);
        }
        List<TableLink> links = new ArrayList<>();
        for (TableLink link : graph.getTableLinks()) {
            if (keep.contains(link.getFrom()) && keep.contains(link.getTo())) {
                links.add(link);
            }
        }
        return new Subgraph(graph, nodesOf(graph, keep), links, new ArrayList<ColumnLink>(), keep);
    }

    private static Set<String> bfsTables(LineageGraph graph, String start, Direction direction,
                                        int depth, Set<String> keep) {        Set<String> visited = new LinkedHashSet<>();
        visited.add(start);
        Deque<String> frontier = new ArrayDeque<>();
        frontier.add(start);
        for (int level = 0; level < depth && !frontier.isEmpty(); level++) {
            Deque<String> next = new ArrayDeque<>();
            for (String table = frontier.poll(); table != null; table = frontier.poll()) {
                if (direction != Direction.UP) {
                    for (TableLink link : graph.downstreamTables(table)) {
                        push(graph, link.getTo(), visited, next, keep);
                    }
                }
                if (direction != Direction.DOWN) {
                    for (TableLink link : graph.upstreamTables(table)) {
                        push(graph, link.getFrom(), visited, next, keep);
                    }
                }
            }
            frontier = next;
        }
        return keep;
    }

    private static void push(LineageGraph graph, String candidate, Set<String> visited,
                            Deque<String> next, Set<String> keep) {
        GraphNode node = graph.getNode(candidate);
        if (node == null || node.isLocal() || !visited.add(candidate)) {
            return;
        }
        next.add(candidate);
        keep.add(candidate);
    }

    /**
     * 字段级子图：从指定表的列出发，双向按深度展开。
     *
     * @param column 单列，null 或空 = 该表全部列
     */
    public static Subgraph columnSubgraph(LineageGraph graph, String table, String column,
                                          int depth) {
        String full = requireTable(graph, table);
        GraphNode node = graph.getNode(full);
        Map<String, String> seeds = new LinkedHashMap<>();
        if (column == null || column.trim().isEmpty()) {
            if (node.getColumns().isEmpty()) {
                throw new IllegalArgumentException("表 " + full + " 没有字段级血缘");
            }
            for (String owned : node.getColumns()) {
                seeds.put(ColumnLink.columnId(full, owned), full);
            }
        } else {
            String name = requireColumn(node, column);
            seeds.put(ColumnLink.columnId(full, name), full);
        }

        Map<String, String> columns = new LinkedHashMap<>(seeds);
        Set<String> visited = new LinkedHashSet<>(seeds.keySet());
        List<ColumnLink> links = new ArrayList<>();
        Set<ColumnLink> seen = new LinkedHashSet<>();
        Set<String> frontier = new LinkedHashSet<>(visited);
        for (int level = 0; level < depth && !frontier.isEmpty(); level++) {
            Set<String> next = new LinkedHashSet<>();
            for (String id : frontier) {
                for (ColumnLink link : graph.sourcesOf(id)) {
                    if (seen.add(link)) {
                        links.add(link);
                    }
                    columns.put(link.getFrom(), link.getFromTable());
                    columns.put(link.getTo(), link.getToTable());
                    next.add(link.getFrom());
                }
                for (ColumnLink link : graph.consumersOf(id)) {
                    if (seen.add(link)) {
                        links.add(link);
                    }
                    columns.put(link.getFrom(), link.getFromTable());
                    columns.put(link.getTo(), link.getToTable());
                    next.add(link.getTo());
                }
            }
            next.removeAll(visited);
            visited.addAll(next);
            frontier = next;
        }

        Set<String> tables = new LinkedHashSet<>();
        tables.add(full);
        tables.addAll(columns.values());
        return new Subgraph(graph, nodesOf(graph, tables), new ArrayList<TableLink>(),
                links, tables, columns);
    }

    /**
     * 字段标识：把用户给的表名/列名（大小写、短名都认）换算成图里登记的 {@code table.column}。
     *
     * <p>单独开一个入口是因为服务端布局要按"这一列"决定谁留在盒子里，而那必须先拿到图内口径的
     * 标识——自己拼字符串就会拼出两种大小写：图里一种，请求里一种，焦点永远对不上。
     */
    public static String columnId(LineageGraph graph, String table, String column) {
        String full = requireTable(graph, table);
        return ColumnLink.columnId(full, requireColumn(graph.getNode(full), column));
    }

    /** 列名大小写不敏感，但返回图里登记的写法：图内标识已归一化，不能把用户的大小写拼进 id */
    private static String requireColumn(GraphNode node, String column) {
        String name = column.trim();
        for (String owned : node.getColumns()) {
            if (owned.equalsIgnoreCase(name)) {
                return owned;
            }
        }
        throw new IllegalArgumentException("表 " + node.getId() + " 没有字段 " + name);
    }

    /** 表名寻址：全名/短名、大小写都认；同名冲突让调用方改传全名而不是猜一个 */
    public static String requireTable(LineageGraph graph, String raw) {
        String name = raw == null ? "" : raw.trim();
        if (graph.getNode(name) != null) {
            return name;
        }
        String dotted = NameNormalizer.normalizeQualified(name);
        if (graph.getNode(dotted) != null) {
            return dotted;
        }
        List<String> hits = new ArrayList<>();
        for (GraphNode node : graph.getNodes().values()) {
            if (node.getName().equalsIgnoreCase(dotted)) {
                hits.add(node.getId());
            }
        }
        if (hits.size() == 1) {
            return hits.get(0);
        }
        if (hits.isEmpty()) {
            throw new IllegalArgumentException("图里没有这张表：" + name);
        }
        throw new IllegalArgumentException("表名 " + name + " 不唯一，请用库前缀全名：" + hits);
    }

    private static List<GraphNode> nodesOf(LineageGraph graph, Set<String> ids) {
        List<GraphNode> nodes = new ArrayList<>();
        for (String id : ids) {
            GraphNode node = graph.getNode(id);
            if (node != null) {
                nodes.add(node);
            }
        }
        return nodes;
    }

    /**
     * 一次裁剪的结果。持有全量图只为了读 ranks 与 local 标记——图本身不可变，可以跨线程共享。
     */
    public static final class Subgraph {

        private final LineageGraph graph;
        private final List<GraphNode> nodes;
        private final List<TableLink> tableLinks;
        private final List<ColumnLink> columnLinks;
        private final Set<String> tables;
        /** 字段级视图的节点：列标识 → 所属关系标识，不用字符串切分是因为 CTE 名带语句前缀 */
        private final Map<String, String> columns;

        private Subgraph(LineageGraph graph, List<GraphNode> nodes, List<TableLink> tableLinks,
                         List<ColumnLink> columnLinks, Set<String> tables) {
            this(graph, nodes, tableLinks, columnLinks, tables,
                    Collections.<String, String>emptyMap());
        }

        private Subgraph(LineageGraph graph, List<GraphNode> nodes, List<TableLink> tableLinks,
                         List<ColumnLink> columnLinks, Set<String> tables,
                         Map<String, String> columns) {
            this.graph = graph;
            this.nodes = nodes;
            this.tableLinks = tableLinks;
            this.columnLinks = columnLinks;
            this.tables = tables;
            this.columns = columns;
        }

        public List<GraphNode> getNodes() {
            return nodes;
        }

        public List<TableLink> getTableLinks() {
            return tableLinks;
        }

        public List<ColumnLink> getColumnLinks() {
            return columnLinks;
        }

        /** Cytoscape 可直接消费的表级 DAG */
        public Map<String, Object> tableView() {
            List<Map<String, Object>> nodes = new ArrayList<>();
            for (GraphNode node : this.nodes) {
                Map<String, Object> json = new LinkedHashMap<>();
                json.put("id", node.getId());
                json.put("name", node.getName());
                json.put("db", node.getNamespace());
                json.put("layer", graph.rankOf(node.getId()));
                json.put("kind", "physical");
                json.put("colCount", node.getColumns().size());
                nodes.add(json);
            }
            List<Map<String, Object>> edges = new ArrayList<>();
            for (TableLink link : tableLinks) {
                Map<String, Object> json = new LinkedHashMap<>();
                json.put("id", "t:" + link.getFrom() + ">" + link.getTo());
                json.put("from", link.getFrom());
                json.put("to", link.getTo());
                json.put("jobId", link.getJobId());
                edges.add(json);
            }
            return view(nodes, edges);
        }

        /** Cytoscape 可直接消费的字段级 DAG：节点是 {@code table.column} */
        public Map<String, Object> columnView() {
            List<Map<String, Object>> nodes = new ArrayList<>();
            for (Map.Entry<String, String> entry : columns.entrySet()) {
                String id = entry.getKey();
                String owner = entry.getValue();
                GraphNode table = graph.getNode(owner);
                int dot = id.lastIndexOf('.');
                Map<String, Object> json = new LinkedHashMap<>();
                json.put("id", id);
                json.put("column", dot < 0 ? id : id.substring(dot + 1));
                json.put("table", owner);
                json.put("name", table == null ? owner : table.getName());
                json.put("local", table != null && table.isLocal());
                json.put("layer", graph.rankOf(owner));
                json.put("kind", "column");
                nodes.add(json);
            }
            List<Map<String, Object>> edges = new ArrayList<>();
            for (ColumnLink link : columnLinks) {
                edges.add(edgeJson(link));
            }
            return view(nodes, edges);
        }

        private Map<String, Object> view(List<Map<String, Object>> nodes,
                                        List<Map<String, Object>> edges) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("nodes", nodes);
            json.put("edges", edges);
            Map<String, Integer> ranks = new LinkedHashMap<>();
            for (String table : tables) {
                if (graph.getRanks().containsKey(table)) {
                    ranks.put(table, graph.rankOf(table));
                }
            }
            json.put("ranks", ranks);
            Set<String> cyclic = new LinkedHashSet<>(graph.getCyclicTables());
            cyclic.retainAll(tables);
            json.put("cyclic", cyclic);
            return json;
        }
    }

    /** 一条字段边的完整证据：裁剪层不做任何省略，前端点开侧栏就能看到来源链 */
    public static Map<String, Object> edgeJson(ColumnLink link) {
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("id", "c:" + link.getFrom() + ">" + link.getTo() + "#" + link.getDerivation());
        json.put("from", link.getFrom());
        json.put("to", link.getTo());
        json.put("derivation", link.getDerivation() == null
                ? null : link.getDerivation().name());
        json.put("confidence", link.getConfidence());
        json.put("transform", link.getTransform());
        json.put("hops", link.getHops());
        json.put("engine", link.getEngine());
        json.put("jobId", link.getJobId());
        json.put("ordinal", link.getOrdinal());
        return json;
    }
}
