package com.bigdata.lineage.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 一次扫描的完整血缘图：节点、字段级边、表级边与分层，全部不可变。
 *
 * <p>索引在建图时一次算好，读侧（REST）拿到的是同一份对象，不需要加锁也不需要防并发修改。
 */
public final class LineageGraph {

    private static final List<ColumnLink> NO_LINKS = Collections.emptyList();
    private static final List<TableLink> NO_TABLE_LINKS = Collections.emptyList();

    private final Map<String, GraphNode> nodes;
    private final List<ColumnLink> columnLinks;
    private final List<TableLink> tableLinks;
    private final Map<String, Integer> ranks;
    private final Set<String> cyclicTables;

    private final Map<String, List<ColumnLink>> columnIn = new LinkedHashMap<>();
    private final Map<String, List<ColumnLink>> columnOut = new LinkedHashMap<>();
    private final Map<String, List<TableLink>> tableIn = new LinkedHashMap<>();
    private final Map<String, List<TableLink>> tableOut = new LinkedHashMap<>();

    LineageGraph(Map<String, GraphNode> nodes, List<ColumnLink> columnLinks,
                 List<TableLink> tableLinks, Map<String, Integer> ranks, Set<String> cyclicTables) {
        this.nodes = Collections.unmodifiableMap(new LinkedHashMap<>(nodes));
        this.columnLinks = Collections.unmodifiableList(new ArrayList<>(columnLinks));
        this.tableLinks = Collections.unmodifiableList(new ArrayList<>(tableLinks));
        this.ranks = Collections.unmodifiableMap(new LinkedHashMap<>(ranks));
        this.cyclicTables = Collections.unmodifiableSet(new LinkedHashSet<>(cyclicTables));
        index();
    }

    public static LineageGraph empty() {
        return new LineageGraph(Collections.<String, GraphNode>emptyMap(),
                Collections.<ColumnLink>emptyList(), Collections.<TableLink>emptyList(),
                Collections.<String, Integer>emptyMap(), Collections.<String>emptySet());
    }

    private void index() {
        for (ColumnLink link : columnLinks) {
            add(columnIn, link.getTo(), link);
            add(columnOut, link.getFrom(), link);
        }
        for (TableLink link : tableLinks) {
            add(tableIn, link.getTo(), link);
            add(tableOut, link.getFrom(), link);
        }
    }

    private static <T> void add(Map<String, List<T>> index, String key, T value) {
        if (key == null || key.isEmpty()) {
            return;
        }
        List<T> bucket = index.get(key);
        if (bucket == null) {
            bucket = new ArrayList<>();
            index.put(key, bucket);
        }
        bucket.add(value);
    }

    public Map<String, GraphNode> getNodes() {
        return nodes;
    }

    public GraphNode getNode(String id) {
        return id == null ? null : nodes.get(id);
    }

    public List<ColumnLink> getColumnLinks() {
        return columnLinks;
    }

    public List<TableLink> getTableLinks() {
        return tableLinks;
    }

    /** 表级分层：源头表为 0 层，环上的节点同层 */
    public Map<String, Integer> getRanks() {
        return ranks;
    }

    public int rankOf(String table) {
        Integer hit = ranks.get(table);
        return hit == null ? 0 : hit;
    }

    public Set<String> getCyclicTables() {
        return cyclicTables;
    }

    /** 该列的上游边 */
    public List<ColumnLink> sourcesOf(String columnId) {
        List<ColumnLink> hit = columnIn.get(columnId);
        return hit == null ? NO_LINKS : Collections.unmodifiableList(hit);
    }

    /** 该列的下游边 */
    public List<ColumnLink> consumersOf(String columnId) {
        List<ColumnLink> hit = columnOut.get(columnId);
        return hit == null ? NO_LINKS : Collections.unmodifiableList(hit);
    }

    public List<TableLink> upstreamTables(String table) {
        List<TableLink> hit = tableIn.get(table);
        return hit == null ? NO_TABLE_LINKS : Collections.unmodifiableList(hit);
    }

    public List<TableLink> downstreamTables(String table) {
        List<TableLink> hit = tableOut.get(table);
        return hit == null ? NO_TABLE_LINKS : Collections.unmodifiableList(hit);
    }

    /**
     * 有字段级边的物理表数：UI 用它区分"能展字段"与"只有表级线"。
     *
     * <p>只数物理表，与 {@code tableCount} 同口径。语句内派生关系几乎必带列，
     * 把它们算进来会得出"有字段的表比表还多"。
     */
    public int tableCountWithColumns() {
        Set<String> withColumns = new LinkedHashSet<>();
        for (GraphNode node : nodes.values()) {
            if (node.isPhysical() && !node.getColumns().isEmpty()) {
                withColumns.add(node.getId());
            }
        }
        return withColumns.size();
    }

    public int size() {
        return nodes.size();
    }
}
