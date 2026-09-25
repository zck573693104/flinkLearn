package com.bigdata.lineage.web.sqlflow;

import com.bigdata.lineage.graph.ColumnLink;
import com.bigdata.lineage.graph.GraphNode;
import com.bigdata.lineage.graph.LineageGraph;
import com.bigdata.lineage.parser.model.ColumnDerivation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 把折叠过的字段边摊回成一站一站：中间跳（子查询/UNNEST/CTE）与聚合函数显形成盒。
 *
 * <p>为什么在 Web 层摊、不在建图时留着：{@code ColumnGraphBuilder} 折掉伪节点，是为了让图里每个
 * 节点都是用户认识的表；这套显示方式恰恰要求把中间站摆回链路上。两个诉求都成立，所以折叠保持
 * 原样、展开只发生在装配这一次请求里，边上的 {@code hops} 就是展开依据。
 *
 * <p>聚合函数盒是显示层造的：解析给的是"若干来源列 → 目标列"这一跳，中间没有函数节点。
 * 造一个出来不等于新增血缘事实——盒上写的表达式就是边上那条 {@code transform}。
 */
final class SqlFlowChain {

    /** 盒子里的一行：一个字段，或聚合函数盒那一行"结果列" */
    static final class Row {

        private final String columnId;
        private final String column;

        private Row(String columnId, String column) {
            this.columnId = columnId;
            this.column = column;
        }

        String getColumnId() {
            return columnId;
        }

        String getColumn() {
            return column;
        }
    }

    /** 画布上的一个盒子：一张物理表、一个语句内关系，或一个合成出来的函数 */
    static final class Box {

        private final String relation;
        private final String name;
        private final String type;
        private final boolean local;
        private final Map<String, Row> rows = new LinkedHashMap<String, Row>();

        private Box(String relation, String name, String type, boolean local) {
            this.relation = relation;
            this.name = name;
            this.type = type;
            this.local = local;
        }

        String getRelation() {
            return relation;
        }

        String getName() {
            return name;
        }

        /** SQLFlow 的类型口径：table / view / cte / select_list / unnest / lateral / function */
        String getType() {
            return type;
        }

        boolean isLocal() {
            return local;
        }

        Map<String, Row> getRows() {
            return rows;
        }

        private void addRow(String columnId, String column) {
            if (!rows.containsKey(columnId)) {
                rows.put(columnId, new Row(columnId, column));
            }
        }
    }

    /** 展开后的一段：两端都是画布上真实存在的字段行 */
    static final class Segment {

        private final ColumnLink link;
        private final String from;
        private final String to;
        private final int step;
        private final int steps;
        private final boolean synthetic;

        private Segment(ColumnLink link, String from, String to, int step, int steps,
                        boolean synthetic) {
            this.link = link;
            this.from = from;
            this.to = to;
            this.step = step;
            this.steps = steps;
            this.synthetic = synthetic;
        }

        ColumnLink getLink() {
            return link;
        }

        String getFrom() {
            return from;
        }

        String getTo() {
            return to;
        }

        int getStep() {
            return step;
        }

        int getSteps() {
            return steps;
        }

        /** 这段不是解析出来的边，是显示层为聚合函数插的一跳 */
        boolean isSynthetic() {
            return synthetic;
        }
    }

    private final Map<String, Box> boxes = new LinkedHashMap<String, Box>();
    private final List<Segment> segments = new ArrayList<Segment>();

    private SqlFlowChain() {
    }

    /**
     * @param views 产出时被 {@code CREATE VIEW} 写过的物理表，由装配层从语句原文判出来
     */
    static SqlFlowChain expand(LineageGraph graph, Collection<ColumnLink> links,
                               Set<String> views) {
        SqlFlowChain chain = new SqlFlowChain();
        for (ColumnLink link : links) {
            chain.walk(graph, link, views);
        }
        return chain;
    }

    Map<String, Box> getBoxes() {
        return boxes;
    }

    List<Segment> getSegments() {
        return segments;
    }

    private void walk(LineageGraph graph, ColumnLink link, Set<String> views) {
        String function = functionName(link);
        String station = function == null ? null : functionRelation(link.getJobId(), function);
        List<String> path = pathOf(link);
        String target = path.get(path.size() - 1);
        if (station != null) {
            // 聚合：来源列先进函数盒，再由函数盒写到目标列，链路多一站、表达式有了落点
            box(station, function, "function", true).addRow(station + "." + columnName(target),
                    columnName(target));
            path.add(path.size() - 1, station + "." + columnName(target));
        }
        for (String columnId : path) {
            register(graph, columnId, views);
        }
        for (int i = 0; i + 1 < path.size(); i++) {
            String from = path.get(i);
            String to = path.get(i + 1);
            boolean synthetic = station != null && (relationOf(from).equals(station)
                    || relationOf(to).equals(station));
            segments.add(new Segment(link, from, to, i + 1, path.size() - 1, synthetic));
        }
    }

    /**
     * 一跳的完整路径：{@code hops} 是 [源, 中间..., 目标]，端点对不上就不猜
     * （UNRESOLVED 边本来就带不出可信路径），保持两端直连。
     */
    private static List<String> pathOf(ColumnLink link) {
        List<String> hops = link.getHops();
        String from = link.getFrom();
        String to = link.getTo();
        List<String> path = new ArrayList<>();
        if (hops != null && hops.size() >= 2 && from.equals(hops.get(0))
                && to.equals(hops.get(hops.size() - 1))) {
            path.addAll(hops);
            return path;
        }
        path.add(from);
        path.add(to);
        return path;
    }

    /** 只有聚合才立函数盒：表达式的第一个"名字(" 就是外层函数，取不到就算了，不硬造 */
    private static String functionName(ColumnLink link) {
        if (link.getDerivation() != ColumnDerivation.AGGREGATE) {
            return null;
        }
        String transform = link.getTransform();
        if (transform == null) {
            return null;
        }
        int i = 0;
        while (i < transform.length() && Character.isWhitespace(transform.charAt(i))) {
            i++;
        }
        int start = i;
        while (i < transform.length() && (Character.isLetterOrDigit(transform.charAt(i))
                || transform.charAt(i) == '_' || transform.charAt(i) == '$')) {
            i++;
        }
        if (i == start) {
            return null;
        }
        String name = transform.substring(start, i);
        if (Character.isDigit(name.charAt(0))) {
            return null;
        }
        return name.toUpperCase(Locale.ROOT);
    }

    /**
     * 函数盒的标识：语句内唯一，同一条语句里的多个聚合共用一个盒。
     *
     * <p>不带目标列是因为盒子里本来就一行一个聚合列；带点号的名字会让"末段是列名"这个
     * 全图通用的切分口径出错，所以列名拼在标识之外。
     */
    private static String functionRelation(String jobId, String function) {
        return "[" + (jobId == null ? "" : jobId) + "]#" + function;
    }

    private void register(LineageGraph graph, String columnId, Set<String> views) {
        String relation = relationOf(columnId);
        Box box = boxes.get(relation);
        if (box == null) {
            box = box(relation, displayName(graph, relation), typeOf(graph, relation, views),
                    graph.getNode(relation) == null || graph.getNode(relation).isLocal());
        }
        box.addRow(columnId, columnName(columnId));
    }

    private Box box(String relation, String name, String type, boolean local) {
        Box box = boxes.get(relation);
        if (box == null) {
            box = new Box(relation, name, type, local);
            boxes.put(relation, box);
        }
        return box;
    }

    private static String typeOf(LineageGraph graph, String relation, Set<String> views) {
        GraphNode node = graph.getNode(relation);
        if (node == null || node.isLocal()) {
            return kindOf(node == null ? relation : node.getName());
        }
        return views.contains(relation) ? "view" : "table";
    }

    /** 伪节点的名字里带着它是哪种中间产物：{@code #sub1} 是折不动的子查询投影 */
    private static String kindOf(String bareName) {
        String name = bareName == null ? "" : bareName;
        int hash = name.lastIndexOf("#");
        String bare = hash < 0 ? name : name.substring(hash);
        String lowered = bare.toLowerCase(Locale.ROOT);
        if (lowered.startsWith("#unnest")) {
            return "unnest";
        }
        if (lowered.startsWith("#lat")) {
            return "lateral";
        }
        if (lowered.startsWith("#sub")) {
            return "select_list";
        }
        return name.indexOf('#') < 0 ? "cte" : "select_list";
    }

    private static String displayName(LineageGraph graph, String relation) {
        GraphNode node = graph.getNode(relation);
        if (node != null) {
            String name = node.getName();
            int hash = name.lastIndexOf('#');
            return hash < 0 ? name : name.substring(hash + 1);
        }
        int dot = relation.lastIndexOf('.');
        String bare = dot < 0 ? relation : relation.substring(dot + 1);
        int bracket = bare.lastIndexOf(']');
        String stripped = bracket < 0 ? bare : bare.substring(bracket + 1);
        int hash = stripped.indexOf('#');
        return hash < 0 ? stripped : stripped.substring(hash + 1);
    }

    /** 关系标识：字段标识的表前缀。末段才是列名，所以只在最后一个点处切 */
    static String relationOf(String columnId) {
        int dot = columnId == null ? -1 : columnId.lastIndexOf('.');
        return dot < 0 ? "" : columnId.substring(0, dot);
    }

    static String columnName(String columnId) {
        int dot = columnId == null ? -1 : columnId.lastIndexOf('.');
        return dot < 0 ? String.valueOf(columnId) : columnId.substring(dot + 1);
    }

    /** 盒级邻接：布局排的是盒子，段落在盒子上就是"谁的下游" */
    Map<String, Set<String>> boxSuccessors() {
        Map<String, Set<String>> successors = new LinkedHashMap<String, Set<String>>();
        for (Segment segment : segments) {
            String from = relationOf(segment.getFrom());
            String to = relationOf(segment.getTo());
            if (from.equals(to)) {
                continue;
            }
            successors.computeIfAbsent(from, key -> new LinkedHashSet<String>()).add(to);
        }
        return successors;
    }
}
