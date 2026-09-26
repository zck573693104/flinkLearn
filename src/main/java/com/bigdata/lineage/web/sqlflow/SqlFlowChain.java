package com.bigdata.lineage.web.sqlflow;

import com.bigdata.lineage.graph.ColumnLink;
import com.bigdata.lineage.graph.GraphNode;
import com.bigdata.lineage.graph.LineageGraph;
import com.bigdata.lineage.graph.TableLink;
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

    /** 表级关系行的显示文案：与对方同形，读法在图例里交代 */
    static final String RELATION_ROW_LABEL = "RelationRows";

    /**
     * 为表级伙伴造的空盒上限：这类盒一列都没有，只有关系行。大快照里一张中枢表
     * 可能挂几十个，不设上限就会把 BOX_CAP 顶爆、整张图拒绝出画。
     */
    private static final int REL_BOX_CAP = 24;

    /** 盒子里的一行：一个字段，聚合函数盒那一行"结果列"，或表级关系行 */
    static final class Row {

        private final String columnId;
        private final String column;
        /** 表级关系行不是模型列：不进右栏字段清单、没有模型标识，只当表级血缘的挂点 */
        private final boolean relation;

        private Row(String columnId, String column) {
            this(columnId, column, false);
        }

        private Row(String columnId, String column, boolean relation) {
            this.columnId = columnId;
            this.column = column;
            this.relation = relation;
        }

        String getColumnId() {
            return columnId;
        }

        String getColumn() {
            return column;
        }

        boolean isRelation() {
            return relation;
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

        private void addRelationRow(String key) {
            if (!rows.containsKey(key)) {
                rows.put(key, new Row(key, RELATION_ROW_LABEL, true));
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

    /** 表级关系段：两端是关系标识（不是列标识），连线挂在两盒的 RelationRows 行之间 */
    static final class RelSegment {

        private final String from;
        private final String to;
        private final String jobId;

        private RelSegment(String from, String to, String jobId) {
            this.from = from;
            this.to = to;
            this.jobId = jobId;
        }

        String getFrom() {
            return from;
        }

        String getTo() {
            return to;
        }

        String getJobId() {
            return jobId;
        }
    }

    private final Map<String, Box> boxes = new LinkedHashMap<String, Box>();
    private final List<Segment> segments = new ArrayList<Segment>();
    /** 只有表级血缘的表对：列级通路覆盖不到的地方，RelationRows 行把它们连起来 */
    private final List<RelSegment> relSegments = new ArrayList<RelSegment>();

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
        chain.expandRelations(graph, views);
        return chain;
    }

    /**
     * 表级关系行与表级关系段：一对表之间有 {@code TableLink} 却没有任何列级通路时，
     * 两盒各补一行 {@code RelationRows}，段连在两行之间。
     *
     * <p>为什么只补"没有列级通路"的对：列级通路本身已经把这对表连起来了，再画一条表级线
     * 就是同一件事说两遍。可达性按列级段算——就算那条通路的行被预算裁掉了，模型里它仍在，
     * 表级线不该替它出场。
     *
     * <p>伙伴表一个列都没上过路径时（join 键只进 WHERE/ON 的表最典型），它连盒都没有——
     * 这正是字段视图原先"凭空消失"的盲区，所以给它造一个只有 RelationRows 行的盒。
     * 造盒有独立上限：大快照里一张中枢表可能挂几十个这种伙伴，放任下去会把
     * {@code BOX_CAP} 顶爆，让原本画得好好的图整张拒绝出画。
     */
    private void expandRelations(LineageGraph graph, Set<String> views) {
        Map<String, Set<String>> adjacency = boxSuccessors();
        Set<String> seen = new LinkedHashSet<String>();
        int created = 0;
        for (TableLink link : graph.getTableLinks()) {
            String from = link.getFrom();
            String to = link.getTo();
            if (from == null || to == null || from.equals(to)) {
                continue;
            }
            if (!seen.add(from + '>' + to)) {
                continue;
            }
            Box source = boxes.get(from);
            Box target = boxes.get(to);
            // 语句内关系（CTE/子查询/函数盒）不是表级血缘的合法端点，整对放弃
            if ((source != null && source.isLocal()) || (target != null && target.isLocal())) {
                continue;
            }
            boolean left = source != null;
            boolean right = target != null;
            if (!left && !right) {
                continue;
            }
            if (left && right && reachable(adjacency, from, to)) {
                continue;
            }
            /*
             * 一端不在画布上：给它造一个只有关系行的盒。造不出来（超上限）就整对放弃——
             * 单端RelationRows行连不到对端，线没有落点。
             */
            if (!left) {
                if (created >= REL_BOX_CAP) {
                    continue;
                }
                source = ensureBox(graph, from, views);
                created++;
            }
            if (!right) {
                if (created >= REL_BOX_CAP) {
                    continue;
                }
                target = ensureBox(graph, to, views);
                created++;
            }
            relSegments.add(new RelSegment(from, to, link.getJobId()));
        }
        for (RelSegment rel : relSegments) {
            boxes.get(rel.getFrom()).addRelationRow(relationRowKey(rel.getFrom()));
            boxes.get(rel.getTo()).addRelationRow(relationRowKey(rel.getTo()));
        }
    }

    /** 取一个已存在的盒，或为表级伙伴造一个空盒（之后只装 RelationRows 行） */
    private Box ensureBox(LineageGraph graph, String relation, Set<String> views) {
        Box box = boxes.get(relation);
        if (box == null) {
            box = box(relation, displayName(graph, relation), typeOf(graph, relation, views),
                    graph.getNode(relation) != null && graph.getNode(relation).isLocal());
        }
        return box;
    }

    private static boolean reachable(Map<String, Set<String>> successors, String from, String to) {
        Set<String> seen = new LinkedHashSet<String>();
        java.util.Deque<String> queue = new java.util.ArrayDeque<String>();
        seen.add(from);
        queue.push(from);
        while (!queue.isEmpty()) {
            for (String next : successors.getOrDefault(queue.pop(), java.util.Collections.emptySet())) {
                if (next.equals(to)) {
                    return true;
                }
                if (seen.add(next)) {
                    queue.push(next);
                }
            }
        }
        return false;
    }

    /** 表级关系行的标识后缀：列标识是 {@code table.column}，双冒号不可能撞上真列 */
    static String relationRowKey(String relation) {
        return relation + "::relation";
    }

    Map<String, Box> getBoxes() {
        return boxes;
    }

    List<Segment> getSegments() {
        return segments;
    }

    List<RelSegment> getRelSegments() {
        return relSegments;
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

    /** 盒级邻接：布局排的是盒子，段落在盒子上就是"谁的下游"；表级段也参与分层 */
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
        for (RelSegment rel : relSegments) {
            successors.computeIfAbsent(rel.getFrom(), key -> new LinkedHashSet<String>())
                    .add(rel.getTo());
        }
        return successors;
    }
}
