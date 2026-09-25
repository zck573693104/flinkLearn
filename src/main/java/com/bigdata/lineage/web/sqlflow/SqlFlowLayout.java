package com.bigdata.lineage.web.sqlflow;

import com.bigdata.lineage.graph.LayeredDagBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 服务端算布局：每个盒、每一行的坐标都进响应，前端只负责照着画。
 *
 * <p>为什么搬到服务端：布局口径一旦只有前端懂，"这张图长什么样"就没法在单测里断言，
 * 也没法被别的消费方（导出、截图服务、第二个前端）复用。坐标是这次请求的一部分结论，
 * 不是渲染副产品。
 *
 * <p>尺寸照抄对方：盒宽 162、行高 16、标题带 21.96875、盒高 25.96875 + 16 * 行数。
 * 列坐标以左上角为原点（对方口径），cytoscape 用的是中心点，前端换算。
 */
final class SqlFlowLayout {

    /** 盒宽：与对方一致，一行一个字段时这个宽度读得出 20 个字符左右的列名 */
    static final double BOX_W = 162;
    /** 一行的高度 */
    static final double ROW_H = 16;
    static final double ROW_W = 160;
    /** 标题带高度：盒子里第一行距离盒顶 */
    static final double ROW_TOP = 21.96875;
    /** 盒高 = 标题带 + 行高 * 行数，多出来的 4px 是盒底留白 */
    static final double BOX_PAD = 25.96875;
    /** 层与层之间沿 X 轴的间距 */
    private static final double RANK_GAP = 220;
    /** 同一层里盒与盒的间距 */
    private static final double BOX_GAP = 22;
    /** 标签字号与行高：字号 12 是对方图上读得出的最小值 */
    private static final String FONT_SIZE = "12";
    private static final String FONT_FAMILY = "Cascadia Mono, Consolas, monospace";
    private static final double CHAR_W = 7.2;

    /** 一个盒落好位的结果 */
    static final class PlacedBox {

        private final SqlFlowChain.Box box;
        private final String id;
        private final double x;
        private final double y;
        private final double height;
        private final Map<String, String> rowIds = new LinkedHashMap<String, String>();

        private PlacedBox(SqlFlowChain.Box box, String id, double x, double y, double height) {
            this.box = box;
            this.id = id;
            this.x = x;
            this.y = y;
            this.height = height;
        }

        SqlFlowChain.Box getBox() {
            return box;
        }

        String getId() {
            return id;
        }

        /** 字段标识 → 图上行标识；表级视图没有行，返回空 */
        Map<String, String> getRowIds() {
            return rowIds;
        }

        List<SqlFlowChain.Row> getRows() {
            List<SqlFlowChain.Row> rows = new ArrayList<>();
            for (String columnId : rowIds.keySet()) {
                SqlFlowChain.Row row = box.getRows().get(columnId);
                if (row != null) {
                    rows.add(row);
                }
            }
            return rows;
        }

        double getX() {
            return x;
        }

        double getY() {
            return y;
        }

        double getHeight() {
            return height;
        }
    }

    /** 一段链路落好位的结果：两端已经是图上的行标识 */
    static final class PlacedEdge {

        private final String id;
        private final String sourceId;
        private final String targetId;
        private final SqlFlowChain.Segment segment;

        private PlacedEdge(String id, String sourceId, String targetId,
                           SqlFlowChain.Segment segment) {
            this.id = id;
            this.sourceId = sourceId;
            this.targetId = targetId;
            this.segment = segment;
        }

        String getId() {
            return id;
        }

        String getSourceId() {
            return sourceId;
        }

        String getTargetId() {
            return targetId;
        }

        SqlFlowChain.Segment getSegment() {
            return segment;
        }
    }

    /** 一次落位的全部结论：盒、线、层号。盒与行的图上标识都在这里，装配层靠它反查模型 */
    static final class Plan {

        private final List<PlacedBox> boxes = new ArrayList<PlacedBox>();
        private final List<PlacedEdge> edges = new ArrayList<PlacedEdge>();
        private final Map<String, Integer> ranks = new LinkedHashMap<String, Integer>();
        private final Set<String> cyclic = new LinkedHashSet<String>();
        /** 留在预算里、却因为两端重合（自环）或盒子没落位而画不出线的段 */
        private int skipped;

        List<PlacedBox> getBoxes() {
            return boxes;
        }

        List<PlacedEdge> getEdges() {
            return edges;
        }

        Map<String, Integer> getRanks() {
            return ranks;
        }

        /** 落在环上的盒：环上的层号是人为断开的，UI 要标出来而不是装作因果成立 */
        Set<String> getCyclic() {
            return cyclic;
        }

        /** 段数与线数对不平的那几个，装配层要把它说出来，不能让线静默消失 */
        int getSkipped() {
            return skipped;
        }
    }

    private SqlFlowLayout() {
    }

    /**
     * @param rowsOf 每个盒最终留下哪些行（已按可读性预算裁剪），键为关系标识
     * @param edges 参与落位的链路段；一行的盒都不画时线连盒
     */
    static Plan plan(SqlFlowChain chain, Map<String, List<SqlFlowChain.Row>> rowsOf,
                     Collection<SqlFlowChain.Segment> edges) {
        Plan plan = new Plan();
        List<String> relations = new ArrayList<>(chain.getBoxes().keySet());
        if (relations.isEmpty()) {
            return plan;
        }
        Map<String, Set<String>> successors = chain.boxSuccessors();
        LayeredDagBuilder.Layers layers = LayeredDagBuilder.build(relations, successors);
        List<List<String>> columns = group(relations, layers.getRank());
        plan.cyclic.addAll(layers.getCyclic());
        barycenter(columns, successors);

        double span = 0;
        for (List<String> group : columns) {
            span = Math.max(span, crossOf(group, rowsOf));
        }
        int boxIndex = 0;
        Map<String, PlacedBox> placed = new LinkedHashMap<String, PlacedBox>();
        for (int rankIndex = 0; rankIndex < columns.size(); rankIndex++) {
            double cursor = (span - crossOf(columns.get(rankIndex), rowsOf)) / 2;
            for (String relation : columns.get(rankIndex)) {
                SqlFlowChain.Box box = chain.getBoxes().get(relation);
                List<SqlFlowChain.Row> rows = rowsOf.get(relation);
                int rowCount = rows == null ? 0 : rows.size();
                double height = BOX_PAD + rowCount * ROW_H;
                String id = "n" + boxIndex++;
                PlacedBox spot = new PlacedBox(box, id, rankIndex * (BOX_W + RANK_GAP), cursor,
                        height);
                for (int i = 0; i < rowCount; i++) {
                    spot.rowIds.put(rows.get(i).getColumnId(), id + "::n" + i);
                }
                placed.put(relation, spot);
                plan.boxes.add(spot);
                plan.ranks.put(relation, rankIndex);
                cursor += height + BOX_GAP;
            }
        }
        int edgeIndex = 0;
        for (SqlFlowChain.Segment segment : edges) {
            PlacedBox from = placed.get(SqlFlowChain.relationOf(segment.getFrom()));
            PlacedBox to = placed.get(SqlFlowChain.relationOf(segment.getTo()));
            if (from == null || to == null) {
                plan.skipped++;
                continue;
            }
            String source = endpoint(from, segment.getFrom());
            String target = endpoint(to, segment.getTo());
            if (source.equals(target)) {
                plan.skipped++;
                continue;
            }
            plan.edges.add(new PlacedEdge("e" + edgeIndex++, source, target, segment));
        }
        return plan;
    }

    /** 行的图上标识；这一行没被留下（预算裁掉了）时退回盒子本身，线连盒不连空位 */
    private static String endpoint(PlacedBox box, String columnId) {
        String row = box.getRowIds().get(columnId);
        return row == null ? box.getId() : row;
    }

    private static List<List<String>> group(List<String> relations, Map<String, Integer> rank) {
        Map<Integer, List<String>> buckets = new LinkedHashMap<Integer, List<String>>();
        for (String relation : relations) {
            Integer hit = rank.get(relation);
            int layer = hit == null ? 0 : hit;
            List<String> bucket = buckets.get(layer);
            if (bucket == null) {
                bucket = new ArrayList<>();
                buckets.put(layer, bucket);
            }
            bucket.add(relation);
        }
        List<Integer> layers = new ArrayList<>(buckets.keySet());
        java.util.Collections.sort(layers);
        List<List<String>> columns = new ArrayList<>();
        for (Integer layer : layers) {
            columns.add(buckets.get(layer));
        }
        return columns;
    }

    /** 同层内的顺序：按已排定的上游位置均值排（重心法一趟），没排过的保持书写序 */
    private static void barycenter(List<List<String>> columns,
                                   Map<String, Set<String>> successors) {
        Map<String, Set<String>> upstream = new LinkedHashMap<String, Set<String>>();
        for (Map.Entry<String, Set<String>> entry : successors.entrySet()) {
            for (String to : entry.getValue()) {
                upstream.computeIfAbsent(to, key -> new LinkedHashSet<String>())
                        .add(entry.getKey());
            }
        }
        Map<String, Integer> position = new LinkedHashMap<String, Integer>();
        for (int rankIndex = 0; rankIndex < columns.size(); rankIndex++) {
            List<String> group = columns.get(rankIndex);
            List<String> ranked = new ArrayList<>(group);
            if (rankIndex > 0) {
                Map<String, Double> key = new LinkedHashMap<String, Double>();
                for (int i = 0; i < ranked.size(); i++) {
                    key.put(ranked.get(i), gravity(upstream.get(ranked.get(i)), position, i));
                }
                Collections.sort(ranked, (left, right) ->
                        Double.compare(key.get(left), key.get(right)));
            }
            for (int i = 0; i < ranked.size(); i++) {
                position.put(ranked.get(i), i);
            }
            group.clear();
            group.addAll(ranked);
        }
    }

    /** 上游还没排过的位置给不出重心，退回原序号：不打乱书写序 */
    private static double gravity(Set<String> sources, Map<String, Integer> position,
                                  int fallback) {
        if (sources == null || sources.isEmpty()) {
            return fallback;
        }
        double sum = 0;
        int hits = 0;
        for (String source : sources) {
            Integer spot = position.get(source);
            if (spot != null) {
                sum += spot;
                hits++;
            }
        }
        return hits == 0 ? fallback : sum / hits;
    }

    private static double crossOf(List<String> relations,
                                  Map<String, List<SqlFlowChain.Row>> rowsOf) {
        double total = 0;
        for (String relation : relations) {
            List<SqlFlowChain.Row> rows = rowsOf.get(relation);
            int count = rows == null ? 0 : rows.size();
            total += BOX_PAD + count * ROW_H + BOX_GAP;
        }
        return Math.max(0, total - BOX_GAP);
    }

    /** 盒的 JSON：坐标照对方口径写左上角，标签度量一并给出，前端不再量字 */
    static Map<String, Object> boxJson(PlacedBox box, String modelId,
                                       Map<String, String> rowModelIds, Integer layer) {
        List<Map<String, Object>> rows = new ArrayList<>();
        int index = 0;
        for (SqlFlowChain.Row row : box.getRows()) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("id", box.rowIds.get(row.getColumnId()));
            json.put("x", box.x + 1);
            json.put("y", box.y + ROW_TOP + index * ROW_H);
            json.put("width", ROW_W);
            json.put("height", ROW_H);
            json.put("label", labelJson(row.getColumn(), ROW_W - 8, 13.96875));
            json.put("qualifiedName", row.getColumnId());
            json.put("modelId", rowModelIds.get(row.getColumnId()));
            rows.add(json);
            index++;
        }
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("id", box.id);
        /*
         * 关系标识必须跟着几何一起下来：前端点盒顶表名要"以这张表为中心重开一片"，
         * 手里只剩 name（短名）时同名短名解不出来，那个手势就只能默默降级成列字段清单。
         */
        json.put("qualifiedName", box.getBox().getRelation());
        json.put("x", box.x);
        json.put("y", box.y);
        json.put("width", BOX_W);
        json.put("height", box.height);
        json.put("label", labelJson(box.box.getName(), BOX_W, 17.96875));
        json.put("type", box.box.getType());
        json.put("local", box.box.isLocal());
        json.put("layer", layer);
        json.put("modelId", modelId);
        json.put("columns", rows);
        return json;
    }

    static Map<String, Object> edgeJson(PlacedEdge edge) {
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("id", edge.id);
        json.put("sourceId", edge.sourceId);
        json.put("targetId", edge.targetId);
        // 函数盒那一跳是显示层的表达、不是解析出的事实，前端据此画虚线，不把假因果写实
        json.put("synthetic", edge.getSegment().isSynthetic());
        return json;
    }

    private static Map<String, Object> labelJson(String content, double maxWidth, double height) {
        String text = content == null ? "" : content;
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("content", text);
        json.put("fontSize", FONT_SIZE);
        json.put("fontFamily", FONT_FAMILY);
        json.put("width", Math.min(maxWidth, Math.round(CHAR_W * text.length())));
        json.put("height", height);
        json.put("x", 0);
        json.put("y", 0);
        return json;
    }
}
