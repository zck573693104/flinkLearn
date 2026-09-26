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
 * 服务端算"这一格在哪一片"：层号、同层序号、盒与行的标识、哪条线连哪两格。
 *
 * <p>为什么这些结论留在后端：层号就是这套图的语义（第几层 = 距源头几跳），同层序号是
 * 重心法减叉的结论。它们一旦只有前端懂，"这张图该长成什么样"就没法在单测里断言，
 * 也没法被第二个消费方复用。
 *
 * <p>为什么像素不留在后端：盒子多高取决于列名被浏览器排成几行，服务端只能靠
 * "一个字符 7.2px" 去猜。猜错一次，前端就要多写一段兜底（字号下限、重排、fit 的
 * 可读性判断），这套东西上一版长了 350 行，最后还是画不出能读的图。
 * 现在服务端给网格坐标（layer/slot），浏览器给像素——盒高交给内容，线在量完之后画。
 *
 * <p>标识是这套图唯一的地址：盒 {@code b3}、行 {@code b3_c1}、边 {@code e7}。
 * 写成 CSS 合法标识符，因为前端要拿它当 DOM id 直接 getElementById。
 */
final class SqlFlowLayout {

    /** 一个盒落好位的结果：只到网格，不到像素 */
    static final class PlacedBox {

        private final SqlFlowChain.Box box;
        private final String id;
        private final int layer;
        private final int slot;
        /** 字段标识 → 图上行标识，顺序就是这一盒里行的显示顺序 */
        private final Map<String, String> rowIds = new LinkedHashMap<String, String>();

        private PlacedBox(SqlFlowChain.Box box, String id, int layer, int slot) {
            this.box = box;
            this.id = id;
            this.layer = layer;
            this.slot = slot;
        }

        SqlFlowChain.Box getBox() {
            return box;
        }

        String getId() {
            return id;
        }

        Map<String, String> getRowIds() {
            return rowIds;
        }

        /** 按落好的顺序取行：预算裁掉的那些不在 rowIds 里，自然不会出现 */
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

        SqlFlowChain.Segment getSegment() {
            return segment;
        }
    }

    /** 表级关系段落好位的结果：两端是 RelationRows 行（行被预算裁掉时退到盒本身） */
    static final class PlacedRelEdge {

        private final String id;
        private final String sourceId;
        private final String targetId;
        private final SqlFlowChain.RelSegment rel;

        private PlacedRelEdge(String id, String sourceId, String targetId,
                              SqlFlowChain.RelSegment rel) {
            this.id = id;
            this.sourceId = sourceId;
            this.targetId = targetId;
            this.rel = rel;
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

        SqlFlowChain.RelSegment getRel() {
            return rel;
        }
    }

    /** 一次落位的全部结论。盒与行的图上标识都在这里，装配层靠它反查模型 */
    static final class Plan {

        private final List<PlacedBox> boxes = new ArrayList<PlacedBox>();
        private final List<PlacedEdge> edges = new ArrayList<PlacedEdge>();
        private final List<PlacedRelEdge> relEdges = new ArrayList<PlacedRelEdge>();
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

        List<PlacedRelEdge> getRelEdges() {
            return relEdges;
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
     * @param rels 只有表级血缘的表对：线连两盒的 RelationRows 行，行没画出来就退到盒
     */
    static Plan plan(SqlFlowChain chain, Map<String, List<SqlFlowChain.Row>> rowsOf,
                     Collection<SqlFlowChain.Segment> edges,
                     Collection<SqlFlowChain.RelSegment> rels) {
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

        int boxIndex = 0;
        Map<String, PlacedBox> placed = new LinkedHashMap<String, PlacedBox>();
        for (int layer = 0; layer < columns.size(); layer++) {
            List<String> column = columns.get(layer);
            for (int slot = 0; slot < column.size(); slot++) {
                String relation = column.get(slot);
                SqlFlowChain.Box box = chain.getBoxes().get(relation);
                PlacedBox spot = new PlacedBox(box, "b" + boxIndex++, layer, slot);
                List<SqlFlowChain.Row> rows = rowsOf.get(relation);
                if (rows != null) {
                    for (int i = 0; i < rows.size(); i++) {
                        spot.rowIds.put(rows.get(i).getColumnId(), spot.id + "_c" + i);
                    }
                }
                placed.put(relation, spot);
                plan.boxes.add(spot);
                plan.ranks.put(relation, layer);
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
        for (SqlFlowChain.RelSegment rel : rels) {
            /*
             * 两端一定落在格子上：expandRelations 只给成对的盒补关系行，哪一端本来没盒就当场
             * 造一个（造不出就整对放弃，不会留下单端的段）。而行标识带着盒前缀，两个不同的盒
             * 挤不出同一个 id——这里没有"自环"可言，不再占 skipped 的计数与文案。
             */
            String source = relEndpoint(placed.get(rel.getFrom()));
            String target = relEndpoint(placed.get(rel.getTo()));
            plan.relEdges.add(new PlacedRelEdge("e" + edgeIndex++, source, target, rel));
        }
        return plan;
    }

    /** 行的图上标识；这一行没被留下（预算裁掉了）时退回盒子本身，线连盒不连空位 */
    private static String endpoint(PlacedBox box, String columnId) {
        String row = box.getRowIds().get(columnId);
        return row == null ? box.getId() : row;
    }

    /** RelationRows 行的图上标识：行没进预算就接在盒上，表级关系不该因为预算而消失 */
    private static String relEndpoint(PlacedBox box) {
        String row = box.getRowIds().get(SqlFlowChain.relationRowKey(box.getBox().getRelation()));
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
        Collections.sort(layers);
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

    /**
     * 盒的 JSON：一份"够画出一个盒子"的语义描述。
     *
     * <p>{@code qualifiedName} 必须和请求参数能认的那个名字一致（前端点盒顶要能以这张表
     * 为中心重开一片），短名 {@code name} 只是标签，同名短名解不出地址——所以两个都给。
     */
    static Map<String, Object> boxJson(PlacedBox box, String modelId,
                                       Map<String, String> rowModelIds) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (SqlFlowChain.Row row : box.getRows()) {
            Map<String, Object> json = new LinkedHashMap<>();
            json.put("id", box.rowIds.get(row.getColumnId()));
            json.put("name", row.getColumn());
            if (row.isRelation()) {
                /*
                 * 表级关系行不是模型列：kind 让前端把它画成虚线挂点；
                 * qualifiedName 给关系本体——右栏证据、定位搜索都用得上，行键里的
                 * "::relation" 后缀是实现细节，不出口。
                 */
                json.put("kind", "relation");
                json.put("qualifiedName", box.box.getRelation());
            } else {
                json.put("qualifiedName", row.getColumnId());
            }
            json.put("modelId", rowModelIds.get(row.getColumnId()));
            rows.add(json);
        }
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("id", box.id);
        json.put("name", box.box.getName());
        json.put("qualifiedName", box.box.getRelation());
        json.put("type", box.box.getType());
        json.put("local", box.box.isLocal());
        json.put("layer", box.layer);
        json.put("slot", box.slot);
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
}
