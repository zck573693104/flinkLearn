package com.bigdata.lineage.graph;

import com.bigdata.lineage.parser.NameNormalizer;
import com.bigdata.lineage.parser.model.ColumnDerivation;
import com.bigdata.lineage.parser.model.ColumnEdge;
import com.bigdata.lineage.parser.model.ColumnRef;
import com.bigdata.lineage.parser.model.TableLineage;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 把逐语句的解析结果拼成一张图：折掉伪节点、给语句内关系划命名空间、算分层。
 *
 * <p>解析层刻意只在**一条语句内部**绑定列；跨语句、跨跳的链路在这里拼，因为只有图构建层
 * 看得见全部语句。两件事必须在这一层做掉：
 * <ul>
 *   <li>子查询与 explode/unnest 的展开行是列绑定的中间产物，不是用户认识的表，
 *       由引擎标成中间关系后折成一跳，完整链路留在 {@link ColumnLink#getHops()}
 *       里给 UI 当证据；</li>
 *   <li>CTE 名与伪节点名只在语句内唯一，{@code tmp}、{@code t} 这类名字在语料里跨语句复用，
 *       不加语句命名空间就会把两条无关的链路接成一个节点。CTE 本身不折叠——它是字段来源的中间站。</li>
 * </ul>
 */
@Slf4j
public final class ColumnGraphBuilder {

    private ColumnGraphBuilder() {
    }

    public static LineageGraph build(List<TableLineage> statements) {
        Accumulator acc = new Accumulator();
        if (statements != null) {
            for (int i = 0; i < statements.size(); i++) {
                TableLineage lineage = statements.get(i);
                if (lineage != null) {
                    handle(lineage, i, acc);
                }
            }
        }
        LayeredDagBuilder.Layers layers = acc.layers();
        return new LineageGraph(acc.nodes(), acc.links, acc.tableLinks,
                layers.getRank(), layers.getCyclic());
    }

    private static void handle(TableLineage lineage, int ordinal, Accumulator acc) {
        StatementContext st = new StatementContext(lineage, ordinal);
        addTableLinks(lineage, st.jobId, acc);
        List<ColumnEdge> edges = lineage.getColumnEdges() == null
                ? Collections.<ColumnEdge>emptyList() : lineage.getColumnEdges();
        // 先把中间关系的产出边登记完再折叠：引擎按遍历序产出，理论上"产出边"在前，
        // 但依赖那个顺序等于给折叠埋一颗雷，两趟扫一遍就好。
        for (ColumnEdge edge : edges) {
            if (edge.isTargetInternal()) {
                st.producers.computeIfAbsent(internalColumnId(st, edge), key -> new ArrayList<>())
                        .add(edge);
                st.internal.add(st.resolve(edge.getTargetTable()));
            }
        }
        for (ColumnEdge edge : edges) {
            if (edge.isTargetInternal()) {
                continue;
            }
            String toTable = st.resolve(edge.getTargetTable());
            acc.record(toTable, edge.getTargetColumn());
            fold(edge, st, toTable, acc);
        }
    }

    private static String internalColumnId(StatementContext st, ColumnEdge edge) {
        return ColumnLink.columnId(st.resolve(edge.getTargetTable()), edge.getTargetColumn());
    }

    private static void fold(ColumnEdge edge, StatementContext st, String toTable,
                             Accumulator acc) {
        FoldContext ctx = new FoldContext(st, edge, toTable, acc);
        List<String> ancestors = Collections.singletonList(
                ColumnLink.columnId(toTable, edge.getTargetColumn()));
        for (ColumnRef ref : nullSafe(edge.getSources())) {
            ctx.walk(ref, edge.getDerivation(), ancestors);
        }
    }

    private static void addTableLinks(TableLineage lineage, String jobId, Accumulator acc) {
        String target = NameNormalizer.normalizeQualified(lineage.getTargetTable());
        if (isEmpty(target)) {
            return;
        }
        acc.record(target, null);
        for (String source : nullSafe(lineage.getSourceTables())) {
            String from = NameNormalizer.normalizeQualified(source);
            if (isEmpty(from)) {
                continue;
            }
            acc.record(from, null);
            acc.addTableLink(new TableLink(from, target, jobId));
        }
    }

    /** 一条语句的作用域：哪些名字是物理表、哪些中间关系该折进来 */
    private static final class StatementContext {

        private final String jobId;
        /** 查找键（全名或末段短名）→ 归一化物理表全名 */
        private final Map<String, String> physical = new LinkedHashMap<>();
        /** 中间关系的列 → 产出它的边 */
        private final Map<String, List<ColumnEdge>> producers = new LinkedHashMap<>();
        /** 本语句里被折掉的中间关系（子查询、展开行），带命名空间后的标识 */
        private final Set<String> internal = new LinkedHashSet<>();

        private StatementContext(TableLineage lineage, int ordinal) {
            this.jobId = localName(lineage, ordinal);
            registerPhysical(lineage.getTargetTable());
            for (String source : nullSafe(lineage.getSourceTables())) {
                registerPhysical(source);
            }
        }

        private boolean isInternal(String resolved) {
            return internal.contains(resolved);
        }

        /**
         * 关系名 → 图上的节点标识：本语句的物理表用全名，其余（CTE、伪节点、绑不上的限定符）
         * 一律带语句前缀，它们都只在语句内唯一。
         */
        private String resolve(String relation) {
            if (isEmpty(relation)) {
                return "";
            }
            String dotted = NameNormalizer.normalizeQualified(relation);
            String hit = physical.get(dotted);
            if (hit == null && dotted.lastIndexOf('.') >= 0) {
                hit = physical.get(dotted.substring(dotted.lastIndexOf('.') + 1));
            }
            return hit != null ? hit : LocalRelation.id(jobId, dotted);
        }

        private void registerPhysical(String table) {
            String dotted = NameNormalizer.normalizeQualified(table);
            if (isEmpty(dotted)) {
                return;
            }
            physical.putIfAbsent(dotted, dotted);
            int dot = dotted.lastIndexOf('.');
            if (dot >= 0) {
                physical.putIfAbsent(dotted.substring(dot + 1), dotted);
            }
        }
    }

    /** 折叠一条边的来源：伪节点顺着产出边往上走，直到落在真实关系上 */
    private static final class FoldContext {

        private final StatementContext st;
        private final ColumnEdge edge;
        private final String toTable;
        private final Accumulator acc;

        private FoldContext(StatementContext st, ColumnEdge edge, String toTable,
                            Accumulator acc) {
            this.st = st;
            this.edge = edge;
            this.toTable = toTable;
            this.acc = acc;
        }

        /**
         * @param ancestors 从目标列到当前位置走过的列，既防环也是 hops 的前半段
         */
        private void walk(ColumnRef ref, ColumnDerivation carried, List<String> ancestors) {
            String relation = relationOf(ref);
            if (relation == null) {
                return;
            }
            String resolved = st.resolve(relation);
            String id = ColumnLink.columnId(resolved, ref.getColumn());
            List<ColumnEdge> producers = st.producers.get(id);
            if (producers == null || producers.isEmpty()) {
                if (st.isInternal(resolved)) {
                    // 中间关系上找不到产出边就宁缺勿假：折不开它就是把它当成用户认识的表
                    log.warn("中间关系 {} 没有产出边可折，丢弃这一跳来源", id);
                } else {
                    emit(resolved, ref.getColumn(), carried, id, ancestors);
                }
                return;
            }
            if (ancestors.contains(id)) {
                log.warn("中间关系 {} 的产出边成环，停止折叠这一跳", id);
                return;
            }
            List<String> deeper = new ArrayList<>(ancestors);
            deeper.add(id);
            for (ColumnEdge up : producers) {
                ColumnDerivation combined = weakest(carried, up.getDerivation());
                for (ColumnRef source : nullSafe(up.getSources())) {
                    walk(source, combined, deeper);
                }
            }
        }

        private void emit(String fromTable, String fromColumn, ColumnDerivation derivation,
                          String fromId, List<String> ancestors) {
            List<String> hops = new ArrayList<>();
            hops.add(fromId);
            for (int i = ancestors.size() - 1; i >= 0; i--) {
                hops.add(ancestors.get(i));
            }
            acc.record(fromTable, fromColumn);
            acc.addLink(new ColumnLink(fromTable, fromColumn, toTable, edge.getTargetColumn(),
                    derivation, st.jobId, edge.getEngine(), edge.getOrdinal(),
                    edge.getTransform(), hops));
        }

        /**
         * 来源指向哪个关系：已绑定的用绑定结果，未绑定的用原句限定符（挂到语句命名空间下，
         * 承认"有个叫这个名字的关系但不知道是什么"）。裸列名无从命名，返回 null 让这条来源不落边。
         */
        private static String relationOf(ColumnRef ref) {
            if (ref.isResolved()) {
                return ref.getBoundTable();
            }
            String qualifier = ref.getQualifier();
            return isEmpty(qualifier) ? null : qualifier;
        }
    }

    /** 整条链上置信度最低的加工方式：折过表达式的直通列不能再当 IDENTITY 看 */
    private static ColumnDerivation weakest(ColumnDerivation a, ColumnDerivation b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.getConfidence() <= b.getConfidence() ? a : b;
    }

    /**
     * 语句标识：解析层没给就用语句文本的短哈希，最后才退回列表下标。
     *
     * <p>为什么不用下标：调用方常常分文件解析再把语句列表拼起来，下标在每个文件里都从 0 开始，
     * 两个文件里的 {@code tmp} 会被接成同一个节点，凭空造出跨作业的 CTE 链路。文本哈希与拼接
     * 顺序无关；同一条语句被扫两遍时反而正确地合成一份。
     */
    private static String localName(TableLineage lineage, int ordinal) {        for (ColumnEdge edge : nullSafe(lineage.getColumnEdges())) {
            if (!isEmpty(edge.getJobId())) {
                return edge.getJobId();
            }
        }
        String sql = lineage.getOriginalSql();
        if (isEmpty(sql)) {
            return "stmt" + ordinal;
        }
        return digest(sql);
    }

    /**
     * 图里用的语句标识，原样暴露给调用方：贴 SQL 试解析这条路径不落快照，
     * 但它要把语句原文按同一个键登记起来好让 UI 回显，自己再推一遍就会和图里对不上。
     */
    public static String jobIdOf(TableLineage lineage, int ordinal) {
        return lineage == null ? "stmt" + ordinal : localName(lineage, ordinal);
    }

    private static String digest(String sql) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256")
                    .digest(sql.trim().toLowerCase(Locale.ROOT).getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder("s");
            for (int i = 0; i < 6; i++) {
                sb.append(Character.forDigit((hash[i] >> 4) & 0xF, 16))
                        .append(Character.forDigit(hash[i] & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 不可用", e);
        }
    }

    private static boolean isEmpty(String value) {
        return value == null || value.isEmpty();
    }

    private static <T> Collection<T> nullSafe(Collection<T> list) {
        return list == null ? Collections.<T>emptyList() : list;
    }

    /** 建图过程中的可变累积，最后一次性转成不可变快照 */
    private static final class Accumulator {

        private final List<ColumnLink> links = new ArrayList<>();
        private final List<TableLink> tableLinks = new ArrayList<>();
        private final Set<String> seenLinks = new LinkedHashSet<>();
        private final Set<String> seenTableLinks = new LinkedHashSet<>();
        /** 关系 → 列清单，插入序即 SELECT 里的书写序 */
        private final Map<String, List<String>> columns = new LinkedHashMap<>();

        /**
         * 同一批语句常有多条产出完全相同（{@code INSERT ALL}、重复扫描同一文件），
         * 图上并排两条一样的边只会让 UI 抖动，按端点+加工方式去重。
         */
        private void addLink(ColumnLink link) {
            if (seenLinks.add(link.getFrom() + ">" + link.getTo() + ">" + link.getDerivation())) {
                links.add(link);
            }
        }

        private void addTableLink(TableLink link) {
            if (seenTableLinks.add(link.getFrom() + ">" + link.getTo())) {
                tableLinks.add(link);
            }
        }

        private void record(String relation, String column) {
            if (isEmpty(relation)) {
                return;
            }
            List<String> owned = columns.computeIfAbsent(relation, key -> new ArrayList<>());
            if (column != null && !owned.contains(column)) {
                owned.add(column);
            }
        }

        private Map<String, GraphNode> nodes() {
            Map<String, GraphNode> nodes = new LinkedHashMap<>();
            for (Map.Entry<String, List<String>> entry : columns.entrySet()) {
                nodes.put(entry.getKey(), node(entry.getKey(), entry.getValue()));
            }
            return nodes;
        }

        /** 分层只跑物理表：语句内关系（CTE、伪节点残留）各自成岛，摆层没有意义 */
        private LayeredDagBuilder.Layers layers() {
            Set<String> physical = new LinkedHashSet<>();
            for (String relation : columns.keySet()) {
                if (!LocalRelation.isLocalId(relation)) {
                    physical.add(relation);
                }
            }
            Map<String, Collection<String>> successors = new LinkedHashMap<>();
            for (TableLink link : tableLinks) {
                successors.computeIfAbsent(link.getFrom(), key -> new ArrayList<>())
                        .add(link.getTo());
            }
            return LayeredDagBuilder.build(physical, successors);
        }
    }

    private static GraphNode node(String id, List<String> columns) {
        boolean local = LocalRelation.isLocalId(id);
        String bare = local ? LocalRelation.nameOf(id) : id;
        int dot = bare.lastIndexOf('.');
        String name = dot < 0 ? bare : bare.substring(dot + 1);
        String namespace = local ? LocalRelation.jobIdOf(id)
                : (dot < 0 ? "" : bare.substring(0, dot));
        return new GraphNode(id, local, name, namespace, columns);
    }
}
