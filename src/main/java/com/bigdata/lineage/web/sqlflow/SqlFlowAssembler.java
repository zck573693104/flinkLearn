package com.bigdata.lineage.web.sqlflow;

import com.bigdata.lineage.graph.ColumnLink;
import com.bigdata.lineage.graph.GraphNode;
import com.bigdata.lineage.graph.LineageGraph;
import com.bigdata.lineage.parser.model.ColumnDerivation;

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
 * 一次请求装配出一份"数据模型 + 落位结论"：对方一次 {@code sqlflow/graph} 调用给全的做法。
 *
 * <p>为什么把模型、布局、统计放进同一个响应：字段级血缘的三件事本来就互相牵着——点一行要知道
 * 它是哪个模型对象（{@code listIdMap}）、这条链的证据在哪条语句上（{@code relationshipIdMap}）、
 * 顶栏那几个数字得和图上看见的一致（{@code summary}）。分成多个端点就会各说各话，
 * 还要在浏览器里再拼一次。
 *
 * <p>落位只到网格（层号 + 同层序号），不到像素：盒子多高取决于浏览器把那串列名排成几行，
 * 服务端量不了，硬量就变成前端的一堆兜底。
 *
 * <p>两处口径与对方有意不同，都是这套系统的事实：证据（加工方式、置信度、引擎、语句）挂在
 * relationship 上而不只是表达式；语句标识用我们的 {@code jobId}（文件名#序号）。
 */
public final class SqlFlowAssembler {

    /** 一个盒最多画几行：整表展开时的可读性闸门 */
    static final int ROWS_CAP = 14;
    /** 整张图最多画多少行：盒多到摊薄时每盒至少留 1 行，宁可字小也不要空白画布 */
    private static final int ROW_BUDGET = 600;
    /**
     * 一个盒在模型里最多登记几列。
     *
     * <p>比画得多给是因为"列清单"这件事本来就是模型的活儿：画布上只摆得起 14 行，右栏却要把
     * 这张表还有什么列列出来给用户挑。语料里有单表 787 列的，所以这里也设一道闸门，
     * 超出的列数写在 {@code hiddenColumns} 上，不装作这张表只有 200 列。
     */
    static final int MODEL_ROWS_CAP = 200;
    /** 盒数上限：超过就不落位，只给模型和告警——渲染上千个盒子等于把浏览器当渲染靶子 */
    public static final int BOX_CAP = 300;

    /** 直连与间接：与对方 fdd/fdi 的口径对齐，置信度另挂在关系上 */
    private static final String DIRECT = "fdd";
    private static final String INDIRECT = "fdi";

    private final SqlFlowContext context;
    private final LineageGraph graph;
    private final List<ColumnLink> links;
    private final String focus;
    private final Set<String> views;

    private final Map<String, String> relationId = new LinkedHashMap<String, String>();
    private final Map<String, String> columnId = new LinkedHashMap<String, String>();
    private final Map<String, String> processId = new LinkedHashMap<String, String>();
    private final Map<SqlFlowChain.Segment, String> relationshipOf =
            new LinkedHashMap<SqlFlowChain.Segment, String>();
    private final List<List<SqlFlowChain.Segment>> groups =
            new ArrayList<List<SqlFlowChain.Segment>>();
    /** 图上边标识 → relationship 标识：对方 relationshipIdMap 的口径 */
    private final Map<String, List<String>> edgeRelationships =
            new LinkedHashMap<String, List<String>>();

    private SqlFlowAssembler(SqlFlowContext context, Collection<ColumnLink> links, String focus) {
        this.context = context;
        this.graph = context.getGraph();
        this.links = new ArrayList<>(links);
        this.focus = focus == null || focus.trim().isEmpty() ? null : focus.trim();
        this.views = viewTargets();
    }

    /**
     * 装配一份响应。
     *
     * @param links 本次要画的字段边，按邻域裁剪在调用方做（见 {@code GraphAssembler}）
     * @param focus 单链路聚焦的字段标识，null 表示整表展开
     */
    public static Map<String, Object> payload(SqlFlowContext context, Collection<ColumnLink> links,
                                              String focus) {
        return new SqlFlowAssembler(context, links, focus).build();
    }

    private Map<String, Object> build() {
        SqlFlowChain chain = SqlFlowChain.expand(graph, links, views);
        Rows rows = planRows(chain);
        boolean drawn = chain.getBoxes().size() <= BOX_CAP;
        SqlFlowLayout.Plan plan = drawn
                ? SqlFlowLayout.plan(chain, rows.map, rows.kept, rows.relCandidates)
                : new SqlFlowLayout.Plan();

        number(chain, rows);
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("sqlflow", model(chain, rows));
        data.put("graph", elements(plan));
        data.put("summary", summary(chain, rows));
        data.put("sessionId", sessionId());
        data.put("metaInfo", meta(chain, rows, plan, drawn));
        return data;
    }

    // ---------- 一张表留哪些行 ----------

    /** 行预算的结论：每个盒画哪些行、模型登记哪些行、真正进图的段，以及被折掉的行数与段数 */
    private static final class Rows {

        /** 画布上留下的行（已按预算裁剪），键为关系标识 */
        private final Map<String, List<SqlFlowChain.Row>> map;
        /** 数据模型登记的全部行：比画布多，右栏的"列清单"要能列出没画出来的列 */
        private final Map<String, List<SqlFlowChain.Row>> model;
        private final List<SqlFlowChain.Segment> kept;
        private final int limit;
        private final int droppedRows;
        private final int droppedSegments;
        /** 关系标识 → 连模型都没登记进去的列数（超出 {@code MODEL_ROWS_CAP} 的部分） */
        private final Map<String, Integer> hidden;
        /** 全部候选表级关系段：行被预算裁掉时线退到接在盒上，不靠这个列表决定画不画 */
        private final List<SqlFlowChain.RelSegment> relCandidates;

        private Rows(Map<String, List<SqlFlowChain.Row>> map,
                     Map<String, List<SqlFlowChain.Row>> model,
                     Map<String, Integer> hidden,
                     List<SqlFlowChain.Segment> kept, int limit, int droppedRows,
                     int totalSegments, List<SqlFlowChain.RelSegment> relCandidates) {
            this.map = map;
            this.model = model;
            this.hidden = hidden;
            this.kept = kept;
            this.limit = limit;
            this.droppedRows = droppedRows;
            this.droppedSegments = totalSegments - kept.size();
            this.relCandidates = relCandidates;
        }

        /** 画布上的字段行数：RelationRows 挂点不算字段，标题里的"N 字段行"不能被它灌水 */
        private int rowCount() {
            return count(map, false);
        }

        /** 画布上的表级关系行数：单独报数，它既不进字段行口径、也不进"未画列数"的减法 */
        private int relRowCount() {
            return count(map, true);
        }

        /** 模型登记的列数：比画布多出来的那部分是"看得见清单、看不见色块" */
        private int modelCount() {
            return count(model, false);
        }

        private static int count(Map<String, List<SqlFlowChain.Row>> rows, boolean relation) {
            int total = 0;
            for (List<SqlFlowChain.Row> group : rows.values()) {
                for (SqlFlowChain.Row row : group) {
                    if (row.isRelation() == relation) {
                        total++;
                    }
                }
            }
            return total;
        }
    }

    /**
     * 一个盒留哪几行：焦点列永远第一，其次是有边的列（孤岛列画进来只是多一行没线的槽位），
     * 同档按列名排——同一份输入两次装配必须长出同一张图。
     */
    private Rows planRows(SqlFlowChain chain) {
        Map<String, Integer> degree = new LinkedHashMap<String, Integer>();
        for (SqlFlowChain.Segment segment : chain.getSegments()) {
            bump(degree, segment.getFrom());
            bump(degree, segment.getTo());
        }
        int limit = Math.max(1, Math.min(ROWS_CAP,
                ROW_BUDGET / Math.max(1, chain.getBoxes().size())));
        Map<String, List<SqlFlowChain.Row>> painted = new LinkedHashMap<>();
        Map<String, List<SqlFlowChain.Row>> model = new LinkedHashMap<>();
        Map<String, Integer> hidden = new LinkedHashMap<String, Integer>();
        int dropped = 0;
        for (SqlFlowChain.Box box : chain.getBoxes().values()) {
            List<SqlFlowChain.Row> ranked = new ArrayList<>();
            for (SqlFlowChain.Row row : box.getRows().values()) {
                // 表级关系行不进排序池：它按名字排可能挤掉真列，预算名额是真列的
                if (!row.isRelation()) {
                    ranked.add(row);
                }
            }
            Collections.sort(ranked, (left, right) -> {
                int byWeight = weight(right.getColumnId(), degree)
                        - weight(left.getColumnId(), degree);
                return byWeight != 0 ? byWeight
                        : left.getColumn().compareToIgnoreCase(right.getColumn());
            });
            dropped += Math.max(0, ranked.size() - limit);
            painted.put(box.getRelation(),
                    new ArrayList<>(ranked.subList(0, Math.min(limit, ranked.size()))));
            model.put(box.getRelation(),
                    new ArrayList<>(ranked.subList(0, Math.min(MODEL_ROWS_CAP, ranked.size()))));
            hidden.put(box.getRelation(), Math.max(0, ranked.size() - MODEL_ROWS_CAP));
        }
        /*
         * 表级关系行排在真列后面，预算有富余才画——真列的位置永远不能被它占。
         * 行被裁掉时表级线退到接在盒上（见 SqlFlowLayout.relEndpoint），不静默消失。
         */
        for (SqlFlowChain.Box box : chain.getBoxes().values()) {
            List<SqlFlowChain.Row> canvas = painted.get(box.getRelation());
            for (SqlFlowChain.Row row : box.getRows().values()) {
                if (row.isRelation() && canvas.size() < limit) {
                    canvas.add(row);
                }
            }
        }
        return new Rows(painted, model, hidden, drawn(chain, painted), limit, dropped,
                chain.getSegments().size(), chain.getRelSegments());
    }

    private int weight(String column, Map<String, Integer> degree) {
        Integer hit = degree.get(column);
        return (column.equals(focus) ? 1000 : 0) + (hit == null ? 0 : hit);
    }

    private static void bump(Map<String, Integer> counts, String key) {
        Integer hit = counts.get(key);
        counts.put(key, hit == null ? 1 : hit + 1);
    }

    /** 两端有一行没画进盒子的段不画：接一半的链路比不接更容易读错 */
    private static List<SqlFlowChain.Segment> drawn(SqlFlowChain chain,
                                                    Map<String, List<SqlFlowChain.Row>> rowsOf) {
        Set<String> kept = new LinkedHashSet<String>();
        for (List<SqlFlowChain.Row> rows : rowsOf.values()) {
            for (SqlFlowChain.Row row : rows) {
                kept.add(row.getColumnId());
            }
        }
        List<SqlFlowChain.Segment> segments = new ArrayList<>();
        for (SqlFlowChain.Segment segment : chain.getSegments()) {
            if (kept.contains(segment.getFrom()) && kept.contains(segment.getTo())) {
                segments.add(segment);
            }
        }
        return segments;
    }

    // ---------- 标识分配 ----------

    /**
     * 模型标识按"盒 → 行 → 语句 → 关系"分配。
     *
     * <p>顺序固定不是讲究：同一份输入两次装配出同一套 id，前端缓存与快照比对才有意义。
     * 行按模型口径（不只是画布上那几行）分配，没画出来的列也要有标识——右栏点它时要能问得到。
     */
    private void number(SqlFlowChain chain, Rows rows) {
        int seed = 1;
        for (SqlFlowChain.Box box : chain.getBoxes().values()) {
            relationId.put(box.getRelation(), String.valueOf(seed++));
            for (SqlFlowChain.Row row : rows.model.get(box.getRelation())) {
                columnId.put(row.getColumnId(), String.valueOf(seed++));
            }
        }
        for (ColumnLink link : links) {
            String jobId = link.getJobId();
            if (jobId != null && !processId.containsKey(jobId)) {
                processId.put(jobId, String.valueOf(seed++));
            }
        }
        groups.addAll(groupsOf(rows.kept));
        for (List<SqlFlowChain.Segment> group : groups) {
            String id = String.valueOf(seed++);
            for (SqlFlowChain.Segment segment : group) {
                relationshipOf.put(segment, id);
            }
        }
    }

    /**
     * 同一个目标列、同一个表达式的多路来源合成一条关系：聚合本来就是"多列进、一列出"。
     *
     * <p>表达式不同的必须分开——UNION 的两个分支并成一条，读的人会以为一条链路有两个来源列。
     */
    private static List<List<SqlFlowChain.Segment>> groupsOf(
            List<SqlFlowChain.Segment> segments) {
        Map<String, List<SqlFlowChain.Segment>> groups =
                new LinkedHashMap<String, List<SqlFlowChain.Segment>>();
        for (SqlFlowChain.Segment segment : segments) {
            ColumnLink link = segment.getLink();
            String key = link.getJobId() + '@' + segment.getTo() + '@' + link.getOrdinal()
                    + '@' + link.getDerivation() + '@' + link.getTransform();
            List<SqlFlowChain.Segment> group = groups.get(key);
            if (group == null) {
                group = new ArrayList<>();
                groups.put(key, group);
            }
            group.add(segment);
        }
        return new ArrayList<>(groups.values());
    }

    // ---------- 数据模型 ----------

    private Map<String, Object> model(SqlFlowChain chain, Rows rows) {
        List<Map<String, Object>> processes = processes();
        Map<String, Object> sqlflow = new LinkedHashMap<String, Object>();
        sqlflow.put("dbobjs", dbobjs(chain, rows, processes));
        sqlflow.put("processes", processes);
        sqlflow.put("relationships", relationships(chain));
        return sqlflow;
    }

    /** 一个 schema 槽位里的三组实体：物理表、视图、中间站 */
    private static final class Slot {

        private final Map<String, Object> tables = new LinkedHashMap<String, Object>();
        private final Map<String, Object> views = new LinkedHashMap<String, Object>();
        private final Map<String, Object> others = new LinkedHashMap<String, Object>();

        private Map<String, Object> of(String key) {
            if ("view".equals(key)) {
                return views;
            }
            return "other".equals(key) ? others : tables;
        }
    }

    /**
     * 服务器 → 库 → schema 的三级嵌套：我们的命名空间正是这一层的来源。
     *
     * <p>语句内关系挂在产出它的那条语句所属的 schema 下，与物理表并列——它们是同一条语句的产物，
     * 单开一组反而看不出归属。
     */
    private Map<String, Object> dbobjs(SqlFlowChain chain, Rows rows,
                                       List<Map<String, Object>> processes) {
        Map<String, Map<String, Slot>> tree = new LinkedHashMap<String, Map<String, Slot>>();
        for (SqlFlowChain.Box box : chain.getBoxes().values()) {
            String[] group = groupOf(box);
            Map<String, Slot> schemas = tree.get(group[0]);
            if (schemas == null) {
                schemas = new LinkedHashMap<String, Slot>();
                tree.put(group[0], schemas);
            }
            Slot slot = schemas.get(group[1]);
            if (slot == null) {
                slot = new Slot();
                schemas.put(group[1], slot);
            }
            slot.of(entityKey(box)).put(relationId.get(box.getRelation()), entity(box, rows));
        }
        List<Map<String, Object>> servers = new ArrayList<>();
        for (Map.Entry<String, Map<String, Slot>> db : tree.entrySet()) {
            List<Map<String, Object>> schemas = new ArrayList<>();
            for (Map.Entry<String, Slot> schema : db.getValue().entrySet()) {
                Map<String, Object> json = new LinkedHashMap<String, Object>();
                json.put("name", schema.getKey());
                json.put("tables", new ArrayList<>(schema.getValue().tables.values()));
                json.put("views", new ArrayList<>(schema.getValue().views.values()));
                json.put("others", new ArrayList<>(schema.getValue().others.values()));
                json.put("processes", processes);
                schemas.add(json);
            }
            Map<String, Object> database = new LinkedHashMap<String, Object>();
            database.put("name", db.getKey());
            database.put("schemas", schemas);
            Map<String, Object> server = new LinkedHashMap<String, Object>();
            server.put("name", "DEFAULT_SERVER");
            server.put("dbVendor", vendor());
            server.put("supportsCatalogs", true);
            server.put("supportsSchemas", true);
            server.put("databases", Collections.singletonList(database));
            servers.add(server);
        }
        Map<String, Object> dbobjs = new LinkedHashMap<String, Object>();
        dbobjs.put("servers", servers);
        return dbobjs;
    }

    private static String entityKey(SqlFlowChain.Box box) {
        return box.isLocal() ? "other" : ("view".equals(box.getType()) ? "view" : "table");
    }

    /** 盒归哪个库/schema：物理表看命名空间，中间站看产出它的那条语句写在哪个 schema 里 */
    private String[] groupOf(SqlFlowChain.Box box) {
        GraphNode node = graph.getNode(box.getRelation());
        String namespace = node != null && !node.isLocal()
                ? node.getNamespace() : statementNamespace(box.getRelation());
        return namespaceParts(namespace);
    }

    private static String[] namespaceParts(String namespace) {
        String bare = namespace == null ? "" : namespace;
        int dot = bare.indexOf('.');
        if (dot < 0) {
            return new String[]{"DEFAULT", bare.isEmpty() ? "DEFAULT" : bare};
        }
        return new String[]{bare.substring(0, dot), bare.substring(dot + 1)};
    }

    /** 语句内关系没有命名空间，挂在它的语句的目标表下面；语句纯查询时退回 DEFAULT */
    private String statementNamespace(String relation) {
        for (ColumnLink link : links) {
            if (!relation.startsWith("[" + link.getJobId() + "]")) {
                continue;
            }
            GraphNode target = graph.getNode(link.getToTable());
            if (target != null && !target.isLocal()) {
                return target.getNamespace();
            }
        }
        return "";
    }

    /**
     * 实体（盒）与它的列清单：模型登记的是"这张表有哪些列"，不是"画布上摆了几行"。
     *
     * <p>两者的差用 {@code uiVisible} 说清楚：没画出来的列在模型里照样查得到（右栏的列清单、
     * 按列名找字段都靠它），但它没进盒子的行清单、也没有图上标识，前端不能凭空摆一行上去。
     */
    private Map<String, Object> entity(SqlFlowChain.Box box, Rows rows) {
        String relation = box.getRelation();
        Set<String> painted = new LinkedHashSet<String>();
        for (SqlFlowChain.Row row : rows.map.get(relation)) {
            painted.add(row.getColumnId());
        }
        List<Map<String, Object>> columns = new ArrayList<>();
        for (SqlFlowChain.Row row : rows.model.get(relation)) {
            Map<String, Object> json = new LinkedHashMap<String, Object>();
            json.put("id", columnId.get(row.getColumnId()));
            json.put("name", row.getColumn());
            json.put("qualifiedName", row.getColumnId());
            json.put("uiVisible", painted.contains(row.getColumnId()));
            columns.add(json);
        }
        Map<String, Object> object = new LinkedHashMap<String, Object>();
        object.put("id", relationId.get(relation));
        object.put("name", box.getName());
        object.put("displayName", box.getName());
        object.put("type", box.getType());
        object.put("qualifiedName", relation);
        object.put("local", box.isLocal());
        int realColumns = 0;
        for (SqlFlowChain.Row row : box.getRows().values()) {
            if (!row.isRelation()) {
                realColumns++;
            }
        }
        object.put("columnCount", realColumns);
        object.put("hiddenColumns", rows.hidden.get(relation));
        object.put("columns", columns);
        return object;
    }

    /** 引擎标识：一条链路的引擎本来就一致，整库混扫时取第一个读得到的 */
    private String vendor() {
        for (ColumnLink link : links) {
            if (link.getEngine() != null && !link.getEngine().isEmpty()) {
                return link.getEngine().toLowerCase(Locale.ROOT);
            }
        }
        return "unknown";
    }

    /** 语句实体：SQL 原文放在 {@code transforms[0].code}，前端点节点时按行号列号高亮 */
    private List<Map<String, Object>> processes() {
        List<Map<String, Object>> list = new ArrayList<>();
        int ordinal = 1;
        for (Map.Entry<String, String> entry : processId.entrySet()) {
            String jobId = entry.getKey();
            String sql = context.sqlOf(jobId);
            SqlFlowStatement statement = SqlFlowStatement.of(sql);
            Map<String, Object> transform = new LinkedHashMap<String, Object>();
            transform.put("code", sql == null ? "" : sql);
            transform.put("type", "statement");
            transform.put("coordinates", Collections.emptyList());
            Map<String, Object> json = new LinkedHashMap<String, Object>();
            json.put("id", entry.getValue());
            json.put("name", statement.getHeading() + "-" + ordinal++);
            json.put("type", statement.getEffectType());
            json.put("qualifiedName", jobId);
            json.put("engine", engineOf(jobId));
            json.put("parseError", context.isParseError(jobId));
            json.put("uiVisible", false);
            json.put("transforms", Collections.singletonList(transform));
            list.add(json);
        }
        return list;
    }

    private String engineOf(String jobId) {
        for (ColumnLink link : links) {
            if (jobId.equals(link.getJobId())) {
                return link.getEngine();
            }
        }
        return null;
    }

    private List<Map<String, Object>> relationships(SqlFlowChain chain) {
        List<Map<String, Object>> list = new ArrayList<>();
        for (List<SqlFlowChain.Segment> group : groups) {
            list.add(relationship(chain, group));
        }
        return list;
    }

    private Map<String, Object> relationship(SqlFlowChain chain,
                                             List<SqlFlowChain.Segment> group) {
        SqlFlowChain.Segment head = group.get(0);
        ColumnLink link = head.getLink();
        List<Map<String, Object>> sources = new ArrayList<>();
        for (SqlFlowChain.Segment segment : group) {
            sources.add(endpoint(segment, segment.getFrom(), true));
        }
        String jobId = link.getJobId();
        Map<String, Object> json = new LinkedHashMap<String, Object>();
        json.put("id", relationshipOf.get(head));
        json.put("processId", processId.get(jobId));
        json.put("effectType", effectType(head, jobId));
        json.put("type", direct(link) ? DIRECT : INDIRECT);
        json.put("sources", sources);
        json.put("target", endpoint(head, head.getTo(), false));
        json.put("derivation", link.getDerivation() == null ? null : link.getDerivation().name());
        json.put("confidence", link.getConfidence());
        json.put("engine", link.getEngine());
        json.put("ordinal", link.getOrdinal());
        json.put("transform", link.getTransform());
        json.put("hops", link.getHops());
        json.put("qualifiedName", link.getFrom() + ">" + link.getTo());
        return json;
    }

    /** 中间站上的那一跳永远只是"把结果写进去"，语句动作看它最终落在哪张表 */
    private String effectType(SqlFlowChain.Segment segment, String jobId) {
        GraphNode target = graph.getNode(SqlFlowChain.relationOf(segment.getTo()));
        if (target == null || target.isLocal()) {
            return "select";
        }
        return SqlFlowStatement.of(context.sqlOf(jobId)).getEffectType();
    }

    private static boolean direct(ColumnLink link) {
        ColumnDerivation derivation = link.getDerivation();
        return derivation == ColumnDerivation.IDENTITY || derivation == ColumnDerivation.CONSTANT;
    }

    /**
     * 来源/目标的一行：{@code parentId} 指向盒的模型标识，前端据此在模型里反查。
     *
     * <p>表达式只挂一次：整条折叠链上它是同一个目标列表达式，挂在进入函数盒的那一段
     * （没有函数盒时挂在第一段），其余段只是路过中间站。
     */
    private Map<String, Object> endpoint(SqlFlowChain.Segment segment, String columnRef,
                                         boolean source) {
        String relation = SqlFlowChain.relationOf(columnRef);
        ColumnLink link = segment.getLink();
        Map<String, Object> json = new LinkedHashMap<String, Object>();
        json.put("id", columnId.get(columnRef));
        json.put("parentId", relationId.get(relation));
        json.put("parentName", nameOf(relation));
        json.put("column", SqlFlowChain.columnName(columnRef));
        json.put("qualifiedName", columnRef);
        boolean carries = source && (segment.getSteps() == 1 || segment.getStep() == 1);
        json.put("transforms", carries
                ? Collections.singletonList(transform(link))
                : Collections.emptyList());
        return json;
    }

    private Map<String, Object> transform(ColumnLink link) {
        String code = link.getTransform();
        Map<String, Object> json = new LinkedHashMap<String, Object>();
        json.put("code", code == null ? "" : code);
        json.put("type", direct(link) ? "simple"
                : link.getDerivation() == null ? "unknown"
                : link.getDerivation().name().toLowerCase(Locale.ROOT));
        json.put("coordinates", locate(context.sqlOf(link.getJobId()), code));
        return json;
    }

    private String nameOf(String relation) {
        GraphNode node = graph.getNode(relation);
        if (node != null) {
            return node.getName();
        }
        int bracket = relation.lastIndexOf(']');
        String bare = bracket < 0 ? relation : relation.substring(bracket + 1);
        int hash = bare.indexOf('#');
        int dot = bare.lastIndexOf('.');
        if (hash >= 0) {
            return bare.substring(hash + 1);
        }
        return dot < 0 ? bare : bare.substring(dot + 1);
    }

    /**
     * 表达式在原句里的行列：点节点定位到 SQL 的哪一段靠的就是这两个数。
     *
     * <p>找不到就返回空清单而不是猜一个：假坐标会把高亮指到别处，比不高亮更糟。
     */
    private static List<Map<String, Object>> locate(String sql, String code) {
        List<Map<String, Object>> points = new ArrayList<>();
        if (sql == null || code == null || code.isEmpty()) {
            return points;
        }
        int at = sql.indexOf(code);
        if (at < 0) {
            at = sql.toLowerCase(Locale.ROOT).indexOf(code.toLowerCase(Locale.ROOT));
        }
        if (at < 0) {
            return points;
        }
        points.add(point(sql, at));
        points.add(point(sql, at + code.length()));
        return points;
    }

    private static Map<String, Object> point(String sql, int offset) {
        int line = 1;
        int lineStart = 0;
        for (int i = 0; i < offset && i < sql.length(); i++) {
            if (sql.charAt(i) == '\n') {
                line++;
                lineStart = i + 1;
            }
        }
        Map<String, Object> point = new LinkedHashMap<String, Object>();
        point.put("x", line);
        point.put("y", offset - lineStart + 1);
        return point;
    }

    // ---------- 图、统计与会话 ----------

    private Map<String, Object> elements(SqlFlowLayout.Plan plan) {
        List<Map<String, Object>> tables = new ArrayList<>();
        Map<String, List<String>> listIdMap = new LinkedHashMap<String, List<String>>();
        for (SqlFlowLayout.PlacedBox box : plan.getBoxes()) {
            String relation = box.getBox().getRelation();
            Map<String, String> rowModelIds = new LinkedHashMap<String, String>();
            for (SqlFlowChain.Row row : box.getRows()) {
                rowModelIds.put(row.getColumnId(), columnId.get(row.getColumnId()));
            }
            tables.add(SqlFlowLayout.boxJson(box, relationId.get(relation), rowModelIds));
            one(listIdMap, box.getId(), relationId.get(relation));
            for (Map.Entry<String, String> row : box.getRowIds().entrySet()) {
                one(listIdMap, row.getValue(), columnId.get(row.getKey()));
            }
        }
        List<Map<String, Object>> edges = new ArrayList<>();
        for (SqlFlowLayout.PlacedEdge edge : plan.getEdges()) {
            edges.add(SqlFlowLayout.edgeJson(edge));
            String relationship = relationshipOf.get(edge.getSegment());
            if (relationship != null) {
                edgeRelationships.put(edge.getId(), Collections.singletonList(relationship));
            }
        }
        for (SqlFlowLayout.PlacedRelEdge rel : plan.getRelEdges()) {
            edges.add(relEdgeJson(rel));
        }
        Map<String, Object> graph = new LinkedHashMap<String, Object>();
        graph.put("elements", pair(tables, edges));
        graph.put("listIdMap", listIdMap);
        graph.put("relationshipIdMap", edgeRelationships);
        return graph;
    }

    private static void one(Map<String, List<String>> index, String key, String value) {
        if (key != null && value != null) {
            index.put(key, Collections.singletonList(value));
        }
    }

    /**
     * 表级关系边：自带端点与语句（不走 relationshipIdMap——它背后没有字段关系），
     * 前端点这条虚线时右栏直接给"哪对表、哪条语句"。
     */
    private Map<String, Object> relEdgeJson(SqlFlowLayout.PlacedRelEdge rel) {
        Map<String, Object> json = new LinkedHashMap<String, Object>();
        json.put("id", rel.getId());
        json.put("sourceId", rel.getSourceId());
        json.put("targetId", rel.getTargetId());
        json.put("synthetic", false);
        json.put("kind", "tableRel");
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("from", rel.getRel().getFrom());
        payload.put("to", rel.getRel().getTo());
        payload.put("jobId", rel.getRel().getJobId());
        payload.put("sqlText", context.sqlOf(rel.getRel().getJobId()));
        json.put("tableRel", payload);
        return json;
    }

    private static Map<String, Object> pair(List<Map<String, Object>> tables,
                                           List<Map<String, Object>> edges) {
        Map<String, Object> elements = new LinkedHashMap<String, Object>();
        elements.put("tables", tables);
        elements.put("edges", edges);
        return elements;
    }

    private Map<String, Object> summary(SqlFlowChain chain, Rows rows) {
        Map<String, Integer> touch = new LinkedHashMap<String, Integer>();
        for (SqlFlowChain.Segment segment : chain.getSegments()) {
            bump(touch, SqlFlowChain.relationOf(segment.getTo()));
        }
        Map<String, Object> summary = new LinkedHashMap<String, Object>();
        summary.put("table", count(chain, "table"));
        summary.put("view", count(chain, "view"));
        summary.put("column", rows.modelCount());
        summary.put("database", 0);
        summary.put("schema", 0);
        summary.put("process", processId.size());
        summary.put("relationship", groups.size());
        summary.put("mostRelationTables", mostTables(chain, touch));
        return summary;
    }

    private static int count(SqlFlowChain chain, String type) {
        int total = 0;
        for (SqlFlowChain.Box box : chain.getBoxes().values()) {
            if (type.equals(box.getType())) {
                total++;
            }
        }
        return total;
    }

    /** 谁被写进来的链路最多：顶栏"热点表"榜单，只数物理表——中间站人人都是中转 */
    private List<Map<String, Object>> mostTables(SqlFlowChain chain, Map<String, Integer> touch) {
        List<Map.Entry<String, Integer>> ranked = new ArrayList<>(touch.entrySet());
        Collections.sort(ranked, (left, right) -> right.getValue() - left.getValue());
        List<Map<String, Object>> most = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : ranked) {
            SqlFlowChain.Box box = chain.getBoxes().get(entry.getKey());
            if (box == null || box.isLocal()) {
                continue;
            }
            Map<String, Object> json = new LinkedHashMap<String, Object>();
            json.put("table", box.getName());
            json.put("relationCount", entry.getValue());
            most.add(json);
            if (most.size() == 5) {
                break;
            }
        }
        return most;
    }

    /** 会话标识：同一份快照 + 同一份输入必然得出同一个值，前端据此判断缓存是否过期 */
    private String sessionId() {
        String seed = context.getSource() + '|' + graph.size() + '|' + links.size() + '|' + focus;
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256")
                    .digest(seed.getBytes(StandardCharsets.UTF_8));
            StringBuilder out = new StringBuilder();
            for (int i = 0; i < 8; i++) {
                out.append(Character.forDigit((hash[i] >> 4) & 0xF, 16))
                        .append(Character.forDigit(hash[i] & 0xF, 16));
            }
            return out.toString();
        } catch (NoSuchAlgorithmException e) {
            return Integer.toHexString(seed.hashCode());
        }
    }

    private Map<String, Object> meta(SqlFlowChain chain, Rows rows, SqlFlowLayout.Plan plan,
                                     boolean drawn) {
        int local = 0;
        for (SqlFlowChain.Box box : chain.getBoxes().values()) {
            if (box.isLocal()) {
                local++;
            }
        }
        List<String> notices = new ArrayList<>();
        if (!drawn) {
            notices.add("表盒 " + chain.getBoxes().size() + " 个，超过 " + BOX_CAP
                    + " 上限：只给数据模型不画。请选中具体表或字段，或调小深度");
        }
        if (local > 0) {
            notices.add(local + " 个中间站（CTE、子查询、UNNEST 与聚合函数）已展开成盒子");
        }
        if (rows.droppedRows > 0) {
            notices.add(rows.droppedRows + " 列未画（每盒最多 " + rows.limit + " 行，模型里仍查得到）："
                    + "点字段行看单列链路，或换一张表为中心");
        }
        if (rows.droppedSegments > 0) {
            notices.add(rows.droppedSegments + " 段链路因两端列未画而暂不显示");
        }
        if (plan.getSkipped() > 0) {
            notices.add(plan.getSkipped() + " 段两端落在同一个盒子或同一行（自环），不画线");
        }
        if (!plan.getRelEdges().isEmpty()) {
            notices.add(plan.getRelEdges().size() + " 对表只有表级关系没有字段级血缘"
                + "（表级虚线）：点虚线看是哪条语句");
        }
        Set<String> parseError = new LinkedHashSet<String>();
        for (String jobId : processId.keySet()) {
            if (context.isParseError(jobId)) {
                parseError.add(jobId);
            }
        }
        Map<String, Object> meta = new LinkedHashMap<String, Object>();
        meta.put("source", context.getSource());
        meta.put("focus", focus);
        meta.put("boxCount", chain.getBoxes().size());
        meta.put("rowCount", rows.rowCount());
        meta.put("tableRelRows", rows.relRowCount());
        meta.put("modelRows", rows.modelCount());
        meta.put("segmentCount", chain.getSegments().size());
        meta.put("tableRelSegments", plan.getRelEdges().size());
        meta.put("drawn", drawn);
        meta.put("limit", rows.limit);
        meta.put("droppedRows", rows.droppedRows);
        meta.put("droppedSegments", rows.droppedSegments);
        meta.put("ranks", plan.getRanks());
        meta.put("cyclic", plan.getCyclic());
        meta.put("parseErrorJobs", parseError);
        meta.put("warning", String.join("；", notices));
        return meta;
    }

    /** 有哪些物理表是视图：只有 CREATE VIEW 写得出来的东西才配叫视图 */
    private Set<String> viewTargets() {
        Set<String> views = new LinkedHashSet<String>();
        for (ColumnLink link : links) {
            GraphNode node = graph.getNode(link.getToTable());
            if (node != null && !node.isLocal()
                    && SqlFlowStatement.of(context.sqlOf(link.getJobId())).producesView()) {
                views.add(node.getId());
            }
        }
        return views;
    }
}
