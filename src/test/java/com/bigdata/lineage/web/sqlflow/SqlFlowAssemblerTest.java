package com.bigdata.lineage.web.sqlflow;

import com.bigdata.lineage.graph.ColumnGraphBuilder;
import com.bigdata.lineage.graph.LineageGraph;
import com.bigdata.lineage.parser.MultiEngineSQLLineageParser;
import com.bigdata.lineage.parser.model.TableLineage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * sqlflow 形状的响应：一次请求同时给出数据模型、坐标与统计，这里把三者的口径钉住。
 *
 * <p>断言只盯"前端能不能照着画"：坐标是否自洽（盒高对得上行数、行落在盒内）、图上标识能否反查
 * 模型（{@code listIdMap}）、中间站是否真的成了一个盒。解析口径本身不在这里测（那是 parser/graph 包）。
 */
class SqlFlowAssemblerTest {

    private static final MultiEngineSQLLineageParser PARSER = new MultiEngineSQLLineageParser();

    private static final String SIMPLE =
            "INSERT INTO dwd.order_detail SELECT order_id, amount FROM ods.orders;";

    private static final String CTE =
            "WITH picked AS (SELECT order_id AS order_id, amount AS amount FROM ods.orders) "
                    + "INSERT INTO dwd.big SELECT order_id FROM picked WHERE amount > 100";

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> list(Map<String, Object> holder, String key) {
        Object hit = holder.get(key);
        return (List<Map<String, Object>>) (hit == null ? new ArrayList<>() : hit);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Map<String, Object> holder, String key) {
        return (Map<String, Object>) holder.get(key);
    }

    private static Map<String, Object> payloadOf(String sql, String focus) {
        List<TableLineage> statements = PARSER.extractTableLineages(sql, false);
        LineageGraph graph = ColumnGraphBuilder.build(statements);
        Map<String, String> sqlByJob = new LinkedHashMap<String, String>();
        Set<String> errors = new LinkedHashSet<String>();
        for (int i = 0; i < statements.size(); i++) {
            String jobId = ColumnGraphBuilder.jobIdOf(statements.get(i), i);
            sqlByJob.put(jobId, statements.get(i).getOriginalSql());
            if (statements.get(i).isParseError()) {
                errors.add(jobId);
            }
        }
        return SqlFlowAssembler.payload(
                SqlFlowContext.adhoc("unit-test", graph, sqlByJob, errors),
                graph.getColumnLinks(), focus);
    }

    private static List<Map<String, Object>> boxes(Map<String, Object> payload) {
        return list(map(map(payload, "graph"), "elements"), "tables");
    }

    private static List<Map<String, Object>> edges(Map<String, Object> payload) {
        return list(map(map(payload, "graph"), "elements"), "edges");
    }

    private static List<Map<String, Object>> relationships(Map<String, Object> payload) {
        return list(map(payload, "sqlflow"), "relationships");
    }

    /** 数据模型里的三组实体合成一张表：盒落在 tables/views/others 哪一组不是这里的重点 */
    private static Collection<Map<String, Object>> modelEntities(Map<String, Object> payload) {
        Map<String, Map<String, Object>> byQualifiedName = new LinkedHashMap<>();
        for (Map<String, Object> server : list(map(map(payload, "sqlflow"), "dbobjs"), "servers")) {
            for (Map<String, Object> database : list(server, "databases")) {
                for (Map<String, Object> schema : list(database, "schemas")) {
                    for (String group : Arrays.asList("tables", "views", "others")) {
                        for (Map<String, Object> entity : list(schema, group)) {
                            byQualifiedName.put(String.valueOf(entity.get("qualifiedName")), entity);
                        }
                    }
                }
            }
        }
        return byQualifiedName.values();
    }

    private static Map<String, Object> modelOf(Map<String, Object> payload, String name) {
        for (Map<String, Object> entity : modelEntities(payload)) {
            if (name.equals(entity.get("name"))) {
                return entity;
            }
        }
        throw new AssertionError("模型里没有 " + name);
    }

    private static String label(Map<String, Object> box) {
        return String.valueOf(map(box, "label").get("content"));
    }

    private static Map<String, Object> boxNamed(Map<String, Object> payload, String name) {
        for (Map<String, Object> box : boxes(payload)) {
            if (name.equals(label(box))) {
                return box;
            }
        }
        throw new AssertionError("图上没有这个盒，实际为 " + labels(payload));
    }

    private static List<String> labels(Map<String, Object> payload) {
        List<String> names = new ArrayList<>();
        for (Map<String, Object> box : boxes(payload)) {
            names.add(label(box));
        }
        return names;
    }

    private static List<String> rowNames(Map<String, Object> box) {
        List<String> names = new ArrayList<>();
        for (Map<String, Object> row : list(box, "columns")) {
            names.add(String.valueOf(map(row, "label").get("content")));
        }
        return names;
    }

    private static double num(Map<String, Object> json, String key) {
        return ((Number) json.get(key)).doubleValue();
    }

    /** 一次调用同时给模型、坐标与统计：前端不需要第二次请求就能画出图 */
    @Test
    void oneResponseCarriesModelGeometryAndSummary() {
        Map<String, Object> payload = payloadOf(SIMPLE, null);

        assertNotNull(payload.get("sqlflow"));
        assertNotNull(payload.get("graph"));
        assertNotNull(payload.get("summary"));
        assertNotNull(payload.get("sessionId"));
        assertNotNull(payload.get("metaInfo"));
        Map<String, Object> sqlflow = map(payload, "sqlflow");
        assertTrue(sqlflow.containsKey("dbobjs"));
        assertTrue(sqlflow.containsKey("processes"));
        assertTrue(sqlflow.containsKey("relationships"));
        assertEquals(2, boxes(payload).size(), "一张源表一张目标表");
        assertEquals(2, edges(payload).size(), "两个字段两行线");
    }

    /** 盒高必须由行数算出来：前端不再量字，对不上就是行溢出盒外 */
    @Test
    void boxGeometryFollowsRowCount() {
        Map<String, Object> payload = payloadOf(SIMPLE, null);

        for (Map<String, Object> box : boxes(payload)) {
            List<Map<String, Object>> rows = list(box, "columns");
            assertEquals(SqlFlowLayout.BOX_PAD + rows.size() * SqlFlowLayout.ROW_H,
                    num(box, "height"), 0.001, "盒高与行数对不上：" + label(box));
            assertEquals(SqlFlowLayout.BOX_W, num(box, "width"), 0.001);
            for (int i = 0; i < rows.size(); i++) {
                Map<String, Object> row = rows.get(i);
                assertEquals(num(box, "x") + 1, num(row, "x"), 0.001, "行没贴着盒左边");
                assertEquals(num(box, "y") + SqlFlowLayout.ROW_TOP + i * SqlFlowLayout.ROW_H,
                        num(row, "y"), 0.001, "第 " + i + " 行没落在盒内");
                assertEquals(SqlFlowLayout.ROW_W, num(row, "width"), 0.001);
                assertEquals(SqlFlowLayout.ROW_H, num(row, "height"), 0.001);
                assertNotNull(row.get("modelId"), "行没带模型标识，前端反查不到列");
            }
        }
    }

    /**
     * 几何里必须带着可寻址的关系标识。
     *
     * <p>点盒顶表名要"以这张表为中心重开一片"，前端手里只有短名（{@code name}）时同名短名解不出来，
     * 那个手势会静默降级成列字段清单——不抛异常、不告警，所以只能由这一条钉住。
     */
    @Test
    void geometryCarriesTheAddressableRelationName() {
        Map<String, Object> payload = payloadOf(SIMPLE, null);
        Map<String, Object> ranks = map(map(payload, "metaInfo"), "ranks");

        for (Map<String, Object> box : boxes(payload)) {
            String relation = String.valueOf(box.get("qualifiedName"));
            assertTrue(ranks.containsKey(relation),
                    "盒子的 qualifiedName 不是图上的关系标识，前端拿它打不开那张表：" + box.get("id"));
            for (Map<String, Object> row : list(box, "columns")) {
                assertTrue(String.valueOf(row.get("qualifiedName")).startsWith(relation + "."),
                        "行的 qualifiedName 对不上它所在的盒：" + row.get("id"));
            }
        }
    }

    /** 自依赖（INSERT OVERWRITE 的输入里有自己）两端重合，画不出线：必须说，不能让段数与线数悄悄对不平 */
    @Test
    void selfDependencyDrawsNoLoopButSaysSo() {
        Map<String, Object> payload = payloadOf(
                "INSERT INTO dwd.report SELECT amount FROM dwd.report;", null);

        assertTrue(edges(payload).isEmpty(), "两端重合的段不该画成线：" + edges(payload));
        String warning = String.valueOf(map(payload, "metaInfo").get("warning"));
        assertTrue(warning.contains("自环"), "线少了却没说为什么，就是让用户自己猜：" + warning);
    }

    /** 数据流方向必须与层号一致：下游盒永远在右，图例的"第几层"才不跟图吵架 */
    @Test
    void downstreamBoxSitsToTheRight() {
        Map<String, Object> payload = payloadOf(
                "INSERT INTO dwd.mid SELECT id FROM ods.src;\n"
                        + "INSERT INTO dws.fin SELECT id FROM dwd.mid;", null);

        Map<String, Object> src = boxNamed(payload, "src");
        Map<String, Object> mid = boxNamed(payload, "mid");
        Map<String, Object> fin = boxNamed(payload, "fin");
        assertTrue(num(src, "x") < num(mid, "x"), "第一跳没在左边");
        assertTrue(num(mid, "x") < num(fin, "x"), "第二跳没在右边");
        assertEquals(0, ((Number) src.get("layer")).intValue());
        assertEquals(2, ((Number) fin.get("layer")).intValue());
    }

    /** CTE 是链路上真实的一站：它自己成盒、被标成中间关系，而不是折进一条直连边 */
    @Test
    void cteBecomesItsOwnStationBox() {
        Map<String, Object> payload = payloadOf(CTE, null);

        assertTrue(labels(payload).contains("picked"), "CTE 没显形：" + labels(payload));
        Map<String, Object> station = modelOf(payload, "picked");
        assertEquals("cte", station.get("type"));
        assertEquals(Boolean.TRUE, station.get("local"));
        assertEquals(5, ((Number) map(payload, "summary").get("column")).intValue(),
                "源表 2 列 + CTE 2 列 + 目标 1 列");

        Set<String> pickedRows = new LinkedHashSet<>();
        for (Map<String, Object> row : list(boxNamed(payload, "picked"), "columns")) {
            pickedRows.add(String.valueOf(row.get("id")));
        }
        boolean inbound = false;
        boolean outbound = false;
        for (Map<String, Object> edge : edges(payload)) {
            inbound |= pickedRows.contains(String.valueOf(edge.get("targetId")));
            outbound |= pickedRows.contains(String.valueOf(edge.get("sourceId")));
        }
        assertTrue(inbound, "没有线接进 CTE 盒");
        assertTrue(outbound, "没有线从 CTE 盒接出去");
    }

    /** 聚合的中间站是显示层造的：落在函数盒上的那一跳必须标 synthetic */
    @Test
    void aggregateGetsAFunctionStation() {
        Map<String, Object> payload = payloadOf(
                "INSERT INTO dws.total SELECT sum(amount) AS total FROM ods.orders;", null);

        assertTrue(labels(payload).contains("SUM"), "聚合函数没有中间站：" + labels(payload));
        assertEquals("function", modelOf(payload, "SUM").get("type"));
        for (Map<String, Object> edge : edges(payload)) {
            assertEquals(Boolean.TRUE, edge.get("synthetic"),
                    "函数盒两端那一跳没标 synthetic，前端会把它画成解析出的事实");
        }
    }

    /** listIdMap / relationshipIdMap 是前端唯一的反查路径，指到不存在的东西等于白指 */
    @Test
    void idMapsResolveIntoTheModel() {
        Map<String, Object> payload = payloadOf(CTE, null);
        Map<String, Object> graph = map(payload, "graph");
        Set<String> modelIds = new LinkedHashSet<>();
        for (Map<String, Object> entity : modelEntities(payload)) {
            modelIds.add(String.valueOf(entity.get("id")));
            for (Map<String, Object> column : list(entity, "columns")) {
                modelIds.add(String.valueOf(column.get("id")));
            }
        }
        Set<String> relationshipIds = new LinkedHashSet<>();
        for (Map<String, Object> relationship : relationships(payload)) {
            relationshipIds.add(String.valueOf(relationship.get("id")));
        }

        Map<String, Object> listIdMap = map(graph, "listIdMap");
        assertFalse(listIdMap.isEmpty());
        for (Map.Entry<String, Object> entry : listIdMap.entrySet()) {
            assertTrue(modelIds.containsAll((Collection<String>) entry.getValue()),
                    "图上 " + entry.getKey() + " 指向了模型外的对象");
        }
        for (Map.Entry<String, Object> entry : map(graph, "relationshipIdMap").entrySet()) {
            assertTrue(relationshipIds.containsAll((Collection<String>) entry.getValue()),
                    "边 " + entry.getKey() + " 指向了不存在的关系");
        }
        for (Map<String, Object> box : boxes(payload)) {
            assertTrue(listIdMap.containsKey(String.valueOf(box.get("id"))), "盒没有 listIdMap 条目");
            for (Map<String, Object> row : list(box, "columns")) {
                assertTrue(listIdMap.containsKey(String.valueOf(row.get("id"))), "行没有条目");
            }
        }
        for (Map<String, Object> box : boxes(payload)) {
            assertTrue(modelIds.contains(String.valueOf(box.get("modelId"))), "盒的 modelId 不在模型里");
        }
    }

    /** 关系上的证据不能因为折叠而丢：加工方式、置信度、语句、中间跳都得在 */
    @Test
    void relationshipsKeepTheirEvidence() {
        Map<String, Object> payload = payloadOf(CTE, null);

        assertFalse(relationships(payload).isEmpty());
        for (Map<String, Object> relationship : relationships(payload)) {
            assertNotNull(relationship.get("id"));
            assertNotNull(relationship.get("effectType"));
            assertNotNull(relationship.get("processId"), "关系没挂语句，SQL 高亮无从定位");
            assertNotNull(relationship.get("confidence"));
            assertFalse(list(relationship, "sources").isEmpty());

            Map<String, Object> target = map(relationship, "target");
            assertNotNull(target.get("parentId"), "目标行没有 parentId，前端找不到它属于哪个盒");
            assertNotNull(target.get("column"));
            for (Map<String, Object> source : list(relationship, "sources")) {
                assertNotNull(source.get("parentId"));
                assertNotNull(source.get("column"));
                assertNotNull(source.get("qualifiedName"));
            }
        }
    }

    /** 表达式只挂一次：多段链路把同一段 SQL 重复三遍，读的人分不清它属于哪一跳 */
    @Test
    void transformIsAttachedToExactlyOneHop() {
        Map<String, Object> payload = payloadOf(CTE, null);

        for (Map<String, Object> relationship : relationships(payload)) {
            int withCode = 0;
            for (Map<String, Object> source : list(relationship, "sources")) {
                withCode += list(source, "transforms").isEmpty() ? 0 : 1;
            }
            assertEquals(1, withCode, "同一条关系的表达式挂在了不止一跳上");
        }
    }

    /** 语句实体带着原文：点节点看 SQL、按行列号高亮，行列号只能从原文算 */
    @Test
    void processCarriesTheStatementText() {
        Map<String, Object> payload = payloadOf(SIMPLE, null);
        List<Map<String, Object>> processes = list(map(payload, "sqlflow"), "processes");

        assertEquals(1, processes.size());
        assertEquals("insert", processes.get(0).get("type"), "语句动作写在 type 上（对方口径）");
        assertEquals(Boolean.FALSE, processes.get(0).get("parseError"));
        Map<String, Object> transform = list(processes.get(0), "transforms").get(0);
        assertEquals("statement", transform.get("type"));
        assertTrue(String.valueOf(transform.get("code")).contains("dwd.order_detail"));
    }

    /** 表达式的行列号让"点字段定位到 SQL"成立；找不到就留空而不是编一个位置 */
    @Test
    void transformCoordinatesPointIntoTheStatement() {
        Map<String, Object> payload = payloadOf(
                "INSERT INTO dws.total SELECT amount + 1 AS next_amount FROM ods.orders;", null);

        Map<String, Object> source = list(relationships(payload).get(0), "sources").get(0);
        List<Map<String, Object>> coordinates = list(
                list(source, "transforms").get(0), "coordinates");
        assertEquals(2, coordinates.size());
        assertEquals(1, ((Number) coordinates.get(0).get("x")).intValue(), "行号从 1 起");
        assertTrue(((Number) coordinates.get(0).get("y")).intValue() > 1);
    }

    /** CREATE VIEW 写出来的物理表是视图，不是表：图例里这两类必须分得开 */
    @Test
    void createViewTargetIsTypedAsView() {
        Map<String, Object> payload = payloadOf(
                "CREATE VIEW dw.customer_revenue AS SELECT customer_id FROM dw.orders;", null);

        assertEquals("view", modelOf(payload, "customer_revenue").get("type"));
        assertEquals(1, ((Number) map(payload, "summary").get("view")).intValue());
    }

    /** 统计必须和响应里剩下的两样东西对得上：画布上的盒数、模型里的列数 */
    @Test
    void summaryMatchesThePaintedGraph() {
        Map<String, Object> payload = payloadOf(CTE, null);
        Map<String, Object> summary = map(payload, "summary");

        int physical = 0;
        for (Map<String, Object> box : boxes(payload)) {
            if (!Boolean.TRUE.equals(box.get("local"))) {
                physical++;
            }
        }
        int inModel = 0;
        for (Map<String, Object> entity : modelEntities(payload)) {
            inModel += list(entity, "columns").size();
        }
        assertEquals(physical, ((Number) summary.get("table")).intValue()
                + ((Number) summary.get("view")).intValue());
        assertEquals(inModel, ((Number) summary.get("column")).intValue());
        assertEquals(boxes(payload).size(),
                ((Number) map(payload, "metaInfo").get("boxCount")).intValue());
        assertEquals(relationships(payload).size(),
                ((Number) summary.get("relationship")).intValue());
    }

    /** 同一份输入两次装配必须长出同一套标识，否则前端缓存与快照比对没有意义 */
    @Test
    void payloadIsDeterministic() {
        assertEquals(payloadOf(CTE, null), payloadOf(CTE, null));
    }

    /** 焦点列决定"哪一行一定看得见"：宽表超预算时它排在最前，其余按名字挤 */
    @Test
    void focusRowSurvivesTheBudget() {
        Map<String, Object> plain = payloadOf(WIDE, null);
        Map<String, Object> focused = payloadOf(WIDE, "dws.wide.c29");

        List<String> dropped = rowNames(boxNamed(plain, "wide"));
        assertFalse(dropped.contains("c29"), "没焦点时 c29 本来会被预算挤掉，测试前提不成立");
        assertTrue(dropped.size() <= SqlFlowAssembler.ROWS_CAP);
        assertTrue(rowNames(boxNamed(focused, "wide")).contains("c29"), "焦点列被预算挤掉了");
    }

    /** 没画进盒子的列照样在模型里：右栏的"列清单"要列得出它，但前端不能给它编坐标 */
    @Test
    void unpaintedColumnsSurviveInTheModel() {
        Map<String, Object> payload = payloadOf(WIDE, null);
        Map<String, Object> entity = modelOf(payload, "wide");

        List<Map<String, Object>> columns = list(entity, "columns");
        assertEquals(30, ((Number) entity.get("columnCount")).intValue());
        assertEquals(30, columns.size(), "模型把没画的列一起抹掉了，列清单就只剩画出来的那几个");
        assertEquals(0, ((Number) entity.get("hiddenColumns")).intValue());
        int painted = 0;
        for (Map<String, Object> column : columns) {
            painted += Boolean.TRUE.equals(column.get("uiVisible")) ? 1 : 0;
        }
        assertEquals(rowNames(boxNamed(payload, "wide")).size(), painted);
        int onCanvas = 0;
        for (Map<String, Object> box : boxes(payload)) {
            onCanvas += list(box, "columns").size();
        }
        assertEquals(onCanvas, ((Number) map(payload, "metaInfo").get("rowCount")).intValue());
        int listed = 0;
        for (Map<String, Object> row : modelEntities(payload)) {
            listed += list(row, "columns").size();
        }
        assertEquals(listed, ((Number) map(payload, "summary").get("column")).intValue());
        assertTrue(onCanvas < listed, "画布比模型还全，uiVisible 就没有存在的意义了");
    }

    /** 超过盒数上限时不画，但模型与告警照给：宁可只给清单，也不把浏览器当渲染靶子 */
    @Test
    void overCapDropsGeometryButKeepsTheModel() {
        StringBuilder sql = new StringBuilder();
        for (int i = 0; i <= SqlFlowAssembler.BOX_CAP; i++) {
            sql.append("INSERT INTO db.t").append(i).append(" SELECT c FROM db.s").append(i)
                    .append(";\n");
        }
        Map<String, Object> payload = payloadOf(sql.toString(), null);

        assertTrue(boxes(payload).isEmpty(), "超上限还落位，等于让浏览器画上千个盒");
        assertTrue(edges(payload).isEmpty());
        assertFalse(modelEntities(payload).isEmpty(), "不画也不能不给模型");
        assertEquals(Boolean.FALSE, map(payload, "metaInfo").get("drawn"));
        assertTrue(String.valueOf(map(payload, "metaInfo").get("warning")).contains("上限"));
    }

    /** 空输入不能抛：快照还没扫完时前端就会来问一次 */
    @Test
    void emptyLinksStillProduceAShapedPayload() {
        Map<String, Object> payload = SqlFlowAssembler.payload(
                SqlFlowContext.adhoc("unit-test", LineageGraph.empty(),
                        new LinkedHashMap<String, String>(), new LinkedHashSet<String>()),
                new ArrayList<>(), null);

        assertTrue(boxes(payload).isEmpty());
        assertEquals(0, ((Number) map(payload, "summary").get("table")).intValue());
        assertNull(map(payload, "metaInfo").get("focus"));
    }

    private static final String WIDE = wide();

    private static String wide() {
        StringBuilder sql = new StringBuilder("INSERT INTO dws.wide SELECT ");
        for (int i = 0; i < 30; i++) {
            sql.append(i == 0 ? "" : ", ").append("c").append(i).append(" AS c").append(i);
        }
        return sql.append(" FROM ods.src;").toString();
    }
}
