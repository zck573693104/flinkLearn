package com.bigdata.lineage.parser;

import com.bigdata.lineage.parser.model.ColumnDerivation;
import com.bigdata.lineage.parser.model.ColumnEdge;
import com.bigdata.lineage.parser.model.ColumnRef;
import com.bigdata.lineage.parser.model.TableLineage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 字段级血缘断言夹具（三种方言共用）。
 *
 * <p>每次先跑表级提取确认没有语法错误：语法缺口会让列级提取静默产出空边，
 * 只看边数会把"解析不出来"误判成"没有血缘"。
 */
final class ColumnAssert {

    private final String sql;
    private final List<ColumnEdge> edges;

    private ColumnAssert(String sql, List<ColumnEdge> edges) {
        this.sql = sql;
        this.edges = edges;
    }

    static ColumnAssert of(String sql, Function<String, TableLineage> tableLevel,
                           Function<String, List<ColumnEdge>> columnLevel) {
        TableLineage lineage = tableLevel.apply(sql);
        assertNotNull(lineage, "表级解析结果不应为 null: " + sql);
        assertFalse(lineage.isParseError(), "语法不应产生解析错误: " + sql);
        List<ColumnEdge> extracted = columnLevel.apply(sql);
        assertNotNull(extracted, "字段级结果不应为 null: " + sql);
        ColumnAssert asserted = new ColumnAssert(sql, extracted);
        // 每条用例都先过一遍泄漏体检：作用域一旦串台，这里立刻红，
        // 不用等到断言某个具体列才发现边数不对
        asserted.noGhostColumns();
        return asserted;
    }

    List<ColumnEdge> edges() {
        return edges;
    }

    ColumnAssert count(int expected) {
        assertEquals(expected, edges.size(), "边数量不匹配，实际:\n" + describe());
        return this;
    }

    ColumnAssert sources(String table, String column, String... nodeIds) {
        ColumnEdge edge = edge(table, column);
        List<String> actual = new ArrayList<>();
        for (ColumnRef ref : edge.getSources()) {
            actual.add(ref.nodeId());
        }
        assertEquals(Arrays.asList(nodeIds), actual,
                "边 " + table + "." + column + " 的来源不匹配:\n" + describe());
        return this;
    }

    ColumnAssert derivation(String table, String column, ColumnDerivation expected) {
        ColumnEdge edge = edge(table, column);
        assertEquals(expected, edge.getDerivation(),
                "边 " + table + "." + column + " 的加工方式不匹配:\n" + describe());
        return this;
    }

    ColumnAssert ordinal(String table, String column, int expected) {
        ColumnEdge edge = edge(table, column);
        assertEquals(expected, edge.getOrdinal(), "边 " + table + "." + column + " 的位置不匹配");
        return this;
    }

    ColumnAssert transformContains(String table, String column, String fragment) {
        ColumnEdge edge = edge(table, column);
        assertNotNull(edge.getTransform(), "transform 不应为 null");
        assertTrue(edge.getTransform().toLowerCase(Locale.ROOT).contains(fragment),
                "transform 应保留原句片段，实际: " + edge.getTransform());
        return this;
    }

    ColumnAssert qualifierOf(String table, String column, String expected) {
        ColumnEdge edge = edge(table, column);
        assertFalse(edge.getSources().isEmpty(), "边没有来源: " + table + "." + column);
        assertEquals(expected, edge.getSources().get(0).getQualifier(),
                "原始限定符应保留给 UI 展示");
        return this;
    }

    /** 返回以指定前缀命名的伪关系目标（{@code #sub1}、{@code #lat1}…） */
    String pseudoTable(String prefix) {
        for (ColumnEdge edge : edges) {
            if (edge.getTargetTable().startsWith(prefix)) {
                return edge.getTargetTable();
            }
        }
        fail("缺少前缀为 " + prefix + " 的伪关系，实际:\n" + describe());
        return null;
    }

    /**
     * 泄漏体检：每个端点的列名必须在原句里真实出现，伪节点与占位列除外。
     * 这是列级版本的"幽灵表"检查——作用域一旦串台，这里就会红。
     */
    ColumnAssert noGhostColumns() {
        String lower = sql.toLowerCase(Locale.ROOT);
        for (ColumnEdge edge : edges) {
            assertAppears(lower, edge.getTargetTable(), edge.getTargetColumn());
            for (ColumnRef ref : edge.getSources()) {
                assertAppears(lower, ref.getBoundTable(), ref.getColumn());
            }
        }
        return this;
    }

    private void assertAppears(String lowerSql, String table, String column) {
        if (table == null || table.startsWith("#") || column == null
                || !column.matches("\\w+") || column.startsWith("expr_")) {
            return;
        }
        assertTrue(lowerSql.contains(column),
                "列名 " + column + " 不在原句中，疑似作用域泄漏: " + lowerSql);
    }

    ColumnEdge edge(String table, String column) {
        for (ColumnEdge edge : edges) {
            if (table.equals(edge.getTargetTable()) && column.equals(edge.getTargetColumn())) {
                return edge;
            }
        }
        fail("缺少目标边 " + table + "." + column + "，实际:\n" + describe());
        return null;
    }

    List<ColumnEdge> toTargets(String table) {
        List<ColumnEdge> hit = new ArrayList<>();
        for (ColumnEdge edge : edges) {
            if (table.equals(edge.getTargetTable())) {
                hit.add(edge);
            }
        }
        return hit;
    }

    String describe() {
        StringBuilder sb = new StringBuilder();
        for (ColumnEdge edge : edges) {
            sb.append(edge.getTargetTable()).append('.').append(edge.getTargetColumn())
                    .append('[').append(edge.getDerivation()).append("] <- ")
                    .append(edge.getSources()).append('\n');
        }
        return sb.toString();
    }
}
