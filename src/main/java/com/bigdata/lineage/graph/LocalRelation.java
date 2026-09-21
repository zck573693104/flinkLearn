package com.bigdata.lineage.graph;

/**
 * 语句内关系的标识。
 *
 * <p>CTE 名与 {@code #sub}/{@code #lat}/{@code #unnest} 伪节点都只在定义它的那条语句里唯一，
 * 而语料里的 CTE 名（{@code tmp}、{@code t}、{@code base}）在多条语句间反复复用：不带上语句
 * 标识就会把两条无关的链路接成一个节点。方括号前缀物理表名永远不可能有，查找时一眼可辨。
 */
final class LocalRelation {

    private LocalRelation() {
    }

    static String id(String jobId, String name) {
        return "[" + jobId + "]" + name;
    }

    static boolean isLocalId(String id) {
        return id.startsWith("[");
    }

    /** 去掉语句前缀，还原关系原名 */
    static String nameOf(String id) {
        int close = id.indexOf(']');
        return close < 0 ? id : id.substring(close + 1);
    }

    static String jobIdOf(String id) {
        int close = id.indexOf(']');
        return close < 0 ? null : id.substring(1, close);
    }
}
