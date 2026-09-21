package com.bigdata.lineage.graph;

import com.bigdata.lineage.parser.model.ColumnDerivation;

import java.util.Collections;
import java.util.List;

/**
 * 折叠后的字段级边：两端都是图里真实存在的列节点，中间跳（伪节点）留在 {@link #getHops()} 里。
 *
 * <p>解析层每条语句只产出"直接来源"，子查询/展开这一层中间节点不对用户暴露，
 * 由 {@link ColumnGraphBuilder} 串成一跳并把加工方式取整条链上置信度最低的那一种。
 */
public final class ColumnLink {

    private final String fromTable;
    private final String fromColumn;
    private final String toTable;
    private final String toColumn;
    private final ColumnDerivation derivation;
    private final String jobId;
    private final String engine;
    private final int ordinal;
    private final String transform;
    private final List<String> hops;

    ColumnLink(String fromTable, String fromColumn, String toTable, String toColumn,
               ColumnDerivation derivation, String jobId, String engine, int ordinal,
               String transform, List<String> hops) {
        this.fromTable = fromTable;
        this.fromColumn = fromColumn;
        this.toTable = toTable;
        this.toColumn = toColumn;
        this.derivation = derivation;
        this.jobId = jobId;
        this.engine = engine;
        this.ordinal = ordinal;
        this.transform = transform;
        this.hops = Collections.unmodifiableList(hops);
    }

    static String columnId(String table, String column) {
        return table == null || table.isEmpty() ? column : table + "." + column;
    }

    public String getFromTable() {
        return fromTable;
    }

    public String getFromColumn() {
        return fromColumn;
    }

    public String getToTable() {
        return toTable;
    }

    public String getToColumn() {
        return toColumn;
    }

    public String getFrom() {
        return columnId(fromTable, fromColumn);
    }

    public String getTo() {
        return columnId(toTable, toColumn);
    }

    public ColumnDerivation getDerivation() {
        return derivation;
    }

    public double getConfidence() {
        return derivation == null ? 0.0 : derivation.getConfidence();
    }

    /** 所属语句标识，{@code 文件名#序号}；单独解析一条语句时是 {@code stmt0} 之类的占位 */
    public String getJobId() {
        return jobId;
    }

    public String getEngine() {
        return engine;
    }

    /** 目标列在 SELECT 列表里的位置，用于同一目标列多来源时保持书写顺序 */
    public int getOrdinal() {
        return ordinal;
    }

    /** 目标列表达式的原句片段 */
    public String getTransform() {
        return transform;
    }

    /** 从来源列到目标列的完整一跳链路，含被折掉的伪节点 */
    public List<String> getHops() {
        return hops;
    }
}
