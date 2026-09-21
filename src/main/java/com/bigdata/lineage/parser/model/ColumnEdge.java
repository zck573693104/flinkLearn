package com.bigdata.lineage.parser.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * 一条字段级血缘边：某个目标字段在**一条语句的一个查询作用域内**的直接来源。
 *
 * <p>跨语句/跨层的链路不在解析期伪造，由图构建层把这些边按关系名串起来。
 */
@Data
@Builder
public class ColumnEdge {

    /** 目标关系：物理表全名、CTE 名或子查询伪节点名 */
    private String targetTable;

    private String targetColumn;

    /** 来源字段；常量列为空列表，表达式列可有多个 */
    private List<ColumnRef> sources;

    private ColumnDerivation derivation;

    /** 归一化后的表达式文本，如 concat(a, '-', b) */
    private String transform;

    /** SELECT 列表下标，用于位置对齐与证据定位 */
    private int ordinal;

    /** 语句标识，如 part3.sql#7；单独解析时为 null */
    private String jobId;

    /** FLINK / SPARK / PRESTO */
    private String engine;

    public double getConfidence() {
        return derivation == null ? 0.0 : derivation.getConfidence();
    }
}
