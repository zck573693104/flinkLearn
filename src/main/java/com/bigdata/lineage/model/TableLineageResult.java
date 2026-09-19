package com.bigdata.lineage.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Set;

/**
 * 表血缘提取结果
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableLineageResult {
    
    /**
     * 源表集合
     */
    private Set<String> sourceTables;
    
    /**
     * 目标表
     */
    private String targetTable;
    
    /**
     * 处理类型 (SELECT/INSERT/UPDATE/DELETE)
     */
    private String processType;
    
    /**
     * 置信度评分 (0.0-1.0)
     */
    private Double confidence;
    
    /**
     * 是否包含 CTE (WITH 子句)
     */
    private Boolean hasCte;
    
    /**
     * 是否包含时间关联 (Temporal Join)
     */
    private Boolean hasTemporalJoin;
    
    /**
     * 是否包含窗口函数
     */
    private Boolean hasWindowFunc;
    
    /**
     * 使用的血缘提取方法
     */
    private String extractionMethod;
    
    /**
     * 血缘特征列表
     */
    private List<String> features;
}
