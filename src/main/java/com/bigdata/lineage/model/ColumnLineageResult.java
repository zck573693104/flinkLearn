package com.bigdata.lineage.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 列血缘提取结果
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ColumnLineageResult {
    
    /**
     * 源表名
     */
    private String sourceTable;
    
    /**
     * 源字段名
     */
    private String sourceColumn;
    
    /**
     * 目标表名
     */
    private String targetTable;
    
    /**
     * 目标字段名
     */
    private String targetColumn;
    
    /**
     * 转换表达式 (如：a + b as c)
     */
    private String transformation;
    
    /**
     * 置信度评分 (0.0-1.0)
     */
    private Double confidence;
    
    /**
     * 使用的血缘提取方法
     */
    private String extractionMethod;
}
