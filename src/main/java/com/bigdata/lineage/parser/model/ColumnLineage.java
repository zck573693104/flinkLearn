package com.bigdata.lineage.parser.model;

import lombok.Builder;
import lombok.Data;
import java.util.List;

/**
 * 列级血缘关系模型
 */
@Data
@Builder
public class ColumnLineage {
    
    /**
     * 源表名
     */
    private String sourceTable;
    
    /**
     * 源列名
     */
    private String sourceColumn;
    
    /**
     * 目标表名（可能为 null，如 SELECT 语句）
     */
    private String targetTable;
    
    /**
     * 目标列名（可能为 null）
     */
    private String targetColumn;
    
    /**
     * 转换逻辑：direct_mapping, function:xxx, expression, star_mapping
     */
    private String transformation;
    
    /**
     * 解析置信度 (0.0 - 1.0)
     */
    private double confidence = 0.95;
    
    /**
     * 多源列列表（用于聚合函数或 JOIN 场景）
     */
    private List<String> sourceColumns;
    
    /**
     * 是否为主键映射
     */
    private boolean isPrimaryKey = false;
    
    /**
     * 是否为计算列
     */
    private boolean isComputed = false;
}
