package com.bigdata.lineage.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 表血缘关系实体
 * 记录表之间的输入输出关系
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableLineage {
    
    /**
     * 主键 ID
     */
    private Long id;
    
    /**
     * 作业 ID (Flink JobID)
     */
    private String jobId;
    
    /**
     * 作业名称
     */
    private String jobName;
    
    /**
     * SQL 语句
     */
    private String sql;
    
    /**
     * 上游表名 (源表)
     */
    private String sourceTable;
    
    /**
     * 下游表名 (目标表)
     */
    private String targetTable;
    
    /**
     * 处理类型 (SELECT/INSERT/UPDATE/DELETE/CREATE TABLE 等)
     */
    private String processType;
    
    /**
     * 血缘提取置信度评分 (0.0-1.0)
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
     * 创建时间
     */
    private LocalDateTime createTime;
    
    /**
     * 备注信息
     */
    private String remark;
}
