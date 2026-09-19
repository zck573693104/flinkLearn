package com.bigdata.lineage.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 表信息实体
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableInfo {
    
    /**
     * 主键 ID
     */
    private Long id;
    
    /**
     * 表名
     */
    private String tableName;
    
    /**
     * 数据库名称 (可选)
     */
    private String databaseName;
    
    /**
     * 表类型 (TABLE/VIEW/TEMPORARY_TABLE)
     */
    private String tableType;
    
    /**
     * 创建时间
     */
    private LocalDateTime createTime;
    
    /**
     * 备注
     */
    private String remark;
}
