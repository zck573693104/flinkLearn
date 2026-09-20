package com.bigdata.lineage.parser.model;

import lombok.Builder;
import lombok.Data;
import java.util.Set;

/**
 * 表级血缘关系模型
 */
@Data
@Builder
public class TableLineage {
    
    /**
     * 目标表（输出表）
     */
    private String targetTable;
    
    /**
     * 源表集合（输入表）
     */
    private Set<String> sourceTables;
    
    /**
     * 处理类型：INSERT, SELECT, CTAS, CREATE_VIEW
     */
    private String processType;
    
    /**
     * 插入模式：INTO, OVERWRITE
     */
    private String insertMode;
    
    /**
     * 解析置信度 (0.0 - 1.0)
     */
    private double confidence = 0.95;
    
    /**
     * 是否包含 CTE (WITH 子句)
     */
    private boolean hasCte = false;
    
    /**
     * 是否包含时间旅行 JOIN (FOR SYSTEM_TIME AS OF)
     */
    private boolean hasTemporalJoin = false;
    
    /**
     * 是否包含窗口函数 (TUMBLE, HOP, CUMULATE 等)
     */
    private boolean hasWindowFunc = false;
    
    /**
     * 解析过程是否存在语法错误（true 表示 ANTLR 错误恢复的部分结果，血缘可能缺表/错表）
     */
    private boolean parseError;
    
    /**
     * 原始 SQL 语句
     */
    private String originalSql;
    
    /**
     * 获取所有涉及的表（包括源表和目标表）
     */
    public Set<String> getAllTables() {
        Set<String> all = new java.util.HashSet<>();
        if (targetTable != null) {
            all.add(targetTable);
        }
        if (sourceTables != null) {
            all.addAll(sourceTables);
        }
        return all;
    }
}
