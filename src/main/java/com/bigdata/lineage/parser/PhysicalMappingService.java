package com.bigdata.lineage.parser;

import com.bigdata.lineage.model.PhysicalMapping;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 物理映射服务
 * 
 * 用于从 SQL 或 Catalog 中提取物理映射信息
 */
@Slf4j
public class PhysicalMappingService {

    /**
     * 从 SQL 解析 connector options
     * 
     * @param sql CREATE TABLE 语句
     * @return 物理映射 Map，key 为逻辑表名
     */
    public Map<String, PhysicalMapping.PhysicalEntity> parseFromSql(String sql) {
        Map<String, PhysicalMapping.PhysicalEntity> mappings = new HashMap<>();
        
        try {
            // Extract physical mapping using PhysicalMapping utility
            PhysicalMapping.PhysicalEntity entity = 
                PhysicalMapping.extractFromCreateTableSql(sql);
            
            if (entity != null && entity.getLogicalTableName() != null) {
                mappings.put(entity.getLogicalTableName(), entity);
                log.debug("Extracted physical mapping for: {}", 
                         entity.getLogicalTableName());
            }
        } catch (Exception e) {
            log.warn("Failed to extract physical mapping from SQL: {}", e.getMessage());
        }
        
        return mappings;
    }
    
    /**
     * 批量解析多个 CREATE TABLE 语句
     */
    public Map<String, PhysicalMapping.PhysicalEntity> parseBatch(List<String> createStatements) {
        Map<String, PhysicalMapping.PhysicalEntity> allMappings = new HashMap<>();
        
        for (String sql : createStatements) {
            allMappings.putAll(parseFromSql(sql));
        }
        
        return allMappings;
    }
}
