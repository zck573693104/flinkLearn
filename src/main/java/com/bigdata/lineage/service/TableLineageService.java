package com.bigdata.lineage.service;

import com.bigdata.lineage.model.TableLineage;
import com.bigdata.lineage.parser.MultiEngineSQLLineageParser;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 表血缘服务
 * 负责血缘关系的提取、存储和查询
 */
@Slf4j
public class TableLineageService {
    
    private final Map<String, List<TableLineage>> lineageCache = new HashMap<>();
    private final MultiEngineSQLLineageParser lineageParser = new MultiEngineSQLLineageParser();
    private Long nextId = 1L;
    
    /**
     * 记录血缘关系（基于 ANTLR4 语法的多引擎解析）
     * 
     * @param jobId Flink 作业 ID
     * @param jobName 作业名称
     * @param sql SQL 语句
     * @return 血缘记录
     */
    public TableLineage recordLineage(String jobId, String jobName, String sql) {
        try {
            List<com.bigdata.lineage.parser.model.TableLineage> results =
                    lineageParser.extractTableLineages(sql);
            
            if (results.isEmpty()) {
                log.warn("Cannot extract lineage from SQL: {}", sql);
                return null;
            }
            
            // 对于多语句/多源表场景，创建多个血缘记录
            List<TableLineage> lineages = new ArrayList<>();
            for (com.bigdata.lineage.parser.model.TableLineage result : results) {
                String targetTable = result.getTargetTable();
                Set<String> sourceTables = result.getSourceTables();
                if (sourceTables == null || sourceTables.isEmpty() || targetTable == null) {
                    continue;
                }
                
                for (String sourceTable : sourceTables) {
                    TableLineage lineage = TableLineage.builder()
                            .id(nextId++)
                            .jobId(jobId)
                            .jobName(jobName)
                            .sql(sql)
                            .sourceTable(sourceTable)
                            .targetTable(targetTable)
                            .processType(result.getProcessType())
                            .confidence(result.getConfidence())
                            .hasCte(result.isHasCte())
                            .hasTemporalJoin(result.isHasTemporalJoin())
                            .hasWindowFunc(result.isHasWindowFunc())
                            .createTime(LocalDateTime.now())
                            .build();
                    
                    // 缓存血缘关系
                    cacheLineage(lineage);
                    
                    log.info("Recorded lineage: {} -> {} (Job: {}, Type: {}, Confidence: {})", 
                            sourceTable, targetTable, jobId, lineage.getProcessType(), 
                            lineage.getConfidence());
                    
                    lineages.add(lineage);
                }
            }
            
            if (lineages.isEmpty()) {
                log.warn("Cannot extract complete lineage from SQL: {}", sql);
                return null;
            }
            
            return lineages.get(0); // 返回第一个作为代表
            
        } catch (Exception e) {
            log.error("Failed to record lineage for SQL: {}", sql, e);
            return null;
        }
    }
    
    /**
     * 缓存血缘关系
     */
    private void cacheLineage(TableLineage lineage) {
        String sourceTable = lineage.getSourceTable();
        String targetTable = lineage.getTargetTable();
        
        // 按源表索引
        lineageCache.computeIfAbsent(sourceTable, k -> new ArrayList<>()).add(lineage);
        
        // 按目标表索引
        lineageCache.computeIfAbsent(targetTable, k -> new ArrayList<>()).add(lineage);
    }
    
    /**
     * 获取上游表（源表）列表
     * 
     * @param targetTable 目标表名
     * @return 上游表列表
     */
    public List<String> getUpstreamTables(String targetTable) {
        List<TableLineage> lineages = lineageCache.getOrDefault(targetTable, new ArrayList<>());
        List<String> upstreams = new ArrayList<>();
        
        for (TableLineage lineage : lineages) {
            if (!lineage.getSourceTable().equals(targetTable)) {
                upstreams.add(lineage.getSourceTable());
            }
        }
        
        return upstreams;
    }
    
    /**
     * 获取下游表（目标表）列表
     * 
     * @param sourceTable 源表名
     * @return 下游表列表
     */
    public List<String> getDownstreamTables(String sourceTable) {
        List<TableLineage> lineages = lineageCache.getOrDefault(sourceTable, new ArrayList<>());
        List<String> downstreams = new ArrayList<>();
        
        for (TableLineage lineage : lineages) {
            if (!lineage.getTargetTable().equals(sourceTable)) {
                downstreams.add(lineage.getTargetTable());
            }
        }
        
        return downstreams;
    }
    
    /**
     * 获取完整血缘链路（递归）
     * 
     * @param tableName 表名
     * @param depth 最大深度
     * @param visited 已访问的表（防止循环）
     * @return 血缘链路
     */
    public Map<String, List<String>> getFullLineage(String tableName, int depth, 
                                                     Map<String, Boolean> visited) {
        if (depth <= 0 || visited.containsKey(tableName)) {
            return new HashMap<>();
        }
        
        visited.put(tableName, true);
        Map<String, List<String>> result = new HashMap<>();
        
        // 获取上游
        List<String> upstreams = getUpstreamTables(tableName);
        result.put(tableName + "_upstream", upstreams);
        
        // 递归获取上游的上游
        for (String upstream : upstreams) {
            result.putAll(getFullLineage(upstream, depth - 1, visited));
        }
        
        // 获取下游
        List<String> downstreams = getDownstreamTables(tableName);
        result.put(tableName + "_downstream", downstreams);
        
        // 递归获取下游的下游
        for (String downstream : downstreams) {
            result.putAll(getFullLineage(downstream, depth - 1, visited));
        }
        
        return result;
    }
    
    /**
     * 便捷方法：获取完整血缘链路（默认深度为 5）
     */
    public Map<String, List<String>> getFullLineage(String tableName) {
        return getFullLineage(tableName, 5, new HashMap<>());
    }
    
    /**
     * 清除所有缓存的血缘数据
     */
    public void clearCache() {
        lineageCache.clear();
        nextId = 1L;
    }
}
