package com.bigdata.lineage.controller;

import com.bigdata.lineage.model.TableLineage;
import com.bigdata.lineage.service.TableLineageService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 表血缘 REST API 控制器
 */
public class TableLineageController {
    
    private final TableLineageService lineageService = new TableLineageService();
    
    /**
     * 记录血缘关系
     */
    public Map<String, Object> recordLineage(String jobId, String jobName, String sql) {
        
        Map<String, Object> result = new HashMap<>();
        try {
            TableLineage lineage = lineageService.recordLineage(jobId, jobName, sql);
            
            if (lineage != null) {
                result.put("success", true);
                result.put("data", lineage);
                result.put("message", "血缘记录成功");
            } else {
                result.put("success", false);
                result.put("message", "无法解析血缘关系");
            }
        } catch (Exception e) {
            result.put("success", false);
            result.put("message", "记录血缘失败：" + e.getMessage());
        }
        
        return result;
    }
    
    /**
     * 获取上游表列表
     */
    public Map<String, Object> getUpstreamTables(String tableName) {
        Map<String, Object> result = new HashMap<>();
        try {
            List<String> upstreams = lineageService.getUpstreamTables(tableName);
            result.put("success", true);
            result.put("tableName", tableName);
            result.put("type", "upstream");
            result.put("count", upstreams.size());
            result.put("tables", upstreams);
        } catch (Exception e) {
            result.put("success", false);
            result.put("message", "查询上游表失败：" + e.getMessage());
        }
        return result;
    }
    
    /**
     * 获取下游表列表
     */
    public Map<String, Object> getDownstreamTables(String tableName) {
        Map<String, Object> result = new HashMap<>();
        try {
            List<String> downstreams = lineageService.getDownstreamTables(tableName);
            result.put("success", true);
            result.put("tableName", tableName);
            result.put("type", "downstream");
            result.put("count", downstreams.size());
            result.put("tables", downstreams);
        } catch (Exception e) {
            result.put("success", false);
            result.put("message", "查询下游表失败：" + e.getMessage());
        }
        return result;
    }
    
    /**
     * 获取完整血缘链路
     */
    public Map<String, Object> getFullLineage(String tableName, int depth) {
        
        Map<String, Object> result = new HashMap<>();
        try {
            Map<String, List<String>> fullLineage = lineageService.getFullLineage(tableName, depth, new HashMap<>());
            
            result.put("success", true);
            result.put("tableName", tableName);
            result.put("depth", depth);
            result.put("lineage", fullLineage);
            result.put("nodeCount", fullLineage.size());
        } catch (Exception e) {
            result.put("success", false);
            result.put("message", "查询完整血缘失败：" + e.getMessage());
        }
        
        return result;
    }
    
    /**
     * 查询所有血缘记录
     */
    public Map<String, Object> getAllLineages() {
        Map<String, Object> result = new HashMap<>();
        try {
            // TODO: 从数据库查询所有记录
            result.put("success", true);
            result.put("count", 0);
            result.put("data", new HashMap<>());
        } catch (Exception e) {
            result.put("success", false);
            result.put("message", "查询所有血缘失败：" + e.getMessage());
        }
        return result;
    }
}
