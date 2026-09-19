package com.bigdata.lineage.parser.model;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 血缘关系图
 */
public class LineageGraph {
    
    /**
     * 边映射：targetTable -> sourceTables
     */
    private final Map<String, Set<String>> edges = new LinkedHashMap<>();
    
    /**
     * 添加边
     */
    public void addEdge(String target, Set<String> sources) {
        if (target == null || sources == null) {
            return;
        }
        edges.put(target, new HashSet<>(sources));
    }
    
    /**
     * 添加单条边
     */
    public void addEdge(String target, String source) {
        Set<String> sources = edges.computeIfAbsent(target, k -> new HashSet<>());
        sources.add(source);
    }
    
    /**
     * 获取目标表的所有源表
     */
    public Set<String> getSourceTables(String target) {
        return edges.getOrDefault(target, Collections.emptySet());
    }
    
    /**
     * 获取源表的所有目标表（下游）
     */
    public Set<String> getTargetTables(String source) {
        return edges.entrySet().stream()
            .filter(e -> e.getValue().contains(source))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }
    
    /**
     * 获取所有节点（表）
     */
    public Set<String> getAllTables() {
        Set<String> all = new HashSet<>(edges.keySet());
        edges.values().forEach(all::addAll);
        return all;
    }
    
    /**
     * 获取节点数量
     */
    public int getNodeCount() {
        return getAllTables().size();
    }
    
    /**
     * 获取边数量
     */
    public int getEdgeCount() {
        return edges.values().stream()
            .mapToInt(Set::size)
            .sum();
    }
    
    /**
     * 检查是否存在路径
     */
    public boolean hasPath(String source, String target) {
        if (source.equals(target)) return true;
        
        Set<String> visited = new HashSet<>();
        Queue<String> queue = new LinkedList<>();
        queue.add(source);
        
        while (!queue.isEmpty()) {
            String current = queue.poll();
            if (current.equals(target)) return true;
            if (visited.contains(current)) continue;
            
            visited.add(current);
            
            // 查找下游
            getTargetTables(current).forEach(t -> {
                if (!visited.contains(t)) {
                    queue.add(t);
                }
            });
        }
        
        return false;
    }
    
    /**
     * 获取上游链路（递归）
     */
    public Set<String> getUpstreamChain(String target) {
        Set<String> result = new HashSet<>();
        Queue<String> queue = new LinkedList<>();
        Set<String> visited = new HashSet<>();
        
        queue.add(target);
        
        while (!queue.isEmpty()) {
            String current = queue.poll();
            if (visited.contains(current)) continue;
            
            visited.add(current);
            
            getSourceTables(current).forEach(source -> {
                if (!result.contains(source)) {
                    result.add(source);
                    queue.add(source);
                }
            });
        }
        
        return result;
    }
    
    /**
     * 获取下游链路（递归）
     */
    public Set<String> getDownstreamChain(String source) {
        Set<String> result = new HashSet<>();
        Queue<String> queue = new LinkedList<>();
        Set<String> visited = new HashSet<>();
        
        queue.add(source);
        
        while (!queue.isEmpty()) {
            String current = queue.poll();
            if (visited.contains(current)) continue;
            
            visited.add(current);
            
            getTargetTables(current).forEach(target -> {
                if (!result.contains(target)) {
                    result.add(target);
                    queue.add(target);
                }
            });
        }
        
        return result;
    }
    
    /**
     * 检测循环依赖
     */
    public Set<String> detectCycles() {
        Set<String> cycles = new HashSet<>();
        
        for (String node : getAllTables()) {
            if (hasPath(node, node) && !node.equals(getSourceTables(node).iterator().next())) {
                cycles.add(node);
            }
        }
        
        return cycles;
    }
    
    /**
     * 获取边的映射
     */
    public Map<String, Set<String>> getEdges() {
        return Collections.unmodifiableMap(edges);
    }
    
    @Override
    public String toString() {
        return "LineageGraph{" +
               "nodes=" + getNodeCount() +
               ", edges=" + getEdgeCount() +
               "}";
    }
}
