package com.bigdata.lineage.parser;

import com.bigdata.lineage.model.PhysicalMapping;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 统一血缘提取器
 * 
 * 整合 Route A (表级) 和 Route B (列级)
 * 提供完整的血缘提取能力
 * 
 * 架构层：Integration Layer
 */
@Slf4j
public class UnifiedLineageExtractor {

    private final FlinkSQLLineageExtractor tableExtractor;
    private final ColumnLineageExtractor columnExtractor;
    private final PhysicalMappingService physicalMappingService;

    public UnifiedLineageExtractor() {
        this.tableExtractor = new FlinkSQLLineageExtractor();
        this.columnExtractor = new ColumnLineageExtractor();
        this.physicalMappingService = new PhysicalMappingService();
    }

    /**
     * 提取完整血缘信息（表级 + 列级）
     */
    public LineageAnalysisResult analyze(String flinkSql) {
        LineageAnalysisResult result = new LineageAnalysisResult();
        result.setSql(flinkSql);

        // Extract table-level lineage
        List<FlinkSQLLineageExtractor.TableLineageResult> tableLineages = 
            tableExtractor.extractTableLineages(flinkSql);
        
        if (!tableLineages.isEmpty()) {
            result.setTableLineages(tableLineages);
            
            // Add physical mapping for each table
            for (FlinkSQLLineageExtractor.TableLineageResult tableLineage : tableLineages) {
                addPhysicalMappings(tableLineage);
            }
        } else {
            log.warn("No table lineage found");
        }

        // Extract column-level lineage
        List<ColumnLineageExtractor.ColumnLineageResult> columnLineages = 
            columnExtractor.extractColumnLineages(flinkSql);
        
        if (!columnLineages.isEmpty()) {
            result.setColumnLineages(columnLineages);
        } else {
            log.debug("No column lineage found (may be expected for simple queries)");
        }

        return result;
    }

    /**
     * 添加物理映射信息
     * 
     * TODO: Currently a placeholder. In production, this should:
     * 1. Extract CREATE TABLE statements from the SQL or catalog
     * 2. Parse WITH options to identify connector type
     * 3. Build PhysicalEntity for each table
     */
    private void addPhysicalMappings(FlinkSQLLineageExtractor.TableLineageResult tableLineage) {
        // Placeholder implementation
        // In production, integrate with Flink Catalog API
        log.debug("Physical mapping for target: {}", tableLineage.getTargetTable());
    }

    /**
     * 批量分析多个 SQL 语句
     */
    public List<LineageAnalysisResult> analyzeBatch(List<String> sqls) {
        return sqls.stream()
            .map(this::analyze)
            .collect(Collectors.toList());
    }

    /**
     * 获取上游表列表
     */
    public Set<String> getUpstreamTables(String targetTable, List<LineageAnalysisResult> results) {
        Set<String> upstreams = new HashSet<>();
        
        for (LineageAnalysisResult result : results) {
            if (result.getTargetTables().contains(targetTable)) {
                upstreams.addAll(result.getSourceTables());
            }
        }
        
        return upstreams;
    }

    /**
     * 获取下游表列表
     */
    public Set<String> getDownstreamTables(String sourceTable, List<LineageAnalysisResult> results) {
        Set<String> downstreams = new HashSet<>();
        
        for (LineageAnalysisResult result : results) {
            if (result.getSourceTables().contains(sourceTable)) {
                downstreams.addAll(result.getTargetTables());
            }
        }
        
        return downstreams;
    }

    /**
     * 构建影响分析报告
     */
    public ImpactAnalysis buildImpactAnalysis(String affectedTable, List<LineageAnalysisResult> results) {
        Set<String> upstreams = getUpstreamTables(affectedTable, results);
        Set<String> downstreams = getDownstreamTables(affectedTable, results);

        return ImpactAnalysis.builder()
            .affectedTable(affectedTable)
            .upstreamTables(upstreams)
            .downstreamTables(downstreams)
            .build();
    }

    /**
     * 血缘分析结果
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LineageAnalysisResult {
        private String sql;
        private List<FlinkSQLLineageExtractor.TableLineageResult> tableLineages;
        private List<ColumnLineageExtractor.ColumnLineageResult> columnLineages;
        private Map<String, PhysicalMapping.PhysicalEntity> physicalMappings;
        private Set<String> sourceTables;
        private Set<String> targetTables;

        public Set<String> getSourceTables() {
            if (sourceTables == null && tableLineages != null) {
                sourceTables = tableLineages.stream()
                    .flatMap(t -> t.getSourceTables().stream())
                    .collect(Collectors.toSet());
            }
            return sourceTables;
        }

        public Set<String> getTargetTables() {
            if (targetTables == null && tableLineages != null) {
                targetTables = tableLineages.stream()
                    .filter(t -> t.getTargetTable() != null)
                    .map(t -> t.getTargetTable())
                    .collect(Collectors.toSet());
            }
            return targetTables;
        }
    }

    /**
     * 影响分析报告
     */
    @Data
    @Builder
    public static class ImpactAnalysis {
        private String affectedTable;
        private Set<String> upstreamTables;
        private Set<String> downstreamTables;
    }
}