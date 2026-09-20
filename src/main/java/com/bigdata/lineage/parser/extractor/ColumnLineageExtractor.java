package com.bigdata.lineage.parser.extractor;

import com.bigdata.lineage.parser.model.ColumnLineage;
import io.github.melin.superior.parser.flink.FlinkSqlHelper;
import io.github.melin.superior.common.relational.dml.InsertTable;
import io.github.melin.superior.common.relational.dml.QueryStmt;
import io.github.melin.superior.common.relational.TableId;
import io.github.melin.superior.common.relational.table.ColumnRel;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 列级血缘提取器
 * 
 * 基于 Superior SQL Parser 实现高精度的列级血缘提取
 */
@Slf4j
public class ColumnLineageExtractor {
    
    private static final double DIRECT_MAPPING_CONFIDENCE = 0.95;
    private static final double FUNCTION_CONFIDENCE = 0.85;
    private static final double EXPRESSION_CONFIDENCE = 0.80;
    private static final double STAR_MAPPING_CONFIDENCE = 0.75;
    
    /**
     * 从 INSERT 语句提取列级血缘
     */
    public List<ColumnLineage> extractFromInsert(InsertTable insertStmt) {
        QueryStmt queryStmt = insertStmt.getQueryStmt();
        
        List<ColumnLineage> results = new ArrayList<>();
        
        String targetTable = insertStmt.getOutputTables().stream()
            .map(TableId::getFullTableName)
            .findFirst()
            .orElse(null);
        
        if (targetTable == null || queryStmt.getInputTables().isEmpty()) {
            return results;
        }
        
        // 获取第一个源表（简化处理，实际可能需要更复杂的逻辑）
        String sourceTable = queryStmt.getInputTables().get(0).getFullTableName();
        
        // 如果有明确的列定义
        if (insertStmt.getColumnRels() != null && !insertStmt.getColumnRels().isEmpty()) {
            results.addAll(extractWithColumnMapping(
                insertStmt.getColumnRels(), 
                sourceTable, 
                targetTable
            ));
        } else {
            // 无明确列定义，使用通配符映射
            results.add(ColumnLineage.builder()
                .sourceTable(sourceTable)
                .sourceColumn("*")
                .targetTable(targetTable)
                .targetColumn("*")
                .transformation("star_mapping")
                .confidence(STAR_MAPPING_CONFIDENCE)
                .build());
        }
        
        log.debug("Extracted {} column lineage(s) from INSERT", results.size());
        return results;
    }
    
    /**
     * 从 SELECT 语句提取列级血缘
     */
    public List<ColumnLineage> extractFromSelect(QueryStmt queryStmt) {
        List<ColumnLineage> results = new ArrayList<>();
        
        if (queryStmt.getInputTables().isEmpty()) {
            return results;
        }
        
        String sourceTable = queryStmt.getInputTables().get(0).getFullTableName();
        
        // 检查是否是全字段选择
        Pattern starPattern = Pattern.compile("SELECT\\s+\\*\\s+FROM", 
                                              Pattern.CASE_INSENSITIVE);
        Matcher matcher = starPattern.matcher(queryStmt.toString());
        
        if (matcher.find()) {
            results.add(ColumnLineage.builder()
                .sourceTable(sourceTable)
                .sourceColumn("*")
                .targetTable(null)
                .targetColumn("*")
                .transformation("select_star")
                .confidence(0.8)
                .build());
        }
        
        return results;
    }
    
    /**
     * 基于列映射关系提取血缘
     */
    private List<ColumnLineage> extractWithColumnMapping(
            List<ColumnRel> columnRels, 
            String sourceTable, 
            String targetTable) {
        
        List<ColumnLineage> results = new ArrayList<>();
        
        for (int i = 0; i < columnRels.size(); i++) {
            ColumnRel col = columnRels.get(i);
            
            var builder = ColumnLineage.builder()
                .sourceTable(sourceTable)
                .targetTable(targetTable)
                .targetColumn(col.getColumnName())
                .confidence(DIRECT_MAPPING_CONFIDENCE);
            
            // 分析源列
            String sourceColumn = analyzeSourceColumn(col, i);
            builder.sourceColumn(sourceColumn);
            
            // 分析转换逻辑
            String transformation = analyzeTransformation(col, i);
            builder.transformation(transformation);
            
            // 标记是否为计算列
            builder.isComputed(col.getComputedExpr() != null);
            
            results.add(builder.build());
        }
        
        return results;
    }
    
    /**
     * 分析源列名
     */
    private String analyzeSourceColumn(ColumnRel col, int index) {
        if (col.getComputedExpr() != null) {
            // 计算列，返回表达式
            return col.getComputedExpr();
        }
        
        // 普通列，假设与目标列同名
        return col.getColumnName();
    }
    
    /**
     * 分析转换逻辑
     */
    private String analyzeTransformation(ColumnRel col, int index) {
        if (col.getComputedExpr() != null) {
            String expr = col.getComputedExpr();
            
            // 检测函数调用
            if (expr.contains("(") && expr.contains(")")) {
                String funcName = extractFunctionName(expr);
                return "function:" + funcName;
            }
            
            // 检测表达式
            if (expr.contains("+") || expr.contains("-") || 
                expr.contains("*") || expr.contains("/") ||
                expr.contains("%")) {
                return "expression";
            }
            
            return "direct_mapping";
        }
        
        return "direct_mapping";
    }
    
    /**
     * 提取函数名
     */
    private String extractFunctionName(String expression) {
        Pattern pattern = Pattern.compile("(\\w+)\\s*\\(");
        Matcher matcher = pattern.matcher(expression);
        
        if (matcher.find()) {
            return matcher.group(1);
        }
        
        return "unknown";
    }
    
    /**
     * 统一入口 - 从 SQL 提取列级血缘
     */
    public List<ColumnLineage> extract(String sql) {
        List<ColumnLineage> results = new ArrayList<>();
        
        try {
            List<io.github.melin.superior.common.relational.Statement> statements = 
                FlinkSqlHelper.parseMultiStatement(sql);
            
            for (var stmt : statements) {
                if (stmt instanceof InsertTable) {
                    results.addAll(extractFromInsert((InsertTable) stmt));
                } else if (stmt instanceof QueryStmt) {
                    results.addAll(extractFromSelect((QueryStmt) stmt));
                }
            }
            
            log.info("Extracted {} column lineage(s)", results.size());
            
        } catch (Exception e) {
            log.error("Failed to extract column lineage", e);
            throw new ColumnExtractionException("Column lineage extraction failed: " + e.getMessage(), e);
        }
        
        return results;
    }
    
    /**
     * 批量提取列级血缘
     */
    public Map<String, List<ColumnLineage>> extractBatch(List<String> sqls) {
        Map<String, List<ColumnLineage>> results = new LinkedHashMap<>();
        
        for (String sql : sqls) {
            try {
                List<ColumnLineage> lineages = extract(sql);
                results.put(sanitizeSql(sql), lineages);
            } catch (Exception e) {
                log.warn("Failed to extract column lineage for SQL", e);
                results.put(sanitizeSql(sql), Collections.emptyList());
            }
        }
        
        return results;
    }
    
    private String sanitizeSql(String sql) {
        return sql.toUpperCase().trim().replaceAll("\\s+", " ");
    }
    
    public static class ColumnExtractionException extends RuntimeException {
        public ColumnExtractionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
