package com.bigdata.lineage.parser;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Paimon Catalog 血缘提取器 - 基于 Flink Catalog API
 * 
 * 核心思路：
 * 1. 使用 Flink Catalog API 解析 Paimon 表元数据
 * 2. 从 Catalog 获取表的完整 schema（包括分区、主键等）
 * 3. 分析 CREATE TABLE AS SELECT 语句的血缘关系
 * 4. 支持 Paimon 特有的特性（流水表、维度表等）
 * 
 * 架构层：Catalog Layer → Semantic Layer → Lineage Extraction Layer
 */
@Slf4j
public class PaimonCatalogLineageExtractor {

    private final CatalogManager catalogManager;
    
    // Paimon 特定的选项前缀
    private static final String PAIMON_PREFIX = "paimon.";
    private static final String FILE_FORMAT = "format";

    public PaimonCatalogLineageExtractor() {
        // 初始化 CatalogManager（需要配置好 Paimon Catalog）
        this.catalogManager = null; // TODO: 需要从 TableEnvironment 获取
        
        log.info("Initialized PaimonCatalogLineageExtractor");
    }

    /**
     * 提取 Paimon 表的血缘关系
     * 
     * @param flinkSql Flink SQL 语句（包含 Paimon 表操作）
     * @return 血缘关系列表
     */
    public List<PaimonLineageResult> extractPaimonLineages(String flinkSql) {
        List<PaimonLineageResult> results = new ArrayList<>();
        
        try {
            log.debug("Extracting Paimon lineage from SQL: {}", flinkSql);
            
            // 步骤 1: 识别 Paimon 相关操作
            if (isPaimonCreateTableAsSelect(flinkSql)) {
                // CREATE TABLE AS SELECT for Paimon
                PaimonLineageResult result = analyzeCreateTableAsSelect(flinkSql);
                if (result != null) {
                    results.add(result);
                }
            } else if (isPaimonInsertInto(flinkSql)) {
                // INSERT INTO Paimon table
                PaimonLineageResult result = analyzeInsertIntoPaimon(flinkSql);
                if (result != null) {
                    results.add(result);
                }
            }
            
        } catch (Exception e) {
            log.error("Failed to extract Paimon lineage", e);
            throw new RuntimeException("Paimon 血缘提取失败：" + e.getMessage(), e);
        }
        
        return results;
    }
    
    /**
     * 从 Catalog 获取 Paimon 表的元数据
     * 
     * @param databaseName 数据库名
     * @param tableName 表名
     * @return CatalogTable 信息
     */
    public CatalogTable getCatalogTable(String databaseName, String tableName) {
        if (catalogManager == null) {
            log.warn("CatalogManager is not initialized, returning null");
            return null;
        }
        
        try {
            // TODO: 在 Flink 1.20.x 中，Catalog API 可能已改变
            // 这里暂时返回 null，实际使用需要适配正确的 API
            
            /*
            // 方式 1: 使用 ObjectPath（旧版本）
            ObjectPath objectPath = new ObjectPath(databaseName, tableName);
            CatalogTable catalogTable = catalogManager.getTable(objectPath);
            
            // 方式 2: 使用 ObjectIdentifier（新版本）
            ObjectIdentifier identifier = ObjectIdentifier.of(databaseName, tableName);
            CatalogTable catalogTable = catalogManager.getTable(identifier);
            */
            
            log.debug("Catalog lookup for {}.{} would be implemented here", databaseName, tableName);
            return null;
            
        } catch (Exception e) {
            log.error("Failed to get catalog table: {}.{}", databaseName, tableName, e);
        }
        
        return null;
    }
    
    /**
     * 分析 CREATE TABLE AS SELECT 语句
     * 
     * 示例：
     * CREATE TABLE paimon_db.paimon_table (
     *   user_id BIGINT,
     *   order_amount DECIMAL(10,2),
     *   PRIMARY KEY (user_id) NOT ENFORCED
     * ) WITH (
     *   'connector' = 'paimon',
     *   'file.format' = 'parquet'
     * ) AS
     * SELECT user_id, SUM(amount) as order_amount
     * FROM source_table
     * GROUP BY user_id;
     */
    private PaimonLineageResult analyzeCreateTableAsSelect(String sql) {
        // 提取目标 Paimon 表
        String targetTable = extractTargetTableName(sql);
        
        // 提取源表
        Set<String> sourceTables = extractSourceTablesFromSelect(sql);
        
        // 分析列映射
        List<ColumnMapping> columnMappings = analyzeColumnMappings(sql);
        
        // 识别 Paimon 特性
        PaimonProperties properties = extractPaimonProperties(sql);
        
        return PaimonLineageResult.builder()
            .targetTable(targetTable)
            .sourceTables(sourceTables)
            .columnMappings(columnMappings)
            .paimonProperties(properties)
            .processType("CREATE_TABLE_AS_SELECT")
            .confidence(0.95)
            .build();
    }
    
    /**
     * 分析 INSERT INTO Paimon 语句
     * 
     * 示例：
     * INSERT INTO paimon_db.streaming_table
     * SELECT user_id, event_type, COUNT(*)
     * FROM source_stream
     * GROUP BY user_id, event_type;
     */
    private PaimonLineageResult analyzeInsertIntoPaimon(String sql) {
        // 提取目标 Paimon 表
        String targetTable = extractTargetTableName(sql);
        
        // 提取源表
        Set<String> sourceTables = extractSourceTablesFromSelect(sql);
        
        // 分析列映射
        List<ColumnMapping> columnMappings = analyzeColumnMappings(sql);
        
        // 识别 Paimon 流式特性
        boolean isStreaming = isStreamingInsert(sql);
        
        return PaimonLineageResult.builder()
            .targetTable(targetTable)
            .sourceTables(sourceTables)
            .columnMappings(columnMappings)
            .isStreaming(isStreaming)
            .processType("INSERT_INTO")
            .confidence(0.95)
            .build();
    }
    
    /**
     * 提取目标表名
     */
    private String extractTargetTableName(String sql) {
        // CREATE TABLE 或 INSERT INTO
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
            "(?:CREATE\\s+TABLE|INSERT\\s+(?:INTO|OVERWRITE)\\s+(?:TABLE\\s+)?)\\s+" +
            "(?:IF\\s+NOT\\s+EXISTS\\s+)?([\\w.]+)",
            java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher matcher = pattern.matcher(sql);
        
        if (matcher.find()) {
            String tableName = matcher.group(1).replaceAll("[^\\w]", "");
            log.debug("Extracted target table: {}", tableName);
            return tableName;
        }
        
        return null;
    }
    
    /**
     * 从 SELECT 子句中提取源表
     */
    private Set<String> extractSourceTablesFromSelect(String sql) {
        Set<String> tables = new HashSet<>();
        String selectPart = extractSelectPart(sql);
        
        if (selectPart != null) {
            String cleanSql = removeComments(selectPart);
            
            // FROM clause
            extractFromClause(cleanSql, tables);
            
            // JOIN clauses
            extractJoinClauses(cleanSql, tables);
            
            // UNION/INTERSECT/EXCEPT
            extractSetOperations(cleanSql, tables);
        }
        
        return tables;
    }
    
    /**
     * 分析列映射关系
     */
    private List<ColumnMapping> analyzeColumnMappings(String sql) {
        List<ColumnMapping> mappings = new ArrayList<>();
        
        /*
        // 伪代码示例：需要完整的 SQL 解析器
        // 1. 提取 CREATE TABLE 的 schema
        // 2. 提取 SELECT 子句的表达式
        // 3. 匹配列映射关系
        
        Pattern createPattern = Pattern.compile(
            "CREATE\\s+TABLE.*?\\)\\s*AS",
            Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
        Matcher createMatcher = createPattern.matcher(sql);
        
        if (createMatcher.find()) {
            String schemaPart = createMatcher.group();
            // 解析 schema 中的列定义
        }
        
        Pattern selectPattern = Pattern.compile("SELECT\\s+(.*?)\\s+FROM", 
                                                Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
        Matcher selectMatcher = selectPattern.matcher(sql);
        
        if (selectMatcher.find()) {
            String selectExpr = selectMatcher.group(1);
            // 解析 SELECT 表达式，提取列别名和来源
        }
        */
        
        log.warn("Column mapping analysis requires full SQL parsing integration");
        return mappings;
    }
    
    /**
     * 提取 Paimon 表属性
     */
    private PaimonProperties extractPaimonProperties(String sql) {
        PaimonProperties properties = new PaimonProperties();
        
        // 识别 WITH 选项
        java.util.regex.Pattern withPattern = java.util.regex.Pattern.compile(
            "\\bWITH\\s*\\(([^)]+)\\)",
            java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher withMatcher = withPattern.matcher(sql);
        
        if (withMatcher.find()) {
            String optionsStr = withMatcher.group(1);
            
            // 解析选项
            if (optionsStr.contains("'connector'")) {
                properties.setConnector("paimon");
            }
            
            if (optionsStr.contains("'primary-key'")) {
                properties.setPrimaryKeys(extractPrimaryKey(optionsStr));
            }
            
            if (optionsStr.contains("'partition'")) {
                properties.setPartitionKeys(extractPartitionKeys(optionsStr));
            }
            
            if (optionsStr.contains("'bucket'")) {
                properties.setBucketCount(extractBucketCount(optionsStr));
            }
            
            if (optionsStr.contains(FILE_FORMAT)) {
                properties.setFileFormat(extractFileFormat(optionsStr));
            }
            
            // 识别 Paimon 表类型
            if (optionsStr.contains("'changelog-producer'")) {
                properties.setTableType("CHANGELOG");
            } else if (optionsStr.contains("'lookup-cache'")) {
                properties.setTableType("DIMENSION");
            } else {
                properties.setTableType("STREAMING");
            }
        }
        
        return properties;
    }
    
    /**
     * 检查是否为流式插入
     */
    private boolean isStreamingInsert(String sql) {
        String upperSql = sql.toUpperCase();
        
        // 检查是否有时间窗口、水线等流式特征
        return upperSql.contains("WATERMARK") || 
               upperSql.contains("OVER(") ||
               upperSql.contains("BETWEEN") && upperSql.contains("UNBOUNDED");
    }
    
    /**
     * 检查是否为 CREATE TABLE AS SELECT
     */
    private boolean isPaimonCreateTableAsSelect(String sql) {
        String upperSql = sql.toUpperCase().trim();
        return upperSql.startsWith("CREATE TABLE") && 
               upperSql.contains("AS") &&
               upperSql.contains("WITH");
    }
    
    /**
     * 检查是否为 INSERT INTO Paimon
     */
    private boolean isPaimonInsertInto(String sql) {
        String upperSql = sql.toUpperCase().trim();
        return upperSql.startsWith("INSERT INTO") || 
               upperSql.startsWith("INSERT OVERWRITE");
    }
    
    /**
     * 提取 SELECT 部分
     */
    private String extractSelectPart(String sql) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
            "AS\\s+(SELECT.*?)(?:ORDER\\s+BY|LIMIT|GROUP\\s+BY|$)",
            java.util.regex.Pattern.DOTALL | java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher matcher = pattern.matcher(sql);
        
        if (matcher.find()) {
            return matcher.group(1);
        }
        
        // 如果没有 AS，直接找 SELECT
        pattern = java.util.regex.Pattern.compile(
            "(SELECT.*?)(?:ORDER\\s+BY|LIMIT|GROUP\\s+BY|$)",
            java.util.regex.Pattern.DOTALL | java.util.regex.Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(sql);
        
        if (matcher.find()) {
            return matcher.group(1);
        }
        
        return null;
    }
    
    /**
     * 提取主键
     */
    private List<String> extractPrimaryKey(String optionsStr) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
            "'primary-key'\\s*=\\s*'([^']+'(?:\\s*,\\s*[^']+)*)'",
            java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher matcher = pattern.matcher(optionsStr);
        
        if (matcher.find()) {
            String pkStr = matcher.group(1);
            return Arrays.asList(pkStr.split("\\s*,\\s*"));
        }
        
        return new ArrayList<>();
    }
    
    /**
     * 提取分区键
     */
    private List<String> extractPartitionKeys(String optionsStr) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
            "'partition'\\s*=\\s*'([^']+'(?:\\s*,\\s*[^']+)*)'",
            java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher matcher = pattern.matcher(optionsStr);
        
        if (matcher.find()) {
            String partitionStr = matcher.group(1);
            return Arrays.asList(partitionStr.split("\\s*,\\s*"));
        }
        
        return new ArrayList<>();
    }
    
    /**
     * 提取桶数量
     */
    private int extractBucketCount(String optionsStr) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
            "'bucket'\\s*=\\s*'?(\\d+)");
        java.util.regex.Matcher matcher = pattern.matcher(optionsStr);
        
        if (matcher.find()) {
            try {
                return Integer.parseInt(matcher.group(1));
            } catch (NumberFormatException e) {
                return -1;
            }
        }
        
        return -1;
    }
    
    /**
     * 提取文件格式
     */
    private String extractFileFormat(String optionsStr) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
            "'" + FILE_FORMAT + "'\\s*=\\s*'([^']+)'",
            java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher matcher = pattern.matcher(optionsStr);
        
        if (matcher.find()) {
            return matcher.group(1).toLowerCase();
        }
        
        return "default";
    }
    
    /**
     * 移除 SQL 注释
     */
    private String removeComments(String sql) {
        sql = sql.replaceAll("--[^\n]*", "");
        sql = sql.replaceAll("/\\*[^*]*\\*+(?:[^/*][^*]*\\*+)*/", "");
        return sql;
    }
    
    /**
     * 提取 FROM 子句
     */
    private void extractFromClause(String sql, Set<String> tables) {
        Pattern pattern = Pattern.compile(
            "\\bFROM\\s+([\\w.]+)(?:\\s+(?:AS\\s+)?(\\w+))?",
            Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        
        while (matcher.find()) {
            addTableName(matcher.group(1), tables);
        }
    }
    
    /**
     * 提取 JOIN 子句
     */
    private void extractJoinClauses(String sql, Set<String> tables) {
        Pattern simpleJoin = Pattern.compile(
            "\\bJOIN\\s+([\\w.]+)", Pattern.CASE_INSENSITIVE);
        extractJoins(simpleJoin, sql, tables);
        
        Pattern complexJoin = Pattern.compile(
            "\\b(CROSS|LEFT|RIGHT|INNER|FULL|OUTER)\\s+JOIN\\s+([\\w.]+)",
            Pattern.CASE_INSENSITIVE);
        extractJoins(complexJoin, sql, tables);
    }
    
    /**
     * 统一处理 JOIN 匹配
     */
    private void extractJoins(Pattern pattern, String sql, Set<String> tables) {
        Matcher matcher = pattern.matcher(sql);
        int groupIndex = pattern.pattern().contains("(CROSS|LEFT") ? 2 : 1;
        
        while (matcher.find()) {
            addTableName(matcher.group(groupIndex), tables);
        }
    }
    
    /**
     * 提取集合操作后的表
     */
    private void extractSetOperations(String sql, Set<String> tables) {
        Pattern pattern = Pattern.compile(
            "\\b(UNION|INTERSECT|EXCEPT)\\s+(ALL|DISTINCT)?\\s*SELECT",
            Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        
        while (matcher.find()) {
            String afterOp = sql.substring(matcher.end());
            Set<String> additional = extractSimpleTables(afterOp);
            tables.addAll(additional);
        }
    }
    
    /**
     * 简单提取表名
     */
    private Set<String> extractSimpleTables(String sql) {
        Set<String> tables = new HashSet<>();
        Pattern pattern = Pattern.compile(
            "\\bFROM\\s+([\\w.]+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        
        while (matcher.find()) {
            addTableName(matcher.group(1), tables);
        }
        
        return tables;
    }
    
    /**
     * 添加表名
     */
    private void addTableName(String tableName, Set<String> tables) {
        if (tableName.startsWith("(") || 
            tableName.toLowerCase().contains("select") ||
            tableName.toLowerCase().contains("from")) {
            log.debug("Skipping subquery or invalid table: {}", tableName);
            return;
        }
        
        if (tableName.contains("(") && !tableName.contains(".")) {
            log.debug("Skipping table function: {}", tableName);
            return;
        }
        
        int dotIndex = tableName.lastIndexOf('.');
        if (dotIndex > 0) {
            tableName = tableName.substring(dotIndex + 1);
        }
        
        tableName = tableName.replaceAll("[^\\w]", "");
        
        if (!tableName.isEmpty()) {
            tables.add(tableName);
        }
    }

    /**
     * Paimon 血缘结果
     */
    @Data
    @Builder
    public static class PaimonLineageResult {
        private String targetTable;
        private Set<String> sourceTables;
        private List<ColumnMapping> columnMappings;
        private PaimonProperties paimonProperties;
        private boolean isStreaming;
        private String processType;
        private double confidence;
    }
    
    /**
     * 列映射
     */
    @Data
    @Builder
    public static class ColumnMapping {
        private String sourceColumn;
        private String targetColumn;
        private String transformation; // direct_mapping, aggregate, expression
        private double confidence;
    }
    
    /**
     * Paimon 表属性
     */
    @Data
    public static class PaimonProperties {
        private String connector;
        private List<String> primaryKeys = new ArrayList<>();
        private List<String> partitionKeys = new ArrayList<>();
        private int bucketCount = -1;
        private String fileFormat = "default";
        private String tableType = "STREAMING"; // STREAMING, CHANGELOG, DIMENSION
    }
}
