package com.bigdata.lineage.model;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * 物理映射层
 * 
 * 将逻辑表名/列名映射到物理存储实体
 * 支持不同 connector 的物理映射规则
 * 
 * 架构层：Physical Mapping Layer
 */
@Slf4j
public class PhysicalMapping {

    /**
     * Connector 类型枚举（扩展版）
     */
    public enum ConnectorType {
        KAFKA,
        JDBC_MYSQL,
        JDBC_POSTGRES,
        JDBC_SQLSERVER,
        HIVE,
        ELASTICSEARCH,
        FILE_CSV,
        FILE_JSON,
        FILE_AVRO,
        FILE_PARQUET,
        FILE_ORC,
        DATAGEN,
        PRINT,
        FILE_S3,
        FILE_AZURE,
        FILE_GCS
    }
    
    /**
     * 根据 connector 名称自动识别类型
     */
    public static ConnectorType identifyConnectorType(String connectorName, 
                                                       Map<String, String> options) {
        if (connectorName == null) return null;
        
        String lower = connectorName.toLowerCase();
        
        switch (lower) {
            case "kafka":
                return ConnectorType.KAFKA;
            case "jdbc":
                // 进一步判断具体数据库类型
                if (options != null && options.containsKey("url")) {
                    String url = options.get("url");
                    if (url.contains("mysql")) return ConnectorType.JDBC_MYSQL;
                    if (url.contains("postgresql") || url.contains("postgres")) 
                        return ConnectorType.JDBC_POSTGRES;
                    if (url.contains("sqlserver")) return ConnectorType.JDBC_SQLSERVER;
                }
                return ConnectorType.JDBC_MYSQL; // default
            case "hive":
                return ConnectorType.HIVE;
            case "elasticsearch":
            case "es":
                return ConnectorType.ELASTICSEARCH;
            case "csv":
                return ConnectorType.FILE_CSV;
            case "json":
                return ConnectorType.FILE_JSON;
            case "avro":
                return ConnectorType.FILE_AVRO;
            case "parquet":
                return ConnectorType.FILE_PARQUET;
            case "orc":
                return ConnectorType.FILE_ORC;
            case "datagen":
                return ConnectorType.DATAGEN;
            case "print":
                return ConnectorType.PRINT;
            case "s3":
                return ConnectorType.FILE_S3;
            case "azure":
                return ConnectorType.FILE_AZURE;
            case "gcs":
                return ConnectorType.FILE_GCS;
            default:
                log.warn("Unknown connector type: {}", connectorName);
                return null;
        }
    }
    
    /**
     * 简化的版本，仅根据 connector 名称判断（不区分具体数据库类型）
     */
    public static ConnectorType identifyConnectorType(String connectorName) {
        return identifyConnectorType(connectorName, null);
    }

    /**
     * 物理映射信息（增强版）
     */
    @Data
    @Builder
    public static class PhysicalEntity {
        private String logicalTableName;
        private ConnectorType connectorType;
        private String physicalDatabase;
        private String physicalTable;
        private Map<String, String> columnMappings; // logical → physical
        private Map<String, String> options; // connector-specific options
        
        // Kafka specific
        private String kafkaTopic;
        private String kafkaPartition;
        private String kafkaBootstrapServers;
        
        // Hive specific
        private String hiveDatabase;
        private String hiveLocation;
        private String hiveWarehouse;
        
        // JDBC specific
        private String jdbcUrl;
        private String jdbcTable;
        private String jdbcUser;
        
        // ES specific
        private String esIndex;
        private String esType;
        private String esHosts;
        
        // File based
        private String filePath;
        private String fileFormat;
        
        // Generic fields for any connector
        private Map<String, Object> properties;
    }

    /**
     * 构建 Kafka 映射规则（增强版）
     */
    public static PhysicalEntity buildKafkaMapping(
        String logicalTable,
        String topic,
        String partition,
        String bootstrapServers) {
        
        return PhysicalEntity.builder()
            .logicalTableName(logicalTable)
            .connectorType(ConnectorType.KAFKA)
            .physicalTable(topic)
            .kafkaTopic(topic)
            .kafkaPartition(partition)
            .kafkaBootstrapServers(bootstrapServers)
            .fileFormat("kafka")
            .build();
    }

    /**
     * 构建 JDBC 映射规则（增强版）
     */
    public static PhysicalEntity buildJdbcMapping(
        String logicalTable,
        String jdbcUrl,
        String database,
        String table,
        String user,
        Map<String, String> columnMappings) {
        
        ConnectorType type = identifyJdbcType(jdbcUrl);
        
        return PhysicalEntity.builder()
            .logicalTableName(logicalTable)
            .connectorType(type)
            .jdbcUrl(jdbcUrl)
            .physicalDatabase(database)
            .physicalTable(table)
            .jdbcTable(table)
            .jdbcUser(user)
            .columnMappings(columnMappings)
            .build();
    }
    
    /**
     * 根据 JDBC URL 识别数据库类型
     */
    private static ConnectorType identifyJdbcType(String jdbcUrl) {
        if (jdbcUrl == null) return ConnectorType.JDBC_MYSQL;
        
        String lower = jdbcUrl.toLowerCase();
        if (lower.contains("mysql")) return ConnectorType.JDBC_MYSQL;
        if (lower.contains("postgresql") || lower.contains("postgres")) 
            return ConnectorType.JDBC_POSTGRES;
        if (lower.contains("sqlserver")) return ConnectorType.JDBC_SQLSERVER;
        if (lower.contains("oracle")) return ConnectorType.JDBC_MYSQL; // TODO: add ORACLE type
        if (lower.contains("mariadb")) return ConnectorType.JDBC_MYSQL;
        
        return ConnectorType.JDBC_MYSQL; // default
    }

    /**
     * 构建 Hive 映射规则（增强版）
     */
    public static PhysicalEntity buildHiveMapping(
        String logicalTable,
        String hiveDatabase,
        String hiveTable,
        String location,
        String warehouse) {
        
        return PhysicalEntity.builder()
            .logicalTableName(logicalTable)
            .connectorType(ConnectorType.HIVE)
            .hiveDatabase(hiveDatabase)
            .physicalTable(hiveTable)
            .hiveLocation(location)
            .hiveWarehouse(warehouse)
            .build();
    }

    /**
     * 构建 ES 映射规则（增强版）
     */
    public static PhysicalEntity buildEsMapping(
        String logicalTable,
        String esIndex,
        String esType,
        String hosts) {
        
        return PhysicalEntity.builder()
            .logicalTableName(logicalTable)
            .connectorType(ConnectorType.ELASTICSEARCH)
            .esIndex(esIndex)
            .esType(esType)
            .physicalTable(esIndex)
            .esHosts(hosts)
            .build();
    }

    /**
     * 解析 connector options 生成物理映射（增强版）
     */
    public static PhysicalEntity parseFromOptions(
        String logicalTableName,
        Map<String, String> options) {
        
        String connector = options.get("connector");
        if (connector == null) {
            log.warn("No connector specified in options");
            return null;
        }
        
        // Pass options to identifyConnectorType for proper JDBC type detection
        ConnectorType type = identifyConnectorType(connector, options);
        if (type == null) {
            log.warn("Unsupported connector type: {}", connector);
            return null;
        }
        
        // Add all options to the entity
        Map<String, Object> properties = new java.util.HashMap<>(options);
        
        switch (type) {
            case KAFKA:
                return PhysicalEntity.builder()
                    .logicalTableName(logicalTableName)
                    .connectorType(ConnectorType.KAFKA)
                    .physicalTable(options.get("topic"))
                    .kafkaTopic(options.get("topic"))
                    .kafkaPartition(options.get("partition"))
                    .kafkaBootstrapServers(options.get("properties.bootstrap.servers"))
                    .fileFormat("kafka")
                    .options(options)
                    .properties(properties)
                    .build();
                    
            case JDBC_MYSQL:
            case JDBC_POSTGRES:
            case JDBC_SQLSERVER:
                String jdbcUrl = options.get("url");
                ConnectorType jdbcType = identifyJdbcType(jdbcUrl);
                return PhysicalEntity.builder()
                    .logicalTableName(logicalTableName)
                    .connectorType(jdbcType)
                    .jdbcUrl(jdbcUrl)
                    .physicalTable(options.get("table"))
                    .jdbcTable(options.get("table"))
                    .jdbcUser(options.get("username"))
                    .options(options)
                    .properties(properties)
                    .build();
                    
            case HIVE:
                return PhysicalEntity.builder()
                    .logicalTableName(logicalTableName)
                    .connectorType(ConnectorType.HIVE)
                    .hiveDatabase(options.get("database"))
                    .physicalTable(options.get("table"))
                    .hiveLocation(options.get("path"))
                    .hiveWarehouse(options.get("warehouse"))
                    .options(options)
                    .properties(properties)
                    .build();
                    
            case ELASTICSEARCH:
                return PhysicalEntity.builder()
                    .logicalTableName(logicalTableName)
                    .connectorType(ConnectorType.ELASTICSEARCH)
                    .esIndex(options.get("index"))
                    .esType(options.get("type"))
                    .physicalTable(options.get("index"))
                    .esHosts(options.get("hosts"))
                    .options(options)
                    .properties(properties)
                    .build();
                    
            case FILE_CSV:
            case FILE_JSON:
            case FILE_AVRO:
            case FILE_PARQUET:
            case FILE_ORC:
                return PhysicalEntity.builder()
                    .logicalTableName(logicalTableName)
                    .connectorType(type)
                    .filePath(options.get("path"))
                    .fileFormat(type.name().toLowerCase())
                    .options(options)
                    .properties(properties)
                    .build();
                    
            case DATAGEN:
            case PRINT:
                return PhysicalEntity.builder()
                    .logicalTableName(logicalTableName)
                    .connectorType(type)
                    .options(options)
                    .properties(properties)
                    .build();
                    
            default:
                log.warn("Unhandled connector type: {}", type);
                return null;
        }
    }
    
    /**
     * 从 CREATE TABLE SQL 中提取 connector options
     * 使用正则表达式匹配 WITH 子句
     */
    public static Map<String, String> extractOptionsFromSql(String sql) {
        Map<String, String> options = new java.util.HashMap<>();
        
        // Match WITH (...) clause
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
            "\\s*WITH\\s*\\(([^)]+)\\)", 
            java.util.regex.Pattern.CASE_INSENSITIVE);
        
        java.util.regex.Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            String optionsStr = matcher.group(1);
            
            // Parse key-value pairs
            java.util.regex.Pattern kvPattern = java.util.regex.Pattern.compile(
                "'(\\w+)'\\s*=\\s*'([^']+)'");
            
            java.util.regex.Matcher kvMatcher = kvPattern.matcher(optionsStr);
            while (kvMatcher.find()) {
                String key = kvMatcher.group(1);
                String value = kvMatcher.group(2);
                options.put(key, value);
            }
        }
        
        return options;
    }
    
    /**
     * 从 CREATE TABLE SQL 提取完整物理映射
     */
    public static PhysicalEntity extractFromCreateTableSql(String sql) {
        // Extract table name
        java.util.regex.Pattern tableNamePattern = java.util.regex.Pattern.compile(
            "CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?([\\w.]+)",
            java.util.regex.Pattern.CASE_INSENSITIVE);
        
        java.util.regex.Matcher tableNameMatcher = tableNamePattern.matcher(sql);
        if (!tableNameMatcher.find()) {
            log.warn("Cannot extract table name from SQL");
            return null;
        }
        
        String tableName = tableNameMatcher.group(1);
        
        // Extract options
        Map<String, String> options = extractOptionsFromSql(sql);
        
        // Build physical mapping
        return parseFromOptions(tableName, options);
    }
}
