package com.bigdata.lineage.parser;

import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL 解析工具类 - 基于正则表达式（通用方案）
 * 
 * 提供 SQL 文本解析的通用功能，不依赖具体 parser 实现
 */
@Slf4j
public class JSQLParserUtils {

    /**
     * 从 SQL 中提取所有表名（基于正则表达式）
     * 
     * @param sql SQL 语句
     * @return 表名集合
     */
    public static Set<String> extractTables(String sql) {
        Set<String> tables = new HashSet<>();
        String cleanSql = removeComments(sql);
        
        // FROM clause
        extractFromClause(cleanSql, tables);
        
        // JOIN clauses
        extractJoinClauses(cleanSql, tables);
        
        // INSERT INTO
        extractInsertTarget(cleanSql, tables);
        
        return tables;
    }
    
    /**
     * 提取 FROM 子句中的表
     */
    private static void extractFromClause(String sql, Set<String> tables) {
        Pattern pattern = Pattern.compile(
            "\\bFROM\\s+([\\w.]+)(?:\\s+(?:AS\\s+)?(\\w+))?",
            Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        
        while (matcher.find()) {
            addTableName(matcher.group(1), tables);
        }
    }
    
    /**
     * 提取 JOIN 子句中的表
     */
    private static void extractJoinClauses(String sql, Set<String> tables) {
        // Simple JOIN
        Pattern simpleJoin = Pattern.compile(
            "\\bJOIN\\s+([\\w.]+)", Pattern.CASE_INSENSITIVE);
        extractJoins(simpleJoin, sql, tables);
        
        // Complex JOIN
        Pattern complexJoin = Pattern.compile(
            "\\b(CROSS|LEFT|RIGHT|INNER|FULL|OUTER)\\s+JOIN\\s+([\\w.]+)",
            Pattern.CASE_INSENSITIVE);
        extractJoins(complexJoin, sql, tables);
    }
    
    /**
     * 统一处理 JOIN 匹配
     */
    private static void extractJoins(Pattern pattern, String sql, Set<String> tables) {
        Matcher matcher = pattern.matcher(sql);
        int groupIndex = pattern.pattern().contains("(CROSS|LEFT") ? 2 : 1;
        
        while (matcher.find()) {
            addTableName(matcher.group(groupIndex), tables);
        }
    }
    
    /**
     * 提取 INSERT INTO 的目标表
     */
    private static void extractInsertTarget(String sql, Set<String> tables) {
        Pattern pattern = Pattern.compile(
            "INSERT\\s+INTO\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?([\\w.]+)",
            Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        
        if (matcher.find()) {
            addTableName(matcher.group(1), tables);
        }
    }
    
    /**
     * 添加表名（去前缀、过滤子查询等）
     */
    private static void addTableName(String tableName, Set<String> tables) {
        if (tableName == null || tableName.isEmpty()) {
            return;
        }
        
        // Skip subqueries
        if (tableName.startsWith("(") || 
            tableName.toLowerCase().contains("select") ||
            tableName.toLowerCase().contains("from")) {
            log.debug("Skipping subquery or invalid table: {}", tableName);
            return;
        }
        
        // Remove database.schema prefix
        int dotIndex = tableName.lastIndexOf('.');
        if (dotIndex > 0) {
            tableName = tableName.substring(dotIndex + 1);
        }
        
        // Clean up
        tableName = tableName.replaceAll("[^\\w]", "");
        
        if (!tableName.isEmpty()) {
            tables.add(tableName);
        }
    }
    
    /**
     * 移除 SQL 注释
     */
    private static String removeComments(String sql) {
        // Single-line comments
        sql = sql.replaceAll("--[^\n]*", "");
        // Multi-line comments
        sql = sql.replaceAll("/\\*[^*]*\\*+(?:[^/*][^*]*\\*+)*/", "");
        return sql;
    }

    /**
     * 判断是否为 INSERT 语句
     * 
     * @param sql SQL 语句
     * @return true 如果是 INSERT 语句
     */
    public static boolean isInsertStatement(String sql) {
        String upperSql = sql.toUpperCase().trim();
        return upperSql.startsWith("INSERT");
    }

    /**
     * 判断是否为 SELECT 语句
     * 
     * @param sql SQL 语句
     * @return true 如果是 SELECT 语句
     */
    public static boolean isSelectStatement(String sql) {
        String upperSql = sql.toUpperCase().trim();
        return upperSql.startsWith("SELECT");
    }

    /**
     * 获取 SQL 类型
     * 
     * @param sql SQL 语句
     * @return SQL 类型（INSERT/SELECT/CREATE/ALTER/DROP）
     */
    public static String getSQLType(String sql) {
        String upperSql = sql.toUpperCase().trim();
        
        if (upperSql.startsWith("INSERT")) {
            return "INSERT";
        } else if (upperSql.startsWith("SELECT")) {
            return "SELECT";
        } else if (upperSql.startsWith("CREATE")) {
            return "CREATE";
        } else if (upperSql.startsWith("DROP")) {
            return "DROP";
        } else if (upperSql.startsWith("ALTER")) {
            return "ALTER";
        } else {
            return "UNKNOWN";
        }
    }
}
