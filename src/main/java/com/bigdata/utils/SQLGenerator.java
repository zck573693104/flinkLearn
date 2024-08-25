package com.bigdata.utils;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.stream.Collectors;

public class SQLGenerator {

    private static final String TAG_SQL_HIS = "partitioned by (partition_time string comment '分区时间,格式:yyyyMMddHHmmss')";
    private static final String TAG_SQL_CM = "partitioned by (perf_month string comment '绩效月份')";

    public static void main(String[] args) {
        try {
            Properties properties = readPropertiesFile("config.properties");
            String delCol = properties.getProperty("del_col");
            String cmMonthTag = "    " + properties.getProperty("cm_month_tag");
            String dwsTableName = properties.getProperty("dws_table_name");
            String tableName = properties.getProperty("table_name");
            String createTableSqlPath = properties.getProperty("create_table_sql_path");
            String runSqlPath = properties.getProperty("run_sql_path");

            Map<String, String> ddlSql = getDDLSQL(createTableSqlPath, tableName, delCol);
            Map<String, String> runSql = getRunSQL(runSqlPath, tableName, cmMonthTag, dwsTableName,delCol);

            System.out.println("CM Insert SQL:");
            System.out.println(runSql.get("cm"));
            System.out.println("\nHIS Insert SQL:");
            System.out.println(runSql.get("his"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Properties readPropertiesFile(String filePath) throws IOException {
        Properties props = new Properties();
        try (InputStream input = new FileInputStream(filePath)) {
            props.load(input);
        }
        return props;
    }

    private static String extractCreateTableStatement(String filePath, String tableName) throws IOException {
        String pattern = "CREATE\\s+TABLE\\s+" + Pattern.quote(tableName) + "\\s*\\((.*?)\\);";
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String content = reader.lines().collect(Collectors.joining());
            Matcher matcher = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(content);
            if (matcher.find()) {
                return "create table " + tableName + matcher.group(1) + ";";
            }
        }
        return null;
    }

    private static String hisSqlDDL(String originSqls, String tag) {
        List<String> lines = Arrays.asList(originSqls.split("\\R"));
        lines.set(0, lines.get(0) + "_td_his");
        lines.add(lines.size() - 1, tag);
        return String.join("\n", lines);
    }

    private static String hisSqlDML(String originSqls) {
        List<String> lines = Arrays.asList(originSqls.split("\\R"));
        lines.set(0, lines.get(0) + "_td_his partition (partition_time='${hivevar:partition_time}')");
        lines.set(1, lines.get(1).replaceAll("\\s+", ""));
        return String.join("\n", lines);
    }

    private static String cmSqlDDL(String originSqls, String tag, String delCol) {
        List<String> lines = Arrays.asList(originSqls.split("\\R"));
        lines = lines.stream().filter(line -> !line.contains(delCol)).collect(Collectors.toList());
        lines.set(0, lines.get(0) + "_cm");
        lines.add(lines.size() - 1, tag);
        return String.join("\n", lines);
    }

    private static String cmSqlDML(String originSqls, String tag, String delCol) {
        List<String> lines = Arrays.asList(originSqls.split("\\R"));
        lines.set(1, lines.get(1).replaceAll("\\s+", ""));
        lines = lines.stream().filter(line -> !line.contains(delCol) && (!line.contains(">=") || !line.contains("<="))).collect(Collectors.toList());
        lines.add(lines.size() - 2, tag);
        lines.set(0, lines.get(0) + "_cm");
        return String.join("\n", lines);
    }

    private static Map<String, String> getDDLSQL(String createTableSqlPath, String tableName, String delCol) throws IOException {
        String originSqls = extractCreateTableStatement(createTableSqlPath, tableName);
        String hisSqls = hisSqlDDL(originSqls, TAG_SQL_HIS);
        String cmSqls = cmSqlDDL(originSqls, TAG_SQL_CM, delCol);
        return Map.of("his", hisSqls, "cm", cmSqls);
    }

    private static String extractRunStatement(String runSqlPath, String tableName) throws IOException {
        String pattern = "INSERT\\s+INTO\\s+" + Pattern.quote(tableName) + "\\s+(.*?);";
        try (BufferedReader reader = new BufferedReader(new FileReader(runSqlPath))) {
            String content = reader.lines().collect(Collectors.joining());
            Matcher matcher = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(content);
            if (matcher.find()) {
                return "insert overwrite table " + tableName + "\n  " + matcher.group(1) + ";";
            }
        }
        return null;
    }

    private static Map<String, String> getRunSQL(String runSqlPath, String tableName, String cmMonthTag, String dwsTableName,String delCol) throws IOException {
        String runSql = extractRunStatement(runSqlPath, tableName);
        runSql = runSql.replace("${etl_time}", "${hivevar:etl_time}")
                       .replace("%Y%m%d%H%i%s", "yyyyMMddHHmmss")
                       .replace("${start_month}", "${hivevar:start_month}")
                       .replace("${end_month}", "${hivevar:end_month}")
                       .replace(dwsTableName, tableName);
        String cmSqls = cmSqlDML(runSql, cmMonthTag, delCol);
        String hisSqls = hisSqlDML(runSql);
        return Map.of("cm", cmSqls, "his", hisSqls);
    }
}
