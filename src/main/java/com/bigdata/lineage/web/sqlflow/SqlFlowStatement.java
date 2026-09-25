package com.bigdata.lineage.web.sqlflow;

import java.util.Locale;

/**
 * 从语句原文读出的"这条语句在干什么"。
 *
 * <p>为什么要重新判一次：解析层把类型放在 {@code TableLineage.processType} 里，快照只留图和语句
 * 原文，装配层拿不到那个 DTO。而原文本来就要带给前端做片段高亮，按前缀再读一次不额外取数据——
 * 语句开头是 SQL 里唯一不会引起歧义的位置。
 */
final class SqlFlowStatement {

    private static final String QUERY = "select";

    private final String effectType;
    private final boolean view;
    private final String heading;

    private SqlFlowStatement(String effectType, boolean view, String heading) {
        this.effectType = effectType;
        this.view = view;
        this.heading = heading;
    }

    static SqlFlowStatement of(String sql) {
        String head = leadingText(sql);
        String flat = head.replaceAll("\\s+", " ");
        if (flat.startsWith("create or replace view") || flat.startsWith("create view")) {
            return create("create_view", true);
        }
        if (flat.startsWith("create temporary view") || flat.startsWith("create temp view")) {
            return create("create_view", true);
        }
        if (flat.startsWith("create materialized view")) {
            return create("create_view", true);
        }
        if (flat.startsWith("create table") || flat.startsWith("create temporary table")
                || flat.startsWith("create temp table") || flat.startsWith("create or replace table")) {
            return create("create_table", false);
        }
        if (flat.startsWith("insert overwrite") || flat.startsWith("insert into")) {
            return new SqlFlowStatement("insert", false, "INSERT");
        }
        if (flat.startsWith("merge into") || flat.startsWith("merge")) {
            return new SqlFlowStatement("merge", false, "MERGE");
        }
        if (flat.startsWith("update")) {
            return new SqlFlowStatement("update", false, "UPDATE");
        }
        if (flat.startsWith("delete")) {
            return new SqlFlowStatement("delete", false, "DELETE");
        }
        return new SqlFlowStatement(QUERY, false, "SELECT");
    }

    private static SqlFlowStatement create(String effectType, boolean view) {
        return new SqlFlowStatement(effectType, view,
                effectType.replace('_', ' ').toUpperCase(Locale.ROOT));
    }

    /** 血缘关系挂在哪个动作上：查询与中间站一律算 select */
    String getEffectType() {
        return effectType;
    }

    /** 产出的是视图还是表：只影响实体类型标签 */
    boolean producesView() {
        return view;
    }

    /** 语句标题，UI 上"第几条语句、什么动作" */
    String getHeading() {
        return heading;
    }

    /** 跳过空白与注释后开头的若干个词，够判类型就行 */
    private static String leadingText(String sql) {
        if (sql == null || sql.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (i < sql.length() && sb.length() < 64) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c)) {
                sb.append(' ');
                i++;
            } else if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
                int end = sql.indexOf('\n', i);
                i = end < 0 ? sql.length() : end + 1;
            } else if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
                int end = sql.indexOf("*/", i + 2);
                i = end < 0 ? sql.length() : end + 2;
            } else {
                sb.append(Character.toLowerCase(c));
                i++;
            }
        }
        return sb.toString().trim();
    }
}
