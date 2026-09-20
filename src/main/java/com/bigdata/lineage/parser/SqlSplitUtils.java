package com.bigdata.lineage.parser;

import java.util.ArrayList;
import java.util.List;

/**
 * SQL 语句工具
 */
public final class SqlSplitUtils {

    private SqlSplitUtils() {
    }

    /**
     * 按分号分割多条 SQL。
     * 只在"正常代码态"的分号处切分：字符串/引用标识符内部、
     * 行注释（--）与块注释（块内可含引号和分号）中的分号都不切
     */
    public static List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        char quote = 0;
        boolean inLineComment = false;
        boolean inBlockComment = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char next = i + 1 < sql.length() ? sql.charAt(i + 1) : '\0';

            if (inLineComment) {
                current.append(c);
                if (c == '\n') {
                    inLineComment = false;
                }
                continue;
            }
            if (inBlockComment) {
                current.append(c);
                if (c == '*' && next == '/') {
                    current.append(next);
                    i++;
                    inBlockComment = false;
                }
                continue;
            }
            if (quote != 0) {
                current.append(c);
                if (c == '\\' && next != '\0') {
                    current.append(next);
                    i++;
                } else if (c == quote) {
                    // '' 是转义引号，连续两个同向引号不算闭合
                    if (next == quote) {
                        current.append(next);
                        i++;
                    } else {
                        quote = 0;
                    }
                }
                continue;
            }

            if (c == '-' && next == '-') {
                inLineComment = true;
                current.append(c);
            } else if (c == '/' && next == '*') {
                inBlockComment = true;
                current.append(c);
            } else if (c == '\'' || c == '"' || c == '`') {
                quote = c;
                current.append(c);
            } else if (c == ';') {
                addIfNotBlank(statements, current);
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        addIfNotBlank(statements, current);

        if (statements.isEmpty()) {
            statements.add(sql.trim());
        }

        return statements;
    }

    private static void addIfNotBlank(List<String> statements, StringBuilder sb) {
        String trimmed = sb.toString().trim();
        if (!trimmed.isEmpty()) {
            statements.add(trimmed);
        }
    }
}
