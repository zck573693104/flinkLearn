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
     * 按分号分割多条 SQL，跳过单引号字符串内的分号
     */
    public static List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inString = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                // '' 为转义引号，toggle 两次后状态不变，天然正确
                inString = !inString;
                current.append(c);
            } else if (c == ';' && !inString) {
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
