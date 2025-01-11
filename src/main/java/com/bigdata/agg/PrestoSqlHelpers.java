package com.bigdata.agg;

import io.github.melin.superior.common.relational.Statement;
import io.github.melin.superior.parser.presto.PrestoSqlHelper;

public class PrestoSqlHelpers implements SqlParser {
    @Override
    public Statement parseStatement(String sql) throws Exception {
        // 实现PrestoSqlHelper的解析逻辑
        return PrestoSqlHelper.parseStatement(sql); // 示例返回
    }
}