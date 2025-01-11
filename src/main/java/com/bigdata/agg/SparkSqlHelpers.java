package com.bigdata.agg;

import io.github.melin.superior.common.relational.Statement;
import io.github.melin.superior.parser.spark.SparkSqlHelper;

public class SparkSqlHelpers implements SqlParser {
    @Override
    public Statement parseStatement(String sql) throws Exception {
        return SparkSqlHelper.parseStatement(sql); // 示例返回
    }
}