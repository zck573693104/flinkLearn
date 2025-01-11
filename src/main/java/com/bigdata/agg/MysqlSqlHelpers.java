package com.bigdata.agg;

import com.github.melin.superior.sql.parser.mysql.MySqlHelper;
import io.github.melin.superior.common.relational.Statement;
import io.github.melin.superior.parser.spark.SparkSqlHelper;

public class MysqlSqlHelpers implements SqlParser {
    @Override
    public Statement parseStatement(String sql) throws Exception {
        return MySqlHelper.parseStatement(sql); // 示例返回
    }
}