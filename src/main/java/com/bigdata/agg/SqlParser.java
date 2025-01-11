package com.bigdata.agg;

import io.github.melin.superior.common.relational.Statement;

public interface SqlParser {
    Statement parseStatement(String sql) throws Exception;
}