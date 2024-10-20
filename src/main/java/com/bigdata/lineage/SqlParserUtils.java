package com.bigdata.lineage;


import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class SqlParserUtils {
    //    store_returns date_dim store customer
    public static void main(String[] args) throws Exception {
        String path = "D:\\python_project\\sqllineage\\sqllineage\\data\\tpcds\\query02.sql";
        String res = FileUtils.readFileToString(new File(path));
        String[] sqls = res.split(";");
        for (String sqlStr : sqls) {
            if (sqlStr.replace(" ", "").isBlank()) {
                break;
            }
            CCJSqlParserManager parserManager = new CCJSqlParserManager();
            Statement statement = parserManager.parse(new StringReader(sqlStr));
            if (statement instanceof Insert) {
                String tableName = ((Insert) statement).getTable().getName();
                Set<String> tableNames = TablesNamesFinder.findTables(sqlStr);
                tableNames.remove(tableName);
                tableNames.forEach(System.out::println);
            }
        }

    }
}
