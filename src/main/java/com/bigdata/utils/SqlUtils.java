package com.bigdata.utils;

import com.bigdata.SqlCommandParser;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public class SqlUtils {
    public static void main(String[] args) throws Exception {
        Optional<SqlCommandParser.SqlCommandCall> sqlCommand = SqlCommandParser.parse("SET table.sql-dialect=hive");
        SqlUtils.callCommand(sqlCommand.get(),null);
    }
    /**
     * 存储多个sink sql 且需要放到最后
     */
    private static final List<String> STATEMENT_SQLS = new ArrayList<>(16);
    /**
     * 是否存在多个sink
     */
    private static boolean isStatement;

    public static void callCommand(SqlCommandParser.SqlCommandCall cmdCall, StreamTableEnvironment tEnv) throws Exception {
        if (cmdCall.command.equals(SqlCommandParser.SqlCommand.END)) {
            isStatement = false;
            runStatements(tEnv);
            return;
        }
        if (isStatement) {
            STATEMENT_SQLS.add(cmdCall.operands[0]);
            return;
        }
        switch (cmdCall.command) {
            case CREATE_TABLE:
            case INSERT_INTO:
            case INSERT_OVERWRITE:
            case DROP_TABLE:
                tEnv.executeSql(cmdCall.operands[0]);
                break;
            case CREATE_VIEW:
                tEnv.createTemporaryView(cmdCall.operands[0], tEnv.sqlQuery(cmdCall.operands[1]));
                break;
            case SELECT:
                System.out.println(cmdCall.operands[0]);
                tEnv.executeSql(cmdCall.operands[0]).print();
                break;
            case BEGIN_STATEMENT_SET:
                isStatement = true;
                break;
            case SET:
                callSet(cmdCall.operands[1], tEnv);
                break;
            case USE:
                tEnv.useDatabase(cmdCall.operands[0]);
                break;
            case USE_CATALOG:
                tEnv.useCatalog(cmdCall.operands[0]);
                break;
            default:
                throw new Exception("Unsupported command: " + cmdCall.command);
        }
    }

    private static void callSet(String operand, StreamTableEnvironment tEnv) {
        if (operand.toUpperCase(Locale.ENGLISH).contains(SqlDialect.HIVE.name())) {
            tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        } else {
            tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        }
    }

    private static void runStatements(StreamTableEnvironment env) {
        StatementSet statementSet = env.createStatementSet();
        STATEMENT_SQLS.forEach(statementSet::addInsertSql);
        statementSet.execute();
    }
}
