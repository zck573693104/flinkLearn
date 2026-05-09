package com.bigdata.utils;

import com.bigdata.SqlCommandParser;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class SqlUtils {
    private static final Logger logger = LoggerFactory.getLogger(SqlUtils.class);
    
    private static final int INITIAL_CAPACITY = 16;
    
    private static final ThreadLocal<List<String>> STATEMENT_SQLS = ThreadLocal.withInitial(() -> new ArrayList<>(INITIAL_CAPACITY));
    
    private SqlUtils() {
        throw new IllegalStateException("Utility class");
    }
    
    public static void executeCommand(SqlCommandParser.SqlCommandCall cmdCall, StreamTableEnvironment tEnv) throws Exception {
        if (cmdCall == null) {
            throw new IllegalArgumentException("SQL command call cannot be null");
        }
        
        if (cmdCall.command.equals(SqlCommandParser.SqlCommand.END)) {
            runStatements(tEnv);
            return;
        }
        
        if (isInStatementSet()) {
            STATEMENT_SQLS.get().add(cmdCall.operands[0]);
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
                logger.info("Executing SELECT query");
                tEnv.executeSql(cmdCall.operands[0]).print();
                break;
            case BEGIN_STATEMENT_SET:
                enterStatementSet();
                break;
            case SET:
                configureSqlDialect(cmdCall.operands[1], tEnv);
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
    
    private static void configureSqlDialect(String operand, StreamTableEnvironment tEnv) {
        if (operand.toUpperCase(Locale.ENGLISH).contains(SqlDialect.HIVE.name())) {
            logger.info("Setting SQL dialect to HIVE");
            tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        } else {
            logger.info("Setting SQL dialect to DEFAULT");
            tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        }
    }
    
    private static void enterStatementSet() {
        STATEMENT_SQLS.remove();
        STATEMENT_SQLS.set(new ArrayList<>(INITIAL_CAPACITY));
        logger.info("Entering statement set mode");
    }
    
    private static boolean isInStatementSet() {
        return !STATEMENT_SQLS.get().isEmpty();
    }
    
    private static void runStatements(StreamTableEnvironment env) {
        List<String> sqls = STATEMENT_SQLS.get();
        
        if (sqls.isEmpty()) {
            logger.warn("No statements to execute");
            return;
        }
        
        logger.info("Executing {} statements in batch", sqls.size());
        
        StatementSet statementSet = env.createStatementSet();
        sqls.forEach(statementSet::addInsertSql);
        statementSet.execute();
        
        STATEMENT_SQLS.remove();
    }
}
