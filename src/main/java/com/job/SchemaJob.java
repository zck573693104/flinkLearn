package com.job;


import com.SqlCommandParser;
import com.utils.ReadFileUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;


public class SchemaJob {

    private static Logger logger = LoggerFactory.getLogger(SchemaJob.class);

    private static List<String> sqlList;

    private static String jobName;

    private static String CHECKPOINT_PATH;

    private static int PARALLELISM;

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        jobName = parameterTool.get("jobName", "test");
        CHECKPOINT_PATH = parameterTool.get("checkpoint_path");
        PARALLELISM = parameterTool.getInt("parallelism", 1);
        sqlList = Arrays.asList(ReadFileUtil.readFileByLines(parameterTool.get("path", "/load/data/flink_csv.sql")));

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironment(new Configuration());
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(streamEnv);
        // access flink configuration
        Configuration configuration = tEnv.getConfig().getConfiguration();
        // set low-level key-value options
        configuration.setString("table.exec.mini-batch.enabled", "true");
        // local-global aggregation depends on mini-batch is enabled
        configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
        configuration.setString("table.exec.mini-batch.size", "5000");
        // enable two-phase, i.e. local-global aggregation
        configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
        configuration.setString("table.optimizer.distinct-agg.split.enabled", "true");
        for (String sql : sqlList) {
            if (StringUtils.isNotBlank(sql)) {
                Optional<SqlCommandParser.SqlCommandCall> sqlCommand = SqlCommandParser.parse(sql);
                if (sqlCommand.isPresent()) {
                    callCommand(sqlCommand.get(), tEnv);
                }
            }
        }
        streamEnv.execute(jobName);
    }

    /**
     * 此处支持非常多的case  具体查看SqlCommand
     *
     * @param cmdCall
     * @param env
     * @throws Exception
     */
    private static void callCommand(SqlCommandParser.SqlCommandCall cmdCall, StreamTableEnvironment env) throws Exception {
        switch (cmdCall.command) {
            case CREATE_TABLE:
                callCreateTable(cmdCall.operands[0], env);
                break;
            case CREATE_VIEW:
                callCreateView(cmdCall, env);
                break;
            case INSERT_INTO:
            case INSERT_OVERWRITE:
                callInsertInto(cmdCall.operands[0], env);
                break;
            case SELECT:
                callSelect(cmdCall.operands[0], env);
                break;
            default:
                throw new Exception("Unsupported command: " + cmdCall.command);
        }
    }

    private static void callCreateView(SqlCommandParser.SqlCommandCall cmdCall, StreamTableEnvironment env) {
        env.createTemporaryView(cmdCall.operands[0], env.sqlQuery(cmdCall.operands[1]));
    }

    /**
     * 版本升级 会更改API方式  一类型一处理方式
     *
     * @param sql
     * @param env
     * @throws IOException
     */
    public static void callInsertInto(String sql, StreamTableEnvironment env) throws IOException {
        env.executeSql(sql);

    }

    private static void callCreateTable(String sql, StreamTableEnvironment env) throws IOException {
        env.executeSql(sql);
    }

    public static void callSelect(String sql, StreamTableEnvironment env) throws IOException {
        env.executeSql(sql).print();
    }
}
