package com.job;


import com.SqlCommandParser;
import com.utils.ReadFileUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;


public class SchemaJob {

    private static Logger logger = LoggerFactory.getLogger(com.job.SchemaJob.class);

    private static List<String> sqlList;

    private static String jobName;

    private static String CHECKPOINT_PATH;

    private static int PARALLELISM;

    public static void main(String[] args) throws Exception {

        if (args.length != 0) {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            jobName = parameterTool.get("jobName","test");
            CHECKPOINT_PATH = parameterTool.get("checkpoint_path");
            PARALLELISM = parameterTool.getInt("parallelism", 1);
            sqlList = Arrays.asList(ReadFileUtil.readFileByLines(parameterTool.get("path")));

        }

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(PARALLELISM);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);
        streamEnv.setStateBackend(new MemoryStateBackend());

        for (String sql : sqlList) {
            Optional<SqlCommandParser.SqlCommandCall> sqlCommand = SqlCommandParser.parse(sql);
            callCommand(sqlCommand.get(), tableEnv);
        }


        streamEnv.execute(jobName);
    }

    /**
     * 此处支持非常多的case  具体查看SqlCommand
     * @param cmdCall
     * @param env
     * @throws Exception
     */
    private static void callCommand(SqlCommandParser.SqlCommandCall cmdCall, StreamTableEnvironment env) throws Exception {
        switch (cmdCall.command) {
            case CREATE_TABLE:
                callCreateTable(cmdCall.operands[0], env);
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

    /**
     * 版本升级 会更改API方式  一类型一处理方式
     * @param sql
     * @param env
     * @throws IOException
     */
    public static void callInsertInto(String sql, StreamTableEnvironment env) throws IOException {
        env.sqlUpdate(sql);

    }

    private static void callCreateTable(String sql, StreamTableEnvironment env) throws IOException {
        env.sqlUpdate(sql);
    }

    public static void callSelect(String sql, StreamTableEnvironment env) throws IOException {
        Table table = env.sqlQuery(sql);
        env.toAppendStream(table, Row.class).print();
    }
}
