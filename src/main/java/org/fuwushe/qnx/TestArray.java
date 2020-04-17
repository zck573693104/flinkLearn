package org.fuwushe.qnx;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.fuwushe.file.ReadFileUtil;

public class TestArray {
    private static String sourceDDL;

    private static String sinkDDL;

    private static String querySql;

    public static void main(String[] args) throws Exception {

        if (args.length != 0) {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            String [] values = ReadFileUtil.readFileByLines(parameterTool.get("path"));
            sourceDDL = values[0];
            querySql = values[1];


        }
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);
        RegisterFunctionUtil.register(tableEnv);
        tableEnv.sqlUpdate(sourceDDL);
        Table aggTable = tableEnv.sqlQuery(querySql);
        tableEnv.toAppendStream(aggTable, Row.class).print();
        streamEnv.execute();
    }
}
