package com.flink.qnx;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import com.flink.file.ReadFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 不做包含状态的工作 只负责数据转换
 */
public class KafkaPreJob {

    private static Logger logger = LoggerFactory.getLogger(KafkaPreJob.class);

    public static final String SPECIAL = "@qnx";
    public static final String FORMAT = "  ";




    private static String sourceDDL;

    private static String sinkDDL;

    private static String sinkSql;

    public static void main(String[] args) throws Exception {

        if (args.length != 0) {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            String [] values = ReadFileUtil.readFileByLines(parameterTool.get("path"));
            sourceDDL = values[0];
            sinkDDL = values[1];
            sinkSql = values[2];
            logger.info("sinkDDL:"+sinkDDL);
            logger.info("sourceDDL:"+sourceDDL);
            logger.info("sinkSql:"+sinkSql);
        }
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);
        RegisterFunctionUtil.register(tableEnv);
        tableEnv.sqlUpdate(sourceDDL);
        tableEnv.sqlUpdate(sinkDDL);
        tableEnv.sqlUpdate(sinkSql);
        streamEnv.execute();
    }
}
