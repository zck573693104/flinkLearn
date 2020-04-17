package org.fuwushe.qnx;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.fuwushe.file.ReadFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据开窗-对今日数据进行聚合处理
 */
public class KafkaES6AggJob {


    private static String sourceDDL;

    private static String sinkDDL;

    private static String aggSql;

    private static String querySql;

    private static Logger logger = LoggerFactory.getLogger(KafkaES6AggJob.class);

    private static String PROD_OR_TEST;

    private static String TOPIC_NAME;

    private static String GROUP_NAME = "flinkProdOrderGroup";

    public static void main(String[] args) throws Exception {

        if (args.length != 0) {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            String[] values = ReadFileUtil.readFileByLines(parameterTool.get("path"));
            sourceDDL = values[0];
            sinkDDL = values[1];
            aggSql = values[2];
            querySql = values[3];
            logger.info("sinkDDL:" + aggSql);
            logger.info("aggSql:" + aggSql);
            logger.info("querySql:" + querySql);
        }
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        RegisterFunctionUtil.register(tableEnv);
        tableEnv.sqlUpdate(sourceDDL);
        tableEnv.sqlUpdate(sinkDDL);
        tableEnv.sqlUpdate(aggSql);
        Table aggTable = tableEnv.sqlQuery(querySql);
        tableEnv.toAppendStream(aggTable, Row.class).print();

        streamEnv.execute();


    }
}

