package com.bigdata.job;


import com.bigdata.SqlCommandParser;
import com.bigdata.constants.MessageConstants;
import com.bigdata.utils.HiveUtils;
import com.bigdata.utils.ReadFileUtils;
import com.bigdata.utils.SqlUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class SchemaJob {
    private static Logger LOGGER = LoggerFactory.getLogger(SchemaJob.class);
    private static final String STREAM = "STREAM";
    private static final String BATCH = "BATCH";// 存储解析出来的原始sql
    private static List<String> SQLS;
    // 任务名称
    private static String JOB_NAME;

    private static String PATH;

    private static String JOB_TYPE;

    private static String ACTIVE;

    private static long CHK_TIME;

    private static int PARALLELISM;

    private static int HIVE_PARALLELISM;
    private static boolean IS_OBJECT_STORE;

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        LOGGER.info("args:" + parameterTool.toMap());
        CHK_TIME = parameterTool.getLong("chk_time", 30 * 60 * 1000L);
        PARALLELISM = parameterTool.getInt("parallelism", 1);
        HIVE_PARALLELISM = parameterTool.getInt("hive_parallelism", 1);
        PATH = parameterTool.get("path", "/load/data/hbase-1.sql");
        ACTIVE = parameterTool.get("active", "local");
        JOB_NAME = StringUtils.isBlank(parameterTool.get("jobName")) ? PATH : parameterTool.get("jobName");
        JOB_TYPE = StringUtils.isBlank(parameterTool.get("jobType")) ? STREAM : BATCH;
        IS_OBJECT_STORE = !StringUtils.isBlank(parameterTool.get("is_object_store"));
        SQLS = ReadFileUtils.readFileByLines(PATH, ACTIVE,IS_OBJECT_STORE);
//        LOGGER.info("sqls:" + SQLS);
//        System.out.println("sqls:" + SQLS);
        Map<String, String> parameterMap = new HashMap<>();

        parameterMap.put(MessageConstants.ACTIVE, ACTIVE);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                10, // number of restart attempts
                Time.of(1, TimeUnit.MINUTES) // delay
        ));
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(CHK_TIME, CheckpointingMode.EXACTLY_ONCE);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        if (BATCH.equals(JOB_TYPE)) {
            settings = EnvironmentSettings.newInstance().inBatchMode().build();
        }
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        RegisterUdf.register(tEnv);
        // access flink configuration
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.set(ConfigOptions.key("pipeline.global-job-parameters").mapType().defaultValue(parameterTool.toMap()), parameterMap);
        configuration.setInteger("table.exec.hive.infer-source-parallelism.max", HIVE_PARALLELISM);
        // local-global aggregation depends on mini-batch is enabled
        configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
        configuration.setString("table.exec.mini-batch.size", "5000");
        configuration.setString("pipeline.name", JOB_NAME);

        Properties props = System.getProperties();
        String osName = props.getProperty("os.name");
        if (!osName.toLowerCase().contains("windows")) {
            HiveUtils.registerHiveEnv(tEnv, ACTIVE);
        }
        for (String sql : SQLS) {
            if (StringUtils.isNotBlank(sql)) {
                Optional<SqlCommandParser.SqlCommandCall> sqlCommand = SqlCommandParser.parse(sql);
                if (sqlCommand.isPresent()) {
                    SqlUtils.callCommand(sqlCommand.get(), tEnv);
                }
            }
        }
    }
}
