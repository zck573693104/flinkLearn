package com.bigdata.job;


import com.bigdata.utils.ReadFileUtils;
import com.bigdata.SqlCommandParser;
import com.bigdata.utils.SqlUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;


public class SchemaJob {
    private static Logger LOGGER = LoggerFactory.getLogger(SchemaJob.class);
    private static final String STREAM = "STREAM";
    private static final String BATCH = "BATCH";// 存储解析出来的原始sql
    private static List<String> SQLS;
    // 任务名称
    private static String JOB_NAME;

    private static String PATH;

    private static String JOB_TYPE;

    private static String CHK_PATH;

    private static long CHK_TIME;

    private static int PARALLELISM;

    private static int HIVE_PARALLELISM;

    // 读取哪个配置文件替换sql参数
    private static String ACTIVE_PROFILE;


    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        LOGGER.info("args:" + parameterTool.toMap());
        ACTIVE_PROFILE = parameterTool.get("active", "local");
        CHK_TIME = parameterTool.getLong("chk_time", 30 * 60 * 1000L);
        PARALLELISM = parameterTool.getInt("parallelism", 1);
        HIVE_PARALLELISM = parameterTool.getInt("hive_parallelism", 1);
        PATH = parameterTool.get("path", "/load/data/hbase.sql");
        CHK_PATH = parameterTool.get("chk_path", "hdfs:///flink/checkpoints");
        JOB_NAME = StringUtils.isBlank(parameterTool.get("jobName")) ? PATH : parameterTool.get("jobName");
        JOB_TYPE = StringUtils.isBlank(parameterTool.get("jobType")) ? STREAM : BATCH;
        SQLS = ReadFileUtils.readFileByLines(PATH, ACTIVE_PROFILE);
        LOGGER.info("sqls:" + SQLS);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(CHK_TIME, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(CHK_PATH));
        EnvironmentSettings settings = null;
        if (STREAM.equals(JOB_TYPE)) {
            settings = EnvironmentSettings.newInstance().inStreamingMode()
                    .build();
        } else if (BATCH.equals(BATCH)) {
            settings = EnvironmentSettings.newInstance().inBatchMode()
                    .build();
        }
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        RegisterUdf.register(tEnv);
        // access flink configuration
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.hive.fallback-mapred-reader", "true");
        configuration.setInteger("table.exec.hive.infer-source-parallelism.max", HIVE_PARALLELISM);
        configuration.setBoolean("table.exec.hive.infer-source-parallelism", false);
        // set low-level key-value options
        configuration.setString("table.exec.mini-batch.enabled", "true");
        // local-global aggregation depends on mini-batch is enabled
        configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
        configuration.setString("table.exec.mini-batch.size", "5000");
        // enable two-phase, i.e. local-global aggregation
        configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
        configuration.setString("table.optimizer.distinct-agg.split.enabled", "true");
        configuration.setString("pipeline.name", JOB_NAME);

        registerHiveEnv(tEnv);
        for (String sql : SQLS) {
            if (StringUtils.isNotBlank(sql)) {
                Optional<SqlCommandParser.SqlCommandCall> sqlCommand = SqlCommandParser.parse(sql);
                if (sqlCommand.isPresent()) {
                    SqlUtils.callCommand(sqlCommand.get(), tEnv);
                }
            }
        }
    }

    private static void registerHiveEnv(StreamTableEnvironment tEnv) {
        String name = "myhive";
        String defaultDatabase = "catalog";
        String hiveConfDir = "/opt/Bigdata/client/Hive/config";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tEnv.registerCatalog(name, hive);
        tEnv.useCatalog(name);
    }
}
