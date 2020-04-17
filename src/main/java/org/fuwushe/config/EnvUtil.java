package org.fuwushe.config;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class EnvUtil {

    private static final String TEST_HDFS_PATH = "hdfs://master:8020/user/flink/test/checkpoints/";
    private static final String PROD_HDFS_PATH = "hdfs://bg03.situation.360es.net:8020/user/flink/prod/checkpoints/";

    public static StreamExecutionEnvironment getProFs(String prodOrTest) {
        String hdfsPath = null ;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if ("local".equals(prodOrTest)){
            return env;
        }
        if ("test".equals(prodOrTest)){
            hdfsPath = TEST_HDFS_PATH;
        }
        if ("prod".equals(prodOrTest)){
            hdfsPath = PROD_HDFS_PATH;
        }
        //重启3次 5分钟重启一次 间隔30秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(5, TimeUnit.MINUTES),Time.of(30, TimeUnit.SECONDS)));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //一小时
        env.enableCheckpointing(60 * 60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);;
        env.setStateBackend(new FsStateBackend(hdfsPath));
        return env;
    }
}
