package com.hycan.bigdata.job;

import com.alibaba.fastjson2.JSONObject;
import com.hycan.bigdata.agg.MileageAggFun;
import com.hycan.bigdata.agg.MileageWindowFun;
import com.hycan.bigdata.utils.MyTimeAssignerUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.time.Duration;

/**
 * 每行驶多少路程进行监控
 *
 * @author kcz
 */
public class MileageMonitorJob {
    private static final String PATH = "path";
    private static  String BOOTSTRAP_SERVERS;
    private static double THRESHOLD;

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        THRESHOLD = parameterTool.getDouble("threshold", 536d);
        BOOTSTRAP_SERVERS = parameterTool.get("bootstrapServers", "127.0.0.1:9092");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics("user_behavior")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "10000")
                .build();


        DataStream<JSONObject> ds = env.fromSource(kafkaSource, WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(20)).withTimestampAssigner(new MyTimeAssignerUtils("times")), "Kafka Source")
                .map(s -> JSONObject.parseObject(s,JSONObject.class));

        WindowedStream<JSONObject, String, GlobalWindow> windowedStream = ds.keyBy(value -> value.getString("vin")).window(GlobalWindows.create());
        windowedStream.trigger(PurgingTrigger.of(DeltaTrigger.of(THRESHOLD
                        , (oldDataPoint, newDataPoint) -> newDataPoint.getDoubleValue(PATH) - oldDataPoint.getDoubleValue(PATH)
                        , TypeInformation.of(JSONObject.class).createSerializer(env.getConfig()))))
                .aggregate(new MileageAggFun(), new MileageWindowFun()).print();

        env.execute("MileageMonitorJob");
    }
}
