package com.bigdata.job;

import com.alibaba.fastjson2.JSONObject;
import com.bigdata.agg.MileageAggFun;
import com.bigdata.agg.MileageWindowFun;
import com.bigdata.utils.MyTimeAssignerUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class MileageMonitorJob {
    private static final Logger logger = LoggerFactory.getLogger(MileageMonitorJob.class);
    
    private static final String PARAM_THRESHOLD = "threshold";
    private static final double DEFAULT_THRESHOLD = 536.0;
    
    private static final String PARAM_BOOTSTRAP_SERVERS = "bootstrapServers";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    
    private static final String PARAM_TIMESTAMP_FIELD = "times";
    private static final String TOPIC_USER_BEHAVIOR = "user_behavior";
    private static final String GROUP_ID = "mileage-monitor-group";
    private static final Duration WATERMARK_DELAY = Duration.ofSeconds(20);
    private static final int PARTITION_DISCOVERY_INTERVAL_MS = 10000;
    
    private static final String FIELD_VIN = "vin";
    private static final String FIELD_PATH = "path";
    private static final int DEFAULT_PARALLELISM = 1;
    
    private static double threshold;
    private static String bootstrapServers;
    
    public static void main(String[] args) throws Exception {
        logger.info("Starting MileageMonitorJob");
        
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        validateAndInitializeParams(parameterTool);
        
        StreamExecutionEnvironment env = createExecutionEnvironment();
        DataStream<JSONObject> dataStream = buildDataPipeline(env);
        processAndOutput(dataStream);
        
        env.execute("MileageMonitorJob");
    }
    
    private static void validateAndInitializeParams(ParameterTool parameterTool) {
        threshold = parameterTool.getDouble(PARAM_THRESHOLD, DEFAULT_THRESHOLD);
        bootstrapServers = parameterTool.get(PARAM_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS);
        
        logger.info("Configuration - Threshold: {}, Bootstrap Servers: {}", threshold, bootstrapServers);
        
        if (threshold <= 0) {
            throw new IllegalArgumentException("Threshold must be positive");
        }
    }
    
    private static StreamExecutionEnvironment createExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(DEFAULT_PARALLELISM);
        logger.info("Execution environment created with parallelism: {}", DEFAULT_PARALLELISM);
        return env;
    }
    
    private static KafkaSource<String> buildKafkaSource() {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(TOPIC_USER_BEHAVIOR)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", String.valueOf(PARTITION_DISCOVERY_INTERVAL_MS))
                .build();
    }
    
    private static DataStream<JSONObject> buildDataPipeline(StreamExecutionEnvironment env) {
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(WATERMARK_DELAY)
                .withTimestampAssigner(new MyTimeAssignerUtils(PARAM_TIMESTAMP_FIELD));
        
        KafkaSource<String> kafkaSource = buildKafkaSource();
        
        return env.fromSource(kafkaSource, watermarkStrategy, "Kafka Source")
                .map(JSONObject::parseObject);
    }
    
    private static void processAndOutput(DataStream<JSONObject> dataStream) {
        WindowedStream<JSONObject, String, GlobalWindow> windowedStream = dataStream
                .keyBy(value -> value.getString(FIELD_VIN))
                .window(GlobalWindows.create());
        
        windowedStream
                .trigger(createMileageTrigger())
                .aggregate(new MileageAggFun(), new MileageWindowFun())
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .print();
        
        logger.info("Data pipeline configured successfully");
    }
    
    private static PurgingTrigger<JSONObject> createMileageTrigger() {
        DeltaTrigger<JSONObject, JSONObject> deltaTrigger = DeltaTrigger.of(
                threshold,
                (oldData, newData) -> newData.getDoubleValue(FIELD_PATH) - oldData.getDoubleValue(FIELD_PATH),
                TypeInformation.of(JSONObject.class).createSerializer(null)
        );
        
        return PurgingTrigger.of(deltaTrigger);
    }
}
