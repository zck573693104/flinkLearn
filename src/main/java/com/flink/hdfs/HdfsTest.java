package com.flink.hdfs;

import com.flink.kafka.KafkaProperties;
import com.sun.xml.internal.fastinfoset.Encoder;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class HdfsTest {
    private static final String path = "hdfs://HACluster/user/flink/test-1";


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);;
        env.setStateBackend(new FsStateBackend(path));
        read(env);
        env.execute();
    }

    public static void write(StreamExecutionEnvironment env){
        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".ext")
                .build();


        Properties properties = KafkaProperties.getConsumerProps("local");
        properties.put("group.id", "test");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("webids-ids_dolog", new SimpleStringSchema(),
                properties);



        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withInactivityInterval(TimeUnit.MILLISECONDS.toMillis(10))
                                .build())
                .build();
        env.addSource(consumer).addSink(sink);

    }
    public static void read(StreamExecutionEnvironment env){

        FileInputFormat fileInputFormat = new TextInputFormat(new Path(path));
        fileInputFormat.setNestedFileEnumeration(true);
        env.readFile(fileInputFormat, path).print();
    }
}
