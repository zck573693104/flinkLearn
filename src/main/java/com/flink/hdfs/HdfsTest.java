package com.flink.hdfs;

import com.sun.xml.internal.fastinfoset.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

import java.util.Arrays;

public class HdfsTest {


    public static void main(String[] args) throws Exception {
        //Kerberos.getKerberosHdfs();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = env.fromCollection(Arrays.asList("1","2"));

        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(
                new Path("hdfs://localhost:8020/test.txt"),
                new SimpleStringEncoder<String>(Encoder.UTF_8))
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                .build();
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.
                forBulkFormat(new Path("/path"), ParquetAvroWriters.forReflectRecord(String.class))
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                .build();
        input.addSink(sink);

        env.execute();
    }
}
