package com.hycan.bigdata;

import com.hycan.bigdata.app.HBaseReader;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class Flink2HBaseOperation {
    private static final Logger logger = LoggerFactory.getLogger(Flink2HBaseOperation.class);

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        DataStreamSource<Tuple2<String, String>> streamSource = env.addSource(new HBaseReader());
        streamSource.print();

        try {
            env.execute("HBaseReaderAndWriter");
        } catch (Exception e) {
            logger.error("tfailed from com.huawei.flink.example.hbase, ", e);
        }
    }
}