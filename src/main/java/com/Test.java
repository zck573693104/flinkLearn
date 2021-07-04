package com;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironment(new Configuration());
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        DataStreamSource<String> data = streamEnv.fromElements(JSONObject.toJSONString(new UserBehavior(1)));
        //FlinkKafkaConsumer consumer = new FlinkKafkaConsumer("user_behavior",new SimpleStringSchema(),properties);
        //streamEnv.addSource(consumer).map(JSONObject::toJSONString).print();
        FlinkKafkaProducer producer = new FlinkKafkaProducer("user_behavior",new SimpleStringSchema(),properties);
        data.addSink(producer);
        streamEnv.execute("test");
    }
}
