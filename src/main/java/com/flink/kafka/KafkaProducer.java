package com.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaProducer {

    private static final String PROD_TOPPIC_NAME = "prodOrderTopic";
    public static void main(String[] args) throws Exception{
        //Kerberos.getKerberosJaas();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.addSource(new MyNoParalleSource()).setParallelism(1);

        Properties properties = KafkaProperties.getLocal();
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer(PROD_TOPPIC_NAME,new SimpleStringSchema(),properties);
/*
        //event-timestamp事件的发生时间
        producer.setWriteTimestampToKafka(true);
*/
        text.addSink(producer);
        env.execute();
    }
}//