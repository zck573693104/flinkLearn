package com.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import com.flink.kerberos.Kerberos;

import java.util.Properties;

public class KafkaConsumer {
    private static String PROD_OR_TEST;

    private static String TOPIC_NAME;

    private static final String PROD_TOPPIC_NAME = "prodOrderDetailTopic";

    private static final String TEST_TOPPIC_NAME = "testOrderDetailTopic";

    private static String GROUP_NAME = "flinkProdOrderDetailGroup";

    public static void main(String[] args) throws Exception{
        if (args.length != 0) {
            Kerberos.getKerberosJaas();
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            PROD_OR_TEST = parameterTool.get("prod_or_test");
            TOPIC_NAME = parameterTool.get("topic_name");
            GROUP_NAME = parameterTool.get("group_name");
        }
        Kerberos.getKerberosJaas();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = KafkaProperties.getConsumerProps(PROD_OR_TEST);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("testOrderTopic", new SimpleStringSchema(), properties);
        DataStream<String> stream = env
                .addSource(consumer);
//        stream.map(new MapFunction<String, Integer>() {
//            @Override
//            public Integer map(String s) throws Exception {
//
//                return 1/0;
//            }
//        }).print();
        stream.print();
        //stream.map();
        env.execute();

    }
}//