package com.flink.es;

import com.alibaba.fastjson.JSONObject;
import com.flink.kafka.KafkaProperties;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ES5Test {

    private static String PROD_OR_TEST;

    private static String TOPIC_NAME;

    private static final String PROD_TOPPIC_NAME = "prodOrderTopic";

    private static final String TEST_TOPPIC_NAME = "testOrderTopic";

    private static String GROUP_NAME = "flinkProdOrderGroup";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = KafkaProperties.getConsumerProps("local");
        properties.put("group.id", GROUP_NAME);
        FlinkKafkaConsumer<String> source = new FlinkKafkaConsumer<>("topic_test_1", new SimpleStringSchema(),
                properties);

        DataStream<String> input1 = env.addSource(source).
                map(new MapFunction<String, String>() {
                    @Override
                    public String map(String s) throws Exception {
                        return JSONObject.parseObject(s).getString("name");
                    }
                });


        DataStream<Tuple2<String,String>> asyncStream = AsyncDataStream.unorderedWait(input1, new AsyncEsDataRequest() ,1, TimeUnit.MINUTES, 1000).setParallelism(1);
        asyncStream.print();

        DataStream<Tuple2<String,Integer>> update =   asyncStream.map(new MapFunction<Tuple2<String, String>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, String> tuple2) throws Exception {
                return new Tuple2<>("AXFizhOb246_9wAPymT_",50);
            }
        });
        ElasticSearchSinkUtil.update(update);
        env.execute("asyncForMysql");

    }
}
