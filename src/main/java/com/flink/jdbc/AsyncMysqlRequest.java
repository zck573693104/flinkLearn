package com.flink.jdbc;

import com.alibaba.fastjson.JSONObject;
import com.flink.es.AsyncEsDataRequest;
import com.flink.es.ElasticSearchSinkUtil;
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


public class AsyncMysqlRequest {

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

        // 接收kafka数据，转为User 对象
        DataStream<User> input = env.addSource(source).map(value -> JSONObject.parseObject(value,User.class));
        // 异步IO 获取mysql数据, timeout 时间 1s，容量 10（超过10个请求，会反压上游节点）
        DataStream<AsyncUser> async = AsyncDataStream.unorderedWait(input, new AsyncFunctionForMysqlJava(), 2, TimeUnit.MINUTES, 20);
//        async.map(new MapFunction<AsyncUser, String>() {
//            @Override
//            public String map(AsyncUser asyncUser) throws Exception {
//                return JSON.toJSONString(asyncUser);
//            }
//        }).print();


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