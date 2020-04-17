package com.flink.broad;

import com.flink.kafka.KafkaProperties;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class BatchDemoBroadcast {
    private static String PROD_OR_TEST;
    public static void testStreamBroadcastState() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = KafkaProperties.getConsumerProps(PROD_OR_TEST);

        FlinkKafkaConsumer<String> originConsumer = new FlinkKafkaConsumer<>("test_topic", new SimpleStringSchema(), properties);
        FlinkKafkaConsumer<String> dynameicConfigConsumer = new FlinkKafkaConsumer<>("test_config_topic", new SimpleStringSchema(), properties);
        DataStreamSource<String> originStream = env.addSource(originConsumer);

        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor("dynamicConfig", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        BroadcastStream<String> configStream = env.addSource(dynameicConfigConsumer).broadcast(descriptor);

        BroadcastConnectedStream<String, String> connectStream = originStream.connect(configStream);
        connectStream.process(new BroadcastProcessFunction<String, String, Void>() {
            @Override
            public void processElement(String value, ReadOnlyContext readOnlyContext, Collector<Void> collector)
                    throws Exception {
                //获取value
                    String configValue  = readOnlyContext.getBroadcastState(descriptor).get("demoConfigKey");
                System.out.println(value+":"+configValue );
            }

            @Override
            public void processBroadcastElement(String value, Context context, Collector<Void> collector) throws Exception {
                //value修改
                context.getBroadcastState(descriptor).put("demoConfigKey",value);
            }
        });

        env.execute("testBroadcastState");
    }


    public static void main(String[] args) throws Exception{
        testStreamBroadcastState();
        batchBroadCastTest();
    }

    private static void batchBroadCastTest() throws Exception {

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1：准备需要广播的数据
        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("zs",18));
        broadData.add(new Tuple2<>("ls",20));
        broadData.add(new Tuple2<>("ww",17));
        DataSet<Tuple2<String, Integer>> tupleData = env.fromCollection(broadData);


        //1.1:处理需要广播的数据,把数据集转换成map类型，map中的key就是用户姓名，value就是用户年龄
        DataSet<HashMap<String, Integer>> toBroadcast = tupleData.map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                HashMap<String, Integer> res = new HashMap<>();
                res.put(value.f0, value.f1);
                return res;
            }
        });

        //源数据
        DataSource<String> data = env.fromElements("zs", "ls", "ww");

        //注意：在这里需要使用到RichMapFunction获取广播变量
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            List<HashMap<String, Integer>> broadCastMap = new ArrayList<>();
            HashMap<String, Integer> allMap = new HashMap<String, Integer>();

            /**
             * 这个方法只会执行一次
             * 可以在这里实现一些初始化的功能
             *
             * 所以，就可以在open方法中获取广播变量数据
             *
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //3:获取广播数据
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadCastMapName");
                for (HashMap map : broadCastMap) {
                    allMap.putAll(map);
                }

            }

            @Override
            public String map(String value) throws Exception {
                Integer age = allMap.get(value);
                return value + "," + age;
            }
        }).withBroadcastSet(toBroadcast, "broadCastMapName");//2：执行广播数据的操作
        result.print();
    }
}
