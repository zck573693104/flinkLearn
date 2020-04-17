package com.flink.item.job;

import com.alibaba.fastjson.JSONObject;
import com.flink.item.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.fuwushe.item.*;
import com.flink.kafka.KafkaProperties;
import com.flink.kerberos.Kerberos;
import com.flink.sink.mysql.SinkToMySQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
public class HotItemsEvnJob {
    private static Logger logger = LoggerFactory.getLogger(HotItemsEvnJob.class);
    private static  String BEHAVIOR = "pv";
    private static String PROD_OR_TEST;
    private static  String TOPIC_NAME ;
    private static final String PROD_TOPPIC_NAME = "prodUserBehaviorTopic";
    private static final String TEST_TOPPIC_NAME = "testUserBehaviorTopic";

    private static  String GROUP_NAME = "flinkProdPVUserBehaviorGroup";

    public static void main(String[] args) throws Exception {
        if (args.length!=0) {
            Kerberos.getKerberosJaas();
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            GROUP_NAME = parameterTool.get("group_name");
            BEHAVIOR = parameterTool.get("behavior");
            PROD_OR_TEST = parameterTool.get("prod_or_test");
            TOPIC_NAME = parameterTool.get("topic_name");
            logger.info("入参 GROUP_NAME:"+GROUP_NAME+":behavior:"+ BEHAVIOR);
        }
        // 创建 execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 告诉系统按照 EventTime 处理
        //env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = KafkaProperties.getConsumerProps(PROD_OR_TEST);
        properties.put("group.id", GROUP_NAME);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(TOPIC_NAME, new SimpleStringSchema(), properties);

        DataStream<UserBehaviorVO> userBehaviorStream = env
                .addSource(consumer).filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        if (StringUtils.isNotBlank(value)){
                            return true;
                        }
                        return false;
                    }
                }).map(new MapFunction<String, UserBehaviorVO>() {
                    @Override
                    public UserBehaviorVO map(String value) throws Exception {
                        return JSONObject.parseObject(value, UserBehaviorVO.class);
                    }
                });

        userBehaviorStream
                // 抽取出时间和生成 watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
                // 过滤出只有点击的数据
                .filter(new FilterFunction<UserBehaviorVO>() {
                    @Override
                    public boolean filter(UserBehaviorVO userBehaviorVO) throws Exception {
                        // 过滤出只有点击的数据
                        return userBehaviorVO.behavior.equals(BEHAVIOR);
                    }
                })
               .keyBy("itemId")
                .timeWindow(Time.minutes(60), Time.minutes(20))
                .aggregate(new CountAgg(), new WindowResultFunction("end"))
                .keyBy("windowEnd")
                .process(new TopNHotItems(20))
                .addSink(new SinkToMySQL(BEHAVIOR, PROD_OR_TEST));

        env.execute(BEHAVIOR+" Hot Items Job");
    }




}