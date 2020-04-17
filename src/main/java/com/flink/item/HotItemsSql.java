package com.flink.item;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import com.flink.kafka.KafkaProperties;
import com.flink.sql.udf.FromUnixTimeUDF;
import com.flink.sink.mysql.SinkUserPvToMySQL2;

import java.util.Properties;


public class HotItemsSql {

    private static final String behavior = "pv";
    private static String PROD_OR_TEST;
    private static final String PROD_TOPPIC_NAME = "prodUserBehaviorTopic";

    public static void main(String[] args) throws Exception {
        //Kerberos.getKerberosJaas();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.registerFunction("from_unixtime_utc8", new FromUnixTimeUDF());

        Properties properties = KafkaProperties.getLocal();
        properties.put("group.id", "flinkSqlProdUserBehaviorGroup");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(PROD_TOPPIC_NAME, new SimpleStringSchema(),
                properties);
        consumer.setStartFromLatest();
        DataStream<UserBehaviorVO> userBehaviorStream = env.addSource(consumer).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {

                if (StringUtils.isNotBlank(value)) {
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
        SingleOutputStreamOperator<UserBehaviorVO> ds = userBehaviorStream
                // 抽取出时间和生成 watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());
        // 过滤出只有点击的数据

        //tableEnv.fromDataStream( userBehaviorStream, "itemId,proctime.proctime");
        tableEnv.registerDataStream("item_view", ds, "itemId,timestamp.rowtime");
        //tableEnv.registerDataStream( "test",userBehaviorStream, "itemId,proctime.proctime");

        Table sqlQuery = tableEnv.sqlQuery(" SELECT itemId, " + " COUNT(itemId) AS viewCount, "
                + " from_unixtime_utc8(HOP_END(`timestamp`, INTERVAL '10' SECOND,INTERVAL '1' MINUTE)) as windowEndDates "
                + " FROM  item_view " + " GROUP BY HOP (`timestamp`, INTERVAL '10' SECOND,INTERVAL '1' MINUTE),"
                + " itemId ");


        DataStream<Tuple3<String, Long, String>> dataStreamResult = tableEnv
                .toAppendStream(sqlQuery, Types.TUPLE(Types.LONG, Types.LONG, Types.STRING));
        dataStreamResult.print();
        dataStreamResult.map(new MapFunction<Tuple3<String, Long, String>, Result>() {
            @Override
            public Result map(Tuple3<String, Long, String> value) throws Exception {

                Result result = new Result();
                result.setItemId(value.f0);
                result.setViewCount(value.f1);
                result.setWindowEndDate(value.f2);
                return result;
            }
        }).addSink(new SinkUserPvToMySQL2(behavior,PROD_OR_TEST));
        env.execute("Hot Items Job SQL");

    }

}