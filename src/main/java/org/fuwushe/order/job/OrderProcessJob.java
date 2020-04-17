package org.fuwushe.order.job;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.fuwushe.config.EnvUtil;
import org.fuwushe.constant.RedisConstants;
import org.fuwushe.kafka.KafkaProperties;
import org.fuwushe.kerberos.Kerberos;
import org.fuwushe.order.OrderAccumulator;
import org.fuwushe.order.aggreate.OrderAndGmvAggregateFunc;
import org.fuwushe.order.rich.FilTerOldOrderData;
import org.fuwushe.order.process.OutputProvinceOrderGmvProcessFunc;
import org.fuwushe.order.process.OutputsubCustomerOrderGmvProcessFunc;
import org.fuwushe.order.vo.OrderVO;
import org.fuwushe.sink.redis.GmvRedisMapper;
import org.fuwushe.utils.Redis2Sink;
import org.fuwushe.utils.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class OrderProcessJob {

    private static Logger logger = LoggerFactory.getLogger(OrderProcessJob.class);

    private static String PROD_OR_TEST;

    private static String TOPIC_NAME;

    private static final String PROD_TOPPIC_NAME = "prodOrderTopic";

    private static final String TEST_TOPPIC_NAME = "testOrderTopic";

    private static String GROUP_NAME = "flinkProdOrderGroup";

    public static void main(String[] args) throws Exception {

        if (args.length != 0) {
            Kerberos.getKerberosJaas();
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            PROD_OR_TEST = parameterTool.get("prod_or_test");
            TOPIC_NAME = parameterTool.get("topic_name");
            GROUP_NAME = parameterTool.get("group_name");
            logger.info("入参 TOPIC_NAME:" + TOPIC_NAME + ":GROUP_NAME:" + GROUP_NAME);
        }
        FlinkJedisPoolConfig jedisPoolConfig = RedisUtil.getRedis(PROD_OR_TEST);
        StreamExecutionEnvironment env = EnvUtil.getProFs(PROD_OR_TEST);

        Properties properties = KafkaProperties.getConsumerProps(PROD_OR_TEST);
        properties.put("group.id", GROUP_NAME);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(TOPIC_NAME, new SimpleStringSchema(),
                properties);
        DataStream<String> sourceStream = env.addSource(consumer);
        DataStream<OrderVO> orderStream = sourceStream
                .map(string -> JSONObject.parseObject(string, OrderVO.class))
                .filter(new FilTerOldOrderData());

        provinceGMV(orderStream, jedisPoolConfig);
        subCustomerGMV(orderStream, jedisPoolConfig);

        env.execute("order job");
    }


    /**
     * 省的GMV
     * @param orderStream
     * @param jedisPoolConfig
     */

    private static void provinceGMV(DataStream<OrderVO> orderStream, FlinkJedisPoolConfig jedisPoolConfig)
            throws IOException {

        WindowedStream<OrderVO, Tuple, TimeWindow> provinceDayWindowStream = orderStream.keyBy("orderType", "areaId")
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)));
        DataStream<OrderAccumulator> areaAggStream = provinceDayWindowStream.aggregate(new OrderAndGmvAggregateFunc())
                .setParallelism(2);

        DataStream<Tuple2<String, String>> areaResultStream = areaAggStream.keyBy("orderType", "areaId")
                .process(new OutputProvinceOrderGmvProcessFunc());

        areaResultStream.addSink(new Redis2Sink<>(jedisPoolConfig,
                new GmvRedisMapper(RedisConstants.setAndGet(PROD_OR_TEST).get("HASH_NAME_PROVINCE_PREFIX"))))
                .setParallelism(1);


    }


    /**
     * 门店的GMV
     * @param orderStream
     * @param jedisPoolConfig
     */
    private static void subCustomerGMV(DataStream<OrderVO> orderStream, FlinkJedisPoolConfig jedisPoolConfig)
            throws IOException {

        WindowedStream<OrderVO, Tuple, TimeWindow> supplierDayWindowStream = orderStream
                .keyBy("orderType", "subCustomerId")
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)));

        DataStream<OrderAccumulator> subCustomerAggStream = supplierDayWindowStream
                .aggregate(new OrderAndGmvAggregateFunc()).setParallelism(2);


        DataStream<Tuple2<String, String>> supplierResultStream = subCustomerAggStream
                .keyBy("orderType", "subCustomerId").process(new OutputsubCustomerOrderGmvProcessFunc());
        supplierResultStream.addSink(new Redis2Sink<>(jedisPoolConfig,
                new GmvRedisMapper(RedisConstants.setAndGet(PROD_OR_TEST).get("HASH_NAME_SUB_CUSTOMER_PREFIX"))))
                .setParallelism(1);


    }

}
