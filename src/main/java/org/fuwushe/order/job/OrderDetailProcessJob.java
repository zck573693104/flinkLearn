package org.fuwushe.order.job;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.fuwushe.item.WindowResultFunction;
import org.fuwushe.kafka.KafkaProperties;
import org.fuwushe.kerberos.Kerberos;
import org.fuwushe.order.OrderAccumulator;
import org.fuwushe.order.aggreate.ItemSalesAggregateFunc;
import org.fuwushe.order.aggreate.OrderAndGmvAggregateFunc;
import org.fuwushe.order.rich.FilTerOldOrderData;
import org.fuwushe.order.process.ItemProcessFunc;
import org.fuwushe.order.process.OutputCategoryCodeGmvProcessFunc;
import org.fuwushe.order.process.OutputItemGmvProcessFunc;
import org.fuwushe.order.vo.OrderVO;
import org.fuwushe.sink.redis.GmvRedisMapper;
import org.fuwushe.sink.redis.RankingRedisMapper;
import org.fuwushe.utils.Redis2Sink;
import org.fuwushe.utils.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class OrderDetailProcessJob {

    private static Logger logger = LoggerFactory.getLogger(OrderDetailProcessJob.class);

    private static String PROD_OR_TEST;

    private static String TOPIC_NAME;

    private static final String PROD_TOPPIC_NAME = "prodOrderDetailTopic";

    private static final String TEST_TOPPIC_NAME = "testOrderDetailTopic";

    private static String GROUP_NAME = "flinkProdOrderDetailGroup";

    public static void main(String[] args) throws Exception {

        if (args.length != 0) {
            Kerberos.getKerberosJaas();
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            PROD_OR_TEST = parameterTool.get("prod_or_test");
            TOPIC_NAME = parameterTool.get("topic_name");
            GROUP_NAME = parameterTool.get("group_name");
            logger.info("入参 TOPIC_NAME:" + TOPIC_NAME + ":GROUP_NAME:" + GROUP_NAME);
        }
        StreamExecutionEnvironment env = EnvUtil.getProFs(PROD_OR_TEST);

        FlinkJedisPoolConfig jedisPoolConfig = RedisUtil.getRedis(PROD_OR_TEST);
        Properties properties = KafkaProperties.getConsumerProps(PROD_OR_TEST);
        properties.put("group.id", GROUP_NAME);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(TOPIC_NAME, new SimpleStringSchema(),
                properties);
        DataStream<String> sourceStream = env.addSource(consumer);
        DataStream<OrderVO> orderStream = sourceStream.map(new MapFunction<String, OrderVO>() {
            @Override
            public OrderVO map(String value) {
                OrderVO orderVO = JSONObject.parseObject(value, OrderVO.class);
                String productCategoryCode = orderVO.getProductCategoryCode();
                if (StringUtils.isNotBlank(productCategoryCode) && productCategoryCode.length() >= 2) {
                    //一级品类
                    orderVO.setProductCategoryCode(productCategoryCode.substring(0, 2));
                }
                return orderVO;
            }
        }).filter(new FilTerOldOrderData());;

        //itemIdGMV(orderStream, jedisPoolConfig);
        categoryGMV(orderStream, jedisPoolConfig);
        itemQTY(orderStream, jedisPoolConfig);

        env.execute("order detail job");
    }

    private static void categoryGMV(DataStream<OrderVO> orderStream, FlinkJedisPoolConfig jedisPoolConfig)
            throws IOException {

        WindowedStream<OrderVO, Tuple, TimeWindow> categoryCodeWindowStream = orderStream.keyBy("orderType","productCategoryCode")
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)));
        DataStream<OrderAccumulator> categoryCodeAggStream = categoryCodeWindowStream
                .aggregate(new OrderAndGmvAggregateFunc()).setParallelism(2);
        DataStream<Tuple2<String, String>> categoryCodeResultStream = categoryCodeAggStream.keyBy("orderType","productCategoryCode")
                .process(new OutputCategoryCodeGmvProcessFunc());

        categoryCodeResultStream.addSink(new Redis2Sink<>(jedisPoolConfig,
                new GmvRedisMapper(RedisConstants.setAndGet(PROD_OR_TEST).get("HASH_NAME_CATEGORY_CODE_PREFIX"))))
                .setParallelism(1);
    }


    private static void itemIdGMV(DataStream<OrderVO> orderStream, FlinkJedisPoolConfig jedisPoolConfig)
            throws IOException {

        WindowedStream<OrderVO, Tuple, TimeWindow> itemDayWindowStream = orderStream.keyBy("orderType","itemId")
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)));
        DataStream<OrderAccumulator> itemAggStream = itemDayWindowStream
                .aggregate(new OrderAndGmvAggregateFunc()).setParallelism(2);
        DataStream<Tuple2<String, String>> categoryCodeResultStream = itemAggStream.keyBy("orderType","itemId")
                .process(new OutputItemGmvProcessFunc());

        categoryCodeResultStream.addSink(new Redis2Sink<>(jedisPoolConfig,
                new GmvRedisMapper(RedisConstants.setAndGet(PROD_OR_TEST).get("ZSET_NAME_PRODUCT_QTY_PREFIX"))))
                .setParallelism(1);

    }

    /**
     * 商品的总销量
     * @param orderStream
     * @param jedisPoolConfig
     */
    private static void itemQTY(DataStream<OrderVO> orderStream, FlinkJedisPoolConfig jedisPoolConfig)
            throws IOException {

        WindowedStream<OrderVO, Tuple, TimeWindow> itemIdWindowStream = orderStream.keyBy("itemId")
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)));

        DataStream<Tuple2<String, Long>> itemIdRankStream = itemIdWindowStream
                .aggregate(new ItemSalesAggregateFunc(), new WindowResultFunction("start")).keyBy("itemId")
                .process(new ItemProcessFunc());

        itemIdRankStream.addSink(new Redis2Sink<>(jedisPoolConfig,
                new RankingRedisMapper(RedisConstants.setAndGet(PROD_OR_TEST).get("ZSET_NAME_PRODUCT_QTY_PREFIX"))))
                .setParallelism(1);
    }


}
