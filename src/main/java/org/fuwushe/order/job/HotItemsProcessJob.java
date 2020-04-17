package org.fuwushe.order.job;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.fuwushe.config.EnvUtil;
import org.fuwushe.constant.RedisConstants;
import org.fuwushe.item.CountAgg;
import org.fuwushe.item.UserBehaviorVO;
import org.fuwushe.item.WindowResultFunction;
import org.fuwushe.kafka.KafkaProperties;
import org.fuwushe.kerberos.Kerberos;
import org.fuwushe.order.rich.FilterUserBehaviorVOWithParameters;
import org.fuwushe.order.process.ItemProcessFunc;
import org.fuwushe.order.rich.MapWithParameters;
import org.fuwushe.sink.redis.RankingRedisMapper;
import org.fuwushe.utils.Redis2Sink;
import org.fuwushe.utils.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class HotItemsProcessJob {

    private static Logger logger = LoggerFactory.getLogger(HotItemsProcessJob.class);

    private static String BEHAVIOR;

    private static String REDIS_KEY;

    private static String TOPIC_NAME;

    private static String PROD_OR_TEST;

    private static final String PROD_TOPPIC_NAME = "prodUserBehaviorProcessTopic";

    private static final String TEST_TOPPIC_NAME = "testUserBehaviorProcessTopic";

    private static String GROUP_NAME = "flinkTestPVUserBehaviorGroup";

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if (args.length != 0) {
            Kerberos.getKerberosJaas();
            PROD_OR_TEST = parameterTool.get("prod_or_test");
            TOPIC_NAME = parameterTool.get("topic_name");
            GROUP_NAME = parameterTool.get("group_name");
            BEHAVIOR = parameterTool.get("behavior");
            REDIS_KEY = parameterTool.get("redis_key");
            logger.info("入参 TOPIC_NAME:" + TOPIC_NAME + ":GROUP_NAME:" + GROUP_NAME + ":BEHAVIOR:" + BEHAVIOR
                    + ":PROD_OR_TEST:" + PROD_OR_TEST);
        }
        FlinkJedisPoolConfig jedisPoolConfig = RedisUtil.getRedis(PROD_OR_TEST);
        StreamExecutionEnvironment env = EnvUtil.getProFs(PROD_OR_TEST);
        env.getConfig().setGlobalJobParameters(parameterTool);

        Properties properties = KafkaProperties.getConsumerProps(PROD_OR_TEST);
        properties.put("group.id", GROUP_NAME);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(TOPIC_NAME, new SimpleStringSchema(),
                properties);

        DataStream<UserBehaviorVO> userBehaviorStream = env.addSource(consumer)
                .map(new MapWithParameters());

        DataStream<Tuple2<String, Long>> itemActionRankStream = userBehaviorStream
                // 过滤出只有符合操作的数据
                .filter(new FilterUserBehaviorVOWithParameters()).keyBy("itemId")
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                .aggregate(new CountAgg(), new WindowResultFunction("start")).keyBy("itemId")
                .process(new ItemProcessFunc());
        itemActionRankStream.addSink(new Redis2Sink<>(jedisPoolConfig,
                new RankingRedisMapper(RedisConstants.setAndGet(PROD_OR_TEST).get(REDIS_KEY)))).setParallelism(1);
        env.execute(BEHAVIOR + " Hot Items Job");
    }


}