package org.fuwushe.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class EventTimeWindowCount {
    private static String PROD_OR_TEST;


    public static void main(String[] args) throws Exception {        // 获取作业名
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();        // 设置使用 EventTime 作为时间戳(默认是 ProcessingTime)
        env.setStreamTimeCharacteristic(
                TimeCharacteristic.EventTime);        // 开启 Checkpoint（每 10 秒保存一次检查点，模式为 Exactly Once）
        env.enableCheckpointing(10000);
        Properties props = KafkaProperties.getConsumerProps(PROD_OR_TEST);
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>("test_topic", new SimpleStringSchema(), props);
        DataStream<String> stream = env.addSource(consumer);        // 默认接上次开始消费，以下的写法（setStartFromLatest）可以从最新开始消费，相应的还有（setStartFromEarliest 从最旧开始消费）
        // DataStream<String> stream = env.addSource(consumer.getInstance("log.orderlog", new SimpleStringSchema()).setStartFromLatest());

        DataStream<String> orderAmount =                // 将读入的字符串转化为 OrderRecord 对象
                stream.map(
                        new ParseOrderRecord())                        // 设置从 OrderRecord 对象中提取时间戳的方式，下文 BoundedOutOfOrdernessGenerator 类中具体实现该方法
                        .assignTimestampsAndWatermarks(
                                new BoundedOutOfOrdernessGenerator())                        // 用 OrderRecord 对象的 action 字段进行分流（相同 action 的进入相同流，不同 action 的进入不同流）
                        .keyBy("action")                        // 触发 10s 的滚动窗口，即每十秒的数据进入同一个窗口
                        .window(TumblingEventTimeWindows.of(Time.seconds(
                                10)))                        // 将同一窗口的每个 OrderRecord 对象的 count 字段加起来（其余字段只保留第一个进入该窗口的，后进去的丢弃）
                        .sum("count")                        // 将结果从 OrderRecord 对象转换为 String，每十万条输出一条
                        .flatMap(
                                new ParseResult());                        // 如果想每条都输出来，那就输得慢一点，每 10 秒输出一条数据（请将上一行的 flatMap 换成下一行的 map）
        // .map(new ParseResultSleep());

        // 输出结果（然后就可以去 Task Manage 的 Stdout 里面看）
        // 小数据量测试的时候可以这么写，正式上线的时候不要这么写！数据量大建议还是写到 Kafka Topic 或者其他的下游里面去
        orderAmount.print();
        env.execute("test");
    }

    public static class ParseOrderRecord implements MapFunction<String, OrderRecord> {

        @Override
        public OrderRecord map(String s) throws Exception {

            JSONObject jsonObject = JSON.parseObject(s);
            long id = jsonObject.getLong("id");
            int dealId = jsonObject.getInteger("dealid");
            String action = jsonObject.getString("_mt_action");
            double amount = jsonObject.getDouble("amount");
            String timestampString = jsonObject.getString("_mt_datetime");            // 将字符串格式的时间戳解析为 long 类型，单位毫秒
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date timestampDate = simpleDateFormat.parse(timestampString);
            long timestamp = timestampDate.getTime();
            return new OrderRecord(id, dealId, action, amount, timestamp);
        }
    }

    public static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<OrderRecord> {

        private final long maxOutOfOrderness = 3500; // 3.5 seconds

        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(OrderRecord record,
                long previousElementTimestamp) {            // 将数据中的时间戳字段（long 类型，精确到毫秒）赋给 timestamp 变量，此处是 OrderRecord 的 timestamp 字段
            long timestamp = record.timestamp;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {            // return the watermark as current highest timestamp minus the out-of-orderness bound
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }
    }

    public static class ParseResult implements FlatMapFunction<OrderRecord, String> {

        private static long msgCount = 0;

        @Override
        public void flatMap(OrderRecord record, Collector<String> out)
                throws Exception {            // 每十万条输出一条，防止输出太多在 Task Manage 的 Stdout 里面刷新不出来
            if (msgCount == 0) {
                out.collect("Start from: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(record.timestamp)
                        + " action: " + record.action + " count = " + record.count);
                msgCount = 0;
            }
            msgCount++;
            msgCount %= 100000;
        }
    }

    public static class ParseResultSleep implements MapFunction<OrderRecord, String> {

        @Override
        public String map(OrderRecord record)
                throws Exception {            // 每 10 秒输出一条数据，防止输出太多在 Task Manage 的 Stdout 里面刷新不出来
            // 正式上线的时候不要这么写！
            Thread.sleep(10000);
            return "Start from: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(record.timestamp) + " action: "
                    + record.action + " count = " + record.count;
        }
    }

    public static class OrderRecord {

        public long id;

        public int dealId;

        public String action;

        public double amount;

        public long timestamp;

        public long count;

        public OrderRecord() {

        }

        public OrderRecord(long id, int dealId, String action, double amount, long timestamp) {

            this.id = id;
            this.dealId = dealId;
            this.action = action;
            this.amount = amount;
            this.timestamp = timestamp;
            this.count = 1;
        }
    }
}

