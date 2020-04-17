package org.fuwushe.sql;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.fuwushe.kafka.KafkaProperties;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;


/**
 * Summary:
 *  时态表(Temporal Table)
 */
@Slf4j
public class TemporalTableTest {

    public static void main(String[] args) throws Exception {

        //2、设置运行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv,settings);
        streamEnv.setParallelism(1);
        //3、注册Kafka数据源
        //注意: 为了在北京时间和时间戳之间有直观的认识，这里的UserBrowseLog中增加了一个字段eventTimeTimestamp作为eventTime的时间戳
        Properties browseProperties = KafkaProperties.getLocal();
        browseProperties.put("group.id", "browTest");
        DataStream<UserBrowseLog> browseStream = streamEnv
                .addSource(new FlinkKafkaConsumer<>("browTopic", new SimpleStringSchema(), browseProperties))
                .map(new MapFunction<String, UserBrowseLog>() {
                    @Override
                    public UserBrowseLog map(String value) throws Exception {

                        UserBrowseLog log = JSON.parseObject(value, UserBrowseLog.class);

                        // 增加一个long类型的时间戳
                        // 指定eventTime为yyyy-MM-dd HH:mm:ss格式的北京时间
                        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        LocalDateTime eventTime = LocalDateTime.parse(log.getEventTime(), format).plusHours(8);
                        // 转换成毫秒时间戳
                        long eventTimeTimestamp = eventTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
                        log.setEventTimeTimestamp(eventTimeTimestamp);
                        return log;
                    }
                })
                .assignTimestampsAndWatermarks(new BrowseTimestampExtractor(Time.seconds(0)));

        tableEnv.registerDataStream("browse", browseStream,
                "userID,eventTime,eventType,productID,productPrice,browseRowtime.rowtime");
        tableEnv.toAppendStream(tableEnv.scan("browse"),Row.class).print();

        //4、注册时态表(Temporal Table)
        //注意: 为了在北京时间和时间戳之间有直观的认识，这里的ProductInfo中增加了一个字段updatedAtTimestamp作为updatedAt的时间戳
        Properties productInfoProperties = KafkaProperties.getLocal();
        productInfoProperties.put("group.id", "proGroup");
        DataStream<ProductInfo> productInfoStream = streamEnv
                .addSource(new FlinkKafkaConsumer<>("proTopic", new SimpleStringSchema(), productInfoProperties))
                .map(new MapFunction<String, ProductInfo>() {
                    @Override
                    public ProductInfo map(String value) throws Exception {
                        ProductInfo log = JSON.parseObject(value, ProductInfo.class);

                        // 增加一个long类型的时间戳
                        // 指定eventTime为yyyy-MM-dd HH:mm:ss格式的北京时间
                        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        LocalDateTime eventTime = LocalDateTime.parse(log.getUpdatedAt(), format).plusHours(8);
                        // 转换成毫秒时间戳
                        long eventTimeTimestamp = eventTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
                        log.setUpdatedAtTimestamp(eventTimeTimestamp);

                        return log;
                    }
                })
                .assignTimestampsAndWatermarks(new ProductInfoTimestampExtractor(Time.seconds(0)));

        tableEnv.registerDataStream("productInfo", productInfoStream,
                "productID,productName,productCategory,updatedAt,updatedAtTimestamp,productInfoRowtime.rowtime");
        //设置Temporal Table的时间属性和主键
        TemporalTableFunction productInfo = tableEnv.scan("productInfo")
                .createTemporalTableFunction("productInfoRowtime", "productID");
        //注册TableFunction
        tableEnv.registerFunction("productInfoFunc", productInfo);
        tableEnv.toAppendStream(tableEnv.scan("productInfo"),Row.class).print();

        //5、运行SQL
        String sql = "" + "SELECT browse.eventTime,productInfo.updatedAt,productInfo.productCategory, productInfo.updatedAtTimestamp  "
                + " FROM  browse, "
                + " LATERAL TABLE (productInfoFunc(browse.browseRowtime)) as productInfo WHERE "
                + " browse.productID = productInfo.productID";

        Table table = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(table, Row.class).print();

        //6、开始执行
        tableEnv.execute(TemporalTableTest.class.getSimpleName());


    }


    /**
     * 提取时间戳生成水印
     */
    static class BrowseTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserBrowseLog> {

        BrowseTimestampExtractor(Time maxOutOfOrderness) {

            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(UserBrowseLog element) {

            return element.getEventTimeTimestamp();
        }
    }




    /**
     * 提取时间戳生成水印
     */
    static class ProductInfoTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<ProductInfo> {

        ProductInfoTimestampExtractor(Time maxOutOfOrderness) {

            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(ProductInfo element) {

            return element.getUpdatedAtTimestamp();
        }
    }

    //{"productID":"product_5","productName":"name50","productCategory":"category50","updatedAt":"2016-01-01 00:00:00"}
    @Data
    public static class ProductInfo {

        public String productID;

        public String productName;

        public String productCategory;

        public String updatedAt;

        public long updatedAtTimestamp;

        public long productInfoRowtime;
    }

    //{"userID": "user_1", "eventTime": "2016-01-01 00:00:00", "eventType": "browse", "productID": "product_5", "productPrice": 20}
    @Data
    public static class UserBrowseLog {

        public String userID;

        public String eventTime;

        public String eventType;

        public String productID;

        public int productPrice;

        public long eventTimeTimestamp;

        public long browseRowtime;

    }
}
