package org.fuwushe;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.StringTokenizer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
public class DistinctAndGroupByIcon {
    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000); //多久checkpoint一次
        env.getCheckpointConfig().setCheckpointTimeout(60000);//checkpoint超时时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);//两个checkpoint的时间，不能小于500ms
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);//允许checkpoint的时候失败
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);//如果当前进程正在checkpoint，系统不会触发另一个checkpoint
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);//确保数据严格一次
        StateBackend fsStateBackend = new FsStateBackend("file:///Users/huzechen/Downloads/FinkStudy/src/main/resources");
        env.setStateBackend(fsStateBackend);//由于我是本地环境，没有设置RocksDB
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);//当作业被取消时,保留外部的checkpoint
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        String topic = "flink2";
        properties.setProperty("group.id", "test-flink");
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer(topic, new SimpleStringSchema(), properties);
        //map函数解析kafka日志
        DataStream<Tuple4<String, String, String, String>> mapText = env.addSource(consumer).map(new MapFunction<String, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> map(String s) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    Date date = new Date(Long.parseLong(jsonObject.get("event_time").toString()));
                    String dt = new SimpleDateFormat("yyyyMMdd").format(date);
                    String hour = new SimpleDateFormat("HH").format(date);
                    return new Tuple4(jsonObject.get("sid"), jsonObject.get("icon"), dt, hour);
                }catch (Exception e) {
                    //捕获异常脏数据，返回空字符串，后边过滤异常数据
                    return new Tuple4("", "", "", "");
                }
            }
        }).filter(new FilterFunction<Tuple4<String, String, String, String>>() {
            @Override
            public boolean filter(Tuple4<String, String, String, String> o) throws Exception {
                return o.f0 != "";
            }
        });
        //状态函数去重复
        DataStream<Tuple4<String, String, String,Long>> dsitinctText = mapText.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple4<String, String, String, String>>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Tuple4<String, String, String, String> element) {
                return System.currentTimeMillis();
            }
        }).keyBy(0,1).process(new ProcessFunction<Tuple4<String, String, String, String>, Tuple4<String, String, String,Long>>() {
            private ValueState<Tuple2<String,String>> state;
            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = StateTtlConfig
                        .newBuilder(org.apache.flink.api.common.time.Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();
                ValueStateDescriptor<Tuple2<String,String>> stateDescriptor = new ValueStateDescriptor<Tuple2<String,String>>("myState", TypeInformation.of(new TypeHint<Tuple2<String,String>>() {}));
                stateDescriptor.enableTimeToLive(ttlConfig);
                state = getRuntimeContext().getState(stateDescriptor);
            }
            @Override
            public void processElement(Tuple4<String, String, String, String> value, Context ctx, Collector<Tuple4<String, String, String,Long>> out) throws Exception {
                if (state.value() == null) {
                    out.collect(new Tuple4(value.f1, value.f2, value.f3,1L));
                    state.update(new Tuple2(value.f0,value.f1));
                }
            }
        });
        //窗口聚合按唯独求和打印
        dsitinctText.keyBy(0,1,2).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(3).map(new MapFunction<Tuple4<String,String,String,Long>, String>() {
            @Override
            public String map(Tuple4<String, String, String, Long> o) throws Exception {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("icon",o.f0);
                jsonObject.put("dt",o.f1);
                jsonObject.put("hour",o.f2);
                jsonObject.put("sid_num",o.f3);
                return jsonObject.toJSONString();
            }
        }).print();
        env.execute();
    }
}
