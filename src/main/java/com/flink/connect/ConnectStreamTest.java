package com.flink.connect;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class ConnectStreamTest {

    public static void main(String[] args) throws Exception {
        //1、连接 DataStream
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> src1 = env.fromElements("1,3");
        DataStream<String> src2 = env.fromElements("2,4");
        ConnectedStreams<String, String> connected = src1.connect(src2);
        connected.flatMap(new CoFlatMapFunction<String, String, String>() {
            @Override
            public void flatMap1(String longLongTuple2, Collector<String> collector) throws Exception {
                collector.collect(longLongTuple2);
            }

            @Override
            public void flatMap2(String longLongTuple2, Collector<String> collector) throws Exception {
                collector.collect(longLongTuple2);
            }
        }).print();
        DataStream<String> un = src1.union(src2);
        un.print();
        //2、连接 BroadcastStream
        MapStateDescriptor<String, Long> ruleStateDescriptor = new MapStateDescriptor<>("RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<Long>() {
        }));
        DataStream<String> src3 = env.fromElements("5");
        DataStream<String> src4 = env.fromElements("6");
        final BroadcastStream<String> broadcast = src4.broadcast(ruleStateDescriptor);
        BroadcastConnectedStream<String, String> connect = src3.connect(broadcast);

        env.execute("connect-test");
    }

}
