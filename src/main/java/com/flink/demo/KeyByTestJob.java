package com.flink.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.flink.sink.mysql.SinkMybatis;

public class KeyByTestJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        // this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
//        env.fromElements(Tuple2.of(2L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(2L, 4L), Tuple2.of(1L, 2L))
//                .keyBy(0) // 以数组的第一个元素作为key
//                .reduce((ReduceFunction<Tuple2<Long, Long>>) (t2, t1) -> new Tuple2<>(t1.f0, t2.f1 + t1.f1)) // value做累加
//                .print();
//        // this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
//        env.fromElements(Tuple2.of(2L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(2L, 4L), Tuple2.of(1L, 2L))
//                .keyBy(0) // 以数组的第一个元素作为key
//                    .map((MapFunction<Tuple2<Long, Long>, String>) longLongTuple2 -> "key:" + longLongTuple2.f0 + ",value:" + longLongTuple2.f1)
//                .print();
        env.setParallelism(1);
        env.fromElements(Tuple3.of(1, 4,1), Tuple3.of(1, 1, 2), Tuple3.of(1, 3, 3))
                .keyBy(0)
                .minBy(1)
                .print();
        env.fromElements(  Tuple2.of("e",new OrderEvent("1","1")))
                .addSink(new SinkMybatis<>());
        env.execute("execute");
    }
}