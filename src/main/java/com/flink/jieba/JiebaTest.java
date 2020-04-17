package com.flink.jieba;

import com.huaban.analysis.jieba.JiebaSegmenter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JiebaTest {
    private static final JiebaSegmenter jiebaSegmenter = new JiebaSegmenter();
    public static void main(String[] args) throws Exception {


   ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

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
        env.fromCollection(new ArrayList<>(
                Arrays.asList("一颗白菜","一颗","3","1"))).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                List<String> values = jiebaSegmenter.sentenceProcess(s);
                for (String value:values){
                    System.out.println(value);
                }
                return values.size()>=2?true:false;
            }
        }).flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String,Integer>> out) throws Exception {
                out.collect(new Tuple2<>(s,1));
            }
        }).sum(1).print();
    }
}
