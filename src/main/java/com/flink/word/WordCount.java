package com.flink.word;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {

 public static void main(String []args) throws Exception {

     final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
     //通过字符串构建数据集
     DataSet<String> text = env.fromElements(
             " Who is there ? ",
             " I think I hear them. Stand, ho ! Who is there ? ");
     text.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
         @Override
         public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
             for (String word : line.split("\\s+")) {
                 out.collect(new Tuple2<>(word, 1));
             }
         }
     }).groupBy(0).sum(1).print();

 }
}
