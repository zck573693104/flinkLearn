package com.bigdata.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountDemo {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> text = env.readTextFile("path/to/input.txt");

        // 执行单词计数
        DataStream<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new Tokenizer())
                .keyBy(0)
                .sum(1);

        // 输出结果
        wordCounts.print();

        // 执行任务
        env.execute("Word Count Demo");
    }

    // 定义 FlatMap 函数
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 规范化并分割行
            String[] words = value.toLowerCase().split("\\W+");

            // 输出单词
            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
