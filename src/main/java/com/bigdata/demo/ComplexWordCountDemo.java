package com.bigdata.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;

public class ComplexWordCountDemo {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        // 使用自定义数据源生成数据
        DataStream<String> text = env.addSource(new CustomDataSource());

        // 执行单词计数
        DataStream<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new Tokenizer())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new WordCountProcessWindowFunction());

        // 输出结果
        wordCounts.print();

        // 执行任务
        env.execute("Complex Word Count Demo");
    }

    // 自定义数据源
    public static class CustomDataSource implements SourceFunction<String> {
        private volatile boolean isRunning = true;
        private final Random random = new Random();

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                String[] words = {"Hello", "Flink", "DataStream", "Window", "State", "Complex"};
                String word = words[random.nextInt(words.length)];
                ctx.collect(word);
                Thread.sleep(1000); // 每秒生成一个单词
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
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

    // 自定义窗口处理函数
    public static class WordCountProcessWindowFunction extends ProcessWindowFunction<
            Tuple2<String, Integer>, // 输入类型
            Tuple2<String, Integer>, // 输出类型
            String, // 键类型
            TimeWindow> { // 窗口类型

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) {
            int sum = 0;
            for (Tuple2<String, Integer> element : elements) {
                sum += element.f1;
            }
            out.collect(new Tuple2<>(key, sum));
        }
    }

    // 自定义 KeyedProcessFunction 示例，用于状态管理
    public static class StatefulProcessFunction extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {
        private transient ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                    "count", // 状态名称
                    Types.INT // 状态类型
            );
            countState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            Integer currentCount = countState.value();
            if (currentCount == null) {
                currentCount = 0;
            }
            currentCount += value.f1;
            countState.update(currentCount);
            out.collect(new Tuple2<>(value.f0, currentCount));
        }
    }
}
