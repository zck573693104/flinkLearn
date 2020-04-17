package org.fuwushe.demo;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class Main {

    private static final OutputTag<String> overFiveTag = new OutputTag<String>("overFive") {
    };
    private static final OutputTag<String> equalFiveTag = new OutputTag<String>("equalFive") {
    };

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.getConfig().setGlobalJobParameters(params);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tokenizer = env.fromElements(WORDS)
                .keyBy(new KeySelector<String, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer getKey(String value) throws Exception {
                        return 0;
                    }
                })
                .process(new Tokenizer());

//        tokenizer.getSideOutput(overFiveTag).print();   //将字符串长度大于 5 的打印出来

//        tokenizer.getSideOutput(equalFiveTag).print();  //将字符串长度等于 5 的打印出来

//        tokenizer.print();  //这个打印出来的是字符串长度小于 5 的

        tokenizer.keyBy(0)
                .sum(1)
                .print();   //做 word count 后打印出来

        env.execute("Streaming WordCount SideOutput");
    }

    public static final class Tokenizer extends ProcessFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");

            for (String token : tokens) {
                if (token.length() > 5) {
                    ctx.output(overFiveTag, token); //将字符串长度大于 5 的 word 放到 overFiveTag 中去
                } else if (token.length() == 5) {
                    ctx.output(equalFiveTag, token); //将字符串长度等于 5 的 word 放到 equalFiveTag 中去
                } else if (token.length() < 5) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }

        }
    }

    public static final String[] WORDS = new String[]{
            "12345 1234 123 123456 123"
    };
}