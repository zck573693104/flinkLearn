package org.fuwushe.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OutPutTag {

    public static void main(String []args) throws Exception {
        final OutputTag<String> overFiveTag = new OutputTag<String>("overFive") {
        };
        final OutputTag<String> equalFiveTag = new OutputTag<String>("equalFive") {
        };

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource dataStreamSource = env.fromElements(WORDS);

        SingleOutputStreamOperator operator =  dataStreamSource.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out)  {
                String[] tokens = value.toLowerCase().split("\\s+");
                for (String token:tokens){
                    if (token.length()>5){
                        ctx.output(overFiveTag,token);
                    }
                    else if (token.length()==5){
                        ctx.output(equalFiveTag,token);
                    }
                    else {
                        out.collect(new Tuple2<>(token,1));
                    }
                }
            }
        });
//        operator.getSideOutput(overFiveTag).print();
//        operator.getSideOutput(equalFiveTag).print();
        operator.keyBy(0).sum(1).print();
        //operator.print();

        env.execute();
    }
    public static final String[] WORDS = new String[]{
            "12345 1234 123 123456 123"
    };
}
