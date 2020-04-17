package org.fuwushe.demo;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

public class CogroupFunctionDemo02 {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Tuple2<String,String>> input1= env.fromElements(Tuple2.of("1", "1"), Tuple2.of("2", "2"), Tuple2.of("3", "3"));

        DataStream<Tuple2<String,String>> input2=env.fromElements(Tuple2.of("2", "4"), Tuple2.of("3", "5"), Tuple2.of("4", "6"));;

        input1.coGroup(input2)
                .where(new KeySelector<Tuple2<String,String>, Object>() {

                    @Override
                    public Object getKey(Tuple2<String, String> value) throws Exception {
                        return value.f0;
                    }
                }).equalTo(new KeySelector<Tuple2<String,String>, Object>() {

            @Override
            public Object getKey(Tuple2<String, String> value) throws Exception {
                return value.f0;
            }
        }).window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
                .trigger(CountTrigger.of(1))
                .apply(new CoGroupFunction<Tuple2<String,String>, Tuple2<String,String>, Object>() {

                    @Override
                    public void coGroup(Iterable<Tuple2<String, String>> iterable, Iterable<Tuple2<String, String>> iterable1, Collector<Object> collector) throws Exception {
                        StringBuffer buffer=new StringBuffer();
                        buffer.append("DataStream frist:\n");
                        for(Tuple2<String,String> value:iterable){
                            buffer.append(value.f0+"=>"+value.f1+"\n");
                        }
                        buffer.append("DataStream second:\n");
                        for(Tuple2<String,String> value:iterable1){
                            buffer.append(value.f0+"=>"+value.f1+"\n");
                        }
                        collector.collect(buffer.toString());
                    }
                }).print();

        env.execute();
    }
}
