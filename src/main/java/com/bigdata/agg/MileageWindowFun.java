package com.bigdata.agg;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class MileageWindowFun implements WindowFunction<Long, Tuple2<String, Long>, String, GlobalWindow> {
    @Override
    public void apply(String key, GlobalWindow window, Iterable<Long> input, Collector<Tuple2<String, Long>> out) throws Exception {
        out.collect(new Tuple2<>(key, input.iterator().next()));
    }
}
