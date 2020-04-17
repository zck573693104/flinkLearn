package com.flink.item;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** 用于输出窗口的结果 */
public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
    //窗口开始还是结束
    private String type;

    public WindowResultFunction(String type) {

        this.type = type;
    }

    @Override
    public void apply(Tuple key,  // 窗口的主键，即 itemId
            TimeWindow window,  // 窗口
            Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
            Collector<ItemViewCount> collector  // 输出类型为 ItemViewCount
    ) throws Exception {

        String itemId = ((Tuple1<String>) key).f0;
        Long count = aggregateResult.iterator().next();
        if ("start".equals(type)){
            collector.collect(ItemViewCount.of(itemId, window.getStart(), count));
        }
        if ("end".equals(type)){
            collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
        }
    }
}