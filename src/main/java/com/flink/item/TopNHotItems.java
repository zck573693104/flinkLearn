package com.flink.item;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/** 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串 */
public class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, List<Result>> {
    private static final DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final int topSize;

    public TopNHotItems(int topSize) {

        this.topSize = topSize;
    }

    // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
    private ListState<ItemViewCount> itemState;

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);
        ListStateDescriptor<ItemViewCount> itemsStateDesc = new ListStateDescriptor<>("itemState-state",
                ItemViewCount.class);
        itemState = getRuntimeContext().getListState(itemsStateDesc);
    }

    @Override
    public void processElement(ItemViewCount input, Context context, Collector<List<Result>> collector) throws Exception {

        // 每条数据都保存到状态中
        itemState.add(input);
        // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
        context.timerService().registerEventTimeTimer(input.windowEnd + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<Result>> out) throws Exception {
        // 获取收到的所有商品点击量
        List<ItemViewCount> allItemList = new ArrayList<>();
        List<Result> resultList = new ArrayList<>(topSize);
        for (ItemViewCount item : itemState.get()) {
            allItemList.add(item);
        }
        // 提前清除状态中的数据，释放空间
        itemState.clear();
        // 按照点击量从大到小排序
        allItemList = allItemList.stream().sorted(Comparator.comparing(ItemViewCount::getViewCount).reversed()).limit(topSize).collect(Collectors.toList());
        for (int i = 0; i < allItemList.size() ; i++) {
            ItemViewCount currentItem = allItemList.get(i);
            Result results = new Result();
            results.setItemId(currentItem.itemId);
            results.setViewCount(currentItem.viewCount);
            results.setWindowEndDate(sdf.format(new Timestamp(timestamp)));
            resultList.add(results);
        }
        out.collect(resultList);
    }
}