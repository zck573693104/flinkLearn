package com.flink.order.process;

import com.flink.item.ItemViewCount;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.flink.utils.DateUtil;

import java.util.Date;

public class ItemProcessFunc extends KeyedProcessFunction<Tuple, ItemViewCount, Tuple2<String, Long>> {

    private MapState<String, Long> state;

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);
        state = this.getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("state_item_count", String.class, Long.class));
    }

    @Override
    public void processElement(ItemViewCount itemViewCount, Context context, Collector<Tuple2<String, Long>> out)
            throws Exception {
        String key = DateUtil.dateToStr(new Date(itemViewCount.getWindowEnd()),DateUtil.YYYY_MM_DD)+"@"+itemViewCount.getItemId();
        Long cacheValue = state.get(key);
        if (cacheValue == null || itemViewCount.getViewCount() != cacheValue) {
            state.put(key, itemViewCount.getViewCount());
            out.collect(new Tuple2<>(key, itemViewCount.getViewCount()));
        }

    }

    @Override
    public void close() throws Exception {

        state.clear();
        super.close();
    }
}
