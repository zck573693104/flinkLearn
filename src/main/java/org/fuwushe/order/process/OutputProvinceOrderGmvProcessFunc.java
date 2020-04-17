package org.fuwushe.order.process;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.fuwushe.order.OrderAccumulator;

public class OutputProvinceOrderGmvProcessFunc extends KeyedProcessFunction<Tuple, OrderAccumulator, Tuple2<String, String>> {


    private MapState<String, OrderAccumulator> state;

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);
        state = this.getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("state_province_order_gmv", String.class, OrderAccumulator.class));
    }

    @Override
    public void processElement(OrderAccumulator value, Context ctx, Collector<Tuple2<String, String>> out)
            throws Exception {
        if (value==null || value.getOrderIdset()==null){
            return;
        }
        String key = value.getOrderType()+"_"+value.getDate()+"@"+value.getAreaId();
        OrderAccumulator cachedValue = state.get(key);

        if (cachedValue == null || cachedValue.getSubOrderDetailSum()!= value.getSubOrderDetailSum()) {
            JSONObject result = new JSONObject();
            result.put("areaId", value.getAreaId());
            result.put("orderCount", value.getOrderIdset().size());
            result.put("gmv", value.getGmv());
            out.collect(new Tuple2<>(key, result.toJSONString()));
            state.put(key, value);
        }
    }

    @Override
    public void close() throws Exception {

        state.clear();
        super.close();
    }
}
