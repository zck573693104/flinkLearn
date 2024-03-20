package com.bigdata.process;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class CountPathProcessFun extends ProcessWindowFunction<JSONObject, String, String, GlobalWindow> {
    private MapState<String, String> state;

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);
        state = this.getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("state_supplier_order_gmv", String.class, String.class));
    }



    @Override
    public void close() throws Exception {
        state.clear();
        super.close();
    }

    @Override
    public void process(String key, Context context, Iterable<JSONObject> iterable, Collector<String> out) throws Exception {
        for (JSONObject json: iterable) {
            System.out.println(json);
        }
        out.collect("ok");
    }
}
