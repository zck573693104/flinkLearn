package com.bigdata.process;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author kcz
 */
public class BrakeProcessFun extends ProcessWindowFunction<JSONObject, String, String, TimeWindow> {
    @Override
    public void process(String key, Context context, Iterable<JSONObject> iterable, Collector<String> out) throws Exception {
        for (JSONObject json: iterable) {
            System.out.println(json);
        }
        out.collect("ok");
    }
}
