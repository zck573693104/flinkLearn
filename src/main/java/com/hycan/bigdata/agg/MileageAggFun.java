package com.hycan.bigdata.agg;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;

public class MileageAggFun implements AggregateFunction<JSONObject, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(JSONObject jsonObject, Long accumulator) {
        //jsonObject.getString()
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
