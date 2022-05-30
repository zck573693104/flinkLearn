package com.hycan.bigdata.utils;


import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

public class MyTimeAssignerUtils implements SerializableTimestampAssigner<String> {

    private final String timestampColumn;

    public MyTimeAssignerUtils(String timestampColumn) {
        this.timestampColumn = timestampColumn;
    }

    /**
     * 提取数据里的timestamp字段为时间戳
     * @param element event
     * @param recordTimestamp element 的当前内部时间戳，或者如果没有分配时间戳，则是一个负数
     * @return The new timestamp.
     */
    @Override
    public long extractTimestamp(String element, long recordTimestamp) {
        String times = JSONObject.parseObject(element,JSONObject.class).getString(timestampColumn);
        return DateUtils.strToLong(times);
    }
}