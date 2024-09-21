package com.bigdata.utils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

public class RateLimiterUtils {
    public DataStream getSecondRateLimiter(DataStream stream, int rate) {
        RateLimiter rateLimiter = RateLimiter.create(rate); // 每秒处理100个数据
        return stream.filter(data -> {
            boolean allowed = rateLimiter.tryAcquire(); // 尝试获取一个令牌
            return allowed;
        });
    }

    public DataStream getMinnRateLimiter(DataStream stream, int rate) {
       return stream.flatMap(new RichFlatMapFunction() {
            private transient RateLimiter rateLimiter;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                rateLimiter = RateLimiter.create(60);
            }
            @Override
            public void flatMap(Object value, Collector out) throws Exception {
                boolean allowed = rateLimiter.tryAcquire(); // 尝试获取一个令牌
                if (allowed) {
                    out.collect(value); // 通过限流的数据输出
                }
            }
        });
    }
}
