package com.connector;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import redis.clients.jedis.Jedis;

public class RedisSourceFunction extends RichSourceFunction<RowData> {
    private final ReadableConfig options;
    private transient Jedis jedis;
    public RedisSourceFunction(ReadableConfig options) {
        this.options = options;
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        // 暂无实现
        ctx.collect(GenericRowData.of(StringData.fromString("demo")));
    }

    @Override
    public void cancel() {
        jedis.close();
    }
}
