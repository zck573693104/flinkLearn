package com.connector;
 
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

/**
 * 自定义 DynamicTableSink 
 **/
public class RedisDynamicTableSink implements DynamicTableSink {
 
    private ReadableConfig options;
 
    public RedisDynamicTableSink(ReadableConfig options) {
        this.options = options;
    }
 
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }
 
    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        String host = options.getOptional(RedisOptions.HOST).get();
        Integer port = options.getOptional(RedisOptions.PORT).get();
        Integer expire = options.getOptional(RedisOptions.EXPIRE).get();
        MyRedisSink myRedisSink = new MyRedisSink(host, port, expire);
        return SinkFunctionProvider.of(myRedisSink);
    }
 
    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(this.options);
    }
 
    @Override
    public String asSummaryString() {
        return "redis table sink";
    }
}