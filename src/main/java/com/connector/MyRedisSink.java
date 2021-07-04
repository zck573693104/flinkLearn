package com.connector;
 
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import redis.clients.jedis.Jedis;
 
/**
 * 自定义 sink 写入 redis
 **/
public class MyRedisSink extends RichSinkFunction<RowData> {
 
    private final String host;
    private final int port;
    private int expire;
    private Jedis jedis;
 
    public MyRedisSink(String host, int port, int expire) {
        this.host = host;
        this.port = port;
        this.expire = expire;
    }
 
    @Override
    public void open(Configuration parameters) throws Exception {
        this.jedis = new Jedis(host, port);
    }
 
    @Override
    public void invoke(RowData value, Context context) throws Exception {
        this.jedis.set(String.valueOf(value.getString(0)), String.valueOf(value.getInt(1)), "NX", "EX", expire);
    }
 
    @Override
    public void close() throws Exception {
        this.jedis.close();
    }
}