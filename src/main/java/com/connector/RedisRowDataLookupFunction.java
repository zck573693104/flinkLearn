package com.connector;

import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import redis.clients.jedis.Jedis;

public class RedisRowDataLookupFunction extends TableFunction<RowData> {
    private static final long serialVersionUID = 1L;

    private final ReadableConfig options;
    private final String command;
    private transient Jedis jedis;

    public RedisRowDataLookupFunction(ReadableConfig options) {
        Preconditions.checkNotNull(options, "No options supplied");
        this.options = options;
        command = options.get(RedisOptions.COMMAND).toUpperCase();
        Preconditions.checkArgument(command.equals("GET") || command.equals("HGET"), "Redis table source only supports GET and HGET commands");
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.jedis = new Jedis(options.get(RedisOptions.HOST), options.get(RedisOptions.PORT));
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    public void eval(Object obj) {
        RowData lookupKey = GenericRowData.of(obj);
        StringData key = lookupKey.getString(0);
        String value = command.equals("GET") ? jedis.get(key.toString()) : "default";
        RowData result = GenericRowData.of(key, StringData.fromString(value));
        collect(result);
    }
}