package com.connector.redis;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class LookUpTest {
    //DDL语句
    public static final String REDIS_TABLE_DIM_DDL = "" +
            "CREATE TABLE redis_dim (\n" +
            "first String,\n" +
            "name String\n" +
            ") WITH (\n" +
            "  'connector.type' = 'redis',  \n" +
            "  'connector.ip' = '127.0.0.1', \n" +
            "  'connector.port' = '6379', \n" +
            "  'connector.lookup.cache.max-rows' = '10', \n" +
            "  'connector.lookup.cache.ttl' = '10000000', \n" +
            "  'connector.version' = '2.6' \n" +
            ")";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        DataStream<Row> ds = streamEnv.addSource(new RichParallelSourceFunction<Row>() {

            volatile boolean flag = true;

            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                while (flag) {
                    Row row = new Row(2);
                    row.setField(0, 1);
                    row.setField(1, "a");
                    ctx.collect(row);
                    Thread.sleep(1000);
                }

            }

            @Override
            public void cancel() {
                flag = false;
            }
        }).returns(Types.ROW(Types.INT, Types.STRING));

        //注册redis维表
        tableEnv.sqlUpdate(REDIS_TABLE_DIM_DDL);

        //source注册成表
        tableEnv.createTemporaryView("test", ds, "id,first,p.proctime");

        //join语句
        Table table = tableEnv.sqlQuery("select * from   redis_dim ");

        //输出
        tableEnv.toAppendStream(table, Row.class).print("FlinkSql07");

        tableEnv.execute("FlinkSql07");


    }
}