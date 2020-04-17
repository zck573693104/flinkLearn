package org.fuwushe.sql;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SqlTop {
    public static void main(String []args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv,settings);
        streamEnv.setParallelism(1);
        String path = SqlTop.class.getClassLoader().getResource("sale.txt").
                getPath();
        DataStream<Sale> dataStream = streamEnv
                .readTextFile(path).map(string -> JSONObject.parseObject(string,Sale.class));
       tableEnv.registerDataStream("shop_sales",dataStream," storeId, category,sales");
    //tableEnv.toAppendStream(tableEnv.sqlQuery("select * from shop_sales"),Sale.class).print();
     Table table =   tableEnv.sqlQuery(" SELECT * FROM (  "
             + " SELECT storeId, category,sales ,    ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS rownum" + "  FROM shop_sales) a "
             + " WHERE rownum <= 2");
     tableEnv.toRetractStream(table, Row.class).print();
     streamEnv.execute();
    }
    @Data
    public static class Sale  {
        public Integer storeId;
        public Integer category;
        public Integer sales;
    }
}
