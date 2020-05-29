package com.flink.qnx;

import com.flink.qnx.registerfunction.*;
import com.flink.sql.udf.FromUnixTimeUDF;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class RegisterFunctionUtil {
    public static void register(StreamTableEnvironment tableEnv){
        tableEnv.registerFunction("ip_to_int", new IpToInt());
        tableEnv.registerFunction("if_null", new IfNull());
        tableEnv.registerFunction("line_to_column", new LineToColumn());
        tableEnv.registerFunction("column_to_line", new ColumnToLine());
        tableEnv.registerFunction("from_unixtime", new FromUnixTimeUDF());
        tableEnv.registerFunction("type_change", new TypeChange());
        tableEnv.registerFunction("udtf", new MyUDTF());

    }
}
