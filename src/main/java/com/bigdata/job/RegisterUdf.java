package com.bigdata.job;

import com.bigdata.udf.MapToStringUDF;
import com.bigdata.udf.ParserSpecialUDTF;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RegisterUdf {
    /**
     *  udf注册
     * @param tEnv tEnv
     */
    public static void register(StreamTableEnvironment tEnv){
        tEnv.createTemporaryFunction("parse_special_udtf",new ParserSpecialUDTF());
        tEnv.createTemporaryFunction("map_to_string",new MapToStringUDF());
    }
}
