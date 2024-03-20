package com.bigdata.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;

@FunctionHint(output = @DataTypeHint("ROW<type STRING,value STRING>"))
public class ParserSpecialUDTF extends TableFunction<Row> {
    /**
     * brake 刹车
     * hard_to 减速
     * unsafe_follow 不安全跟车
     * force_to_quit 强制退出
     */
    private static final String[] TYPES = new String[]{"brake", "hard_to","unsafe_follow", "force_to_quit"};
    private static final Logger LOGGER = Logger.getLogger(ParserSpecialUDTF.class);

    public void eval(String values) {
        try {
            String[] datas = values.split(",");
            for (int i = 0; i < datas.length; i++) {
                collect(Row.of(TYPES[i], datas[i]));
            }
        } catch (Exception e) {
            LOGGER.error("ParserSpecialUDTF error:" + e.getMessage());
        }
    }
}