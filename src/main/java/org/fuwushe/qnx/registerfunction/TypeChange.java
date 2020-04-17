package org.fuwushe.qnx.registerfunction;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class TypeChange extends ScalarFunction {
    /**
     * 为null，但是数组有长度
     * @param rows
     * @return
     */
    public String eval(Row [] rows){
        return JSONObject.toJSONString(rows);
    }

}
