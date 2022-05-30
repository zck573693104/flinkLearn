package com.hycan.bigdata.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Map;
import java.util.stream.Collectors;

public class MapToStringUDF extends ScalarFunction {

    public String eval(Map<String,String> json) {
        if (json == null) {
            return StringUtils.EMPTY;
        }
        return json.entrySet().stream().map(Object::toString).collect(Collectors.joining(","));
    }
}
