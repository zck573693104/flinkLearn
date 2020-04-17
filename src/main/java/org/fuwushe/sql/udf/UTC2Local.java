package org.fuwushe.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

public class UTC2Local extends ScalarFunction {

    public Timestamp eval(Timestamp value) {

        long timestamp = value.getTime() + 28800000;

        return new Timestamp(timestamp);
    }

}