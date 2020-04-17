package com.flink.qnx.registerfunction;

import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class MyUDTF extends TableFunction<Row> {
    /**
     *
     * @param rows blink执行计划 为null，但是数组有长度 。默认执行计划有值
     */
    public void eval(Row [] rows) {

        collector.collect(Row.of(1, 1));

    }
}