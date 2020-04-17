package com.flink.qnx.registerfunction;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * 列 转 行
 */
public class ColumnToLine extends TableFunction<Row> {

    private String separator = " ";
    private int size = 0;

    public ColumnToLine() {
    }

    public void eval(Row[] row, String separator, Integer size) {
        String value = row.toString();
        value.replace("[", "").replace("]", "");
        int temp = 0;
        String[] results = new String[size];
        List<String> list = new ArrayList<>(size);
        for (String temps : value.split(separator)) {
            if (temp == size) {
                collector.collect(Row.of(results));
                list.clear();
            }
            temp++;
            list.add(temps);


        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING, Types.STRING);
    }
}
