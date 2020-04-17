package com.flink.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;
import com.flink.utils.DateUtil;

import java.sql.Timestamp;
import java.util.Date;

public class FromUnixTimeUDF extends ScalarFunction {
    public String DATE_FORMAT;

    public FromUnixTimeUDF() {
        this.DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    }

    public FromUnixTimeUDF(String dateFormat) {
        this.DATE_FORMAT = dateFormat;
    }

    public String eval(String longTime) {
        try {
            Date date = new Date(Long.parseLong(longTime) * 1000);
            return DateUtil.dateToStr(date,DateUtil.YYYY_MM_DD_HH_MM_SS);
        } catch (Exception e) {
            return null;
        }
    }

    public String eval(Timestamp value) {
        try {
            Date date = new Date(value.getTime() + 28800000);
            return DateUtil.dateToStr(date,DateUtil.YYYY_MM_DD_HH_MM_SS);
        } catch (Exception e) {
            return null;
        }
    }
}
