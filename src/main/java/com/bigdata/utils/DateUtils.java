package com.bigdata.utils;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class DateUtils {

    public static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";

    public static long strToLong(String times, String model){
        DateTimeFormatter df = DateTimeFormatter.ofPattern(model);
        return LocalDateTime.parse(times, df).toEpochSecond(ZoneOffset.ofHours(8));
    }

    public static long strToLong(String times){
        DateTimeFormatter df = DateTimeFormatter.ofPattern(YYYY_MM_DD_HH_MM_SS);
        return LocalDateTime.parse(times, df).toEpochSecond(ZoneOffset.ofHours(8));
    }

    public static String getStringByLocalDateTime(LocalDateTime localDateTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(YYYY_MM_DD_HH_MM_SS);
        return localDateTime.format(formatter);
    }
}
