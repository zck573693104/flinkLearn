package com.bigdata.utils;

import java.math.BigDecimal;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class DateUtils {

    public static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(YYYY_MM_DD_HH_MM_SS);

    public static final String YYYY_MM_DD = "yyyy-MM-dd";
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(YYYY_MM_DD);

    public static void main(String[] args) {
//        System.out.println(getStringByLocalDateTime(LocalDateTime.of(2023, 1, 1, 1, 1, 1)));
        System.out.println(getLocalDateTimeByLong(1520754566856L).toInstant(ZoneOffset.of("+8")).toEpochMilli());
        System.out.println(getLocalDateTimeByLong(1520754566856L));
        System.out.println(getStringByLong(1520754566856L));
        System.out.println(getDateStringByLocalDate(LocalDate.now()));
        System.out.println(new BigDecimal("-1.171875E-4").toString());
        System.out.println(new BigDecimal("-0.0001171875").toString());
        System.out.println(new BigDecimal("-1").toString());
        System.out.println(new BigDecimal("-2.0").toString());
        System.out.println(new BigDecimal("-2.11").toString());
        System.out.println(new BigDecimal("3").toString());
    }

    public static long strToLong(String times, String model) {
        DateTimeFormatter df = DateTimeFormatter.ofPattern(model);
        return LocalDateTime.parse(times, df).toEpochSecond(ZoneOffset.ofHours(8));
    }

    public static LocalDateTime strToLocalDateTime(String dateStr) throws ParseException {
        return LocalDateTime.parse(dateStr, DATE_TIME_FORMATTER);
    }

    public static long strToLong(String times) {
        return LocalDateTime.parse(times, DATE_TIME_FORMATTER).toEpochSecond(ZoneOffset.ofHours(8));
    }

    public static String getDateStringByLocalDateTime(LocalDateTime localDateTime) {
        return localDateTime.format(DATE_FORMATTER);
    }

    public static String getDateStringByLocalDate(LocalDate localDate) {
        return localDate.format(DATE_FORMATTER);
    }
    public static String getStringByLocalDateTime(LocalDateTime localDateTime) {
        return localDateTime.format(DATE_TIME_FORMATTER);
    }

    public static String getStringByLong(long timestamp) {
        return getStringByLocalDateTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()));
    }

    public static LocalDateTime getLocalDateTimeByLong(long time) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault());
    }
}
