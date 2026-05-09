package com.bigdata.utils;

import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class DateUtils {
    
    public static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern(DATETIME_PATTERN);
    
    public static final String DATE_PATTERN = "yyyy-MM-dd";
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(DATE_PATTERN);
    
    public static final ZoneOffset DEFAULT_ZONE_OFFSET = ZoneOffset.ofHours(8);
    public static final ZoneId DEFAULT_ZONE = ZoneId.systemDefault();
    
    private DateUtils() {
        throw new IllegalStateException("Utility class");
    }
    
    public static long parseToEpochSecond(String timestamp, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return LocalDateTime.parse(timestamp, formatter).toEpochSecond(DEFAULT_ZONE_OFFSET);
    }
    
    public static long parseToEpochSecond(String timestamp) {
        return LocalDateTime.parse(timestamp, DATETIME_FORMATTER).toEpochSecond(DEFAULT_ZONE_OFFSET);
    }
    
    public static LocalDateTime parseToLocalDateTime(String dateStr) throws ParseException {
        return LocalDateTime.parse(dateStr, DATETIME_FORMATTER);
    }
    
    public static String formatDateTime(LocalDateTime localDateTime) {
        return localDateTime.format(DATETIME_FORMATTER);
    }
    
    public static String formatDate(LocalDate localDate) {
        return localDate.format(DATE_FORMATTER);
    }
    
    public static String formatTimestamp(long timestamp) {
        return formatDateTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), DEFAULT_ZONE));
    }
    
    public static LocalDateTime parseTimestamp(long timestamp) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), DEFAULT_ZONE);
    }
}
