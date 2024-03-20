package com.bigdata.utils;

import com.google.common.base.Splitter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Pattern;

/**
 * 加载SQL文件并且替换key
 */
public class ReadFileUtils {

    private static final Pattern pattern = Pattern.compile(";(?=(?:[^']*'[^']*')*[^']*$)");

    public static List<String> readFileByLines(String fileName, String activeProfile) throws IOException {
        String res = FileUtils.readFileToString(new File(fileName), Charset.defaultCharset());
        return Splitter.on(pattern).splitToList(initKey(activeProfile, res));
    }

    private static String initKey(String activeProfile, String res) throws IOException {
        InputStream in = ReadFileUtils.class.getResourceAsStream("/" + activeProfile + ".properties");
        Properties properties = new Properties();
        properties.load(in);
        Set<Object> keys = properties.keySet();
        for (Object key : keys) {
            res = res.replace("${" + key + "}", properties.getProperty(key.toString()));
        }
        return res;
    }

    public static void main(String[] args) throws IOException {
        Map<String, Integer> map = new TreeMap<>();
        for (String value : FileUtils.readLines(new File("/load/data/type.txt"))) {
            String key = value.split("_")[0];
            if (map.containsKey(key)) {
                map.put(key, map.get(key) + 1);
            } else {
                map.put(key, 1);
            }
        }
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println(entry.getValue());
        }
        System.out.println(readFileByLines("/load/data/flink_sql_test.sql", "local"));
    }
}



