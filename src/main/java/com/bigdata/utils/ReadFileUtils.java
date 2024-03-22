package com.bigdata.utils;

import org.apache.commons.io.FileUtils;
import org.apache.flink.shaded.guava31.com.google.common.base.Splitter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 加载SQL文件并且替换key
 */
public class ReadFileUtils {

    private static final Pattern pattern = Pattern.compile(";\n");
    // 因为一次性正则会溢出  分隔符处理下
    private static final String SPLIT = "--split--";

    public static List<String> readFileByLines(String fileName, String activeProfile, boolean isObjectStore) throws IOException {
        List<String> lists = new LinkedList<>();
        String res;
        if (isObjectStore) {
            res = ObsUtils.read(fileName);
        }else {
            res = FileUtils.readFileToString(new File(fileName), Charset.defaultCharset());
        }
        String[] deals = res.split(SPLIT);
        for (String deal:deals) {
            deal = deal.replaceAll("(?m)^--.*", "").replaceAll("\r\n","\n");
            deal  = initKey(activeProfile, deal);
            lists.addAll(Splitter.on(pattern).splitToList(deal));
        }
        return lists;
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
//        Map<String, Integer> map = new TreeMap<>();
//        for (String value : FileUtils.readLines(new File("/load/data/type.txt"))) {
//            String key = value.split("_")[0];
//            if (map.containsKey(key)) {
//                map.put(key, map.get(key) + 1);
//            } else {
//                map.put(key, 1);
//            }
//        }
//        for (Map.Entry<String, Integer> entry : map.entrySet()) {
//            System.out.println(entry.getValue());
//        }
        System.out.println(readFileByLines("test.sql", "local", true));
    }
}



