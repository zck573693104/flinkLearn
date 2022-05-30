package com;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public class Test {

    private String getType(Object obj){
        if (obj instanceof JSONArray) {
            return "JSONArray";
        }
        if (obj instanceof JSONObject) {
            return "JSONObject";
        }
        return StringUtils.EMPTY;
    }

    public static void main(String[] args) throws IOException {
        StringBuilder sourceSb = new StringBuilder();
        StringBuilder sinkSb = new StringBuilder();
        File file = new File("/load/data/json.json");
        String res = FileUtils.readFileToString(file, Charset.defaultCharset());
        JSONObject json = JSONObject.parseObject(res);
        sourceSb.append("create table source_").append(file.getName()).append(" (\n");
        sinkSb.append("create table sink_").append(file.getName()).append(" (\n");
        for (String key : json.keySet()) {
            Object obj = json.get(key);
            if (obj instanceof JSONArray) {
                for (Object mapKey : json.getJSONArray(key)) {
                    sourceSb.append(key).append(" array<row<\n");
                    for (String sonKey : JSONObject.parseObject(mapKey.toString()).keySet()) {
                        sourceSb.append(sonKey).append(" string,\n");
                        sinkSb.append(sonKey).append(" string,\n");
                    }
                    sourceSb.deleteCharAt(sourceSb.lastIndexOf(","));
                    sourceSb.deleteCharAt(sourceSb.lastIndexOf("\n"));
                    sourceSb.append(">>,\n");
                    break;
                }
            } else if (obj instanceof JSONObject) {
                sourceSb.append(key).append(" row<\n");
                JSONObject.parseObject(obj.toString()).keySet().forEach(sonKey -> sourceSb.append(sonKey).append(" string,\n"));
                JSONObject.parseObject(obj.toString()).keySet().forEach(sonKey -> sinkSb.append(sonKey).append(" string,\n"));
                sourceSb.deleteCharAt(sourceSb.lastIndexOf(","));
                sourceSb.deleteCharAt(sourceSb.lastIndexOf("\n"));
                sourceSb.append(">,\n");
            }
            else {
                sourceSb.append(key).append(" string,\n");
                sinkSb.append(key).append(" string,\n");
            }
        }
        sourceSb.deleteCharAt(sourceSb.lastIndexOf(","));
        sourceSb.append(" ) WITH (\n" +
                "   'connector' = 'kafka',\n" +
                "  'topic' = 'tbox_period',\n" +
                "  'properties.bootstrap.servers' = '${kafka.gb.bootstrap.servers}',\n" +
                "  'properties.group.id' = 'enterprise_tel_ul_tx',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ");");
        System.out.println(sourceSb);


        sinkSb.deleteCharAt(sinkSb.lastIndexOf(","));
        sinkSb.append(" ) WITH (\n" +
                "   'connector' = 'kafka',\n" +
                "  'topic' = 'hycan-period-compress',\n" +
                "  'properties.bootstrap.servers' = '${kafka.gb.bootstrap.servers}',\n" +
                "  'properties.group.id' = 'enterprise_tel_ul_tx',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ");");
        System.out.println(sinkSb);
    }
}
