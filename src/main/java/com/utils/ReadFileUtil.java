package com.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

public class ReadFileUtil {

    public static String[] readFileByLines(String fileName) throws IOException {
        String res = FileUtils.readFileToString(new File(fileName), Charset.defaultCharset());
        return res.split(";");
    }

    public static void main(String []args) throws IOException {
        System.out.println(Arrays.toString(readFileByLines("/load/data/flink_csv.sql")));
    }
}



