package com.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class ReadFileUtil {

    public static String[] readFileByLines(String fileName) throws IOException {
        File file = new File(fileName);
        BufferedReader reader = null;
        StringBuilder sb = new StringBuilder();
        reader = new BufferedReader(new FileReader(file));
        String tempString = null;
        while ((tempString = reader.readLine()) != null) {
            sb.append(tempString);
        }
        reader.close();
        return sb.toString().split(";");
    }

    public static void main(String []args) throws IOException {
        System.out.println(readFileByLines("/load/data/pre.txt").toString());
    }
}



