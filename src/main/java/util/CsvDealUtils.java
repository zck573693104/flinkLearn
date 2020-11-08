package util;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class CsvDealUtils {
    public static void main(String[] args) throws IOException {
        csvDealSpecial();
    }

    /**
     * 处理 csv存在换行数据
     * @throws IOException
     */
    private static void csvDealSpecial() throws IOException {
        StringBuilder sb = new StringBuilder();
        File sinkFile = new File("/load/data/test_2.csv");
        // true 替换
        boolean whetherReplace = true;
        char[] chars = FileUtils.readFileToString(new File("/load/data/test_1.csv")).toCharArray();
        for (char ch : chars) {
            if (ch == '\r') {
                whetherReplace = false;
            }
            if (ch == '"') {
                sb.append("\"");

            } else {
                String str = whetherReplace ? String.valueOf(ch).replace("\n", " ") : String.valueOf(ch);
                sb.append(str);
            }
        }
        FileUtils.write(sinkFile, sb.toString());
        FileUtils.readLines(sinkFile).forEach(System.out::println);
    }
}
