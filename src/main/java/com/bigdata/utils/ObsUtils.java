package com.bigdata.utils;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.ObsObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

public class ObsUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObsUtils.class);
    private static final String AK = "";
    private static final String SK = "";
    private static final String ENDPOINT = "";
    private static final String BUCKETNAME = "";
    private static final String PATH_PREFIX = "sql/";
    public static void main(String[] args) {
        System.out.println(read("test.sql"));
    }
    public static String read(String fileName){

        ObsClient obsClient = new ObsClient(AK, SK,ENDPOINT);
        try {
            // 流式下载
            ObsObject obsObject = obsClient.getObject(BUCKETNAME, PATH_PREFIX+fileName);
            // 读取对象内容
            InputStream input = obsObject.getObjectContent();
            byte[] b = new byte[1024];
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            int len;
            while ((len = input.read(b)) != -1) {
                bos.write(b, 0, len);
            }
            bos.close();
            input.close();
            return bos.toString();
        } catch (ObsException e) {
            LOGGER.info("getObjectContent failed");
            // 请求失败,打印http状态码
            LOGGER.info("HTTP Code:" + e.getResponseCode());
            // 请求失败,打印服务端错误码
            LOGGER.info("Error Code:" + e.getErrorCode());
            // 请求失败,打印详细错误信息
            LOGGER.info("Error Message:" + e.getErrorMessage());
            // 请求失败,打印请求id
            LOGGER.info("Request ID:" + e.getErrorRequestId());
            LOGGER.info("Host ID:" + e.getErrorHostId());
            LOGGER.error(e.getMessage());
        } catch (Exception e) {
            LOGGER.info("getObjectContent failed");
            // 其他异常信息打印
            LOGGER.error(e.getMessage());
        }
        return StringUtils.EMPTY;
    }
}
