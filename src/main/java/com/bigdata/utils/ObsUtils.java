package com.bigdata.utils;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.ObsObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class ObsUtils {
    private static final Logger logger = LoggerFactory.getLogger(ObsUtils.class);
    
    private static final String AK;
    private static final String SK;
    private static final String ENDPOINT;
    private static final String BUCKET_NAME;
    private static final String PATH_PREFIX = "sql/";
    
    static {
        AK = System.getenv("OBS_AK");
        SK = System.getenv("OBS_SK");
        ENDPOINT = System.getenv("OBS_ENDPOINT");
        BUCKET_NAME = System.getenv("OBS_BUCKET_NAME");
        
        if (StringUtils.isAnyBlank(AK, SK, ENDPOINT, BUCKET_NAME)) {
            logger.warn("OBS credentials not fully configured via environment variables");
        }
    }
    
    private ObsUtils() {
        throw new IllegalStateException("Utility class");
    }
    
    public static String read(String fileName) {
        if (StringUtils.isBlank(fileName)) {
            logger.error("File name cannot be blank");
            return StringUtils.EMPTY;
        }
        
        String objectKey = PATH_PREFIX + fileName;
        logger.info("Reading OBS object: {}", objectKey);
        
        try (ObsClient obsClient = new ObsClient(AK, SK, ENDPOINT);
             InputStream input = obsClient.getObject(BUCKET_NAME, objectKey).getObjectContent()) {
            
            byte[] buffer = new byte[1024];
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            int bytesRead;
            
            while ((bytesRead = input.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            
            logger.info("Successfully read object: {}", objectKey);
            return outputStream.toString();
            
        } catch (ObsException e) {
            handleObsException(e);
        } catch (IOException e) {
            logger.error("IO error while reading object {}: {}", objectKey, e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected error reading object {}: {}", objectKey, e.getMessage());
        }
        
        return StringUtils.EMPTY;
    }
    
    private static void handleObsException(ObsException e) {
        logger.error("OBS operation failed - HTTP Code: {}, Error Code: {}, Message: {}", 
                     e.getResponseCode(), e.getErrorCode(), e.getErrorMessage());
        logger.debug("Request ID: {}, Host ID: {}", e.getErrorRequestId(), e.getErrorHostId());
    }
}
