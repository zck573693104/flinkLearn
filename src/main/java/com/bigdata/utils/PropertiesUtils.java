package com.bigdata.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;

public class PropertiesUtils {
    private static final Logger logger = LoggerFactory.getLogger(PropertiesUtils.class);
    
    private static final String DEV_PROPERTIES = "/dev.properties";
    private static final String PROD_PROPERTIES = "/prod.properties";
    private static final String LOCAL_PROPERTIES = "/local.properties";
    private static final String DEFAULT_PROPERTIES = LOCAL_PROPERTIES;
    
    private PropertiesUtils() {
        throw new IllegalStateException("Utility class");
    }
    
    public static Properties getProperties(String active) {
        if (active == null) {
            logger.warn("Active environment is null, using default: {}", DEFAULT_PROPERTIES);
            active = "local";
        }
        
        String configFile = resolveConfigFile(active);
        logger.info("Loading properties file: {}", configFile);
        
        try (InputStream inputStream = PropertiesUtils.class.getResourceAsStream(configFile)) {
            if (inputStream == null) {
                logger.error("Configuration file not found: {}", configFile);
                throw new IllegalStateException("Configuration file not found: " + configFile);
            }
            
            Properties properties = new Properties();
            properties.load(inputStream);
            logger.info("Successfully loaded {} properties", properties.size());
            
            return properties;
            
        } catch (IOException e) {
            logger.error("Failed to load properties from {}: {}", configFile, e.getMessage());
            throw new RuntimeException("Failed to load properties", e);
        }
    }
    
    private static String resolveConfigFile(String active) {
        String normalizedActive = active.toLowerCase(Locale.ROOT);
        
        if (normalizedActive.contains("prod")) {
            return PROD_PROPERTIES;
        } else if (normalizedActive.contains("dev")) {
            return DEV_PROPERTIES;
        } else if (normalizedActive.contains("local")) {
            return LOCAL_PROPERTIES;
        } else {
            logger.warn("Unknown environment '{}', using default: {}", active, DEFAULT_PROPERTIES);
            return DEFAULT_PROPERTIES;
        }
    }
}
