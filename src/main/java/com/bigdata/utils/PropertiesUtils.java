package com.bigdata.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtils {
    public static Properties getPropertites(String active) {
        InputStream resourceAsStream = null;
        if (active.contains("prod")) {
            resourceAsStream = PropertiesUtils.class.getResourceAsStream("/prod.properties");
        }
        if (active.contains("dev")) {
            resourceAsStream = PropertiesUtils.class.getResourceAsStream("/dev.properties");
        }
        if (active.contains("local")) {
            resourceAsStream = PropertiesUtils.class.getResourceAsStream("/local.properties");
        }
        Properties properties = new Properties();
        try {
            properties.load(resourceAsStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return properties;
    }
}
