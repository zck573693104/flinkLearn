package com.flink.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class RedisUtil {

    public static FlinkJedisPoolConfig getRedis(String pordOrTest) throws IOException {
        InputStream resourceAsStream = null;
        if (pordOrTest.contains("prod")){
            resourceAsStream = HikariDatasource.class.getResourceAsStream("/constants-prod.properties");
        }
        if (pordOrTest.contains("test")) {
            resourceAsStream = HikariDatasource.class.getResourceAsStream("/constants-test.properties");
        }
        if (pordOrTest.contains("local")) {
            resourceAsStream = HikariDatasource.class.getResourceAsStream("/constants-local.properties");
        }
        Properties properties = new Properties();
        properties.load(resourceAsStream);
        String redisUrl = properties.getProperty("redisUrl");
        String password = properties.getProperty("redis-password");
        int database = Integer.valueOf(properties.getProperty("database"));
        if (StringUtils.isNotBlank(password)){
            FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost(redisUrl)
                    .setPassword(password).setDatabase(database).build();
            return jedisPoolConfig;
        }
        else {
            FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost(redisUrl).setDatabase(database).build();
            return jedisPoolConfig;
        }
    }
}
