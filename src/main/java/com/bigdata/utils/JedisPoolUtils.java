package com.bigdata.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Properties;

public class JedisPoolUtils {
    private static volatile JedisPool JEDIS_POOL;

    private JedisPoolUtils() {
    }

    public static JedisPool getJedisPool(String pordOrTest) {
        if (JEDIS_POOL == null) {
            synchronized (JedisPoolUtils.class) {
                if (JEDIS_POOL == null) {
                    //读取配置文件
                    Properties properties = PropertiesUtils.getPropertites(pordOrTest);
                    JedisPoolConfig config = new JedisPoolConfig();
                    String[] servers = properties.getProperty("redis.url").split(":");
                    String host = servers[0];
                    int port = Integer.parseInt(servers[1]);
                    String password = properties.getProperty("redis.password");
                    JEDIS_POOL = new JedisPool(config, host, port, 2000, password);
                }
            }
        }
        return JEDIS_POOL;
    }

    public static void release(Jedis jedis) {
        if (jedis != null) {
            try {
                jedis.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
