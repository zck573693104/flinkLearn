package com.bigdata.utils;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class JedisClusterUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(JedisClusterUtils.class);
    private static volatile JedisCluster jedisCluster;

    private JedisClusterUtils() {
    }

    public static JedisCluster getJedisCluster(String pordOrTest) {
        if (jedisCluster == null) {
            synchronized (JedisClusterUtils.class) {
                if (jedisCluster == null) {
                    Properties properties = PropertiesUtils.getPropertites(pordOrTest);
                    String servers = properties.getProperty("redis.url");
                    LOGGER.info("redis ips:{}",servers);
                    String password = properties.getProperty("redis.password");
                    Set<HostAndPort> nodes = new HashSet<>();
                    for (String server : servers.split(",")) {
                        String[] ipPortPair = server.split(":");
                        HostAndPort hostAndPort = new HostAndPort(ipPortPair[0].trim(), Integer.parseInt(ipPortPair[1].trim()));
                        nodes.add(hostAndPort);
                    }
                    GenericObjectPoolConfig jedisPoolConfig = new GenericObjectPoolConfig();
                    jedisPoolConfig.setMaxTotal(20);
                    jedisPoolConfig.setMaxIdle(10);
                    jedisCluster = new JedisCluster(nodes, 5000, 3000, 10, password, jedisPoolConfig);
                }
            }
        }
        return jedisCluster;
    }

    public static void release(JedisCluster jedisCluster) {
        if (jedisCluster != null) {
            try {
                jedisCluster.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
