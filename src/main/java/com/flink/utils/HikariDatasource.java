package com.flink.utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class HikariDatasource {

    private static HikariDataSource dataSource = null;

    private static String driverClassName = null;

    private static String jdbcUrl = null;

    private static String user = null;

    private static String password = null;

    public static HikariDataSource getDatasource(String pordOrTest) throws IOException {

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
        driverClassName = properties.getProperty("driverClassName");
        jdbcUrl = properties.getProperty("jdbcUrl");
        user = properties.getProperty("user");
        password = properties.getProperty("password");
        //配置文件
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setDriverClassName(driverClassName);
        hikariConfig.setUsername(user);
        hikariConfig.setPassword(password);
        hikariConfig.setConnectionTimeout(30000);
        hikariConfig.setIdleTimeout(30000);
        hikariConfig.setMaxLifetime(1800000);
        hikariConfig.setConnectionTestQuery("SELECT 1");
        dataSource = new HikariDataSource(hikariConfig);
        return dataSource;

    }

}