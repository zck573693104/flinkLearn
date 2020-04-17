package org.fuwushe.utils;

import com.alibaba.druid.pool.DruidDataSource;

import java.io.InputStream;
import java.util.Properties;

//引入站点的配置信息


public class DruidDatasource {

    private static DruidDataSource dataSource = null;

    private static String driverClassName = null;

    private static String jdbcUrl = null;

    private static String user = null;

    private static String password = null;


    public static DruidDataSource getDatasource() throws Exception {

        InputStream resourceAsStream = HikariDatasource.class.getResourceAsStream("/db.properties");
        Properties properties = new Properties();
        properties.load(resourceAsStream);
        driverClassName = properties.getProperty("driverClassName");
        jdbcUrl = properties.getProperty("jdbcUrl");
        user = properties.getProperty("user");
        password = properties.getProperty("password");

        dataSource = new DruidDataSource();
        //设置连接参数
        dataSource.setUrl(jdbcUrl);
        dataSource.setDriverClassName(driverClassName);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        //                //配置初始化大小、最小、最大
        dataSource.setInitialSize(10);
        dataSource.setMinIdle(30);
        dataSource.setMaxActive(100);
        //                //连接泄漏监测
        dataSource.setRemoveAbandoned(true);
        dataSource.setRemoveAbandonedTimeout(30);
        //                //配置获取连接等待超时的时间
        dataSource.setMaxWait(60000);
        //                //配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        //                //防止过期
        dataSource.setValidationQuery("SELECT 'x'");
        dataSource.setTestWhileIdle(true);
        dataSource.setTestOnBorrow(false);
        return dataSource;


    }



}