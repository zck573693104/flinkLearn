package com.bigdata.lineage.web;

import com.bigdata.lineage.graph.LineageStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * 字段级血缘 WebUI 的入口。
 *
 * <p>组件扫描限定在本包：{@code com.bigdata.lineage.graph} 与 {@code .parser} 是纯 POJO，
 * 由这里显式装配成 Bean，不让注解决定它们的生命周期。
 */
@SpringBootApplication(scanBasePackages = "com.bigdata.lineage.web")
@EnableConfigurationProperties(LineageProperties.class)
public class LineageWebApplication {

    public static void main(String[] args) {
        SpringApplication.run(LineageWebApplication.class, args);
    }

    @Bean
    public LineageStore lineageStore() {
        return new LineageStore();
    }
}
