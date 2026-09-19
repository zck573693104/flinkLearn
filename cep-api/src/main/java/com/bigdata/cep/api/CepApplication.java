package com.bigdata.cep.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * Flink CEP 动态加载系统
 * 提供规则管理、实时监控和告警推送功能
 */
@SpringBootApplication
@ComponentScan(basePackages = {"com.bigdata.cep"})
public class CepApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(CepApplication.class, args);
        System.out.println("========================================");
        System.out.println("Flink CEP Dynamic Loading System Started!");
        System.out.println("========================================");
    }
}
