package com.bigdata.lineage.parser;

import lombok.extern.slf4j.Slf4j;

/**
 * Presto Parser 管理器
 * 
 * 负责检测和使用 Presto SQL Parser
 * 提供优雅降级：如果依赖不存在，自动回退到文本解析
 */
@Slf4j
public class PrestoParserManager {

    private final boolean parserAvailable;
    private final Object parserInstance; // 使用 Object 避免编译时依赖

    /**
     * 构造函数 - 尝试加载 Presto Parser
     */
    public PrestoParserManager() {
        this.parserInstance = loadPrestoParser();
        this.parserAvailable = this.parserInstance != null;
        
        if (parserAvailable) {
            log.info("Initialized with Presto native parser");
        } else {
            log.warn("Presto parser not available, will use text parsing fallback");
            log.warn("To enable Presto native parsing, add dependency to pom.xml:");
            log.warn("  <dependency>");
            log.warn("    <groupId>io.prestosql</groupId>");
            log.warn("    <artifactId>presto-parser</artifactId>");
            log.warn("    <version>372</version>");
            log.warn("  </dependency>");
        }
    }

    /**
     * 尝试加载 Presto SqlParser
     */
    @SuppressWarnings("unchecked")
    private Object loadPrestoParser() {
        try {
            // 使用 Class.forName 动态加载，避免编译时依赖
            Class<?> parserClass = Class.forName("io.prestosql.sql.SqlParser");
            
            // 获取构造函数并实例化
            var constructor = parserClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            Object parser = constructor.newInstance();
            
            return parser;
            
        } catch (ClassNotFoundException e) {
            log.debug("Presto parser class not found in classpath");
            return null;
        } catch (Exception e) {
            log.warn("Failed to initialize Presto parser: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 检查 Presto Parser 是否可用
     */
    public boolean isAvailable() {
        return parserAvailable;
    }

    /**
     * 获取 Parser 实例（如果可用）
     */
    public Object getParser() {
        return parserInstance;
    }

    /**
     * 判断应该使用哪种模式
     */
    public String getExtractionMethod() {
        return parserAvailable ? "PRESTO_NATIVE_PARSER" : "TEXT_PARSING";
    }
}
