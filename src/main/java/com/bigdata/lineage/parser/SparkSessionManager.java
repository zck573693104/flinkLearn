package com.bigdata.lineage.parser;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

/**
 * SparkSession 管理器
 * 
 * 负责 SparkSession 的初始化和生命周期管理
 * 提供优雅降级机制：如果 Spark 不可用，返回 null
 */
@Slf4j
public class SparkSessionManager {

    private static volatile SparkSession sparkSession;
    private static volatile boolean initialized = false;
    private static final Object lock = new Object();

    /**
     * 获取 SparkSession（单例模式）
     * 
     * @return SparkSession 实例，如果初始化失败则返回 null
     */
    public static SparkSession getInstance() {
        if (sparkSession != null) {
            return sparkSession;
        }

        synchronized (lock) {
            if (sparkSession == null) {
                try {
                    log.info("Initializing SparkSession for lineage extraction...");
                    
                    sparkSession = SparkSession.builder()
                        .appName("SQLLineageExtractor")
                        .master("local[*]")  // 本地模式
                        .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
                        .config("spark.ui.enabled", "false")  // 禁用 UI 减少资源占用
                        .getOrCreate();
                    
                    initialized = true;
                    log.info("SparkSession initialized successfully");
                    
                } catch (Exception e) {
                    log.warn("Failed to initialize SparkSession, falling back to text parsing");
                    log.debug("Spark initialization error: {}", e.getMessage());
                    initialized = false;
                    sparkSession = null;
                }
            }
        }

        return sparkSession;
    }

    /**
     * 检查 SparkSession 是否可用
     */
    public static boolean isAvailable() {
        return initialized && sparkSession != null;
    }

    /**
     * 重置 SparkSession（用于测试）
     */
    public static void reset() {
        synchronized (lock) {
            if (sparkSession != null) {
                try {
                    sparkSession.stop();
                } catch (Exception e) {
                    log.warn("Error stopping SparkSession: {}", e.getMessage());
                }
                sparkSession = null;
                initialized = false;
            }
        }
    }

    /**
     * 关闭所有资源
     */
    public static void shutdown() {
        reset();
    }
}
