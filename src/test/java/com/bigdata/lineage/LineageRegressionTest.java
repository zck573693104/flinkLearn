package com.bigdata.lineage;

import com.bigdata.lineage.parser.FlinkSQLLineageExtractor;
import com.bigdata.lineage.parser.SparkSQLLineageExtractor;
import com.bigdata.lineage.parser.PaimonCatalogLineageExtractor;
import com.bigdata.lineage.service.TableLineageService;
import com.bigdata.lineage.model.TableLineage;

import java.util.List;
import java.util.Set;

/**
 * Lineage Extraction System Regression Test
 * 
 * Test Scope:
 * 1. Flink SQL Lineage Extractor
 * 2. Spark SQL Lineage Extractor  
 * 3. Paimon Catalog Lineage Extractor
 * 4. TableLineageService Layer
 */
public class LineageRegressionTest {
    
    private static int passedTests = 0;
    private static int failedTests = 0;
    
    public static void main(String[] args) {
        System.out.println("================================================================================");
        System.out.println("Flink SQL Lineage Extraction System - Regression Test");
        System.out.println("================================================================================");
        System.out.println();
        
        // Test Flink SQL Extractor
        testFlinkSQLExtractor();
        
        // Test Spark SQL Extractor
        testSparkSQLExtractor();
        
        // Test Paimon Catalog Extractor
        testPaimonCatalogExtractor();
        
        // Test Service Layer
        testTableLineageService();
        
        // Output Test Results
        System.out.println();
        System.out.println("================================================================================");
        System.out.println("Test Results Summary");
        System.out.println("================================================================================");
        System.out.println("Passed: " + passedTests);
        System.out.println("Failed: " + failedTests);
        System.out.println("Total: " + (passedTests + failedTests));
        
        if (failedTests == 0) {
            System.out.println("\nAll tests PASSED!");
            System.exit(0);
        } else {
            System.out.println("\nSome tests FAILED!");
            System.exit(1);
        }
    }
    
    /**
     * Test Flink SQL Lineage Extractor
     */
    private static void testFlinkSQLExtractor() {
        System.out.println("\n[Test Group] Flink SQL Lineage Extractor");
        System.out.println("-".repeat(80));
        
        FlinkSQLLineageExtractor extractor = new FlinkSQLLineageExtractor();
        
        // Test 1: Simple INSERT INTO
        test("Simple INSERT INTO", () -> {
            String sql = "INSERT INTO target_table SELECT * FROM source_table";
            List<FlinkSQLLineageExtractor.TableLineageResult> results = 
                extractor.extractTableLineages(sql);
            
            assert results.size() == 1 : "Should return 1 result";
            assert results.get(0).getTargetTable().equals("target_table") : "Target table error";
            assert results.get(0).getSourceTables().contains("source_table") : "Source table error";
            assert results.get(0).getProcessType().equals("INSERT") : "Process type error";
        });
        
        // Test 2: JOIN statement
        test("JOIN statement", () -> {
            String sql = "INSERT INTO result SELECT a.*, b.name " +
                        "FROM table_a a " +
                        "JOIN table_b b ON a.id = b.a_id";
            List<FlinkSQLLineageExtractor.TableLineageResult> results = 
                extractor.extractTableLineages(sql);
            
            assert results.size() == 1 : "Should return 1 result";
            Set<String> sources = results.get(0).getSourceTables();
            assert sources.contains("table_a") : "Missing table_a";
            assert sources.contains("table_b") : "Missing table_b";
        });
        
        // Test 3: UNION operation
        test("UNION operation", () -> {
            String sql = "INSERT INTO combined " +
                        "SELECT * FROM table_a " +
                        "UNION ALL " +
                        "SELECT * FROM table_b";
            List<FlinkSQLLineageExtractor.TableLineageResult> results = 
                extractor.extractTableLineages(sql);
            
            assert results.size() == 1 : "Should return 1 result";
            Set<String> sources = results.get(0).getSourceTables();
            assert sources.contains("table_a") : "Missing table_a";
            assert sources.contains("table_b") : "Missing table_b";
        });
        
        // Test 4: LEFT JOIN
        test("LEFT JOIN", () -> {
            String sql = "INSERT INTO result " +
                        "SELECT a.*, b.order_count " +
                        "FROM user_info a " +
                        "LEFT JOIN (SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id) b " +
                        "ON a.user_id = b.user_id";
            List<FlinkSQLLineageExtractor.TableLineageResult> results = 
                extractor.extractTableLineages(sql);
            
            assert results.size() == 1 : "Should return 1 result";
            Set<String> sources = results.get(0).getSourceTables();
            assert sources.contains("user_info") : "Missing user_info";
            assert sources.contains("orders") : "Missing orders";
        });
        
        // Test 5: SELECT statement (no target table)
        test("SELECT statement", () -> {
            String sql = "SELECT user_id, name FROM users WHERE age > 18";
            List<FlinkSQLLineageExtractor.TableLineageResult> results = 
                extractor.extractTableLineages(sql);
            
            assert results.size() == 1 : "Should return 1 result";
            assert results.get(0).getTargetTable() == null : "SELECT should not have target table";
            assert results.get(0).getSourceTables().contains("users") : "Missing users";
        });
        
        System.out.println("-".repeat(80));
    }
    
    /**
     * 测试 Spark SQL 血缘提取器
     */
    private static void testSparkSQLExtractor() {
        System.out.println("\n【测试组】Spark SQL 血缘提取器");
        System.out.println("-".repeat(80));
        
        SparkSQLLineageExtractor extractor = new SparkSQLLineageExtractor();
        
        // 测试 1: INSERT OVERWRITE
        test("INSERT OVERWRITE", () -> {
            String sql = "INSERT OVERWRITE TABLE daily_stats " +
                        "SELECT user_id, COUNT(*), SUM(amount) " +
                        "FROM events " +
                        "WHERE dt = '2024-01-01' " +
                        "GROUP BY user_id";
            List<SparkSQLLineageExtractor.TableLineageResult> results = 
                extractor.extractTableLineages(sql);
            
            assert results.size() == 1 : "应该返回 1 个结果";
            assert results.get(0).getTargetTable().equals("daily_stats") : "目标表错误";
            assert results.get(0).getSourceTables().contains("events") : "缺少 events";
            assert results.get(0).getConfidence() >= 0.9 : "置信度过低";
        });
        
        // 测试 2: 多表 JOIN
        test("多表 JOIN", () -> {
            String sql = "INSERT INTO result " +
                        "SELECT a.user_id, b.amount, c.name " +
                        "FROM table_a a " +
                        "JOIN table_b b ON a.id = b.a_id " +
                        "JOIN table_c c ON b.c_id = c.id";
            List<SparkSQLLineageExtractor.TableLineageResult> results = 
                extractor.extractTableLineages(sql);
            
            assert results.size() == 1 : "应该返回 1 个结果";
            Set<String> sources = results.get(0).getSourceTables();
            assert sources.contains("table_a") : "缺少 table_a";
            assert sources.contains("table_b") : "缺少 table_b";
            assert sources.contains("table_c") : "缺少 table_c";
        });
        
        System.out.println("-".repeat(80));
    }
    
    /**
     * 测试 Paimon Catalog 血缘提取器
     */
    private static void testPaimonCatalogExtractor() {
        System.out.println("\n【测试组】Paimon Catalog 血缘提取器");
        System.out.println("-".repeat(80));
        
        PaimonCatalogLineageExtractor extractor = new PaimonCatalogLineageExtractor();
        
        // 测试 1: CREATE TABLE AS SELECT
        test("CTAS 创建 Paimon 表", () -> {
            String sql = "CREATE TABLE paimon_db.customer_stats (" +
                        "user_id BIGINT," +
                        "order_count BIGINT," +
                        "PRIMARY KEY (user_id) NOT ENFORCED" +
                        ") WITH (" +
                        "  'connector' = 'paimon'," +
                        "  'file.format' = 'parquet'" +
                        ") AS" +
                        "SELECT user_id, COUNT(*) as order_count " +
                        "FROM orders " +
                        "GROUP BY user_id";
            List<PaimonCatalogLineageExtractor.PaimonLineageResult> results = 
                extractor.extractPaimonLineages(sql);
            
            assert results.size() == 1 : "应该返回 1 个结果";
            assert results.get(0).getTargetTable().equals("customer_stats") : "目标表错误";
            assert results.get(0).getSourceTables().contains("orders") : "缺少 orders";
            
            // 检查 Paimon 属性
            PaimonCatalogLineageExtractor.PaimonProperties props = 
                results.get(0).getPaimonProperties();
            assert props != null : "PaimonProperties 为空";
            assert "paimon".equals(props.getConnector()) : "连接器类型错误";
            assert "parquet".equals(props.getFileFormat()) : "文件格式错误";
        });
        
        // 测试 2: INSERT INTO Paimon
        test("INSERT INTO Paimon 表", () -> {
            String sql = "INSERT INTO paimon_db.streaming_events " +
                        "SELECT user_id, event_type, COUNT(*) as cnt " +
                        "FROM event_stream " +
                        "GROUP BY user_id, event_type";
            List<PaimonCatalogLineageExtractor.PaimonLineageResult> results = 
                extractor.extractPaimonLineages(sql);
            
            assert results.size() == 1 : "应该返回 1 个结果";
            assert results.get(0).getTargetTable().equals("streaming_events") : "目标表错误";
            assert results.get(0).getSourceTables().contains("event_stream") : "缺少 event_stream";
        });
        
        // 测试 3: 带分区的 Paimon 表
        test("分区 Paimon 表", () -> {
            String sql = "CREATE TABLE paimon_db.partitioned_table (" +
                        "dt STRING," +
                        "user_id BIGINT," +
                        "amount DECIMAL(10,2)" +
                        ") WITH (" +
                        "  'connector' = 'paimon'," +
                        "  'partition' = 'dt'," +
                        "  'bucket' = '4'" +
                        ") AS" +
                        "SELECT dt, user_id, amount FROM raw_events";
            List<PaimonCatalogLineageExtractor.PaimonLineageResult> results = 
                extractor.extractPaimonLineages(sql);
            
            assert results.size() == 1 : "应该返回 1 个结果";
            PaimonCatalogLineageExtractor.PaimonProperties props = 
                results.get(0).getPaimonProperties();
            assert props.getPartitionKeys().contains("dt") : "分区键识别错误";
            assert props.getBucketCount() == 4 : "桶数识别错误";
        });
        
        System.out.println("-".repeat(80));
    }
    
    /**
     * 测试 TableLineageService 服务层
     */
    private static void testTableLineageService() {
        System.out.println("\n【测试组】TableLineageService 服务层");
        System.out.println("-".repeat(80));
        
        TableLineageService service = new TableLineageService();
        
        // 测试 1: 记录血缘关系
        test("记录血缘关系", () -> {
            TableLineage lineage = service.recordLineage(
                "job_test_123",
                "Test ETL Job",
                "INSERT INTO agg_user_count SELECT user_id, COUNT(*) as cnt FROM user_log GROUP BY user_id"
            );
            
            assert lineage != null : "血缘记录为空";
            assert lineage.getTargetTable().equals("agg_user_count") : "目标表错误";
            assert lineage.getSourceTable().equals("user_log") : "源表错误";
            assert lineage.getJobId().equals("job_test_123") : "JobID 错误";
        });
        
        // 测试 2: 获取上游表
        test("获取上游表", () -> {
            service.recordLineage("job1", "Job1", 
                "INSERT INTO table_b SELECT * FROM table_a");
            service.recordLineage("job2", "Job2", 
                "INSERT INTO table_c SELECT * FROM table_b");
            
            List<String> upstreams = service.getUpstreamTables("table_c");
            
            assert !upstreams.isEmpty() : "上游表列表为空";
            assert upstreams.contains("table_b") : "缺少 table_b";
        });
        
        // 测试 3: 获取下游表
        test("获取下游表", () -> {
            service.recordLineage("job1", "Job1", 
                "INSERT INTO table_b SELECT * FROM table_a");
            service.recordLineage("job2", "Job2", 
                "INSERT INTO table_c SELECT * FROM table_b");
            
            List<String> downstreams = service.getDownstreamTables("table_a");
            
            assert !downstreams.isEmpty() : "下游表列表为空";
            assert downstreams.contains("table_b") : "缺少 table_b";
        });
        
        // 测试 4: 清除缓存
        test("清除缓存", () -> {
            service.recordLineage("job1", "Job1", 
                "INSERT INTO table1 SELECT * FROM table2");
            
            service.clearCache();
            
            List<String> upstreams = service.getUpstreamTables("table1");
            assert upstreams.isEmpty() : "缓存未清空";
        });
        
        System.out.println("-".repeat(80));
    }
    
    /**
     * 通用测试方法
     */
    private static void test(String testName, TestRunnable test) {
        try {
            test.run();
            System.out.println("✅ " + testName);
            passedTests++;
        } catch (AssertionError e) {
            System.out.println("❌ " + testName + " - " + e.getMessage());
            failedTests++;
        } catch (Exception e) {
            System.out.println("❌ " + testName + " - 异常：" + e.getMessage());
            failedTests++;
        }
    }
    
    /**
     * 测试运行接口
     */
    @FunctionalInterface
    interface TestRunnable {
        void run() throws Exception;
    }
}
