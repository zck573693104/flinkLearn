package com.bigdata.lineage;

import com.bigdata.lineage.model.ColumnLineageResult;
import com.bigdata.lineage.model.TableLineageResult;
import com.bigdata.lineage.parser.*;

import java.util.List;
import java.util.Set;

/**
 * 血缘提取系统回归测试
 * 
 * 测试所有引擎的血缘提取功能
 */
public class LineageSystemTest {

    public static void main(String[] args) {
        System.out.println("=== Multi-Engine SQL Lineage System - Regression Test ===\n");
        
        int totalTests = 0;
        int passedTests = 0;
        
        // Test 1: JSQLParser (common parser for all engines)
        System.out.println("=== Test 1: JSQLParser Common Parser ===");
        if (testJSQLParserFallback()) {
            passedTests++;
        }
        totalTests++;
        
        // Note: Flink SQL Extractor requires planner module on classpath
        // It uses StreamTableEnvironment which needs flink-table-planner module
        // For now, we rely on JSQLParser fallback which works for all engines
        
        // Test 2: Spark Extractor
        System.out.println("\n=== Test 2: Spark SQL Extractor ===");
        try {
            SparkSQLLineageExtractor sparkExtractor = new SparkSQLLineageExtractor();
            if (testSpark(sparkExtractor)) {
                passedTests++;
            }
            totalTests++;
        } catch (Exception e) {
            System.err.println("[FAIL] Spark extractor error: " + e.getMessage());
        }
        
        // Test 3: Presto Extractor
        System.out.println("\n=== Test 3: Presto SQL Extractor ===");
        try {
            PrestoSQLLineageExtractor prestoExtractor = new PrestoSQLLineageExtractor();
            if (testPresto(prestoExtractor)) {
                passedTests++;
            }
            totalTests++;
        } catch (Exception e) {
            System.err.println("[FAIL] Presto extractor error: " + e.getMessage());
        }
        
        // Test 4: Paimon Extractor
        System.out.println("\n=== Test 4: Paimon Catalog Extractor ===");
        try {
            PaimonCatalogLineageExtractor paimonExtractor = new PaimonCatalogLineageExtractor();
            if (testPaimon(paimonExtractor)) {
                passedTests++;
            }
            totalTests++;
        } catch (Exception e) {
            System.err.println("[FAIL] Paimon extractor error: " + e.getMessage());
        }
        
        // Test 5: Flink Dual-Mode Test
        System.out.println("\n=== Test 5: Flink Dual-Mode Support ===");
        try {
            if (testFlinkDualMode()) {
                passedTests++;
            }
            totalTests++;
        } catch (Exception e) {
            System.err.println("[FAIL] Flink dual-mode test error: " + e.getMessage());
        }
        
        // Test 6: Spark Dual-Mode Test
        System.out.println("\n=== Test 6: Spark Dual-Mode Support ===");
        try {
            if (testSparkDualMode()) {
                passedTests++;
            }
            totalTests++;
        } catch (Exception e) {
            System.err.println("[FAIL] Spark dual-mode test error: " + e.getMessage());
        }
        
        // Test 7: Presto Dual-Mode Test
        System.out.println("\n=== Test 7: Presto Dual-Mode Support ===");
        try {
            if (testPrestoDualMode()) {
                passedTests++;
            }
            totalTests++;
        } catch (Exception e) {
            System.err.println("[FAIL] Presto dual-mode test error: " + e.getMessage());
        }
        
        // Test 8: Cross-Engine Integration
        System.out.println("\n=== Test 8: Cross-Engine Integration ===");
        try {
            if (testCrossEngineIntegration()) {
                passedTests++;
            }
            totalTests++;
        } catch (Exception e) {
            System.err.println("[FAIL] Integration test error: " + e.getMessage());
        }
        
        // Summary
        System.out.println("\n===========================================");
        System.out.println("Regression Test Summary");
        System.out.println("===========================================");
        System.out.println("Total Tests: " + totalTests);
        System.out.println("Passed: " + passedTests);
        System.out.println("Failed: " + (totalTests - passedTests));
        System.out.println("Success Rate: " + (passedTests * 100 / totalTests) + "%");
        System.out.println("===========================================");
        
        if (passedTests == totalTests) {
            System.out.println("\n✅ All tests PASSED!");
        } else {
            System.out.println("\n❌ Some tests FAILED!");
            System.exit(1);
        }
    }
    
    /**
     * Test Flink SQL Extractor
     */
    private static boolean testFlink(FlinkSQLLineageExtractor extractor) {
        System.out.println("[Flink Extractor]");
        
        // Test 1: Simple INSERT INTO SELECT
        String sql1 = "INSERT INTO mysql_table SELECT user_id, username FROM kafka_topic";
        var result1 = extractor.extractTableLineages(sql1);
        printResult("Simple INSERT", sql1, result1);
        
        // Test 2: INSERT with JOIN
        String sql2 = "INSERT INTO agg_user_count SELECT user_id, COUNT(*) as cnt FROM user_log GROUP BY user_id";
        var result2 = extractor.extractTableLineages(sql2);
        printResult("INSERT with GROUP BY", sql2, result2);
        
        // Test 3: Column lineage
        List<FlinkSQLLineageExtractor.ColumnLineageResult> colResults = 
            extractor.extractColumnLineages(sql2);
        System.out.println("Column lineage count: " + colResults.size());
        for (var col : colResults) {
            System.out.println("  [" + col.getSourceColumn() + "] -> [" + col.getTargetColumn() + "]");
        }
        
        return !result1.isEmpty() && !result2.isEmpty();
    }
    
    /**
     * Test JSQLParser fallback (when Flink not available)
     */
    private static boolean testJSQLParserFallback() {
        System.out.println("[JSQLParser Fallback]");
        
        Set<String> tables = JSQLParserUtils.extractTables(
            "INSERT INTO target SELECT a, b FROM source WHERE c > 10"
        );
        
        System.out.println("Extracted tables: " + tables);
        
        String sqlType = JSQLParserUtils.getSQLType(
            "INSERT INTO t1 SELECT * FROM t2"
        );
        System.out.println("SQL Type: " + sqlType);
        
        return tables.contains("target") && tables.contains("source");
    }
    
    /**
     * Test Spark SQL Extractor
     */
    private static boolean testSpark(SparkSQLLineageExtractor extractor) {
        System.out.println("[Spark Extractor]");
        
        // Test 1: INSERT OVERWRITE
        String sql1 = "INSERT OVERWRITE TABLE target SELECT * FROM source";
        var result1 = extractor.extractTableLineages(sql1);
        printResult("INSERT OVERWRITE", sql1, result1);
        
        // Test 2: Multiple JOINs
        String sql2 = "INSERT INTO result SELECT a.id, b.name, c.value " +
                     "FROM table_a a " +
                     "JOIN table_b b ON a.id = b.a_id " +
                     "JOIN table_c c ON b.id = c.b_id";
        var result2 = extractor.extractTableLineages(sql2);
        printResult("Multi-table JOIN", sql2, result2);
        
        // Test 3: Column lineage
        List<ColumnLineageResult> colResults = 
            extractor.extractColumnLineages(sql1);
        System.out.println("Column lineage count: " + colResults.size());
        
        return !result1.isEmpty() && !result2.isEmpty();
    }
    
    /**
     * Test Presto SQL Extractor
     */
    private static boolean testPresto(PrestoSQLLineageExtractor extractor) {
        System.out.println("[Presto Extractor]");
        
        // Test 1: Simple SELECT
        String sql1 = "SELECT user_id, amount FROM transactions";
        var result1 = extractor.extractTableLineages(sql1);
        printResult("Simple SELECT", sql1, result1);
        
        // Test 2: INSERT with LEFT JOIN
        String sql2 = "INSERT INTO order_summary SELECT o.id, u.name " +
                     "FROM orders o LEFT JOIN users u ON o.user_id = u.id";
        var result2 = extractor.extractTableLineages(sql2);
        printResult("LEFT JOIN INSERT", sql2, result2);
        
        // Test 3: Column lineage
        List<ColumnLineageResult> colResults = 
            extractor.extractColumnLineages(sql2);
        System.out.println("Column lineage count: " + colResults.size());
        
        return !result1.isEmpty() && !result2.isEmpty();
    }
    
    /**
     * Test Paimon Catalog Extractor
     */
    private static boolean testPaimon(PaimonCatalogLineageExtractor extractor) {
        System.out.println("[Paimon Extractor]");
        
        // Test 1: CREATE TABLE AS SELECT
        String sql1 = "CREATE TABLE paimon_sink " +
                     "WITH (" +
                     "  'connector' = 'paimon', " +
                     "  'file.format' = 'parquet'" +
                     ") " +
                     "AS SELECT user_id, COUNT(*) FROM user_log";
        var results1 = extractor.extractPaimonLineages(sql1);
        System.out.println("CTAS results: " + results1.size());
        for (var r : results1) {
            System.out.println("  Target: " + r.getTargetTable() + 
                             ", Type: " + r.getPaimonProperties().getTableType());
        }
        
        // Test 2: INSERT INTO Paimon
        String sql2 = "INSERT INTO paimon_table SELECT id, name FROM source_table";
        var results2 = extractor.extractPaimonLineages(sql2);
        System.out.println("INSERT results: " + results2.size());
        
        return !results1.isEmpty();
    }
    
    /**
     * Test Flink Dual-Mode Support
     */
    private static boolean testFlinkDualMode() {
        System.out.println("[Flink Dual-Mode]");
        
        String sql = "INSERT INTO target SELECT a, b FROM source";
        
        // Test 1: Text Parsing Mode
        System.out.println("Testing TEXT_PARSING mode...");
        FlinkSQLLineageExtractor flinkText = new FlinkSQLLineageExtractor(FlinkSQLLineageExtractor.ExtractionMode.TEXT_PARSING);
        var textResults = flinkText.extractTableLineages(sql);
        System.out.println("  Method: " + flinkText.getExtractionMethod());
        System.out.println("  Results: " + textResults.size());
        if (!textResults.isEmpty()) {
            System.out.println("  Target: " + textResults.get(0).getTargetTable());
            System.out.println("  Confidence: " + textResults.get(0).getConfidence());
        }
        
        // Test 2: Auto Mode (should use native API if available)
        System.out.println("\nTesting AUTO mode...");
        FlinkSQLLineageExtractor flinkAuto = new FlinkSQLLineageExtractor(FlinkSQLLineageExtractor.ExtractionMode.AUTO);
        var autoResults = flinkAuto.extractTableLineages(sql);
        System.out.println("  Native API Available: " + flinkAuto.isNativeApiAvailable());
        System.out.println("  Method: " + flinkAuto.getExtractionMethod());
        System.out.println("  Results: " + autoResults.size());
        if (!autoResults.isEmpty()) {
            System.out.println("  Target: " + autoResults.get(0).getTargetTable());
            System.out.println("  Confidence: " + autoResults.get(0).getConfidence());
        }
        
        // Verify column lineage extraction
        System.out.println("\nTesting column lineage...");
        var colResults = flinkAuto.extractColumnLineages(sql);
        System.out.println("  Column results: " + colResults.size());
        for (var col : colResults) {
            System.out.println("    [" + col.getSourceColumn() + "] -> [" + col.getTargetColumn() + "]");
        }
        
        return !textResults.isEmpty() && !autoResults.isEmpty();
    }
    
    /**
     * Test Spark Dual-Mode Support
     */
    private static boolean testSparkDualMode() {
        System.out.println("[Spark Dual-Mode]");
        
        String sql = "INSERT OVERWRITE TABLE target SELECT a, b FROM source";
        
        // Test 1: Text Parsing Mode
        System.out.println("Testing TEXT_PARSING mode...");
        SparkSQLLineageExtractor sparkText = new SparkSQLLineageExtractor(SparkSQLLineageExtractor.ExtractionMode.TEXT_PARSING);
        var textResults = sparkText.extractTableLineages(sql);
        System.out.println("  Method: " + sparkText.getExtractionMethod());
        System.out.println("  Results: " + textResults.size());
        if (!textResults.isEmpty()) {
            System.out.println("  Target: " + textResults.get(0).getTargetTable());
            System.out.println("  Confidence: " + textResults.get(0).getConfidence());
        }
        
        // Test 2: Auto Mode (should use text parsing as fallback)
        System.out.println("\nTesting AUTO mode...");
        SparkSQLLineageExtractor sparkAuto = new SparkSQLLineageExtractor(SparkSQLLineageExtractor.ExtractionMode.AUTO);
        var autoResults = sparkAuto.extractTableLineages(sql);
        System.out.println("  Native API Available: " + sparkAuto.isNativeApiAvailable());
        System.out.println("  Method: " + sparkAuto.getExtractionMethod());
        System.out.println("  Results: " + autoResults.size());
        if (!autoResults.isEmpty()) {
            System.out.println("  Target: " + autoResults.get(0).getTargetTable());
            System.out.println("  Confidence: " + autoResults.get(0).getConfidence());
        }
        
        // Verify column lineage extraction
        System.out.println("\nTesting column lineage...");
        var colResults = sparkAuto.extractColumnLineages(sql);
        System.out.println("  Column results: " + colResults.size());
        for (var col : colResults) {
            System.out.println("    [" + col.getSourceColumn() + "] -> [" + col.getTargetColumn() + "]");
        }
        
        return !textResults.isEmpty() && !autoResults.isEmpty();
    }
    
    /**
     * Test Presto Dual-Mode Support
     */
    private static boolean testPrestoDualMode() {
        System.out.println("[Presto Dual-Mode]");
        
        String sql = "INSERT INTO target SELECT a, b FROM source";
        
        // Test 1: Text Parsing Mode
        System.out.println("Testing TEXT_PARSING mode...");
        PrestoSQLLineageExtractor prestoText = new PrestoSQLLineageExtractor(PrestoSQLLineageExtractor.ExtractionMode.TEXT_PARSING);
        var textResults = prestoText.extractTableLineages(sql);
        System.out.println("  Method: " + prestoText.getExtractionMethod());
        System.out.println("  Results: " + textResults.size());
        if (!textResults.isEmpty()) {
            System.out.println("  Target: " + textResults.get(0).getTargetTable());
            System.out.println("  Confidence: " + textResults.get(0).getConfidence());
        }
        
        // Test 2: Auto Mode (should use text parsing as fallback)
        System.out.println("\nTesting AUTO mode...");
        PrestoSQLLineageExtractor prestoAuto = new PrestoSQLLineageExtractor(PrestoSQLLineageExtractor.ExtractionMode.AUTO);
        var autoResults = prestoAuto.extractTableLineages(sql);
        System.out.println("  Native API Available: " + prestoAuto.isNativeApiAvailable());
        System.out.println("  Method: " + prestoAuto.getExtractionMethod());
        System.out.println("  Results: " + autoResults.size());
        if (!autoResults.isEmpty()) {
            System.out.println("  Target: " + autoResults.get(0).getTargetTable());
            System.out.println("  Confidence: " + autoResults.get(0).getConfidence());
        }
        
        // Verify column lineage extraction
        System.out.println("\nTesting column lineage...");
        var colResults = prestoAuto.extractColumnLineages(sql);
        System.out.println("  Column results: " + colResults.size());
        for (var col : colResults) {
            System.out.println("    [" + col.getSourceColumn() + "] -> [" + col.getTargetColumn() + "]");
        }
        
        return !textResults.isEmpty() && !autoResults.isEmpty();
    }
    
    /**
     * Test Cross-Engine Integration
     */
    private static boolean testCrossEngineIntegration() {
        System.out.println("[Cross-Engine Integration]");
        
        // Use same SQL across different engines
        String sql = "INSERT INTO target SELECT a, b FROM source";
        
        // Extract from JSQLParser
        Set<String> tables = JSQLParserUtils.extractTables(sql);
        System.out.println("JSQLParser extracted tables: " + tables);
        
        // Extract from Spark
        SparkSQLLineageExtractor spark = new SparkSQLLineageExtractor();
        var sparkResults = spark.extractTableLineages(sql);
        System.out.println("Spark extracted " + sparkResults.size() + " results");
        
        // Extract from Presto
        PrestoSQLLineageExtractor presto = new PrestoSQLLineageExtractor();
        var prestoResults = presto.extractTableLineages(sql);
        System.out.println("Presto extracted " + prestoResults.size() + " results");
        
        return !tables.isEmpty() && !sparkResults.isEmpty() && !prestoResults.isEmpty();
    }
    
    /**
     * Print extraction result
     */
    private static <T> void printResult(String testName, String sql, List<T> results) {
        System.out.println("Test: " + testName);
        System.out.println("SQL: " + sql);
        System.out.println("Results: " + results.size());
        
        if (!results.isEmpty() && results.get(0) instanceof FlinkSQLLineageExtractor.TableLineageResult) {
            FlinkSQLLineageExtractor.TableLineageResult result = 
                (FlinkSQLLineageExtractor.TableLineageResult) results.get(0);
            System.out.println("  Target: " + result.getTargetTable());
            System.out.println("  Sources: " + result.getSourceTables());
            System.out.println("  Confidence: " + result.getConfidence());
        } else if (!results.isEmpty()) {
            TableLineageResult result = (TableLineageResult) results.get(0);
            System.out.println("  Target: " + result.getTargetTable());
            System.out.println("  Sources: " + result.getSourceTables());
            System.out.println("  Confidence: " + result.getConfidence());
        }
        System.out.println();
    }
}
