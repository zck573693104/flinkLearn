package com.bigdata.lineage;

import com.bigdata.lineage.model.TableLineageResult;
import com.bigdata.lineage.parser.FlinkSQLLineageExtractor;
import com.bigdata.lineage.parser.SparkSQLLineageExtractor;
import com.bigdata.lineage.parser.PaimonCatalogLineageExtractor;
import com.bigdata.lineage.service.TableLineageService;

import java.util.List;
import java.util.Set;

/**
 * Lineage Extraction System - Simple Regression Test
 */
public class SimpleRegressionTest {
    
    private static int passed = 0;
    private static int failed = 0;
    
    public static void main(String[] args) {
        System.out.println("=== Flink SQL Lineage System - Regression Test ===\n");
        
        testFlink();
        testSpark();
        testPaimon();
        testService();
        
        System.out.println("\n=== Results ===");
        System.out.println("Passed: " + passed);
        System.out.println("Failed: " + failed);
        System.out.println(failed == 0 ? "\nSUCCESS!" : "\nFAILED!");
        System.exit(failed == 0 ? 0 : 1);
    }
    
    private static void testFlink() {
        System.out.println("[Flink Extractor]");
        FlinkSQLLineageExtractor ext = new FlinkSQLLineageExtractor();
        
        // Test INSERT INTO
        List<FlinkSQLLineageExtractor.TableLineageResult> r1 = 
            ext.extractTableLineages("INSERT INTO t1 SELECT * FROM s1");
        assert r1.size() == 1 && r1.get(0).getTargetTable().equals("t1") : "Test 1 failed";
        System.out.println("  [OK] INSERT INTO");
        
        // Test JOIN
        List<FlinkSQLLineageExtractor.TableLineageResult> r2 = 
            ext.extractTableLineages("INSERT INTO t2 SELECT a.*, b.* FROM ta a JOIN tb b ON a.id=b.a_id");
        Set<String> srcs = r2.get(0).getSourceTables();
        assert srcs.contains("ta") && srcs.contains("tb") : "Test 2 failed";
        System.out.println("  [OK] JOIN");
        
        // Test UNION
        List<FlinkSQLLineageExtractor.TableLineageResult> r3 = 
            ext.extractTableLineages("INSERT INTO t3 SELECT * FROM ta UNION ALL SELECT * FROM tb");
        srcs = r3.get(0).getSourceTables();
        assert srcs.contains("ta") && srcs.contains("tb") : "Test 3 failed";
        System.out.println("  [OK] UNION");
        
        passed += 3;
    }
    
    private static void testSpark() {
        System.out.println("[Spark Extractor]");
        SparkSQLLineageExtractor ext = new SparkSQLLineageExtractor();
        
        // Test INSERT OVERWRITE
        List<TableLineageResult> r1 = 
            ext.extractTableLineages("INSERT OVERWRITE TABLE t1 SELECT user_id, COUNT(*) FROM events GROUP BY user_id");
        assert r1.size() == 1 && r1.get(0).getTargetTable().equals("t1") : "Test 1 failed";
        System.out.println("  [OK] INSERT OVERWRITE");
        
        passed += 1;
    }
    
    private static void testPaimon() {
        System.out.println("[Paimon Extractor]");
        PaimonCatalogLineageExtractor ext = new PaimonCatalogLineageExtractor();
        
        // Test CTAS
        List<PaimonCatalogLineageExtractor.PaimonLineageResult> r1 = 
            ext.extractPaimonLineages("CREATE TABLE paimon_db.t1 (id BIGINT PRIMARY KEY NOT ENFORCED) WITH ('connector'='paimon') AS SELECT id FROM s1");
        assert r1.size() == 1 && r1.get(0).getTargetTable().equals("t1") : "Test 1 failed";
        System.out.println("  [OK] CTAS");
        
        // Test INSERT INTO
        List<PaimonCatalogLineageExtractor.PaimonLineageResult> r2 = 
            ext.extractPaimonLineages("INSERT INTO paimon_db.t2 SELECT user_id, cnt FROM stream_data");
        assert r2.size() == 1 && r2.get(0).getSourceTables().contains("stream_data") : "Test 2 failed";
        System.out.println("  [OK] INSERT INTO");
        
        passed += 2;
    }
    
    private static void testService() {
        System.out.println("[Service Layer]");
        TableLineageService svc = new TableLineageService();
        
        // Test record lineage
        var l1 = svc.recordLineage("job1", "Job1", "INSERT INTO t2 SELECT * FROM t1");
        assert l1 != null && l1.getTargetTable().equals("t2") : "Test 1 failed";
        System.out.println("  [OK] Record lineage");
        
        // Test upstream
        svc.recordLineage("job2", "Job2", "INSERT INTO t3 SELECT * FROM t2");
        var upstreams = svc.getUpstreamTables("t3");
        assert upstreams.contains("t2") : "Test 2 failed";
        System.out.println("  [OK] Get upstream");
        
        // Test downstream
        var downstreams = svc.getDownstreamTables("t1");
        assert downstreams.contains("t2") : "Test 3 failed";
        System.out.println("  [OK] Get downstream");
        
        passed += 3;
    }
}
