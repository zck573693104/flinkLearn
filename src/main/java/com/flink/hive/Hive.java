package com.flink.hive;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class Hive {
    public static void main(String []args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        String name            = "myhive";
        String defaultDatabase = "situation";
        String hiveConfDir     = "/load/data/hive/hive-conf"; // a local path
        String version         = "1.2.1";

        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);

        hiveCatalog.open();

        tableEnv.registerCatalog("myhive", hiveCatalog);

        Optional<Catalog> myHive = tableEnv.getCatalog("myhive");

        ObjectPath myTablePath = new ObjectPath("situation", "flink_test");
// 这里可以打印
        System.out.println(myHive.get().getTable(myTablePath).getSchema());

        tableEnv.useCatalog("myhive");





        // create tablePath
        ObjectPath tablePath = new ObjectPath(
                "situation",
                "flink_hive_test_partition");
// createa partition
        CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(
                new HashMap<String, String>() {{
                    put("data", "2019-09-07");
                    put("hour", "18");
                }});
        CatalogPartition partition = new CatalogPartitionImpl(new HashMap<String, String>() {{
            put("is_streaming", "false");
            //Currently only supports non-generic CatalogPartition
            put("is_generic", "false");
        }}, "test comment");

        hiveCatalog.createPartition(tablePath, partitionSpec, partition, true);

        // partition
        CatalogPartitionSpec dropPartitionSpec = new CatalogPartitionSpec(
                new HashMap<String, String>() {{
                    put("data", "2019-09-07");
                    put("hour", "18");
                }});
        hiveCatalog.dropPartition(tablePath, dropPartitionSpec, true);


        // create tablePath
        ObjectPath dropTablePath = new ObjectPath(
                "situation",
                "flink_hive_test_partition");
        hiveCatalog.dropTable(dropTablePath, true);

        tableEnv.execute("");
    }
}
