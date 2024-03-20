package com.bigdata.utils;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class HiveUtils {
    public static void registerHiveEnv(StreamTableEnvironment tEnv, String active) {
        String name = "myhive";
        String defaultDatabase = "catalog";
        String hiveConfDir = "test".equals(active) ? "/etc/hive/conf.cloudera.hive/" : "/opt/Bigdata/client/Hive/config";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tEnv.registerCatalog(name, hive);
        tEnv.useCatalog(name);
    }
}
