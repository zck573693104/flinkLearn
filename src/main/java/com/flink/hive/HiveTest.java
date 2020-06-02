package com.flink.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableUtils;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Optional;

public class HiveTest {


    public static void main(String []args) throws Exception {
        //Kerberos.getKerberosHdfs();
        System.setProperty("HADOOP_USER_NAME", "work");

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name            = "myhive";
        String defaultDatabase = "situation";
        String hiveConfDir     = "/load/data/hive/hive-conf"; // a local path
        String version         = "1.2.1";
        String CATALOG_NAME = "myhive";

        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        hiveCatalog.open();
        tableEnv.registerCatalog(CATALOG_NAME, hiveCatalog);

        Optional<Catalog> myHive = tableEnv.getCatalog(CATALOG_NAME);
        ObjectPath myTablePath = new ObjectPath("situation", "flink_test");
        System.out.println(myHive.get().getTable(myTablePath).getSchema());


        //集成Hive内置函数
       tableEnv.loadModule("hiveModule",new HiveModule(version));

        tableEnv.useCatalog(CATALOG_NAME);

        tableEnv.sqlUpdate("insert into situation.flink_test values (3,'kcz3')");
        Table table = tableEnv.sqlQuery(" select * from situation.flink_test");
        List<Row> rowList =  TableUtils.collectToList(table);
        System.out.println(rowList);


    }
}
