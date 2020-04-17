package com.flink.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.Optional;

public class HiveTest {


    public static void main(String []args) throws Exception {
        //Kerberos.getKerberosHdfs();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name            = "myhive";
        String defaultDatabase = "biz_da";
        String hiveConfDir     = "/load/data/hive-2/hive-conf"; // a local path
        String version         = "2.3.2";
        String CATALOG_NAME = "myhive";

        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        hiveCatalog.open();
        tableEnv.registerCatalog(CATALOG_NAME, hiveCatalog);

        Optional<Catalog> myHive = tableEnv.getCatalog(CATALOG_NAME);
        ObjectPath myTablePath = new ObjectPath("biz_da", "orders");
        System.out.println(myHive.get().getTable(myTablePath).getSchema());


        //集成Hive内置函数
       // tableEnv.loadModule("hiveModule",new HiveModule(version));

        tableEnv.useCatalog(CATALOG_NAME);
//        Table table = tableEnv.sqlQuery(" select * from orders");
//        List<Row> rowList =  TableUtils.collectToList(table);
//        System.out.println(rowList);

        tableEnv.sqlUpdate("insert into biz_da.orders values ('order_3','store_3')");

        tableEnv.execute("test");
    }
}
