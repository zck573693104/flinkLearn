package com.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

public class KafkaSql {

    public static void main(String[] args) throws Exception {

        //2、设置运行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);
        streamEnv.setParallelism(1);
        tableEnv
                .connect(
                        new Kafka()
                                .version("universal")
                                .topic("browTopic")
                                .startFromEarliest()
                                .property("zookeeper.connect",
                                        "localhost:2181")
                                .property("bootstrap.servers",
                                        "localhost:9092")
                )
                .withFormat(
                        new Json()
                )
                .withSchema(
                        new Schema()
                                .field("business", DataTypes.STRING())
                                .field("data", DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("tracking_number",
                                                        DataTypes.STRING()),
                                                DataTypes.FIELD("invoice_no",
                                                        DataTypes.STRING()))))
                                .field("database", DataTypes.STRING())
//                                .field("old",
//                                        DataTypes.ARRAY(DataTypes.ROW(DataTypes.FIELD("logistics_status",
//                                                DataTypes.DECIMAL(38,18)))))
                                .field("table", DataTypes.STRING())
                                .field("ts", DataTypes.BIGINT())
                                .field("type", DataTypes.STRING())
                                .field("putRowNum", DataTypes.BIGINT())
                )
                .createTemporaryTable("Test");

       // Table table =  tableEnv.sqlQuery("select * from Test ");
       // tableEnv.toRetractStream(table, Row.class).print();
        String sourceDDL = "CREATE TABLE sourceTable (userId VARCHAR, eventType VARCHAR) WITH (\n"
                + "\t'connector.type' = 'kafka',\n" + "\t'connector.version' = 'universal',\n"
                + "\t'connector.startup-mode' = 'earliest-offset',\n" + "\t'connector.topic' = 'browTopic',\n"
                + "\t  'connector.properties.group.id' = 'testGroup',\n"
                + "\t'connector.properties.zookeeper.connect' = 'localhost:2181',\n"
                + "\t'connector.properties.bootstrap.servers' = 'localhost:9092',\n" + "\t'update-mode' = 'append',\n"
                + "\t'format.type' = 'json',\n" + "\t'format.derive-schema' = 'true'\n" + ")";
        System.out.println(sourceDDL);
        String sinkDDL =
                " CREATE TABLE sinkTable (\n" + "    userId VARCHAR,\n" + "    eventType VARCHAR\n" + ") WITH (\n"
                        + "    'connector.type' = 'jdbc',\n"
                        + "    'connector.url' = 'jdbc:mysql://localhost:3306/flink_test?autoReconnect=true&failOverReadOnly=false&useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8',\n"
                        + "    'connector.table' = 'sinkTable',\n" + "    'connector.username' = 'root',\n"
                        + "    'connector.password' = '123456',\n" + "    'connector.write.flush.max-rows' = '1'\n"
                        + ") ";

        String sinkSql = "insert into sinkTable select * from sourceTable";

        tableEnv.sqlUpdate(sourceDDL);
        tableEnv.sqlUpdate(sinkDDL);
        tableEnv.sqlUpdate(sinkSql);

        streamEnv.execute();
    }
}
