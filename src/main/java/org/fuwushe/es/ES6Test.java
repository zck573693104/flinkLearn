package org.fuwushe.es;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;

public class ES6Test {
    public static void main(String[] args) throws Exception {

        //2、设置运行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);
        streamEnv.setParallelism(1);
        tableEnv.connect(
                new Elasticsearch()
                        .version("6")                      // required: valid connector versions are "6"
                        .host("localhost", 9200, "http")   // required: one or more Elasticsearch hosts to connect to
                        .index("zck_index")                  // required: Elasticsearch index
                        .documentType("user")              // required: Elasticsearch document type

        )
                .withFormat(new Json())
                .withSchema(new Schema().field("name", DataTypes.STRING()))
                .inUpsertMode()
                .createTemporaryTable("test_es");
        tableEnv.sqlUpdate("insert into test_es values ('test')");
        streamEnv.execute("es");
    }

}
