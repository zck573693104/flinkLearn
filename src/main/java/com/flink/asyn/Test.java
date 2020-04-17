package com.flink.asyn;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.addons.hbase.HBaseTableSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.io.jdbc.JDBCLookupOptions;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCTableSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import com.flink.item.UserBehaviorVO;

import java.util.Properties;


@Slf4j
public class Test {
    public static void main(String[] args) throws Exception{


        //1、解析命令行参数
        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(fromArgs.getRequired("application"));

        String kafkaBootstrapServers = parameterTool.getRequired("kafkaBootstrapServers");
        String browseTopic = parameterTool.getRequired("browseTopic");
        String browseTopicGroupID = parameterTool.getRequired("browseTopicGroupID");

        String hbaseZookeeperQuorum = parameterTool.getRequired("hbaseZookeeperQuorum");
        String hbaseZnode = parameterTool.getRequired("hbaseZnode");
        String hbaseTable = parameterTool.getRequired("hbaseTable");

        String mysqlDBUrl = parameterTool.getRequired("mysqlDBUrl");
        String mysqlUser = parameterTool.getRequired("mysqlUser");
        String mysqlPwd = parameterTool.getRequired("mysqlPwd");
        String mysqlTable = parameterTool.getRequired("mysqlTable");

        //2、设置运行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);
        streamEnv.setParallelism(1);

        //3、注册Kafka数据源
        //自己造的测试数据,某个用户在某个时刻点击了某个商品，以及商品的价值，如下
        //{"userID": "user_1", "eventTime": "2016-01-01 10:02:00", "eventType": "browse", "productID": "product_1", "productPrice": 20}
        Properties browseProperties = new Properties();
        browseProperties.put("bootstrap.servers",kafkaBootstrapServers);
        browseProperties.put("group.id",browseTopicGroupID);
        DataStream<UserBehaviorVO> browseStream=streamEnv
                .addSource(new FlinkKafkaConsumer<>(browseTopic, new SimpleStringSchema(), browseProperties))
                .process(new BrowseKafkaProcessFunction());
        tableEnv.registerDataStream("kafka",browseStream,"userID,eventTime,eventTimeTimestamp,eventType,productID,productPrice");
        //tableEnv.toAppendStream(tableEnv.scan("kafka"),Row.class).print();

        //4、注册HBase数据源(Lookup Table Source)
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
        conf.set("zookeeper.znode.parent",hbaseZnode);
        HBaseTableSource hBaseTableSource = new HBaseTableSource(conf, hbaseTable);
        hBaseTableSource.setRowKey("uid",String.class);
        hBaseTableSource.addColumn("f1","name",String.class);
        hBaseTableSource.addColumn("f1","age",Integer.class);
        tableEnv.registerTableSource("hbase",hBaseTableSource);
        //注册TableFunction
        tableEnv.registerFunction("hbaseLookup", hBaseTableSource.getLookupFunction(new String[]{"uid"}));

        //5、注册Mysql数据源(Lookup Table Source)
        String[] mysqlFieldNames={"pid","productName","productCategory","updatedAt"};
        DataType[] mysqlFieldTypes={DataTypes.STRING(),DataTypes.STRING(),DataTypes.STRING(),DataTypes.STRING()};
        TableSchema mysqlTableSchema = TableSchema.builder().fields(mysqlFieldNames, mysqlFieldTypes).build();
        JDBCOptions jdbcOptions = JDBCOptions.builder()
                .setDriverName("com.mysql.jdbc.Driver")
                .setDBUrl(mysqlDBUrl)
                .setUsername(mysqlUser)
                .setPassword(mysqlPwd)
                .setTableName(mysqlTable)
                .build();

        JDBCLookupOptions jdbcLookupOptions = JDBCLookupOptions.builder()
                .setCacheExpireMs(10 * 1000) //缓存有效期
                .setCacheMaxSize(10) //最大缓存数据条数
                .setMaxRetryTimes(3) //最大重试次数
                .build();

        JDBCTableSource jdbcTableSource = JDBCTableSource.builder()
                .setOptions(jdbcOptions)
                .setLookupOptions(jdbcLookupOptions)
                .setSchema(mysqlTableSchema)
                .build();
        tableEnv.registerTableSource("mysql",jdbcTableSource);
        //注册TableFunction
        tableEnv.registerFunction("mysqlLookup",jdbcTableSource.getLookupFunction(new String[]{"pid"}));


        //6、查询
        //根据userID, 从HBase表中获取用户基础信息
        //根据productID, 从Mysql表中获取产品基础信息
        String sql = ""
                + "SELECT "
                + "       userID, "
                + "       eventTime, "
                + "       eventType, "
                + "       productID, "
                + "       productPrice, "
                + "       f1.name AS userName, "
                + "       f1.age AS userAge, "
                + "       productName, "
                + "       productCategory "
                + "FROM "
                + "     kafka, "
                + "     LATERAL TABLE(hbaseLookup(userID)), "
                + "     LATERAL TABLE (mysqlLookup(productID))";

        tableEnv.toAppendStream(tableEnv.sqlQuery(sql),Row.class).print();

        //7、开始执行
        tableEnv.execute(Test.class.getSimpleName());
    }

    /**
     * 解析Kafka数据
     */
    static class BrowseKafkaProcessFunction extends ProcessFunction<String, UserBehaviorVO> {
        @Override
        public void processElement(String value, Context ctx, Collector<UserBehaviorVO> out) throws Exception {
            try {

                UserBehaviorVO log = JSON.parseObject(value, UserBehaviorVO.class);


                out.collect(log);
            }catch (Exception ex){
                log.error("解析Kafka数据异常...",ex);
            }
        }
    }

}
