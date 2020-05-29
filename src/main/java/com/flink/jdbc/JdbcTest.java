package com.flink.jdbc;

import com.alibaba.fastjson.JSONObject;
import com.flink.es.ElasticSearchSinkUtil;
import lombok.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class JdbcTest {
    private static String  driverClass = "com.mysql.cj.jdbc.Driver";
    private static String dbUrl = "jdbc:mysql://localhost:3306/flink?autoReconnect=true&failOverReadOnly=false&useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8";
    private static String userNmae = "root";
    private static String passWord = "123456";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Person> input = env.fromCollection(Arrays.asList(new Person("123", 25), new Person("456", 24)));
        DataStream<Row> ds = input.map(new RichMapFunction<Person, Row>() {
            @Override
            public Row map(Person person) throws Exception {
                return Row.of(person.getAge(),person.getName());
            }
        });
        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO


        };

        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
        //写入mysql
        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setDrivername(driverClass)
                .setDBUrl(dbUrl)
                .setUsername(userNmae)
                .setPassword(passWord)
                .setParameterTypes(fieldTypes)
//                .setQuery("insert into student values(?,?)")
                .setQuery("update `stu-dent` set age = ? where name = ?")
                .build();

     //   sink.emitDataStream(ds);

        //查询mysql
        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(driverClass)
                .setDBUrl(dbUrl)
                .setUsername(userNmae)
                .setPassword(passWord)
                .setFetchSize(2)

                .setQuery("select name,age from student")
                .setRowTypeInfo(rowTypeInfo)
                .finish();
        DataStreamSource<Row> input1 = env.createInput(jdbcInputFormat);
        DataStream<String> dataStream =  input1.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row row) throws Exception {
                JSONObject json = new JSONObject();
                json.put("name",row.getField(0));
                json.put("age",row.getField(1));
                return json.toString();
            }
        });
        ElasticSearchSinkUtil.sink(dataStream);

        env.execute();
    }
    @Data
    public static class Person {
        public String name;
        public Integer age;

        public Person() {
        }

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }
    }
}
