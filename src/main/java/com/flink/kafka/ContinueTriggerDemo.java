package com.flink.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ContinueTriggerDemo {

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub

        String hostName = "219.234.84.246";
        Integer port = Integer.parseInt("8001");
        ;

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        // 从指定socket获取输入数据
        DataStream<String> text = env.socketTextStream(hostName, port);

        text.flatMap(new LineSplitter()) //数据语句分词
                .keyBy(0) // 流按照单词分区
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))// 设置一个20s的滚动窗口
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(11)))//窗口每统计一次当前计算结果
                .sum(1)// count求和
                .map(new Mapdemo())//输出结果加上时间戳
                .print();

        env.execute("Java WordCount from SocketTextStream Example");

    }

    /**
     * Implements the string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and
     * splits it into multiple pairs in the form of "(word,1)" (Tuple2<String,
     * Integer>).
     */
    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }

    public static final class Mapdemo
            implements MapFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>> {

        @Override
        public Tuple3<String, String, Integer> map(Tuple2<String, Integer> value)
                throws Exception {
            // TODO Auto-generated method stub

            DateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String s = format2.format(new Date());

            return new Tuple3<String, String, Integer>(value.f0, s, value.f1);
        }
    }
    


}