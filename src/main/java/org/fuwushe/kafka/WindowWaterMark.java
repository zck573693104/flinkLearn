package org.fuwushe.kafka;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class WindowWaterMark {

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub


        String hostName = "master";
        Integer port = Integer.parseInt("8001");

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);
        env.getConfig().setAutoWatermarkInterval(9000);

        // get input data
        DataStream<String> text = env.socketTextStream(hostName, port);

        DataStream<Tuple3<String, Long, Integer>> counts =

                text.filter(new FilterClass()).map(new LineSplitter()).assignTimestampsAndWatermarks(
                        new AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>>() {

                            private long currentMaxTimestamp = 0;

                            private final long maxOutOfOrderness = 3500;

                            @Override
                            public long extractTimestamp(Tuple3<String, Long, Integer> element,
                                    long previousElementTimestamp) {

                                long timestamp = element.f1;
                                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                                System.out.println(
                                        "get timestamp is " + new Date(timestamp) + " currentMaxTimestamp " + new Date(
                                                currentMaxTimestamp));
                                return timestamp;
                            }

                            @Override
                            public Watermark getCurrentWatermark() {
                                // TODO Auto-generated method stub
                                System.out.println(
                                        "wall clock is " + new Date(System.currentTimeMillis()) + " new watermark " + (
                                                currentMaxTimestamp - maxOutOfOrderness));
                                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);

                            }

                        }).keyBy(0).timeWindow(Time.seconds(5))
                        //        .allowedLateness(Time.seconds(10))
                        .sum(2);

        counts.print();

        // execute program
        env.execute("Java WordCount from SocketTextStream Example");
    }


    public static final class LineSplitter implements MapFunction<String, Tuple3<String, Long, Integer>> {

        @Override
        public Tuple3<String, Long, Integer> map(String value) throws Exception {
            // TODO Auto-generated method stub
            String[] tokens = value.toLowerCase().split("\\W+");

            long eventtime = Long.parseLong(tokens[1]);

            return new Tuple3<>(tokens[0], eventtime, 1);
        }
    }


    public static final class FilterClass implements FilterFunction<String> {

        @Override
        public boolean filter(String value) throws Exception {
            if (StringUtils.isNullOrWhitespaceOnly(value)) {
                return false;
            } else {
                return true;
            }
        }

    }

}