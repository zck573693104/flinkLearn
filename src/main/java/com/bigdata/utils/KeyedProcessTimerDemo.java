package com.bigdata.utils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;

import java.time.Duration;


public class KeyedProcessTimerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L)
                );


        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        // TODO Process:keyed
        SingleOutputStreamOperator<String> process = sensorKS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    /**
                     * 来一条数据调用一次
                     * @param value
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processElement(WaterSensor value, CoProcessFunction.Context ctx, Collector<String> out) throws Exception {
                        //获取当前数据的key
                        String currentKey = ctx.getCurrentKey();

                        // TODO 1.定时器注册
                        TimerService timerService = ctx.timerService();

                        // 1、事件时间的案例
                        Long currentEventTime = ctx.timestamp(); // 数据中提取出来的事件时间
                        timerService.registerEventTimeTimer(5000L);
                        System.out.println("当前key=" + currentKey + ",当前时间=" + currentEventTime + ",注册了一个5s的定时器");

                        // 2、处理时间的案例
//                        long currentTs = timerService.currentProcessingTime();
//                        timerService.registerProcessingTimeTimer(currentTs + 5000L);
//                        System.out.println("当前key=" + currentKey + ",当前时间=" + currentTs + ",注册了一个5s后的定时器");


                        // 3、获取 process的 当前watermark
//                        long currentWatermark = timerService.currentWatermark();
//                        System.out.println("当前数据=" + value + ",当前watermark=" + currentWatermark);


                        
                        // 注册定时器： 处理时间、事件时间
//                        timerService.registerProcessingTimeTimer();
//                        timerService.registerEventTimeTimer();
                        // 删除定时器： 处理时间、事件时间
//                        timerService.deleteEventTimeTimer();
//                        timerService.deleteProcessingTimeTimer();

                        // 获取当前时间进展： 处理时间-当前系统时间，  事件时间-当前watermark
//                        long currentTs = timerService.currentProcessingTime();
//                        long wm = timerService.currentWatermark();
                    }


                    /**
                     * TODO 2.时间进展到定时器注册的时间，调用该方法
                     * @param timestamp 当前时间进展，就是定时器被触发时的时间
                     * @param ctx       上下文
                     * @param out       采集器
                     * @throws Exception
                     */
                    @Override
                    public void onTimer(long timestamp, CoProcessFunction.OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        String currentKey = ctx.getCurrentKey();

                        System.out.println("key=" + currentKey + "现在时间是" + timestamp + "定时器触发");
                    }
                }
        );

        process.print();

        env.execute();
    }
}
