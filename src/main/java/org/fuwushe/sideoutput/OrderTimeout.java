package org.fuwushe.sideoutput;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

public class OrderTimeout {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<OrderEvent> loginEventStream = env.fromCollection(Arrays.asList(
                new OrderEvent("1", "create"),
                new OrderEvent("2", "create"),
                new OrderEvent("2", "pay")

        ));

        Pattern<OrderEvent, OrderEvent> loginFailPattern = Pattern.<OrderEvent>
                begin("begin")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent loginEvent, Context context) throws InterruptedException {
                        return loginEvent.getType().equals("create");
                    }
                })
                .next("next")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent loginEvent, Context context) {
                        return loginEvent.getType().equals("pay");
                    }
                })
                .within(Time.seconds(1));

        PatternStream<OrderEvent> patternStream = CEP.pattern(
                loginEventStream.keyBy(OrderEvent::getUserId),
                loginFailPattern);

        OutputTag<OrderEvent> orderTiemoutOutput = new OutputTag<OrderEvent>("orderTimeout") {
        };

        SingleOutputStreamOperator<OrderEvent> complexResult = patternStream.select(
                orderTiemoutOutput,
                (PatternTimeoutFunction<OrderEvent, OrderEvent>) (map, l) -> new OrderEvent("orderTimeout", map.get("begin").get(0).getUserId()),
                (PatternSelectFunction<OrderEvent, OrderEvent>) map -> new OrderEvent("success", map.get("next").get(0).getUserId())
        );
        DataStream<OrderEvent> timeoutResult = complexResult.getSideOutput(orderTiemoutOutput);

        complexResult.print();
        timeoutResult.print();

        env.execute();

    }

}