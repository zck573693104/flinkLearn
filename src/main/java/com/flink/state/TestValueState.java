package com.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TestValueState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(Tuple2.of(1L, 2L), Tuple2.of(1L, 3L), Tuple2.of(1L, 4L),Tuple2.of(1L, 5L),Tuple2.of(1L, 6L))
                .keyBy(0)
                .flatMap(new CountWindowAverage())
                .print();
        env.execute("test-value");
    }

    public static class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        /**
         * ValueState状态句柄. 第一个值为count，第二个值为sum。
         */
        private transient ValueState<Tuple2<Long, Long>> valueState;

        @Override
        public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
            // 获取当前状态值
            Tuple2<Long, Long> currentSum = valueState.value();

            // 更新
            currentSum.f0 += 1;
            currentSum.f1 += input.f1;

            // 更新状态值
            valueState.update(currentSum);

            // 如果count >=2 清空状态值，重新计算
            if (currentSum.f0 >= 2) {
                out.collect(new Tuple2<>(input.f0, input.f1));
                valueState.clear();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                    new ValueStateDescriptor<>(
                            "average", // 状态名称
                            TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}),Tuple2.of(0L,0L)); // 状态默认值
            valueState = getRuntimeContext().getState(descriptor);
        }
    }
}
