package org.fuwushe.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 将获取到的每条Kafka消息根据
 */
public class MessageSplitter implements FlatMapFunction<String, Tuple2<String, Long>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {

        String[] parts = value.split(",");
        out.collect(new Tuple2<>(parts[1], Long.parseLong(parts[0])));

    }
}