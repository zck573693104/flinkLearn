package org.fuwushe.kafka;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * 根据Kafka消息确定Flink的水位
 */
public class MessageWaterEmitter implements AssignerWithPunctuatedWatermarks<String> {

    @Override
    public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {

        String[] parts = lastElement.split(",");
        return new Watermark(Long.parseLong(parts[0]));

    }

    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {

        String[] parts = element.split(",");
        return Long.parseLong(parts[0]);
    }
}